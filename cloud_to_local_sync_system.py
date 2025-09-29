#!/usr/bin/env python3
"""
云到本地数据同步系统
完整的数据同步机制，包括增量同步、一致性检查和自动修复
"""

import json
import os
import subprocess
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from google.cloud import bigquery
import sqlite3
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/a606/cloud_function_source/logs/sync_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CloudToLocalSyncSystem:
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.local_data_dir = "/Users/a606/cloud_function_source/local_data"
        self.sync_db_path = "/Users/a606/cloud_function_source/local_data/sync_metadata.db"
        self.export_log_path = "/Users/a606/cloud_function_source/local_data/export_log.json"
        self.sync_config_path = "/Users/a606/cloud_function_source/sync_config.json"
        
        # 确保目录存在
        os.makedirs(self.local_data_dir, exist_ok=True)
        os.makedirs("/Users/a606/cloud_function_source/logs", exist_ok=True)
        
        # 初始化BigQuery客户端
        self.bq_client = bigquery.Client(project=self.project_id)
        
        # 初始化同步元数据数据库
        self.init_sync_database()
        
        # 加载同步配置
        self.sync_config = self.load_sync_config()
        
    def init_sync_database(self):
        """初始化同步元数据数据库"""
        try:
            conn = sqlite3.connect(self.sync_db_path)
            cursor = conn.cursor()
            
            # 创建同步状态表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_status (
                    table_name TEXT PRIMARY KEY,
                    last_sync_time TEXT,
                    last_sync_hash TEXT,
                    row_count INTEGER,
                    sync_status TEXT,
                    error_message TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            
            # 创建同步日志表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT,
                    sync_type TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    status TEXT,
                    rows_synced INTEGER,
                    error_message TEXT,
                    created_at TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("同步元数据数据库初始化完成")
            
        except Exception as e:
            logger.error(f"初始化同步数据库失败: {e}")
    
    def load_sync_config(self) -> Dict[str, Any]:
        """加载同步配置"""
        default_config = {
            "sync_interval_minutes": 30,
            "max_concurrent_syncs": 5,
            "priority_tables": [
                "p_ensemble_today_norm_v5",
                "p_cloud_today_canon_v",
                "p_map_today_canon_v",
                "p_size_today_canon_v",
                "combo_based_predictions"
            ],
            "sync_strategies": {
                "full_sync_tables": ["combo_based_predictions"],
                "incremental_sync_tables": ["p_ensemble_today_norm_v5"],
                "daily_sync_tables": ["p_cloud_today_canon_v", "p_map_today_canon_v", "p_size_today_canon_v"]
            },
            "data_retention_days": 7,
            "enable_consistency_check": True,
            "enable_auto_repair": True
        }
        
        try:
            if os.path.exists(self.sync_config_path):
                with open(self.sync_config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # 合并默认配置
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
            else:
                # 创建默认配置文件
                with open(self.sync_config_path, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, ensure_ascii=False, indent=2)
                return default_config
                
        except Exception as e:
            logger.error(f"加载同步配置失败: {e}")
            return default_config
    
    def get_table_hash(self, table_name: str) -> Optional[str]:
        """获取表的数据哈希值用于一致性检查"""
        try:
            query = f"""
            SELECT 
                COUNT(*) as row_count,
                FARM_FINGERPRINT(
                    STRING_AGG(
                        TO_JSON_STRING(t), 
                        '' ORDER BY TO_JSON_STRING(t)
                    )
                ) as data_hash
            FROM `{self.project_id}.{self.dataset_id}.{table_name}` t
            """
            
            query_job = self.bq_client.query(query)
            results = list(query_job.result())
            
            if results:
                row = results[0]
                return f"{row.row_count}_{row.data_hash}"
            return None
            
        except Exception as e:
            logger.error(f"获取表 {table_name} 哈希值失败: {e}")
            return None
    
    def sync_table_data(self, table_name: str, sync_type: str = "full") -> bool:
        """同步单个表的数据"""
        start_time = datetime.now()
        
        try:
            logger.info(f"开始同步表: {table_name} (类型: {sync_type})")
            
            # 构建查询
            if sync_type == "incremental":
                # 增量同步 - 只同步今天的数据
                query = f"""
                SELECT *
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE DATE(_PARTITIONTIME) = CURRENT_DATE('Asia/Shanghai')
                   OR DATE(COALESCE(ts_utc, timestamp, created_at)) = CURRENT_DATE('Asia/Shanghai')
                LIMIT 50000
                """
            elif sync_type == "daily":
                # 每日同步 - 同步最近3天的数据
                query = f"""
                SELECT *
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE DATE(COALESCE(ts_utc, timestamp, created_at)) >= DATE_SUB(CURRENT_DATE('Asia/Shanghai'), INTERVAL 3 DAY)
                LIMIT 50000
                """
            else:
                # 全量同步
                query = f"""
                SELECT *
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                LIMIT 50000
                """
            
            # 执行查询
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            # 转换数据
            rows = []
            schema_info = []
            
            # 获取schema信息
            for field in results.schema:
                schema_info.append({
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode
                })
            
            # 获取数据行
            for row in results:
                row_dict = {}
                for field in results.schema:
                    value = row[field.name]
                    # 处理特殊数据类型
                    if value is not None:
                        if hasattr(value, 'isoformat'):  # datetime对象
                            row_dict[field.name] = value.isoformat()
                        else:
                            row_dict[field.name] = value
                    else:
                        row_dict[field.name] = None
                rows.append(row_dict)
            
            # 保存到本地文件
            output_file = os.path.join(self.local_data_dir, f"{table_name}.json")
            export_data = {
                "table_name": table_name,
                "sync_type": sync_type,
                "sync_time": start_time.isoformat(),
                "row_count": len(rows),
                "schema": schema_info,
                "data": rows
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            # 获取数据哈希
            data_hash = self.get_table_hash(table_name)
            
            # 更新同步状态
            self.update_sync_status(table_name, start_time, data_hash, len(rows), "success", None)
            
            # 记录同步日志
            end_time = datetime.now()
            self.log_sync_operation(table_name, sync_type, start_time, end_time, "success", len(rows), None)
            
            logger.info(f"表 {table_name} 同步成功，共 {len(rows)} 行数据")
            return True
            
        except Exception as e:
            error_msg = f"同步表 {table_name} 失败: {str(e)}"
            logger.error(error_msg)
            
            # 更新同步状态
            self.update_sync_status(table_name, start_time, None, 0, "failed", error_msg)
            
            # 记录同步日志
            end_time = datetime.now()
            self.log_sync_operation(table_name, sync_type, start_time, end_time, "failed", 0, error_msg)
            
            return False
    
    def update_sync_status(self, table_name: str, sync_time: datetime, data_hash: str, 
                          row_count: int, status: str, error_msg: str):
        """更新同步状态"""
        try:
            conn = sqlite3.connect(self.sync_db_path)
            cursor = conn.cursor()
            
            now = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT OR REPLACE INTO sync_status 
                (table_name, last_sync_time, last_sync_hash, row_count, sync_status, 
                 error_message, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 
                        COALESCE((SELECT created_at FROM sync_status WHERE table_name = ?), ?), ?)
            ''', (table_name, sync_time.isoformat(), data_hash, row_count, status, 
                  error_msg, table_name, now, now))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"更新同步状态失败: {e}")
    
    def log_sync_operation(self, table_name: str, sync_type: str, start_time: datetime, 
                          end_time: datetime, status: str, rows_synced: int, error_msg: str):
        """记录同步操作日志"""
        try:
            conn = sqlite3.connect(self.sync_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO sync_logs 
                (table_name, sync_type, start_time, end_time, status, rows_synced, error_message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (table_name, sync_type, start_time.isoformat(), end_time.isoformat(), 
                  status, rows_synced, error_msg, datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"记录同步日志失败: {e}")
    
    def run_full_sync(self):
        """运行完整同步"""
        logger.info("开始运行完整同步")
        
        priority_tables = self.sync_config.get("priority_tables", [])
        max_workers = self.sync_config.get("max_concurrent_syncs", 5)
        
        # 优先同步重要表
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {}
            
            for table_name in priority_tables:
                # 确定同步策略
                sync_type = "full"
                if table_name in self.sync_config.get("sync_strategies", {}).get("incremental_sync_tables", []):
                    sync_type = "incremental"
                elif table_name in self.sync_config.get("sync_strategies", {}).get("daily_sync_tables", []):
                    sync_type = "daily"
                
                future = executor.submit(self.sync_table_data, table_name, sync_type)
                future_to_table[future] = table_name
            
            # 等待所有任务完成
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    success = future.result()
                    if success:
                        logger.info(f"✅ 表 {table_name} 同步成功")
                    else:
                        logger.error(f"❌ 表 {table_name} 同步失败")
                except Exception as e:
                    logger.error(f"❌ 表 {table_name} 同步异常: {e}")
        
        logger.info("完整同步完成")
    
    def check_data_consistency(self) -> Dict[str, Any]:
        """检查数据一致性"""
        logger.info("开始数据一致性检查")
        
        consistency_report = {
            "check_time": datetime.now().isoformat(),
            "tables_checked": 0,
            "consistent_tables": 0,
            "inconsistent_tables": 0,
            "issues": []
        }
        
        try:
            conn = sqlite3.connect(self.sync_db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT table_name, last_sync_hash FROM sync_status WHERE sync_status = 'success'")
            synced_tables = cursor.fetchall()
            
            for table_name, stored_hash in synced_tables:
                consistency_report["tables_checked"] += 1
                
                # 获取当前哈希
                current_hash = self.get_table_hash(table_name)
                
                if current_hash == stored_hash:
                    consistency_report["consistent_tables"] += 1
                    logger.info(f"✅ 表 {table_name} 数据一致")
                else:
                    consistency_report["inconsistent_tables"] += 1
                    issue = {
                        "table_name": table_name,
                        "issue_type": "hash_mismatch",
                        "stored_hash": stored_hash,
                        "current_hash": current_hash,
                        "detected_at": datetime.now().isoformat()
                    }
                    consistency_report["issues"].append(issue)
                    logger.warning(f"⚠️ 表 {table_name} 数据不一致")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"数据一致性检查失败: {e}")
            consistency_report["error"] = str(e)
        
        # 保存一致性报告
        report_file = os.path.join(self.local_data_dir, "consistency_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(consistency_report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"一致性检查完成: {consistency_report['consistent_tables']}/{consistency_report['tables_checked']} 表一致")
        return consistency_report
    
    def auto_repair_inconsistencies(self, consistency_report: Dict[str, Any]) -> bool:
        """自动修复数据不一致问题"""
        if not self.sync_config.get("enable_auto_repair", False):
            logger.info("自动修复功能已禁用")
            return False
        
        logger.info("开始自动修复数据不一致问题")
        
        repaired_count = 0
        for issue in consistency_report.get("issues", []):
            if issue["issue_type"] == "hash_mismatch":
                table_name = issue["table_name"]
                logger.info(f"修复表 {table_name} 的数据不一致问题")
                
                # 重新同步该表
                if self.sync_table_data(table_name, "full"):
                    repaired_count += 1
                    logger.info(f"✅ 表 {table_name} 修复成功")
                else:
                    logger.error(f"❌ 表 {table_name} 修复失败")
        
        logger.info(f"自动修复完成，成功修复 {repaired_count} 个问题")
        return repaired_count > 0
    
    def get_sync_status_report(self) -> Dict[str, Any]:
        """获取同步状态报告"""
        try:
            conn = sqlite3.connect(self.sync_db_path)
            cursor = conn.cursor()
            
            # 获取同步状态统计
            cursor.execute('''
                SELECT 
                    sync_status,
                    COUNT(*) as count
                FROM sync_status 
                GROUP BY sync_status
            ''')
            status_stats = dict(cursor.fetchall())
            
            # 获取最近的同步日志
            cursor.execute('''
                SELECT table_name, sync_type, start_time, end_time, status, rows_synced
                FROM sync_logs 
                ORDER BY created_at DESC 
                LIMIT 20
            ''')
            recent_logs = cursor.fetchall()
            
            conn.close()
            
            report = {
                "report_time": datetime.now().isoformat(),
                "status_statistics": status_stats,
                "recent_sync_logs": [
                    {
                        "table_name": log[0],
                        "sync_type": log[1],
                        "start_time": log[2],
                        "end_time": log[3],
                        "status": log[4],
                        "rows_synced": log[5]
                    }
                    for log in recent_logs
                ],
                "total_tables_synced": sum(status_stats.values()),
                "successful_syncs": status_stats.get("success", 0),
                "failed_syncs": status_stats.get("failed", 0)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"获取同步状态报告失败: {e}")
            return {"error": str(e)}
    
    def run_monitoring_cycle(self):
        """运行一个完整的监控周期"""
        logger.info("开始运行监控周期")
        
        # 1. 运行完整同步
        self.run_full_sync()
        
        # 2. 检查数据一致性
        if self.sync_config.get("enable_consistency_check", True):
            consistency_report = self.check_data_consistency()
            
            # 3. 自动修复不一致问题
            if consistency_report.get("inconsistent_tables", 0) > 0:
                self.auto_repair_inconsistencies(consistency_report)
        
        # 4. 生成状态报告
        status_report = self.get_sync_status_report()
        
        # 保存状态报告
        report_file = os.path.join("/Users/a606/cloud_function_source/logs", "sync_status_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(status_report, f, ensure_ascii=False, indent=2)
        
        logger.info("监控周期完成")
        return status_report

def main():
    """主函数"""
    sync_system = CloudToLocalSyncSystem()
    
    # 运行一个完整的监控周期
    status_report = sync_system.run_monitoring_cycle()
    
    print("=" * 60)
    print("云到本地数据同步系统运行完成")
    print("=" * 60)
    print(f"总同步表数: {status_report.get('total_tables_synced', 0)}")
    print(f"成功同步: {status_report.get('successful_syncs', 0)}")
    print(f"失败同步: {status_report.get('failed_syncs', 0)}")
    print("=" * 60)
    
    return status_report

if __name__ == "__main__":
    main()