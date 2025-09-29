#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
云数据修复系统
自动化检查和修复BigQuery数据问题，将云上所有逻辑保存到本地
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from google.cloud import bigquery
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cloud_data_repair.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CloudDataRepairSystem:
    """云数据修复系统"""
    
    def __init__(self):
        """初始化修复系统"""
        self.project_id = "wprojectl"
        self.client = bigquery.Client(project=self.project_id)
        self.local_db_path = "local_data/pc28_local.db"
        self.repair_log = []
        
        # 确保本地数据目录存在
        os.makedirs("local_data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # 初始化本地数据库
        self._init_local_database()
    
    def _init_local_database(self):
        """初始化本地SQLite数据库"""
        try:
            conn = sqlite3.connect(self.local_db_path)
            cursor = conn.cursor()
            
            # 创建本地数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS draws_raw (
                    issue TEXT PRIMARY KEY,
                    timestamp DATETIME,
                    a INTEGER,
                    b INTEGER,
                    c INTEGER,
                    sum INTEGER,
                    tail INTEGER,
                    hour INTEGER,
                    session TEXT,
                    source TEXT,
                    size TEXT,
                    odd_even TEXT,
                    size_calculated TEXT,
                    odd_even_calculated TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cloud_sync_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_time DATETIME,
                    table_name TEXT,
                    records_synced INTEGER,
                    status TEXT,
                    error_message TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS repair_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repair_time DATETIME,
                    issue_type TEXT,
                    description TEXT,
                    action_taken TEXT,
                    status TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("本地数据库初始化完成")
            
        except Exception as e:
            logger.error(f"本地数据库初始化失败: {e}")
            raise
    
    def check_bigquery_data_integrity(self) -> Dict[str, Any]:
        """检查BigQuery数据完整性"""
        logger.info("开始检查BigQuery数据完整性...")
        
        integrity_report = {
            "timestamp": datetime.now().isoformat(),
            "tables": {},
            "issues": [],
            "recommendations": []
        }
        
        # 检查主要数据表
        tables_to_check = [
            "pc28.draws_raw",
            "pc28_lab.draws_14w_clean",
            "pc28_lab.draws_14w_partitioned",
            "pc28_lab.cloud_pred_today_norm",
            "pc28_lab.lab_push_candidates_v2"
        ]
        
        for table_name in tables_to_check:
            try:
                table_info = self._check_table_status(table_name)
                integrity_report["tables"][table_name] = table_info
                
                # 检查数据新鲜度
                if table_info["latest_record"]:
                    latest_time = datetime.fromisoformat(table_info["latest_record"].replace('Z', '+00:00'))
                    hours_old = (datetime.now() - latest_time.replace(tzinfo=None)).total_seconds() / 3600
                    
                    if hours_old > 24:
                        integrity_report["issues"].append({
                            "table": table_name,
                            "type": "data_freshness",
                            "description": f"数据已过时 {hours_old:.1f} 小时",
                            "severity": "high" if hours_old > 72 else "medium"
                        })
                
                # 检查数据量
                if table_info["row_count"] == 0:
                    integrity_report["issues"].append({
                        "table": table_name,
                        "type": "empty_table",
                        "description": "表为空",
                        "severity": "high"
                    })
                
            except Exception as e:
                logger.error(f"检查表 {table_name} 时出错: {e}")
                integrity_report["issues"].append({
                    "table": table_name,
                    "type": "access_error",
                    "description": str(e),
                    "severity": "critical"
                })
        
        # 生成修复建议
        self._generate_repair_recommendations(integrity_report)
        
        return integrity_report
    
    def _check_table_status(self, table_name: str) -> Dict[str, Any]:
        """检查单个表的状态"""
        full_table_name = f"{self.project_id}.{table_name}"
        
        # 获取表信息
        query = f"""
        SELECT 
            COUNT(*) as row_count,
            MAX(timestamp) as latest_record
        FROM `{full_table_name}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        """
        
        result = self.client.query(query).to_dataframe()
        
        return {
            "row_count": int(result.iloc[0]['row_count']),
            "latest_record": str(result.iloc[0]['latest_record']) if result.iloc[0]['latest_record'] else None,
            "table_size_mb": self._get_table_size(full_table_name)
        }
    
    def _get_table_size(self, table_name: str) -> float:
        """获取表大小（MB）"""
        try:
            query = f"""
            SELECT 
                ROUND(size_bytes / 1024 / 1024, 2) as size_mb
            FROM `{self.project_id}.__TABLES__`
            WHERE table_id = '{table_name.split('.')[-1]}'
            """
            result = self.client.query(query).to_dataframe()
            return float(result.iloc[0]['size_mb']) if not result.empty else 0.0
        except:
            return 0.0
    
    def _generate_repair_recommendations(self, report: Dict[str, Any]):
        """生成修复建议"""
        recommendations = []
        
        for issue in report["issues"]:
            if issue["type"] == "data_freshness":
                recommendations.append(f"检查 {issue['table']} 的数据采集流程")
            elif issue["type"] == "empty_table":
                recommendations.append(f"重新初始化 {issue['table']} 数据")
            elif issue["type"] == "access_error":
                recommendations.append(f"检查 {issue['table']} 的访问权限")
        
        report["recommendations"] = recommendations
    
    def sync_cloud_data_to_local(self, table_name: str = "pc28_lab.draws_14w_clean", limit: int = 10000) -> Dict[str, Any]:
        """将云数据同步到本地"""
        logger.info(f"开始同步云数据到本地: {table_name}")
        
        sync_result = {
            "table_name": table_name,
            "start_time": datetime.now().isoformat(),
            "records_synced": 0,
            "status": "started",
            "error": None
        }
        
        try:
            # 查询云数据
            query = f"""
            SELECT *
            FROM `{self.project_id}.{table_name}`
            ORDER BY timestamp DESC
            LIMIT {limit}
            """
            
            df = self.client.query(query).to_dataframe()
            
            if df.empty:
                sync_result["status"] = "no_data"
                sync_result["error"] = "云端表为空"
                return sync_result
            
            # 写入本地数据库
            conn = sqlite3.connect(self.local_db_path)
            
            # 清空本地表（可选）
            conn.execute("DELETE FROM draws_raw")
            
            # 插入新数据
            for _, row in df.iterrows():
                conn.execute('''
                    INSERT OR REPLACE INTO draws_raw 
                    (issue, timestamp, a, b, c, sum, tail, hour, session, source, 
                     size, odd_even, size_calculated, odd_even_calculated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('issue', ''),
                    row.get('timestamp', ''),
                    row.get('a', 0),
                    row.get('b', 0),
                    row.get('c', 0),
                    row.get('sum', 0),
                    row.get('tail', 0),
                    row.get('hour', 0),
                    row.get('session', ''),
                    row.get('source', ''),
                    row.get('size', ''),
                    row.get('odd_even', ''),
                    row.get('size_calculated', ''),
                    row.get('odd_even_calculated', '')
                ))
            
            conn.commit()
            
            # 记录同步日志
            conn.execute('''
                INSERT INTO cloud_sync_log 
                (sync_time, table_name, records_synced, status)
                VALUES (?, ?, ?, ?)
            ''', (datetime.now(), table_name, len(df), "success"))
            
            conn.commit()
            conn.close()
            
            sync_result["records_synced"] = len(df)
            sync_result["status"] = "success"
            sync_result["end_time"] = datetime.now().isoformat()
            
            logger.info(f"成功同步 {len(df)} 条记录到本地")
            
        except Exception as e:
            logger.error(f"同步数据失败: {e}")
            sync_result["status"] = "error"
            sync_result["error"] = str(e)
            sync_result["end_time"] = datetime.now().isoformat()
            
            # 记录错误日志
            try:
                conn = sqlite3.connect(self.local_db_path)
                conn.execute('''
                    INSERT INTO cloud_sync_log 
                    (sync_time, table_name, records_synced, status, error_message)
                    VALUES (?, ?, ?, ?, ?)
                ''', (datetime.now(), table_name, 0, "error", str(e)))
                conn.commit()
                conn.close()
            except:
                pass
        
        return sync_result
    
    def create_local_pull_mechanism(self) -> Dict[str, Any]:
        """创建本地主动拉取云数据的机制"""
        logger.info("创建本地数据拉取机制...")
        
        pull_config = {
            "enabled": True,
            "pull_interval_minutes": 30,
            "tables_to_sync": [
                "pc28_lab.draws_14w_clean",
                "pc28_lab.cloud_pred_today_norm",
                "pc28_lab.lab_push_candidates_v2"
            ],
            "max_records_per_sync": 5000,
            "auto_repair": True
        }
        
        # 保存配置到本地
        with open("local_data/pull_config.json", "w", encoding="utf-8") as f:
            json.dump(pull_config, f, indent=2, ensure_ascii=False)
        
        # 创建拉取脚本
        pull_script = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动数据拉取脚本
定期从云端拉取数据到本地
"""

import json
import time
from cloud_data_repair_system import CloudDataRepairSystem

def main():
    """主函数"""
    repair_system = CloudDataRepairSystem()
    
    # 加载配置
    with open("local_data/pull_config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    
    if not config.get("enabled", False):
        print("数据拉取已禁用")
        return
    
    # 执行数据同步
    for table_name in config["tables_to_sync"]:
        print(f"同步表: {table_name}")
        result = repair_system.sync_cloud_data_to_local(
            table_name=table_name,
            limit=config["max_records_per_sync"]
        )
        print(f"同步结果: {result['status']}, 记录数: {result['records_synced']}")
        
        if result["status"] == "error":
            print(f"同步错误: {result['error']}")
    
    # 检查数据完整性
    if config.get("auto_repair", False):
        print("检查数据完整性...")
        integrity_report = repair_system.check_bigquery_data_integrity()
        print(f"发现 {len(integrity_report['issues'])} 个问题")

if __name__ == "__main__":
    main()
'''
        
        with open("auto_pull_data.py", "w", encoding="utf-8") as f:
            f.write(pull_script)
        
        logger.info("本地数据拉取机制创建完成")
        return pull_config
    
    def generate_repair_report(self) -> str:
        """生成修复报告"""
        logger.info("生成修复报告...")
        
        # 检查数据完整性
        integrity_report = self.check_bigquery_data_integrity()
        
        # 同步数据到本地
        sync_results = []
        for table in ["pc28_lab.draws_14w_clean", "pc28_lab.cloud_pred_today_norm"]:
            result = self.sync_cloud_data_to_local(table, limit=1000)
            sync_results.append(result)
        
        # 生成报告
        report = f"""
# 云数据修复系统报告
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 数据完整性检查
- 检查表数量: {len(integrity_report['tables'])}
- 发现问题: {len(integrity_report['issues'])}
- 修复建议: {len(integrity_report['recommendations'])}

### 表状态详情
"""
        
        for table_name, info in integrity_report['tables'].items():
            report += f"""
#### {table_name}
- 记录数: {info['row_count']:,}
- 最新记录: {info['latest_record']}
- 表大小: {info['table_size_mb']} MB
"""
        
        if integrity_report['issues']:
            report += "\n### 发现的问题\n"
            for issue in integrity_report['issues']:
                report += f"- **{issue['type']}** ({issue['severity']}): {issue['description']}\n"
        
        if integrity_report['recommendations']:
            report += "\n### 修复建议\n"
            for rec in integrity_report['recommendations']:
                report += f"- {rec}\n"
        
        report += "\n## 数据同步结果\n"
        for result in sync_results:
            report += f"""
### {result['table_name']}
- 状态: {result['status']}
- 同步记录数: {result['records_synced']}
- 开始时间: {result['start_time']}
- 结束时间: {result.get('end_time', 'N/A')}
"""
            if result.get('error'):
                report += f"- 错误: {result['error']}\n"
        
        # 保存报告
        report_file = f"logs/repair_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)
        
        logger.info(f"修复报告已保存: {report_file}")
        return report_file

def main():
    """主函数"""
    logger.info("启动云数据修复系统...")
    
    try:
        repair_system = CloudDataRepairSystem()
        
        # 生成修复报告
        report_file = repair_system.generate_repair_report()
        
        # 创建本地拉取机制
        pull_config = repair_system.create_local_pull_mechanism()
        
        logger.info("云数据修复系统运行完成")
        logger.info(f"修复报告: {report_file}")
        logger.info(f"拉取配置: {pull_config}")
        
    except Exception as e:
        logger.error(f"系统运行失败: {e}")
        raise

if __name__ == "__main__":
    main()