#!/usr/bin/env python3
"""
数据一致性检查和自动修复系统
检查本地数据与云端数据的一致性，并提供自动修复功能
"""

import json
import os
import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from google.cloud import bigquery
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/a606/cloud_function_source/logs/consistency_checker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataConsistencyChecker:
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.local_data_dir = "/Users/a606/cloud_function_source/local_data"
        self.consistency_db_path = "/Users/a606/cloud_function_source/local_data/consistency_checks.db"
        
        # 确保目录存在
        os.makedirs(self.local_data_dir, exist_ok=True)
        os.makedirs("/Users/a606/cloud_function_source/logs", exist_ok=True)
        
        # 初始化BigQuery客户端
        self.bq_client = bigquery.Client(project=self.project_id)
        
        # 初始化一致性检查数据库
        self.init_consistency_database()
        
    def init_consistency_database(self):
        """初始化一致性检查数据库"""
        try:
            conn = sqlite3.connect(self.consistency_db_path)
            cursor = conn.cursor()
            
            # 创建一致性检查记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS consistency_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT,
                    check_time TEXT,
                    local_hash TEXT,
                    cloud_hash TEXT,
                    local_row_count INTEGER,
                    cloud_row_count INTEGER,
                    is_consistent BOOLEAN,
                    inconsistency_type TEXT,
                    auto_repair_attempted BOOLEAN,
                    auto_repair_success BOOLEAN,
                    created_at TEXT
                )
            ''')
            
            # 创建修复操作记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS repair_operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT,
                    repair_type TEXT,
                    repair_time TEXT,
                    before_state TEXT,
                    after_state TEXT,
                    success BOOLEAN,
                    error_message TEXT,
                    created_at TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("一致性检查数据库初始化完成")
            
        except Exception as e:
            logger.error(f"初始化一致性检查数据库失败: {e}")
    
    def calculate_data_hash(self, data: List[Dict[str, Any]]) -> str:
        """计算数据的哈希值"""
        try:
            # 将数据转换为字符串并排序以确保一致性
            data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
            return hashlib.md5(data_str.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"计算数据哈希失败: {e}")
            return ""
    
    def get_local_data_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取本地数据信息"""
        try:
            local_file = os.path.join(self.local_data_dir, f"{table_name}.json")
            
            if not os.path.exists(local_file):
                return None
            
            with open(local_file, 'r', encoding='utf-8') as f:
                local_data = json.load(f)
            
            data_rows = local_data.get("data", [])
            
            return {
                "row_count": len(data_rows),
                "data_hash": self.calculate_data_hash(data_rows),
                "last_modified": datetime.fromtimestamp(os.path.getmtime(local_file)).isoformat(),
                "file_size": os.path.getsize(local_file)
            }
            
        except Exception as e:
            logger.error(f"获取本地数据信息失败 {table_name}: {e}")
            return None
    
    def get_cloud_data_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取云端数据信息"""
        try:
            # 获取行数
            count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            """
            
            count_job = self.bq_client.query(count_query)
            count_result = list(count_job.result())
            row_count = count_result[0].row_count if count_result else 0
            
            # 获取数据样本用于哈希计算（限制行数以避免性能问题）
            sample_query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            ORDER BY 1
            LIMIT 1000
            """
            
            sample_job = self.bq_client.query(sample_query)
            sample_results = sample_job.result()
            
            # 转换为字典列表
            sample_data = []
            for row in sample_results:
                row_dict = {}
                for field in sample_results.schema:
                    value = row[field.name]
                    if value is not None:
                        if hasattr(value, 'isoformat'):  # datetime对象
                            row_dict[field.name] = value.isoformat()
                        else:
                            row_dict[field.name] = value
                    else:
                        row_dict[field.name] = None
                sample_data.append(row_dict)
            
            return {
                "row_count": row_count,
                "data_hash": self.calculate_data_hash(sample_data),
                "sample_size": len(sample_data),
                "check_time": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"获取云端数据信息失败 {table_name}: {e}")
            return None
    
    def check_table_consistency(self, table_name: str) -> Dict[str, Any]:
        """检查单个表的一致性"""
        logger.info(f"检查表一致性: {table_name}")
        
        check_result = {
            "table_name": table_name,
            "check_time": datetime.now().isoformat(),
            "is_consistent": False,
            "inconsistency_type": None,
            "local_info": None,
            "cloud_info": None,
            "error": None
        }
        
        try:
            # 获取本地数据信息
            local_info = self.get_local_data_info(table_name)
            check_result["local_info"] = local_info
            
            # 获取云端数据信息
            cloud_info = self.get_cloud_data_info(table_name)
            check_result["cloud_info"] = cloud_info
            
            if local_info is None and cloud_info is None:
                check_result["inconsistency_type"] = "both_missing"
                check_result["error"] = "本地和云端都没有数据"
            elif local_info is None:
                check_result["inconsistency_type"] = "local_missing"
                check_result["error"] = "本地数据缺失"
            elif cloud_info is None:
                check_result["inconsistency_type"] = "cloud_missing"
                check_result["error"] = "云端数据缺失"
            else:
                # 比较数据
                if local_info["row_count"] != cloud_info["row_count"]:
                    check_result["inconsistency_type"] = "row_count_mismatch"
                    check_result["error"] = f"行数不匹配: 本地{local_info['row_count']}, 云端{cloud_info['row_count']}"
                elif local_info["data_hash"] != cloud_info["data_hash"]:
                    check_result["inconsistency_type"] = "data_hash_mismatch"
                    check_result["error"] = "数据内容不一致"
                else:
                    check_result["is_consistent"] = True
                    logger.info(f"✅ 表 {table_name} 数据一致")
            
            # 记录检查结果
            self.record_consistency_check(check_result)
            
            if not check_result["is_consistent"]:
                logger.warning(f"⚠️ 表 {table_name} 数据不一致: {check_result['error']}")
            
            return check_result
            
        except Exception as e:
            error_msg = f"检查表 {table_name} 一致性失败: {str(e)}"
            logger.error(error_msg)
            check_result["error"] = error_msg
            return check_result
    
    def record_consistency_check(self, check_result: Dict[str, Any]):
        """记录一致性检查结果"""
        try:
            conn = sqlite3.connect(self.consistency_db_path)
            cursor = conn.cursor()
            
            local_info = check_result.get("local_info", {})
            cloud_info = check_result.get("cloud_info", {})
            
            cursor.execute('''
                INSERT INTO consistency_checks 
                (table_name, check_time, local_hash, cloud_hash, local_row_count, 
                 cloud_row_count, is_consistent, inconsistency_type, auto_repair_attempted,
                 auto_repair_success, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                check_result["table_name"],
                check_result["check_time"],
                local_info.get("data_hash", "") if local_info else "",
                cloud_info.get("data_hash", "") if cloud_info else "",
                local_info.get("row_count", 0) if local_info else 0,
                cloud_info.get("row_count", 0) if cloud_info else 0,
                check_result["is_consistent"],
                check_result["inconsistency_type"],
                False,  # auto_repair_attempted
                False,  # auto_repair_success
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"记录一致性检查结果失败: {e}")
    
    def auto_repair_inconsistency(self, check_result: Dict[str, Any]) -> bool:
        """自动修复数据不一致问题"""
        table_name = check_result["table_name"]
        inconsistency_type = check_result["inconsistency_type"]
        
        logger.info(f"尝试自动修复表 {table_name} 的不一致问题: {inconsistency_type}")
        
        repair_success = False
        repair_error = None
        
        try:
            if inconsistency_type == "local_missing":
                # 本地数据缺失，从云端重新下载
                repair_success = self.repair_by_redownload(table_name)
                
            elif inconsistency_type in ["row_count_mismatch", "data_hash_mismatch"]:
                # 数据不一致，重新同步
                repair_success = self.repair_by_resync(table_name)
                
            else:
                repair_error = f"不支持的修复类型: {inconsistency_type}"
                logger.warning(repair_error)
            
            # 记录修复操作
            self.record_repair_operation(
                table_name, inconsistency_type, repair_success, repair_error
            )
            
            if repair_success:
                logger.info(f"✅ 表 {table_name} 自动修复成功")
            else:
                logger.error(f"❌ 表 {table_name} 自动修复失败: {repair_error}")
            
            return repair_success
            
        except Exception as e:
            repair_error = f"自动修复失败: {str(e)}"
            logger.error(f"表 {table_name} 自动修复异常: {repair_error}")
            
            self.record_repair_operation(
                table_name, inconsistency_type, False, repair_error
            )
            
            return False
    
    def repair_by_redownload(self, table_name: str) -> bool:
        """通过重新下载修复"""
        try:
            # 从云端获取数据
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            LIMIT 50000
            """
            
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            # 转换数据
            rows = []
            schema_info = []
            
            for field in results.schema:
                schema_info.append({
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode
                })
            
            for row in results:
                row_dict = {}
                for field in results.schema:
                    value = row[field.name]
                    if value is not None:
                        if hasattr(value, 'isoformat'):
                            row_dict[field.name] = value.isoformat()
                        else:
                            row_dict[field.name] = value
                    else:
                        row_dict[field.name] = None
                rows.append(row_dict)
            
            # 保存到本地
            output_file = os.path.join(self.local_data_dir, f"{table_name}.json")
            export_data = {
                "table_name": table_name,
                "repair_time": datetime.now().isoformat(),
                "row_count": len(rows),
                "schema": schema_info,
                "data": rows
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"重新下载表 {table_name} 完成，共 {len(rows)} 行")
            return True
            
        except Exception as e:
            logger.error(f"重新下载表 {table_name} 失败: {e}")
            return False
    
    def repair_by_resync(self, table_name: str) -> bool:
        """通过重新同步修复"""
        # 这里可以调用同步系统的方法
        return self.repair_by_redownload(table_name)
    
    def record_repair_operation(self, table_name: str, repair_type: str, 
                               success: bool, error_message: str = None):
        """记录修复操作"""
        try:
            conn = sqlite3.connect(self.consistency_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO repair_operations 
                (table_name, repair_type, repair_time, success, error_message, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                table_name,
                repair_type,
                datetime.now().isoformat(),
                success,
                error_message,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"记录修复操作失败: {e}")
    
    def run_comprehensive_consistency_check(self, table_list: List[str] = None) -> Dict[str, Any]:
        """运行全面的一致性检查"""
        logger.info("开始运行全面的数据一致性检查")
        
        if table_list is None:
            # 获取本地所有数据文件
            table_list = []
            for file_name in os.listdir(self.local_data_dir):
                if file_name.endswith('.json') and not file_name.startswith('.'):
                    table_name = file_name[:-5]  # 移除.json后缀
                    if not table_name.endswith('_log') and not table_name.endswith('_report'):
                        table_list.append(table_name)
        
        check_results = {
            "check_time": datetime.now().isoformat(),
            "total_tables": len(table_list),
            "consistent_tables": 0,
            "inconsistent_tables": 0,
            "failed_checks": 0,
            "auto_repairs_attempted": 0,
            "auto_repairs_successful": 0,
            "table_results": []
        }
        
        # 并发检查多个表
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_table = {
                executor.submit(self.check_table_consistency, table_name): table_name
                for table_name in table_list
            }
            
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    check_results["table_results"].append(result)
                    
                    if result.get("error"):
                        check_results["failed_checks"] += 1
                    elif result["is_consistent"]:
                        check_results["consistent_tables"] += 1
                    else:
                        check_results["inconsistent_tables"] += 1
                        
                        # 尝试自动修复
                        check_results["auto_repairs_attempted"] += 1
                        if self.auto_repair_inconsistency(result):
                            check_results["auto_repairs_successful"] += 1
                
                except Exception as e:
                    logger.error(f"检查表 {table_name} 时发生异常: {e}")
                    check_results["failed_checks"] += 1
        
        # 保存检查报告
        report_file = os.path.join(self.local_data_dir, "consistency_check_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(check_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"一致性检查完成: {check_results['consistent_tables']}/{check_results['total_tables']} 表一致")
        return check_results
    
    def get_consistency_summary(self) -> Dict[str, Any]:
        """获取一致性检查摘要"""
        try:
            conn = sqlite3.connect(self.consistency_db_path)
            cursor = conn.cursor()
            
            # 获取最近的检查统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN is_consistent THEN 1 ELSE 0 END) as consistent_count,
                    SUM(CASE WHEN auto_repair_attempted THEN 1 ELSE 0 END) as repair_attempts,
                    SUM(CASE WHEN auto_repair_success THEN 1 ELSE 0 END) as repair_successes
                FROM consistency_checks 
                WHERE DATE(check_time) = DATE('now')
            ''')
            
            stats = cursor.fetchone()
            
            # 获取最近的不一致问题
            cursor.execute('''
                SELECT table_name, inconsistency_type, check_time
                FROM consistency_checks 
                WHERE NOT is_consistent 
                ORDER BY check_time DESC 
                LIMIT 10
            ''')
            
            recent_issues = cursor.fetchall()
            
            conn.close()
            
            return {
                "summary_time": datetime.now().isoformat(),
                "today_total_checks": stats[0] if stats else 0,
                "today_consistent_count": stats[1] if stats else 0,
                "today_repair_attempts": stats[2] if stats else 0,
                "today_repair_successes": stats[3] if stats else 0,
                "recent_issues": [
                    {
                        "table_name": issue[0],
                        "inconsistency_type": issue[1],
                        "check_time": issue[2]
                    }
                    for issue in recent_issues
                ]
            }
            
        except Exception as e:
            logger.error(f"获取一致性摘要失败: {e}")
            return {"error": str(e)}

def main():
    """主函数"""
    checker = DataConsistencyChecker()
    
    print("=" * 60)
    print("数据一致性检查和自动修复系统")
    print("=" * 60)
    
    # 运行全面一致性检查
    check_results = checker.run_comprehensive_consistency_check()
    
    print(f"总检查表数: {check_results['total_tables']}")
    print(f"一致表数: {check_results['consistent_tables']}")
    print(f"不一致表数: {check_results['inconsistent_tables']}")
    print(f"检查失败: {check_results['failed_checks']}")
    print(f"尝试自动修复: {check_results['auto_repairs_attempted']}")
    print(f"修复成功: {check_results['auto_repairs_successful']}")
    
    # 获取摘要
    summary = checker.get_consistency_summary()
    if "error" not in summary:
        print(f"\n今日检查统计:")
        print(f"  总检查次数: {summary['today_total_checks']}")
        print(f"  一致性通过: {summary['today_consistent_count']}")
        print(f"  修复尝试: {summary['today_repair_attempts']}")
        print(f"  修复成功: {summary['today_repair_successes']}")
    
    print("=" * 60)
    print("数据一致性检查完成")
    print("=" * 60)
    
    return check_results

if __name__ == "__main__":
    main()