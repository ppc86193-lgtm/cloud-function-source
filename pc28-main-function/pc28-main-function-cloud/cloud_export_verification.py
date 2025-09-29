#!/usr/bin/env python3
"""
云数据导出系统验证工具
验证云数据导出系统的完整性和稳定性
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from google.cloud import bigquery
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/a606/cloud_function_source/logs/export_verification.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CloudExportVerification:
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.local_data_dir = "/Users/a606/cloud_function_source/local_data"
        self.verification_db_path = "/Users/a606/cloud_function_source/local_data/export_verification.db"
        
        # 确保目录存在
        os.makedirs(self.local_data_dir, exist_ok=True)
        os.makedirs("/Users/a606/cloud_function_source/logs", exist_ok=True)
        
        # 初始化BigQuery客户端
        self.bq_client = bigquery.Client(project=self.project_id)
        
        # 初始化验证数据库
        self.init_verification_database()
        
        # 关键表和视图列表
        self.critical_tables = [
            "p_cloud_today_canon_v",
            "p_map_today_canon_v", 
            "p_size_today_canon_v",
            "p_ensemble_today_norm_v5",
            "cloud_pred_today_norm",
            "combo_based_predictions"
        ]
        
    def init_verification_database(self):
        """初始化验证数据库"""
        try:
            conn = sqlite3.connect(self.verification_db_path)
            cursor = conn.cursor()
            
            # 创建导出验证记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS export_verifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT,
                    verification_time TEXT,
                    cloud_row_count INTEGER,
                    local_row_count INTEGER,
                    export_file_size_mb REAL,
                    data_freshness_hours REAL,
                    schema_match BOOLEAN,
                    data_quality_score REAL,
                    verification_status TEXT,
                    error_message TEXT,
                    created_at TEXT
                )
            ''')
            
            # 创建系统稳定性记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stability_tests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_type TEXT,
                    test_time TEXT,
                    duration_seconds REAL,
                    success_rate REAL,
                    total_operations INTEGER,
                    successful_operations INTEGER,
                    failed_operations INTEGER,
                    avg_response_time_ms REAL,
                    error_details TEXT,
                    created_at TEXT
                )
            ''')
            
            # 创建性能基准表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS performance_benchmarks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    benchmark_time TEXT,
                    table_name TEXT,
                    export_duration_seconds REAL,
                    data_transfer_rate_mbps REAL,
                    query_execution_time_ms REAL,
                    memory_usage_mb REAL,
                    cpu_usage_percent REAL,
                    created_at TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("导出验证数据库初始化完成")
            
        except Exception as e:
            logger.error(f"初始化验证数据库失败: {e}")
    
    def get_cloud_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取云端表信息"""
        try:
            # 获取表的基本信息
            table_ref = self.bq_client.dataset(self.dataset_id).table(table_name)
            table = self.bq_client.get_table(table_ref)
            
            # 获取行数
            count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            """
            
            count_job = self.bq_client.query(count_query)
            count_result = list(count_job.result())
            row_count = count_result[0].row_count if count_result else 0
            
            # 获取最新数据时间戳（如果有时间字段）
            latest_timestamp = None
            timestamp_fields = ['timestamp', 'ts_utc', 'ts_cst', 'created_at', 'updated_at']
            
            for field in timestamp_fields:
                try:
                    timestamp_query = f"""
                    SELECT MAX({field}) as latest_timestamp
                    FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                    WHERE {field} IS NOT NULL
                    """
                    
                    timestamp_job = self.bq_client.query(timestamp_query)
                    timestamp_result = list(timestamp_job.result())
                    
                    if timestamp_result and timestamp_result[0].latest_timestamp:
                        latest_timestamp = timestamp_result[0].latest_timestamp
                        break
                        
                except Exception:
                    continue
            
            return {
                "table_name": table_name,
                "row_count": row_count,
                "schema": [{"name": field.name, "type": field.field_type, "mode": field.mode} 
                          for field in table.schema],
                "latest_timestamp": latest_timestamp.isoformat() if latest_timestamp else None,
                "table_size_bytes": table.num_bytes,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None
            }
            
        except Exception as e:
            logger.error(f"获取云端表信息失败 {table_name}: {e}")
            return None
    
    def get_local_export_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取本地导出文件信息"""
        try:
            export_file = os.path.join(self.local_data_dir, f"{table_name}.json")
            
            if not os.path.exists(export_file):
                return None
            
            # 获取文件信息
            file_size = os.path.getsize(export_file)
            file_mtime = os.path.getmtime(export_file)
            
            # 读取导出数据
            with open(export_file, 'r', encoding='utf-8') as f:
                export_data = json.load(f)
            
            data_rows = export_data.get("data", [])
            schema_info = export_data.get("schema", [])
            
            return {
                "file_path": export_file,
                "file_size_bytes": file_size,
                "file_size_mb": file_size / (1024 * 1024),
                "last_modified": datetime.fromtimestamp(file_mtime).isoformat(),
                "row_count": len(data_rows),
                "schema": schema_info,
                "export_time": export_data.get("export_time"),
                "data_freshness_hours": (time.time() - file_mtime) / 3600
            }
            
        except Exception as e:
            logger.error(f"获取本地导出信息失败 {table_name}: {e}")
            return None
    
    def verify_schema_compatibility(self, cloud_schema: List[Dict], local_schema: List[Dict]) -> Dict[str, Any]:
        """验证模式兼容性"""
        try:
            cloud_fields = {field["name"]: field for field in cloud_schema}
            local_fields = {field["name"]: field for field in local_schema}
            
            missing_in_local = set(cloud_fields.keys()) - set(local_fields.keys())
            extra_in_local = set(local_fields.keys()) - set(cloud_fields.keys())
            
            type_mismatches = []
            for field_name in cloud_fields.keys() & local_fields.keys():
                cloud_type = cloud_fields[field_name]["type"]
                local_type = local_fields[field_name]["type"]
                if cloud_type != local_type:
                    type_mismatches.append({
                        "field": field_name,
                        "cloud_type": cloud_type,
                        "local_type": local_type
                    })
            
            is_compatible = len(missing_in_local) == 0 and len(type_mismatches) == 0
            
            return {
                "is_compatible": is_compatible,
                "missing_in_local": list(missing_in_local),
                "extra_in_local": list(extra_in_local),
                "type_mismatches": type_mismatches,
                "compatibility_score": 1.0 if is_compatible else 0.5 if len(type_mismatches) == 0 else 0.0
            }
            
        except Exception as e:
            logger.error(f"验证模式兼容性失败: {e}")
            return {"is_compatible": False, "error": str(e)}
    
    def calculate_data_quality_score(self, table_name: str, local_data: List[Dict]) -> float:
        """计算数据质量分数"""
        try:
            if not local_data:
                return 0.0
            
            total_fields = 0
            non_null_fields = 0
            
            for row in local_data[:100]:  # 检查前100行
                for field_name, value in row.items():
                    total_fields += 1
                    if value is not None and value != "":
                        non_null_fields += 1
            
            completeness_score = non_null_fields / total_fields if total_fields > 0 else 0.0
            
            # 检查数据一致性（简单检查）
            consistency_score = 1.0
            if len(local_data) > 1:
                first_row_keys = set(local_data[0].keys())
                for row in local_data[1:10]:  # 检查前10行的键一致性
                    if set(row.keys()) != first_row_keys:
                        consistency_score = 0.8
                        break
            
            # 综合质量分数
            quality_score = (completeness_score * 0.7) + (consistency_score * 0.3)
            
            return min(1.0, max(0.0, quality_score))
            
        except Exception as e:
            logger.error(f"计算数据质量分数失败 {table_name}: {e}")
            return 0.0
    
    def verify_single_table(self, table_name: str) -> Dict[str, Any]:
        """验证单个表的导出"""
        logger.info(f"验证表导出: {table_name}")
        
        verification_result = {
            "table_name": table_name,
            "verification_time": datetime.now().isoformat(),
            "verification_status": "failed",
            "cloud_info": None,
            "local_info": None,
            "schema_compatibility": None,
            "data_quality_score": 0.0,
            "issues": [],
            "recommendations": []
        }
        
        try:
            # 获取云端信息
            cloud_info = self.get_cloud_table_info(table_name)
            verification_result["cloud_info"] = cloud_info
            
            if not cloud_info:
                verification_result["issues"].append("无法获取云端表信息")
                verification_result["recommendations"].append("检查表名和权限设置")
                return verification_result
            
            # 获取本地导出信息
            local_info = self.get_local_export_info(table_name)
            verification_result["local_info"] = local_info
            
            if not local_info:
                verification_result["issues"].append("本地导出文件不存在")
                verification_result["recommendations"].append("运行数据导出程序")
                return verification_result
            
            # 验证模式兼容性
            schema_compatibility = self.verify_schema_compatibility(
                cloud_info["schema"], local_info["schema"]
            )
            verification_result["schema_compatibility"] = schema_compatibility
            
            if not schema_compatibility["is_compatible"]:
                verification_result["issues"].append("模式不兼容")
                verification_result["recommendations"].append("更新导出程序以匹配云端模式")
            
            # 验证数据行数
            cloud_rows = cloud_info["row_count"]
            local_rows = local_info["row_count"]
            
            if abs(cloud_rows - local_rows) > max(cloud_rows * 0.05, 100):  # 允许5%或100行的差异
                verification_result["issues"].append(f"行数差异过大: 云端{cloud_rows}, 本地{local_rows}")
                verification_result["recommendations"].append("重新运行完整导出")
            
            # 验证数据新鲜度
            data_freshness_hours = local_info["data_freshness_hours"]
            if data_freshness_hours > 24:  # 数据超过24小时
                verification_result["issues"].append(f"数据过期: {data_freshness_hours:.1f}小时")
                verification_result["recommendations"].append("增加导出频率或设置自动更新")
            
            # 计算数据质量分数
            with open(local_info["file_path"], 'r', encoding='utf-8') as f:
                export_data = json.load(f)
            
            data_quality_score = self.calculate_data_quality_score(
                table_name, export_data.get("data", [])
            )
            verification_result["data_quality_score"] = data_quality_score
            
            if data_quality_score < 0.8:
                verification_result["issues"].append(f"数据质量较低: {data_quality_score:.2f}")
                verification_result["recommendations"].append("检查数据源和导出逻辑")
            
            # 确定验证状态
            if len(verification_result["issues"]) == 0:
                verification_result["verification_status"] = "passed"
            elif data_quality_score > 0.7 and schema_compatibility.get("is_compatible", False):
                verification_result["verification_status"] = "warning"
            else:
                verification_result["verification_status"] = "failed"
            
            # 记录验证结果
            self.record_verification_result(verification_result)
            
            status_emoji = "✅" if verification_result["verification_status"] == "passed" else \
                          "⚠️" if verification_result["verification_status"] == "warning" else "❌"
            
            logger.info(f"{status_emoji} 表 {table_name} 验证完成: {verification_result['verification_status']}")
            
            return verification_result
            
        except Exception as e:
            error_msg = f"验证表 {table_name} 失败: {str(e)}"
            logger.error(error_msg)
            verification_result["issues"].append(error_msg)
            verification_result["recommendations"].append("检查系统日志和网络连接")
            return verification_result
    
    def record_verification_result(self, result: Dict[str, Any]):
        """记录验证结果"""
        try:
            conn = sqlite3.connect(self.verification_db_path)
            cursor = conn.cursor()
            
            cloud_info = result.get("cloud_info", {})
            local_info = result.get("local_info", {})
            schema_compat = result.get("schema_compatibility", {})
            
            cursor.execute('''
                INSERT INTO export_verifications 
                (table_name, verification_time, cloud_row_count, local_row_count,
                 export_file_size_mb, data_freshness_hours, schema_match,
                 data_quality_score, verification_status, error_message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                result["table_name"],
                result["verification_time"],
                cloud_info.get("row_count", 0) if cloud_info else 0,
                local_info.get("row_count", 0) if local_info else 0,
                local_info.get("file_size_mb", 0.0) if local_info else 0.0,
                local_info.get("data_freshness_hours", 0.0) if local_info else 0.0,
                schema_compat.get("is_compatible", False) if schema_compat else False,
                result["data_quality_score"],
                result["verification_status"],
                "; ".join(result["issues"]) if result["issues"] else None,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"记录验证结果失败: {e}")
    
    def run_stability_test(self, duration_minutes: int = 30) -> Dict[str, Any]:
        """运行稳定性测试"""
        logger.info(f"开始运行稳定性测试，持续时间: {duration_minutes}分钟")
        
        test_start_time = datetime.now()
        test_results = {
            "test_type": "stability_test",
            "test_time": test_start_time.isoformat(),
            "duration_minutes": duration_minutes,
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "response_times": [],
            "errors": []
        }
        
        end_time = test_start_time + timedelta(minutes=duration_minutes)
        
        while datetime.now() < end_time:
            for table_name in self.critical_tables:
                operation_start = time.time()
                test_results["total_operations"] += 1
                
                try:
                    # 测试云端查询
                    query = f"""
                    SELECT COUNT(*) as count
                    FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                    LIMIT 1
                    """
                    
                    job = self.bq_client.query(query)
                    list(job.result())
                    
                    operation_time = (time.time() - operation_start) * 1000  # 毫秒
                    test_results["response_times"].append(operation_time)
                    test_results["successful_operations"] += 1
                    
                except Exception as e:
                    test_results["failed_operations"] += 1
                    test_results["errors"].append(f"{table_name}: {str(e)}")
                    logger.warning(f"稳定性测试失败 {table_name}: {e}")
                
                # 短暂休息
                time.sleep(1)
            
            # 每轮测试后休息
            time.sleep(30)
        
        # 计算统计信息
        test_duration = (datetime.now() - test_start_time).total_seconds()
        success_rate = test_results["successful_operations"] / test_results["total_operations"] if test_results["total_operations"] > 0 else 0
        avg_response_time = sum(test_results["response_times"]) / len(test_results["response_times"]) if test_results["response_times"] else 0
        
        # 记录稳定性测试结果
        try:
            conn = sqlite3.connect(self.verification_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO stability_tests 
                (test_type, test_time, duration_seconds, success_rate, total_operations,
                 successful_operations, failed_operations, avg_response_time_ms, error_details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                "stability_test",
                test_start_time.isoformat(),
                test_duration,
                success_rate,
                test_results["total_operations"],
                test_results["successful_operations"],
                test_results["failed_operations"],
                avg_response_time,
                "; ".join(test_results["errors"][:10]) if test_results["errors"] else None,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"记录稳定性测试结果失败: {e}")
        
        logger.info(f"稳定性测试完成 - 成功率: {success_rate:.2%}, 平均响应时间: {avg_response_time:.1f}ms")
        
        return {
            "success_rate": success_rate,
            "avg_response_time_ms": avg_response_time,
            "total_operations": test_results["total_operations"],
            "successful_operations": test_results["successful_operations"],
            "failed_operations": test_results["failed_operations"],
            "error_count": len(test_results["errors"])
        }
    
    def run_comprehensive_verification(self) -> Dict[str, Any]:
        """运行全面验证"""
        logger.info("开始运行云数据导出系统全面验证")
        
        verification_summary = {
            "verification_time": datetime.now().isoformat(),
            "total_tables": len(self.critical_tables),
            "passed_tables": 0,
            "warning_tables": 0,
            "failed_tables": 0,
            "overall_status": "unknown",
            "table_results": [],
            "stability_test": None,
            "recommendations": []
        }
        
        # 并发验证所有关键表
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_table = {
                executor.submit(self.verify_single_table, table_name): table_name
                for table_name in self.critical_tables
            }
            
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    verification_summary["table_results"].append(result)
                    
                    if result["verification_status"] == "passed":
                        verification_summary["passed_tables"] += 1
                    elif result["verification_status"] == "warning":
                        verification_summary["warning_tables"] += 1
                    else:
                        verification_summary["failed_tables"] += 1
                
                except Exception as e:
                    logger.error(f"验证表 {table_name} 时发生异常: {e}")
                    verification_summary["failed_tables"] += 1
        
        # 运行稳定性测试（5分钟）
        logger.info("开始运行稳定性测试...")
        stability_result = self.run_stability_test(duration_minutes=5)
        verification_summary["stability_test"] = stability_result
        
        # 确定整体状态
        if verification_summary["failed_tables"] == 0 and stability_result["success_rate"] > 0.95:
            verification_summary["overall_status"] = "excellent"
        elif verification_summary["failed_tables"] <= 1 and stability_result["success_rate"] > 0.90:
            verification_summary["overall_status"] = "good"
        elif verification_summary["passed_tables"] > verification_summary["failed_tables"]:
            verification_summary["overall_status"] = "acceptable"
        else:
            verification_summary["overall_status"] = "poor"
        
        # 生成建议
        if verification_summary["failed_tables"] > 0:
            verification_summary["recommendations"].append("修复失败的表导出问题")
        
        if stability_result["success_rate"] < 0.95:
            verification_summary["recommendations"].append("提高系统稳定性，检查网络和服务配置")
        
        if verification_summary["warning_tables"] > 0:
            verification_summary["recommendations"].append("处理警告状态的表，优化数据质量")
        
        # 保存验证报告
        report_file = os.path.join(self.local_data_dir, "export_verification_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(verification_summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"全面验证完成 - 整体状态: {verification_summary['overall_status']}")
        return verification_summary

def main():
    """主函数"""
    verifier = CloudExportVerification()
    
    print("=" * 60)
    print("云数据导出系统验证")
    print("=" * 60)
    
    # 运行全面验证
    verification_result = verifier.run_comprehensive_verification()
    
    print(f"验证时间: {verification_result['verification_time']}")
    print(f"总表数: {verification_result['total_tables']}")
    print(f"通过: {verification_result['passed_tables']}")
    print(f"警告: {verification_result['warning_tables']}")
    print(f"失败: {verification_result['failed_tables']}")
    print(f"整体状态: {verification_result['overall_status']}")
    
    if verification_result["stability_test"]:
        stability = verification_result["stability_test"]
        print(f"\n稳定性测试:")
        print(f"  成功率: {stability['success_rate']:.2%}")
        print(f"  平均响应时间: {stability['avg_response_time_ms']:.1f}ms")
        print(f"  总操作数: {stability['total_operations']}")
        print(f"  失败操作数: {stability['failed_operations']}")
    
    if verification_result["recommendations"]:
        print(f"\n建议:")
        for i, rec in enumerate(verification_result["recommendations"], 1):
            print(f"  {i}. {rec}")
    
    print("\n详细结果:")
    for result in verification_result["table_results"]:
        status_emoji = "✅" if result["verification_status"] == "passed" else \
                      "⚠️" if result["verification_status"] == "warning" else "❌"
        
        print(f"  {status_emoji} {result['table_name']}: {result['verification_status']}")
        
        if result["issues"]:
            for issue in result["issues"]:
                print(f"    - {issue}")
    
    print("=" * 60)
    print("验证完成")
    print("=" * 60)
    
    return verification_result

if __name__ == "__main__":
    main()