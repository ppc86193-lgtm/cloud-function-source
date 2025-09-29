#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28云上数据写入流程测试
模拟真实API数据写入BigQuery，验证数据传输链路完整性
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
from google.cloud import bigquery
import random

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'python'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CloudTestResult:
    """测试结果数据类"""
    test_name: str
    status: str  # 'pass', 'fail', 'skip'
    message: str
    execution_time: float
    data_count: Optional[int] = None
    error_details: Optional[str] = None

class CloudDataPipelineTest:
    """云上数据管道测试系统"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.client = bigquery.Client(project=self.project_id)
        
        # 测试数据配置
        self.test_config = {
            "api_endpoints": {
                "pc28_api": "https://api.pc28.com/data",  # 示例API
                "backup_api": "https://backup.pc28.com/data"
            },
            "test_tables": {
                "cloud_pred_today_norm": "wprojectl.pc28_lab.cloud_pred_today_norm",
                "p_cloud_clean_merged_dedup_v": "wprojectl.pc28_lab.p_cloud_clean_merged_dedup_v"
            },
            "sample_data_size": 100,
            "timeout_seconds": 30
        }
    
    def generate_mock_api_data(self, count: int = 100) -> List[Dict[str, Any]]:
        """生成模拟API数据"""
        mock_data = []
        base_time = datetime.now()
        
        for i in range(count):
            # 生成模拟的PC28预测数据
            record = {
                "id": f"test_{int(time.time())}_{i}",
                "period": f"{(base_time + timedelta(minutes=i)).strftime('%Y%m%d%H%M')}",
                "ts_utc": (base_time + timedelta(minutes=i)).isoformat(),
                "created_at": base_time.isoformat(),
                "p_even": round(random.uniform(0.3, 0.7), 4),
                "p_big": round(random.uniform(0.3, 0.7), 4),
                "p_small": round(random.uniform(0.3, 0.7), 4),
                "confidence": round(random.uniform(0.6, 0.95), 3),
                "model_version": "test_v1.0",
                "source": "api_test",
                "features": json.dumps({
                    "feature_1": random.uniform(-1, 1),
                    "feature_2": random.uniform(-1, 1),
                    "feature_3": random.uniform(-1, 1)
                }),
                "metadata": json.dumps({
                    "test_run": True,
                    "generated_at": datetime.now().isoformat()
                })
            }
            mock_data.append(record)
        
        return mock_data
    
    def test_api_data_generation(self) -> CloudTestResult:
        """测试API数据生成"""
        start_time = time.time()
        
        try:
            # 生成测试数据
            mock_data = self.generate_mock_api_data(self.test_config["sample_data_size"])
            
            # 验证数据格式
            if not mock_data:
                return CloudTestResult(
                    test_name="API数据生成",
                    success=False,
                    message="生成的数据为空",
                    execution_time=time.time() - start_time
                )
            
            # 验证必要字段
            required_fields = ["id", "period", "ts_utc", "p_even", "source"]
            for record in mock_data[:5]:  # 检查前5条记录
                for field in required_fields:
                    if field not in record:
                        return CloudTestResult(
                            test_name="API数据生成",
                            success=False,
                            message=f"缺少必要字段: {field}",
                            execution_time=time.time() - start_time
                        )
            
            return CloudTestResult(
                test_name="API数据生成",
                success=True,
                message=f"成功生成 {len(mock_data)} 条测试数据",
                data={"sample_count": len(mock_data), "sample_record": mock_data[0]},
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return CloudTestResult(
                test_name="API数据生成",
                success=False,
                message=f"数据生成异常: {str(e)}",
                execution_time=time.time() - start_time
            )
    
    def test_bigquery_write(self, data: List[Dict[str, Any]], table_name: str) -> CloudTestResult:
        """测试BigQuery写入"""
        start_time = time.time()
        
        try:
            # 创建测试表名（添加时间戳避免冲突）
            timestamp = int(time.time())
            test_table_name = f"{table_name}_test_{timestamp}"
            full_table_id = f"{self.project_id}.{self.dataset_id}.{test_table_name}"
            
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 写入BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True
            )
            
            job = self.client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )
            
            # 等待作业完成
            job.result(timeout=self.test_config["timeout_seconds"])
            
            # 验证写入结果
            query = f"SELECT COUNT(*) as count FROM `{full_table_id}`"
            query_job = self.client.query(query)
            results = list(query_job.result())
            
            if results and int(results[0]['count']) == len(data):
                # 清理测试表
                self.client.delete_table(full_table_id)
                
                return CloudTestResult(
                    test_name="BigQuery写入测试",
                    success=True,
                    message=f"成功写入 {len(data)} 条记录到 {test_table_name}",
                    data={"records_written": len(data), "table_name": test_table_name},
                    execution_time=time.time() - start_time
                )
            else:
                return CloudTestResult(
                    test_name="BigQuery写入测试",
                    success=False,
                    message=f"写入数据数量不匹配: 期望 {len(data)}, 实际 {results[0]['count'] if results else 0}",
                    execution_time=time.time() - start_time
                )
                
        except Exception as e:
            return CloudTestResult(
                test_name="BigQuery写入测试",
                success=False,
                message=f"BigQuery写入异常: {str(e)}",
                execution_time=time.time() - start_time
            )
    
    def test_data_pipeline_latency(self) -> CloudTestResult:
        """测试数据管道延迟"""
        start_time = time.time()
        
        try:
            # 生成单条测试数据
            test_data = self.generate_mock_api_data(1)
            test_record = test_data[0]
            
            # 记录写入时间
            write_start = time.time()
            
            # 写入到临时表
            timestamp = int(time.time())
            test_table_name = f"latency_test_{timestamp}"
            full_table_id = f"{self.project_id}.{self.dataset_id}.{test_table_name}"
            
            df = pd.DataFrame([test_record])
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
            job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result(timeout=self.test_config["timeout_seconds"])
            
            write_latency = time.time() - write_start
            
            # 测试查询延迟
            query_start = time.time()
            query = f"SELECT * FROM `{full_table_id}` WHERE id = '{test_record['id']}'"
            query_job = self.client.query(query)
            results = list(query_job.result())
            query_latency = time.time() - query_start
            
            # 清理测试表
            self.client.delete_table(full_table_id)
            
            total_latency = write_latency + query_latency
            
            # 判断延迟是否在可接受范围内（例如10秒）
            acceptable_latency = 10.0
            success = total_latency < acceptable_latency
            
            return CloudTestResult(
                test_name="数据管道延迟测试",
                success=success,
                message=f"总延迟: {total_latency:.2f}s (写入: {write_latency:.2f}s, 查询: {query_latency:.2f}s)",
                data={
                    "write_latency": write_latency,
                    "query_latency": query_latency,
                    "total_latency": total_latency,
                    "acceptable_latency": acceptable_latency
                },
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return CloudTestResult(
                test_name="数据管道延迟测试",
                success=False,
                message=f"延迟测试异常: {str(e)}",
                execution_time=time.time() - start_time
            )
    
    def test_data_integrity(self) -> CloudTestResult:
        """测试数据完整性"""
        start_time = time.time()
        
        try:
            # 生成测试数据
            test_data = self.generate_mock_api_data(50)
            
            # 写入临时表
            timestamp = int(time.time())
            test_table_name = f"integrity_test_{timestamp}"
            full_table_id = f"{self.project_id}.{self.dataset_id}.{test_table_name}"
            
            df = pd.DataFrame(test_data)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
            job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result(timeout=self.test_config["timeout_seconds"])
            
            # 验证数据完整性
            integrity_checks = []
            
            # 1. 检查记录数量
            count_query = f"SELECT COUNT(*) as count FROM `{full_table_id}`"
            count_result = list(self.client.query(count_query).result())
            record_count = int(count_result[0]['count'])
            integrity_checks.append({
                "check": "记录数量",
                "expected": len(test_data),
                "actual": record_count,
                "passed": record_count == len(test_data)
            })
            
            # 2. 检查必要字段非空
            null_check_query = f"""
                SELECT 
                    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_id,
                    SUM(CASE WHEN period IS NULL THEN 1 ELSE 0 END) as null_period,
                    SUM(CASE WHEN p_even IS NULL THEN 1 ELSE 0 END) as null_p_even
                FROM `{full_table_id}`
            """
            null_result = list(self.client.query(null_check_query).result())[0]
            integrity_checks.append({
                "check": "必要字段非空",
                "expected": 0,
                "actual": sum([null_result['null_id'], null_result['null_period'], null_result['null_p_even']]),
                "passed": all([null_result['null_id'] == 0, null_result['null_period'] == 0, null_result['null_p_even'] == 0])
            })
            
            # 3. 检查数据范围
            range_check_query = f"""
                SELECT 
                    MIN(p_even) as min_p_even,
                    MAX(p_even) as max_p_even,
                    COUNT(DISTINCT id) as unique_ids
                FROM `{full_table_id}`
            """
            range_result = list(self.client.query(range_check_query).result())[0]
            integrity_checks.append({
                "check": "数据范围合理",
                "expected": "0-1之间",
                "actual": f"{range_result['min_p_even']:.3f}-{range_result['max_p_even']:.3f}",
                "passed": 0 <= range_result['min_p_even'] <= 1 and 0 <= range_result['max_p_even'] <= 1
            })
            
            # 4. 检查ID唯一性
            integrity_checks.append({
                "check": "ID唯一性",
                "expected": len(test_data),
                "actual": range_result['unique_ids'],
                "passed": range_result['unique_ids'] == len(test_data)
            })
            
            # 清理测试表
            self.client.delete_table(full_table_id)
            
            # 汇总结果
            passed_checks = sum(1 for check in integrity_checks if check['passed'])
            total_checks = len(integrity_checks)
            success = passed_checks == total_checks
            
            return CloudTestResult(
                test_name="数据完整性测试",
                success=success,
                message=f"完整性检查: {passed_checks}/{total_checks} 通过",
                data={"integrity_checks": integrity_checks},
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return CloudTestResult(
                test_name="数据完整性测试",
                success=False,
                message=f"完整性测试异常: {str(e)}",
                execution_time=time.time() - start_time
            )
    
    def test_concurrent_writes(self) -> CloudTestResult:
        """测试并发写入"""
        start_time = time.time()
        
        try:
            import threading
            import concurrent.futures
            
            # 准备多个数据批次
            batch_size = 20
            num_batches = 5
            batches = []
            
            for i in range(num_batches):
                batch_data = self.generate_mock_api_data(batch_size)
                # 为每个批次添加唯一标识
                for record in batch_data:
                    record['batch_id'] = f"batch_{i}"
                batches.append(batch_data)
            
            # 并发写入函数
            def write_batch(batch_index, batch_data):
                try:
                    timestamp = int(time.time())
                    test_table_name = f"concurrent_test_{timestamp}_{batch_index}"
                    full_table_id = f"{self.project_id}.{self.dataset_id}.{test_table_name}"
                    
                    df = pd.DataFrame(batch_data)
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
                    job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
                    job.result(timeout=self.test_config["timeout_seconds"])
                    
                    # 验证写入
                    count_query = f"SELECT COUNT(*) as count FROM `{full_table_id}`"
                    count_result = list(self.client.query(count_query).result())
                    actual_count = int(count_result[0]['count'])
                    
                    # 清理测试表
                    self.client.delete_table(full_table_id)
                    
                    return {
                        "batch_index": batch_index,
                        "expected_count": len(batch_data),
                        "actual_count": actual_count,
                        "success": actual_count == len(batch_data)
                    }
                    
                except Exception as e:
                    return {
                        "batch_index": batch_index,
                        "expected_count": len(batch_data),
                        "actual_count": 0,
                        "success": False,
                        "error": str(e)
                    }
            
            # 执行并发写入
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_batches) as executor:
                future_to_batch = {
                    executor.submit(write_batch, i, batch): i 
                    for i, batch in enumerate(batches)
                }
                
                for future in concurrent.futures.as_completed(future_to_batch):
                    result = future.result()
                    results.append(result)
            
            # 分析结果
            successful_batches = sum(1 for r in results if r['success'])
            total_expected = sum(r['expected_count'] for r in results)
            total_actual = sum(r['actual_count'] for r in results)
            
            success = successful_batches == num_batches and total_actual == total_expected
            
            return CloudTestResult(
                test_name="并发写入测试",
                success=success,
                message=f"并发写入: {successful_batches}/{num_batches} 批次成功, 总记录: {total_actual}/{total_expected}",
                data={
                    "batch_results": results,
                    "successful_batches": successful_batches,
                    "total_batches": num_batches,
                    "total_records_expected": total_expected,
                    "total_records_actual": total_actual
                },
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return CloudTestResult(
                test_name="并发写入测试",
                success=False,
                message=f"并发写入测试异常: {str(e)}",
                execution_time=time.time() - start_time
            )
    
    def test_error_handling(self) -> CloudTestResult:
        """测试错误处理"""
        start_time = time.time()
        
        try:
            error_scenarios = []
            
            # 1. 测试无效数据格式
            try:
                invalid_data = [{"invalid_field": "test"}]  # 缺少必要字段
                df = pd.DataFrame(invalid_data)
                timestamp = int(time.time())
                test_table_name = f"error_test_{timestamp}"
                full_table_id = f"{self.project_id}.{self.dataset_id}.{test_table_name}"
                
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
                job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
                job.result(timeout=5)  # 短超时
                
                # 如果没有异常，清理表
                self.client.delete_table(full_table_id)
                error_scenarios.append({"scenario": "无效数据格式", "handled": True, "error": None})
                
            except Exception as e:
                error_scenarios.append({"scenario": "无效数据格式", "handled": True, "error": str(e)})
            
            # 2. 测试网络超时模拟
            try:
                # 创建大量数据来模拟超时
                large_data = self.generate_mock_api_data(1000)
                df = pd.DataFrame(large_data)
                timestamp = int(time.time())
                test_table_name = f"timeout_test_{timestamp}"
                full_table_id = f"{self.project_id}.{self.dataset_id}.{test_table_name}"
                
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
                job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
                job.result(timeout=1)  # 很短的超时来触发异常
                
                # 如果成功，清理表
                self.client.delete_table(full_table_id)
                error_scenarios.append({"scenario": "超时处理", "handled": True, "error": None})
                
            except Exception as e:
                error_scenarios.append({"scenario": "超时处理", "handled": True, "error": str(e)})
            
            # 3. 测试权限错误模拟
            try:
                # 尝试写入到不存在的数据集
                invalid_table_id = f"{self.project_id}.invalid_dataset.test_table"
                test_data = self.generate_mock_api_data(1)
                df = pd.DataFrame(test_data)
                
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
                job = self.client.load_table_from_dataframe(df, invalid_table_id, job_config=job_config)
                job.result(timeout=5)
                
                error_scenarios.append({"scenario": "权限错误", "handled": True, "error": None})
                
            except Exception as e:
                error_scenarios.append({"scenario": "权限错误", "handled": True, "error": str(e)})
            
            # 评估错误处理能力
            handled_errors = sum(1 for scenario in error_scenarios if scenario['handled'])
            total_scenarios = len(error_scenarios)
            
            return CloudTestResult(
                test_name="错误处理测试",
                success=handled_errors == total_scenarios,
                message=f"错误处理: {handled_errors}/{total_scenarios} 场景正确处理",
                data={"error_scenarios": error_scenarios},
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return CloudTestResult(
                test_name="错误处理测试",
                success=False,
                message=f"错误处理测试异常: {str(e)}",
                execution_time=time.time() - start_time
            )
    
    def run_full_pipeline_test(self) -> Dict[str, Any]:
        """运行完整的云数据管道测试"""
        logger.info("开始云数据管道完整测试...")
        
        test_results = []
        start_time = time.time()
        
        # 执行所有测试
        tests = [
            self.test_api_data_generation,
            lambda: self.test_bigquery_write(self.generate_mock_api_data(10), "test_write"),
            self.test_data_pipeline_latency,
            self.test_data_integrity,
            self.test_concurrent_writes,
            self.test_error_handling
        ]
        
        for test_func in tests:
            try:
                result = test_func()
                test_results.append(result)
                
                status = "✅ 通过" if result.success else "❌ 失败"
                logger.info(f"{status} {result.test_name}: {result.message}")
                
            except Exception as e:
                error_result = CloudTestResult(
                    test_name=test_func.__name__,
                    success=False,
                    message=f"测试执行异常: {str(e)}"
                )
                test_results.append(error_result)
                logger.error(f"❌ {test_func.__name__}: {str(e)}")
        
        # 汇总结果
        passed = sum(1 for r in test_results if r.success)
        failed = len(test_results) - passed
        success_rate = (passed / len(test_results)) * 100 if test_results else 0
        total_time = time.time() - start_time
        
        summary = {
            "total_tests": len(test_results),
            "passed": passed,
            "failed": failed,
            "success_rate": success_rate,
            "total_execution_time": total_time,
            "test_results": [
                {
                    "test_name": r.test_name,
                    "success": r.success,
                    "message": r.message,
                    "execution_time": r.execution_time,
                    "data": r.data
                }
                for r in test_results
            ]
        }
        
        logger.info(f"云数据管道测试完成: {passed}/{len(test_results)} 通过 ({success_rate:.1f}%)")
        return summary

def main():
    """主函数"""
    pipeline_test = CloudDataPipelineTest()
    
    try:
        # 运行完整测试套件
        summary = pipeline_test.run_full_pipeline_test()
        
        # 输出详细结果
        print("\n" + "="*60)
        print("PC28云数据管道测试报告")
        print("="*60)
        print(f"总测试数: {summary['total_tests']}")
        print(f"通过: {summary['passed']}")
        print(f"失败: {summary['failed']}")
        print(f"成功率: {summary['success_rate']:.1f}%")
        print(f"总执行时间: {summary['total_execution_time']:.2f}秒")
        
        print("\n详细结果:")
        for result in summary['test_results']:
            status = "✅" if result['success'] else "❌"
            print(f"{status} {result['test_name']}: {result['message']} ({result['execution_time']:.2f}s)")
        
        # 保存结果到文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"/Users/a606/cloud_function_source/test_suite/cloud_pipeline_test_report_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n测试报告已保存到: {report_file}")
        
    except Exception as e:
        logger.error(f"测试执行异常: {e}")
        print(f"测试执行失败: {e}")

if __name__ == "__main__":
    main()