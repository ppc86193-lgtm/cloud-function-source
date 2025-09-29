#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统端到端数据流测试套件
测试从API数据采集到signal_pool生成的完整流程
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import unittest

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'python'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PC28TestResult:
    """测试结果数据类"""
    test_name: str
    status: str  # 'pass', 'fail', 'skip'
    message: str
    execution_time: float
    data_count: Optional[int] = None
    error_details: Optional[str] = None

class PC28DataFlowTester:
    """PC28数据流程测试器"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_lab = "pc28_lab"
        self.dataset_prod = "pc28"
        self.test_results = []
        
    def run_bq_query(self, query: str) -> Tuple[bool, Any]:
        """执行BigQuery查询"""
        try:
            cmd = [
                'bq', 'query', '--use_legacy_sql=false', 
                '--format=json', '--max_rows=1000', query
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                if result.stdout.strip():
                    return True, json.loads(result.stdout)
                else:
                    return True, []
            else:
                logger.error(f"BQ查询失败: {result.stderr}")
                return False, result.stderr
                
        except Exception as e:
            logger.error(f"执行BQ查询异常: {e}")
            return False, str(e)
    
    def test_raw_data_availability(self) -> PC28TestResult:
        """测试原始数据可用性"""
        start_time = time.time()
        test_name = "原始数据可用性测试"
        
        try:
            # 测试cloud_pred_today_norm表
            query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.cloud_pred_today_norm`"
            success, result = self.run_bq_query(query)
            
            if not success:
                return PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="无法查询cloud_pred_today_norm表",
                    execution_time=time.time() - start_time,
                    error_details=str(result)
                )
            
            count = int(result[0]['count']) if result else 0
            
            if count > 0:
                return PC28TestResult(
                    test_name=test_name,
                    status="pass",
                    message=f"cloud_pred_today_norm表有{count}行数据",
                    execution_time=time.time() - start_time,
                    data_count=count
                )
            else:
                return PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="cloud_pred_today_norm表无数据",
                    execution_time=time.time() - start_time,
                    data_count=0
                )
                
        except Exception as e:
            return PC28TestResult(
                test_name=test_name,
                status="fail",
                message="测试执行异常",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def test_prediction_views(self) -> List[PC28TestResult]:
        """测试预测视图层"""
        results = []
        views = [
            "p_cloud_today_v",
            "p_map_today_v", 
            "p_size_today_v"
        ]
        
        for view in views:
            start_time = time.time()
            test_name = f"预测视图测试: {view}"
            
            try:
                query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.{view}`"
                success, result = self.run_bq_query(query)
                
                if not success:
                    results.append(PC28TestResult(
                        test_name=test_name,
                        status="fail",
                        message=f"无法查询{view}视图",
                        execution_time=time.time() - start_time,
                        error_details=str(result)
                    ))
                    continue
                
                count = int(result[0]['count']) if result else 0
                
                if count > 0:
                    results.append(PC28TestResult(
                        test_name=test_name,
                        status="pass",
                        message=f"{view}视图有{count}行数据",
                        execution_time=time.time() - start_time,
                        data_count=count
                    ))
                else:
                    results.append(PC28TestResult(
                        test_name=test_name,
                        status="fail",
                        message=f"{view}视图无数据",
                        execution_time=time.time() - start_time,
                        data_count=0
                    ))
                    
            except Exception as e:
                results.append(PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="测试执行异常",
                    execution_time=time.time() - start_time,
                    error_details=str(e)
                ))
        
        return results
    
    def test_canonical_views(self) -> List[PC28TestResult]:
        """测试标准化视图层"""
        results = []
        views = [
            "p_map_today_canon_v",
            "p_size_today_canon_v"
        ]
        
        for view in views:
            start_time = time.time()
            test_name = f"标准化视图测试: {view}"
            
            try:
                query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.{view}`"
                success, result = self.run_bq_query(query)
                
                if not success:
                    results.append(PC28TestResult(
                        test_name=test_name,
                        status="fail",
                        message=f"无法查询{view}视图",
                        execution_time=time.time() - start_time,
                        error_details=str(result)
                    ))
                    continue
                
                count = int(result[0]['count']) if result else 0
                
                # 对于标准化视图，我们需要检查数据质量
                if count > 0:
                    # 进一步检查数据结构
                    sample_query = f"SELECT * FROM `{self.project_id}.{self.dataset_lab}.{view}` LIMIT 5"
                    sample_success, sample_result = self.run_bq_query(sample_query)
                    
                    if sample_success and sample_result:
                        results.append(PC28TestResult(
                            test_name=test_name,
                            status="pass",
                            message=f"{view}视图有{count}行数据，数据结构正常",
                            execution_time=time.time() - start_time,
                            data_count=count
                        ))
                    else:
                        results.append(PC28TestResult(
                            test_name=test_name,
                            status="fail",
                            message=f"{view}视图有数据但结构异常",
                            execution_time=time.time() - start_time,
                            data_count=count,
                            error_details="数据结构检查失败"
                        ))
                else:
                    results.append(PC28TestResult(
                        test_name=test_name,
                        status="fail",
                        message=f"{view}视图无数据",
                        execution_time=time.time() - start_time,
                        data_count=0
                    ))
                    
            except Exception as e:
                results.append(PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="测试执行异常",
                    execution_time=time.time() - start_time,
                    error_details=str(e)
                ))
        
        return results
    
    def test_signal_pool_generation(self) -> PC28TestResult:
        """测试信号池生成"""
        start_time = time.time()
        test_name = "信号池生成测试"
        
        try:
            # 测试signal_pool_union_v3
            query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`"
            success, result = self.run_bq_query(query)
            
            if not success:
                return PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="无法查询signal_pool_union_v3视图",
                    execution_time=time.time() - start_time,
                    error_details=str(result)
                )
            
            count = int(result[0]['count']) if result else 0
            
            if count > 0:
                return PC28TestResult(
                    test_name=test_name,
                    status="pass",
                    message=f"signal_pool_union_v3有{count}行数据",
                    execution_time=time.time() - start_time,
                    data_count=count
                )
            else:
                return PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="signal_pool_union_v3无数据，需要检查上游视图",
                    execution_time=time.time() - start_time,
                    data_count=0
                )
                
        except Exception as e:
            return PC28TestResult(
                test_name=test_name,
                status="fail",
                message="测试执行异常",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def test_decision_pipeline(self) -> PC28TestResult:
        """测试决策管道"""
        start_time = time.time()
        test_name = "决策管道测试"
        
        try:
            # 测试lab_push_candidates_v2
            query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`"
            success, result = self.run_bq_query(query)
            
            if not success:
                return PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="无法查询lab_push_candidates_v2视图",
                    execution_time=time.time() - start_time,
                    error_details=str(result)
                )
            
            count = int(result[0]['count']) if result else 0
            
            if count > 0:
                return PC28TestResult(
                    test_name=test_name,
                    status="pass",
                    message=f"lab_push_candidates_v2有{count}行候选决策",
                    execution_time=time.time() - start_time,
                    data_count=count
                )
            else:
                return PC28TestResult(
                    test_name=test_name,
                    status="fail",
                    message="lab_push_candidates_v2无候选决策",
                    execution_time=time.time() - start_time,
                    data_count=0
                )
                
        except Exception as e:
            return PC28TestResult(
                test_name=test_name,
                status="fail",
                message="测试执行异常",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def run_full_test_suite(self) -> Dict[str, Any]:
        """运行完整测试套件"""
        logger.info("开始运行PC28数据流程测试套件")
        start_time = time.time()
        
        # 清空之前的结果
        self.test_results = []
        
        # 1. 原始数据测试
        logger.info("测试原始数据可用性...")
        self.test_results.append(self.test_raw_data_availability())
        
        # 2. 预测视图测试
        logger.info("测试预测视图层...")
        self.test_results.extend(self.test_prediction_views())
        
        # 3. 标准化视图测试
        logger.info("测试标准化视图层...")
        self.test_results.extend(self.test_canonical_views())
        
        # 4. 信号池测试
        logger.info("测试信号池生成...")
        self.test_results.append(self.test_signal_pool_generation())
        
        # 5. 决策管道测试
        logger.info("测试决策管道...")
        self.test_results.append(self.test_decision_pipeline())
        
        # 统计结果
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r.status == 'pass'])
        failed_tests = len([r for r in self.test_results if r.status == 'fail'])
        skipped_tests = len([r for r in self.test_results if r.status == 'skip'])
        
        total_time = time.time() - start_time
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_tests': total_tests,
            'passed': passed_tests,
            'failed': failed_tests,
            'skipped': skipped_tests,
            'success_rate': passed_tests / total_tests if total_tests > 0 else 0,
            'total_execution_time': total_time,
            'test_results': [
                {
                    'test_name': r.test_name,
                    'status': r.status,
                    'message': r.message,
                    'execution_time': r.execution_time,
                    'data_count': r.data_count,
                    'error_details': r.error_details
                }
                for r in self.test_results
            ]
        }
        
        logger.info(f"测试完成: {passed_tests}/{total_tests} 通过, 耗时 {total_time:.2f}s")
        return summary
    
    def generate_test_report(self, summary: Dict[str, Any]) -> str:
        """生成测试报告"""
        report = []
        report.append("# PC28数据流程测试报告")
        report.append(f"**测试时间**: {summary['timestamp']}")
        report.append(f"**总测试数**: {summary['total_tests']}")
        report.append(f"**通过**: {summary['passed']}")
        report.append(f"**失败**: {summary['failed']}")
        report.append(f"**跳过**: {summary['skipped']}")
        report.append(f"**成功率**: {summary['success_rate']:.2%}")
        report.append(f"**总耗时**: {summary['total_execution_time']:.2f}秒")
        report.append("")
        
        report.append("## 详细测试结果")
        for result in summary['test_results']:
            status_emoji = "✅" if result['status'] == 'pass' else "❌" if result['status'] == 'fail' else "⏭️"
            report.append(f"### {status_emoji} {result['test_name']}")
            report.append(f"**状态**: {result['status']}")
            report.append(f"**消息**: {result['message']}")
            report.append(f"**耗时**: {result['execution_time']:.2f}秒")
            
            if result['data_count'] is not None:
                report.append(f"**数据行数**: {result['data_count']}")
            
            if result['error_details']:
                report.append(f"**错误详情**: {result['error_details']}")
            
            report.append("")
        
        return "\n".join(report)

def main():
    """主函数"""
    tester = PC28DataFlowTester()
    
    # 运行测试套件
    summary = tester.run_full_test_suite()
    
    # 生成报告
    report = tester.generate_test_report(summary)
    
    # 保存报告
    report_file = f"/Users/a606/cloud_function_source/test_suite/test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    # 保存JSON结果
    json_file = f"/Users/a606/cloud_function_source/test_suite/test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(report)
    print(f"\n报告已保存到: {report_file}")
    print(f"JSON结果已保存到: {json_file}")
    
    # 返回退出码
    return 0 if summary['failed'] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())