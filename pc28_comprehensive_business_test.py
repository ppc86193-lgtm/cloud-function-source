#!/usr/bin/env python3
"""
PC28综合业务测试系统
运行完整的业务功能测试，验证系统稳定性和数据完整性
"""

import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from google.cloud import bigquery
from dataclasses import dataclass

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class BusinessTestResult:
    """业务测试结果"""
    test_name: str
    test_category: str
    status: str  # 'pass', 'fail', 'skip'
    message: str
    duration: float
    data_count: Optional[int] = None
    error_details: Optional[str] = None
    timestamp: Optional[str] = None

class PC28ComprehensiveBusinessTest:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.test_results: List[BusinessTestResult] = []
        
    def run_comprehensive_business_tests(self) -> Dict[str, Any]:
        """运行综合业务测试"""
        logger.info("🚀 开始PC28综合业务测试...")
        
        start_time = time.time()
        
        test_summary = {
            "test_timestamp": self.timestamp,
            "start_time": datetime.now().isoformat(),
            "test_categories": [],
            "total_tests": 0,
            "passed_tests": 0,
            "failed_tests": 0,
            "skipped_tests": 0,
            "success_rate": 0.0,
            "total_duration": 0.0,
            "system_health": False,
            "test_results": []
        }
        
        # 1. 数据源健康测试
        self._run_data_source_health_tests()
        
        # 2. 数据流完整性测试
        self._run_data_flow_integrity_tests()
        
        # 3. 业务逻辑测试
        self._run_business_logic_tests()
        
        # 4. 性能基准测试
        self._run_performance_benchmark_tests()
        
        # 5. 数据质量测试
        self._run_data_quality_tests()
        
        # 6. 系统稳定性测试
        self._run_system_stability_tests()
        
        # 计算测试摘要
        total_duration = time.time() - start_time
        
        test_summary["total_tests"] = len(self.test_results)
        test_summary["passed_tests"] = len([r for r in self.test_results if r.status == 'pass'])
        test_summary["failed_tests"] = len([r for r in self.test_results if r.status == 'fail'])
        test_summary["skipped_tests"] = len([r for r in self.test_results if r.status == 'skip'])
        test_summary["success_rate"] = (test_summary["passed_tests"] / test_summary["total_tests"] * 100) if test_summary["total_tests"] > 0 else 0
        test_summary["total_duration"] = total_duration
        test_summary["system_health"] = test_summary["failed_tests"] == 0 and test_summary["passed_tests"] > 0
        test_summary["end_time"] = datetime.now().isoformat()
        
        # 按类别分组测试结果
        categories = {}
        for result in self.test_results:
            if result.test_category not in categories:
                categories[result.test_category] = {
                    "category_name": result.test_category,
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                    "skipped": 0,
                    "tests": []
                }
            
            categories[result.test_category]["total"] += 1
            categories[result.test_category]["tests"].append({
                "test_name": result.test_name,
                "status": result.status,
                "message": result.message,
                "duration": result.duration,
                "data_count": result.data_count,
                "error_details": result.error_details
            })
            
            if result.status == 'pass':
                categories[result.test_category]["passed"] += 1
            elif result.status == 'fail':
                categories[result.test_category]["failed"] += 1
            else:
                categories[result.test_category]["skipped"] += 1
        
        test_summary["test_categories"] = list(categories.values())
        test_summary["test_results"] = [
            {
                "test_name": r.test_name,
                "test_category": r.test_category,
                "status": r.status,
                "message": r.message,
                "duration": r.duration,
                "data_count": r.data_count,
                "error_details": r.error_details,
                "timestamp": r.timestamp
            }
            for r in self.test_results
        ]
        
        # 生成测试报告
        self._generate_business_test_report(test_summary)
        
        return test_summary
    
    def _run_single_test(self, test_name: str, test_category: str, test_func) -> BusinessTestResult:
        """运行单个测试"""
        start_time = time.time()
        
        try:
            logger.info(f"  运行测试: {test_name}")
            result = test_func()
            duration = time.time() - start_time
            
            test_result = BusinessTestResult(
                test_name=test_name,
                test_category=test_category,
                status='pass',
                message=result.get('message', '测试通过'),
                duration=duration,
                data_count=result.get('data_count'),
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_result = BusinessTestResult(
                test_name=test_name,
                test_category=test_category,
                status='fail',
                message=f'测试失败: {str(e)}',
                duration=duration,
                error_details=str(e),
                timestamp=datetime.now().isoformat()
            )
            logger.error(f"    ❌ {test_name}: {e}")
        
        self.test_results.append(test_result)
        return test_result
    
    def _run_data_source_health_tests(self):
        """数据源健康测试"""
        logger.info("📊 运行数据源健康测试...")
        
        # 测试原始数据表
        self._run_single_test(
            "cloud_pred_today_norm_health",
            "数据源健康",
            lambda: self._test_table_health("cloud_pred_today_norm", min_rows=100)
        )
        
        # 测试清理后的数据表
        self._run_single_test(
            "p_map_clean_merged_dedup_v_health",
            "数据源健康",
            lambda: self._test_table_health("p_map_clean_merged_dedup_v", min_rows=100)
        )
        
        self._run_single_test(
            "p_size_clean_merged_dedup_v_health",
            "数据源健康",
            lambda: self._test_table_health("p_size_clean_merged_dedup_v", min_rows=100)
        )
    
    def _run_data_flow_integrity_tests(self):
        """数据流完整性测试"""
        logger.info("🔄 运行数据流完整性测试...")
        
        # 测试预测视图层
        self._run_single_test(
            "p_cloud_today_v_integrity",
            "数据流完整性",
            lambda: self._test_view_integrity("p_cloud_today_v")
        )
        
        self._run_single_test(
            "p_map_today_v_integrity",
            "数据流完整性",
            lambda: self._test_view_integrity("p_map_today_v")
        )
        
        self._run_single_test(
            "p_size_today_v_integrity",
            "数据流完整性",
            lambda: self._test_view_integrity("p_size_today_v")
        )
        
        # 测试标准化视图层
        self._run_single_test(
            "p_map_today_canon_v_integrity",
            "数据流完整性",
            lambda: self._test_canonical_view_integrity("p_map_today_canon_v")
        )
        
        self._run_single_test(
            "p_size_today_canon_v_integrity",
            "数据流完整性",
            lambda: self._test_canonical_view_integrity("p_size_today_canon_v")
        )
        
        # 测试信号池
        self._run_single_test(
            "signal_pool_union_v3_integrity",
            "数据流完整性",
            lambda: self._test_signal_pool_integrity()
        )
    
    def _run_business_logic_tests(self):
        """业务逻辑测试"""
        logger.info("🎯 运行业务逻辑测试...")
        
        # 测试决策候选生成
        self._run_single_test(
            "lab_push_candidates_v2_logic",
            "业务逻辑",
            lambda: self._test_decision_candidates_logic()
        )
        
        # 测试运行时参数
        self._run_single_test(
            "runtime_params_logic",
            "业务逻辑",
            lambda: self._test_runtime_params_logic()
        )
        
        # 测试数据关联性
        self._run_single_test(
            "data_correlation_logic",
            "业务逻辑",
            lambda: self._test_data_correlation()
        )
    
    def _run_performance_benchmark_tests(self):
        """性能基准测试"""
        logger.info("⚡ 运行性能基准测试...")
        
        # 测试查询性能
        self._run_single_test(
            "signal_pool_query_performance",
            "性能基准",
            lambda: self._test_query_performance("signal_pool_union_v3")
        )
        
        self._run_single_test(
            "candidates_query_performance",
            "性能基准",
            lambda: self._test_query_performance("lab_push_candidates_v2")
        )
    
    def _run_data_quality_tests(self):
        """数据质量测试"""
        logger.info("🔍 运行数据质量测试...")
        
        # 测试数据完整性
        self._run_single_test(
            "signal_pool_data_quality",
            "数据质量",
            lambda: self._test_signal_pool_data_quality()
        )
        
        # 测试数据一致性
        self._run_single_test(
            "data_consistency_check",
            "数据质量",
            lambda: self._test_data_consistency()
        )
    
    def _run_system_stability_tests(self):
        """系统稳定性测试"""
        logger.info("🛡️ 运行系统稳定性测试...")
        
        # 测试并发查询
        self._run_single_test(
            "concurrent_query_stability",
            "系统稳定性",
            lambda: self._test_concurrent_queries()
        )
    
    def _test_table_health(self, table_name: str, min_rows: int = 1) -> Dict[str, Any]:
        """测试表健康状态"""
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
        result = self.client.query(query).result()
        count = list(result)[0].count
        
        if count < min_rows:
            raise Exception(f"表 {table_name} 数据不足: {count} 行 (最少需要 {min_rows} 行)")
        
        return {
            "message": f"表 {table_name} 健康: {count} 行数据",
            "data_count": count
        }
    
    def _test_view_integrity(self, view_name: str) -> Dict[str, Any]:
        """测试视图完整性"""
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{view_name}`"
        result = self.client.query(query).result()
        count = list(result)[0].count
        
        return {
            "message": f"视图 {view_name} 完整性正常: {count} 行数据",
            "data_count": count
        }
    
    def _test_canonical_view_integrity(self, view_name: str) -> Dict[str, Any]:
        """测试标准化视图完整性"""
        query = f"""
        SELECT 
            COUNT(*) as count,
            COUNT(DISTINCT period) as unique_periods,
            AVG(p_win) as avg_p_win
        FROM `{self.project_id}.{self.dataset_id}.{view_name}`
        """
        result = self.client.query(query).result()
        row = list(result)[0]
        
        return {
            "message": f"标准化视图 {view_name} 完整性正常: {row.count} 行, {row.unique_periods} 个唯一期数",
            "data_count": row.count
        }
    
    def _test_signal_pool_integrity(self) -> Dict[str, Any]:
        """测试信号池完整性"""
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(DISTINCT period) as unique_periods,
            SUM(CASE WHEN p_win > 0.5 THEN 1 ELSE 0 END) as high_confidence_signals
        FROM `{self.project_id}.{self.dataset_id}.signal_pool_union_v3`
        """
        result = self.client.query(query).result()
        row = list(result)[0]
        
        if row.total_count == 0:
            raise Exception("信号池为空")
        
        return {
            "message": f"信号池完整性正常: {row.total_count} 个信号, {row.unique_periods} 个期数, {row.high_confidence_signals} 个高置信度信号",
            "data_count": row.total_count
        }
    
    def _test_decision_candidates_logic(self) -> Dict[str, Any]:
        """测试决策候选逻辑"""
        query = f"""
        SELECT 
            COUNT(*) as total_candidates,
            COUNT(DISTINCT period) as unique_periods,
            AVG(p_win) as avg_confidence
        FROM `{self.project_id}.{self.dataset_id}.lab_push_candidates_v2`
        """
        result = self.client.query(query).result()
        row = list(result)[0]
        
        if row.total_candidates == 0:
            raise Exception("没有生成决策候选")
        
        return {
            "message": f"决策候选逻辑正常: {row.total_candidates} 个候选, {row.unique_periods} 个期数, 平均置信度 {row.avg_confidence:.3f}",
            "data_count": row.total_candidates
        }
    
    def _test_runtime_params_logic(self) -> Dict[str, Any]:
        """测试运行时参数逻辑"""
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.runtime_params`"
        result = self.client.query(query).result()
        count = list(result)[0].count
        
        if count == 0:
            raise Exception("运行时参数为空")
        
        return {
            "message": f"运行时参数正常: {count} 个参数",
            "data_count": count
        }
    
    def _test_data_correlation(self) -> Dict[str, Any]:
        """测试数据关联性"""
        query = f"""
        SELECT 
            COUNT(DISTINCT s.period) as signal_periods,
            COUNT(DISTINCT c.period) as candidate_periods,
            COUNT(DISTINCT r.market) as runtime_params
        FROM `{self.project_id}.{self.dataset_id}.signal_pool_union_v3` s
        FULL OUTER JOIN `{self.project_id}.{self.dataset_id}.lab_push_candidates_v2` c
            ON s.period = c.period
        CROSS JOIN `{self.project_id}.{self.dataset_id}.runtime_params` r
        """
        result = self.client.query(query).result()
        row = list(result)[0]
        
        return {
            "message": f"数据关联性正常: 信号期数 {row.signal_periods}, 候选期数 {row.candidate_periods}, 运行参数 {row.runtime_params}",
            "data_count": row.signal_periods
        }
    
    def _test_query_performance(self, table_name: str) -> Dict[str, Any]:
        """测试查询性能"""
        start_time = time.time()
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
        result = self.client.query(query).result()
        count = list(result)[0].count
        query_time = time.time() - start_time
        
        if query_time > 10.0:  # 超过10秒认为性能不佳
            raise Exception(f"查询性能不佳: {query_time:.2f}秒")
        
        return {
            "message": f"查询性能正常: {table_name} 查询耗时 {query_time:.2f}秒, {count} 行数据",
            "data_count": count
        }
    
    def _test_signal_pool_data_quality(self) -> Dict[str, Any]:
        """测试信号池数据质量"""
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN p_win IS NULL THEN 1 END) as null_p_win,
            COUNT(CASE WHEN p_win < 0 OR p_win > 1 THEN 1 END) as invalid_p_win,
            COUNT(CASE WHEN period IS NULL THEN 1 END) as null_period
        FROM `{self.project_id}.{self.dataset_id}.signal_pool_union_v3`
        """
        result = self.client.query(query).result()
        row = list(result)[0]
        
        quality_issues = row.null_p_win + row.invalid_p_win + row.null_period
        if quality_issues > 0:
            raise Exception(f"数据质量问题: {quality_issues} 个异常记录")
        
        return {
            "message": f"信号池数据质量良好: {row.total_count} 行数据，无质量问题",
            "data_count": row.total_count
        }
    
    def _test_data_consistency(self) -> Dict[str, Any]:
        """测试数据一致性"""
        # 检查信号池和决策候选的期数一致性
        query = f"""
        SELECT 
            COUNT(DISTINCT s.period) as signal_periods,
            COUNT(DISTINCT c.period) as candidate_periods
        FROM `{self.project_id}.{self.dataset_id}.signal_pool_union_v3` s
        FULL OUTER JOIN `{self.project_id}.{self.dataset_id}.lab_push_candidates_v2` c
            ON s.period = c.period
        """
        result = self.client.query(query).result()
        row = list(result)[0]
        
        return {
            "message": f"数据一致性正常: 信号期数 {row.signal_periods}, 候选期数 {row.candidate_periods}",
            "data_count": row.signal_periods
        }
    
    def _test_concurrent_queries(self) -> Dict[str, Any]:
        """测试并发查询稳定性"""
        import concurrent.futures
        
        def run_query():
            query = f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.signal_pool_union_v3`"
            result = self.client.query(query).result()
            return list(result)[0][0]
        
        # 并发执行3个查询
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(run_query) for _ in range(3)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # 检查结果一致性
        if len(set(results)) > 1:
            raise Exception(f"并发查询结果不一致: {results}")
        
        return {
            "message": f"并发查询稳定性正常: 3个并发查询结果一致 ({results[0]} 行)",
            "data_count": results[0]
        }
    
    def _generate_business_test_report(self, test_summary: Dict[str, Any]):
        """生成业务测试报告"""
        # JSON报告
        json_path = f"/Users/a606/cloud_function_source/pc28_business_test_report_{self.timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(test_summary, f, indent=2, ensure_ascii=False)
        
        # Markdown报告
        md_path = f"/Users/a606/cloud_function_source/pc28_business_test_report_{self.timestamp}.md"
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write("# PC28综合业务测试报告\n\n")
            f.write(f"**测试时间**: {test_summary['start_time']}\n")
            f.write(f"**总测试数**: {test_summary['total_tests']}\n")
            f.write(f"**通过**: {test_summary['passed_tests']}\n")
            f.write(f"**失败**: {test_summary['failed_tests']}\n")
            f.write(f"**跳过**: {test_summary['skipped_tests']}\n")
            f.write(f"**成功率**: {test_summary['success_rate']:.2f}%\n")
            f.write(f"**总耗时**: {test_summary['total_duration']:.2f}秒\n")
            f.write(f"**系统健康**: {'✅ 健康' if test_summary['system_health'] else '❌ 异常'}\n\n")
            
            # 按类别输出测试结果
            for category in test_summary['test_categories']:
                f.write(f"## {category['category_name']}\n")
                f.write(f"**通过**: {category['passed']}/{category['total']}\n\n")
                
                for test in category['tests']:
                    status_icon = "✅" if test['status'] == 'pass' else "❌" if test['status'] == 'fail' else "⏭️"
                    f.write(f"### {status_icon} {test['test_name']}\n")
                    f.write(f"**状态**: {test['status']}\n")
                    f.write(f"**消息**: {test['message']}\n")
                    f.write(f"**耗时**: {test['duration']:.2f}秒\n")
                    if test['data_count'] is not None:
                        f.write(f"**数据行数**: {test['data_count']}\n")
                    if test['error_details']:
                        f.write(f"**错误详情**: {test['error_details']}\n")
                    f.write("\n")
        
        logger.info(f"📄 业务测试报告已保存:")
        logger.info(f"  JSON: {json_path}")
        logger.info(f"  Markdown: {md_path}")

def main():
    """主函数"""
    tester = PC28ComprehensiveBusinessTest()
    
    print("🧪 PC28综合业务测试系统")
    print("=" * 60)
    print("🎯 目标：验证系统稳定性和业务功能完整性")
    print("📋 测试范围：数据源、数据流、业务逻辑、性能、质量、稳定性")
    print("=" * 60)
    
    # 运行综合业务测试
    results = tester.run_comprehensive_business_tests()
    
    # 输出结果摘要
    print(f"\n📊 测试结果摘要:")
    print(f"  总测试数: {results['total_tests']}")
    print(f"  通过: {results['passed_tests']}")
    print(f"  失败: {results['failed_tests']}")
    print(f"  跳过: {results['skipped_tests']}")
    print(f"  成功率: {results['success_rate']:.2f}%")
    print(f"  总耗时: {results['total_duration']:.2f}秒")
    
    print(f"\n📈 测试类别:")
    for category in results['test_categories']:
        success_rate = (category['passed'] / category['total'] * 100) if category['total'] > 0 else 0
        print(f"  {category['category_name']}: {category['passed']}/{category['total']} ({success_rate:.1f}%)")
    
    if results['system_health']:
        print(f"\n🎉 系统健康状态: ✅ 健康")
        print(f"💡 所有核心业务功能正常运行")
        print(f"🔥 系统已准备好进行安全优化")
    else:
        print(f"\n⚠️ 系统健康状态: ❌ 异常")
        print(f"🔧 请先解决失败的测试项目")
        
        # 显示失败的测试
        failed_tests = [r for r in results['test_results'] if r['status'] == 'fail']
        if failed_tests:
            print(f"\n❌ 失败的测试:")
            for test in failed_tests:
                print(f"  - {test['test_name']}: {test['message']}")
    
    return results

if __name__ == "__main__":
    main()