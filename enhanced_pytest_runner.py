#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统增强版pytest测试运行器
提供完整的测试执行、结果捕获、分析和报告功能
"""

import os
import sys
import json
import time
import logging
import subprocess
import pytest
import coverage
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict
from collections import defaultdict
import traceback
import argparse
import threading
import queue
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TestStepResult:
    """测试步骤结果"""
    step_name: str
    status: str  # setup, call, teardown
    duration: float
    outcome: str  # passed, failed, error, skipped
    error_message: Optional[str] = None
    timestamp: Optional[datetime] = None

@dataclass
class DetailedTestResult:
    """详细测试结果"""
    test_id: str
    test_name: str
    test_file: str
    test_class: Optional[str]
    test_method: str
    final_status: str  # passed, failed, error, skipped, xfail, xpass
    total_duration: float
    steps: List[TestStepResult]
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    test_type: str = "unit"
    markers: List[str] = None
    parameters: Optional[Dict] = None
    
    def __post_init__(self):
        if self.markers is None:
            self.markers = []
        if self.steps is None:
            self.steps = []

@dataclass
class TestSuiteAnalysis:
    """测试套件分析结果"""
    suite_name: str
    test_file: str
    test_type: str
    tests: List[DetailedTestResult]
    total_duration: float
    setup_duration: float
    teardown_duration: float
    passed_count: int
    failed_count: int
    error_count: int
    skipped_count: int
    xfail_count: int
    xpass_count: int
    success_rate: float
    performance_metrics: Dict[str, Any]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    coverage_data: Optional[Dict] = None

@dataclass
class ComprehensiveTestReport:
    """综合测试报告"""
    report_id: str
    generation_time: datetime
    execution_summary: Dict[str, Any]
    test_suites: List[TestSuiteAnalysis]
    coverage_analysis: Optional[Dict] = None
    performance_analysis: Optional[Dict] = None
    failure_analysis: Optional[Dict] = None
    environment_info: Optional[Dict] = None
    recommendations: Optional[List[str]] = None

class EnhancedPytestCollector:
    """增强版pytest结果收集器"""
    
    def __init__(self):
        self.results = []
        self.current_test = None
        self.session_start = None
        self.session_end = None
        self.step_timings = {}
        
    def pytest_runtest_setup(self, item):
        """测试设置阶段"""
        test_info = self._extract_test_info(item)
        test_info['setup_start'] = datetime.now()
        item.test_info = test_info
        self.current_test = test_info
        logger.debug(f"开始设置测试: {item.nodeid}")
        
    def pytest_runtest_call(self, item):
        """测试执行阶段"""
        if hasattr(item, 'test_info'):
            item.test_info['call_start'] = datetime.now()
            logger.debug(f"开始执行测试: {item.nodeid}")
            
    def pytest_runtest_teardown(self, item):
        """测试清理阶段"""
        if hasattr(item, 'test_info'):
            item.test_info['teardown_start'] = datetime.now()
            logger.debug(f"开始清理测试: {item.nodeid}")
            
    def pytest_runtest_logreport(self, report):
        """收集测试报告"""
        if report.when == 'call':
            test_result = self._create_detailed_test_result(report)
            self.results.append(test_result)
            logger.info(f"测试完成: {report.nodeid} - {report.outcome}")
        elif report.when in ['setup', 'teardown']:
            # 记录步骤信息
            self._record_step_result(report)
            
    def pytest_sessionstart(self, session):
        """测试会话开始"""
        self.session_start = datetime.now()
        logger.info(f"Pytest会话开始: {self.session_start}")
        
    def pytest_sessionfinish(self, session, exitstatus):
        """测试会话结束"""
        self.session_end = datetime.now()
        duration = (self.session_end - self.session_start).total_seconds()
        logger.info(f"Pytest会话结束: {self.session_end}, 耗时: {duration:.2f}秒, 退出状态: {exitstatus}")
        
    def _extract_test_info(self, item):
        """提取测试信息"""
        # 提取参数化信息
        parameters = {}
        if hasattr(item, 'callspec') and item.callspec:
            parameters = dict(item.callspec.params)
            
        return {
            'test_id': item.nodeid,
            'test_name': item.name,
            'test_file': str(item.fspath),
            'test_class': item.cls.__name__ if item.cls else None,
            'test_method': item.function.__name__,
            'markers': [marker.name for marker in item.iter_markers()],
            'test_type': self._determine_test_type(item),
            'parameters': parameters
        }
        
    def _determine_test_type(self, item):
        """确定测试类型"""
        markers = [marker.name for marker in item.iter_markers()]
        if 'integration' in markers:
            return 'integration'
        elif 'performance' in markers:
            return 'performance'
        elif 'e2e' in markers:
            return 'e2e'
        elif 'data' in markers:
            return 'data'
        elif any(m in markers for m in ['lottery', 'betting', 'payout', 'risk']):
            return 'business_logic'
        else:
            return 'unit'
            
    def _record_step_result(self, report):
        """记录步骤结果"""
        if not hasattr(report.item, 'test_info'):
            return
            
        step_name = report.when
        test_id = report.item.test_info['test_id']
        
        if test_id not in self.step_timings:
            self.step_timings[test_id] = []
            
        step_result = TestStepResult(
            step_name=step_name,
            status=step_name,
            duration=report.duration,
            outcome=report.outcome,
            error_message=str(report.longrepr) if report.failed else None,
            timestamp=datetime.now()
        )
        
        self.step_timings[test_id].append(step_result)
        
    def _create_detailed_test_result(self, report):
        """创建详细测试结果"""
        item = report.item
        test_info = getattr(item, 'test_info', {})
        
        # 获取步骤信息
        test_id = test_info.get('test_id', item.nodeid)
        steps = self.step_timings.get(test_id, [])
        
        # 计算各阶段时间
        setup_duration = 0.0
        call_duration = report.duration
        teardown_duration = 0.0
        
        for step in steps:
            if step.step_name == 'setup':
                setup_duration = step.duration
            elif step.step_name == 'teardown':
                teardown_duration = step.duration
                
        total_duration = setup_duration + call_duration + teardown_duration
        
        # 处理错误信息
        error_message = None
        error_traceback = None
        if report.failed or report.outcome == 'error':
            if hasattr(report, 'longrepr') and report.longrepr:
                error_lines = str(report.longrepr).split('\n')
                error_message = error_lines[-1] if error_lines else None
                error_traceback = str(report.longrepr)
                
        return DetailedTestResult(
            test_id=test_id,
            test_name=test_info.get('test_name', item.name),
            test_file=test_info.get('test_file', str(item.fspath)),
            test_class=test_info.get('test_class'),
            test_method=test_info.get('test_method', item.function.__name__),
            final_status=report.outcome,
            total_duration=total_duration,
            steps=steps,
            error_message=error_message,
            error_traceback=error_traceback,
            start_time=test_info.get('setup_start'),
            end_time=datetime.now(),
            test_type=test_info.get('test_type', 'unit'),
            markers=test_info.get('markers', []),
            parameters=test_info.get('parameters')
        )

class TestAnalyzer:
    """测试分析器"""
    
    def __init__(self):
        self.failure_patterns = {}
        self.performance_thresholds = {
            'slow_test': 5.0,  # 超过5秒的测试
            'very_slow_test': 10.0  # 超过10秒的测试
        }
        
    def analyze_test_results(self, results: List[DetailedTestResult]) -> Dict[str, Any]:
        """分析测试结果"""
        
        analysis = {
            'summary': self._analyze_summary(results),
            'performance': self._analyze_performance(results),
            'failures': self._analyze_failures(results),
            'coverage': self._analyze_coverage(results),
            'trends': self._analyze_trends(results),
            'recommendations': self._generate_recommendations(results)
        }
        
        return analysis
        
    def _analyze_summary(self, results: List[DetailedTestResult]) -> Dict[str, Any]:
        """分析测试摘要"""
        total_tests = len(results)
        if total_tests == 0:
            return {'total': 0, 'passed': 0, 'failed': 0, 'error': 0, 'skipped': 0, 'success_rate': 0}
            
        passed = sum(1 for r in results if r.final_status == 'passed')
        failed = sum(1 for r in results if r.final_status == 'failed')
        error = sum(1 for r in results if r.final_status == 'error')
        skipped = sum(1 for r in results if r.final_status == 'skipped')
        
        success_rate = (passed / total_tests) * 100
        
        return {
            'total': total_tests,
            'passed': passed,
            'failed': failed,
            'error': error,
            'skipped': skipped,
            'success_rate': success_rate,
            'total_duration': sum(r.total_duration for r in results)
        }
        
    def _analyze_performance(self, results: List[DetailedTestResult]) -> Dict[str, Any]:
        """分析性能指标"""
        if not results:
            return {}
            
        durations = [r.total_duration for r in results]
        
        # 统计信息
        avg_duration = sum(durations) / len(durations)
        max_duration = max(durations)
        min_duration = min(durations)
        
        # 慢测试识别
        slow_tests = [r for r in results if r.total_duration > self.performance_thresholds['slow_test']]
        very_slow_tests = [r for r in results if r.total_duration > self.performance_thresholds['very_slow_test']]
        
        # 按类型分组性能
        type_performance = defaultdict(list)
        for result in results:
            type_performance[result.test_type].append(result.total_duration)
            
        type_avg = {
            test_type: sum(durations) / len(durations)
            for test_type, durations in type_performance.items()
        }
        
        return {
            'average_duration': avg_duration,
            'max_duration': max_duration,
            'min_duration': min_duration,
            'slow_tests_count': len(slow_tests),
            'very_slow_tests_count': len(very_slow_tests),
            'slowest_tests': sorted(results, key=lambda x: x.total_duration, reverse=True)[:10],
            'fastest_tests': sorted(results, key=lambda x: x.total_duration)[:10],
            'performance_by_type': type_avg
        }
        
    def _analyze_failures(self, results: List[DetailedTestResult]) -> Dict[str, Any]:
        """分析失败模式"""
        failed_results = [r for r in results if r.final_status in ['failed', 'error']]
        
        if not failed_results:
            return {'total_failures': 0, 'patterns': {}, 'common_errors': []}
            
        # 错误模式分析
        error_patterns = defaultdict(list)
        for result in failed_results:
            if result.error_message:
                # 简化错误消息以识别模式
                simplified_error = self._simplify_error_message(result.error_message)
                error_patterns[simplified_error].append(result)
                
        # 按文件分组失败
        failures_by_file = defaultdict(list)
        for result in failed_results:
            failures_by_file[result.test_file].append(result)
            
        # 按类型分组失败
        failures_by_type = defaultdict(list)
        for result in failed_results:
            failures_by_type[result.test_type].append(result)
            
        return {
            'total_failures': len(failed_results),
            'error_patterns': dict(error_patterns),
            'failures_by_file': dict(failures_by_file),
            'failures_by_type': dict(failures_by_type),
            'common_errors': self._identify_common_errors(failed_results)
        }
        
    def _analyze_coverage(self, results: List[DetailedTestResult]) -> Dict[str, Any]:
        """分析测试覆盖率"""
        # 这里可以集成覆盖率数据分析
        return {
            'files_tested': len(set(r.test_file for r in results)),
            'test_types_coverage': {
                test_type: len([r for r in results if r.test_type == test_type])
                for test_type in set(r.test_type for r in results)
            }
        }
        
    def _analyze_trends(self, results: List[DetailedTestResult]) -> Dict[str, Any]:
        """分析测试趋势"""
        # 按时间分析测试执行趋势
        if not results:
            return {}
            
        # 按小时分组
        hourly_stats = defaultdict(lambda: {'count': 0, 'passed': 0, 'failed': 0})
        
        for result in results:
            if result.start_time:
                hour = result.start_time.hour
                hourly_stats[hour]['count'] += 1
                if result.final_status == 'passed':
                    hourly_stats[hour]['passed'] += 1
                elif result.final_status in ['failed', 'error']:
                    hourly_stats[hour]['failed'] += 1
                    
        return {
            'hourly_distribution': dict(hourly_stats),
            'peak_testing_hour': max(hourly_stats.keys(), key=lambda h: hourly_stats[h]['count']) if hourly_stats else None
        }
        
    def _generate_recommendations(self, results: List[DetailedTestResult]) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        # 性能建议
        slow_tests = [r for r in results if r.total_duration > self.performance_thresholds['slow_test']]
        if slow_tests:
            recommendations.append(f"发现 {len(slow_tests)} 个慢测试，建议优化测试性能")
            
        # 失败率建议
        failed_tests = [r for r in results if r.final_status in ['failed', 'error']]
        if failed_tests:
            failure_rate = len(failed_tests) / len(results) * 100
            if failure_rate > 10:
                recommendations.append(f"测试失败率较高 ({failure_rate:.1f}%)，建议检查测试质量")
                
        # 覆盖率建议
        test_types = set(r.test_type for r in results)
        if 'integration' not in test_types:
            recommendations.append("缺少集成测试，建议添加集成测试用例")
            
        if 'performance' not in test_types:
            recommendations.append("缺少性能测试，建议添加性能测试用例")
            
        return recommendations
        
    def _simplify_error_message(self, error_message: str) -> str:
        """简化错误消息以识别模式"""
        # 移除具体的值和路径，保留错误类型
        simplified = re.sub(r'\d+', 'N', error_message)
        simplified = re.sub(r"'[^']*'", "'VALUE'", simplified)
        simplified = re.sub(r'"[^"]*"', '"VALUE"', simplified)
        simplified = re.sub(r'/[^\s]*', '/PATH', simplified)
        return simplified[:100]  # 限制长度
        
    def _identify_common_errors(self, failed_results: List[DetailedTestResult]) -> List[Dict[str, Any]]:
        """识别常见错误"""
        error_counts = defaultdict(int)
        error_examples = {}
        
        for result in failed_results:
            if result.error_message:
                simplified = self._simplify_error_message(result.error_message)
                error_counts[simplified] += 1
                if simplified not in error_examples:
                    error_examples[simplified] = result
                    
        # 返回最常见的错误
        common_errors = []
        for error, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            common_errors.append({
                'pattern': error,
                'count': count,
                'example': error_examples[error].test_name,
                'percentage': (count / len(failed_results)) * 100
            })
            
        return common_errors

class EnhancedPytestRunner:
    """增强版pytest运行器"""
    
    def __init__(self, root_path: str = ".", output_dir: str = "enhanced_pytest_reports"):
        self.root_path = Path(root_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.collector = EnhancedPytestCollector()
        self.analyzer = TestAnalyzer()
        self.coverage_analyzer = None
        
    def run_comprehensive_tests(self, 
                              test_patterns: List[str] = None,
                              markers: List[str] = None,
                              enable_coverage: bool = True,
                              parallel: bool = False,
                              verbose: bool = True) -> ComprehensiveTestReport:
        """运行综合测试"""
        
        logger.info("开始运行综合测试...")
        
        # 发现测试文件
        test_files = self._discover_test_files(test_patterns)
        logger.info(f"发现 {len(test_files)} 个测试文件")
        
        # 准备pytest参数
        pytest_args = self._prepare_pytest_args(test_files, markers, verbose, parallel)
        
        # 启用覆盖率
        coverage_data = None
        if enable_coverage:
            coverage_data = self._setup_coverage()
            
        # 执行测试
        start_time = datetime.now()
        try:
            # 使用插件运行pytest
            exit_code = pytest.main(pytest_args, plugins=[self.collector])
            logger.info(f"Pytest执行完成，退出码: {exit_code}")
        except Exception as e:
            logger.error(f"Pytest执行失败: {e}")
            raise
        finally:
            end_time = datetime.now()
            
        # 停止覆盖率收集
        if enable_coverage and self.coverage_analyzer:
            coverage_data = self._stop_coverage()
            
        # 分析结果
        analysis = self.analyzer.analyze_test_results(self.collector.results)
        
        # 创建测试套件分析
        test_suites = self._create_test_suite_analyses(self.collector.results)
        
        # 生成综合报告
        report = ComprehensiveTestReport(
            report_id=f"comprehensive_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            generation_time=datetime.now(),
            execution_summary={
                'start_time': start_time,
                'end_time': end_time,
                'total_duration': (end_time - start_time).total_seconds(),
                'exit_code': exit_code,
                **analysis['summary']
            },
            test_suites=test_suites,
            coverage_analysis=coverage_data,
            performance_analysis=analysis['performance'],
            failure_analysis=analysis['failures'],
            environment_info=self._collect_environment_info(),
            recommendations=analysis['recommendations']
        )
        
        return report
        
    def run_tests_by_steps(self, 
                          test_categories: List[str] = None,
                          enable_coverage: bool = True) -> Dict[str, ComprehensiveTestReport]:
        """分步骤运行测试"""
        
        if test_categories is None:
            test_categories = ['unit', 'integration', 'performance', 'business_logic']
            
        step_reports = {}
        
        for category in test_categories:
            logger.info(f"执行 {category} 测试...")
            
            try:
                # 根据类别确定标记
                markers = self._get_markers_for_category(category)
                
                # 运行测试
                report = self.run_comprehensive_tests(
                    markers=markers,
                    enable_coverage=enable_coverage
                )
                
                step_reports[category] = report
                
                logger.info(f"{category} 测试完成: {report.execution_summary['passed']}/{report.execution_summary['total']} 通过")
                
            except Exception as e:
                logger.error(f"{category} 测试执行失败: {e}")
                # 创建错误报告
                step_reports[category] = self._create_error_report(category, str(e))
                
        return step_reports
        
    def generate_comprehensive_reports(self, report: ComprehensiveTestReport) -> Dict[str, str]:
        """生成综合报告"""
        
        report_files = {}
        
        # JSON报告
        json_file = self.output_dir / f"{report.report_id}.json"
        self._save_json_report(report, json_file)
        report_files['json'] = str(json_file)
        
        # HTML报告
        html_file = self.output_dir / f"{report.report_id}.html"
        self._generate_html_report(report, html_file)
        report_files['html'] = str(html_file)
        
        # Markdown报告
        md_file = self.output_dir / f"{report.report_id}.md"
        self._generate_markdown_report(report, md_file)
        report_files['markdown'] = str(md_file)
        
        # Excel报告（如果需要）
        try:
            excel_file = self.output_dir / f"{report.report_id}.xlsx"
            self._generate_excel_report(report, excel_file)
            report_files['excel'] = str(excel_file)
        except ImportError:
            logger.warning("无法生成Excel报告，缺少openpyxl依赖")
            
        return report_files
        
    def _discover_test_files(self, test_patterns: List[str] = None) -> List[Path]:
        """发现测试文件"""
        if test_patterns is None:
            test_patterns = ["test_*.py", "*_test.py"]
            
        test_files = []
        
        # 扫描pc28_business_logic_tests目录
        business_logic_dir = self.root_path / "pc28_business_logic_tests"
        if business_logic_dir.exists():
            for pattern in test_patterns:
                test_files.extend(business_logic_dir.rglob(pattern))
                
        # 扫描根目录下的测试文件
        for pattern in test_patterns:
            test_files.extend(self.root_path.glob(pattern))
            
        # 去重并排序
        test_files = sorted(list(set(test_files)))
        
        return test_files
        
    def _prepare_pytest_args(self, 
                           test_files: List[Path],
                           markers: List[str] = None,
                           verbose: bool = True,
                           parallel: bool = False) -> List[str]:
        """准备pytest参数"""
        
        args = []
        
        # 添加测试文件
        if test_files:
            args.extend([str(f) for f in test_files])
        else:
            args.extend(["pc28_business_logic_tests/", "test_*.py"])
            
        # 添加标记过滤
        if markers:
            for marker in markers:
                args.extend(["-m", marker])
                
        # 详细输出
        if verbose:
            args.append("-v")
            
        # 并行执行
        if parallel:
            args.extend(["-n", "auto"])
            
        # 其他有用的参数
        args.extend([
            "--tb=short",  # 简短的traceback
            "-s",  # 不捕获输出
            "--strict-markers",  # 严格标记模式
        ])
        
        return args
        
    def _setup_coverage(self) -> Optional[Dict]:
        """设置覆盖率分析"""
        try:
            self.coverage_analyzer = coverage.Coverage(
                source=[str(self.root_path)],
                omit=[
                    "*/venv/*",
                    "*/test*",
                    "*/__pycache__/*",
                    "*/migrations/*",
                    "*/enhanced_pytest_runner.py"
                ]
            )
            self.coverage_analyzer.start()
            logger.info("覆盖率分析已启动")
            return {}
        except Exception as e:
            logger.warning(f"无法启动覆盖率分析: {e}")
            return None
            
    def _stop_coverage(self) -> Optional[Dict]:
        """停止覆盖率分析"""
        if not self.coverage_analyzer:
            return None
            
        try:
            self.coverage_analyzer.stop()
            self.coverage_analyzer.save()
            
            # 生成覆盖率报告
            coverage_file = self.output_dir / "coverage.json"
            self.coverage_analyzer.json_report(outfile=str(coverage_file))
            
            # 读取覆盖率数据
            if coverage_file.exists():
                with open(coverage_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
                    
        except Exception as e:
            logger.warning(f"覆盖率分析失败: {e}")
            
        return None
        
    def _create_test_suite_analyses(self, results: List[DetailedTestResult]) -> List[TestSuiteAnalysis]:
        """创建测试套件分析"""
        
        # 按文件分组
        suites_by_file = defaultdict(list)
        for result in results:
            suites_by_file[result.test_file].append(result)
            
        test_suites = []
        
        for test_file, file_results in suites_by_file.items():
            # 计算统计信息
            passed_count = sum(1 for r in file_results if r.final_status == 'passed')
            failed_count = sum(1 for r in file_results if r.final_status == 'failed')
            error_count = sum(1 for r in file_results if r.final_status == 'error')
            skipped_count = sum(1 for r in file_results if r.final_status == 'skipped')
            xfail_count = sum(1 for r in file_results if r.final_status == 'xfail')
            xpass_count = sum(1 for r in file_results if r.final_status == 'xpass')
            
            total_duration = sum(r.total_duration for r in file_results)
            setup_duration = sum(sum(s.duration for s in r.steps if s.step_name == 'setup') for r in file_results)
            teardown_duration = sum(sum(s.duration for s in r.steps if s.step_name == 'teardown') for r in file_results)
            
            success_rate = (passed_count / len(file_results) * 100) if file_results else 0
            
            # 性能指标
            durations = [r.total_duration for r in file_results]
            performance_metrics = {
                'average_duration': sum(durations) / len(durations) if durations else 0,
                'max_duration': max(durations) if durations else 0,
                'min_duration': min(durations) if durations else 0,
                'slowest_test': max(file_results, key=lambda x: x.total_duration) if file_results else None,
                'fastest_test': min(file_results, key=lambda x: x.total_duration) if file_results else None
            }
            
            # 确定测试类型
            test_types = [r.test_type for r in file_results]
            primary_type = max(set(test_types), key=test_types.count) if test_types else 'unit'
            
            suite_analysis = TestSuiteAnalysis(
                suite_name=Path(test_file).stem,
                test_file=test_file,
                test_type=primary_type,
                tests=file_results,
                total_duration=total_duration,
                setup_duration=setup_duration,
                teardown_duration=teardown_duration,
                passed_count=passed_count,
                failed_count=failed_count,
                error_count=error_count,
                skipped_count=skipped_count,
                xfail_count=xfail_count,
                xpass_count=xpass_count,
                success_rate=success_rate,
                performance_metrics=performance_metrics,
                start_time=min(r.start_time for r in file_results if r.start_time) if file_results else None,
                end_time=max(r.end_time for r in file_results if r.end_time) if file_results else None
            )
            
            test_suites.append(suite_analysis)
            
        return test_suites
        
    def _get_markers_for_category(self, category: str) -> List[str]:
        """获取类别对应的标记"""
        marker_map = {
            'unit': [],  # 默认，无特殊标记
            'integration': ['integration'],
            'performance': ['performance'],
            'business_logic': ['lottery', 'betting', 'payout', 'risk'],
            'data': ['data']
        }
        return marker_map.get(category, [])
        
    def _collect_environment_info(self) -> Dict[str, Any]:
        """收集环境信息"""
        import platform
        
        return {
            'python_version': sys.version,
            'pytest_version': pytest.__version__,
            'platform': platform.platform(),
            'working_directory': str(self.root_path),
            'timestamp': datetime.now().isoformat(),
            'environment_variables': {
                key: value for key, value in os.environ.items()
                if key.startswith(('PYTEST_', 'TEST_', 'CI_', 'GITHUB_'))
            }
        }
        
    def _create_error_report(self, category: str, error_message: str) -> ComprehensiveTestReport:
        """创建错误报告"""
        return ComprehensiveTestReport(
            report_id=f"error_report_{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            generation_time=datetime.now(),
            execution_summary={
                'total': 0,
                'passed': 0,
                'failed': 0,
                'error': 1,
                'skipped': 0,
                'success_rate': 0,
                'error_message': error_message
            },
            test_suites=[],
            environment_info={'error': error_message}
        )
        
    def _save_json_report(self, report: ComprehensiveTestReport, file_path: Path):
        """保存JSON报告"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(report), f, indent=2, ensure_ascii=False, default=str)
            
    def _generate_html_report(self, report: ComprehensiveTestReport, file_path: Path):
        """生成HTML报告"""
        # 这里可以实现详细的HTML报告生成
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>PC28 综合测试报告</title>
    <meta charset="utf-8">
</head>
<body>
    <h1>PC28 综合测试报告</h1>
    <p>报告ID: {report.report_id}</p>
    <p>生成时间: {report.generation_time}</p>
    <!-- 更多HTML内容 -->
</body>
</html>
"""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
    def _generate_markdown_report(self, report: ComprehensiveTestReport, file_path: Path):
        """生成Markdown报告"""
        md_content = f"""# PC28 综合测试报告

## 基本信息
- 报告ID: {report.report_id}
- 生成时间: {report.generation_time}

## 执行摘要
- 总测试数: {report.execution_summary.get('total', 0)}
- 通过: {report.execution_summary.get('passed', 0)}
- 失败: {report.execution_summary.get('failed', 0)}
- 错误: {report.execution_summary.get('error', 0)}
- 跳过: {report.execution_summary.get('skipped', 0)}
- 成功率: {report.execution_summary.get('success_rate', 0):.1f}%

## 改进建议
"""
        if report.recommendations:
            for rec in report.recommendations:
                md_content += f"- {rec}\n"
        else:
            md_content += "- 暂无建议\n"
            
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
            
    def _generate_excel_report(self, report: ComprehensiveTestReport, file_path: Path):
        """生成Excel报告"""
        try:
            import pandas as pd
            
            # 创建测试结果数据框
            test_data = []
            for suite in report.test_suites:
                for test in suite.tests:
                    test_data.append({
                        'Suite': suite.suite_name,
                        'Test Name': test.test_name,
                        'Status': test.final_status,
                        'Duration': test.total_duration,
                        'Type': test.test_type,
                        'File': test.test_file
                    })
                    
            df = pd.DataFrame(test_data)
            
            # 保存到Excel
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Test Results', index=False)
                
                # 添加摘要表
                summary_data = {
                    'Metric': ['Total', 'Passed', 'Failed', 'Error', 'Skipped', 'Success Rate'],
                    'Value': [
                        report.execution_summary.get('total', 0),
                        report.execution_summary.get('passed', 0),
                        report.execution_summary.get('failed', 0),
                        report.execution_summary.get('error', 0),
                        report.execution_summary.get('skipped', 0),
                        f"{report.execution_summary.get('success_rate', 0):.1f}%"
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
        except ImportError:
            raise ImportError("需要安装pandas和openpyxl来生成Excel报告")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PC28增强版pytest测试运行器')
    parser.add_argument('--root', default='.', help='项目根目录')
    parser.add_argument('--output', default='enhanced_pytest_reports', help='输出目录')
    parser.add_argument('--markers', nargs='*', help='测试标记过滤')
    parser.add_argument('--no-coverage', action='store_true', help='禁用覆盖率')
    parser.add_argument('--parallel', action='store_true', help='并行执行')
    parser.add_argument('--steps', action='store_true', help='分步骤执行')
    parser.add_argument('--categories', nargs='*', help='测试类别')
    
    args = parser.parse_args()
    
    # 创建运行器
    runner = EnhancedPytestRunner(args.root, args.output)
    
    try:
        if args.steps:
            # 分步骤执行
            categories = args.categories or ['unit', 'integration', 'performance', 'business_logic']
            step_reports = runner.run_tests_by_steps(
                test_categories=categories,
                enable_coverage=not args.no_coverage
            )
            
            # 生成每个步骤的报告
            for category, report in step_reports.items():
                logger.info(f"生成 {category} 测试报告...")
                report_files = runner.generate_comprehensive_reports(report)
                logger.info(f"{category} 报告文件: {report_files}")
                
        else:
            # 综合执行
            report = runner.run_comprehensive_tests(
                markers=args.markers,
                enable_coverage=not args.no_coverage,
                parallel=args.parallel
            )
            
            # 生成报告
            report_files = runner.generate_comprehensive_reports(report)
            
            # 输出结果
            logger.info("测试执行完成!")
            logger.info(f"总测试数: {report.execution_summary['total']}")
            logger.info(f"通过: {report.execution_summary['passed']}")
            logger.info(f"失败: {report.execution_summary['failed']}")
            logger.info(f"错误: {report.execution_summary['error']}")
            logger.info(f"跳过: {report.execution_summary['skipped']}")
            logger.info(f"成功率: {report.execution_summary['success_rate']:.1f}%")
            logger.info(f"总执行时间: {report.execution_summary['total_duration']:.2f}秒")
            
            if report.recommendations:
                logger.info("改进建议:")
                for rec in report.recommendations:
                    logger.info(f"  - {rec}")
                    
            logger.info("生成的报告文件:")
            for format_type, file_path in report_files.items():
                logger.info(f"  {format_type.upper()}: {file_path}")
                
    except Exception as e:
        logger.error(f"执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()