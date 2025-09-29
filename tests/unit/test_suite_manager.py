#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一测试套件管理器
自动发现、执行和报告所有测试，支持多种测试框架和并行执行
"""

import os
import sys
import json
import time
import logging
import unittest
import subprocess
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict
from collections import defaultdict
import traceback
import argparse
import importlib.util
import coverage

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ManagerTestResult:
    """测试结果数据类"""
    test_id: str
    test_name: str
    test_file: str
    test_class: Optional[str]
    test_method: str
    status: str  # passed, failed, error, skipped
    duration: float
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    test_type: str = "unit"  # unit, integration, e2e
    
@dataclass
class ManagerTestSuite:
    """测试套件数据类"""
    suite_name: str
    test_file: str
    test_type: str
    tests: List[ManagerTestResult]
    total_duration: float
    passed_count: int
    failed_count: int
    error_count: int
    skipped_count: int
    success_rate: float
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

@dataclass
class ManagerTestReport:
    """测试报告数据类"""
    report_id: str
    generation_time: datetime
    total_tests: int
    total_suites: int
    passed_tests: int
    failed_tests: int
    error_tests: int
    skipped_tests: int
    total_duration: float
    success_rate: float
    test_suites: List[ManagerTestSuite]
    coverage_data: Optional[Dict] = None
    performance_metrics: Optional[Dict] = None
    environment_info: Optional[Dict] = None

class PC28TestDiscovery:
    """测试发现器"""
    
    def __init__(self, root_path: str):
        self.root_path = Path(root_path)
        self.test_patterns = [
            'test_*.py',
            '*_test.py',
            'tests.py'
        ]
        
    def discover_test_files(self) -> List[Path]:
        """发现所有测试文件"""
        test_files = []
        
        # 排除的目录模式
        exclude_patterns = {
            'venv', 'env', '.venv', '.env',
            'node_modules', '__pycache__', '.git',
            'site-packages', 'dist-packages',
            '.pytest_cache', '.coverage'
        }
        
        for pattern in self.test_patterns:
            for file_path in self.root_path.rglob(pattern):
                # 检查是否在排除目录中
                should_exclude = False
                for part in file_path.parts:
                    if part in exclude_patterns or part.startswith('.'):
                        should_exclude = True
                        break
                
                if not should_exclude:
                    test_files.append(file_path)
            
        # 去重并排序
        test_files = list(set(test_files))
        test_files.sort()
        
        logger.info(f"发现 {len(test_files)} 个测试文件")
        return test_files
    
    def classify_test_type(self, test_file: Path) -> str:
        """根据文件名和路径分类测试类型"""
        file_name = test_file.name.lower()
        file_path = str(test_file).lower()
        
        if 'e2e' in file_name or 'end_to_end' in file_name:
            return 'e2e'
        elif 'integration' in file_name or 'integration' in file_path:
            return 'integration'
        else:
            return 'unit'
    
    def analyze_test_file(self, test_file: Path) -> Dict[str, Any]:
        """分析测试文件内容"""
        try:
            with open(test_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 检测测试框架
            framework = 'unknown'
            if 'import unittest' in content or 'from unittest' in content:
                framework = 'unittest'
            elif 'import pytest' in content or 'def test_' in content:
                framework = 'pytest'
            
            # 统计测试方法数量
            test_methods = []
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('def test_') or line.startswith('async def test_'):
                    method_name = line.split('(')[0].replace('def ', '').replace('async def ', '')
                    test_methods.append(method_name)
            
            return {
                'file_path': str(test_file),
                'framework': framework,
                'test_type': self.classify_test_type(test_file),
                'test_methods': test_methods,
                'method_count': len(test_methods)
            }
            
        except Exception as e:
            logger.error(f"分析测试文件失败 {test_file}: {e}")
            return {
                'file_path': str(test_file),
                'framework': 'unknown',
                'test_type': 'unknown',
                'test_methods': [],
                'method_count': 0,
                'error': str(e)
            }

class PC28TestExecutor:
    """测试执行器"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.results = []
        
    def execute_unittest_file(self, test_file: Path) -> List[ManagerTestResult]:
        """执行unittest测试文件"""
        results = []
        
        try:
            # 动态导入测试模块
            spec = importlib.util.spec_from_file_location(
                test_file.stem, test_file
            )
            module = importlib.util.module_from_spec(spec)
            
            # 添加到sys.modules以支持相对导入
            sys.modules[test_file.stem] = module
            spec.loader.exec_module(module)
            
            # 创建测试套件
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromModule(module)
            
            # 自定义测试结果收集器
            class TestResultCollector(unittest.TestResult):
                def __init__(self):
                    super().__init__()
                    self.test_results = []
                
                def startTest(self, test):
                    super().startTest(test)
                    test._start_time = time.time()
                
                def stopTest(self, test):
                    super().stopTest(test)
                    duration = time.time() - test._start_time
                    
                    test_id = f"{test.__class__.__module__}.{test.__class__.__name__}.{test._testMethodName}"
                    
                    result = ManagerTestResult(
                        test_id=test_id,
                        test_name=test._testMethodName,
                        test_file=str(test_file),
                        test_class=test.__class__.__name__,
                        test_method=test._testMethodName,
                        status='passed',
                        duration=duration,
                        start_time=datetime.fromtimestamp(test._start_time),
                        end_time=datetime.now()
                    )
                    
                    self.test_results.append(result)
                
                def addError(self, test, err):
                    super().addError(test, err)
                    if self.test_results:
                        self.test_results[-1].status = 'error'
                        self.test_results[-1].error_message = str(err[1])
                        self.test_results[-1].error_traceback = ''.join(
                            traceback.format_exception(*err)
                        )
                
                def addFailure(self, test, err):
                    super().addFailure(test, err)
                    if self.test_results:
                        self.test_results[-1].status = 'failed'
                        self.test_results[-1].error_message = str(err[1])
                        self.test_results[-1].error_traceback = ''.join(
                            traceback.format_exception(*err)
                        )
                
                def addSkip(self, test, reason):
                    super().addSkip(test, reason)
                    if self.test_results:
                        self.test_results[-1].status = 'skipped'
                        self.test_results[-1].error_message = reason
            
            # 运行测试
            result_collector = TestResultCollector()
            suite.run(result_collector)
            
            results.extend(result_collector.test_results)
            
        except Exception as e:
            logger.error(f"执行unittest文件失败 {test_file}: {e}")
            # 创建错误结果
            error_result = ManagerTestResult(
                test_id=f"{test_file.stem}.error",
                test_name="file_execution_error",
                test_file=str(test_file),
                test_class=None,
                test_method="file_execution_error",
                status='error',
                duration=0.0,
                error_message=str(e),
                error_traceback=traceback.format_exc()
            )
            results.append(error_result)
        
        return results
    
    def execute_pytest_file(self, test_file: Path) -> List[ManagerTestResult]:
        """执行pytest测试文件"""
        results = []
        
        try:
            # 使用subprocess运行pytest并收集结果
            cmd = [
                sys.executable, '-m', 'pytest',
                str(test_file),
                '--json-report',
                '--json-report-file=/tmp/pytest_report.json',
                '-v'
            ]
            
            start_time = time.time()
            result = subprocess.run(
                cmd, capture_output=True, text=True, cwd=test_file.parent
            )
            duration = time.time() - start_time
            
            # 解析pytest输出
            if result.returncode == 0:
                # 成功执行
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if '::' in line and ('PASSED' in line or 'FAILED' in line or 'SKIPPED' in line):
                        parts = line.split(' ')
                        test_name = parts[0] if parts else 'unknown'
                        status = 'passed' if 'PASSED' in line else ('failed' if 'FAILED' in line else 'skipped')
                        
                        test_result = ManagerTestResult(
                            test_id=f"{test_file.stem}.{test_name}",
                            test_name=test_name,
                            test_file=str(test_file),
                            test_class=None,
                            test_method=test_name,
                            status=status,
                            duration=duration / max(1, len(output_lines)),  # 平均分配时间
                            start_time=datetime.fromtimestamp(start_time),
                            end_time=datetime.now()
                        )
                        results.append(test_result)
            else:
                # 执行失败
                error_result = ManagerTestResult(
                    test_id=f"{test_file.stem}.pytest_error",
                    test_name="pytest_execution_error",
                    test_file=str(test_file),
                    test_class=None,
                    test_method="pytest_execution_error",
                    status='error',
                    duration=duration,
                    error_message=result.stderr or result.stdout,
                    start_time=datetime.fromtimestamp(start_time),
                    end_time=datetime.now()
                )
                results.append(error_result)
                
        except Exception as e:
            logger.error(f"执行pytest文件失败 {test_file}: {e}")
            error_result = ManagerTestResult(
                test_id=f"{test_file.stem}.execution_error",
                test_name="file_execution_error",
                test_file=str(test_file),
                test_class=None,
                test_method="file_execution_error",
                status='error',
                duration=0.0,
                error_message=str(e),
                error_traceback=traceback.format_exc()
            )
            results.append(error_result)
        
        return results
    
    def execute_test_file(self, test_file: Path, framework: str) -> List[ManagerTestResult]:
        """执行单个测试文件"""
        logger.info(f"执行测试文件: {test_file} (框架: {framework})")
        
        if framework == 'unittest':
            return self.execute_unittest_file(test_file)
        elif framework == 'pytest':
            return self.execute_pytest_file(test_file)
        else:
            # 尝试通用执行
            try:
                return self.execute_unittest_file(test_file)
            except:
                return self.execute_pytest_file(test_file)
    
    def execute_tests_parallel(self, test_files: List[Tuple[Path, str]]) -> List[ManagerTestResult]:
        """并行执行测试"""
        all_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有测试任务
            future_to_file = {
                executor.submit(self.execute_test_file, test_file, framework): (test_file, framework)
                for test_file, framework in test_files
            }
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_file):
                test_file, framework = future_to_file[future]
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"并行执行测试失败 {test_file}: {e}")
                    # 创建错误结果
                    error_result = ManagerTestResult(
                        test_id=f"{test_file.stem}.parallel_error",
                        test_name="parallel_execution_error",
                        test_file=str(test_file),
                        test_class=None,
                        test_method="parallel_execution_error",
                        status='error',
                        duration=0.0,
                        error_message=str(e),
                        error_traceback=traceback.format_exc()
                    )
                    all_results.append(error_result)
        
        return all_results

class CoverageAnalyzer:
    """测试覆盖率分析器"""
    
    def __init__(self, source_dirs: List[str]):
        self.source_dirs = source_dirs
        self.cov = None
    
    def start_coverage(self):
        """开始覆盖率收集"""
        self.cov = coverage.Coverage(source=self.source_dirs)
        self.cov.start()
    
    def stop_coverage(self) -> Dict[str, Any]:
        """停止覆盖率收集并生成报告"""
        if not self.cov:
            return {}
        
        self.cov.stop()
        self.cov.save()
        
        # 生成覆盖率报告
        coverage_data = {
            'total_statements': 0,
            'covered_statements': 0,
            'missing_statements': 0,
            'coverage_percentage': 0.0,
            'files': {}
        }
        
        try:
            # 获取覆盖率数据
            for filename in self.cov.get_data().measured_files():
                analysis = self.cov.analysis2(filename)
                statements = len(analysis.statements)
                missing = len(analysis.missing)
                covered = statements - missing
                
                coverage_data['files'][filename] = {
                    'statements': statements,
                    'covered': covered,
                    'missing': missing,
                    'coverage': (covered / statements * 100) if statements > 0 else 0
                }
                
                coverage_data['total_statements'] += statements
                coverage_data['covered_statements'] += covered
                coverage_data['missing_statements'] += missing
            
            # 计算总覆盖率
            if coverage_data['total_statements'] > 0:
                coverage_data['coverage_percentage'] = (
                    coverage_data['covered_statements'] / coverage_data['total_statements'] * 100
                )
        
        except Exception as e:
            logger.error(f"生成覆盖率报告失败: {e}")
            coverage_data['error'] = str(e)
        
        return coverage_data

class ReportGenerator:
    """测试报告生成器"""
    
    def __init__(self, output_dir: str = "test_reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_json_report(self, report: ManagerTestReport) -> str:
        """生成JSON格式报告"""
        report_file = self.output_dir / f"test_report_{report.report_id}.json"
        
        # 转换为可序列化的字典
        report_dict = asdict(report)
        
        # 处理datetime对象
        def serialize_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return obj
        
        # 递归处理所有datetime对象
        def process_dict(d):
            if isinstance(d, dict):
                return {k: process_dict(v) for k, v in d.items()}
            elif isinstance(d, list):
                return [process_dict(item) for item in d]
            else:
                return serialize_datetime(d)
        
        report_dict = process_dict(report_dict)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"JSON报告已生成: {report_file}")
        return str(report_file)
    
    def generate_html_report(self, report: ManagerTestReport) -> str:
        """生成HTML格式报告"""
        report_file = self.output_dir / f"test_report_{report.report_id}.html"
        
        # HTML模板
        html_template = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>测试报告 - {report_id}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; margin-bottom: 30px; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .metric {{ background: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; }}
        .metric h3 {{ margin: 0 0 10px 0; color: #333; }}
        .metric .value {{ font-size: 24px; font-weight: bold; }}
        .passed {{ color: #28a745; }}
        .failed {{ color: #dc3545; }}
        .error {{ color: #fd7e14; }}
        .skipped {{ color: #6c757d; }}
        .suite {{ margin-bottom: 20px; border: 1px solid #ddd; border-radius: 5px; }}
        .suite-header {{ background: #e9ecef; padding: 10px; font-weight: bold; }}
        .test-list {{ padding: 10px; }}
        .test-item {{ padding: 8px; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; align-items: center; }}
        .test-item:last-child {{ border-bottom: none; }}
        .status-badge {{ padding: 2px 8px; border-radius: 3px; color: white; font-size: 12px; }}
        .status-passed {{ background-color: #28a745; }}
        .status-failed {{ background-color: #dc3545; }}
        .status-error {{ background-color: #fd7e14; }}
        .status-skipped {{ background-color: #6c757d; }}
        .error-details {{ background: #f8d7da; padding: 10px; margin-top: 5px; border-radius: 3px; font-family: monospace; font-size: 12px; }}
        .coverage-section {{ margin-top: 30px; }}
        .coverage-bar {{ background: #e9ecef; height: 20px; border-radius: 10px; overflow: hidden; }}
        .coverage-fill {{ background: #28a745; height: 100%; transition: width 0.3s ease; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>测试报告</h1>
            <p>报告ID: {report_id}</p>
            <p>生成时间: {generation_time}</p>
        </div>
        
        <div class="summary">
            <div class="metric">
                <h3>总测试数</h3>
                <div class="value">{total_tests}</div>
            </div>
            <div class="metric">
                <h3>通过</h3>
                <div class="value passed">{passed_tests}</div>
            </div>
            <div class="metric">
                <h3>失败</h3>
                <div class="value failed">{failed_tests}</div>
            </div>
            <div class="metric">
                <h3>错误</h3>
                <div class="value error">{error_tests}</div>
            </div>
            <div class="metric">
                <h3>跳过</h3>
                <div class="value skipped">{skipped_tests}</div>
            </div>
            <div class="metric">
                <h3>成功率</h3>
                <div class="value">{success_rate:.1f}%</div>
            </div>
            <div class="metric">
                <h3>总耗时</h3>
                <div class="value">{total_duration:.2f}s</div>
            </div>
        </div>
        
        {coverage_section}
        
        <div class="suites">
            <h2>测试套件详情</h2>
            {suites_html}
        </div>
    </div>
</body>
</html>
        """
        
        # 生成套件HTML
        suites_html = ""
        for suite in report.test_suites:
            tests_html = ""
            for test in suite.tests:
                status_class = f"status-{test.status}"
                error_html = ""
                if test.error_message:
                    error_html = f'<div class="error-details">{test.error_message}</div>'
                
                tests_html += f"""
                <div class="test-item">
                    <div>
                        <strong>{test.test_name}</strong>
                        <span class="status-badge {status_class}">{test.status.upper()}</span>
                        <small>({test.duration:.3f}s)</small>
                        {error_html}
                    </div>
                </div>
                """
            
            suites_html += f"""
            <div class="suite">
                <div class="suite-header">
                    {suite.suite_name} - {suite.test_type.upper()}
                    <small>({suite.passed_count}通过 / {suite.failed_count}失败 / {suite.error_count}错误 / {suite.skipped_count}跳过)</small>
                </div>
                <div class="test-list">
                    {tests_html}
                </div>
            </div>
            """
        
        # 生成覆盖率部分
        coverage_section = ""
        if report.coverage_data:
            coverage_percentage = report.coverage_data.get('coverage_percentage', 0)
            coverage_section = f"""
            <div class="coverage-section">
                <h2>代码覆盖率</h2>
                <div class="metric">
                    <h3>总覆盖率</h3>
                    <div class="coverage-bar">
                        <div class="coverage-fill" style="width: {coverage_percentage}%"></div>
                    </div>
                    <div class="value">{coverage_percentage:.1f}%</div>
                </div>
            </div>
            """
        
        # 填充模板
        html_content = html_template.format(
            report_id=report.report_id,
            generation_time=report.generation_time.strftime('%Y-%m-%d %H:%M:%S'),
            total_tests=report.total_tests,
            passed_tests=report.passed_tests,
            failed_tests=report.failed_tests,
            error_tests=report.error_tests,
            skipped_tests=report.skipped_tests,
            success_rate=report.success_rate,
            total_duration=report.total_duration,
            coverage_section=coverage_section,
            suites_html=suites_html
        )
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML报告已生成: {report_file}")
        return str(report_file)

class PC28TestSuiteManager:
    """统一测试套件管理器"""
    
    def __init__(self, root_path: str = ".", max_workers: int = 4):
        self.root_path = Path(root_path).resolve()
        self.discovery = PC28TestDiscovery(str(self.root_path))
        self.executor = PC28TestExecutor(max_workers)
        self.coverage_analyzer = CoverageAnalyzer([str(self.root_path)])
        self.report_generator = ReportGenerator()
        
        logger.info(f"测试套件管理器初始化完成，根路径: {self.root_path}")
    
    def check_environment(self) -> Dict[str, Any]:
        """检查测试环境"""
        env_info = {
            'python_version': sys.version,
            'working_directory': str(self.root_path),
            'available_frameworks': [],
            'dependencies': {},
            'issues': []
        }
        
        # 检查测试框架
        try:
            import unittest
            env_info['available_frameworks'].append('unittest')
        except ImportError:
            env_info['issues'].append('unittest不可用')
        
        try:
            import pytest
            env_info['available_frameworks'].append('pytest')
            env_info['dependencies']['pytest'] = pytest.__version__
        except ImportError:
            env_info['issues'].append('pytest不可用')
        
        try:
            import coverage
            env_info['dependencies']['coverage'] = coverage.__version__
        except ImportError:
            env_info['issues'].append('coverage不可用，无法进行覆盖率分析')
        
        return env_info
    
    def run_all_tests(self, 
                     enable_coverage: bool = True,
                     parallel: bool = True,
                     test_types: Optional[List[str]] = None) -> ManagerTestReport:
        """运行所有测试"""
        logger.info("开始运行所有测试...")
        
        # 检查环境
        env_info = self.check_environment()
        if env_info['issues']:
            logger.warning(f"环境检查发现问题: {env_info['issues']}")
        
        # 发现测试文件
        test_files = self.discovery.discover_test_files()
        if not test_files:
            logger.warning("未发现任何测试文件")
            return self._create_empty_report()
        
        # 分析测试文件
        test_file_info = []
        for test_file in test_files:
            info = self.discovery.analyze_test_file(test_file)
            if test_types and info['test_type'] not in test_types:
                continue
            test_file_info.append((test_file, info['framework']))
        
        if not test_file_info:
            logger.warning("没有匹配的测试文件")
            return self._create_empty_report()
        
        # 开始覆盖率收集
        coverage_data = None
        if enable_coverage:
            try:
                self.coverage_analyzer.start_coverage()
            except Exception as e:
                logger.warning(f"启动覆盖率收集失败: {e}")
        
        # 执行测试
        start_time = time.time()
        if parallel and len(test_file_info) > 1:
            all_results = self.executor.execute_tests_parallel(test_file_info)
        else:
            all_results = []
            for test_file, framework in test_file_info:
                results = self.executor.execute_test_file(test_file, framework)
                all_results.extend(results)
        
        total_duration = time.time() - start_time
        
        # 停止覆盖率收集
        if enable_coverage:
            try:
                coverage_data = self.coverage_analyzer.stop_coverage()
            except Exception as e:
                logger.warning(f"停止覆盖率收集失败: {e}")
        
        # 生成测试报告
        report = self._create_test_report(all_results, total_duration, coverage_data, env_info)
        
        logger.info(f"测试完成，共执行 {report.total_tests} 个测试，成功率 {report.success_rate:.1f}%")
        return report
    
    def _create_test_report(self, 
                           results: List[ManagerTestResult], 
                           total_duration: float,
                           coverage_data: Optional[Dict] = None,
                           env_info: Optional[Dict] = None) -> ManagerTestReport:
        """创建测试报告"""
        # 按文件分组测试结果
        suites_by_file = defaultdict(list)
        for result in results:
            suites_by_file[result.test_file].append(result)
        
        # 创建测试套件
        test_suites = []
        for test_file, file_results in suites_by_file.items():
            # 计算套件统计
            passed_count = sum(1 for r in file_results if r.status == 'passed')
            failed_count = sum(1 for r in file_results if r.status == 'failed')
            error_count = sum(1 for r in file_results if r.status == 'error')
            skipped_count = sum(1 for r in file_results if r.status == 'skipped')
            suite_duration = sum(r.duration for r in file_results)
            
            success_rate = (passed_count / len(file_results) * 100) if file_results else 0
            
            # 确定测试类型
            test_type = 'unit'
            if file_results:
                test_type = file_results[0].test_type or self.discovery.classify_test_type(Path(test_file))
            
            suite = ManagerTestSuite(
                suite_name=Path(test_file).stem,
                test_file=test_file,
                test_type=test_type,
                tests=file_results,
                total_duration=suite_duration,
                passed_count=passed_count,
                failed_count=failed_count,
                error_count=error_count,
                skipped_count=skipped_count,
                success_rate=success_rate
            )
            test_suites.append(suite)
        
        # 计算总体统计
        total_tests = len(results)
        passed_tests = sum(1 for r in results if r.status == 'passed')
        failed_tests = sum(1 for r in results if r.status == 'failed')
        error_tests = sum(1 for r in results if r.status == 'error')
        skipped_tests = sum(1 for r in results if r.status == 'skipped')
        
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        # 创建报告
        report_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        report = ManagerTestReport(
            report_id=report_id,
            generation_time=datetime.now(),
            total_tests=total_tests,
            total_suites=len(test_suites),
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            error_tests=error_tests,
            skipped_tests=skipped_tests,
            total_duration=total_duration,
            success_rate=success_rate,
            test_suites=test_suites,
            coverage_data=coverage_data,
            environment_info=env_info
        )
        
        return report
    
    def _create_empty_report(self) -> ManagerTestReport:
        """创建空测试报告"""
        report_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        return ManagerTestReport(
            report_id=report_id,
            generation_time=datetime.now(),
            total_tests=0,
            total_suites=0,
            passed_tests=0,
            failed_tests=0,
            error_tests=0,
            skipped_tests=0,
            total_duration=0.0,
            success_rate=0.0,
            test_suites=[]
        )
    
    def generate_reports(self, report: ManagerTestReport) -> Dict[str, str]:
        """生成测试报告文件"""
        report_files = {}
        
        try:
            json_file = self.report_generator.generate_json_report(report)
            report_files['json'] = json_file
        except Exception as e:
            logger.error(f"生成JSON报告失败: {e}")
        
        try:
            html_file = self.report_generator.generate_html_report(report)
            report_files['html'] = html_file
        except Exception as e:
            logger.error(f"生成HTML报告失败: {e}")
        
        return report_files

def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='统一测试套件管理器')
    parser.add_argument('--root', '-r', default='.', help='测试根目录')
    parser.add_argument('--workers', '-w', type=int, default=4, help='并行工作线程数')
    parser.add_argument('--no-coverage', action='store_true', help='禁用覆盖率分析')
    parser.add_argument('--no-parallel', action='store_true', help='禁用并行执行')
    parser.add_argument('--types', nargs='+', choices=['unit', 'integration', 'e2e'], 
                       help='指定测试类型')
    parser.add_argument('--output', '-o', default='test_reports', help='报告输出目录')
    parser.add_argument('--check-env', action='store_true', help='只检查环境')
    
    args = parser.parse_args()
    
    # 创建管理器
    manager = PC28TestSuiteManager(args.root, args.workers)
    manager.report_generator = ReportGenerator(args.output)
    
    if args.check_env:
        # 只检查环境
        env_info = manager.check_environment()
        print("\n=== 测试环境检查 ===")
        print(f"Python版本: {env_info['python_version']}")
        print(f"工作目录: {env_info['working_directory']}")
        print(f"可用框架: {', '.join(env_info['available_frameworks'])}")
        if env_info['dependencies']:
            print("依赖版本:")
            for dep, version in env_info['dependencies'].items():
                print(f"  {dep}: {version}")
        if env_info['issues']:
            print("发现问题:")
            for issue in env_info['issues']:
                print(f"  - {issue}")
        return
    
    # 运行测试
    print("\n🚀 开始运行测试套件...")
    report = manager.run_all_tests(
        enable_coverage=not args.no_coverage,
        parallel=not args.no_parallel,
        test_types=args.types
    )
    
    # 生成报告
    print("\n📊 生成测试报告...")
    report_files = manager.generate_reports(report)
    
    # 显示结果
    print("\n=== 测试结果摘要 ===")
    print(f"总测试数: {report.total_tests}")
    print(f"通过: {report.passed_tests}")
    print(f"失败: {report.failed_tests}")
    print(f"错误: {report.error_tests}")
    print(f"跳过: {report.skipped_tests}")
    print(f"成功率: {report.success_rate:.1f}%")
    print(f"总耗时: {report.total_duration:.2f}秒")
    
    if report.coverage_data:
        print(f"代码覆盖率: {report.coverage_data.get('coverage_percentage', 0):.1f}%")
    
    print("\n=== 生成的报告 ===")
    for format_type, file_path in report_files.items():
        print(f"{format_type.upper()}报告: {file_path}")
    
    # 返回退出码
    if report.failed_tests > 0 or report.error_tests > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main()