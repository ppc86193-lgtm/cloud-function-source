#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统自动化pytest测试执行器
自动发现、运行和捕获pytest测试的详细执行结果
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
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PytestTestResult:
    """Pytest测试结果数据类"""
    test_id: str
    test_name: str
    test_file: str
    test_class: Optional[str]
    test_method: str
    status: str  # passed, failed, error, skipped, xfail, xpass
    duration: float
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    test_type: str = "unit"
    markers: List[str] = None
    setup_duration: float = 0.0
    teardown_duration: float = 0.0
    
    def __post_init__(self):
        if self.markers is None:
            self.markers = []

@dataclass
class PytestSuiteResult:
    """Pytest测试套件结果"""
    suite_name: str
    test_file: str
    test_type: str
    tests: List[PytestTestResult]
    total_duration: float
    passed_count: int
    failed_count: int
    error_count: int
    skipped_count: int
    xfail_count: int
    xpass_count: int
    success_rate: float
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    coverage_data: Optional[Dict] = None

@dataclass
class PytestExecutionReport:
    """Pytest执行报告"""
    report_id: str
    generation_time: datetime
    total_tests: int
    total_suites: int
    passed_tests: int
    failed_tests: int
    error_tests: int
    skipped_tests: int
    xfail_tests: int
    xpass_tests: int
    total_duration: float
    success_rate: float
    test_suites: List[PytestSuiteResult]
    coverage_data: Optional[Dict] = None
    performance_metrics: Optional[Dict] = None
    environment_info: Optional[Dict] = None
    command_line_args: Optional[List[str]] = None

class PytestResultCollector:
    """Pytest结果收集器"""
    
    def __init__(self):
        self.results = []
        self.current_suite = None
        self.start_time = None
        self.end_time = None
        
    def pytest_runtest_setup(self, item):
        """测试设置阶段"""
        test_info = self._extract_test_info(item)
        test_info['setup_start'] = datetime.now()
        item.test_info = test_info
        
    def pytest_runtest_call(self, item):
        """测试执行阶段"""
        if hasattr(item, 'test_info'):
            item.test_info['call_start'] = datetime.now()
            
    def pytest_runtest_teardown(self, item):
        """测试清理阶段"""
        if hasattr(item, 'test_info'):
            item.test_info['teardown_start'] = datetime.now()
            
    def pytest_runtest_logreport(self, report):
        """收集测试报告"""
        if report.when == 'call':
            test_result = self._create_test_result(report)
            self.results.append(test_result)
            
    def pytest_sessionstart(self, session):
        """测试会话开始"""
        self.start_time = datetime.now()
        logger.info(f"Pytest会话开始: {self.start_time}")
        
    def pytest_sessionfinish(self, session, exitstatus):
        """测试会话结束"""
        self.end_time = datetime.now()
        logger.info(f"Pytest会话结束: {self.end_time}, 退出状态: {exitstatus}")
        
    def _extract_test_info(self, item):
        """提取测试信息"""
        return {
            'test_id': item.nodeid,
            'test_name': item.name,
            'test_file': str(item.fspath),
            'test_class': item.cls.__name__ if item.cls else None,
            'test_method': item.function.__name__,
            'markers': [marker.name for marker in item.iter_markers()],
            'test_type': self._determine_test_type(item)
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
        else:
            return 'unit'
            
    def _create_test_result(self, report):
        """创建测试结果对象"""
        item = report.item
        test_info = getattr(item, 'test_info', {})
        
        # 计算执行时间
        setup_duration = 0.0
        call_duration = report.duration
        teardown_duration = 0.0
        
        if 'setup_start' in test_info and 'call_start' in test_info:
            setup_duration = (test_info['call_start'] - test_info['setup_start']).total_seconds()
        if 'teardown_start' in test_info:
            teardown_duration = 0.1  # 估算值
            
        # 处理错误信息
        error_message = None
        error_traceback = None
        if report.failed or report.outcome == 'error':
            if hasattr(report, 'longrepr') and report.longrepr:
                error_message = str(report.longrepr).split('\n')[-1] if report.longrepr else None
                error_traceback = str(report.longrepr)
                
        return PytestTestResult(
            test_id=test_info.get('test_id', item.nodeid),
            test_name=test_info.get('test_name', item.name),
            test_file=test_info.get('test_file', str(item.fspath)),
            test_class=test_info.get('test_class'),
            test_method=test_info.get('test_method', item.function.__name__),
            status=report.outcome,
            duration=call_duration,
            error_message=error_message,
            error_traceback=error_traceback,
            start_time=test_info.get('call_start'),
            end_time=datetime.now(),
            test_type=test_info.get('test_type', 'unit'),
            markers=test_info.get('markers', []),
            setup_duration=setup_duration,
            teardown_duration=teardown_duration
        )

class AutomatedPytestExecutor:
    """自动化Pytest执行器"""
    
    def __init__(self, root_path: str = ".", output_dir: str = "pytest_reports"):
        self.root_path = Path(root_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.collector = PytestResultCollector()
        self.coverage_analyzer = None
        
    def discover_test_files(self, test_patterns: List[str] = None) -> List[Path]:
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
        
        logger.info(f"发现 {len(test_files)} 个测试文件")
        for test_file in test_files:
            logger.info(f"  - {test_file}")
            
        return test_files
        
    def run_pytest_with_collection(self, 
                                 test_files: List[Path] = None,
                                 markers: List[str] = None,
                                 enable_coverage: bool = True,
                                 verbose: bool = True,
                                 capture: str = "no",
                                 extra_args: List[str] = None) -> PytestExecutionReport:
        """运行pytest并收集结果"""
        
        # 准备pytest参数
        pytest_args = []
        
        # 添加测试文件
        if test_files:
            pytest_args.extend([str(f) for f in test_files])
        else:
            # 使用默认测试路径
            pytest_args.extend([
                "pc28_business_logic_tests/",
                "test_*.py"
            ])
            
        # 添加标记过滤
        if markers:
            for marker in markers:
                pytest_args.extend(["-m", marker])
                
        # 添加详细输出
        if verbose:
            pytest_args.append("-v")
            
        # 设置输出捕获
        pytest_args.extend(["-s" if capture == "no" else f"--capture={capture}"])
        
        # 添加额外参数
        if extra_args:
            pytest_args.extend(extra_args)
            
        # 启用覆盖率
        if enable_coverage:
            self._setup_coverage()
            
        logger.info(f"执行pytest命令: pytest {' '.join(pytest_args)}")
        
        # 注册插件
        pytest.main(pytest_args + ["--tb=short"], plugins=[self.collector])
        
        # 停止覆盖率收集
        coverage_data = None
        if enable_coverage and self.coverage_analyzer:
            coverage_data = self._stop_coverage()
            
        # 生成报告
        return self._create_execution_report(
            results=self.collector.results,
            coverage_data=coverage_data,
            command_args=pytest_args
        )
        
    def run_pytest_subprocess(self,
                            test_files: List[Path] = None,
                            markers: List[str] = None,
                            enable_coverage: bool = True,
                            verbose: bool = True,
                            output_format: str = "json") -> PytestExecutionReport:
        """通过子进程运行pytest"""
        
        # 构建命令
        cmd = [sys.executable, "-m", "pytest"]
        
        # 添加测试文件
        if test_files:
            cmd.extend([str(f) for f in test_files])
        else:
            cmd.extend(["pc28_business_logic_tests/", "test_*.py"])
            
        # 添加标记过滤
        if markers:
            for marker in markers:
                cmd.extend(["-m", marker])
                
        # 添加输出格式
        if output_format == "json":
            json_report = self.output_dir / f"pytest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            cmd.extend(["--json-report", f"--json-report-file={json_report}"])
            
        # 添加详细输出
        if verbose:
            cmd.append("-v")
            
        # 添加覆盖率
        if enable_coverage:
            cmd.extend([
                "--cov=.",
                "--cov-report=json",
                f"--cov-report=json:{self.output_dir}/coverage.json"
            ])
            
        logger.info(f"执行命令: {' '.join(cmd)}")
        
        # 执行命令
        start_time = datetime.now()
        try:
            result = subprocess.run(
                cmd,
                cwd=self.root_path,
                capture_output=True,
                text=True,
                timeout=3600  # 1小时超时
            )
            end_time = datetime.now()
            
            # 解析结果
            return self._parse_subprocess_result(
                result, start_time, end_time, json_report if output_format == "json" else None
            )
            
        except subprocess.TimeoutExpired:
            logger.error("Pytest执行超时")
            return self._create_empty_report("执行超时")
        except Exception as e:
            logger.error(f"Pytest执行失败: {e}")
            return self._create_empty_report(f"执行失败: {e}")
            
    def run_tests_by_category(self, enable_coverage: bool = True) -> Dict[str, PytestExecutionReport]:
        """按类别运行测试"""
        categories = {
            "lottery": ["lottery"],
            "betting": ["betting"],
            "payout": ["payout"],
            "risk": ["risk"],
            "data": ["data"],
            "integration": ["integration"],
            "performance": ["performance"]
        }
        
        results = {}
        
        for category, markers in categories.items():
            logger.info(f"运行 {category} 类别的测试...")
            try:
                report = self.run_pytest_with_collection(
                    markers=markers,
                    enable_coverage=enable_coverage
                )
                results[category] = report
                logger.info(f"{category} 测试完成: {report.passed_tests}/{report.total_tests} 通过")
            except Exception as e:
                logger.error(f"{category} 测试执行失败: {e}")
                results[category] = self._create_empty_report(f"执行失败: {e}")
                
        return results
        
    def _setup_coverage(self):
        """设置覆盖率分析"""
        try:
            self.coverage_analyzer = coverage.Coverage(
                source=[str(self.root_path)],
                omit=[
                    "*/venv/*",
                    "*/test*",
                    "*/__pycache__/*",
                    "*/migrations/*"
                ]
            )
            self.coverage_analyzer.start()
            logger.info("覆盖率分析已启动")
        except Exception as e:
            logger.warning(f"无法启动覆盖率分析: {e}")
            
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
        
    def _create_execution_report(self, 
                               results: List[PytestTestResult],
                               coverage_data: Optional[Dict] = None,
                               command_args: Optional[List[str]] = None) -> PytestExecutionReport:
        """创建执行报告"""
        
        # 按文件分组测试结果
        suites_by_file = defaultdict(list)
        for result in results:
            suites_by_file[result.test_file].append(result)
            
        # 创建测试套件
        test_suites = []
        for test_file, file_results in suites_by_file.items():
            suite = self._create_suite_result(test_file, file_results)
            test_suites.append(suite)
            
        # 计算总体统计
        total_tests = len(results)
        passed_tests = sum(1 for r in results if r.status == 'passed')
        failed_tests = sum(1 for r in results if r.status == 'failed')
        error_tests = sum(1 for r in results if r.status == 'error')
        skipped_tests = sum(1 for r in results if r.status == 'skipped')
        xfail_tests = sum(1 for r in results if r.status == 'xfail')
        xpass_tests = sum(1 for r in results if r.status == 'xpass')
        
        total_duration = sum(r.duration for r in results)
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        # 环境信息
        env_info = {
            'python_version': sys.version,
            'pytest_version': pytest.__version__,
            'working_directory': str(self.root_path),
            'timestamp': datetime.now().isoformat()
        }
        
        # 性能指标
        performance_metrics = {
            'average_test_duration': total_duration / total_tests if total_tests > 0 else 0,
            'slowest_tests': sorted(results, key=lambda x: x.duration, reverse=True)[:5],
            'fastest_tests': sorted(results, key=lambda x: x.duration)[:5]
        }
        
        return PytestExecutionReport(
            report_id=f"pytest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            generation_time=datetime.now(),
            total_tests=total_tests,
            total_suites=len(test_suites),
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            error_tests=error_tests,
            skipped_tests=skipped_tests,
            xfail_tests=xfail_tests,
            xpass_tests=xpass_tests,
            total_duration=total_duration,
            success_rate=success_rate,
            test_suites=test_suites,
            coverage_data=coverage_data,
            performance_metrics=performance_metrics,
            environment_info=env_info,
            command_line_args=command_args
        )
        
    def _create_suite_result(self, test_file: str, results: List[PytestTestResult]) -> PytestSuiteResult:
        """创建测试套件结果"""
        
        passed_count = sum(1 for r in results if r.status == 'passed')
        failed_count = sum(1 for r in results if r.status == 'failed')
        error_count = sum(1 for r in results if r.status == 'error')
        skipped_count = sum(1 for r in results if r.status == 'skipped')
        xfail_count = sum(1 for r in results if r.status == 'xfail')
        xpass_count = sum(1 for r in results if r.status == 'xpass')
        
        total_duration = sum(r.duration for r in results)
        success_rate = (passed_count / len(results) * 100) if results else 0
        
        # 确定测试类型
        test_type = "unit"
        if any("integration" in r.markers for r in results):
            test_type = "integration"
        elif any("performance" in r.markers for r in results):
            test_type = "performance"
            
        return PytestSuiteResult(
            suite_name=Path(test_file).stem,
            test_file=test_file,
            test_type=test_type,
            tests=results,
            total_duration=total_duration,
            passed_count=passed_count,
            failed_count=failed_count,
            error_count=error_count,
            skipped_count=skipped_count,
            xfail_count=xfail_count,
            xpass_count=xpass_count,
            success_rate=success_rate,
            start_time=min(r.start_time for r in results if r.start_time) if results else None,
            end_time=max(r.end_time for r in results if r.end_time) if results else None
        )
        
    def _parse_subprocess_result(self, 
                               result: subprocess.CompletedProcess,
                               start_time: datetime,
                               end_time: datetime,
                               json_report_file: Optional[Path] = None) -> PytestExecutionReport:
        """解析子进程执行结果"""
        
        # 尝试从JSON报告文件读取结果
        if json_report_file and json_report_file.exists():
            try:
                with open(json_report_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                return self._parse_json_report(json_data)
            except Exception as e:
                logger.warning(f"无法解析JSON报告: {e}")
                
        # 解析标准输出
        return self._parse_text_output(result.stdout, result.stderr, start_time, end_time)
        
    def _parse_json_report(self, json_data: Dict) -> PytestExecutionReport:
        """解析JSON格式的pytest报告"""
        # 这里需要根据pytest-json-report插件的输出格式来解析
        # 由于格式可能变化，这里提供基本实现
        
        summary = json_data.get('summary', {})
        tests = json_data.get('tests', [])
        
        # 转换测试结果
        pytest_results = []
        for test in tests:
            pytest_results.append(PytestTestResult(
                test_id=test.get('nodeid', ''),
                test_name=test.get('name', ''),
                test_file=test.get('file', ''),
                test_class=test.get('class'),
                test_method=test.get('function', ''),
                status=test.get('outcome', 'unknown'),
                duration=test.get('duration', 0.0),
                error_message=test.get('message'),
                error_traceback=test.get('traceback'),
                markers=test.get('markers', [])
            ))
            
        return self._create_execution_report(pytest_results)
        
    def _parse_text_output(self, 
                         stdout: str, 
                         stderr: str,
                         start_time: datetime,
                         end_time: datetime) -> PytestExecutionReport:
        """解析文本输出"""
        
        # 简单的文本解析实现
        lines = stdout.split('\n')
        
        # 查找测试结果摘要
        passed = failed = error = skipped = 0
        duration = (end_time - start_time).total_seconds()
        
        for line in lines:
            if 'passed' in line and 'failed' in line:
                # 解析类似 "5 passed, 2 failed in 10.5s" 的行
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == 'passed' and i > 0:
                        passed = int(parts[i-1])
                    elif part == 'failed' and i > 0:
                        failed = int(parts[i-1])
                    elif part == 'error' and i > 0:
                        error = int(parts[i-1])
                    elif part == 'skipped' and i > 0:
                        skipped = int(parts[i-1])
                        
        total_tests = passed + failed + error + skipped
        success_rate = (passed / total_tests * 100) if total_tests > 0 else 0
        
        return PytestExecutionReport(
            report_id=f"pytest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            generation_time=datetime.now(),
            total_tests=total_tests,
            total_suites=0,
            passed_tests=passed,
            failed_tests=failed,
            error_tests=error,
            skipped_tests=skipped,
            xfail_tests=0,
            xpass_tests=0,
            total_duration=duration,
            success_rate=success_rate,
            test_suites=[],
            environment_info={
                'stdout': stdout,
                'stderr': stderr,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
        )
        
    def _create_empty_report(self, error_message: str = "") -> PytestExecutionReport:
        """创建空报告"""
        return PytestExecutionReport(
            report_id=f"pytest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            generation_time=datetime.now(),
            total_tests=0,
            total_suites=0,
            passed_tests=0,
            failed_tests=0,
            error_tests=0,
            skipped_tests=0,
            xfail_tests=0,
            xpass_tests=0,
            total_duration=0.0,
            success_rate=0.0,
            test_suites=[],
            environment_info={'error': error_message}
        )
        
    def generate_detailed_report(self, report: PytestExecutionReport) -> Dict[str, str]:
        """生成详细报告"""
        
        reports = {}
        
        # JSON报告
        json_file = self.output_dir / f"{report.report_id}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(report), f, indent=2, ensure_ascii=False, default=str)
        reports['json'] = str(json_file)
        
        # HTML报告
        html_file = self.output_dir / f"{report.report_id}.html"
        html_content = self._generate_html_report(report)
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        reports['html'] = str(html_file)
        
        # Markdown报告
        md_file = self.output_dir / f"{report.report_id}.md"
        md_content = self._generate_markdown_report(report)
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(md_content)
        reports['markdown'] = str(md_file)
        
        return reports
        
    def _generate_html_report(self, report: PytestExecutionReport) -> str:
        """生成HTML报告"""
        
        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PC28 Pytest执行报告 - {report.report_id}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #f5f5f5; padding: 20px; border-radius: 5px; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .metric {{ background: white; padding: 15px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .passed {{ color: #28a745; }}
        .failed {{ color: #dc3545; }}
        .error {{ color: #fd7e14; }}
        .skipped {{ color: #6c757d; }}
        .suite {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .test-result {{ margin: 10px 0; padding: 10px; border-left: 4px solid #ddd; }}
        .test-result.passed {{ border-left-color: #28a745; }}
        .test-result.failed {{ border-left-color: #dc3545; }}
        .test-result.error {{ border-left-color: #fd7e14; }}
        .test-result.skipped {{ border-left-color: #6c757d; }}
        .error-details {{ background: #f8f9fa; padding: 10px; margin: 10px 0; border-radius: 3px; }}
        pre {{ white-space: pre-wrap; word-wrap: break-word; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>PC28 Pytest执行报告</h1>
        <p><strong>报告ID:</strong> {report.report_id}</p>
        <p><strong>生成时间:</strong> {report.generation_time}</p>
        <p><strong>总执行时间:</strong> {report.total_duration:.2f}秒</p>
    </div>
    
    <div class="summary">
        <div class="metric">
            <h3>总测试数</h3>
            <div style="font-size: 2em; font-weight: bold;">{report.total_tests}</div>
        </div>
        <div class="metric">
            <h3 class="passed">通过</h3>
            <div style="font-size: 2em; font-weight: bold; color: #28a745;">{report.passed_tests}</div>
        </div>
        <div class="metric">
            <h3 class="failed">失败</h3>
            <div style="font-size: 2em; font-weight: bold; color: #dc3545;">{report.failed_tests}</div>
        </div>
        <div class="metric">
            <h3 class="error">错误</h3>
            <div style="font-size: 2em; font-weight: bold; color: #fd7e14;">{report.error_tests}</div>
        </div>
        <div class="metric">
            <h3 class="skipped">跳过</h3>
            <div style="font-size: 2em; font-weight: bold; color: #6c757d;">{report.skipped_tests}</div>
        </div>
        <div class="metric">
            <h3>成功率</h3>
            <div style="font-size: 2em; font-weight: bold;">{report.success_rate:.1f}%</div>
        </div>
    </div>
"""
        
        # 添加测试套件详情
        for suite in report.test_suites:
            html += f"""
    <div class="suite">
        <h2>{suite.suite_name}</h2>
        <p><strong>文件:</strong> {suite.test_file}</p>
        <p><strong>类型:</strong> {suite.test_type}</p>
        <p><strong>执行时间:</strong> {suite.total_duration:.2f}秒</p>
        <p><strong>成功率:</strong> {suite.success_rate:.1f}%</p>
        
        <h3>测试结果</h3>
"""
            
            for test in suite.tests:
                html += f"""
        <div class="test-result {test.status}">
            <h4>{test.test_name}</h4>
            <p><strong>状态:</strong> <span class="{test.status}">{test.status.upper()}</span></p>
            <p><strong>执行时间:</strong> {test.duration:.3f}秒</p>
            <p><strong>标记:</strong> {', '.join(test.markers) if test.markers else '无'}</p>
"""
                
                if test.error_message:
                    html += f"""
            <div class="error-details">
                <strong>错误信息:</strong>
                <pre>{test.error_message}</pre>
"""
                    if test.error_traceback:
                        html += f"""
                <strong>错误堆栈:</strong>
                <pre>{test.error_traceback}</pre>
"""
                    html += "</div>"
                    
                html += "</div>"
                
            html += "</div>"
            
        html += """
</body>
</html>
"""
        return html
        
    def _generate_markdown_report(self, report: PytestExecutionReport) -> str:
        """生成Markdown报告"""
        
        md = f"""# PC28 Pytest执行报告

## 基本信息

- **报告ID**: {report.report_id}
- **生成时间**: {report.generation_time}
- **总执行时间**: {report.total_duration:.2f}秒
- **测试套件数**: {report.total_suites}

## 执行摘要

| 指标 | 数量 | 百分比 |
|------|------|--------|
| 总测试数 | {report.total_tests} | 100% |
| 通过 | {report.passed_tests} | {(report.passed_tests/report.total_tests*100):.1f}% |
| 失败 | {report.failed_tests} | {(report.failed_tests/report.total_tests*100):.1f}% |
| 错误 | {report.error_tests} | {(report.error_tests/report.total_tests*100):.1f}% |
| 跳过 | {report.skipped_tests} | {(report.skipped_tests/report.total_tests*100):.1f}% |
| **成功率** | **{report.success_rate:.1f}%** | - |

## 测试套件详情

"""
        
        for suite in report.test_suites:
            md += f"""### {suite.suite_name}

- **文件**: `{suite.test_file}`
- **类型**: {suite.test_type}
- **执行时间**: {suite.total_duration:.2f}秒
- **成功率**: {suite.success_rate:.1f}%
- **测试统计**: 通过 {suite.passed_count}, 失败 {suite.failed_count}, 错误 {suite.error_count}, 跳过 {suite.skipped_count}

#### 测试结果

| 测试名称 | 状态 | 执行时间 | 标记 |
|----------|------|----------|------|
"""
            
            for test in suite.tests:
                status_emoji = {
                    'passed': '✅',
                    'failed': '❌',
                    'error': '💥',
                    'skipped': '⏭️',
                    'xfail': '⚠️',
                    'xpass': '🎉'
                }.get(test.status, '❓')
                
                markers_str = ', '.join(test.markers) if test.markers else '-'
                md += f"| {test.test_name} | {status_emoji} {test.status} | {test.duration:.3f}s | {markers_str} |\n"
                
            # 添加失败测试的详细信息
            failed_tests = [t for t in suite.tests if t.status in ['failed', 'error']]
            if failed_tests:
                md += f"\n#### 失败测试详情\n\n"
                for test in failed_tests:
                    md += f"##### {test.test_name}\n\n"
                    if test.error_message:
                        md += f"**错误信息**: {test.error_message}\n\n"
                    if test.error_traceback:
                        md += f"**错误堆栈**:\n```\n{test.error_traceback}\n```\n\n"
                        
            md += "\n"
            
        # 添加性能指标
        if report.performance_metrics:
            md += "## 性能指标\n\n"
            metrics = report.performance_metrics
            md += f"- **平均测试执行时间**: {metrics.get('average_test_duration', 0):.3f}秒\n\n"
            
            if 'slowest_tests' in metrics:
                md += "### 最慢的测试\n\n"
                for i, test in enumerate(metrics['slowest_tests'][:5], 1):
                    md += f"{i}. {test.test_name}: {test.duration:.3f}秒\n"
                md += "\n"
                
        # 添加覆盖率信息
        if report.coverage_data:
            md += "## 代码覆盖率\n\n"
            coverage = report.coverage_data
            if 'totals' in coverage:
                totals = coverage['totals']
                md += f"- **总覆盖率**: {totals.get('percent_covered', 0):.1f}%\n"
                md += f"- **覆盖行数**: {totals.get('covered_lines', 0)}\n"
                md += f"- **总行数**: {totals.get('num_statements', 0)}\n\n"
                
        return md

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PC28自动化Pytest执行器')
    parser.add_argument('--root', default='.', help='项目根目录')
    parser.add_argument('--output', default='pytest_reports', help='输出目录')
    parser.add_argument('--markers', nargs='*', help='测试标记过滤')
    parser.add_argument('--no-coverage', action='store_true', help='禁用覆盖率')
    parser.add_argument('--category', help='按类别运行测试')
    parser.add_argument('--subprocess', action='store_true', help='使用子进程模式')
    parser.add_argument('--verbose', action='store_true', help='详细输出')
    
    args = parser.parse_args()
    
    # 创建执行器
    executor = AutomatedPytestExecutor(args.root, args.output)
    
    try:
        if args.category:
            # 按类别运行
            reports = executor.run_tests_by_category(enable_coverage=not args.no_coverage)
            for category, report in reports.items():
                logger.info(f"{category} 测试结果: {report.passed_tests}/{report.total_tests} 通过")
                executor.generate_detailed_report(report)
        else:
            # 发现测试文件
            test_files = executor.discover_test_files()
            
            if args.subprocess:
                # 子进程模式
                report = executor.run_pytest_subprocess(
                    test_files=test_files,
                    markers=args.markers,
                    enable_coverage=not args.no_coverage,
                    verbose=args.verbose
                )
            else:
                # 直接模式
                report = executor.run_pytest_with_collection(
                    test_files=test_files,
                    markers=args.markers,
                    enable_coverage=not args.no_coverage,
                    verbose=args.verbose
                )
                
            # 生成报告
            report_files = executor.generate_detailed_report(report)
            
            # 输出结果
            logger.info(f"测试执行完成!")
            logger.info(f"总测试数: {report.total_tests}")
            logger.info(f"通过: {report.passed_tests}")
            logger.info(f"失败: {report.failed_tests}")
            logger.info(f"错误: {report.error_tests}")
            logger.info(f"跳过: {report.skipped_tests}")
            logger.info(f"成功率: {report.success_rate:.1f}%")
            logger.info(f"总执行时间: {report.total_duration:.2f}秒")
            
            logger.info("生成的报告文件:")
            for format_type, file_path in report_files.items():
                logger.info(f"  {format_type.upper()}: {file_path}")
                
    except Exception as e:
        logger.error(f"执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()