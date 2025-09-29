#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统pytest执行管理器
统一管理pytest测试的执行、结果捕获和报告生成
"""

import os
import sys
import json
import time
import logging
import subprocess
import pytest
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict
from collections import defaultdict
import traceback
import argparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ExecutionStep:
    """执行步骤"""
    step_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: float = 0.0
    status: str = "running"  # running, completed, failed
    output: str = ""
    error: Optional[str] = None
    
    def complete(self, status: str = "completed", error: str = None):
        """完成步骤"""
        self.end_time = datetime.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = status
        if error:
            self.error = error

@dataclass
class TestExecutionResult:
    """测试执行结果"""
    test_id: str
    test_name: str
    test_file: str
    status: str  # passed, failed, error, skipped
    duration: float
    steps: List[ExecutionStep]
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    markers: List[str] = None
    
    def __post_init__(self):
        if self.markers is None:
            self.markers = []
        if self.steps is None:
            self.steps = []

@dataclass
class ExecutionReport:
    """执行报告"""
    report_id: str
    start_time: datetime
    end_time: Optional[datetime]
    total_duration: float
    command_executed: str
    exit_code: int
    total_tests: int
    passed_tests: int
    failed_tests: int
    error_tests: int
    skipped_tests: int
    success_rate: float
    test_results: List[TestExecutionResult]
    execution_steps: List[ExecutionStep]
    environment_info: Dict[str, Any]
    performance_metrics: Dict[str, Any]

class PytestExecutionManager:
    """Pytest执行管理器"""
    
    def __init__(self, root_path: str = ".", output_dir: str = "pytest_execution_reports"):
        self.root_path = Path(root_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # 创建按日期分组的子目录结构
        self.today_dir = self.output_dir / datetime.now().strftime('%Y-%m-%d')
        self.today_dir.mkdir(exist_ok=True)
        
        # 创建归档目录
        self.archive_dir = self.output_dir / "archive"
        self.archive_dir.mkdir(exist_ok=True)
        
        self.execution_steps = []
        self.current_step = None
        
    def execute_pytest_with_capture(self, 
                                  test_paths: List[str] = None,
                                  markers: List[str] = None,
                                  verbose: bool = True,
                                  capture_output: bool = True,
                                  timeout: int = 3600) -> ExecutionReport:
        """执行pytest并捕获详细结果"""
        
        report_id = f"pytest_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        
        logger.info(f"开始pytest执行 - 报告ID: {report_id}")
        
        # 步骤1: 准备执行环境
        self._start_step("准备执行环境")
        try:
            env_info = self._prepare_environment()
            self._complete_step("completed")
        except Exception as e:
            self._complete_step("failed", str(e))
            raise
            
        # 步骤2: 构建pytest命令
        self._start_step("构建pytest命令")
        try:
            cmd = self._build_pytest_command(test_paths, markers, verbose, capture_output)
            command_str = " ".join(cmd)
            logger.info(f"执行命令: {command_str}")
            self._complete_step("completed")
        except Exception as e:
            self._complete_step("failed", str(e))
            raise
            
        # 步骤3: 执行pytest
        self._start_step("执行pytest测试")
        try:
            result = self._execute_pytest_subprocess(cmd, timeout)
            self._complete_step("completed")
        except Exception as e:
            self._complete_step("failed", str(e))
            raise
            
        # 步骤4: 解析测试结果
        self._start_step("解析测试结果")
        try:
            test_results, summary = self._parse_pytest_output(result.stdout, result.stderr)
            self._complete_step("completed")
        except Exception as e:
            self._complete_step("failed", str(e))
            # 即使解析失败，也要继续生成基本报告
            test_results, summary = [], {}
            
        # 步骤5: 生成性能指标
        self._start_step("生成性能指标")
        try:
            performance_metrics = self._calculate_performance_metrics(test_results)
            self._complete_step("completed")
        except Exception as e:
            self._complete_step("failed", str(e))
            performance_metrics = {}
            
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        # 创建执行报告
        report = ExecutionReport(
            report_id=report_id,
            start_time=start_time,
            end_time=end_time,
            total_duration=total_duration,
            command_executed=command_str,
            exit_code=result.returncode,
            total_tests=summary.get('total', len(test_results)),
            passed_tests=summary.get('passed', 0),
            failed_tests=summary.get('failed', 0),
            error_tests=summary.get('error', 0),
            skipped_tests=summary.get('skipped', 0),
            success_rate=summary.get('success_rate', 0),
            test_results=test_results,
            execution_steps=self.execution_steps.copy(),
            environment_info=env_info,
            performance_metrics=performance_metrics
        )
        
        logger.info(f"pytest执行完成 - 耗时: {total_duration:.2f}秒")
        logger.info(f"测试结果: {report.passed_tests}/{report.total_tests} 通过 ({report.success_rate:.1f}%)")
        
        return report
        
    def execute_tests_by_category(self, 
                                categories: List[str] = None,
                                enable_detailed_capture: bool = True) -> Dict[str, ExecutionReport]:
        """按类别执行测试"""
        
        if categories is None:
            categories = ['lottery', 'betting', 'payout', 'risk', 'data', 'integration']
            
        category_reports = {}
        
        for category in categories:
            logger.info(f"执行 {category} 类别测试...")
            
            try:
                # 根据类别确定测试路径和标记
                test_paths, markers = self._get_category_config(category)
                
                # 执行测试
                report = self.execute_pytest_with_capture(
                    test_paths=test_paths,
                    markers=markers,
                    verbose=True,
                    capture_output=enable_detailed_capture
                )
                
                category_reports[category] = report
                
                logger.info(f"{category} 测试完成: {report.passed_tests}/{report.total_tests} 通过")
                
            except Exception as e:
                logger.error(f"{category} 测试执行失败: {e}")
                # 创建错误报告
                category_reports[category] = self._create_error_report(category, str(e))
                
        return category_reports
        
    def generate_execution_reports(self, report: ExecutionReport) -> Dict[str, str]:
        """生成执行报告 - 确保不覆盖历史记录"""
        
        report_files = {}
        
        # 使用今日目录存储报告，确保不覆盖
        base_dir = self.today_dir
        
        # JSON报告 - 包含完整执行数据
        json_file = base_dir / f"{report.report_id}.json"
        self._save_json_report(report, json_file)
        report_files['json'] = str(json_file)
        
        # 详细文本报告 - 人类可读格式
        txt_file = base_dir / f"{report.report_id}_detailed.txt"
        self._generate_text_report(report, txt_file)
        report_files['text'] = str(txt_file)
        
        # HTML报告 - 可视化展示
        html_file = base_dir / f"{report.report_id}.html"
        self._generate_html_report(report, html_file)
        report_files['html'] = str(html_file)
        
        # CSV报告 - 测试结果数据
        csv_file = base_dir / f"{report.report_id}_results.csv"
        self._generate_csv_report(report, csv_file)
        report_files['csv'] = str(csv_file)
        
        # 生成执行摘要报告
        summary_file = base_dir / f"{report.report_id}_summary.md"
        self._generate_summary_report(report, summary_file)
        report_files['summary'] = str(summary_file)
        
        # 如果有错误，生成专门的错误报告
        if report.failed_tests > 0 or report.error_tests > 0:
            error_file = base_dir / f"{report.report_id}_errors.log"
            self._generate_error_report_file(report, error_file)
            report_files['errors'] = str(error_file)
        
        logger.info(f"报告已保存到目录: {base_dir}")
        logger.info(f"生成的报告文件: {list(report_files.keys())}")
        
        return report_files
        
    def _start_step(self, step_name: str):
        """开始执行步骤"""
        self.current_step = ExecutionStep(
            step_name=step_name,
            start_time=datetime.now()
        )
        self.execution_steps.append(self.current_step)
        logger.info(f"开始步骤: {step_name}")
        
    def _complete_step(self, status: str = "completed", error: str = None):
        """完成当前步骤"""
        if self.current_step:
            self.current_step.complete(status, error)
            logger.info(f"完成步骤: {self.current_step.step_name} - {status} ({self.current_step.duration:.2f}s)")
            if error:
                logger.error(f"步骤错误: {error}")
            self.current_step = None
            
    def _prepare_environment(self) -> Dict[str, Any]:
        """准备执行环境"""
        import platform
        
        env_info = {
            'python_version': sys.version,
            'pytest_version': pytest.__version__,
            'platform': platform.platform(),
            'working_directory': str(self.root_path),
            'timestamp': datetime.now().isoformat(),
            'environment_variables': {
                key: value for key, value in os.environ.items()
                if key.startswith(('PYTEST_', 'TEST_', 'PC28_'))
            }
        }
        
        # 检查pytest.ini配置
        pytest_ini = self.root_path / "pytest.ini"
        if pytest_ini.exists():
            env_info['pytest_config'] = str(pytest_ini)
            
        # 检查测试目录
        test_dirs = []
        for test_dir in ['pc28_business_logic_tests', 'tests', 'test']:
            test_path = self.root_path / test_dir
            if test_path.exists():
                test_dirs.append(str(test_path))
        env_info['test_directories'] = test_dirs
        
        return env_info
        
    def _build_pytest_command(self, 
                            test_paths: List[str] = None,
                            markers: List[str] = None,
                            verbose: bool = True,
                            capture_output: bool = True) -> List[str]:
        """构建pytest命令"""
        
        cmd = [sys.executable, "-m", "pytest"]
        
        # 添加测试路径
        if test_paths:
            cmd.extend(test_paths)
        else:
            # 默认测试路径
            if (self.root_path / "pc28_business_logic_tests").exists():
                cmd.append("pc28_business_logic_tests/")
            cmd.extend(["test_*.py"])
            
        # 添加标记过滤
        if markers:
            for marker in markers:
                cmd.extend(["-m", marker])
                
        # 输出选项
        if verbose:
            cmd.append("-v")
            
        if not capture_output:
            cmd.append("-s")
            
        # 其他有用选项
        cmd.extend([
            "--tb=short",  # 简短traceback
            "--strict-markers",  # 严格标记
            "--disable-warnings",  # 禁用警告
        ])
        
        return cmd
        
    def _execute_pytest_subprocess(self, cmd: List[str], timeout: int) -> subprocess.CompletedProcess:
        """执行pytest子进程"""
        
        logger.info(f"执行命令: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.root_path,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            logger.info(f"命令执行完成，退出码: {result.returncode}")
            
            return result
            
        except subprocess.TimeoutExpired as e:
            logger.error(f"命令执行超时 ({timeout}秒)")
            raise
        except Exception as e:
            logger.error(f"命令执行失败: {e}")
            raise
            
    def _parse_pytest_output(self, stdout: str, stderr: str) -> Tuple[List[TestExecutionResult], Dict[str, Any]]:
        """解析pytest输出"""
        
        test_results = []
        summary = {}
        
        lines = stdout.split('\n')
        
        # 解析测试结果行
        test_pattern = re.compile(r'^(.+?)::(.*?)\s+(PASSED|FAILED|ERROR|SKIPPED|XFAIL|XPASS).*?\[.*?\]$')
        
        for line in lines:
            match = test_pattern.match(line.strip())
            if match:
                test_file = match.group(1)
                test_name = match.group(2)
                status = match.group(3).lower()
                
                # 提取执行时间（如果有）
                duration = 0.0
                time_match = re.search(r'\[([\d.]+)s\]', line)
                if time_match:
                    duration = float(time_match.group(1))
                    
                test_result = TestExecutionResult(
                    test_id=f"{test_file}::{test_name}",
                    test_name=test_name,
                    test_file=test_file,
                    status=status,
                    duration=duration,
                    steps=[]
                )
                
                test_results.append(test_result)
                
        # 解析摘要信息
        summary_pattern = re.compile(r'=+ (.+) =+')
        for line in lines:
            if 'passed' in line and ('failed' in line or 'error' in line or 'skipped' in line):
                # 解析类似 "5 passed, 2 failed, 1 skipped in 10.5s" 的行
                parts = line.split()
                summary_data = {}
                
                for i, part in enumerate(parts):
                    if part in ['passed', 'failed', 'error', 'skipped'] and i > 0:
                        try:
                            count = int(parts[i-1])
                            summary_data[part] = count
                        except (ValueError, IndexError):
                            pass
                            
                # 计算总数和成功率
                total = sum(summary_data.values())
                passed = summary_data.get('passed', 0)
                success_rate = (passed / total * 100) if total > 0 else 0
                
                summary = {
                    'total': total,
                    'passed': passed,
                    'failed': summary_data.get('failed', 0),
                    'error': summary_data.get('error', 0),
                    'skipped': summary_data.get('skipped', 0),
                    'success_rate': success_rate
                }
                break
                
        # 如果没有找到摘要，从测试结果计算
        if not summary and test_results:
            status_counts = defaultdict(int)
            for result in test_results:
                status_counts[result.status] += 1
                
            total = len(test_results)
            passed = status_counts.get('passed', 0)
            
            summary = {
                'total': total,
                'passed': passed,
                'failed': status_counts.get('failed', 0),
                'error': status_counts.get('error', 0),
                'skipped': status_counts.get('skipped', 0),
                'success_rate': (passed / total * 100) if total > 0 else 0
            }
            
        return test_results, summary
        
    def _calculate_performance_metrics(self, test_results: List[TestExecutionResult]) -> Dict[str, Any]:
        """计算性能指标"""
        
        if not test_results:
            return {}
            
        durations = [r.duration for r in test_results if r.duration > 0]
        
        if not durations:
            return {}
            
        metrics = {
            'total_test_time': sum(durations),
            'average_test_time': sum(durations) / len(durations),
            'max_test_time': max(durations),
            'min_test_time': min(durations),
            'slowest_tests': sorted(test_results, key=lambda x: x.duration, reverse=True)[:5],
            'fastest_tests': sorted(test_results, key=lambda x: x.duration)[:5]
        }
        
        # 按状态分组的性能
        status_performance = defaultdict(list)
        for result in test_results:
            if result.duration > 0:
                status_performance[result.status].append(result.duration)
                
        for status, times in status_performance.items():
            if times:
                metrics[f'{status}_avg_time'] = sum(times) / len(times)
                metrics[f'{status}_max_time'] = max(times)
                
        return metrics
        
    def _get_category_config(self, category: str) -> Tuple[List[str], List[str]]:
        """获取类别配置"""
        
        category_configs = {
            'lottery': {
                'paths': ['pc28_business_logic_tests/lottery_logic/'],
                'markers': ['lottery']
            },
            'betting': {
                'paths': ['pc28_business_logic_tests/betting_logic/'],
                'markers': ['betting']
            },
            'payout': {
                'paths': ['pc28_business_logic_tests/payout_logic/'],
                'markers': ['payout']
            },
            'risk': {
                'paths': ['pc28_business_logic_tests/risk_management/'],
                'markers': ['risk']
            },
            'data': {
                'paths': ['pc28_business_logic_tests/data_processing/'],
                'markers': ['data']
            },
            'integration': {
                'paths': ['pc28_business_logic_tests/integration_logic/'],
                'markers': ['integration']
            }
        }
        
        config = category_configs.get(category, {'paths': [], 'markers': []})
        return config['paths'], config['markers']
        
    def _create_error_report(self, category: str, error_message: str) -> ExecutionReport:
        """创建错误报告"""
        
        return ExecutionReport(
            report_id=f"error_{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            start_time=datetime.now(),
            end_time=datetime.now(),
            total_duration=0.0,
            command_executed="N/A",
            exit_code=-1,
            total_tests=0,
            passed_tests=0,
            failed_tests=0,
            error_tests=1,
            skipped_tests=0,
            success_rate=0.0,
            test_results=[],
            execution_steps=[],
            environment_info={'error': error_message},
            performance_metrics={}
        )
        
    def _save_json_report(self, report: ExecutionReport, file_path: Path):
        """保存JSON报告"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(report), f, indent=2, ensure_ascii=False, default=str)
            
    def _generate_text_report(self, report: ExecutionReport, file_path: Path):
        """生成详细文本报告"""
        
        content = f"""PC28 Pytest执行详细报告
{'='*50}

基本信息:
- 报告ID: {report.report_id}
- 开始时间: {report.start_time}
- 结束时间: {report.end_time}
- 总执行时间: {report.total_duration:.2f}秒
- 执行命令: {report.command_executed}
- 退出码: {report.exit_code}

测试结果摘要:
- 总测试数: {report.total_tests}
- 通过: {report.passed_tests}
- 失败: {report.failed_tests}
- 错误: {report.error_tests}
- 跳过: {report.skipped_tests}
- 成功率: {report.success_rate:.1f}%

执行步骤:
"""
        
        for i, step in enumerate(report.execution_steps, 1):
            content += f"{i}. {step.step_name}\n"
            content += f"   状态: {step.status}\n"
            content += f"   耗时: {step.duration:.2f}秒\n"
            if step.error:
                content += f"   错误: {step.error}\n"
            content += "\n"
            
        if report.test_results:
            content += "详细测试结果:\n"
            content += "-" * 50 + "\n"
            
            for result in report.test_results:
                content += f"测试: {result.test_name}\n"
                content += f"文件: {result.test_file}\n"
                content += f"状态: {result.status.upper()}\n"
                content += f"耗时: {result.duration:.3f}秒\n"
                if result.error_message:
                    content += f"错误: {result.error_message}\n"
                content += "\n"
                
        # 性能指标
        if report.performance_metrics:
            content += "性能指标:\n"
            content += "-" * 30 + "\n"
            metrics = report.performance_metrics
            content += f"总测试时间: {metrics.get('total_test_time', 0):.2f}秒\n"
            content += f"平均测试时间: {metrics.get('average_test_time', 0):.3f}秒\n"
            content += f"最长测试时间: {metrics.get('max_test_time', 0):.3f}秒\n"
            content += f"最短测试时间: {metrics.get('min_test_time', 0):.3f}秒\n"
            
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
    def _generate_html_report(self, report: ExecutionReport, file_path: Path):
        """生成HTML报告"""
        
        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PC28 Pytest执行报告 - {report.report_id}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        .header {{ background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
        .metric {{ background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }}
        .metric h3 {{ margin: 0 0 10px 0; color: #333; }}
        .metric .value {{ font-size: 2em; font-weight: bold; }}
        .passed {{ color: #28a745; }}
        .failed {{ color: #dc3545; }}
        .error {{ color: #fd7e14; }}
        .skipped {{ color: #6c757d; }}
        .steps {{ margin: 20px 0; }}
        .step {{ background: #f8f9fa; padding: 10px; margin: 5px 0; border-radius: 5px; }}
        .step.completed {{ border-left: 4px solid #28a745; }}
        .step.failed {{ border-left: 4px solid #dc3545; }}
        .test-results {{ margin: 20px 0; }}
        .test-result {{ background: white; padding: 15px; margin: 10px 0; border-radius: 5px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .test-result.passed {{ border-left: 4px solid #28a745; }}
        .test-result.failed {{ border-left: 4px solid #dc3545; }}
        .test-result.error {{ border-left: 4px solid #fd7e14; }}
        .test-result.skipped {{ border-left: 4px solid #6c757d; }}
        .performance {{ background: #e9ecef; padding: 15px; border-radius: 5px; margin: 20px 0; }}
        pre {{ background: #f8f9fa; padding: 10px; border-radius: 3px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>PC28 Pytest执行报告</h1>
        <p><strong>报告ID:</strong> {report.report_id}</p>
        <p><strong>执行时间:</strong> {report.start_time} - {report.end_time}</p>
        <p><strong>总耗时:</strong> {report.total_duration:.2f}秒</p>
        <p><strong>执行命令:</strong> <code>{report.command_executed}</code></p>
        <p><strong>退出码:</strong> {report.exit_code}</p>
    </div>
    
    <div class="summary">
        <div class="metric">
            <h3>总测试数</h3>
            <div class="value">{report.total_tests}</div>
        </div>
        <div class="metric">
            <h3 class="passed">通过</h3>
            <div class="value passed">{report.passed_tests}</div>
        </div>
        <div class="metric">
            <h3 class="failed">失败</h3>
            <div class="value failed">{report.failed_tests}</div>
        </div>
        <div class="metric">
            <h3 class="error">错误</h3>
            <div class="value error">{report.error_tests}</div>
        </div>
        <div class="metric">
            <h3 class="skipped">跳过</h3>
            <div class="value skipped">{report.skipped_tests}</div>
        </div>
        <div class="metric">
            <h3>成功率</h3>
            <div class="value">{report.success_rate:.1f}%</div>
        </div>
    </div>
    
    <div class="steps">
        <h2>执行步骤</h2>
"""
        
        for i, step in enumerate(report.execution_steps, 1):
            html_content += f"""
        <div class="step {step.status}">
            <strong>{i}. {step.step_name}</strong>
            <span style="float: right;">
                {step.status.upper()} ({step.duration:.2f}s)
            </span>
            {f'<br><span style="color: #dc3545;">错误: {step.error}</span>' if step.error else ''}
        </div>
"""
        
        html_content += """
    </div>
    
    <div class="test-results">
        <h2>测试结果详情</h2>
"""
        
        for result in report.test_results:
            html_content += f"""
        <div class="test-result {result.status}">
            <h4>{result.test_name}</h4>
            <p><strong>文件:</strong> {result.test_file}</p>
            <p><strong>状态:</strong> <span class="{result.status}">{result.status.upper()}</span></p>
            <p><strong>耗时:</strong> {result.duration:.3f}秒</p>
            {f'<p><strong>错误:</strong> <pre>{result.error_message}</pre></p>' if result.error_message else ''}
        </div>
"""
        
        # 性能指标
        if report.performance_metrics:
            metrics = report.performance_metrics
            html_content += f"""
    </div>
    
    <div class="performance">
        <h2>性能指标</h2>
        <p><strong>总测试时间:</strong> {metrics.get('total_test_time', 0):.2f}秒</p>
        <p><strong>平均测试时间:</strong> {metrics.get('average_test_time', 0):.3f}秒</p>
        <p><strong>最长测试时间:</strong> {metrics.get('max_test_time', 0):.3f}秒</p>
        <p><strong>最短测试时间:</strong> {metrics.get('min_test_time', 0):.3f}秒</p>
    </div>
"""
        
        html_content += """
</body>
</html>
"""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
    def _generate_csv_report(self, report: ExecutionReport, file_path: Path):
        """生成CSV格式报告"""
        
        import csv
        
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # 写入标题行
            writer.writerow([
                'Test ID', 'Test Name', 'Test File', 'Status', 
                'Duration (s)', 'Error Message', 'Markers'
            ])
            
            # 写入测试结果
            for result in report.test_results:
                writer.writerow([
                    result.test_id,
                    result.test_name,
                    result.test_file,
                    result.status,
                    f"{result.duration:.3f}",
                    result.error_message or '',
                    ', '.join(result.markers or [])
                ])
                
        logger.info(f"CSV报告已保存: {file_path}")
    
    def _generate_summary_report(self, report: ExecutionReport, file_path: Path):
        """生成执行摘要报告"""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# Pytest执行摘要报告\n\n")
            f.write(f"**报告ID**: {report.report_id}\n")
            f.write(f"**执行时间**: {report.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**总耗时**: {report.total_duration:.2f}秒\n")
            f.write(f"**执行命令**: `{report.command_executed}`\n")
            f.write(f"**退出码**: {report.exit_code}\n\n")
            
            f.write(f"## 测试结果统计\n\n")
            f.write(f"- **总测试数**: {report.total_tests}\n")
            f.write(f"- **通过**: {report.passed_tests}\n")
            f.write(f"- **失败**: {report.failed_tests}\n")
            f.write(f"- **错误**: {report.error_tests}\n")
            f.write(f"- **跳过**: {report.skipped_tests}\n")
            f.write(f"- **成功率**: {report.success_rate:.1f}%\n\n")
            
            if report.failed_tests > 0 or report.error_tests > 0:
                f.write(f"## 失败测试详情\n\n")
                for result in report.test_results:
                    if result.status in ['failed', 'error']:
                        f.write(f"### {result.test_name}\n")
                        f.write(f"- **文件**: {result.test_file}\n")
                        f.write(f"- **状态**: {result.status}\n")
                        f.write(f"- **错误**: {result.error_message or 'N/A'}\n\n")
            
            f.write(f"## 执行步骤\n\n")
            for i, step in enumerate(report.execution_steps, 1):
                status_icon = "✅" if step.status == "completed" else "❌" if step.status == "failed" else "⏳"
                f.write(f"{i}. {status_icon} **{step.step_name}** ({step.duration:.3f}s)\n")
                if step.error:
                    f.write(f"   - 错误: {step.error}\n")
            
        logger.info(f"摘要报告已保存: {file_path}")
    
    def _generate_error_report_file(self, report: ExecutionReport, file_path: Path):
        """生成专门的错误报告文件"""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"Pytest执行错误报告 - {report.report_id}\n")
            f.write(f"{'='*60}\n\n")
            f.write(f"执行时间: {report.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"失败测试数: {report.failed_tests}\n")
            f.write(f"错误测试数: {report.error_tests}\n\n")
            
            for result in report.test_results:
                if result.status in ['failed', 'error']:
                    f.write(f"测试: {result.test_id}\n")
                    f.write(f"状态: {result.status.upper()}\n")
                    f.write(f"文件: {result.test_file}\n")
                    if result.error_message:
                        f.write(f"错误信息: {result.error_message}\n")
                    if result.error_traceback:
                        f.write(f"堆栈跟踪:\n{result.error_traceback}\n")
                    f.write(f"{'-'*40}\n\n")
            
            # 记录执行步骤中的错误
            f.write(f"执行步骤错误:\n")
            for step in report.execution_steps:
                if step.status == "failed" and step.error:
                    f.write(f"步骤: {step.step_name}\n")
                    f.write(f"错误: {step.error}\n")
                    f.write(f"{'-'*20}\n")
        
        logger.info(f"错误报告已保存: {file_path}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PC28 Pytest执行管理器')
    parser.add_argument('--root', default='.', help='项目根目录')
    parser.add_argument('--output', default='pytest_execution_reports', help='输出目录')
    parser.add_argument('--paths', nargs='*', help='测试路径')
    parser.add_argument('--markers', nargs='*', help='测试标记')
    parser.add_argument('--categories', nargs='*', help='测试类别')
    parser.add_argument('--timeout', type=int, default=3600, help='执行超时时间（秒）')
    parser.add_argument('--no-capture', action='store_true', help='不捕获输出')
    parser.add_argument('--quiet', action='store_true', help='静默模式')
    
    args = parser.parse_args()
    
    # 创建执行管理器
    manager = PytestExecutionManager(args.root, args.output)
    
    try:
        if args.categories:
            # 按类别执行
            logger.info(f"按类别执行测试: {args.categories}")
            category_reports = manager.execute_tests_by_category(
                categories=args.categories,
                enable_detailed_capture=not args.no_capture
            )
            
            # 生成每个类别的报告
            for category, report in category_reports.items():
                logger.info(f"生成 {category} 类别报告...")
                report_files = manager.generate_execution_reports(report)
                
                if not args.quiet:
                    logger.info(f"{category} 测试结果:")
                    logger.info(f"  通过: {report.passed_tests}/{report.total_tests}")
                    logger.info(f"  成功率: {report.success_rate:.1f}%")
                    logger.info(f"  耗时: {report.total_duration:.2f}秒")
                    logger.info(f"  报告文件: {report_files}")
                    
        else:
            # 直接执行
            logger.info("执行pytest测试...")
            report = manager.execute_pytest_with_capture(
                test_paths=args.paths,
                markers=args.markers,
                verbose=not args.quiet,
                capture_output=not args.no_capture,
                timeout=args.timeout
            )
            
            # 生成报告
            report_files = manager.generate_execution_reports(report)
            
            # 输出结果
            if not args.quiet:
                logger.info("测试执行完成!")
                logger.info(f"总测试数: {report.total_tests}")
                logger.info(f"通过: {report.passed_tests}")
                logger.info(f"失败: {report.failed_tests}")
                logger.info(f"错误: {report.error_tests}")
                logger.info(f"跳过: {report.skipped_tests}")
                logger.info(f"成功率: {report.success_rate:.1f}%")
                logger.info(f"总耗时: {report.total_duration:.2f}秒")
                
                logger.info("生成的报告文件:")
                for format_type, file_path in report_files.items():
                    logger.info(f"  {format_type.upper()}: {file_path}")
                    
    except Exception as e:
        logger.error(f"执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()