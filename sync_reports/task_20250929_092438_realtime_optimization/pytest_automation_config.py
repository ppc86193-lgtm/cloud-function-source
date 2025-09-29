#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统pytest自动化测试配置
自动化运行测试并捕获每个步骤的执行结果日志
"""

import pytest
import logging
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

class TestResultCapture:
    """测试结果捕获器"""
    
    def __init__(self, output_dir: str = None):
        self.output_dir = Path(output_dir) if output_dir else Path("test_results")
        self.output_dir.mkdir(exist_ok=True)
        
        self.test_results = []
        self.current_test = None
        self.start_time = None
        
        # 配置日志
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志配置"""
        log_file = self.output_dir / f"test_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        
    def pytest_runtest_setup(self, item):
        """测试开始前的钩子"""
        self.current_test = {
            "test_name": item.name,
            "test_file": str(item.fspath),
            "start_time": datetime.now().isoformat(),
            "status": "running",
            "logs": [],
            "duration": 0,
            "error": None
        }
        
        self.logger.info(f"开始执行测试: {item.name}")
        self.start_time = time.time()
        
    def pytest_runtest_call(self, item):
        """测试执行期间的钩子"""
        if self.current_test:
            self.current_test["logs"].append({
                "timestamp": datetime.now().isoformat(),
                "level": "INFO",
                "message": f"执行测试函数: {item.function.__name__}"
            })
            
    def pytest_runtest_teardown(self, item):
        """测试结束后的钩子"""
        if self.current_test:
            self.current_test["duration"] = time.time() - self.start_time
            self.current_test["end_time"] = datetime.now().isoformat()
            
    def pytest_runtest_logreport(self, report):
        """测试报告钩子"""
        if not self.current_test:
            return
            
        if report.when == "call":
            if report.outcome == "passed":
                self.current_test["status"] = "passed"
                self.logger.info(f"测试通过: {self.current_test['test_name']}")
            elif report.outcome == "failed":
                self.current_test["status"] = "failed"
                self.current_test["error"] = str(report.longrepr)
                self.logger.error(f"测试失败: {self.current_test['test_name']} - {report.longrepr}")
            elif report.outcome == "skipped":
                self.current_test["status"] = "skipped"
                self.logger.warning(f"测试跳过: {self.current_test['test_name']}")
                
            # 保存当前测试结果
            self.test_results.append(self.current_test.copy())
            
    def pytest_sessionfinish(self, session):
        """测试会话结束钩子"""
        self.generate_final_report()
        
    def generate_final_report(self):
        """生成最终测试报告"""
        report = {
            "execution_time": datetime.now().isoformat(),
            "total_tests": len(self.test_results),
            "passed": len([t for t in self.test_results if t["status"] == "passed"]),
            "failed": len([t for t in self.test_results if t["status"] == "failed"]),
            "skipped": len([t for t in self.test_results if t["status"] == "skipped"]),
            "total_duration": sum(t["duration"] for t in self.test_results),
            "tests": self.test_results
        }
        
        # 保存JSON报告
        report_file = self.output_dir / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
            
        # 生成HTML报告
        self.generate_html_report(report)
        
        self.logger.info(f"测试报告已生成: {report_file}")
        
    def generate_html_report(self, report: Dict[str, Any]):
        """生成HTML格式的测试报告"""
        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PC28系统测试报告</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .metric {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; text-align: center; }}
        .test-item {{ border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; }}
        .passed {{ border-left: 5px solid #4CAF50; }}
        .failed {{ border-left: 5px solid #f44336; }}
        .skipped {{ border-left: 5px solid #ff9800; }}
        .logs {{ background-color: #f9f9f9; padding: 10px; margin: 10px 0; border-radius: 3px; }}
        .error {{ background-color: #ffebee; padding: 10px; margin: 10px 0; border-radius: 3px; color: #c62828; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>PC28系统自动化测试报告</h1>
        <p>执行时间: {report['execution_time']}</p>
        <p>总耗时: {report['total_duration']:.2f}秒</p>
    </div>
    
    <div class="summary">
        <div class="metric">
            <h3>总测试数</h3>
            <p>{report['total_tests']}</p>
        </div>
        <div class="metric">
            <h3>通过</h3>
            <p style="color: #4CAF50;">{report['passed']}</p>
        </div>
        <div class="metric">
            <h3>失败</h3>
            <p style="color: #f44336;">{report['failed']}</p>
        </div>
        <div class="metric">
            <h3>跳过</h3>
            <p style="color: #ff9800;">{report['skipped']}</p>
        </div>
        <div class="metric">
            <h3>成功率</h3>
            <p>{(report['passed'] / report['total_tests'] * 100):.1f}%</p>
        </div>
    </div>
    
    <h2>详细测试结果</h2>
"""
        
        for test in report['tests']:
            status_class = test['status']
            html_content += f"""
    <div class="test-item {status_class}">
        <h3>{test['test_name']}</h3>
        <p><strong>文件:</strong> {test['test_file']}</p>
        <p><strong>状态:</strong> {test['status'].upper()}</p>
        <p><strong>耗时:</strong> {test['duration']:.3f}秒</p>
        <p><strong>开始时间:</strong> {test['start_time']}</p>
        <p><strong>结束时间:</strong> {test.get('end_time', 'N/A')}</p>
"""
            
            if test.get('error'):
                html_content += f"""
        <div class="error">
            <strong>错误信息:</strong><br>
            <pre>{test['error']}</pre>
        </div>
"""
            
            if test.get('logs'):
                html_content += """
        <div class="logs">
            <strong>执行日志:</strong><br>
"""
                for log in test['logs']:
                    html_content += f"<p>[{log['timestamp']}] {log['level']}: {log['message']}</p>"
                    
                html_content += "</div>"
                
            html_content += "</div>"
            
        html_content += """
</body>
</html>
"""
        
        html_file = self.output_dir / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

class PC28TestRunner:
    """PC28系统测试运行器"""
    
    def __init__(self, test_dir: str = "tests", output_dir: str = None):
        self.test_dir = Path(test_dir)
        self.output_dir = Path(output_dir) if output_dir else Path("test_results")
        self.capture = TestResultCapture(str(self.output_dir))
        
    def run_all_tests(self, verbose: bool = True, capture_output: bool = True):
        """运行所有测试"""
        args = [
            str(self.test_dir),
            "-v" if verbose else "",
            "--tb=short",
            f"--html={self.output_dir}/pytest_report.html",
            "--self-contained-html"
        ]
        
        # 过滤空字符串
        args = [arg for arg in args if arg]
        
        if capture_output:
            # 注册插件
            pytest.main(args + ["-p", "no:cacheprovider"])
        else:
            pytest.main(args)
            
    def run_specific_tests(self, test_patterns: List[str], **kwargs):
        """运行特定的测试"""
        for pattern in test_patterns:
            test_files = list(self.test_dir.glob(pattern))
            for test_file in test_files:
                self.run_single_test(str(test_file), **kwargs)
                
    def run_single_test(self, test_file: str, verbose: bool = True):
        """运行单个测试文件"""
        args = [
            test_file,
            "-v" if verbose else "",
            "--tb=short"
        ]
        
        args = [arg for arg in args if arg]
        pytest.main(args)
        
    def run_with_markers(self, markers: List[str], **kwargs):
        """根据标记运行测试"""
        marker_expr = " or ".join(markers)
        args = [
            str(self.test_dir),
            "-m", marker_expr,
            "-v",
            "--tb=short"
        ]
        
        pytest.main(args)

def create_pytest_ini():
    """创建pytest配置文件"""
    pytest_ini_content = """
[tool:pytest]
testpaths = tests
python_files = test_*.py *_test.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
    --disable-warnings
markers =
    unit: 单元测试
    integration: 集成测试
    performance: 性能测试
    realtime: 实时系统测试
    backfill: 历史回填测试
    consistency: 数据一致性测试
    slow: 慢速测试
    fast: 快速测试
"""
    
    with open("pytest.ini", "w", encoding="utf-8") as f:
        f.write(pytest_ini_content.strip())

def create_conftest():
    """创建pytest配置文件"""
    conftest_content = '''
import pytest
import logging
from datetime import datetime
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(__file__))

@pytest.fixture(scope="session")
def test_config():
    """测试配置fixture"""
    return {
        "test_start_time": datetime.now(),
        "log_level": logging.INFO,
        "output_dir": "test_results"
    }

@pytest.fixture(scope="function")
def test_logger():
    """测试日志fixture"""
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """自动设置测试环境"""
    # 创建测试结果目录
    os.makedirs("test_results", exist_ok=True)
    
    # 设置测试日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    yield
    
    # 清理测试环境
    print("\\n测试环境清理完成")

@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    """pytest配置钩子"""
    # 注册自定义标记
    config.addinivalue_line("markers", "unit: 单元测试")
    config.addinivalue_line("markers", "integration: 集成测试")
    config.addinivalue_line("markers", "performance: 性能测试")
    config.addinivalue_line("markers", "realtime: 实时系统测试")
    config.addinivalue_line("markers", "backfill: 历史回填测试")
    config.addinivalue_line("markers", "consistency: 数据一致性测试")

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """测试运行前的设置"""
    print(f"\\n开始执行测试: {item.name}")

@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item, nextitem):
    """测试运行后的清理"""
    print(f"完成测试: {item.name}")
'''
    
    with open("conftest.py", "w", encoding="utf-8") as f:
        f.write(conftest_content.strip())

# 使用示例
if __name__ == "__main__":
    # 创建配置文件
    create_pytest_ini()
    create_conftest()
    
    # 创建测试运行器
    runner = PC28TestRunner(
        test_dir="tests",
        output_dir="sync_reports/task_20250929_092438_realtime_optimization/test_results"
    )
    
    print("PC28系统pytest自动化测试配置完成")
    print("使用方法:")
    print("1. python pytest_automation_config.py  # 创建配置文件")
    print("2. pytest tests/ -v --html=report.html  # 运行所有测试")
    print("3. pytest -m realtime  # 运行实时系统测试")
    print("4. pytest -m consistency  # 运行数据一致性测试")