#!/usr/bin/env python3
"""
增强的Pytest配置文件
集成日志系统和测试规范验证
"""

import pytest
import logging
import json
import datetime
import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 导入测试配置
from tests.pytest_config import setup_logging, log_test_metadata, validate_test_structure

# 配置日志
logger = setup_logging()

def pytest_configure(config):
    """
    pytest启动时的配置
    """
    logger.info("="*60)
    logger.info("PYTEST 测试会话开始")
    logger.info(f"命令行参数: {' '.join(sys.argv)}")
    logger.info("="*60)
    
    # 记录测试元数据
    log_test_metadata()
    
    # 验证测试结构
    validate_test_structure()
    
    # 添加自定义标记
    config.addinivalue_line(
        "markers", "unit: 单元测试"
    )
    config.addinivalue_line(
        "markers", "integration: 集成测试"
    )
    config.addinivalue_line(
        "markers", "e2e: 端到端测试"
    )
    config.addinivalue_line(
        "markers", "slow: 慢速测试"
    )

def pytest_collection_modifyitems(config, items):
    """
    测试收集完成后的处理
    """
    logger.info(f"收集到 {len(items)} 个测试用例")
    
    # 统计测试类型
    test_types = {"unit": 0, "integration": 0, "e2e": 0, "other": 0}
    
    for item in items:
        markers = [marker.name for marker in item.iter_markers()]
        if "unit" in markers:
            test_types["unit"] += 1
        elif "integration" in markers:
            test_types["integration"] += 1
        elif "e2e" in markers:
            test_types["e2e"] += 1
        else:
            test_types["other"] += 1
        
        # 记录每个测试
        logger.debug(f"  - {item.nodeid} [{', '.join(markers) or '无标记'}]")
    
    logger.info("测试类型统计:")
    for test_type, count in test_types.items():
        if count > 0:
            logger.info(f"  {test_type}: {count} 个")

@pytest.fixture(autouse=True)
def log_test_execution(request):
    """
    自动为每个测试添加日志
    """
    test_name = request.node.name
    test_file = request.node.parent.name if hasattr(request.node, 'parent') else 'unknown'
    
    logger.info("-" * 40)
    logger.info(f"开始执行测试: {test_name}")
    logger.info(f"测试文件: {test_file}")
    logger.info(f"测试路径: {request.node.nodeid}")
    
    start_time = datetime.datetime.now()
    
    yield
    
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"测试完成: {test_name}")
    logger.info(f"执行时间: {duration:.3f} 秒")
    logger.info("-" * 40)

@pytest.fixture
def test_logger():
    """
    为测试提供logger实例
    """
    return logging.getLogger("test")

def pytest_runtest_logreport(report):
    """
    记录测试结果
    """
    if report.when == "call":
        if report.passed:
            logger.info(f"✓ 测试通过: {report.nodeid}")
        elif report.failed:
            logger.error(f"✗ 测试失败: {report.nodeid}")
            if report.longrepr:
                logger.error(f"失败原因: {report.longreprtext}")
        elif report.skipped:
            logger.warning(f"⊘ 测试跳过: {report.nodeid}")

def pytest_sessionfinish(session, exitstatus):
    """
    测试会话结束时的处理
    """
    logger.info("="*60)
    logger.info("PYTEST 测试会话结束")
    logger.info(f"退出状态: {exitstatus}")
    logger.info(f"结束时间: {datetime.datetime.now()}")
    
    # 生成测试摘要
    if hasattr(session, 'testscollected'):
        logger.info(f"总共执行: {session.testscollected} 个测试")
    
    if exitstatus == 0:
        logger.info("✓ 所有测试通过")
    else:
        logger.error(f"✗ 测试失败，退出码: {exitstatus}")
    
    logger.info("="*60)
    
    # 确保日志被写入文件
    for handler in logger.handlers:
        handler.flush()

# 添加命令行选项
def pytest_addoption(parser):
    """
    添加自定义命令行选项
    """
    parser.addoption(
        "--log-to-file",
        action="store_true",
        default=True,
        help="将日志输出到result.log文件"
    )
    parser.addoption(
        "--strict-mode",
        action="store_true",
        default=False,
        help="严格模式：任何警告都视为错误"
    )

# 测试钩子
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    生成测试报告
    """
    outcome = yield
    report = outcome.get_result()
    
    # 将测试结果保存到item中
    if report.when == "call":
        item.test_result = report.outcome
        
        # 记录详细的测试结果
        result_data = {
            "test_name": item.name,
            "test_file": str(item.fspath),
            "outcome": report.outcome,
            "duration": report.duration,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        # 保存到JSON文件
        results_file = Path("logs") / "test_results.json"
        results = []
        if results_file.exists():
            with open(results_file, 'r') as f:
                try:
                    results = json.load(f)
                except json.JSONDecodeError:
                    results = []
        
        results.append(result_data)
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)

if __name__ == "__main__":
    print("这是pytest配置文件，应该通过pytest命令运行")
    print("使用方法: pytest --log-to-file")
    print("查看日志: cat logs/result.log")