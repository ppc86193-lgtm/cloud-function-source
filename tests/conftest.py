import pytest
import logging
import json
from datetime import datetime
from pathlib import Path

# 配置日志
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'result.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def log_test_info(request):
    """记录每个测试的信息"""
    logger.info(f"Starting test: {request.node.name}")
    yield
    logger.info(f"Finished test: {request.node.name}")

def pytest_sessionstart(session):
    """测试会话开始时的钩子"""
    logger.info("="*50)
    logger.info(f"Pytest session started at {datetime.now()}")
    logger.info(f"Test directory: {session.config.rootdir}")
    logger.info("="*50)

def pytest_sessionfinish(session, exitstatus):
    """测试会话结束时的钩子"""
    logger.info("="*50)
    logger.info(f"Pytest session finished at {datetime.now()}")
    logger.info(f"Exit status: {exitstatus}")
    logger.info("="*50)
