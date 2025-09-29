#!/usr/bin/env python3
"""
Pytest配置和日志设置
根据PROJECT_RULES.md要求配置测试环境
"""

import logging
import os
import json
import datetime
from pathlib import Path

# 创建logs目录
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# 配置日志级别和格式
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logging():
    """
    配置测试日志系统
    - 输出到文件result.log供审计
    - 输出到控制台便于实时查看
    - 记录所有级别的日志
    """
    # 日志文件路径
    log_file = LOGS_DIR / "result.log"
    
    # 清除现有的处理器
    logger = logging.getLogger()
    logger.handlers.clear()
    
    # 设置日志级别为DEBUG，记录所有日志
    logger.setLevel(logging.DEBUG)
    
    # 文件处理器 - 记录所有日志到result.log
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    file_handler.setFormatter(file_formatter)
    
    # 控制台处理器 - 显示INFO及以上级别
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 记录测试开始
    logger.info("="*60)
    logger.info(f"测试开始时间: {datetime.datetime.now()}")
    logger.info(f"日志文件: {log_file}")
    logger.info("="*60)
    
    return logger

def log_test_metadata():
    """
    记录测试环境元数据
    """
    logger = logging.getLogger(__name__)
    
    metadata = {
        "python_version": os.sys.version,
        "platform": os.sys.platform,
        "cwd": os.getcwd(),
        "user": os.environ.get("USER", "unknown"),
        "timestamp": datetime.datetime.now().isoformat(),
        "test_directory": str(Path(__file__).parent),
        "log_level": logging.getLevelName(logger.level)
    }
    
    logger.info("测试环境元数据:")
    for key, value in metadata.items():
        logger.info(f"  {key}: {value}")
    
    # 保存元数据到JSON文件
    metadata_file = LOGS_DIR / "test_metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    return metadata

def validate_test_structure():
    """
    验证测试目录结构是否符合规范
    """
    logger = logging.getLogger(__name__)
    tests_dir = Path(__file__).parent
    
    # 检查测试文件命名规范
    test_files = list(tests_dir.glob("test_*.py"))
    logger.info(f"发现 {len(test_files)} 个测试文件")
    
    for test_file in test_files:
        logger.debug(f"  - {test_file.name}")
    
    # 检查测试目录结构
    required_dirs = ["unit", "integration", "e2e"]
    for dir_name in required_dirs:
        dir_path = tests_dir / dir_name
        if dir_path.exists():
            logger.info(f"✓ {dir_name}/ 目录存在")
        else:
            logger.warning(f"✗ {dir_name}/ 目录不存在")
    
    return test_files

# 自动初始化日志系统
if __name__ != "__main__":
    logger = setup_logging()
    log_test_metadata()
    validate_test_structure()

if __name__ == "__main__":
    # 测试日志配置
    logger = setup_logging()
    logger.debug("DEBUG级别日志测试")
    logger.info("INFO级别日志测试")
    logger.warning("WARNING级别日志测试")
    logger.error("ERROR级别日志测试")
    logger.critical("CRITICAL级别日志测试")
    
    print(f"\n日志已保存到: {LOGS_DIR / 'result.log'}")
    print("测试配置完成！")