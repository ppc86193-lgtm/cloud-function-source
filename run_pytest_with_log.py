#!/usr/bin/env python3
"""
pytest执行脚本 - 确保每次代码提交时运行pytest并生成result.log
根据智能合约要求，每次提交必须包含pytest测试日志
"""

import subprocess
import sys
import os
from datetime import datetime
import logging

# 配置基础日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('result.log', mode='w', encoding='utf-8'),  # 覆盖模式
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_pytest():
    """
    运行pytest测试并生成完整日志
    """
    logger.info("="*80)
    logger.info(f"开始执行pytest自动化测试")
    logger.info(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"工作目录: {os.getcwd()}")
    logger.info("="*80)
    
    # pytest命令参数
    pytest_args = [
        'pytest',
        'tests/',  # 测试目录
        '-v',  # 详细输出
        '--tb=short',  # 简短的traceback格式
        '--strict-markers',  # 严格标记检查
        '--cov=.',  # 测试覆盖率
        '--cov-report=term-missing',  # 终端显示缺失覆盖
        '--cov-report=html',  # HTML覆盖率报告
        '--cov-report=json',  # JSON覆盖率报告
        '--html=pytest_report.html',  # HTML测试报告
        '--self-contained-html',  # 独立HTML文件
        '--json-report',  # JSON报告
        '--json-report-file=pytest_report.json',  # JSON报告文件
        '--junitxml=pytest_junit.xml',  # JUnit XML报告
        '--maxfail=10',  # 最多允许10个失败
        '--ignore=CHANGESETS',  # 忽略CHANGESETS目录
        '--ignore=BACKUPS',  # 忽略BACKUPS目录
        '--log-cli=true',  # 启用CLI日志
        '--log-cli-level=DEBUG',  # CLI日志级别
        '--log-file=result.log',  # 日志文件
        '--log-file-level=DEBUG'  # 文件日志级别
    ]
    
    logger.info(f"执行命令: {' '.join(pytest_args)}")
    logger.info("-"*80)
    
    try:
        # 执行pytest
        result = subprocess.run(
            pytest_args,
            capture_output=True,
            text=True,
            check=False  # 不抛出异常，手动处理返回码
        )
        
        # 记录输出
        if result.stdout:
            logger.info("标准输出:")
            logger.info(result.stdout)
        
        if result.stderr:
            logger.warning("错误输出:")
            logger.warning(result.stderr)
        
        # 记录结果
        logger.info("-"*80)
        logger.info(f"pytest执行完成")
        logger.info(f"退出码: {result.returncode}")
        
        # 根据返回码判断结果
        if result.returncode == 0:
            logger.info("✅ 所有测试通过")
        elif result.returncode == 1:
            logger.warning("⚠️ 部分测试失败")
        elif result.returncode == 2:
            logger.error("❌ 测试执行被中断")
        elif result.returncode == 3:
            logger.error("❌ 内部错误")
        elif result.returncode == 4:
            logger.error("❌ pytest命令行使用错误")
        elif result.returncode == 5:
            logger.error("❌ 未收集到任何测试")
        else:
            logger.error(f"❌ 未知错误，退出码: {result.returncode}")
        
        # 生成提交信息
        logger.info("="*80)
        logger.info("测试日志生成完成，可以提交到Git")
        logger.info("建议的Git提交命令:")
        logger.info("  git add result.log pytest_report.html pytest_report.json")
        logger.info("  git commit -m '测试: 执行pytest自动化测试并生成日志'")
        logger.info("="*80)
        
        return result.returncode
        
    except FileNotFoundError:
        logger.error("❌ 未找到pytest命令，请先安装: pip install pytest pytest-cov pytest-html pytest-json-report")
        return 1
    except Exception as e:
        logger.error(f"❌ 执行pytest时发生错误: {e}")
        return 1

def check_dependencies():
    """
    检查必要的依赖
    """
    required_packages = [
        'pytest',
        'pytest-cov',
        'pytest-html',
        'pytest-json-report'
    ]
    
    logger.info("检查依赖包...")
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            logger.info(f"  ✅ {package} 已安装")
        except ImportError:
            logger.warning(f"  ❌ {package} 未安装")
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"缺少必要的包: {', '.join(missing_packages)}")
        logger.info(f"请运行: pip install {' '.join(missing_packages)}")
        return False
    
    return True

if __name__ == '__main__':
    logger.info("🚀 pytest自动化测试脚本启动")
    logger.info("根据智能合约要求，每次代码提交必须包含pytest测试日志")
    
    # 检查依赖
    if not check_dependencies():
        sys.exit(1)
    
    # 运行pytest
    exit_code = run_pytest()
    
    # 确认日志文件已生成
    if os.path.exists('result.log'):
        file_size = os.path.getsize('result.log')
        logger.info(f"✅ result.log 已生成 (大小: {file_size} 字节)")
    else:
        logger.error("❌ result.log 未生成")
    
    sys.exit(exit_code)