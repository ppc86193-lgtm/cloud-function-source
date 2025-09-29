#!/bin/bash

# Git项目标准化初始化脚本
# 该脚本用于在新的Git项目上执行标准的pytest和审计配置

set -e  # 遇到错误立即退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Git项目标准化初始化工具${NC}"
echo -e "${GREEN}========================================${NC}"

# 检查是否在Git仓库中
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${YELLOW}当前目录不是Git仓库，正在初始化...${NC}"
    git init
    echo -e "${GREEN}✓ Git仓库已初始化${NC}"
else
    echo -e "${GREEN}✓ 检测到现有Git仓库${NC}"
fi

# 获取项目根目录
PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$PROJECT_ROOT"

echo -e "\n${YELLOW}开始配置项目标准...${NC}"

# 1. 创建必要的目录结构
echo -e "\n${YELLOW}[1/10] 创建目录结构...${NC}"
mkdir -p tests/{unit,integration,e2e}
mkdir -p logs
mkdir -p docs
mkdir -p .github/workflows
echo -e "${GREEN}✓ 目录结构已创建${NC}"

# 2. 复制或创建pytest配置文件
echo -e "\n${YELLOW}[2/10] 配置pytest...${NC}"
if [ -f "$(dirname "$0")/pytest.ini" ] && [ "$(dirname "$0")" != "." ]; then
    cp "$(dirname "$0")/pytest.ini" ./pytest.ini
elif [ ! -f "./pytest.ini" ]; then
    cat > pytest.ini << 'EOF'
[pytest]
# Pytest configuration file
addopts = 
    -v
    --strict-markers
    --tb=short
    --capture=no
    --log-cli=true
    --log-cli-level=INFO
    --log-file=logs/result.log
    --log-file-level=DEBUG
    --cov=.
    --cov-report=html:htmlcov
    --cov-report=term-missing
    --junitxml=pytest_results.xml
    --html=pytest_report.html
    --self-contained-html
    --maxfail=5
    --timeout=300

testpaths = tests
norecursedirs = .git .tox dist build *.egg venv .venv node_modules
python_files = test_*.py
python_classes = Test*
python_functions = test_*

markers =
    unit: Unit tests
    integration: Integration tests
    e2e: End-to-end tests
    slow: Tests that take more than 5 seconds
    smoke: Quick smoke tests
    regression: Regression tests

minversion = 6.0
timeout = 300
strict = true
console_output_style = progress
log_level = INFO
EOF
fi
echo -e "${GREEN}✓ pytest.ini 已配置${NC}"

# 3. 创建conftest.py
echo -e "\n${YELLOW}[3/10] 创建conftest.py...${NC}"
if [ -f "$(dirname "$0")/tests/conftest.py" ] && [ "$(dirname "$0")" != "." ]; then
    cp "$(dirname "$0")/tests/conftest.py" ./tests/conftest.py
elif [ ! -f "./tests/conftest.py" ]; then
    cat > tests/conftest.py << 'EOF'
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
EOF
fi
echo -e "${GREEN}✓ conftest.py 已创建${NC}"

# 4. 复制或创建审计脚本
echo -e "\n${YELLOW}[4/10] 创建审计脚本...${NC}"
if [ -f "$(dirname "$0")/audit_test_log.py" ] && [ "$(dirname "$0")" != "." ]; then
    cp "$(dirname "$0")/audit_test_log.py" ./audit_test_log.py
elif [ ! -f "./audit_test_log.py" ]; then
    cat > audit_test_log.py << 'EOF'
#!/usr/bin/env python3
"""测试日志审计脚本"""

import os
import json
import re
import sys
from datetime import datetime
from pathlib import Path

def audit_test_log():
    """审计测试日志"""
    log_file = Path('logs/result.log')
    audit_results = {
        'timestamp': datetime.now().isoformat(),
        'audit_passed': False,
        'log_exists': False,
        'log_valid': False,
        'test_count': 0,
        'failures': [],
        'warnings': [],
        'errors': []
    }
    
    # 检查日志文件是否存在
    if not log_file.exists():
        audit_results['errors'].append('result.log file not found')
        save_audit_report(audit_results)
        return False
    
    audit_results['log_exists'] = True
    
    # 读取并分析日志
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            
        # 检查测试结果
        if 'FAILED' in content:
            audit_results['warnings'].append('Failed test cases detected')
        
        # 统计测试数量
        test_matches = re.findall(r'test_\w+', content)
        audit_results['test_count'] = len(set(test_matches))
        
        if audit_results['test_count'] > 0:
            audit_results['log_valid'] = True
        
        # 判断审计是否通过
        if audit_results['log_exists'] and audit_results['log_valid']:
            audit_results['audit_passed'] = True
            
    except Exception as e:
        audit_results['errors'].append(f'Error reading log file: {str(e)}')
    
    # 保存审计报告
    save_audit_report(audit_results)
    
    return audit_results['audit_passed']

def save_audit_report(results):
    """保存审计报告"""
    with open('audit_report.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # 打印摘要
    print("\n" + "="*50)
    print("测试日志审计报告")
    print("="*50)
    print(f"审计时间: {results['timestamp']}")
    print(f"日志文件存在: {'✓' if results['log_exists'] else '✗'}")
    print(f"日志有效: {'✓' if results['log_valid'] else '✗'}")
    print(f"测试数量: {results['test_count']}")
    print(f"审计结果: {'✅ 通过' if results['audit_passed'] else '❌ 失败'}")
    
    if results['warnings']:
        print("\n⚠️  警告:")
        for warning in results['warnings']:
            print(f"  - {warning}")
    
    if results['errors']:
        print("\n❌ 错误:")
        for error in results['errors']:
            print(f"  - {error}")
    
    print("="*50 + "\n")

if __name__ == '__main__':
    success = audit_test_log()
    sys.exit(0 if success else 1)
EOF
fi
chmod +x audit_test_log.py
echo -e "${GREEN}✓ audit_test_log.py 已创建${NC}"

# 5. 创建Git预提交钩子
echo -e "\n${YELLOW}[5/10] 配置Git预提交钩子...${NC}"
mkdir -p .git/hooks
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash

# Git pre-commit hook - 自动运行测试和审计

echo "Running pre-commit tests..."

# 切换到项目根目录
cd "$(git rev-parse --show-toplevel)"

# 创建logs目录
mkdir -p logs

# 运行pytest
if command -v pytest &> /dev/null; then
    pytest --log-file=logs/result.log --log-file-level=DEBUG -v
    TEST_EXIT_CODE=$?
else
    echo "Warning: pytest not installed, skipping tests"
    TEST_EXIT_CODE=0
fi

# 运行审计
if [ -f audit_test_log.py ]; then
    python3 audit_test_log.py
    AUDIT_EXIT_CODE=$?
else
    echo "Warning: audit script not found"
    AUDIT_EXIT_CODE=0
fi

# 添加日志文件到提交
if [ -f logs/result.log ]; then
    git add logs/result.log
fi

if [ -f audit_report.json ]; then
    git add audit_report.json
fi

# 检查结果
if [ $TEST_EXIT_CODE -ne 0 ] || [ $AUDIT_EXIT_CODE -ne 0 ]; then
    echo "Pre-commit checks failed!"
    exit 1
fi

echo "Pre-commit checks passed!"
exit 0
EOF
chmod +x .git/hooks/pre-commit
echo -e "${GREEN}✓ Git预提交钩子已配置${NC}"

# 6. 创建GitHub Actions工作流
echo -e "\n${YELLOW}[6/10] 创建GitHub Actions工作流...${NC}"
if [ -f "$(dirname "$0")/.github/workflows/python-test.yml" ] && [ "$(dirname "$0")" != "." ]; then
    cp "$(dirname "$0")/.github/workflows/python-test.yml" ./.github/workflows/python-test.yml
elif [ ! -f "./.github/workflows/python-test.yml" ]; then
    cat > .github/workflows/python-test.yml << 'EOF'
name: Python Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, '3.10', 3.11]
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pytest pytest-cov pytest-html
        if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
    
    - name: Run tests
      run: |
        mkdir -p logs
        pytest --log-file=logs/result.log --cov=. --cov-report=html
    
    - name: Run audit
      run: |
        python audit_test_log.py
    
    - name: Upload test results
      uses: actions/upload-artifact@v3
      with:
        name: test-results-${{ matrix.python-version }}
        path: |
          logs/result.log
          audit_report.json
          htmlcov/
          pytest_report.html
EOF
fi
echo -e "${GREEN}✓ GitHub Actions工作流已创建${NC}"

# 7. 创建示例测试文件
echo -e "\n${YELLOW}[7/10] 创建示例测试文件...${NC}"
cat > tests/test_example.py << 'EOF'
"""示例测试文件"""

import pytest

def test_addition():
    """测试加法"""
    assert 2 + 2 == 4

def test_string_operations():
    """测试字符串操作"""
    assert "hello" + " " + "world" == "hello world"
    assert "python".upper() == "PYTHON"

@pytest.mark.unit
def test_list_operations():
    """测试列表操作"""
    test_list = [1, 2, 3]
    test_list.append(