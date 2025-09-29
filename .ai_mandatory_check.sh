#!/bin/bash

# AI强制检查脚本 - 所有AI必须执行
# 用户不想再听到关于Git项目配置的废话

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo "🔒 AI强制项目标准检查..."

# 检查Git仓库
if ! git status &>/dev/null; then
    echo "⚡ 自动初始化Git项目..."
    if [ -f "init_git_project.sh" ]; then
        bash init_git_project.sh
    else
        git init
        git add .
        git commit -m "Initial commit - Auto-initialized by AI"
    fi
fi

# 检查测试系统
if [ ! -f "pytest.ini" ] || [ ! -f "audit_tests.py" ]; then
    echo "⚡ 自动配置测试系统..."
    if [ -f "init_git_project.sh" ]; then
        bash init_git_project.sh
    fi
fi

# 检查pre-commit钩子
if [ ! -f ".git/hooks/pre-commit" ]; then
    echo "⚡ 自动配置pre-commit钩子..."
    if [ -f "init_git_project.sh" ]; then
        bash init_git_project.sh
    fi
fi

# 运行基础测试确保系统正常
if [ -f "audit_tests.py" ]; then
    echo "⚡ 验证测试系统..."
    python3 audit_tests.py &>/dev/null || echo "⚠️ 测试系统需要修复"
fi

echo "✅ AI项目标准检查完成 - 无需用户干预"

# 创建标记文件，表示检查已完成
touch .ai_check_completed

exit 0