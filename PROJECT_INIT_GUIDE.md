# Git项目标准化配置指南

## 🚀 快速开始

### 1. 在新项目中执行标准化配置

```bash
# 方式1：直接运行初始化脚本
bash init_git_project.sh

# 方式2：手动执行各步骤
git init  # 如果还没有初始化Git
./init_git_project.sh
```

### 2. 安装Python依赖

```bash
pip install -r requirements.txt
```

### 3. 运行测试验证配置

```bash
# 运行所有测试
pytest

# 运行审计
python audit_test_log.py

# 查看测试日志
cat logs/result.log
```

## 📁 标准项目结构

```
project/
├── .git/
│   └── hooks/
│       └── pre-commit          # Git预提交钩子
├── .github/
│   └── workflows/
│       └── python-test.yml     # GitHub Actions CI/CD
├── tests/                      # 测试目录
│   ├── __init__.py
│   ├── conftest.py            # Pytest配置
│   ├── unit/                  # 单元测试
│   ├── integration/           # 集成测试
│   └── e2e/                   # 端到端测试
├── logs/                      # 日志目录
│   └── result.log            # 测试结果日志
├── docs/                      # 文档目录
├── pytest.ini                 # Pytest配置文件
├── .coveragerc               # 代码覆盖率配置
├── audit_test_log.py         # 审计脚本
├── requirements.txt          # Python依赖
└── README.md                 # 项目说明
```

## ✅ 功能特性

### 1. 自动化测试
- ✅ Pytest框架配置
- ✅ 单元测试、集成测试、端到端测试分类
- ✅ 代码覆盖率报告
- ✅ HTML测试报告生成
- ✅ JUnit XML格式输出

### 2. 日志和审计
- ✅ 自动生成`result.log`
- ✅ 测试执行审计
- ✅ 审计报告生成
- ✅ 合规性检查

### 3. Git集成
- ✅ 预提交钩子自动运行测试
- ✅ 测试失败阻止提交
- ✅ 自动添加测试日志到提交

### 4. CI/CD
- ✅ GitHub Actions工作流
- ✅ 多Python版本测试
- ✅ 自动上传测试结果
- ✅ 代码质量检查

## 📋 使用规范

### 测试文件命名
- 所有测试文件必须以`test_`开头
- 测试类必须以`Test`开头
- 测试函数必须以`test_`开头

### 测试标记
```python
@pytest.mark.unit        # 单元测试
@pytest.mark.integration # 集成测试
@pytest.mark.e2e         # 端到端测试
@pytest.mark.slow        # 慢速测试
@pytest.mark.smoke       # 冒烟测试
@pytest.mark.regression  # 回归测试
```

### 运行特定类型测试
```bash
pytest -m unit           # 只运行单元测试
pytest -m integration    # 只运行集成测试
pytest -m "not slow"     # 跳过慢速测试
```

## 🔧 配置说明

### pytest.ini配置
- 日志输出：`logs/result.log`
- 覆盖率报告：HTML格式
- 超时设置：300秒
- 最大失败数：5

### Git钩子配置
- 自动运行pytest
- 执行审计脚本
- 添加日志到提交
- 失败时阻止提交

### CI/CD配置
- Python版本：3.8-3.11
- 触发条件：push和PR到main/develop分支
- 生成物：测试报告、覆盖率报告、审计报告

## 📊 审计要求

每次提交必须满足：
1. 所有测试通过
2. `result.log`文件存在
3. 审计验证成功
4. 测试覆盖率达标（可配置）

## 🛠️ 故障排除

### 问题1：pytest未安装
```bash
pip install pytest pytest-cov pytest-html
```

### 问题2：权限错误
```bash
chmod +x .git/hooks/pre-commit
chmod +x audit_test_log.py
```

### 问题3：日志目录不存在
```bash
mkdir -p logs
```

## 📚 相关文档

- [Pytest官方文档](https://docs.pytest.org/)
- [GitHub Actions文档](https://docs.github.com/en/actions)
- [代码覆盖率最佳实践](https://coverage.readthedocs.io/)

## 🤝 贡献指南

1. Fork项目
2. 创建特性分支
3. 编写测试
4. 确保所有测试通过
5. 提交PR

## 📄 许可证

MIT License

---

*该配置由自动化工具生成，确保项目符合测试和审计标准*