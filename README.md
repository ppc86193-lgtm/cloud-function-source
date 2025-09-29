# PC28 系统 - Supabase 集成与 CI/CD 自动化部署

## 项目概述

PC28 系统是一个综合性的数据处理和分析平台，集成了多种数据源（BigQuery、SQLite）和实时数据同步功能。本项目现已扩展支持 Supabase 集成，并通过 GitHub Actions 实现自动化 CI/CD 部署。

## 核心功能

### 🎯 数据处理与分析
- **多数据源支持**: BigQuery、SQLite、Supabase
- **实时数据同步**: 支持增量和全量同步模式
- **数据质量检查**: 自动化数据验证和完整性检查
- **性能优化**: 数据库查询优化和缓存机制

### 🔄 数据同步系统
- **PC28 到 Supabase 数据同步**
- **实时数据流处理**
- **批量数据迁移**
- **数据一致性验证**

### 🚀 CI/CD 自动化
- **GitHub Actions 工作流**
- **自动化测试执行**
- **Supabase 自动部署**
- **代码质量检查**

### 📊 监控与审计
- **实时系统监控**
- **数据同步状态跟踪**
- **定期审计报告**
- **错误告警机制**

## 项目结构

```
├── .github/workflows/          # GitHub Actions 工作流
├── supabase/                   # Supabase 配置和迁移
│   └── migrations/            # 数据库迁移脚本
├── config/                     # 配置文件
├── local_system/              # 本地数据库系统
├── python/                    # Python 模块
├── test_reports/              # 测试报告
├── pytest_execution_reports/  # pytest 执行报告
├── logs/                      # 日志文件
└── requirements.txt           # Python 依赖
```

## 核心数据表

### 主要数据表
- `lab_push_candidates_v2` - 决策候选数据
- `cloud_pred_today_norm` - 预测数据
- `signal_pool_union_v3` - 信号池数据
- `p_size_clean_merged_dedup_v` - 大小预测清洗数据
- `draws_14w_dedup_v` - 开奖数据
- `score_ledger` - 评分账本

## 快速开始

### 环境要求
- Python 3.8+
- PostgreSQL (Supabase)
- Git

### 安装依赖
```bash
pip install -r requirements.txt
```

### 配置环境变量
```bash
# 复制环境变量模板
cp .env.example .env

# 编辑环境变量
# SUPABASE_URL=your_supabase_url
# SUPABASE_ANON_KEY=your_anon_key
# SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

### 运行测试
```bash
# 运行所有测试
pytest

# 运行特定测试类别
pytest -m "data"
pytest -m "integration"
```

## CI/CD 工作流

### 自动化流程
1. **代码推送触发**: 推送到 main 分支自动触发 CI/CD
2. **代码质量检查**: 运行 linting 和代码格式检查
3. **自动化测试**: 执行完整的 pytest 测试套件
4. **数据库迁移**: 自动应用 Supabase 迁移
5. **部署验证**: 验证部署状态和数据完整性

### 手动触发
```bash
# 手动触发数据同步
python data_sync_manager.py --mode full

# 手动触发审计检查
python audit_system.py --check-all
```

## 数据同步

### 支持的同步模式
- **实时同步**: 监听数据变化，实时同步到 Supabase
- **增量同步**: 定期同步新增和修改的数据
- **全量同步**: 完整数据重新同步

### 同步配置
```python
# 配置示例
SYNC_CONFIG = {
    "tables": {
        "lab_push_candidates_v2": {
            "sync_type": "realtime",
            "batch_size": 1000,
            "sync_interval": 300
        }
    }
}
```

## 监控与审计

### 监控指标
- 数据同步状态
- 系统性能指标
- 错误率和响应时间
- 数据质量指标

### 审计功能
- 数据完整性检查
- 同步一致性验证
- 性能基准测试
- 安全审计日志

## 测试框架

### 测试类别
- `@pytest.mark.data` - 数据处理测试
- `@pytest.mark.integration` - 集成测试
- `@pytest.mark.performance` - 性能测试
- `@pytest.mark.security` - 安全测试

### 测试报告
测试执行后会生成详细报告：
- JSON 格式结构化数据
- HTML 可视化报告
- CSV 数据分析报告
- Markdown 摘要报告

## 安全考虑

### 密钥管理
- 使用 GitHub Secrets 管理敏感信息
- 环境变量隔离
- 定期密钥轮换

### 数据安全
- 数据传输加密
- 访问权限控制
- 审计日志记录

## 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 联系方式

如有问题或建议，请创建 Issue 或联系项目维护者。

---

**注意**: 请确保在生产环境中正确配置所有环境变量和安全设置。