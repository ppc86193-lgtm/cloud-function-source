# PC28 系统完整项目文档

## 目录
- [项目概述](#项目概述)
- [系统架构](#系统架构)
- [核心组件](#核心组件)
- [数据流图](#数据流图)
- [配置管理](#配置管理)
- [安全最佳实践](#安全最佳实践)
- [API 接口](#api-接口)
- [数据库设计](#数据库设计)
- [监控和日志](#监控和日志)

## 项目概述

PC28 是一个综合性的数据处理和监控系统，集成了多个数据源的采集、处理、分析和可视化功能。系统采用模块化设计，支持实时数据处理、性能优化、自动化部署和智能监控。

### 主要功能
- **数据采集**: 支持多种数据源的实时采集
- **数据处理**: 高效的数据清洗、转换和分析
- **性能优化**: 智能代码优化和性能监控
- **自动化部署**: 完整的 CI/CD 流程
- **监控告警**: 实时系统监控和智能告警
- **可视化**: 丰富的数据可视化和报表功能

## 系统架构

### 整体架构图
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   数据采集层     │    │   数据处理层     │    │   应用服务层     │
│                │    │                │    │                │
│ • API 采集器    │───▶│ • 数据清洗      │───▶│ • Web 服务      │
│ • 文件监控      │    │ • 数据转换      │    │ • API 网关      │
│ • 实时流处理    │    │ • 数据分析      │    │ • 任务调度      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   存储层        │    │   监控层        │    │   展示层        │
│                │    │                │    │                │
│ • 关系数据库    │    │ • 性能监控      │    │ • 管理界面      │
│ • 时序数据库    │    │ • 日志监控      │    │ • 数据可视化    │
│ • 文件存储      │    │ • 告警系统      │    │ • 报表系统      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 技术栈
- **后端**: Python 3.8+, FastAPI, SQLAlchemy
- **数据库**: PostgreSQL, Redis, InfluxDB
- **消息队列**: RabbitMQ, Celery
- **监控**: Prometheus, Grafana, ELK Stack
- **部署**: Docker, Kubernetes, Google Cloud Platform
- **前端**: React, TypeScript, Ant Design

## 核心组件

### 1. 数据采集组件 (Data Collector)

**位置**: `data_collector/`

**功能**: 负责从各种数据源采集数据

**主要模块**:
- `api_collector.py`: API 数据采集器
- `file_monitor.py`: 文件监控器
- `stream_processor.py`: 实时流处理器

**配置示例**:
```python
# config/collector_config.py
COLLECTOR_CONFIG = {
    'api_endpoints': [
        {
            'name': 'bigquery_api',
            'url': 'https://bigquery.googleapis.com/bigquery/v2',
            'auth_type': 'oauth2',
            'interval': 300  # 5分钟
        }
    ],
    'file_monitors': [
        {
            'path': '/data/input/',
            'pattern': '*.json',
            'recursive': True
        }
    ]
}
```

### 2. 数据处理引擎 (Processing Engine)

**位置**: `processing_engine/`

**功能**: 数据清洗、转换和分析

**主要模块**:
- `data_cleaner.py`: 数据清洗
- `data_transformer.py`: 数据转换
- `analytics_engine.py`: 数据分析

**使用示例**:
```python
from processing_engine import DataProcessor

processor = DataProcessor()
result = processor.process_batch({
    'source': 'api_data',
    'transformations': ['clean_nulls', 'normalize_dates'],
    'output_format': 'json'
})
```

### 3. 性能优化器 (Performance Optimizer)

**位置**: `advanced_performance_optimizer.py`

**功能**: 智能代码分析和性能优化

**特性**:
- 复杂度分析
- 性能瓶颈识别
- 自动优化建议
- 风险评估

**使用示例**:
```python
from advanced_performance_optimizer import AdvancedPerformanceOptimizer

optimizer = AdvancedPerformanceOptimizer()
result = optimizer.analyze_file('component_updater.py')
print(f"发现 {len(result.suggestions)} 个优化建议")
```

### 4. 监控系统 (Monitoring System)

**位置**: `monitoring/`

**功能**: 系统监控、告警和日志管理

**主要模块**:
- `system_monitor.py`: 系统监控
- `alert_manager.py`: 告警管理
- `log_analyzer.py`: 日志分析

### 5. 部署管理器 (Deployment Manager)

**位置**: `deploy_ops_system.py`

**功能**: 自动化部署和运维

**支持的部署方式**:
- Google Cloud Functions
- Kubernetes 集群
- Docker 容器

## 数据流图

### 数据处理流程
```
外部数据源
    │
    ▼
┌─────────────┐
│ 数据采集器   │
└─────────────┘
    │
    ▼
┌─────────────┐
│ 数据验证     │
└─────────────┘
    │
    ▼
┌─────────────┐
│ 数据清洗     │
└─────────────┘
    │
    ▼
┌─────────────┐
│ 数据转换     │
└─────────────┘
    │
    ▼
┌─────────────┐
│ 数据存储     │
└─────────────┘
    │
    ▼
┌─────────────┐
│ 数据分析     │
└─────────────┘
    │
    ▼
┌─────────────┐
│ 结果输出     │
└─────────────┘
```

### 监控数据流
```
应用组件 ──┐
          │
系统指标 ──┼──▶ 监控采集器 ──▶ 时序数据库 ──▶ 可视化界面
          │                    │
日志数据 ──┘                    ▼
                           告警系统
```

## 配置管理

### 配置文件结构
```
config/
├── app_config.py          # 应用主配置
├── database_config.py     # 数据库配置
├── monitoring_config.py   # 监控配置
├── security_config.py     # 安全配置
└── deployment_config.py   # 部署配置
```

### 环境变量管理
```bash
# .env 文件示例
ENVIRONMENT=production
DATABASE_URL=postgresql://user:pass@localhost:5432/pc28
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=your-secret-key-here
GOOGLE_CLOUD_PROJECT=your-project-id
MONITORING_ENABLED=true
LOG_LEVEL=INFO
```

### 配置加载机制
```python
# config/config_loader.py
import os
from typing import Dict, Any

class ConfigLoader:
    def __init__(self):
        self.env = os.getenv('ENVIRONMENT', 'development')
    
    def load_config(self) -> Dict[str, Any]:
        base_config = self._load_base_config()
        env_config = self._load_env_config()
        return {**base_config, **env_config}
    
    def _load_base_config(self) -> Dict[str, Any]:
        return {
            'app_name': 'PC28',
            'version': '1.0.0',
            'debug': False
        }
    
    def _load_env_config(self) -> Dict[str, Any]:
        return {
            'database_url': os.getenv('DATABASE_URL'),
            'redis_url': os.getenv('REDIS_URL'),
            'secret_key': os.getenv('SECRET_KEY')
        }
```

## 安全最佳实践

### 1. 身份认证和授权

**JWT Token 管理**:
```python
# security/auth.py
import jwt
from datetime import datetime, timedelta

class AuthManager:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
    
    def generate_token(self, user_id: str, permissions: list) -> str:
        payload = {
            'user_id': user_id,
            'permissions': permissions,
            'exp': datetime.utcnow() + timedelta(hours=24),
            'iat': datetime.utcnow()
        }
        return jwt.encode(payload, self.secret_key, algorithm='HS256')
    
    def verify_token(self, token: str) -> dict:
        try:
            return jwt.decode(token, self.secret_key, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            raise Exception('Token 已过期')
        except jwt.InvalidTokenError:
            raise Exception('无效的 Token')
```

### 2. 数据加密

**敏感数据加密**:
```python
# security/encryption.py
from cryptography.fernet import Fernet
import base64

class DataEncryption:
    def __init__(self, key: bytes = None):
        self.key = key or Fernet.generate_key()
        self.cipher = Fernet(self.key)
    
    def encrypt(self, data: str) -> str:
        encrypted_data = self.cipher.encrypt(data.encode())
        return base64.urlsafe_b64encode(encrypted_data).decode()
    
    def decrypt(self, encrypted_data: str) -> str:
        decoded_data = base64.urlsafe_b64decode(encrypted_data.encode())
        decrypted_data = self.cipher.decrypt(decoded_data)
        return decrypted_data.decode()
```

### 3. 输入验证

**API 输入验证**:
```python
# security/validation.py
from pydantic import BaseModel, validator
from typing import Optional

class APIRequest(BaseModel):
    user_id: str
    action: str
    data: Optional[dict] = None
    
    @validator('user_id')
    def validate_user_id(cls, v):
        if not v or len(v) < 3:
            raise ValueError('用户ID必须至少3个字符')
        return v
    
    @validator('action')
    def validate_action(cls, v):
        allowed_actions = ['read', 'write', 'delete', 'update']
        if v not in allowed_actions:
            raise ValueError(f'不支持的操作: {v}')
        return v
```

### 4. 安全配置检查清单

- [ ] 所有敏感配置使用环境变量
- [ ] 数据库连接使用 SSL
- [ ] API 端点启用 HTTPS
- [ ] 实施速率限制
- [ ] 启用 CORS 保护
- [ ] 定期更新依赖包
- [ ] 实施日志审计
- [ ] 备份数据加密

## API 接口

### 系统API设计

**基础 URL**: `http://localhost:8080`

**认证方式**: 无需认证（内部系统）

**通用响应格式**:
```json
{
  "status": "success",
  "data": {},
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### 运维管理API端点

| 方法 | 端点 | 描述 | 功能 |
|------|------|------|------|
| GET | `/` | 主仪表板 | Web界面 |
| GET | `/api/health` | 系统健康检查 | 获取系统健康状态 |
| GET | `/api/status` | 系统状态 | 获取详细系统状态 |
| GET | `/api/data-quality` | 数据质量检查 | 运行数据质量检查 |
| GET | `/api/concurrency` | 并发参数建议 | 获取并发优化建议 |
| GET | `/api/components` | 组件状态 | 检查组件运行状态 |
| GET | `/api/e2e-test` | 端到端测试 | 运行完整系统测试 |

### 外部数据API

**第三方API配置**:
- **基础URL**: `https://rijb.api.storeapi.net/api/119/261`
- **认证方式**: MD5签名认证
- **请求参数**: appid, format, time, sign

### 请求文件协议

根据 `API_PROTOCOL.md` 定义的协议：

**1. bucket_floor_request.json**:
```json
{
  "market": "oe|size|both",
  "bucket_floor": 0.33,
  "ttl_min": 60
}
```

**2. mode_switch_request.json**:
```json
{
  "mode": "conservative|balanced|aggressive",
  "ttl_min": 60
}
```

**协议说明**:
- 由桥接器消费写入 `runtime_params/runtime_mode`
- 冲突处理：后到覆盖前到
- TTL超时由上游清理

### API 使用示例

**健康检查请求**:
```bash
curl -X GET http://localhost:8080/api/health
```

**响应示例**:
```json
{
  "overall_health": "healthy",
  "system_resources": {
    "cpu_percent": 25.3,
    "memory_percent": 45.2,
    "disk_percent": 60.1
  },
  "uptime_seconds": 3600,
  "components": {
    "database": "healthy",
    "api_service": "healthy",
    "monitoring": "healthy"
  }
}
```

**数据质量检查请求**:
```bash
curl -X GET http://localhost:8080/api/data-quality
```

**响应示例**:
```json
{
  "status": "passed",
  "overall_score": 95.5,
  "checks": [
    {
      "name": "数据完整性",
      "status": "passed",
      "score": 98.0
    },
    {
      "name": "数据时效性",
      "status": "passed",
      "score": 93.0
    }
  ]
}
```

**第三方API请求示例**:
```python
import hashlib
import time
import requests

def make_api_request(appid, secret_key):
    params = {
        'appid': appid,
        'format': 'json',
        'time': str(int(time.time()))
    }
    
    # 生成MD5签名
    sign_string = ''.join([f"{k}{v}" for k, v in sorted(params.items())]) + secret_key
    params['sign'] = hashlib.md5(sign_string.encode()).hexdigest()
    
    response = requests.get('https://rijb.api.storeapi.net/api/119/261', params=params)
    return response.json()
```
```

## 数据库设计

### 主要数据表

**用户表 (users)**:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);
```

**数据处理任务表 (processing_jobs)**:
```sql
CREATE TABLE processing_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    source_type VARCHAR(50) NOT NULL,
    config JSONB,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**监控指标表 (metrics)**:
```sql
CREATE TABLE metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,4),
    metric_type VARCHAR(20),
    tags JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50)
);

CREATE INDEX idx_metrics_name_time ON metrics(metric_name, timestamp);
CREATE INDEX idx_metrics_tags ON metrics USING GIN(tags);
```

### 数据库连接管理

```python
# database/connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os

class DatabaseManager:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        self.engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
    
    def get_session(self):
        return self.SessionLocal()
    
    def close_connections(self):
        self.engine.dispose()
```

## 监控和日志

### 监控指标

**系统指标**:
- CPU 使用率
- 内存使用率
- 磁盘 I/O
- 网络流量

**应用指标**:
- 请求响应时间
- 错误率
- 吞吐量
- 数据处理延迟

**业务指标**:
- 数据处理成功率
- 用户活跃度
- 系统可用性

### 日志配置

```python
# logging_config.py
import logging
import logging.handlers
from pythonjsonlogger import jsonlogger

def setup_logging():
    # 创建格式化器
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 文件处理器
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/app.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    return root_logger
```

### 告警规则

```yaml
# alerts.yml
groups:
  - name: pc28_alerts
    rules:
      - alert: HighCPUUsage
        expr: cpu_usage_percent > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "CPU 使用率过高"
          description: "CPU 使用率已超过 80% 持续 5 分钟"
      
      - alert: DatabaseConnectionError
        expr: database_connection_errors > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "数据库连接错误"
          description: "检测到数据库连接错误"
```

---

## 版本信息

- **文档版本**: 1.0.0
- **系统版本**: PC28 v1.0.0
- **最后更新**: 2024-01-15
- **维护者**: PC28 开发团队

## 相关文档

- [运维手册](OPERATIONS_MANUAL.md)
- [故障排除指南](TROUBLESHOOTING_GUIDE.md)
- [开发指南](DEVELOPMENT_GUIDE.md)
- [API 协议文档](API_PROTOCOL.md)