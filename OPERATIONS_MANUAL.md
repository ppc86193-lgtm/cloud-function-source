# PC28 系统运维手册

## 目录
- [系统概述](#系统概述)
- [系统启动和停止](#系统启动和停止)
- [监控和告警](#监控和告警)
- [性能调优](#性能调优)
- [备份和恢复](#备份和恢复)
- [日志管理](#日志管理)
- [容量规划](#容量规划)
- [安全运维](#安全运维)
- [应急响应](#应急响应)
- [维护计划](#维护计划)

## 系统概述

PC28 是一个综合性的数据处理和监控系统，包含数据采集、处理、分析、监控和可视化等功能模块。本手册提供完整的运维操作指南。

### 系统架构组件
- **数据采集层**: API采集器、文件监控器、实时流处理器
- **数据处理层**: 数据清洗、转换、分析引擎
- **存储层**: SQLite、文件存储、缓存系统
- **监控层**: 系统监控、性能监控、告警系统
- **应用层**: Web界面、API服务、任务调度

### 关键服务端口
- **主服务**: 8080 (HTTP)
- **监控服务**: 9090 (Prometheus)
- **数据库**: 5432 (PostgreSQL)
- **缓存**: 6379 (Redis)

## 系统启动和停止

### 系统启动流程

#### 1. 环境检查
```bash
# 检查Python环境
python3 --version
# 应该显示 Python 3.8 或更高版本

# 检查依赖包
pip3 list | grep -E "requests|flask|sqlalchemy"

# 检查系统资源
df -h  # 磁盘空间
free -h  # 内存使用
top  # CPU使用情况
```

#### 2. 配置文件检查
```bash
# 检查配置文件是否存在
ls -la config/
# 应该包含：
# - app_config.py
# - database_config.py
# - monitoring_config.py

# 验证环境变量
echo $DATABASE_URL
echo $REDIS_URL
echo $LOG_LEVEL
```

#### 3. 数据库初始化
```bash
# 检查数据库连接
python3 -c "import sqlite3; conn = sqlite3.connect('lottery_data.db'); print('数据库连接成功')"

# 运行数据库迁移（如果需要）
python3 database_manager.py --migrate
```

#### 4. 启动主服务
```bash
# 方式1: 直接启动
python3 simple_ops_system.py

# 方式2: 后台启动
nohup python3 simple_ops_system.py > logs/system.log 2>&1 &

# 方式3: 使用systemd服务
sudo systemctl start pc28-system
```

#### 5. 启动验证
```bash
# 检查服务状态
curl -s http://localhost:8080/api/health | jq .

# 检查进程
ps aux | grep python3

# 检查端口监听
netstat -tlnp | grep 8080
```

### 系统停止流程

#### 1. 优雅停止
```bash
# 发送停止信号
kill -TERM $(pgrep -f simple_ops_system.py)

# 等待进程结束
while pgrep -f simple_ops_system.py > /dev/null; do
    echo "等待进程结束..."
    sleep 2
done
```

#### 2. 强制停止
```bash
# 如果优雅停止失败
kill -KILL $(pgrep -f simple_ops_system.py)

# 清理临时文件
rm -f /tmp/pc28_*.tmp
rm -f logs/*.lock
```

#### 3. 使用systemd管理
```bash
# 创建systemd服务文件
sudo tee /etc/systemd/system/pc28-system.service << EOF
[Unit]
Description=PC28 System Service
After=network.target

[Service]
Type=simple
User=pc28
WorkingDirectory=/opt/pc28
ExecStart=/usr/bin/python3 simple_ops_system.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# 启用服务
sudo systemctl daemon-reload
sudo systemctl enable pc28-system
sudo systemctl start pc28-system

# 查看状态
sudo systemctl status pc28-system
```

## 监控和告警

### 系统监控指标

#### 1. 基础系统指标
```python
# 监控脚本示例
#!/usr/bin/env python3
import psutil
import requests
import json
from datetime import datetime

def collect_system_metrics():
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'cpu_percent': psutil.cpu_percent(interval=1),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_percent': psutil.disk_usage('/').percent,
        'network_io': psutil.net_io_counters()._asdict(),
        'process_count': len(psutil.pids())
    }
    return metrics

def check_service_health():
    try:
        response = requests.get('http://localhost:8080/api/health', timeout=5)
        return response.status_code == 200
    except:
        return False

if __name__ == '__main__':
    metrics = collect_system_metrics()
    health = check_service_health()
    
    print(f"系统指标: {json.dumps(metrics, indent=2)}")
    print(f"服务健康: {'正常' if health else '异常'}")
```

#### 2. 应用监控指标
```bash
# 监控脚本
#!/bin/bash

# 检查API响应时间
response_time=$(curl -o /dev/null -s -w '%{time_total}' http://localhost:8080/api/health)
echo "API响应时间: ${response_time}s"

# 检查错误日志
error_count=$(tail -n 1000 logs/system.log | grep -c "ERROR")
echo "最近1000行日志中的错误数: $error_count"

# 检查数据库大小
db_size=$(du -sh lottery_data.db | cut -f1)
echo "数据库大小: $db_size"

# 检查内存使用
mem_usage=$(ps -o pid,ppid,cmd,%mem,%cpu --sort=-%mem -C python3 | head -5)
echo "Python进程内存使用:"
echo "$mem_usage"
```

### 告警配置

#### 1. 告警规则配置
```yaml
# alerts.yml
alert_rules:
  - name: "高CPU使用率"
    condition: "cpu_percent > 80"
    duration: "5m"
    severity: "warning"
    message: "CPU使用率超过80%已持续5分钟"
    
  - name: "高内存使用率"
    condition: "memory_percent > 85"
    duration: "3m"
    severity: "critical"
    message: "内存使用率超过85%已持续3分钟"
    
  - name: "磁盘空间不足"
    condition: "disk_percent > 90"
    duration: "1m"
    severity: "critical"
    message: "磁盘使用率超过90%"
    
  - name: "服务不可用"
    condition: "service_health == false"
    duration: "1m"
    severity: "critical"
    message: "主服务无法访问"
    
  - name: "API响应慢"
    condition: "api_response_time > 5"
    duration: "2m"
    severity: "warning"
    message: "API响应时间超过5秒"
```

#### 2. 告警通知脚本
```python
#!/usr/bin/env python3
# alert_notifier.py
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

class AlertNotifier:
    def __init__(self, config_file='config/alert_config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
    
    def send_email_alert(self, subject, message, severity='info'):
        """发送邮件告警"""
        try:
            email_config = self.config['channels']['email']
            if not email_config['enabled']:
                return False
            
            msg = MIMEMultipart()
            msg['From'] = email_config['config']['from_email']
            msg['To'] = ', '.join(email_config['config']['to_emails'])
            msg['Subject'] = f"[{severity.upper()}] {subject}"
            
            body = f"""
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
严重级别: {severity}
消息: {message}

系统: PC28 监控系统
            """
            
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            server = smtplib.SMTP(email_config['config']['smtp_host'], 
                                email_config['config']['smtp_port'])
            server.starttls()
            server.login(email_config['config']['username'], 
                        email_config['config']['password'])
            
            text = msg.as_string()
            server.sendmail(email_config['config']['from_email'], 
                          email_config['config']['to_emails'], text)
            server.quit()
            
            return True
        except Exception as e:
            print(f"发送邮件告警失败: {e}")
            return False
    
    def send_webhook_alert(self, message, severity='info'):
        """发送Webhook告警"""
        # 实现Webhook通知逻辑
        pass

if __name__ == '__main__':
    notifier = AlertNotifier()
    notifier.send_email_alert(
        "测试告警", 
        "这是一个测试告警消息", 
        "warning"
    )
```

### 监控仪表板

#### 1. 启动监控仪表板
```bash
# 访问主仪表板
open http://localhost:8080

# 或使用curl检查
curl -s http://localhost:8080/api/health | jq .
```

#### 2. 监控数据收集
```python
# monitoring_collector.py
#!/usr/bin/env python3
import time
import json
import sqlite3
from datetime import datetime
import requests
import psutil

class MonitoringCollector:
    def __init__(self, db_path='monitoring.db'):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    tags TEXT
                )
            """)
    
    def collect_and_store(self):
        # 收集系统指标
        metrics = {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent
        }
        
        # 收集应用指标
        try:
            response = requests.get('http://localhost:8080/api/health', timeout=5)
            metrics['api_response_time'] = response.elapsed.total_seconds()
            metrics['api_status'] = 1 if response.status_code == 200 else 0
        except:
            metrics['api_response_time'] = -1
            metrics['api_status'] = 0
        
        # 存储到数据库
        with sqlite3.connect(self.db_path) as conn:
            for name, value in metrics.items():
                conn.execute(
                    "INSERT INTO metrics (metric_name, metric_value) VALUES (?, ?)",
                    (name, value)
                )
    
    def run_forever(self, interval=60):
        while True:
            try:
                self.collect_and_store()
                print(f"[{datetime.now()}] 监控数据收集完成")
            except Exception as e:
                print(f"[{datetime.now()}] 监控数据收集失败: {e}")
            time.sleep(interval)

if __name__ == '__main__':
    collector = MonitoringCollector()
    collector.run_forever()
```

## 性能调优

### 系统性能优化

#### 1. CPU优化
```bash
# 查看CPU使用情况
top -p $(pgrep -f simple_ops_system.py)

# 调整进程优先级
sudo renice -10 $(pgrep -f simple_ops_system.py)

# 设置CPU亲和性
taskset -cp 0,1 $(pgrep -f simple_ops_system.py)
```

#### 2. 内存优化
```python
# memory_optimizer.py
import gc
import psutil
import os

class MemoryOptimizer:
    def __init__(self):
        self.process = psutil.Process(os.getpid())
    
    def get_memory_usage(self):
        return self.process.memory_info().rss / 1024 / 1024  # MB
    
    def optimize_memory(self):
        # 强制垃圾回收
        gc.collect()
        
        # 清理缓存
        if hasattr(self, '_cache'):
            self._cache.clear()
        
        print(f"内存优化后使用量: {self.get_memory_usage():.2f} MB")
    
    def monitor_memory(self, threshold_mb=500):
        current_usage = self.get_memory_usage()
        if current_usage > threshold_mb:
            print(f"内存使用量过高: {current_usage:.2f} MB")
            self.optimize_memory()
            return True
        return False
```

#### 3. 数据库性能优化
```sql
-- 数据库优化SQL
-- 1. 创建索引
CREATE INDEX IF NOT EXISTS idx_lottery_records_timestamp ON lottery_records(timestamp);
CREATE INDEX IF NOT EXISTS idx_lottery_records_issue ON lottery_records(issue);
CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON metrics(metric_name, timestamp);

-- 2. 分析表统计信息
ANALYZE lottery_records;
ANALYZE metrics;

-- 3. 清理旧数据
DELETE FROM metrics WHERE timestamp < datetime('now', '-30 days');
DELETE FROM lottery_records WHERE timestamp < datetime('now', '-90 days');

-- 4. 重建索引
REINDEX;

-- 5. 压缩数据库
VACUUM;
```

#### 4. 网络性能优化
```python
# network_optimizer.py
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class NetworkOptimizer:
    def __init__(self):
        self.session = self._create_optimized_session()
    
    def _create_optimized_session(self):
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        # 配置连接池
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置超时
        session.timeout = 30
        
        return session
    
    def make_request(self, url, **kwargs):
        return self.session.get(url, **kwargs)
```

### 应用性能调优

#### 1. 并发优化
```python
# concurrency_tuner.py
import threading
import concurrent.futures
from queue import Queue
import time

class ConcurrencyTuner:
    def __init__(self):
        self.optimal_workers = self._calculate_optimal_workers()
        self.task_queue = Queue(maxsize=1000)
    
    def _calculate_optimal_workers(self):
        import os
        cpu_count = os.cpu_count()
        # I/O密集型任务：CPU核心数 * 2
        # CPU密集型任务：CPU核心数
        return cpu_count * 2
    
    def process_with_threadpool(self, tasks, max_workers=None):
        if max_workers is None:
            max_workers = self.optimal_workers
        
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(task): task for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result(timeout=30)
                    results.append(result)
                except Exception as e:
                    print(f"任务执行失败: {e}")
        
        return results
    
    def benchmark_workers(self, test_tasks, worker_counts=[1, 2, 4, 8, 16]):
        """基准测试不同工作线程数的性能"""
        results = {}
        
        for worker_count in worker_counts:
            start_time = time.time()
            self.process_with_threadpool(test_tasks, worker_count)
            end_time = time.time()
            
            results[worker_count] = end_time - start_time
            print(f"工作线程数 {worker_count}: {results[worker_count]:.2f}秒")
        
        # 找到最优工作线程数
        optimal = min(results, key=results.get)
        print(f"最优工作线程数: {optimal}")
        return optimal
```

#### 2. 缓存优化
```python
# cache_optimizer.py
import functools
import time
from typing import Any, Dict

class CacheOptimizer:
    def __init__(self, max_size=1000, ttl=3600):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.ttl = ttl
    
    def get(self, key: str) -> Any:
        if key in self.cache:
            entry = self.cache[key]
            if time.time() - entry['timestamp'] < self.ttl:
                entry['hits'] += 1
                return entry['value']
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        # 如果缓存满了，删除最少使用的条目
        if len(self.cache) >= self.max_size:
            lru_key = min(self.cache.keys(), 
                         key=lambda k: self.cache[k]['hits'])
            del self.cache[lru_key]
        
        self.cache[key] = {
            'value': value,
            'timestamp': time.time(),
            'hits': 0
        }
    
    def cached_function(self, ttl=None):
        """装饰器：为函数添加缓存"""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # 生成缓存键
                cache_key = f"{func.__name__}:{hash(str(args) + str(kwargs))}"
                
                # 尝试从缓存获取
                result = self.get(cache_key)
                if result is not None:
                    return result
                
                # 执行函数并缓存结果
                result = func(*args, **kwargs)
                self.set(cache_key, result)
                return result
            return wrapper
        return decorator
    
    def get_stats(self):
        total_entries = len(self.cache)
        total_hits = sum(entry['hits'] for entry in self.cache.values())
        return {
            'total_entries': total_entries,
            'total_hits': total_hits,
            'hit_rate': total_hits / max(total_entries, 1)
        }
```

## 备份和恢复

### 数据备份策略

#### 1. 自动备份脚本
```bash
#!/bin/bash
# backup.sh - 自动备份脚本

BACKUP_DIR="/opt/pc28/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="pc28_backup_$DATE"

# 创建备份目录
mkdir -p $BACKUP_DIR/$BACKUP_NAME

echo "开始备份 PC28 系统..."

# 1. 备份数据库
echo "备份数据库..."
cp lottery_data.db $BACKUP_DIR/$BACKUP_NAME/
cp monitoring.db $BACKUP_DIR/$BACKUP_NAME/
cp deduplication.db $BACKUP_DIR/$BACKUP_NAME/

# 2. 备份配置文件
echo "备份配置文件..."
cp -r config/ $BACKUP_DIR/$BACKUP_NAME/
cp -r CHANGESETS/ $BACKUP_DIR/$BACKUP_NAME/

# 3. 备份日志文件
echo "备份日志文件..."
cp -r logs/ $BACKUP_DIR/$BACKUP_NAME/

# 4. 备份应用代码
echo "备份应用代码..."
cp *.py $BACKUP_DIR/$BACKUP_NAME/

# 5. 创建备份信息文件
cat > $BACKUP_DIR/$BACKUP_NAME/backup_info.txt << EOF
备份时间: $(date)
备份类型: 完整备份
系统版本: $(python3 --version)
备份大小: $(du -sh $BACKUP_DIR/$BACKUP_NAME | cut -f1)
EOF

# 6. 压缩备份
echo "压缩备份文件..."
cd $BACKUP_DIR
tar -czf ${BACKUP_NAME}.tar.gz $BACKUP_NAME
rm -rf $BACKUP_NAME

# 7. 清理旧备份（保留最近7天）
find $BACKUP_DIR -name "pc28_backup_*.tar.gz" -mtime +7 -delete

echo "备份完成: $BACKUP_DIR/${BACKUP_NAME}.tar.gz"
echo "备份大小: $(du -sh $BACKUP_DIR/${BACKUP_NAME}.tar.gz | cut -f1)"
```

#### 2. 增量备份脚本
```bash
#!/bin/bash
# incremental_backup.sh - 增量备份脚本

BACKUP_DIR="/opt/pc28/backups"
INCREMENTAL_DIR="$BACKUP_DIR/incremental"
DATE=$(date +%Y%m%d_%H%M%S)
LAST_BACKUP_FILE="$BACKUP_DIR/.last_backup_timestamp"

mkdir -p $INCREMENTAL_DIR

# 获取上次备份时间
if [ -f $LAST_BACKUP_FILE ]; then
    LAST_BACKUP=$(cat $LAST_BACKUP_FILE)
else
    LAST_BACKUP="1970-01-01 00:00:00"
fi

echo "开始增量备份，上次备份时间: $LAST_BACKUP"

# 查找修改过的文件
find . -type f -newer $LAST_BACKUP_FILE 2>/dev/null | while read file; do
    # 创建目录结构
    target_dir="$INCREMENTAL_DIR/incremental_$DATE/$(dirname "$file")"
    mkdir -p "$target_dir"
    
    # 复制文件
    cp "$file" "$target_dir/"
    echo "备份文件: $file"
done

# 更新备份时间戳
date > $LAST_BACKUP_FILE

# 压缩增量备份
cd $INCREMENTAL_DIR
tar -czf "incremental_$DATE.tar.gz" "incremental_$DATE"
rm -rf "incremental_$DATE"

echo "增量备份完成: $INCREMENTAL_DIR/incremental_$DATE.tar.gz"
```

### 数据恢复流程

#### 1. 完整恢复脚本
```bash
#!/bin/bash
# restore.sh - 数据恢复脚本

if [ $# -ne 1 ]; then
    echo "使用方法: $0 <备份文件路径>"
    echo "示例: $0 /opt/pc28/backups/pc28_backup_20240115_120000.tar.gz"
    exit 1
fi

BACKUP_FILE=$1
RESTORE_DIR="/tmp/pc28_restore_$(date +%s)"
CURRENT_DIR=$(pwd)

if [ ! -f $BACKUP_FILE ]; then
    echo "错误: 备份文件不存在: $BACKUP_FILE"
    exit 1
fi

echo "开始恢复 PC28 系统..."
echo "备份文件: $BACKUP_FILE"
echo "恢复目录: $RESTORE_DIR"

# 1. 停止服务
echo "停止 PC28 服务..."
pkill -f simple_ops_system.py
sleep 5

# 2. 备份当前数据
echo "备份当前数据..."
cp -r . "${CURRENT_DIR}_backup_$(date +%s)"

# 3. 解压备份文件
echo "解压备份文件..."
mkdir -p $RESTORE_DIR
tar -xzf $BACKUP_FILE -C $RESTORE_DIR

# 4. 恢复数据库
echo "恢复数据库..."
cp $RESTORE_DIR/*/lottery_data.db ./
cp $RESTORE_DIR/*/monitoring.db ./
cp $RESTORE_DIR/*/deduplication.db ./

# 5. 恢复配置文件
echo "恢复配置文件..."
cp -r $RESTORE_DIR/*/config/ ./

# 6. 恢复日志文件
echo "恢复日志文件..."
cp -r $RESTORE_DIR/*/logs/ ./

# 7. 验证恢复
echo "验证数据完整性..."
python3 -c "
import sqlite3
try:
    conn = sqlite3.connect('lottery_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM lottery_records')
    count = cursor.fetchone()[0]
    print(f'数据库记录数: {count}')
    conn.close()
    print('数据库验证成功')
except Exception as e:
    print(f'数据库验证失败: {e}')
    exit(1)
"

# 8. 重启服务
echo "重启 PC28 服务..."
python3 simple_ops_system.py &
sleep 10

# 9. 验证服务
echo "验证服务状态..."
if curl -s http://localhost:8080/api/health > /dev/null; then
    echo "✅ 恢复成功！服务正常运行"
else
    echo "❌ 恢复失败！服务无法访问"
    exit 1
fi

# 10. 清理临时文件
rm -rf $RESTORE_DIR

echo "恢复完成！"
```

#### 2. 选择性恢复脚本
```bash
#!/bin/bash
# selective_restore.sh - 选择性恢复脚本

show_usage() {
    echo "使用方法: $0 [选项] <备份文件>"
    echo "选项:"
    echo "  --database-only    仅恢复数据库"
    echo "  --config-only      仅恢复配置文件"
    echo "  --logs-only        仅恢复日志文件"
    echo "  --verify-only      仅验证备份文件"
}

RESTORE_DATABASE=false
RESTORE_CONFIG=false
RESTORE_LOGS=false
VERIFY_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --database-only)
            RESTORE_DATABASE=true
            shift
            ;;
        --config-only)
            RESTORE_CONFIG=true
            shift
            ;;
        --logs-only)
            RESTORE_LOGS=true
            shift
            ;;
        --verify-only)
            VERIFY_ONLY=true
            shift
            ;;
        -*)
            echo "未知选项: $1"
            show_usage
            exit 1
            ;;
        *)
            BACKUP_FILE=$1
            shift
            ;;
    esac
done

if [ -z "$BACKUP_FILE" ]; then
    show_usage
    exit 1
fi

# 验证备份文件
echo "验证备份文件: $BACKUP_FILE"
if ! tar -tzf $BACKUP_FILE > /dev/null 2>&1; then
    echo "错误: 备份文件损坏或格式不正确"
    exit 1
fi

echo "备份文件验证成功"

if [ "$VERIFY_ONLY" = true ]; then
    echo "仅验证模式，退出"
    exit 0
fi

# 如果没有指定具体选项，恢复所有内容
if [ "$RESTORE_DATABASE" = false ] && [ "$RESTORE_CONFIG" = false ] && [ "$RESTORE_LOGS" = false ]; then
    RESTORE_DATABASE=true
    RESTORE_CONFIG=true
    RESTORE_LOGS=true
fi

RESTORE_DIR="/tmp/selective_restore_$(date +%s)"
mkdir -p $RESTORE_DIR
tar -xzf $BACKUP_FILE -C $RESTORE_DIR

if [ "$RESTORE_DATABASE" = true ]; then
    echo "恢复数据库..."
    cp $RESTORE_DIR/*/lottery_data.db ./
    cp $RESTORE_DIR/*/monitoring.db ./
    cp $RESTORE_DIR/*/deduplication.db ./
fi

if [ "$RESTORE_CONFIG" = true ]; then
    echo "恢复配置文件..."
    cp -r $RESTORE_DIR/*/config/ ./
fi

if [ "$RESTORE_LOGS" = true ]; then
    echo "恢复日志文件..."
    cp -r $RESTORE_DIR/*/logs/ ./
fi

rm -rf $RESTORE_DIR
echo "选择性恢复完成"
```

## 日志管理

### 日志配置

#### 1. 日志轮转配置
```bash
# /etc/logrotate.d/pc28
/opt/pc28/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 pc28 pc28
    postrotate
        /bin/kill -HUP $(cat /var/run/pc28.pid 2>/dev/null) 2>/dev/null || true
    endscript
}
```

#### 2. 日志分析脚本
```python
#!/usr/bin/env python3
# log_analyzer.py
import re
import json
from datetime import datetime, timedelta
from collections import defaultdict, Counter

class LogAnalyzer:
    def __init__(self, log_file='logs/system.log'):
        self.log_file = log_file
        self.patterns = {
            'error': re.compile(r'ERROR.*'),
            'warning': re.compile(r'WARNING.*'),
            'info': re.compile(r'INFO.*'),
            'timestamp': re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
        }
    
    def analyze_logs(self, hours=24):
        """分析最近N小时的日志"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        stats = {
            'total_lines': 0,
            'error_count': 0,
            'warning_count': 0,
            'info_count': 0,
            'error_messages': [],
            'warning_messages': [],
            'hourly_stats': defaultdict(lambda: {'errors': 0, 'warnings': 0})
        }
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    stats['total_lines'] += 1
                    
                    # 提取时间戳
                    timestamp_match = self.patterns['timestamp'].search(line)
                    if timestamp_match:
                        try:
                            log_time = datetime.strptime(
                                timestamp_match.group(), 
                                '%Y-%m-%d %H:%M:%S'
                            )
                            if log_time < cutoff_time:
                                continue
                            
                            hour_key = log_time.strftime('%Y-%m-%d %H:00')
                        except ValueError:
                            continue
                    else:
                        continue
                    
                    # 分析日志级别
                    if self.patterns['error'].search(line):
                        stats['error_count'] += 1
                        stats['error_messages'].append(line.strip())
                        stats['hourly_stats'][hour_key]['errors'] += 1
                    elif self.patterns['warning'].search(line):
                        stats['warning_count'] += 1
                        stats['warning_messages'].append(line.strip())
                        stats['hourly_stats'][hour_key]['warnings'] += 1
                    elif self.patterns['info'].search(line):
                        stats['info_count'] += 1
        
        except FileNotFoundError:
            print(f"日志文件不存在: {self.log_file}")
        
        return stats
    
    def find_error_patterns(self):
        """查找错误模式"""
        error_patterns = Counter()
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if 'ERROR' in line:
                        # 提取错误类型
                        if 'Exception' in line:
                            error_type = re.search(r'(\w+Exception)', line)
                            if error_type:
                                error_patterns[error_type.group(1)] += 1
                        elif 'Error' in line:
                            error_type = re.search(r'(\w+Error)', line)
                            if error_type:
                                error_patterns[error_type.group(1)] += 1
        
        except FileNotFoundError:
            pass
        
        return dict(error_patterns.most_common(10))
    
    def generate_report(self, hours=24):
        """生成日志分析报告"""
        stats = self.analyze_logs(hours)
        error_patterns = self.find_error_patterns()
        
        report = {
            'analysis_time': datetime.now().isoformat(),
            'time_range_hours': hours,
            'summary': {
                'total_lines': stats['total_lines'],
                'error_count': stats['error_count'],
                'warning_count': stats['warning_count'],
                'info_count': stats['info_count']
            },
            'error_rate': stats['error_count'] / max(stats['total_lines'], 1),
            'warning_rate': stats['warning_count'] / max(stats['total_lines'], 1),
            'top_error_patterns': error_patterns,
            'recent_errors': stats['error_messages'][-10:],
            'recent_warnings': stats['warning_messages'][-10:],
            'hourly_distribution': dict(stats['hourly_stats'])
        }
        
        return report

if __name__ == '__main__':
    analyzer = LogAnalyzer()
    report = analyzer.generate_report(24)
    
    print("=== PC28 日志分析报告 ===")
    print(f"分析时间: {report['analysis_time']}")
    print(f"时间范围: 最近 {report['time_range_hours']} 小时")
    print(f"总日志行数: {report['summary']['total_lines']}")
    print(f"错误数量: {report['summary']['error_count']}")
    print(f"警告数量: {report['summary']['warning_count']}")
    print(f"错误率: {report['error_rate']:.2%}")
    print(f"警告率: {report['warning_rate']:.2%}")
    
    if report['top_error_patterns']:
        print("\n=== 主要错误类型 ===")
        for error_type, count in report['top_error_patterns'].items():
            print(f"  {error_type}: {count} 次")
    
    if report['recent_errors']:
        print("\n=== 最近错误 ===")
        for error in report['recent_errors'][-5:]:
            print(f"  {error}")
```

## 容量规划

### 存储容量规划

#### 1. 存储使用监控
```python
#!/usr/bin/env python3
# capacity_monitor.py
import os
import sqlite3
import json
from datetime import datetime, timedelta

class CapacityMonitor:
    def __init__(self):
        self.data_files = [
            'lottery_data.db',
            'monitoring.db',
            'deduplication.db'
        ]
        self.log_dirs = ['logs']
    
    def get_file_size(self, filepath):
        """获取文件大小（MB）"""
        try:
            return os.path.getsize(filepath) / 1024 / 1024
        except OSError:
            return 0
    
    def get_directory_size(self, dirpath):
        """获取目录大小（MB）"""
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(dirpath):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        pass
        except OSError:
            pass
        return total_size / 1024 / 1024
    
    def analyze_database_growth(self, db_path='lottery_data.db'):
        """分析数据库增长趋势"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # 获取记录数
                cursor.execute("SELECT COUNT(*) FROM lottery_records")
                total_records = cursor.fetchone()[0]
                
                # 获取最近7天的记录数
                cursor.execute("""
                    SELECT COUNT(*) FROM lottery_records 
                    WHERE timestamp > datetime('now', '-7 days')
                """)
                recent_records = cursor.fetchone()[0]
                
                # 计算平均每日增长
                daily_growth = recent_records / 7
                
                return {
                    'total_records': total_records,
                    'recent_records': recent_records,
                    'daily_growth': daily_growth,
                    'estimated_monthly_growth': daily_growth * 30
                }
        except Exception as e:
            print(f"数据库分析失败: {e}")
            return None
    
    def predict_storage_needs(self, days_ahead=90):
        """预测存储需求"""
        current_usage = {}
        
        # 当前数据库大小
        for db_file in self.data_files:
            current_usage[db_file] = self.get_file_size(db_file)
        
        # 当前日志大小
        for log_dir in self.log_dirs:
            current_usage[f"{log_dir}_dir"] = self.get_directory_size(log_dir)
        
        # 分析增长趋势
        db_analysis = self.analyze_database_growth()
        
        predictions = {
            'current_usage_mb': current_usage,
            'total_current_mb': sum(current_usage.values()),
            'analysis_date': datetime.now().isoformat()
        }
        
        if db_analysis:
            # 预测数据库增长
            daily_records = db_analysis['daily_growth']
            avg_record_size = (current_usage.get('lottery_data.db', 0) * 1024 * 1024) / max(db_analysis['total_records'], 1)
            
            predicted_records = daily_records * days_ahead
            predicted_db_growth = (predicted_records * avg_record_size) / 1024 / 1024
            
            predictions.update({
                'database_analysis': db_analysis,
                'avg_record_size_bytes': avg_record_size,
                'predicted_db_growth_mb': predicted_db_growth,
                'predicted_total_mb': predictions['total_current_mb'] + predicted_db_growth,
                'days_ahead': days_ahead
            })
        
        return predictions
    
    def check_disk_space(self):
        """检查磁盘空间"""
        import shutil
        
        total, used, free = shutil.disk_usage('.')
        
        return {
            'total_gb': total / 1024 / 1024 / 1024,
            'used_gb': used / 1024 / 1024 / 1024,
            'free_gb': free / 1024 / 1024 / 1024,
            'usage_percent': (used / total) * 100
        }
    
    def generate_capacity_report(self):
        """生成容量规划报告"""
        disk_info = self.check_disk_space()
        predictions = self.predict_storage_needs()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'disk_space': disk_info,
            'storage_predictions': predictions,
            'recommendations': []
        }
        
        # 生成建议
        if disk_info['usage_percent'] > 80:
            report['recommendations'].append({
                'type': 'critical',
                'message': '磁盘使用率超过80%，需要立即清理或扩容'
            })
        
        if disk_info['free_gb'] < 5:
            report['recommendations'].append({
                'type': 'critical',
                'message': '可用磁盘空间不足5GB，系统可能无法正常运行'
            })
        
        if predictions.get('predicted_total_mb', 0) > disk_info['free_gb'] * 1024:
            report['recommendations'].append({
                'type': 'warning',
                'message': f"预计{predictions.get('days_ahead', 90)}天后存储空间不足"
            })
        
        return report

if __name__ == '__main__':
    monitor = CapacityMonitor()
    report = monitor.generate_capacity_report()
    
    print("=== PC28 容量规划报告 ===")
    print(f"报告时间: {report['timestamp']}")
    
    disk = report['disk_space']
    print(f"\n=== 磁盘空间 ===")
    print(f"总容量: {disk['total_gb']:.2f} GB")
    print(f"已使用: {disk['used_gb']:.2f} GB ({disk['usage_percent']:.1f}%)")
    print(f"可用空间: {disk['free_gb']:.2f} GB")
    
    pred = report['storage_predictions']
    print(f"\n=== 存储预测 ===")
    print(f"当前使用: {pred['total_current_mb']:.2f} MB")
    if 'predicted_total_mb' in pred:
        print(f"预计{pred['days_ahead']}天后: {pred['predicted_total_mb']:.2f} MB")
    
    if report['recommendations']:
        print(f"\n=== 建议 ===")
        for rec in report['recommendations']:
            print(f"[{rec['type'].upper()}] {rec['message']}")
```

### 性能容量规划

#### 1. 并发处理能力评估
```python
#!/usr/bin/env python3
# performance_capacity.py
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

class PerformanceCapacityPlanner:
    def __init__(self, base_url='http://localhost:8080'):
        self.base_url = base_url
        self.results = []
    
    def single_request_test(self, endpoint='/api/health'):
        """单个请求测试"""
        start_time = time.time()
        try:
            response = requests.get(f"{self.base_url}{endpoint}", timeout=10)
            end_time = time.time()
            return {
                'success': response.status_code == 200,
                'response_time': end_time - start_time,
                'status_code': response.status_code
            }
        except Exception as e:
            end_time = time.time()
            return {
                'success': False,
                'response_time': end_time - start_time,
                'error': str(e)
            }
    
    def concurrent_load_test(self, concurrent_users=10, requests_per_user=10):
        """并发负载测试"""
        print(f"开始负载测试: {concurrent_users} 并发用户，每用户 {requests_per_user} 请求")
        
        results = []
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
            # 提交所有任务
            futures = []
            for user in range(concurrent_users):
                for req in range(requests_per_user):
                    future = executor.submit(self.single_request_test)
                    futures.append(future)
            
            # 收集结果
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # 分析结果
        successful_requests = [r for r in results if r['success']]
        failed_requests = [r for r in results if not r['success']]
        
        response_times = [r['response_time'] for r in successful_requests]
        
        analysis = {
            'total_requests': len(results),
            'successful_requests': len(successful_requests),
            'failed_requests': len(failed_requests),
            'success_rate': len(successful_requests) / len(results) * 100,
            'total_test_time': total_time,
            'requests_per_second': len(results) / total_time,
            'concurrent_users': concurrent_users
        }
        
        if response_times:
            analysis.update({
                'avg_response_time': statistics.mean(response_times),
                'min_response_time': min(response_times),
                'max_response_time': max(response_times),
                'median_response_time': statistics.median(response_times)
            })
            
            if len(response_times) > 1:
                analysis['response_time_std'] = statistics.stdev(response_times)
        
        return analysis
    
    def find_max_capacity(self, start_users=1, max_users=100, step=5, success_threshold=95):
        """寻找最大处理能力"""
        print(f"寻找最大处理能力，成功率阈值: {success_threshold}%")
        
        capacity_results = []
        
        for users in range(start_users, max_users + 1, step):
            print(f"测试 {users} 并发用户...")
            result = self.concurrent_load_test(users, 5)
            capacity_results.append(result)
            
            print(f"  成功率: {result['success_rate']:.1f}%")
            print(f"  平均响应时间: {result.get('avg_response_time', 0):.3f}s")
            print(f"  RPS: {result['requests_per_second']:.1f}")
            
            # 如果成功率低于阈值，停止测试
            if result['success_rate'] < success_threshold:
                print(f"成功率低于{success_threshold}%，停止测试")
                break
            
            # 如果平均响应时间超过5秒，停止测试
            if result.get('avg_response_time', 0) > 5:
                print("平均响应时间超过5秒，停止测试")
                break
            
            time.sleep(2)  # 测试间隔
        
        return capacity_results
    
    def generate_capacity_report(self):
        """生成容量规划报告"""
        print("生成容量规划报告...")
        
        # 基准测试
        baseline = self.concurrent_load_test(1, 10)
        
        # 寻找最大容量
        capacity_results = self.find_max_capacity()
        
        # 找到最佳性能点
        best_performance = max(capacity_results, 
                             key=lambda x: x['requests_per_second'] if x['success_rate'] >= 95 else 0)
        
        report = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'baseline_performance': baseline,
            'capacity_test_results': capacity_results,
            'recommended_capacity': {
                'max_concurrent_users': best_performance['concurrent_users'],
                'max_rps': best_performance['requests_per_second'],
                'avg_response_time': best_performance.get('avg_response_time', 0)
            },
            'recommendations': []
        }
        
        # 生成建议
        if best_performance.get('avg_response_time', 0) > 1:
            report['recommendations'].append(
                "平均响应时间超过1秒，建议优化应用性能"
            )
        
        if best_performance['concurrent_users'] < 10:
            report['recommendations'].append(
                "并发处理能力较低，建议增加服务器资源或优化代码"
            )
        
        if best_performance['success_rate'] < 99:
            report['recommendations'].append(
                "在高负载下成功率下降，建议检查错误处理和资源限制"
            )
        
        return report

if __name__ == '__main__':
    planner = PerformanceCapacityPlanner()
    
    # 检查服务是否可用
    health_check = planner.single_request_test()
    if not health_check['success']:
        print("错误: 服务不可用，请先启动 PC28 系统")
        exit(1)
    
    print("服务健康检查通过，开始容量规划测试...")
    
    report = planner.generate_capacity_report()
    
    print("\n=== PC28 性能容量规划报告 ===")
    print(f"测试时间: {report['timestamp']}")
    
    baseline = report['baseline_performance']
    print(f"\n=== 基准性能 ===")
    print(f"单用户成功率: {baseline['success_rate']:.1f}%")
    print(f"单用户平均响应时间: {baseline.get('avg_response_time', 0):.3f}s")
    
    capacity = report['recommended_capacity']
    print(f"\n=== 推荐容量 ===")
    print(f"最大并发用户数: {capacity['max_concurrent_users']}")
    print(f"最大RPS: {capacity['max_rps']:.1f}")
    print(f"平均响应时间: {capacity['avg_response_time']:.3f}s")
    
    if report['recommendations']:
        print(f"\n=== 优化建议 ===")
        for i, rec in enumerate(report['recommendations'], 1):
            print(f"{i}. {rec}")
```

## 安全运维

### 安全检查清单

```bash
#!/bin/bash
# security_check.sh - 安全检查脚本

echo "=== PC28 系统安全检查 ==="
echo "检查时间: $(date)"
echo

# 1. 文件权限检查
echo "1. 检查关键文件权限..."
check_file_permissions() {
    local file=$1
    local expected=$2
    
    if [ -f "$file" ]; then
        actual=$(stat -c "%a" "$file" 2>/dev/null || stat -f "%A" "$file" 2>/dev/null)
        if [ "$actual" = "$expected" ]; then
            echo "  ✅ $file: $actual (正确)"
        else
            echo "  ❌ $file: $actual (应为 $expected)"
        fi
    else
        echo "  ⚠️  $file: 文件不存在"
    fi
}

check_file_permissions "lottery_data.db" "600"
check_file_permissions "config/app_config.py" "600"
check_file_permissions "logs/system.log" "644"

# 2.