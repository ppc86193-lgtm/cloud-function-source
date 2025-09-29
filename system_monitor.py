#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统监控和维护机制
持续监控系统运行状态、性能指标、资源使用情况和异常检测
"""

import psutil
import sqlite3
import threading
import time
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import os
import gc

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """系统指标数据结构"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_available_mb: float
    disk_usage_percent: float
    disk_free_gb: float
    network_bytes_sent: int
    network_bytes_recv: int
    process_count: int
    thread_count: int
    load_average: Optional[Tuple[float, float, float]] = None
    
@dataclass
class DatabaseMetrics:
    """数据库指标数据结构"""
    timestamp: datetime
    db_path: str
    db_size_mb: float
    table_count: int
    record_count: int
    connection_count: int
    query_response_time_ms: float
    last_backup_time: Optional[datetime] = None
    integrity_check_passed: bool = True
    
@dataclass
class ServiceMetrics:
    """服务指标数据结构"""
    timestamp: datetime
    service_name: str
    status: str  # running, stopped, error
    uptime_seconds: float
    request_count: int
    error_count: int
    success_rate: float
    avg_response_time_ms: float
    memory_usage_mb: float
    cpu_usage_percent: float
    
@dataclass
class AlertRule:
    """告警规则"""
    rule_id: str
    name: str
    metric_type: str  # system, database, service
    metric_name: str
    operator: str  # >, <, >=, <=, ==, !=
    threshold: float
    severity: str  # critical, warning, info
    enabled: bool = True
    consecutive_count: int = 1  # 连续触发次数
    
@dataclass
class Alert:
    """告警信息"""
    alert_id: str
    rule_id: str
    timestamp: datetime
    severity: str
    message: str
    metric_value: float
    threshold: float
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    
class SystemMonitor:
    """系统监控器"""
    
    def __init__(self, monitor_db_path: str = "system_monitor.db"):
        self.monitor_db_path = monitor_db_path
        self.monitoring = False
        self.monitor_thread = None
        self.lock = threading.RLock()
        
        # 监控间隔（秒）
        self.system_monitor_interval = 30
        self.database_monitor_interval = 60
        self.service_monitor_interval = 45
        
        # 数据保留期（天）
        self.metrics_retention_days = 30
        self.alerts_retention_days = 90
        
        # 告警规则
        self.alert_rules = []
        self.active_alerts = {}
        
        # 服务列表
        self.monitored_services = [
            "real_api_data_system",
            "data_deduplication_system", 
            "online_data_validator",
            "historical_data_protection",
            "database_performance_optimizer"
        ]
        
        # 数据库路径列表
        self.monitored_databases = [
            "lottery_data.db",
            "deduplication.db",
            "validation.db",
            "protection.db",
            "performance.db"
        ]
        
        self._init_database()
        self._init_default_alert_rules()
        
    def _init_database(self):
        """初始化监控数据库"""
        try:
            with sqlite3.connect(self.monitor_db_path) as conn:
                # 系统指标表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        cpu_percent REAL NOT NULL,
                        memory_percent REAL NOT NULL,
                        memory_used_mb REAL NOT NULL,
                        memory_available_mb REAL NOT NULL,
                        disk_usage_percent REAL NOT NULL,
                        disk_free_gb REAL NOT NULL,
                        network_bytes_sent INTEGER NOT NULL,
                        network_bytes_recv INTEGER NOT NULL,
                        process_count INTEGER NOT NULL,
                        thread_count INTEGER NOT NULL,
                        load_average TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 数据库指标表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS database_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        db_path TEXT NOT NULL,
                        db_size_mb REAL NOT NULL,
                        table_count INTEGER NOT NULL,
                        record_count INTEGER NOT NULL,
                        connection_count INTEGER NOT NULL,
                        query_response_time_ms REAL NOT NULL,
                        last_backup_time DATETIME,
                        integrity_check_passed BOOLEAN NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 服务指标表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS service_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        service_name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        uptime_seconds REAL NOT NULL,
                        request_count INTEGER NOT NULL,
                        error_count INTEGER NOT NULL,
                        success_rate REAL NOT NULL,
                        avg_response_time_ms REAL NOT NULL,
                        memory_usage_mb REAL NOT NULL,
                        cpu_usage_percent REAL NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 告警规则表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS alert_rules (
                        rule_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        metric_type TEXT NOT NULL,
                        metric_name TEXT NOT NULL,
                        operator TEXT NOT NULL,
                        threshold REAL NOT NULL,
                        severity TEXT NOT NULL,
                        enabled BOOLEAN NOT NULL,
                        consecutive_count INTEGER NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 告警记录表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        alert_id TEXT PRIMARY KEY,
                        rule_id TEXT NOT NULL,
                        timestamp DATETIME NOT NULL,
                        severity TEXT NOT NULL,
                        message TEXT NOT NULL,
                        metric_value REAL NOT NULL,
                        threshold REAL NOT NULL,
                        resolved BOOLEAN NOT NULL,
                        resolved_at DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (rule_id) REFERENCES alert_rules (rule_id)
                    )
                """)
                
                # 创建索引
                conn.execute("CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON system_metrics(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_database_metrics_timestamp ON database_metrics(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_service_metrics_timestamp ON service_metrics(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity)")
                
                conn.commit()
                logger.info("监控数据库初始化完成")
        except Exception as e:
            logger.error(f"监控数据库初始化失败: {e}")
            raise
    
    def _init_default_alert_rules(self):
        """初始化默认告警规则"""
        default_rules = [
            AlertRule("cpu_high", "CPU使用率过高", "system", "cpu_percent", ">", 80.0, "warning", True, 3),
            AlertRule("cpu_critical", "CPU使用率严重过高", "system", "cpu_percent", ">", 95.0, "critical", True, 2),
            AlertRule("memory_high", "内存使用率过高", "system", "memory_percent", ">", 85.0, "warning", True, 3),
            AlertRule("memory_critical", "内存使用率严重过高", "system", "memory_percent", ">", 95.0, "critical", True, 2),
            AlertRule("disk_high", "磁盘使用率过高", "system", "disk_usage_percent", ">", 90.0, "warning", True, 2),
            AlertRule("disk_critical", "磁盘使用率严重过高", "system", "disk_usage_percent", ">", 95.0, "critical", True, 1),
            AlertRule("db_response_slow", "数据库响应缓慢", "database", "query_response_time_ms", ">", 1000.0, "warning", True, 3),
            AlertRule("db_response_critical", "数据库响应严重缓慢", "database", "query_response_time_ms", ">", 5000.0, "critical", True, 2),
            AlertRule("service_error_rate", "服务错误率过高", "service", "success_rate", "<", 95.0, "warning", True, 3),
            AlertRule("service_down", "服务停止", "service", "status", "==", "stopped", "critical", True, 1)
        ]
        
        self.alert_rules = default_rules
        self._save_alert_rules()
    
    def _save_alert_rules(self):
        """保存告警规则到数据库"""
        try:
            with sqlite3.connect(self.monitor_db_path) as conn:
                for rule in self.alert_rules:
                    conn.execute("""
                        INSERT OR REPLACE INTO alert_rules 
                        (rule_id, name, metric_type, metric_name, operator, threshold, severity, enabled, consecutive_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        rule.rule_id, rule.name, rule.metric_type, rule.metric_name,
                        rule.operator, rule.threshold, rule.severity, rule.enabled, rule.consecutive_count
                    ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存告警规则失败: {e}")
    
    def collect_system_metrics(self) -> SystemMetrics:
        """收集系统指标"""
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 内存信息
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used_mb = memory.used / 1024 / 1024
            memory_available_mb = memory.available / 1024 / 1024
            
            # 磁盘信息
            disk = psutil.disk_usage('/')
            disk_usage_percent = disk.percent
            disk_free_gb = disk.free / 1024 / 1024 / 1024
            
            # 网络信息
            network = psutil.net_io_counters()
            network_bytes_sent = network.bytes_sent
            network_bytes_recv = network.bytes_recv
            
            # 进程信息
            process_count = len(psutil.pids())
            thread_count = sum(p.num_threads() for p in psutil.process_iter(['num_threads']) if p.info['num_threads'])
            
            # 负载平均值（仅Unix系统）
            load_average = None
            try:
                if hasattr(os, 'getloadavg'):
                    load_average = os.getloadavg()
            except:
                pass
            
            return SystemMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_used_mb=memory_used_mb,
                memory_available_mb=memory_available_mb,
                disk_usage_percent=disk_usage_percent,
                disk_free_gb=disk_free_gb,
                network_bytes_sent=network_bytes_sent,
                network_bytes_recv=network_bytes_recv,
                process_count=process_count,
                thread_count=thread_count,
                load_average=load_average
            )
        except Exception as e:
            logger.error(f"收集系统指标失败: {e}")
            return None
    
    def collect_database_metrics(self, db_path: str) -> Optional[DatabaseMetrics]:
        """收集数据库指标"""
        try:
            if not Path(db_path).exists():
                return None
            
            # 数据库文件大小
            db_size_mb = Path(db_path).stat().st_size / 1024 / 1024
            
            # 查询响应时间测试
            start_time = time.time()
            with sqlite3.connect(db_path) as conn:
                # 获取表数量
                cursor = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                table_count = cursor.fetchone()[0]
                
                # 获取记录总数（估算）
                record_count = 0
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                for table in tables:
                    try:
                        cursor = conn.execute(f"SELECT COUNT(*) FROM {table[0]}")
                        record_count += cursor.fetchone()[0]
                    except:
                        pass
                
                # 完整性检查
                cursor = conn.execute("PRAGMA integrity_check")
                integrity_result = cursor.fetchone()[0]
                integrity_check_passed = integrity_result == "ok"
            
            query_response_time_ms = (time.time() - start_time) * 1000
            
            return DatabaseMetrics(
                timestamp=datetime.now(),
                db_path=db_path,
                db_size_mb=db_size_mb,
                table_count=table_count,
                record_count=record_count,
                connection_count=1,  # 简化处理
                query_response_time_ms=query_response_time_ms,
                integrity_check_passed=integrity_check_passed
            )
        except Exception as e:
            logger.error(f"收集数据库指标失败 {db_path}: {e}")
            return None
    
    def collect_service_metrics(self, service_name: str) -> Optional[ServiceMetrics]:
        """收集服务指标"""
        try:
            # 简化的服务监控，实际应用中需要根据具体服务实现
            # 这里模拟服务状态检查
            
            status = "running"  # 默认运行状态
            uptime_seconds = time.time() % 86400  # 模拟运行时间
            request_count = 100  # 模拟请求数
            error_count = 2  # 模拟错误数
            success_rate = (request_count - error_count) / request_count * 100
            avg_response_time_ms = 150.0  # 模拟响应时间
            memory_usage_mb = 50.0  # 模拟内存使用
            cpu_usage_percent = 5.0  # 模拟CPU使用
            
            return ServiceMetrics(
                timestamp=datetime.now(),
                service_name=service_name,
                status=status,
                uptime_seconds=uptime_seconds,
                request_count=request_count,
                error_count=error_count,
                success_rate=success_rate,
                avg_response_time_ms=avg_response_time_ms,
                memory_usage_mb=memory_usage_mb,
                cpu_usage_percent=cpu_usage_percent
            )
        except Exception as e:
            logger.error(f"收集服务指标失败 {service_name}: {e}")
            return None
    
    def save_metrics(self, metrics: Any, metrics_type: str):
        """保存指标到数据库"""
        try:
            with sqlite3.connect(self.monitor_db_path) as conn:
                if metrics_type == "system" and isinstance(metrics, SystemMetrics):
                    conn.execute("""
                        INSERT INTO system_metrics 
                        (timestamp, cpu_percent, memory_percent, memory_used_mb, memory_available_mb,
                         disk_usage_percent, disk_free_gb, network_bytes_sent, network_bytes_recv,
                         process_count, thread_count, load_average)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        metrics.timestamp, metrics.cpu_percent, metrics.memory_percent,
                        metrics.memory_used_mb, metrics.memory_available_mb, metrics.disk_usage_percent,
                        metrics.disk_free_gb, metrics.network_bytes_sent, metrics.network_bytes_recv,
                        metrics.process_count, metrics.thread_count, json.dumps(metrics.load_average)
                    ))
                
                elif metrics_type == "database" and isinstance(metrics, DatabaseMetrics):
                    conn.execute("""
                        INSERT INTO database_metrics 
                        (timestamp, db_path, db_size_mb, table_count, record_count, connection_count,
                         query_response_time_ms, last_backup_time, integrity_check_passed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        metrics.timestamp, metrics.db_path, metrics.db_size_mb, metrics.table_count,
                        metrics.record_count, metrics.connection_count, metrics.query_response_time_ms,
                        metrics.last_backup_time, metrics.integrity_check_passed
                    ))
                
                elif metrics_type == "service" and isinstance(metrics, ServiceMetrics):
                    conn.execute("""
                        INSERT INTO service_metrics 
                        (timestamp, service_name, status, uptime_seconds, request_count, error_count,
                         success_rate, avg_response_time_ms, memory_usage_mb, cpu_usage_percent)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        metrics.timestamp, metrics.service_name, metrics.status, metrics.uptime_seconds,
                        metrics.request_count, metrics.error_count, metrics.success_rate,
                        metrics.avg_response_time_ms, metrics.memory_usage_mb, metrics.cpu_usage_percent
                    ))
                
                conn.commit()
        except Exception as e:
            logger.error(f"保存指标失败: {e}")
    
    def check_alerts(self, metrics: Any, metrics_type: str):
        """检查告警条件"""
        try:
            for rule in self.alert_rules:
                if not rule.enabled or rule.metric_type != metrics_type:
                    continue
                
                # 获取指标值
                metric_value = getattr(metrics, rule.metric_name, None)
                if metric_value is None:
                    continue
                
                # 检查告警条件
                triggered = False
                if rule.operator == ">" and metric_value > rule.threshold:
                    triggered = True
                elif rule.operator == "<" and metric_value < rule.threshold:
                    triggered = True
                elif rule.operator == ">=" and metric_value >= rule.threshold:
                    triggered = True
                elif rule.operator == "<=" and metric_value <= rule.threshold:
                    triggered = True
                elif rule.operator == "==" and metric_value == rule.threshold:
                    triggered = True
                elif rule.operator == "!=" and metric_value != rule.threshold:
                    triggered = True
                
                if triggered:
                    self._handle_alert(rule, metric_value, metrics.timestamp)
                else:
                    self._resolve_alert(rule.rule_id)
        except Exception as e:
            logger.error(f"检查告警失败: {e}")
    
    def _handle_alert(self, rule: AlertRule, metric_value: float, timestamp: datetime):
        """处理告警"""
        try:
            alert_key = f"{rule.rule_id}_{rule.metric_name}"
            
            # 检查是否已有活跃告警
            if alert_key in self.active_alerts:
                self.active_alerts[alert_key]['count'] += 1
            else:
                self.active_alerts[alert_key] = {
                    'rule': rule,
                    'count': 1,
                    'first_triggered': timestamp
                }
            
            # 检查是否达到连续触发次数
            if self.active_alerts[alert_key]['count'] >= rule.consecutive_count:
                alert_id = f"{rule.rule_id}_{int(timestamp.timestamp())}"
                message = f"{rule.name}: {rule.metric_name}={metric_value}, 阈值={rule.threshold}"
                
                alert = Alert(
                    alert_id=alert_id,
                    rule_id=rule.rule_id,
                    timestamp=timestamp,
                    severity=rule.severity,
                    message=message,
                    metric_value=metric_value,
                    threshold=rule.threshold
                )
                
                self._save_alert(alert)
                logger.warning(f"告警触发: {message}")
                
                # 重置计数器
                self.active_alerts[alert_key]['count'] = 0
        except Exception as e:
            logger.error(f"处理告警失败: {e}")
    
    def _resolve_alert(self, rule_id: str):
        """解决告警"""
        try:
            # 清除活跃告警计数
            keys_to_remove = [key for key in self.active_alerts.keys() if key.startswith(rule_id)]
            for key in keys_to_remove:
                del self.active_alerts[key]
        except Exception as e:
            logger.error(f"解决告警失败: {e}")
    
    def _save_alert(self, alert: Alert):
        """保存告警到数据库"""
        try:
            with sqlite3.connect(self.monitor_db_path) as conn:
                conn.execute("""
                    INSERT INTO alerts 
                    (alert_id, rule_id, timestamp, severity, message, metric_value, threshold, resolved)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    alert.alert_id, alert.rule_id, alert.timestamp, alert.severity,
                    alert.message, alert.metric_value, alert.threshold, alert.resolved
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存告警失败: {e}")
    
    def start_monitoring(self):
        """启动监控"""
        if self.monitoring:
            logger.warning("监控已在运行中")
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("系统监控已启动")
    
    def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("系统监控已停止")
    
    def _monitor_loop(self):
        """监控主循环"""
        last_system_check = 0
        last_database_check = 0
        last_service_check = 0
        
        while self.monitoring:
            try:
                current_time = time.time()
                
                # 系统指标监控
                if current_time - last_system_check >= self.system_monitor_interval:
                    metrics = self.collect_system_metrics()
                    if metrics:
                        self.save_metrics(metrics, "system")
                        self.check_alerts(metrics, "system")
                    last_system_check = current_time
                
                # 数据库指标监控
                if current_time - last_database_check >= self.database_monitor_interval:
                    for db_path in self.monitored_databases:
                        metrics = self.collect_database_metrics(db_path)
                        if metrics:
                            self.save_metrics(metrics, "database")
                            self.check_alerts(metrics, "database")
                    last_database_check = current_time
                
                # 服务指标监控
                if current_time - last_service_check >= self.service_monitor_interval:
                    for service_name in self.monitored_services:
                        metrics = self.collect_service_metrics(service_name)
                        if metrics:
                            self.save_metrics(metrics, "service")
                            self.check_alerts(metrics, "service")
                    last_service_check = current_time
                
                # 清理过期数据
                self._cleanup_old_data()
                
                time.sleep(10)  # 主循环间隔
                
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                time.sleep(30)  # 异常时等待更长时间
    
    def _cleanup_old_data(self):
        """清理过期数据"""
        try:
            with sqlite3.connect(self.monitor_db_path) as conn:
                # 清理过期指标数据
                cutoff_date = datetime.now() - timedelta(days=self.metrics_retention_days)
                conn.execute("DELETE FROM system_metrics WHERE timestamp < ?", (cutoff_date,))
                conn.execute("DELETE FROM database_metrics WHERE timestamp < ?", (cutoff_date,))
                conn.execute("DELETE FROM service_metrics WHERE timestamp < ?", (cutoff_date,))
                
                # 清理过期告警数据
                alert_cutoff_date = datetime.now() - timedelta(days=self.alerts_retention_days)
                conn.execute("DELETE FROM alerts WHERE timestamp < ?", (alert_cutoff_date,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"清理过期数据失败: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态摘要"""
        try:
            with sqlite3.connect(self.monitor_db_path) as conn:
                # 最新系统指标
                cursor = conn.execute("""
                    SELECT * FROM system_metrics 
                    ORDER BY timestamp DESC LIMIT 1
                """)
                latest_system = cursor.fetchone()
                
                # 活跃告警数量
                cursor = conn.execute("""
                    SELECT severity, COUNT(*) FROM alerts 
                    WHERE resolved = 0 AND timestamp > datetime('now', '-1 hour')
                    GROUP BY severity
                """)
                alert_counts = dict(cursor.fetchall())
                
                # 数据库状态
                cursor = conn.execute("""
                    SELECT db_path, integrity_check_passed, query_response_time_ms
                    FROM database_metrics 
                    WHERE timestamp > datetime('now', '-1 hour')
                    ORDER BY timestamp DESC
                """)
                db_status = cursor.fetchall()
                
                return {
                    'monitoring_active': self.monitoring,
                    'latest_system_metrics': latest_system,
                    'active_alerts': alert_counts,
                    'database_status': db_status,
                    'monitored_services': len(self.monitored_services),
                    'monitored_databases': len(self.monitored_databases)
                }
        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")
            return {}
    
    def generate_health_report(self) -> Dict[str, Any]:
        """生成健康报告"""
        try:
            with sqlite3.connect(self.monitor_db_path) as conn:
                report = {
                    'timestamp': datetime.now().isoformat(),
                    'monitoring_status': 'active' if self.monitoring else 'inactive'
                }
                
                # 系统健康评分
                cursor = conn.execute("""
                    SELECT AVG(cpu_percent), AVG(memory_percent), AVG(disk_usage_percent)
                    FROM system_metrics 
                    WHERE timestamp > datetime('now', '-1 hour')
                """)
                avg_metrics = cursor.fetchone()
                
                if avg_metrics and all(x is not None for x in avg_metrics):
                    cpu_score = max(0, 100 - avg_metrics[0])
                    memory_score = max(0, 100 - avg_metrics[1])
                    disk_score = max(0, 100 - avg_metrics[2])
                    system_health_score = (cpu_score + memory_score + disk_score) / 3
                    
                    report['system_health'] = {
                        'score': round(system_health_score, 2),
                        'cpu_avg': round(avg_metrics[0], 2),
                        'memory_avg': round(avg_metrics[1], 2),
                        'disk_avg': round(avg_metrics[2], 2)
                    }
                
                # 告警统计
                cursor = conn.execute("""
                    SELECT severity, COUNT(*) FROM alerts 
                    WHERE timestamp > datetime('now', '-24 hours')
                    GROUP BY severity
                """)
                alert_stats = dict(cursor.fetchall())
                report['alerts_24h'] = alert_stats
                
                # 数据库健康
                cursor = conn.execute("""
                    SELECT db_path, 
                           AVG(query_response_time_ms) as avg_response,
                           MIN(integrity_check_passed) as integrity_ok
                    FROM database_metrics 
                    WHERE timestamp > datetime('now', '-1 hour')
                    GROUP BY db_path
                """)
                db_health = cursor.fetchall()
                report['database_health'] = db_health
                
                # 服务健康
                cursor = conn.execute("""
                    SELECT service_name,
                           AVG(success_rate) as avg_success_rate,
                           AVG(avg_response_time_ms) as avg_response_time
                    FROM service_metrics 
                    WHERE timestamp > datetime('now', '-1 hour')
                    GROUP BY service_name
                """)
                service_health = cursor.fetchall()
                report['service_health'] = service_health
                
                return report
        except Exception as e:
            logger.error(f"生成健康报告失败: {e}")
            return {'error': str(e)}

def main():
    """测试系统监控器"""
    print("=== 系统监控器测试 ===")
    
    # 创建监控器
    monitor = SystemMonitor()
    
    try:
        # 启动监控
        monitor.start_monitoring()
        print("监控已启动，运行30秒...")
        
        # 运行一段时间
        time.sleep(30)
        
        # 获取系统状态
        status = monitor.get_system_status()
        print(f"\n系统状态: {json.dumps(status, indent=2, default=str)}")
        
        # 生成健康报告
        report = monitor.generate_health_report()
        print(f"\n健康报告: {json.dumps(report, indent=2, default=str)}")
        
    finally:
        # 停止监控
        monitor.stop_monitoring()
        print("\n监控已停止")
    
    print("\n=== 系统监控器测试完成 ===")

if __name__ == "__main__":
    main()