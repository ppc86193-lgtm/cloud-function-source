"""
PC28 系统监控和报告系统
提供实时监控、性能分析、错误跟踪和自动化报告功能
"""

import os
import json
import time
import psutil
import sqlite3
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import defaultdict, deque

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """系统性能指标"""
    timestamp: str
    cpu_percent: float
    memory_percent: float
    memory_used_gb: float
    memory_total_gb: float
    disk_percent: float
    disk_used_gb: float
    disk_total_gb: float
    network_sent_mb: float
    network_recv_mb: float
    active_connections: int
    process_count: int

@dataclass
class DatabaseMetrics:
    """数据库性能指标"""
    timestamp: str
    database_name: str
    table_count: int
    total_records: int
    database_size_mb: float
    query_response_time_ms: float
    active_connections: int
    last_sync_time: Optional[str]
    sync_status: str

@dataclass
class SyncMetrics:
    """同步性能指标"""
    timestamp: str
    sync_id: str
    table_name: str
    records_processed: int
    records_synced: int
    sync_duration_seconds: float
    sync_status: str
    error_count: int
    throughput_records_per_second: float

@dataclass
class AlertEvent:
    """告警事件"""
    timestamp: str
    alert_id: str
    severity: str  # INFO, WARNING, ERROR, CRITICAL
    category: str  # SYSTEM, DATABASE, SYNC, SECURITY
    message: str
    details: Dict[str, Any]
    resolved: bool = False
    resolved_at: Optional[str] = None

class SystemMonitor:
    """系统监控器"""
    
    def __init__(self, monitoring_interval: int = 60):
        """
        初始化系统监控器
        
        Args:
            monitoring_interval: 监控间隔（秒）
        """
        self.monitoring_interval = monitoring_interval
        self.is_monitoring = False
        self.monitoring_thread = None
        
        # 数据存储
        self.metrics_history = deque(maxlen=1440)  # 保留24小时的数据（每分钟一个点）
        self.database_metrics = {}
        self.sync_metrics = deque(maxlen=1000)
        self.alert_events = deque(maxlen=500)
        
        # 告警阈值
        self.alert_thresholds = {
            'cpu_percent': 80.0,
            'memory_percent': 85.0,
            'disk_percent': 90.0,
            'sync_failure_rate': 0.1,  # 10%
            'query_response_time_ms': 5000.0,  # 5秒
            'sync_duration_threshold': 300.0  # 5分钟
        }
        
        # 网络基线（用于计算增量）
        self.network_baseline = None
        
        # 初始化监控数据库
        self._init_monitoring_database()
    
    def _init_monitoring_database(self):
        """初始化监控数据库"""
        self.monitoring_db_path = 'monitoring/monitoring_data.db'
        os.makedirs(os.path.dirname(self.monitoring_db_path), exist_ok=True)
        
        try:
            with sqlite3.connect(self.monitoring_db_path) as conn:
                cursor = conn.cursor()
                
                # 系统指标表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        cpu_percent REAL,
                        memory_percent REAL,
                        memory_used_gb REAL,
                        memory_total_gb REAL,
                        disk_percent REAL,
                        disk_used_gb REAL,
                        disk_total_gb REAL,
                        network_sent_mb REAL,
                        network_recv_mb REAL,
                        active_connections INTEGER,
                        process_count INTEGER
                    )
                ''')
                
                # 数据库指标表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS database_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        database_name TEXT NOT NULL,
                        table_count INTEGER,
                        total_records INTEGER,
                        database_size_mb REAL,
                        query_response_time_ms REAL,
                        active_connections INTEGER,
                        last_sync_time TEXT,
                        sync_status TEXT
                    )
                ''')
                
                # 同步指标表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sync_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        sync_id TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        records_processed INTEGER,
                        records_synced INTEGER,
                        sync_duration_seconds REAL,
                        sync_status TEXT,
                        error_count INTEGER,
                        throughput_records_per_second REAL
                    )
                ''')
                
                # 告警事件表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS alert_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        alert_id TEXT UNIQUE NOT NULL,
                        severity TEXT NOT NULL,
                        category TEXT NOT NULL,
                        message TEXT NOT NULL,
                        details TEXT,
                        resolved BOOLEAN DEFAULT FALSE,
                        resolved_at TEXT
                    )
                ''')
                
                # 创建索引
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON system_metrics(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_database_metrics_timestamp ON database_metrics(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_metrics_timestamp ON sync_metrics(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_alert_events_timestamp ON alert_events(timestamp)')
                
                conn.commit()
                logger.info("Monitoring database initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize monitoring database: {e}")
    
    def collect_system_metrics(self) -> SystemMetrics:
        """收集系统性能指标"""
        try:
            # CPU 使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 内存使用情况
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used_gb = memory.used / (1024**3)
            memory_total_gb = memory.total / (1024**3)
            
            # 磁盘使用情况
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            disk_used_gb = disk.used / (1024**3)
            disk_total_gb = disk.total / (1024**3)
            
            # 网络使用情况
            network = psutil.net_io_counters()
            if self.network_baseline is None:
                self.network_baseline = network
                network_sent_mb = 0
                network_recv_mb = 0
            else:
                network_sent_mb = (network.bytes_sent - self.network_baseline.bytes_sent) / (1024**2)
                network_recv_mb = (network.bytes_recv - self.network_baseline.bytes_recv) / (1024**2)
            
            # 进程和连接数
            process_count = len(psutil.pids())
            
            # 活跃连接数（简化版本）
            try:
                active_connections = len(psutil.net_connections())
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                active_connections = 0
            
            metrics = SystemMetrics(
                timestamp=datetime.now().isoformat(),
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_used_gb=memory_used_gb,
                memory_total_gb=memory_total_gb,
                disk_percent=disk_percent,
                disk_used_gb=disk_used_gb,
                disk_total_gb=disk_total_gb,
                network_sent_mb=network_sent_mb,
                network_recv_mb=network_recv_mb,
                active_connections=active_connections,
                process_count=process_count
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
            return None
    
    def collect_database_metrics(self, db_path: str, db_name: str) -> DatabaseMetrics:
        """收集数据库性能指标"""
        try:
            start_time = time.time()
            
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # 查询响应时间测试
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                table_count = cursor.fetchone()[0]
                
                query_response_time_ms = (time.time() - start_time) * 1000
                
                # 计算总记录数
                total_records = 0
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                for (table_name,) in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]
                        total_records += count
                    except Exception:
                        continue
                
                # 数据库文件大小
                database_size_mb = os.path.getsize(db_path) / (1024**2) if os.path.exists(db_path) else 0
                
                # 获取最后同步时间（如果存在 sync_status 表）
                last_sync_time = None
                sync_status = "unknown"
                try:
                    cursor.execute("SELECT last_sync_time, status FROM sync_status ORDER BY last_sync_time DESC LIMIT 1")
                    result = cursor.fetchone()
                    if result:
                        last_sync_time, sync_status = result
                except Exception:
                    pass
                
                metrics = DatabaseMetrics(
                    timestamp=datetime.now().isoformat(),
                    database_name=db_name,
                    table_count=table_count,
                    total_records=total_records,
                    database_size_mb=database_size_mb,
                    query_response_time_ms=query_response_time_ms,
                    active_connections=1,  # SQLite 单连接
                    last_sync_time=last_sync_time,
                    sync_status=sync_status
                )
                
                return metrics
                
        except Exception as e:
            logger.error(f"Failed to collect database metrics for {db_name}: {e}")
            return None
    
    def record_sync_metrics(self, sync_id: str, table_name: str, 
                          records_processed: int, records_synced: int,
                          sync_duration_seconds: float, sync_status: str,
                          error_count: int = 0):
        """记录同步性能指标"""
        try:
            throughput = records_synced / sync_duration_seconds if sync_duration_seconds > 0 else 0
            
            metrics = SyncMetrics(
                timestamp=datetime.now().isoformat(),
                sync_id=sync_id,
                table_name=table_name,
                records_processed=records_processed,
                records_synced=records_synced,
                sync_duration_seconds=sync_duration_seconds,
                sync_status=sync_status,
                error_count=error_count,
                throughput_records_per_second=throughput
            )
            
            self.sync_metrics.append(metrics)
            self._save_sync_metrics(metrics)
            
            # 检查同步告警
            self._check_sync_alerts(metrics)
            
        except Exception as e:
            logger.error(f"Failed to record sync metrics: {e}")
    
    def create_alert(self, severity: str, category: str, message: str, 
                    details: Dict[str, Any] = None):
        """创建告警事件"""
        try:
            alert_id = f"{category}_{severity}_{int(time.time())}"
            
            alert = AlertEvent(
                timestamp=datetime.now().isoformat(),
                alert_id=alert_id,
                severity=severity,
                category=category,
                message=message,
                details=details or {}
            )
            
            self.alert_events.append(alert)
            self._save_alert_event(alert)
            
            logger.warning(f"Alert created: [{severity}] {category} - {message}")
            
        except Exception as e:
            logger.error(f"Failed to create alert: {e}")
    
    def _check_system_alerts(self, metrics: SystemMetrics):
        """检查系统告警"""
        if metrics.cpu_percent > self.alert_thresholds['cpu_percent']:
            self.create_alert(
                "WARNING", "SYSTEM",
                f"High CPU usage: {metrics.cpu_percent:.1f}%",
                {"cpu_percent": metrics.cpu_percent, "threshold": self.alert_thresholds['cpu_percent']}
            )
        
        if metrics.memory_percent > self.alert_thresholds['memory_percent']:
            self.create_alert(
                "WARNING", "SYSTEM",
                f"High memory usage: {metrics.memory_percent:.1f}%",
                {"memory_percent": metrics.memory_percent, "threshold": self.alert_thresholds['memory_percent']}
            )
        
        if metrics.disk_percent > self.alert_thresholds['disk_percent']:
            self.create_alert(
                "CRITICAL", "SYSTEM",
                f"High disk usage: {metrics.disk_percent:.1f}%",
                {"disk_percent": metrics.disk_percent, "threshold": self.alert_thresholds['disk_percent']}
            )
    
    def _check_database_alerts(self, metrics: DatabaseMetrics):
        """检查数据库告警"""
        if metrics.query_response_time_ms > self.alert_thresholds['query_response_time_ms']:
            self.create_alert(
                "WARNING", "DATABASE",
                f"Slow database query: {metrics.query_response_time_ms:.1f}ms",
                {"database": metrics.database_name, "response_time": metrics.query_response_time_ms}
            )
    
    def _check_sync_alerts(self, metrics: SyncMetrics):
        """检查同步告警"""
        if metrics.sync_status == "failed":
            self.create_alert(
                "ERROR", "SYNC",
                f"Sync failed for table {metrics.table_name}",
                {"table_name": metrics.table_name, "sync_id": metrics.sync_id}
            )
        
        if metrics.sync_duration_seconds > self.alert_thresholds['sync_duration_threshold']:
            self.create_alert(
                "WARNING", "SYNC",
                f"Long sync duration for table {metrics.table_name}: {metrics.sync_duration_seconds:.1f}s",
                {"table_name": metrics.table_name, "duration": metrics.sync_duration_seconds}
            )
    
    def _save_system_metrics(self, metrics: SystemMetrics):
        """保存系统指标到数据库"""
        try:
            with sqlite3.connect(self.monitoring_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO system_metrics (
                        timestamp, cpu_percent, memory_percent, memory_used_gb, memory_total_gb,
                        disk_percent, disk_used_gb, disk_total_gb, network_sent_mb, network_recv_mb,
                        active_connections, process_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metrics.timestamp, metrics.cpu_percent, metrics.memory_percent,
                    metrics.memory_used_gb, metrics.memory_total_gb, metrics.disk_percent,
                    metrics.disk_used_gb, metrics.disk_total_gb, metrics.network_sent_mb,
                    metrics.network_recv_mb, metrics.active_connections, metrics.process_count
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to save system metrics: {e}")
    
    def _save_database_metrics(self, metrics: DatabaseMetrics):
        """保存数据库指标到数据库"""
        try:
            with sqlite3.connect(self.monitoring_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO database_metrics (
                        timestamp, database_name, table_count, total_records, database_size_mb,
                        query_response_time_ms, active_connections, last_sync_time, sync_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metrics.timestamp, metrics.database_name, metrics.table_count,
                    metrics.total_records, metrics.database_size_mb, metrics.query_response_time_ms,
                    metrics.active_connections, metrics.last_sync_time, metrics.sync_status
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to save database metrics: {e}")
    
    def _save_sync_metrics(self, metrics: SyncMetrics):
        """保存同步指标到数据库"""
        try:
            with sqlite3.connect(self.monitoring_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO sync_metrics (
                        timestamp, sync_id, table_name, records_processed, records_synced,
                        sync_duration_seconds, sync_status, error_count, throughput_records_per_second
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metrics.timestamp, metrics.sync_id, metrics.table_name,
                    metrics.records_processed, metrics.records_synced, metrics.sync_duration_seconds,
                    metrics.sync_status, metrics.error_count, metrics.throughput_records_per_second
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to save sync metrics: {e}")
    
    def _save_alert_event(self, alert: AlertEvent):
        """保存告警事件到数据库"""
        try:
            with sqlite3.connect(self.monitoring_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO alert_events (
                        timestamp, alert_id, severity, category, message, details, resolved, resolved_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    alert.timestamp, alert.alert_id, alert.severity, alert.category,
                    alert.message, json.dumps(alert.details), alert.resolved, alert.resolved_at
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to save alert event: {e}")
    
    def start_monitoring(self):
        """启动监控"""
        if self.is_monitoring:
            logger.warning("Monitoring is already running")
            return
        
        self.is_monitoring = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        logger.info("System monitoring started")
    
    def stop_monitoring(self):
        """停止监控"""
        self.is_monitoring = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        logger.info("System monitoring stopped")
    
    def _monitoring_loop(self):
        """监控循环"""
        while self.is_monitoring:
            try:
                # 收集系统指标
                system_metrics = self.collect_system_metrics()
                if system_metrics:
                    self.metrics_history.append(system_metrics)
                    self._save_system_metrics(system_metrics)
                    self._check_system_alerts(system_metrics)
                
                # 收集数据库指标
                db_configs = [
                    ('pc28_data.db', 'PC28_Main'),
                    ('test_pc28_data.db', 'PC28_Test')
                ]
                
                for db_path, db_name in db_configs:
                    if os.path.exists(db_path):
                        db_metrics = self.collect_database_metrics(db_path, db_name)
                        if db_metrics:
                            self.database_metrics[db_name] = db_metrics
                            self._save_database_metrics(db_metrics)
                            self._check_database_alerts(db_metrics)
                
                # 等待下一个监控周期
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.monitoring_interval)
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态摘要"""
        try:
            current_metrics = self.metrics_history[-1] if self.metrics_history else None
            
            # 计算平均值（最近1小时）
            recent_metrics = list(self.metrics_history)[-60:] if len(self.metrics_history) >= 60 else list(self.metrics_history)
            
            avg_cpu = sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics) if recent_metrics else 0
            avg_memory = sum(m.memory_percent for m in recent_metrics) / len(recent_metrics) if recent_metrics else 0
            
            # 统计告警
            recent_alerts = [a for a in self.alert_events if not a.resolved]
            alert_counts = defaultdict(int)
            for alert in recent_alerts:
                alert_counts[alert.severity] += 1
            
            # 同步状态
            recent_syncs = list(self.sync_metrics)[-10:] if self.sync_metrics else []
            successful_syncs = sum(1 for s in recent_syncs if s.sync_status == "success")
            sync_success_rate = successful_syncs / len(recent_syncs) if recent_syncs else 0
            
            status = {
                'timestamp': datetime.now().isoformat(),
                'system': {
                    'status': 'healthy' if current_metrics and current_metrics.cpu_percent < 80 and current_metrics.memory_percent < 85 else 'warning',
                    'cpu_percent': current_metrics.cpu_percent if current_metrics else 0,
                    'memory_percent': current_metrics.memory_percent if current_metrics else 0,
                    'disk_percent': current_metrics.disk_percent if current_metrics else 0,
                    'avg_cpu_1h': avg_cpu,
                    'avg_memory_1h': avg_memory
                },
                'database': {
                    'databases': len(self.database_metrics),
                    'status': 'healthy',  # 简化版本
                    'total_tables': sum(db.table_count for db in self.database_metrics.values()),
                    'total_records': sum(db.total_records for db in self.database_metrics.values())
                },
                'sync': {
                    'recent_syncs': len(recent_syncs),
                    'success_rate': sync_success_rate,
                    'status': 'healthy' if sync_success_rate > 0.9 else 'warning'
                },
                'alerts': {
                    'total_active': len(recent_alerts),
                    'critical': alert_counts['CRITICAL'],
                    'error': alert_counts['ERROR'],
                    'warning': alert_counts['WARNING'],
                    'info': alert_counts['INFO']
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {'error': str(e)}
    
    def generate_monitoring_report(self, hours: int = 24) -> str:
        """生成监控报告"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # 查询历史数据
            with sqlite3.connect(self.monitoring_db_path) as conn:
                cursor = conn.cursor()
                
                # 系统指标统计
                cursor.execute('''
                    SELECT AVG(cpu_percent), AVG(memory_percent), AVG(disk_percent),
                           MAX(cpu_percent), MAX(memory_percent), MAX(disk_percent)
                    FROM system_metrics 
                    WHERE timestamp >= ?
                ''', (start_time.isoformat(),))
                
                system_stats = cursor.fetchone()
                
                # 同步统计
                cursor.execute('''
                    SELECT COUNT(*), 
                           SUM(CASE WHEN sync_status = 'success' THEN 1 ELSE 0 END),
                           AVG(sync_duration_seconds),
                           SUM(records_synced)
                    FROM sync_metrics 
                    WHERE timestamp >= ?
                ''', (start_time.isoformat(),))
                
                sync_stats = cursor.fetchone()
                
                # 告警统计
                cursor.execute('''
                    SELECT severity, COUNT(*) 
                    FROM alert_events 
                    WHERE timestamp >= ? 
                    GROUP BY severity
                ''', (start_time.isoformat(),))
                
                alert_stats = dict(cursor.fetchall())
            
            # 生成报告
            report_lines = [
                f"# PC28 系统监控报告",
                f"",
                f"**报告时间**: {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
                f"**统计周期**: 最近 {hours} 小时",
                f"",
                f"## 系统性能概览",
                f"",
                f"### CPU 使用率",
                f"- 平均: {system_stats[0]:.1f}%" if system_stats[0] else "- 平均: N/A",
                f"- 峰值: {system_stats[3]:.1f}%" if system_stats[3] else "- 峰值: N/A",
                f"",
                f"### 内存使用率",
                f"- 平均: {system_stats[1]:.1f}%" if system_stats[1] else "- 平均: N/A",
                f"- 峰值: {system_stats[4]:.1f}%" if system_stats[4] else "- 峰值: N/A",
                f"",
                f"### 磁盘使用率",
                f"- 平均: {system_stats[2]:.1f}%" if system_stats[2] else "- 平均: N/A",
                f"- 峰值: {system_stats[5]:.1f}%" if system_stats[5] else "- 峰值: N/A",
                f"",
                f"## 数据同步统计",
                f"",
                f"- 总同步次数: {sync_stats[0] or 0}",
                f"- 成功次数: {sync_stats[1] or 0}",
                f"- 成功率: {(sync_stats[1] / sync_stats[0] * 100):.1f}%" if sync_stats[0] and sync_stats[1] else "- 成功率: N/A",
                f"- 平均同步时间: {sync_stats[2]:.1f}秒" if sync_stats[2] else "- 平均同步时间: N/A",
                f"- 总同步记录数: {sync_stats[3] or 0}",
                f"",
                f"## 告警统计",
                f""
            ]
            
            if alert_stats:
                for severity, count in alert_stats.items():
                    report_lines.append(f"- {severity}: {count} 次")
            else:
                report_lines.append("- 无告警事件")
            
            report_lines.extend([
                f"",
                f"## 系统健康状态",
                f"",
                f"- 系统状态: {'🟢 健康' if (system_stats[3] or 0) < 80 and (system_stats[4] or 0) < 85 else '🟡 需要关注'}",
                f"- 同步状态: {'🟢 正常' if (sync_stats[1] or 0) / (sync_stats[0] or 1) > 0.9 else '🟡 需要关注'}",
                f"- 告警状态: {'🟢 正常' if alert_stats.get('CRITICAL', 0) == 0 and alert_stats.get('ERROR', 0) == 0 else '🔴 需要处理'}",
                f"",
                f"---",
                f"*报告由 PC28 监控系统自动生成*"
            ])
            
            return "\n".join(report_lines)
            
        except Exception as e:
            logger.error(f"Failed to generate monitoring report: {e}")
            return f"报告生成失败: {e}"


# 全局监控实例
system_monitor = SystemMonitor()

# 便捷函数
def start_system_monitoring():
    """启动系统监控"""
    system_monitor.start_monitoring()

def stop_system_monitoring():
    """停止系统监控"""
    system_monitor.stop_monitoring()

def get_current_status() -> Dict[str, Any]:
    """获取当前系统状态"""
    return system_monitor.get_system_status()

def record_sync_event(sync_id: str, table_name: str, records_processed: int, 
                     records_synced: int, duration: float, status: str, errors: int = 0):
    """记录同步事件"""
    system_monitor.record_sync_metrics(
        sync_id, table_name, records_processed, records_synced, 
        duration, status, errors
    )

def create_system_alert(severity: str, category: str, message: str, details: Dict[str, Any] = None):
    """创建系统告警"""
    system_monitor.create_alert(severity, category, message, details)


if __name__ == "__main__":
    # 演示用法
    print("📊 PC28 System Monitor Demo")
    
    # 启动监控
    start_system_monitoring()
    
    # 等待一些数据收集
    print("⏳ Collecting metrics for 10 seconds...")
    time.sleep(10)
    
    # 获取系统状态
    status = get_current_status()
    print(f"\n🖥️ System Status:")
    print(f"  CPU: {status['system']['cpu_percent']:.1f}%")
    print(f"  Memory: {status['system']['memory_percent']:.1f}%")
    print(f"  Disk: {status['system']['disk_percent']:.1f}%")
    
    # 模拟一些同步事件
    print("\n🔄 Simulating sync events...")
    record_sync_event("sync_001", "test_table", 1000, 1000, 5.2, "success")
    record_sync_event("sync_002", "test_table2", 500, 450, 12.8, "partial", 50)
    
    # 创建测试告警
    create_system_alert("WARNING", "SYSTEM", "Test alert for demonstration", 
                       {"test_value": 123, "threshold": 100})
    
    # 生成报告
    print("\n📄 Generating monitoring report...")
    report = system_monitor.generate_monitoring_report(1)  # 最近1小时
    
    # 保存报告
    report_path = 'monitoring/demo_report.md'
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"✅ Report saved to: {report_path}")
    print("\n📊 Report preview:")
    print(report[:500] + "..." if len(report) > 500 else report)
    
    # 停止监控
    stop_system_monitoring()
    print("\n🛑 Monitoring stopped")