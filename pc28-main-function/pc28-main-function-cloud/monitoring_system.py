#!/usr/bin/env python3
"""
定时同步和监控系统
提供定时数据同步、监控和报警功能
"""

import json
import os
import time
import schedule
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import sqlite3
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import subprocess
import psutil
from concurrent.futures import ThreadPoolExecutor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/a606/cloud_function_source/logs/monitoring_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MonitoringSystem:
    def __init__(self):
        self.local_data_dir = "/Users/a606/cloud_function_source/local_data"
        self.logs_dir = "/Users/a606/cloud_function_source/logs"
        self.monitoring_db_path = "/Users/a606/cloud_function_source/local_data/monitoring.db"
        
        # 确保目录存在
        os.makedirs(self.local_data_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # 监控配置
        self.config = {
            "sync_interval_minutes": 60,  # 同步间隔（分钟）
            "consistency_check_interval_hours": 6,  # 一致性检查间隔（小时）
            "health_check_interval_minutes": 15,  # 健康检查间隔（分钟）
            "cleanup_interval_days": 7,  # 清理间隔（天）
            "max_log_size_mb": 100,  # 最大日志文件大小（MB）
            "max_data_age_days": 30,  # 最大数据保留天数
            "alert_thresholds": {
                "disk_usage_percent": 85,
                "memory_usage_percent": 90,
                "failed_sync_count": 3,
                "consistency_failure_rate": 0.2
            }
        }
        
        # 初始化监控数据库
        self.init_monitoring_database()
        
        # 运行状态
        self.is_running = False
        self.scheduler_thread = None
        
    def init_monitoring_database(self):
        """初始化监控数据库"""
        try:
            conn = sqlite3.connect(self.monitoring_db_path)
            cursor = conn.cursor()
            
            # 创建同步任务记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    duration_seconds REAL,
                    status TEXT,
                    tables_processed INTEGER,
                    tables_successful INTEGER,
                    tables_failed INTEGER,
                    error_message TEXT,
                    created_at TEXT
                )
            ''')
            
            # 创建系统健康记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_health (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_time TEXT,
                    cpu_usage_percent REAL,
                    memory_usage_percent REAL,
                    disk_usage_percent REAL,
                    disk_free_gb REAL,
                    active_processes INTEGER,
                    network_status TEXT,
                    bigquery_status TEXT,
                    created_at TEXT
                )
            ''')
            
            # 创建告警记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_type TEXT,
                    severity TEXT,
                    message TEXT,
                    details TEXT,
                    resolved BOOLEAN,
                    alert_time TEXT,
                    resolved_time TEXT,
                    created_at TEXT
                )
            ''')
            
            # 创建性能指标表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_time TEXT,
                    sync_success_rate REAL,
                    avg_sync_duration REAL,
                    consistency_success_rate REAL,
                    data_freshness_hours REAL,
                    total_data_size_mb REAL,
                    created_at TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("监控数据库初始化完成")
            
        except Exception as e:
            logger.error(f"初始化监控数据库失败: {e}")
    
    def record_sync_task(self, task_type: str, start_time: datetime, 
                        end_time: datetime, status: str, 
                        tables_processed: int = 0, tables_successful: int = 0, 
                        tables_failed: int = 0, error_message: str = None):
        """记录同步任务"""
        try:
            conn = sqlite3.connect(self.monitoring_db_path)
            cursor = conn.cursor()
            
            duration = (end_time - start_time).total_seconds()
            
            cursor.execute('''
                INSERT INTO sync_tasks 
                (task_type, start_time, end_time, duration_seconds, status,
                 tables_processed, tables_successful, tables_failed, error_message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                task_type,
                start_time.isoformat(),
                end_time.isoformat(),
                duration,
                status,
                tables_processed,
                tables_successful,
                tables_failed,
                error_message,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"记录同步任务失败: {e}")
    
    def record_system_health(self):
        """记录系统健康状态"""
        try:
            # 获取系统指标
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # 检查网络状态（简单ping测试）
            network_status = "healthy"
            try:
                result = subprocess.run(['ping', '-c', '1', '8.8.8.8'], 
                                      capture_output=True, timeout=5)
                if result.returncode != 0:
                    network_status = "degraded"
            except:
                network_status = "failed"
            
            # 检查BigQuery状态（尝试简单查询）
            bigquery_status = "healthy"
            try:
                from google.cloud import bigquery
                client = bigquery.Client(project="wprojectl")
                query = "SELECT 1 as test"
                job = client.query(query)
                list(job.result())
            except Exception as e:
                bigquery_status = f"failed: {str(e)[:100]}"
            
            # 记录到数据库
            conn = sqlite3.connect(self.monitoring_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO system_health 
                (check_time, cpu_usage_percent, memory_usage_percent, disk_usage_percent,
                 disk_free_gb, active_processes, network_status, bigquery_status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now().isoformat(),
                cpu_percent,
                memory.percent,
                disk.percent,
                disk.free / (1024**3),  # GB
                len(psutil.pids()),
                network_status,
                bigquery_status,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
            # 检查告警阈值
            self.check_health_alerts(cpu_percent, memory.percent, disk.percent, 
                                   network_status, bigquery_status)
            
            logger.info(f"系统健康检查完成 - CPU: {cpu_percent:.1f}%, "
                       f"内存: {memory.percent:.1f}%, 磁盘: {disk.percent:.1f}%")
            
        except Exception as e:
            logger.error(f"记录系统健康状态失败: {e}")
    
    def check_health_alerts(self, cpu_percent: float, memory_percent: float, 
                           disk_percent: float, network_status: str, bigquery_status: str):
        """检查健康状态告警"""
        alerts = []
        
        # CPU使用率告警
        if cpu_percent > 90:
            alerts.append({
                "type": "high_cpu_usage",
                "severity": "critical",
                "message": f"CPU使用率过高: {cpu_percent:.1f}%"
            })
        
        # 内存使用率告警
        if memory_percent > self.config["alert_thresholds"]["memory_usage_percent"]:
            alerts.append({
                "type": "high_memory_usage",
                "severity": "critical",
                "message": f"内存使用率过高: {memory_percent:.1f}%"
            })
        
        # 磁盘使用率告警
        if disk_percent > self.config["alert_thresholds"]["disk_usage_percent"]:
            alerts.append({
                "type": "high_disk_usage",
                "severity": "warning",
                "message": f"磁盘使用率过高: {disk_percent:.1f}%"
            })
        
        # 网络状态告警
        if network_status != "healthy":
            alerts.append({
                "type": "network_issue",
                "severity": "warning",
                "message": f"网络状态异常: {network_status}"
            })
        
        # BigQuery状态告警
        if not bigquery_status.startswith("healthy"):
            alerts.append({
                "type": "bigquery_issue",
                "severity": "critical",
                "message": f"BigQuery连接异常: {bigquery_status}"
            })
        
        # 记录告警
        for alert in alerts:
            self.record_alert(alert["type"], alert["severity"], alert["message"])
    
    def record_alert(self, alert_type: str, severity: str, message: str, details: str = None):
        """记录告警"""
        try:
            conn = sqlite3.connect(self.monitoring_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO alerts 
                (alert_type, severity, message, details, resolved, alert_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                alert_type,
                severity,
                message,
                details,
                False,
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
            logger.warning(f"告警记录: [{severity}] {alert_type} - {message}")
            
        except Exception as e:
            logger.error(f"记录告警失败: {e}")
    
    def calculate_performance_metrics(self):
        """计算性能指标"""
        try:
            conn = sqlite3.connect(self.monitoring_db_path)
            cursor = conn.cursor()
            
            # 计算最近24小时的同步成功率
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_syncs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_syncs,
                    AVG(duration_seconds) as avg_duration
                FROM sync_tasks 
                WHERE datetime(start_time) > datetime('now', '-24 hours')
            ''')
            
            sync_stats = cursor.fetchone()
            sync_success_rate = 0.0
            avg_sync_duration = 0.0
            
            if sync_stats and sync_stats[0] > 0:
                sync_success_rate = sync_stats[1] / sync_stats[0]
                avg_sync_duration = sync_stats[2] or 0.0
            
            # 计算一致性检查成功率
            consistency_db_path = "/Users/a606/cloud_function_source/local_data/consistency_checks.db"
            consistency_success_rate = 0.0
            
            if os.path.exists(consistency_db_path):
                consistency_conn = sqlite3.connect(consistency_db_path)
                consistency_cursor = consistency_conn.cursor()
                
                consistency_cursor.execute('''
                    SELECT 
                        COUNT(*) as total_checks,
                        SUM(CASE WHEN is_consistent THEN 1 ELSE 0 END) as consistent_checks
                    FROM consistency_checks 
                    WHERE datetime(check_time) > datetime('now', '-24 hours')
                ''')
                
                consistency_stats = consistency_cursor.fetchone()
                if consistency_stats and consistency_stats[0] > 0:
                    consistency_success_rate = consistency_stats[1] / consistency_stats[0]
                
                consistency_conn.close()
            
            # 计算数据新鲜度（最新数据的时间差）
            data_freshness_hours = 0.0
            try:
                # 检查最新的数据文件修改时间
                latest_time = 0
                for file_name in os.listdir(self.local_data_dir):
                    if file_name.endswith('.json'):
                        file_path = os.path.join(self.local_data_dir, file_name)
                        mtime = os.path.getmtime(file_path)
                        latest_time = max(latest_time, mtime)
                
                if latest_time > 0:
                    data_freshness_hours = (time.time() - latest_time) / 3600
            except:
                pass
            
            # 计算总数据大小
            total_data_size_mb = 0.0
            try:
                for file_name in os.listdir(self.local_data_dir):
                    if file_name.endswith('.json'):
                        file_path = os.path.join(self.local_data_dir, file_name)
                        total_data_size_mb += os.path.getsize(file_path) / (1024 * 1024)
            except:
                pass
            
            # 记录性能指标
            cursor.execute('''
                INSERT INTO performance_metrics 
                (metric_time, sync_success_rate, avg_sync_duration, consistency_success_rate,
                 data_freshness_hours, total_data_size_mb, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now().isoformat(),
                sync_success_rate,
                avg_sync_duration,
                consistency_success_rate,
                data_freshness_hours,
                total_data_size_mb,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"性能指标计算完成 - 同步成功率: {sync_success_rate:.2%}, "
                       f"一致性成功率: {consistency_success_rate:.2%}, "
                       f"数据新鲜度: {data_freshness_hours:.1f}小时")
            
        except Exception as e:
            logger.error(f"计算性能指标失败: {e}")
    
    def run_scheduled_sync(self):
        """运行定时同步"""
        logger.info("开始定时数据同步")
        start_time = datetime.now()
        
        try:
            # 运行云到本地同步系统
            result = subprocess.run([
                'python3', 'cloud_to_local_sync_system.py'
            ], capture_output=True, text=True, timeout=3600)
            
            end_time = datetime.now()
            
            if result.returncode == 0:
                status = "success"
                error_message = None
                logger.info("定时同步完成")
            else:
                status = "failed"
                error_message = result.stderr[:500] if result.stderr else "未知错误"
                logger.error(f"定时同步失败: {error_message}")
            
            # 记录同步任务
            self.record_sync_task(
                "scheduled_sync", start_time, end_time, status,
                error_message=error_message
            )
            
        except subprocess.TimeoutExpired:
            end_time = datetime.now()
            error_message = "同步任务超时"
            logger.error(error_message)
            
            self.record_sync_task(
                "scheduled_sync", start_time, end_time, "timeout",
                error_message=error_message
            )
            
        except Exception as e:
            end_time = datetime.now()
            error_message = f"同步任务异常: {str(e)}"
            logger.error(error_message)
            
            self.record_sync_task(
                "scheduled_sync", start_time, end_time, "error",
                error_message=error_message
            )
    
    def run_scheduled_consistency_check(self):
        """运行定时一致性检查"""
        logger.info("开始定时一致性检查")
        start_time = datetime.now()
        
        try:
            # 运行数据一致性检查
            result = subprocess.run([
                'python3', 'data_consistency_checker.py'
            ], capture_output=True, text=True, timeout=1800)
            
            end_time = datetime.now()
            
            if result.returncode == 0:
                status = "success"
                error_message = None
                logger.info("定时一致性检查完成")
            else:
                status = "failed"
                error_message = result.stderr[:500] if result.stderr else "未知错误"
                logger.error(f"定时一致性检查失败: {error_message}")
            
            # 记录任务
            self.record_sync_task(
                "consistency_check", start_time, end_time, status,
                error_message=error_message
            )
            
        except Exception as e:
            end_time = datetime.now()
            error_message = f"一致性检查异常: {str(e)}"
            logger.error(error_message)
            
            self.record_sync_task(
                "consistency_check", start_time, end_time, "error",
                error_message=error_message
            )
    
    def cleanup_old_data(self):
        """清理旧数据"""
        logger.info("开始清理旧数据")
        
        try:
            # 清理旧的日志文件
            log_files_cleaned = 0
            for file_name in os.listdir(self.logs_dir):
                if file_name.endswith('.log'):
                    file_path = os.path.join(self.logs_dir, file_name)
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    
                    if file_size_mb > self.config["max_log_size_mb"]:
                        # 备份并清空大日志文件
                        backup_path = f"{file_path}.backup.{int(time.time())}"
                        os.rename(file_path, backup_path)
                        open(file_path, 'w').close()
                        log_files_cleaned += 1
                        logger.info(f"清理日志文件: {file_name} ({file_size_mb:.1f}MB)")
            
            # 清理数据库中的旧记录
            conn = sqlite3.connect(self.monitoring_db_path)
            cursor = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=self.config["max_data_age_days"])).isoformat()
            
            # 清理旧的同步任务记录
            cursor.execute('DELETE FROM sync_tasks WHERE created_at < ?', (cutoff_date,))
            sync_records_deleted = cursor.rowcount
            
            # 清理旧的系统健康记录
            cursor.execute('DELETE FROM system_health WHERE created_at < ?', (cutoff_date,))
            health_records_deleted = cursor.rowcount
            
            # 清理已解决的旧告警
            cursor.execute('DELETE FROM alerts WHERE resolved = 1 AND created_at < ?', (cutoff_date,))
            alerts_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"清理完成 - 日志文件: {log_files_cleaned}, "
                       f"同步记录: {sync_records_deleted}, "
                       f"健康记录: {health_records_deleted}, "
                       f"告警记录: {alerts_deleted}")
            
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")
    
    def setup_schedules(self):
        """设置定时任务"""
        # 数据同步任务
        schedule.every(self.config["sync_interval_minutes"]).minutes.do(self.run_scheduled_sync)
        
        # 一致性检查任务
        schedule.every(self.config["consistency_check_interval_hours"]).hours.do(self.run_scheduled_consistency_check)
        
        # 系统健康检查任务
        schedule.every(self.config["health_check_interval_minutes"]).minutes.do(self.record_system_health)
        
        # 性能指标计算任务
        schedule.every(30).minutes.do(self.calculate_performance_metrics)
        
        # 清理任务
        schedule.every(self.config["cleanup_interval_days"]).days.do(self.cleanup_old_data)
        
        logger.info("定时任务设置完成")
    
    def run_scheduler(self):
        """运行调度器"""
        logger.info("监控系统调度器启动")
        
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except Exception as e:
                logger.error(f"调度器运行异常: {e}")
                time.sleep(60)
        
        logger.info("监控系统调度器停止")
    
    def start(self):
        """启动监控系统"""
        if self.is_running:
            logger.warning("监控系统已在运行")
            return
        
        logger.info("启动监控系统")
        
        # 设置定时任务
        self.setup_schedules()
        
        # 启动调度器线程
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        # 立即执行一次健康检查和性能指标计算
        self.record_system_health()
        self.calculate_performance_metrics()
        
        logger.info("监控系统启动完成")
    
    def stop(self):
        """停止监控系统"""
        if not self.is_running:
            logger.warning("监控系统未在运行")
            return
        
        logger.info("停止监控系统")
        self.is_running = False
        
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        logger.info("监控系统已停止")
    
    def get_monitoring_dashboard(self) -> Dict[str, Any]:
        """获取监控仪表板数据"""
        try:
            conn = sqlite3.connect(self.monitoring_db_path)
            cursor = conn.cursor()
            
            # 获取最近的系统健康状态
            cursor.execute('''
                SELECT * FROM system_health 
                ORDER BY check_time DESC 
                LIMIT 1
            ''')
            latest_health = cursor.fetchone()
            
            # 获取最近24小时的同步统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_syncs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_syncs,
                    AVG(duration_seconds) as avg_duration
                FROM sync_tasks 
                WHERE datetime(start_time) > datetime('now', '-24 hours')
            ''')
            sync_stats = cursor.fetchone()
            
            # 获取未解决的告警
            cursor.execute('''
                SELECT alert_type, severity, message, alert_time
                FROM alerts 
                WHERE resolved = 0 
                ORDER BY alert_time DESC
            ''')
            active_alerts = cursor.fetchall()
            
            # 获取最新的性能指标
            cursor.execute('''
                SELECT * FROM performance_metrics 
                ORDER BY metric_time DESC 
                LIMIT 1
            ''')
            latest_metrics = cursor.fetchone()
            
            conn.close()
            
            dashboard = {
                "dashboard_time": datetime.now().isoformat(),
                "system_health": {
                    "cpu_usage": latest_health[2] if latest_health else 0,
                    "memory_usage": latest_health[3] if latest_health else 0,
                    "disk_usage": latest_health[4] if latest_health else 0,
                    "disk_free_gb": latest_health[5] if latest_health else 0,
                    "network_status": latest_health[7] if latest_health else "unknown",
                    "bigquery_status": latest_health[8] if latest_health else "unknown"
                },
                "sync_statistics": {
                    "total_syncs_24h": sync_stats[0] if sync_stats else 0,
                    "successful_syncs_24h": sync_stats[1] if sync_stats else 0,
                    "success_rate_24h": (sync_stats[1] / sync_stats[0]) if sync_stats and sync_stats[0] > 0 else 0,
                    "avg_duration_seconds": sync_stats[2] if sync_stats else 0
                },
                "active_alerts": [
                    {
                        "type": alert[0],
                        "severity": alert[1],
                        "message": alert[2],
                        "time": alert[3]
                    }
                    for alert in active_alerts
                ],
                "performance_metrics": {
                    "sync_success_rate": latest_metrics[2] if latest_metrics else 0,
                    "consistency_success_rate": latest_metrics[4] if latest_metrics else 0,
                    "data_freshness_hours": latest_metrics[5] if latest_metrics else 0,
                    "total_data_size_mb": latest_metrics[6] if latest_metrics else 0
                } if latest_metrics else {}
            }
            
            return dashboard
            
        except Exception as e:
            logger.error(f"获取监控仪表板数据失败: {e}")
            return {"error": str(e)}

def main():
    """主函数"""
    monitoring = MonitoringSystem()
    
    print("=" * 60)
    print("定时同步和监控系统")
    print("=" * 60)
    
    try:
        # 启动监控系统
        monitoring.start()
        
        # 获取仪表板数据
        dashboard = monitoring.get_monitoring_dashboard()
        
        if "error" not in dashboard:
            print(f"系统健康状态:")
            health = dashboard["system_health"]
            print(f"  CPU使用率: {health['cpu_usage']:.1f}%")
            print(f"  内存使用率: {health['memory_usage']:.1f}%")
            print(f"  磁盘使用率: {health['disk_usage']:.1f}%")
            print(f"  可用磁盘: {health['disk_free_gb']:.1f}GB")
            print(f"  网络状态: {health['network_status']}")
            print(f"  BigQuery状态: {health['bigquery_status']}")
            
            print(f"\n同步统计 (24小时):")
            sync_stats = dashboard["sync_statistics"]
            print(f"  总同步次数: {sync_stats['total_syncs_24h']}")
            print(f"  成功次数: {sync_stats['successful_syncs_24h']}")
            print(f"  成功率: {sync_stats['success_rate_24h']:.2%}")
            print(f"  平均耗时: {sync_stats['avg_duration_seconds']:.1f}秒")
            
            print(f"\n活跃告警: {len(dashboard['active_alerts'])}")
            for alert in dashboard["active_alerts"][:5]:  # 显示前5个告警
                print(f"  [{alert['severity']}] {alert['type']}: {alert['message']}")
            
            if dashboard["performance_metrics"]:
                metrics = dashboard["performance_metrics"]
                print(f"\n性能指标:")
                print(f"  同步成功率: {metrics.get('sync_success_rate', 0):.2%}")
                print(f"  一致性成功率: {metrics.get('consistency_success_rate', 0):.2%}")
                print(f"  数据新鲜度: {metrics.get('data_freshness_hours', 0):.1f}小时")
                print(f"  总数据大小: {metrics.get('total_data_size_mb', 0):.1f}MB")
        
        print(f"\n监控系统已启动，将持续运行...")
        print(f"配置信息:")
        print(f"  同步间隔: {monitoring.config['sync_interval_minutes']}分钟")
        print(f"  一致性检查间隔: {monitoring.config['consistency_check_interval_hours']}小时")
        print(f"  健康检查间隔: {monitoring.config['health_check_interval_minutes']}分钟")
        
        # 保持运行
        while True:
            time.sleep(300)  # 每5分钟显示一次状态
            dashboard = monitoring.get_monitoring_dashboard()
            if "error" not in dashboard:
                health = dashboard["system_health"]
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"CPU: {health['cpu_usage']:.1f}% | "
                      f"内存: {health['memory_usage']:.1f}% | "
                      f"磁盘: {health['disk_usage']:.1f}% | "
                      f"告警: {len(dashboard['active_alerts'])}")
    
    except KeyboardInterrupt:
        print("\n收到停止信号，正在关闭监控系统...")
        monitoring.stop()
        print("监控系统已停止")
    
    except Exception as e:
        logger.error(f"监控系统运行异常: {e}")
        monitoring.stop()
    
    print("=" * 60)
    print("监控系统已退出")
    print("=" * 60)

if __name__ == "__main__":
    main()