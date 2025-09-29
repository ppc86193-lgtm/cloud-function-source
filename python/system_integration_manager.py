#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统集成管理器
统一管理回填服务、实时服务、监控和通知系统
"""

import json
import time
import logging
import threading
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import sqlite3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import schedule

from enhanced_backfill_service import EnhancedBackfillService, BackfillMode, BackfillStatus
from enhanced_realtime_service import EnhancedRealtimeService, NotificationType
from pc28_upstream_api import PC28UpstreamAPI

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pc28_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SystemStatus(Enum):
    """系统状态枚举"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"
    MAINTENANCE = "maintenance"

class ServiceType(Enum):
    """服务类型枚举"""
    BACKFILL = "backfill"
    REALTIME = "realtime"
    MONITORING = "monitoring"
    NOTIFICATION = "notification"

@dataclass
class SystemMetrics:
    """系统整体指标"""
    system_status: str = SystemStatus.STOPPED.value
    uptime_seconds: float = 0.0
    total_data_processed: int = 0
    active_services: List[str] = None
    error_count: int = 0
    last_error_time: Optional[str] = None
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    def __post_init__(self):
        if self.active_services is None:
            self.active_services = []

@dataclass
class ServiceHealth:
    """服务健康状态"""
    service_name: str
    status: str
    last_check_time: str
    response_time_ms: float
    error_count: int = 0
    success_rate: float = 100.0
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}

class SystemIntegrationManager:
    """
    PC28系统集成管理器
    统一管理所有服务和监控
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化系统管理器
        
        Args:
            config: 系统配置
        """
        self.config = config or self._load_default_config()
        
        # 系统状态
        self.system_status = SystemStatus.STOPPED
        self.start_time = None
        self.shutdown_requested = False
        
        # 服务实例
        self.backfill_service = None
        self.realtime_service = None
        
        # 监控和指标
        self.system_metrics = SystemMetrics()
        self.service_health: Dict[str, ServiceHealth] = {}
        self.metrics_lock = threading.Lock()
        
        # 通知系统
        self.notification_handlers: List[Callable] = []
        self.alert_thresholds = self.config.get('alert_thresholds', {})
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=5)
        
        # 数据库
        self.db_path = "system_integration.db"
        self._init_database()
        
        # 定时任务
        self.scheduler_thread = None
        
        logger.info("系统集成管理器初始化完成")
    
    def _load_default_config(self) -> Dict[str, Any]:
        """加载默认配置"""
        return {
            'appid': '45928',
            'secret_key': 'ca9edbfee35c22a0d6c4cf6722506af0',
            'realtime_fetch_interval': 30,
            'health_check_interval': 60,
            'metrics_collection_interval': 300,
            'auto_backfill_enabled': True,
            'auto_backfill_days': 7,
            'alert_thresholds': {
                'error_rate': 5.0,  # 错误率阈值（%）
                'response_time': 5000,  # 响应时间阈值（ms）
                'memory_usage': 1024,  # 内存使用阈值（MB）
                'cpu_usage': 80.0  # CPU使用率阈值（%）
            },
            'notification_channels': ['console', 'log'],
            'data_retention_days': 30
        }
    
    def _init_database(self):
        """初始化系统数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 系统指标表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        system_status TEXT,
                        uptime_seconds REAL,
                        total_data_processed INTEGER,
                        active_services TEXT,
                        error_count INTEGER,
                        memory_usage_mb REAL,
                        cpu_usage_percent REAL
                    )
                """)
                
                # 服务健康表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS service_health (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        service_name TEXT,
                        status TEXT,
                        response_time_ms REAL,
                        error_count INTEGER,
                        success_rate REAL,
                        details TEXT
                    )
                """)
                
                # 系统事件表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS system_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        event_type TEXT,
                        service_name TEXT,
                        severity TEXT,
                        message TEXT,
                        details TEXT
                    )
                """)
                
                # 告警记录表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS alert_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        alert_type TEXT,
                        severity TEXT,
                        message TEXT,
                        resolved_at TEXT,
                        details TEXT
                    )
                """)
                
                conn.commit()
                logger.info("系统数据库初始化完成")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def start_system(self):
        """
        启动整个系统
        """
        if self.system_status != SystemStatus.STOPPED:
            logger.warning(f"系统已在运行，当前状态: {self.system_status.value}")
            return False
        
        logger.info("启动PC28系统集成管理器")
        
        try:
            self.system_status = SystemStatus.STARTING
            self.start_time = time.time()
            
            # 初始化服务
            self._initialize_services()
            
            # 启动核心服务
            self._start_core_services()
            
            # 启动监控系统
            self._start_monitoring_system()
            
            # 启动定时任务
            self._start_scheduler()
            
            # 系统启动完成
            self.system_status = SystemStatus.RUNNING
            
            # 记录系统事件
            self._log_system_event("system_start", "system", "info", "系统启动成功")
            
            # 发送启动通知
            self._send_system_notification("系统启动", "PC28系统已成功启动并开始运行")
            
            logger.info("PC28系统启动完成")
            return True
            
        except Exception as e:
            logger.error(f"系统启动失败: {e}")
            self.system_status = SystemStatus.ERROR
            self._log_system_event("system_start_error", "system", "error", f"系统启动失败: {e}")
            return False
    
    def _initialize_services(self):
        """初始化服务实例"""
        logger.info("初始化服务实例")
        
        # 初始化回填服务
        self.backfill_service = EnhancedBackfillService(
            appid=self.config['appid'],
            secret_key=self.config['secret_key'],
            config=self.config
        )
        
        # 初始化实时服务
        self.realtime_service = EnhancedRealtimeService(
            appid=self.config['appid'],
            secret_key=self.config['secret_key'],
            config=self.config
        )
        
        # 设置实时服务配置
        self.realtime_service.fetch_interval = self.config['realtime_fetch_interval']
        
        # 添加实时服务通知回调
        self.realtime_service.add_notification_callback(self._handle_realtime_notification)
        
        logger.info("服务实例初始化完成")
    
    def _start_core_services(self):
        """启动核心服务"""
        logger.info("启动核心服务")
        
        # 启动实时监控服务
        self.realtime_service.start_realtime_monitoring()
        
        # 如果启用自动回填，启动回填任务
        if self.config.get('auto_backfill_enabled', False):
            self._start_auto_backfill()
        
        # 更新活跃服务列表
        with self.metrics_lock:
            self.system_metrics.active_services = ['realtime']
            if self.config.get('auto_backfill_enabled', False):
                self.system_metrics.active_services.append('backfill')
        
        logger.info("核心服务启动完成")
    
    def _start_auto_backfill(self):
        """启动自动回填"""
        logger.info("启动自动回填任务")
        
        try:
            # 计算回填日期范围
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=self.config['auto_backfill_days'])).strftime("%Y-%m-%d")
            
            # 创建智能回填任务
            task_id = self.backfill_service.create_backfill_task(
                mode=BackfillMode.SMART,
                start_date=start_date,
                end_date=end_date
            )
            
            # 启动任务
            if self.backfill_service.start_backfill_task(task_id):
                logger.info(f"自动回填任务启动成功: {task_id}")
                self._log_system_event("auto_backfill_start", "backfill", "info", f"自动回填任务启动: {task_id}")
            else:
                logger.error(f"自动回填任务启动失败: {task_id}")
                
        except Exception as e:
            logger.error(f"启动自动回填失败: {e}")
    
    def _start_monitoring_system(self):
        """启动监控系统"""
        logger.info("启动监控系统")
        
        # 启动健康检查线程
        threading.Thread(target=self._health_check_loop, daemon=True).start()
        
        # 启动指标收集线程
        threading.Thread(target=self._metrics_collection_loop, daemon=True).start()
        
        # 启动告警检查线程
        threading.Thread(target=self._alert_check_loop, daemon=True).start()
        
        logger.info("监控系统启动完成")
    
    def _start_scheduler(self):
        """启动定时任务调度器"""
        logger.info("启动定时任务调度器")
        
        # 配置定时任务
        schedule.every(1).hours.do(self._hourly_maintenance)
        schedule.every(1).days.do(self._daily_maintenance)
        schedule.every(1).weeks.do(self._weekly_maintenance)
        
        # 启动调度器线程
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        logger.info("定时任务调度器启动完成")
    
    def _scheduler_loop(self):
        """调度器循环"""
        while not self.shutdown_requested:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except Exception as e:
                logger.error(f"调度器异常: {e}")
                time.sleep(60)
    
    def _health_check_loop(self):
        """健康检查循环"""
        logger.info("健康检查循环启动")
        
        while not self.shutdown_requested:
            try:
                self._perform_health_checks()
                time.sleep(self.config['health_check_interval'])
            except Exception as e:
                logger.error(f"健康检查异常: {e}")
                time.sleep(60)
    
    def _perform_health_checks(self):
        """执行健康检查"""
        logger.debug("执行服务健康检查")
        
        # 检查实时服务
        self._check_realtime_service_health()
        
        # 检查回填服务
        self._check_backfill_service_health()
        
        # 检查系统资源
        self._check_system_resources()
    
    def _check_realtime_service_health(self):
        """检查实时服务健康状态"""
        try:
            start_time = time.time()
            
            # 获取实时服务指标
            metrics = self.realtime_service.get_realtime_metrics()
            
            response_time = (time.time() - start_time) * 1000  # 转换为毫秒
            
            # 计算成功率
            total_fetches = metrics.get('total_draws_fetched', 0)
            successful_fetches = metrics.get('successful_fetches', 0)
            success_rate = (successful_fetches / total_fetches * 100) if total_fetches > 0 else 100.0
            
            # 判断服务状态
            status = "healthy"
            if success_rate < 90:
                status = "degraded"
            elif success_rate < 50:
                status = "unhealthy"
            
            # 更新健康状态
            health = ServiceHealth(
                service_name="realtime",
                status=status,
                last_check_time=datetime.now(timezone.utc).isoformat(),
                response_time_ms=response_time,
                error_count=metrics.get('failed_fetches', 0),
                success_rate=success_rate,
                details=metrics
            )
            
            self.service_health["realtime"] = health
            self._save_service_health(health)
            
        except Exception as e:
            logger.error(f"实时服务健康检查失败: {e}")
            
            # 记录不健康状态
            health = ServiceHealth(
                service_name="realtime",
                status="error",
                last_check_time=datetime.now(timezone.utc).isoformat(),
                response_time_ms=0,
                error_count=1,
                success_rate=0.0,
                details={'error': str(e)}
            )
            
            self.service_health["realtime"] = health
    
    def _check_backfill_service_health(self):
        """检查回填服务健康状态"""
        try:
            start_time = time.time()
            
            # 获取活跃任务
            active_tasks = self.backfill_service.list_active_tasks()
            
            response_time = (time.time() - start_time) * 1000
            
            # 计算任务统计
            total_tasks = len(active_tasks)
            running_tasks = sum(1 for task in active_tasks if task['status'] == 'running')
            failed_tasks = sum(1 for task in active_tasks if task['status'] == 'failed')
            
            # 判断服务状态
            status = "healthy"
            if failed_tasks > 0:
                status = "degraded"
            
            success_rate = ((total_tasks - failed_tasks) / total_tasks * 100) if total_tasks > 0 else 100.0
            
            # 更新健康状态
            health = ServiceHealth(
                service_name="backfill",
                status=status,
                last_check_time=datetime.now(timezone.utc).isoformat(),
                response_time_ms=response_time,
                error_count=failed_tasks,
                success_rate=success_rate,
                details={
                    'total_tasks': total_tasks,
                    'running_tasks': running_tasks,
                    'failed_tasks': failed_tasks
                }
            )
            
            self.service_health["backfill"] = health
            self._save_service_health(health)
            
        except Exception as e:
            logger.error(f"回填服务健康检查失败: {e}")
            
            health = ServiceHealth(
                service_name="backfill",
                status="error",
                last_check_time=datetime.now(timezone.utc).isoformat(),
                response_time_ms=0,
                error_count=1,
                success_rate=0.0,
                details={'error': str(e)}
            )
            
            self.service_health["backfill"] = health
    
    def _check_system_resources(self):
        """检查系统资源"""
        try:
            import psutil
            
            # 获取内存使用情况
            memory = psutil.virtual_memory()
            memory_usage_mb = memory.used / 1024 / 1024
            
            # 获取CPU使用率
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # 更新系统指标
            with self.metrics_lock:
                self.system_metrics.memory_usage_mb = memory_usage_mb
                self.system_metrics.cpu_usage_percent = cpu_usage
                
                if self.start_time:
                    self.system_metrics.uptime_seconds = time.time() - self.start_time
            
        except ImportError:
            logger.warning("psutil未安装，无法获取系统资源信息")
        except Exception as e:
            logger.error(f"系统资源检查失败: {e}")
    
    def _metrics_collection_loop(self):
        """指标收集循环"""
        logger.info("指标收集循环启动")
        
        while not self.shutdown_requested:
            try:
                self._collect_system_metrics()
                time.sleep(self.config['metrics_collection_interval'])
            except Exception as e:
                logger.error(f"指标收集异常: {e}")
                time.sleep(60)
    
    def _collect_system_metrics(self):
        """收集系统指标"""
        try:
            with self.metrics_lock:
                # 更新系统状态
                self.system_metrics.system_status = self.system_status.value
                
                # 计算总处理数据量
                realtime_metrics = self.realtime_service.get_realtime_metrics()
                self.system_metrics.total_data_processed = realtime_metrics.get('successful_fetches', 0)
                
                # 保存指标到数据库
                self._save_system_metrics()
            
            logger.debug("系统指标收集完成")
            
        except Exception as e:
            logger.error(f"系统指标收集失败: {e}")
    
    def _alert_check_loop(self):
        """告警检查循环"""
        logger.info("告警检查循环启动")
        
        while not self.shutdown_requested:
            try:
                self._check_alerts()
                time.sleep(60)  # 每分钟检查一次告警
            except Exception as e:
                logger.error(f"告警检查异常: {e}")
                time.sleep(60)
    
    def _check_alerts(self):
        """检查告警条件"""
        try:
            # 检查错误率告警
            self._check_error_rate_alert()
            
            # 检查响应时间告警
            self._check_response_time_alert()
            
            # 检查资源使用告警
            self._check_resource_usage_alert()
            
            # 检查服务健康告警
            self._check_service_health_alert()
            
        except Exception as e:
            logger.error(f"告警检查失败: {e}")
    
    def _check_error_rate_alert(self):
        """检查错误率告警"""
        try:
            realtime_metrics = self.realtime_service.get_realtime_metrics()
            error_rate = realtime_metrics.get('error_rate', 0)
            
            threshold = self.alert_thresholds.get('error_rate', 5.0)
            
            if error_rate > threshold:
                self._trigger_alert(
                    alert_type="high_error_rate",
                    severity="warning",
                    message=f"实时服务错误率过高: {error_rate:.2f}% (阈值: {threshold}%)",
                    details={'error_rate': error_rate, 'threshold': threshold}
                )
        except Exception as e:
            logger.error(f"错误率告警检查失败: {e}")
    
    def _check_response_time_alert(self):
        """检查响应时间告警"""
        try:
            for service_name, health in self.service_health.items():
                threshold = self.alert_thresholds.get('response_time', 5000)
                
                if health.response_time_ms > threshold:
                    self._trigger_alert(
                        alert_type="high_response_time",
                        severity="warning",
                        message=f"{service_name}服务响应时间过长: {health.response_time_ms:.2f}ms (阈值: {threshold}ms)",
                        details={'service': service_name, 'response_time': health.response_time_ms, 'threshold': threshold}
                    )
        except Exception as e:
            logger.error(f"响应时间告警检查失败: {e}")
    
    def _check_resource_usage_alert(self):
        """检查资源使用告警"""
        try:
            with self.metrics_lock:
                # 检查内存使用
                memory_threshold = self.alert_thresholds.get('memory_usage', 1024)
                if self.system_metrics.memory_usage_mb > memory_threshold:
                    self._trigger_alert(
                        alert_type="high_memory_usage",
                        severity="warning",
                        message=f"内存使用过高: {self.system_metrics.memory_usage_mb:.2f}MB (阈值: {memory_threshold}MB)",
                        details={'memory_usage': self.system_metrics.memory_usage_mb, 'threshold': memory_threshold}
                    )
                
                # 检查CPU使用
                cpu_threshold = self.alert_thresholds.get('cpu_usage', 80.0)
                if self.system_metrics.cpu_usage_percent > cpu_threshold:
                    self._trigger_alert(
                        alert_type="high_cpu_usage",
                        severity="warning",
                        message=f"CPU使用率过高: {self.system_metrics.cpu_usage_percent:.2f}% (阈值: {cpu_threshold}%)",
                        details={'cpu_usage': self.system_metrics.cpu_usage_percent, 'threshold': cpu_threshold}
                    )
        except Exception as e:
            logger.error(f"资源使用告警检查失败: {e}")
    
    def _check_service_health_alert(self):
        """检查服务健康告警"""
        try:
            for service_name, health in self.service_health.items():
                if health.status in ['unhealthy', 'error']:
                    self._trigger_alert(
                        alert_type="service_unhealthy",
                        severity="critical",
                        message=f"{service_name}服务不健康: {health.status}",
                        details={'service': service_name, 'status': health.status, 'details': health.details}
                    )
                elif health.status == 'degraded':
                    self._trigger_alert(
                        alert_type="service_degraded",
                        severity="warning",
                        message=f"{service_name}服务性能下降: {health.status}",
                        details={'service': service_name, 'status': health.status, 'success_rate': health.success_rate}
                    )
        except Exception as e:
            logger.error(f"服务健康告警检查失败: {e}")
    
    def _trigger_alert(self, alert_type: str, severity: str, message: str, details: Dict[str, Any]):
        """触发告警"""
        try:
            alert_data = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'alert_type': alert_type,
                'severity': severity,
                'message': message,
                'details': details
            }
            
            # 记录告警
            self._save_alert(alert_data)
            
            # 发送告警通知
            self._send_alert_notification(alert_data)
            
            logger.warning(f"告警触发: {message}")
            
        except Exception as e:
            logger.error(f"触发告警失败: {e}")
    
    def _handle_realtime_notification(self, notification_type: str, data: Any):
        """处理实时服务通知"""
        try:
            if notification_type == NotificationType.NEW_DRAW.value:
                logger.info(f"收到新开奖通知: {data.draw_id}")
                
                # 可以在这里添加额外的处理逻辑
                # 例如：数据验证、存储、转发等
                
            elif notification_type == NotificationType.ERROR.value:
                logger.error(f"收到实时服务错误通知: {data}")
                
                # 记录错误事件
                self._log_system_event("realtime_error", "realtime", "error", str(data))
                
        except Exception as e:
            logger.error(f"处理实时通知失败: {e}")
    
    def _hourly_maintenance(self):
        """每小时维护任务"""
        logger.info("执行每小时维护任务")
        
        try:
            # 清理过期缓存
            self._cleanup_expired_cache()
            
            # 检查数据完整性
            self._check_data_integrity()
            
        except Exception as e:
            logger.error(f"每小时维护任务失败: {e}")
    
    def _daily_maintenance(self):
        """每日维护任务"""
        logger.info("执行每日维护任务")
        
        try:
            # 清理旧日志
            self._cleanup_old_logs()
            
            # 生成日报
            self._generate_daily_report()
            
            # 数据备份
            self._backup_data()
            
        except Exception as e:
            logger.error(f"每日维护任务失败: {e}")
    
    def _weekly_maintenance(self):
        """每周维护任务"""
        logger.info("执行每周维护任务")
        
        try:
            # 系统优化
            self._optimize_system()
            
            # 生成周报
            self._generate_weekly_report()
            
        except Exception as e:
            logger.error(f"每周维护任务失败: {e}")
    
    def _cleanup_expired_cache(self):
        """清理过期缓存"""
        logger.debug("清理过期缓存")
        # 实现缓存清理逻辑
    
    def _check_data_integrity(self):
        """检查数据完整性"""
        logger.debug("检查数据完整性")
        # 实现数据完整性检查逻辑
    
    def _cleanup_old_logs(self):
        """清理旧日志"""
        logger.debug("清理旧日志")
        
        try:
            retention_days = self.config.get('data_retention_days', 30)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 清理旧的系统指标
                cursor.execute(
                    "DELETE FROM system_metrics WHERE timestamp < ?",
                    (cutoff_date.isoformat(),)
                )
                
                # 清理旧的服务健康记录
                cursor.execute(
                    "DELETE FROM service_health WHERE timestamp < ?",
                    (cutoff_date.isoformat(),)
                )
                
                # 清理旧的系统事件
                cursor.execute(
                    "DELETE FROM system_events WHERE timestamp < ?",
                    (cutoff_date.isoformat(),)
                )
                
                conn.commit()
                logger.info(f"清理了 {retention_days} 天前的旧日志")
                
        except Exception as e:
            logger.error(f"清理旧日志失败: {e}")
    
    def _generate_daily_report(self):
        """生成日报"""
        logger.info("生成系统日报")
        # 实现日报生成逻辑
    
    def _generate_weekly_report(self):
        """生成周报"""
        logger.info("生成系统周报")
        # 实现周报生成逻辑
    
    def _backup_data(self):
        """数据备份"""
        logger.info("执行数据备份")
        # 实现数据备份逻辑
    
    def _optimize_system(self):
        """系统优化"""
        logger.info("执行系统优化")
        # 实现系统优化逻辑
    
    def _save_system_metrics(self):
        """保存系统指标"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO system_metrics 
                    (timestamp, system_status, uptime_seconds, total_data_processed,
                     active_services, error_count, memory_usage_mb, cpu_usage_percent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now(timezone.utc).isoformat(),
                    self.system_metrics.system_status,
                    self.system_metrics.uptime_seconds,
                    self.system_metrics.total_data_processed,
                    json.dumps(self.system_metrics.active_services),
                    self.system_metrics.error_count,
                    self.system_metrics.memory_usage_mb,
                    self.system_metrics.cpu_usage_percent
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存系统指标失败: {e}")
    
    def _save_service_health(self, health: ServiceHealth):
        """保存服务健康状态"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO service_health 
                    (timestamp, service_name, status, response_time_ms, error_count, success_rate, details)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    health.last_check_time,
                    health.service_name,
                    health.status,
                    health.response_time_ms,
                    health.error_count,
                    health.success_rate,
                    json.dumps(health.details)
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存服务健康状态失败: {e}")
    
    def _log_system_event(self, event_type: str, service_name: str, severity: str, message: str, details: Dict[str, Any] = None):
        """记录系统事件"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO system_events 
                    (timestamp, event_type, service_name, severity, message, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now(timezone.utc).isoformat(),
                    event_type,
                    service_name,
                    severity,
                    message,
                    json.dumps(details) if details else None
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"记录系统事件失败: {e}")
    
    def _save_alert(self, alert_data: Dict[str, Any]):
        """保存告警记录"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO alert_log 
                    (timestamp, alert_type, severity, message, details)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    alert_data['timestamp'],
                    alert_data['alert_type'],
                    alert_data['severity'],
                    alert_data['message'],
                    json.dumps(alert_data['details'])
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存告警记录失败: {e}")
    
    def _send_system_notification(self, title: str, message: str):
        """发送系统通知"""
        try:
            notification = {
                'title': title,
                'message': message,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # 控制台通知
            if 'console' in self.config.get('notification_channels', []):
                print(f"[系统通知] {title}: {message}")
            
            # 日志通知
            if 'log' in self.config.get('notification_channels', []):
                logger.info(f"系统通知 - {title}: {message}")
            
            # 调用自定义通知处理器
            for handler in self.notification_handlers:
                try:
                    handler(notification)
                except Exception as e:
                    logger.error(f"通知处理器执行失败: {e}")
                    
        except Exception as e:
            logger.error(f"发送系统通知失败: {e}")
    
    def _send_alert_notification(self, alert_data: Dict[str, Any]):
        """发送告警通知"""
        try:
            # 控制台告警
            if 'console' in self.config.get('notification_channels', []):
                print(f"[告警] {alert_data['severity'].upper()}: {alert_data['message']}")
            
            # 日志告警
            if 'log' in self.config.get('notification_channels', []):
                if alert_data['severity'] == 'critical':
                    logger.critical(f"告警 - {alert_data['message']}")
                elif alert_data['severity'] == 'warning':
                    logger.warning(f"告警 - {alert_data['message']}")
                else:
                    logger.info(f"告警 - {alert_data['message']}")
            
        except Exception as e:
            logger.error(f"发送告警通知失败: {e}")
    
    def add_notification_handler(self, handler: Callable[[Dict[str, Any]], None]):
        """添加通知处理器"""
        self.notification_handlers.append(handler)
        logger.info(f"添加通知处理器，当前处理器数量: {len(self.notification_handlers)}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        with self.metrics_lock:
            return {
                'system_status': self.system_status.value,
                'uptime_seconds': self.system_metrics.uptime_seconds,
                'active_services': self.system_metrics.active_services,
                'service_health': {name: asdict(health) for name, health in self.service_health.items()},
                'system_metrics': asdict(self.system_metrics)
            }
    
    def check_service_health(self) -> Dict[str, Any]:
        """检查服务健康状态"""
        # 执行健康检查
        self._perform_health_checks()
        
        with self.metrics_lock:
            return {name: asdict(health) for name, health in self.service_health.items()}
    
    def collect_metrics(self) -> Dict[str, Any]:
        """收集系统指标"""
        # 收集最新指标
        self._collect_system_metrics()
        
        with self.metrics_lock:
            return asdict(self.system_metrics)
    
    def create_manual_backfill_task(self, start_date: str, end_date: str, mode: str = "smart") -> Optional[str]:
        """创建手动回填任务"""
        try:
            backfill_mode = BackfillMode(mode)
            task_id = self.backfill_service.create_backfill_task(
                mode=backfill_mode,
                start_date=start_date,
                end_date=end_date
            )
            
            if self.backfill_service.start_backfill_task(task_id):
                logger.info(f"手动回填任务创建成功: {task_id}")
                self._log_system_event("manual_backfill_start", "backfill", "info", f"手动回填任务启动: {task_id}")
                return task_id
            else:
                logger.error(f"手动回填任务启动失败: {task_id}")
                return None
                
        except Exception as e:
            logger.error(f"创建手动回填任务失败: {e}")
            return None
    
    def stop_system(self):
        """停止系统"""
        if self.system_status == SystemStatus.STOPPED:
            logger.warning("系统已停止")
            return
        
        logger.info("开始停止PC28系统")
        
        try:
            self.system_status = SystemStatus.STOPPING
            self.shutdown_requested = True
            
            # 停止实时服务
            if self.realtime_service:
                self.realtime_service.stop_realtime_monitoring()
            
            # 等待线程结束
            time.sleep(2)
            
            # 关闭线程池
            self.executor.shutdown(wait=True)
            
            self.system_status = SystemStatus.STOPPED
            
            # 记录系统事件
            self._log_system_event("system_stop", "system", "info", "系统停止成功")
            
            # 发送停止通知
            self._send_system_notification("系统停止", "PC28系统已成功停止")
            
            logger.info("PC28系统停止完成")
            
        except Exception as e:
            logger.error(f"系统停止失败: {e}")
            self.system_status = SystemStatus.ERROR

# 使用示例
if __name__ == "__main__":
    # 创建系统管理器
    system_manager = SystemIntegrationManager()
    
    # 添加自定义通知处理器
    def custom_notification_handler(notification):
        print(f"自定义通知: {notification['title']} - {notification['message']}")
    
    system_manager.add_notification_handler(custom_notification_handler)
    
    try:
        # 启动系统
        if system_manager.start_system():
            print("系统启动成功")
            
            # 运行系统
            while True:
                time.sleep(30)
                
                # 显示系统状态
                status = system_manager.get_system_status()
                print(f"\n系统状态: {status['system_status']}")
                print(f"运行时间: {status['uptime_seconds']:.0f}秒")
                print(f"活跃服务: {status['active_services']}")
                
                # 显示服务健康状态
                for service_name, health in status['service_health'].items():
                    print(f"{service_name}服务: {health['status']} (成功率: {health['success_rate']:.1f}%)")
        else:
            print("系统启动失败")
    
    except KeyboardInterrupt:
        print("\n收到停止信号，正在关闭系统...")
        system_manager.stop_system()
        print("系统已关闭")