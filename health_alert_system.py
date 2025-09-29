#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统健康报告和告警机制
实现自动化的系统健康监控、报告生成和告警通知功能
"""

import sqlite3
import json
import logging
import threading
import time
import smtplib
import os
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any, Callable
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import hashlib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class HealthMetric:
    """健康指标"""
    metric_name: str
    current_value: float
    threshold_warning: float
    threshold_critical: float
    status: str  # 'healthy', 'warning', 'critical'
    trend: str   # 'improving', 'stable', 'degrading'
    description: str
    timestamp: datetime

@dataclass
class AlertRule:
    """告警规则"""
    rule_id: str
    rule_name: str
    metric_name: str
    condition: str  # 'greater_than', 'less_than', 'equals', 'not_equals'
    threshold: float
    severity: str   # 'info', 'warning', 'critical'
    enabled: bool
    cooldown_minutes: int
    notification_channels: List[str]
    description: str

@dataclass
class Alert:
    """告警信息"""
    alert_id: str
    rule_id: str
    metric_name: str
    current_value: float
    threshold: float
    severity: str
    message: str
    status: str  # 'active', 'resolved', 'acknowledged'
    created_at: datetime
    resolved_at: Optional[datetime]
    acknowledged_at: Optional[datetime]

@dataclass
class HealthReport:
    """健康报告"""
    report_id: str
    timestamp: datetime
    overall_health_score: float
    system_status: str  # 'healthy', 'warning', 'critical'
    metrics: List[HealthMetric]
    active_alerts: List[Alert]
    recommendations: List[str]
    summary: str

@dataclass
class NotificationConfig:
    """通知配置"""
    email_enabled: bool
    email_smtp_server: str
    email_smtp_port: int
    email_username: str
    email_password: str
    email_recipients: List[str]
    webhook_enabled: bool
    webhook_url: str
    log_enabled: bool
    log_file: str

class HealthAlertSystem:
    """系统健康报告和告警系统"""
    
    def __init__(self, db_path: str = "health_alerts.db"):
        self.db_path = db_path
        self.monitoring_active = False
        self.check_interval = 300  # 5分钟检查一次
        self.monitor_thread = None
        
        # 告警规则
        self.alert_rules = {}
        self.active_alerts = {}
        self.alert_cooldowns = {}
        
        # 通知配置
        self.notification_config = NotificationConfig(
            email_enabled=False,
            email_smtp_server="smtp.gmail.com",
            email_smtp_port=587,
            email_username="",
            email_password="",
            email_recipients=[],
            webhook_enabled=False,
            webhook_url="",
            log_enabled=True,
            log_file="health_alerts.log"
        )
        
        # 健康指标收集器
        self.metric_collectors = {}
        
        self._init_database()
        self._setup_default_rules()
    
    def _init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            # 健康指标表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS health_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    current_value REAL NOT NULL,
                    threshold_warning REAL,
                    threshold_critical REAL,
                    status TEXT NOT NULL,
                    trend TEXT,
                    description TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 告警规则表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_rules (
                    rule_id TEXT PRIMARY KEY,
                    rule_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    condition TEXT NOT NULL,
                    threshold REAL NOT NULL,
                    severity TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT 1,
                    cooldown_minutes INTEGER DEFAULT 30,
                    notification_channels TEXT,
                    description TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 告警记录表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id TEXT PRIMARY KEY,
                    rule_id TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    current_value REAL NOT NULL,
                    threshold REAL NOT NULL,
                    severity TEXT NOT NULL,
                    message TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    resolved_at DATETIME,
                    acknowledged_at DATETIME
                )
            """)
            
            # 健康报告表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS health_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id TEXT UNIQUE NOT NULL,
                    overall_health_score REAL NOT NULL,
                    system_status TEXT NOT NULL,
                    summary TEXT,
                    recommendations TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 通知历史表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS notification_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
    
    def _setup_default_rules(self):
        """设置默认告警规则"""
        default_rules = [
            AlertRule(
                rule_id="cpu_high",
                rule_name="CPU使用率过高",
                metric_name="cpu_usage",
                condition="greater_than",
                threshold=80.0,
                severity="warning",
                enabled=True,
                cooldown_minutes=15,
                notification_channels=["log", "email"],
                description="CPU使用率超过80%"
            ),
            AlertRule(
                rule_id="cpu_critical",
                rule_name="CPU使用率严重过高",
                metric_name="cpu_usage",
                condition="greater_than",
                threshold=95.0,
                severity="critical",
                enabled=True,
                cooldown_minutes=5,
                notification_channels=["log", "email", "webhook"],
                description="CPU使用率超过95%"
            ),
            AlertRule(
                rule_id="memory_high",
                rule_name="内存使用率过高",
                metric_name="memory_usage",
                condition="greater_than",
                threshold=85.0,
                severity="warning",
                enabled=True,
                cooldown_minutes=15,
                notification_channels=["log", "email"],
                description="内存使用率超过85%"
            ),
            AlertRule(
                rule_id="disk_space_low",
                rule_name="磁盘空间不足",
                metric_name="disk_usage",
                condition="greater_than",
                threshold=90.0,
                severity="critical",
                enabled=True,
                cooldown_minutes=30,
                notification_channels=["log", "email", "webhook"],
                description="磁盘使用率超过90%"
            ),
            AlertRule(
                rule_id="data_quality_low",
                rule_name="数据质量分数过低",
                metric_name="data_quality_score",
                condition="less_than",
                threshold=70.0,
                severity="warning",
                enabled=True,
                cooldown_minutes=60,
                notification_channels=["log", "email"],
                description="数据质量分数低于70%"
            ),
            AlertRule(
                rule_id="service_down",
                rule_name="服务不可用",
                metric_name="service_availability",
                condition="less_than",
                threshold=95.0,
                severity="critical",
                enabled=True,
                cooldown_minutes=5,
                notification_channels=["log", "email", "webhook"],
                description="服务可用性低于95%"
            )
        ]
        
        for rule in default_rules:
            self.add_alert_rule(rule)
    
    def add_alert_rule(self, rule: AlertRule):
        """添加告警规则"""
        self.alert_rules[rule.rule_id] = rule
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO alert_rules 
                (rule_id, rule_name, metric_name, condition, threshold, 
                 severity, enabled, cooldown_minutes, notification_channels, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.rule_id, rule.rule_name, rule.metric_name, rule.condition,
                rule.threshold, rule.severity, rule.enabled, rule.cooldown_minutes,
                json.dumps(rule.notification_channels), rule.description
            ))
            conn.commit()
    
    def register_metric_collector(self, metric_name: str, collector_func: Callable[[], float]):
        """注册指标收集器"""
        self.metric_collectors[metric_name] = collector_func
        logger.info(f"已注册指标收集器: {metric_name}")
    
    def collect_system_metrics(self) -> List[HealthMetric]:
        """收集系统指标"""
        metrics = []
        
        try:
            # CPU使用率
            import psutil
            cpu_usage = psutil.cpu_percent(interval=1)
            metrics.append(HealthMetric(
                metric_name="cpu_usage",
                current_value=cpu_usage,
                threshold_warning=80.0,
                threshold_critical=95.0,
                status="healthy" if cpu_usage < 80 else "warning" if cpu_usage < 95 else "critical",
                trend="stable",
                description=f"CPU使用率: {cpu_usage:.1f}%",
                timestamp=datetime.now()
            ))
            
            # 内存使用率
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            metrics.append(HealthMetric(
                metric_name="memory_usage",
                current_value=memory_usage,
                threshold_warning=85.0,
                threshold_critical=95.0,
                status="healthy" if memory_usage < 85 else "warning" if memory_usage < 95 else "critical",
                trend="stable",
                description=f"内存使用率: {memory_usage:.1f}%",
                timestamp=datetime.now()
            ))
            
            # 磁盘使用率
            disk = psutil.disk_usage('/')
            disk_usage = (disk.used / disk.total) * 100
            metrics.append(HealthMetric(
                metric_name="disk_usage",
                current_value=disk_usage,
                threshold_warning=80.0,
                threshold_critical=90.0,
                status="healthy" if disk_usage < 80 else "warning" if disk_usage < 90 else "critical",
                trend="stable",
                description=f"磁盘使用率: {disk_usage:.1f}%",
                timestamp=datetime.now()
            ))
            
        except ImportError:
            # 如果没有psutil，使用模拟数据
            import random
            metrics.extend([
                HealthMetric(
                    metric_name="cpu_usage",
                    current_value=random.uniform(20, 60),
                    threshold_warning=80.0,
                    threshold_critical=95.0,
                    status="healthy",
                    trend="stable",
                    description="CPU使用率 (模拟)",
                    timestamp=datetime.now()
                ),
                HealthMetric(
                    metric_name="memory_usage",
                    current_value=random.uniform(40, 70),
                    threshold_warning=85.0,
                    threshold_critical=95.0,
                    status="healthy",
                    trend="stable",
                    description="内存使用率 (模拟)",
                    timestamp=datetime.now()
                ),
                HealthMetric(
                    metric_name="disk_usage",
                    current_value=random.uniform(30, 60),
                    threshold_warning=80.0,
                    threshold_critical=90.0,
                    status="healthy",
                    trend="stable",
                    description="磁盘使用率 (模拟)",
                    timestamp=datetime.now()
                )
            ])
        
        # 收集自定义指标
        for metric_name, collector_func in self.metric_collectors.items():
            try:
                value = collector_func()
                metrics.append(HealthMetric(
                    metric_name=metric_name,
                    current_value=value,
                    threshold_warning=80.0,
                    threshold_critical=95.0,
                    status="healthy" if value < 80 else "warning" if value < 95 else "critical",
                    trend="stable",
                    description=f"自定义指标 {metric_name}: {value:.2f}",
                    timestamp=datetime.now()
                ))
            except Exception as e:
                logger.error(f"收集指标 {metric_name} 时出错: {e}")
        
        return metrics
    
    def check_alerts(self, metrics: List[HealthMetric]) -> List[Alert]:
        """检查告警条件"""
        new_alerts = []
        
        for metric in metrics:
            # 检查所有相关的告警规则
            for rule_id, rule in self.alert_rules.items():
                if not rule.enabled or rule.metric_name != metric.metric_name:
                    continue
                
                # 检查冷却时间
                if rule_id in self.alert_cooldowns:
                    if datetime.now() < self.alert_cooldowns[rule_id]:
                        continue
                
                # 检查告警条件
                triggered = False
                if rule.condition == "greater_than" and metric.current_value > rule.threshold:
                    triggered = True
                elif rule.condition == "less_than" and metric.current_value < rule.threshold:
                    triggered = True
                elif rule.condition == "equals" and abs(metric.current_value - rule.threshold) < 0.001:
                    triggered = True
                elif rule.condition == "not_equals" and abs(metric.current_value - rule.threshold) >= 0.001:
                    triggered = True
                
                if triggered:
                    # 创建告警
                    alert_id = hashlib.md5(f"{rule_id}_{datetime.now().isoformat()}".encode()).hexdigest()[:8]
                    
                    alert = Alert(
                        alert_id=alert_id,
                        rule_id=rule_id,
                        metric_name=metric.metric_name,
                        current_value=metric.current_value,
                        threshold=rule.threshold,
                        severity=rule.severity,
                        message=f"{rule.rule_name}: {metric.description} (阈值: {rule.threshold})",
                        status="active",
                        created_at=datetime.now(),
                        resolved_at=None,
                        acknowledged_at=None
                    )
                    
                    new_alerts.append(alert)
                    self.active_alerts[alert_id] = alert
                    
                    # 设置冷却时间
                    self.alert_cooldowns[rule_id] = datetime.now() + timedelta(minutes=rule.cooldown_minutes)
                    
                    # 发送通知
                    self._send_notification(alert, rule)
        
        return new_alerts
    
    def _send_notification(self, alert: Alert, rule: AlertRule):
        """发送通知"""
        for channel in rule.notification_channels:
            try:
                if channel == "log":
                    self._send_log_notification(alert)
                elif channel == "email" and self.notification_config.email_enabled:
                    self._send_email_notification(alert)
                elif channel == "webhook" and self.notification_config.webhook_enabled:
                    self._send_webhook_notification(alert)
                
                # 记录通知历史
                self._record_notification(alert.alert_id, channel, "success", "通知发送成功")
                
            except Exception as e:
                logger.error(f"发送 {channel} 通知失败: {e}")
                self._record_notification(alert.alert_id, channel, "failed", str(e))
    
    def _send_log_notification(self, alert: Alert):
        """发送日志通知"""
        log_level = logging.CRITICAL if alert.severity == "critical" else logging.WARNING
        logger.log(log_level, f"告警: {alert.message}")
        
        # 写入告警日志文件
        if self.notification_config.log_enabled:
            log_file = self.notification_config.log_file
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now().isoformat()} - {alert.severity.upper()} - {alert.message}\n")
    
    def _send_email_notification(self, alert: Alert):
        """发送邮件通知"""
        if not self.notification_config.email_recipients:
            return
        
        subject = f"[{alert.severity.upper()}] 系统告警: {alert.metric_name}"
        body = f"""
告警详情:
- 告警ID: {alert.alert_id}
- 指标名称: {alert.metric_name}
- 当前值: {alert.current_value:.2f}
- 阈值: {alert.threshold:.2f}
- 严重程度: {alert.severity}
- 消息: {alert.message}
- 时间: {alert.created_at.isoformat()}

请及时处理此告警。
        """
        
        msg = MIMEMultipart()
        msg['From'] = self.notification_config.email_username
        msg['To'] = ", ".join(self.notification_config.email_recipients)
        msg['Subject'] = subject
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        server = smtplib.SMTP(self.notification_config.email_smtp_server, self.notification_config.email_smtp_port)
        server.starttls()
        server.login(self.notification_config.email_username, self.notification_config.email_password)
        
        text = msg.as_string()
        server.sendmail(self.notification_config.email_username, self.notification_config.email_recipients, text)
        server.quit()
    
    def _send_webhook_notification(self, alert: Alert):
        """发送Webhook通知"""
        import requests
        
        payload = {
            "alert_id": alert.alert_id,
            "metric_name": alert.metric_name,
            "current_value": alert.current_value,
            "threshold": alert.threshold,
            "severity": alert.severity,
            "message": alert.message,
            "timestamp": alert.created_at.isoformat()
        }
        
        response = requests.post(
            self.notification_config.webhook_url,
            json=payload,
            timeout=10
        )
        response.raise_for_status()
    
    def _record_notification(self, alert_id: str, channel: str, status: str, message: str):
        """记录通知历史"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO notification_history 
                (alert_id, channel, status, message)
                VALUES (?, ?, ?, ?)
            """, (alert_id, channel, status, message))
            conn.commit()
    
    def generate_health_report(self) -> HealthReport:
        """生成健康报告"""
        # 收集指标
        metrics = self.collect_system_metrics()
        
        # 检查告警
        new_alerts = self.check_alerts(metrics)
        
        # 保存指标到数据库
        self._save_metrics(metrics)
        
        # 保存告警到数据库
        for alert in new_alerts:
            self._save_alert(alert)
        
        # 计算总体健康分数
        health_scores = []
        for metric in metrics:
            if metric.status == "healthy":
                score = 100
            elif metric.status == "warning":
                score = 70
            else:  # critical
                score = 30
            health_scores.append(score)
        
        overall_health_score = sum(health_scores) / len(health_scores) if health_scores else 0
        
        # 确定系统状态
        critical_count = sum(1 for m in metrics if m.status == "critical")
        warning_count = sum(1 for m in metrics if m.status == "warning")
        
        if critical_count > 0:
            system_status = "critical"
        elif warning_count > 0:
            system_status = "warning"
        else:
            system_status = "healthy"
        
        # 生成建议
        recommendations = []
        for metric in metrics:
            if metric.status == "critical":
                recommendations.append(f"紧急处理: {metric.description}")
            elif metric.status == "warning":
                recommendations.append(f"关注: {metric.description}")
        
        # 获取活跃告警
        active_alerts = list(self.active_alerts.values())
        
        # 生成摘要
        summary = f"系统整体健康分数: {overall_health_score:.1f}%, 状态: {system_status}, 活跃告警: {len(active_alerts)} 个"
        
        report_id = hashlib.md5(f"{datetime.now().isoformat()}".encode()).hexdigest()[:8]
        
        report = HealthReport(
            report_id=report_id,
            timestamp=datetime.now(),
            overall_health_score=overall_health_score,
            system_status=system_status,
            metrics=metrics,
            active_alerts=active_alerts,
            recommendations=recommendations,
            summary=summary
        )
        
        # 保存报告
        self._save_health_report(report)
        
        return report
    
    def _save_metrics(self, metrics: List[HealthMetric]):
        """保存指标到数据库"""
        with sqlite3.connect(self.db_path) as conn:
            for metric in metrics:
                conn.execute("""
                    INSERT INTO health_metrics 
                    (metric_name, current_value, threshold_warning, threshold_critical, 
                     status, trend, description, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metric.metric_name, metric.current_value, metric.threshold_warning,
                    metric.threshold_critical, metric.status, metric.trend,
                    metric.description, metric.timestamp
                ))
            conn.commit()
    
    def _save_alert(self, alert: Alert):
        """保存告警到数据库"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO alerts 
                (alert_id, rule_id, metric_name, current_value, threshold, 
                 severity, message, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert.alert_id, alert.rule_id, alert.metric_name, alert.current_value,
                alert.threshold, alert.severity, alert.message, alert.status, alert.created_at
            ))
            conn.commit()
    
    def _save_health_report(self, report: HealthReport):
        """保存健康报告"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO health_reports 
                (report_id, overall_health_score, system_status, summary, recommendations)
                VALUES (?, ?, ?, ?, ?)
            """, (
                report.report_id, report.overall_health_score, report.system_status,
                report.summary, json.dumps(report.recommendations)
            ))
            conn.commit()
    
    def start_monitoring(self):
        """启动健康监控"""
        if self.monitoring_active:
            logger.warning("健康监控已经在运行中")
            return
        
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("系统健康监控已启动")
    
    def stop_monitoring(self):
        """停止监控"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("系统健康监控已停止")
    
    def _monitoring_loop(self):
        """监控循环"""
        while self.monitoring_active:
            try:
                logger.info("生成健康报告...")
                report = self.generate_health_report()
                
                logger.info(f"健康报告生成完成 - 总体分数: {report.overall_health_score:.1f}%")
                logger.info(f"系统状态: {report.system_status}, 活跃告警: {len(report.active_alerts)} 个")
                
                # 如果有严重问题，记录详细信息
                if report.system_status == "critical":
                    logger.critical(f"系统状态严重: {report.summary}")
                    for rec in report.recommendations:
                        logger.critical(f"建议: {rec}")
                
            except Exception as e:
                logger.error(f"健康监控过程中出错: {e}")
            
            # 等待下次检查
            for _ in range(self.check_interval):
                if not self.monitoring_active:
                    break
                time.sleep(1)
    
    def acknowledge_alert(self, alert_id: str):
        """确认告警"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].acknowledged_at = datetime.now()
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE alerts SET acknowledged_at = ? WHERE alert_id = ?",
                    (datetime.now(), alert_id)
                )
                conn.commit()
            
            logger.info(f"告警 {alert_id} 已确认")
    
    def resolve_alert(self, alert_id: str):
        """解决告警"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].resolved_at = datetime.now()
            self.active_alerts[alert_id].status = "resolved"
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE alerts SET resolved_at = ?, status = 'resolved' WHERE alert_id = ?",
                    (datetime.now(), alert_id)
                )
                conn.commit()
            
            # 从活跃告警中移除
            del self.active_alerts[alert_id]
            
            logger.info(f"告警 {alert_id} 已解决")
    
    def get_latest_report(self) -> Optional[Dict]:
        """获取最新的健康报告"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                report_data = conn.execute("""
                    SELECT report_id, overall_health_score, system_status, 
                           summary, recommendations, timestamp
                    FROM health_reports 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """).fetchone()
                
                if not report_data:
                    return None
                
                return {
                    "report_id": report_data[0],
                    "overall_health_score": report_data[1],
                    "system_status": report_data[2],
                    "summary": report_data[3],
                    "recommendations": json.loads(report_data[4]) if report_data[4] else [],
                    "timestamp": report_data[5]
                }
        
        except Exception as e:
            logger.error(f"获取最新报告时出错: {e}")
            return None
    
    def get_active_alerts(self) -> List[Dict]:
        """获取活跃告警"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                alerts = conn.execute("""
                    SELECT alert_id, rule_id, metric_name, current_value, 
                           threshold, severity, message, created_at
                    FROM alerts 
                    WHERE status = 'active'
                    ORDER BY created_at DESC
                """).fetchall()
                
                return [
                    {
                        "alert_id": alert[0],
                        "rule_id": alert[1],
                        "metric_name": alert[2],
                        "current_value": alert[3],
                        "threshold": alert[4],
                        "severity": alert[5],
                        "message": alert[6],
                        "created_at": alert[7]
                    } for alert in alerts
                ]
        
        except Exception as e:
            logger.error(f"获取活跃告警时出错: {e}")
            return []
    
    def cleanup_old_data(self, days: int = 30):
        """清理旧数据"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM health_metrics WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM alerts WHERE created_at < ? AND status = 'resolved'", (cutoff_date,))
            conn.execute("DELETE FROM health_reports WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM notification_history WHERE timestamp < ?", (cutoff_date,))
            conn.commit()
        
        logger.info(f"已清理 {days} 天前的旧数据")

def main():
    """测试健康告警系统"""
    print("=== 系统健康报告和告警系统测试 ===")
    
    # 创建系统实例
    health_system = HealthAlertSystem()
    
    # 注册自定义指标收集器
    def get_data_quality_score():
        import random
        return random.uniform(60, 95)  # 模拟数据质量分数
    
    def get_service_availability():
        import random
        return random.uniform(90, 100)  # 模拟服务可用性
    
    health_system.register_metric_collector("data_quality_score", get_data_quality_score)
    health_system.register_metric_collector("service_availability", get_service_availability)
    
    # 生成健康报告
    print("\n生成健康报告...")
    report = health_system.generate_health_report()
    
    print(f"\n报告ID: {report.report_id}")
    print(f"总体健康分数: {report.overall_health_score:.1f}%")
    print(f"系统状态: {report.system_status}")
    print(f"摘要: {report.summary}")
    
    print("\n健康指标:")
    for metric in report.metrics:
        print(f"- {metric.metric_name}: {metric.current_value:.2f} ({metric.status}) - {metric.description}")
    
    if report.active_alerts:
        print("\n活跃告警:")
        for alert in report.active_alerts:
            print(f"- [{alert.severity}] {alert.message}")
    
    if report.recommendations:
        print("\n建议:")
        for i, rec in enumerate(report.recommendations, 1):
            print(f"{i}. {rec}")
    
    # 获取最新报告
    print("\n获取最新报告...")
    latest_report = health_system.get_latest_report()
    if latest_report:
        print(f"最新报告时间: {latest_report['timestamp']}")
        print(f"系统状态: {latest_report['system_status']}")
    
    # 获取活跃告警
    active_alerts = health_system.get_active_alerts()
    print(f"\n当前活跃告警数量: {len(active_alerts)}")
    
    # 启动短期监控测试
    print("\n启动监控测试 (15秒)...")
    health_system.check_interval = 5  # 5秒检查一次
    health_system.start_monitoring()
    
    time.sleep(15)
    
    health_system.stop_monitoring()
    
    print("\n=== 健康告警系统测试完成 ===")

if __name__ == "__main__":
    main()