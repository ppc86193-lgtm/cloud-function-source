#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28监控和告警系统
实现实时监控、异常检测、自动告警和可视化
"""

import os
import sys
import json
import time
import logging
import smtplib
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.animation import FuncAnimation
import pandas as pd
import numpy as np

from local_database import get_local_db
from auto_repair_system import AutoRepairSystem
from cloud_sync_manager import CloudSyncManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Alert:
    """告警数据类"""
    id: str
    alert_type: str  # 'data_quality', 'system_health', 'sync_failure', 'api_error'
    severity: str  # 'low', 'medium', 'high', 'critical'
    title: str
    message: str
    source: str
    created_at: str
    resolved_at: Optional[str] = None
    status: str = 'active'  # 'active', 'resolved', 'suppressed'
    metadata: Optional[Dict] = None

@dataclass
class MetricPoint:
    """监控指标数据点"""
    timestamp: str
    metric_name: str
    value: float
    tags: Dict[str, str]

class MonitoringAlertingSystem:
    """监控和告警系统"""
    
    def __init__(self):
        """初始化监控系统"""
        self.db = get_local_db()
        self.repair_system = AutoRepairSystem()
        self.sync_manager = CloudSyncManager()
        
        # 监控状态
        self.is_monitoring = False
        self.monitor_thread = None
        
        # 告警配置
        self.alert_config = self._get_alert_config()
        
        # 监控指标
        self.metrics = {}
        self.metric_history = []
        
        # 告警历史
        self.active_alerts = []
        
        # 初始化监控表
        self._init_monitoring_tables()
    
    def _get_alert_config(self) -> Dict[str, Any]:
        """获取告警配置"""
        return {
            'thresholds': {
                'data_freshness_hours': 2,  # 数据新鲜度阈值（小时）
                'api_response_time_seconds': 10,  # API响应时间阈值（秒）
                'error_rate_percent': 5,  # 错误率阈值（百分比）
                'sync_failure_count': 3,  # 同步失败次数阈值
                'disk_usage_percent': 80,  # 磁盘使用率阈值（百分比）
                'memory_usage_percent': 85  # 内存使用率阈值（百分比）
            },
            'notification': {
                'email_enabled': False,  # 邮件通知开关
                'email_smtp_server': 'smtp.gmail.com',
                'email_smtp_port': 587,
                'email_username': '',
                'email_password': '',
                'email_recipients': [],
                'webhook_enabled': False,  # Webhook通知开关
                'webhook_url': ''
            },
            'monitoring_interval_seconds': 60,  # 监控间隔（秒）
            'alert_cooldown_minutes': 15  # 告警冷却时间（分钟）
        }
    
    def _init_monitoring_tables(self):
        """初始化监控表"""
        try:
            # 创建告警表
            self.db.execute_update("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    status TEXT DEFAULT 'active',
                    metadata TEXT
                )
            """)
            
            # 创建监控指标表
            self.db.execute_update("""
                CREATE TABLE IF NOT EXISTS monitoring_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    value REAL NOT NULL,
                    tags TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建监控状态表
            self.db.execute_update("""
                CREATE TABLE IF NOT EXISTS monitoring_status (
                    component TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_check TEXT NOT NULL,
                    details TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            self.db.execute_update("CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at)")
            self.db.execute_update("CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status)")
            self.db.execute_update("CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON monitoring_metrics(timestamp)")
            self.db.execute_update("CREATE INDEX IF NOT EXISTS idx_metrics_name ON monitoring_metrics(metric_name)")
            
            logger.info("监控表初始化完成")
            
        except Exception as e:
            logger.error(f"监控表初始化失败: {e}")
    
    def collect_system_metrics(self) -> Dict[str, float]:
        """收集系统指标"""
        try:
            import psutil
            
            metrics = {
                'cpu_usage_percent': psutil.cpu_percent(interval=1),
                'memory_usage_percent': psutil.virtual_memory().percent,
                'disk_usage_percent': psutil.disk_usage('/').percent,
                'network_bytes_sent': psutil.net_io_counters().bytes_sent,
                'network_bytes_recv': psutil.net_io_counters().bytes_recv
            }
            
            return metrics
            
        except ImportError:
            logger.warning("psutil未安装，无法收集系统指标")
            return {}
        except Exception as e:
            logger.error(f"收集系统指标失败: {e}")
            return {}
    
    def collect_data_metrics(self) -> Dict[str, float]:
        """收集数据指标"""
        try:
            metrics = {}
            
            # 数据表行数统计
            tables = ['cloud_pred_today_norm', 'signal_pool_union_v3', 'lab_push_candidates_v2', 'runtime_params']
            
            for table in tables:
                try:
                    result = self.db.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                    metrics[f'{table}_row_count'] = result[0]['count'] if result else 0
                except:
                    metrics[f'{table}_row_count'] = 0
            
            # 数据新鲜度检查
            for table in tables:
                try:
                    result = self.db.execute_query(f"""
                        SELECT MAX(created_at) as latest_time FROM {table}
                    """)
                    
                    if result and result[0]['latest_time']:
                        latest_time = datetime.fromisoformat(result[0]['latest_time'])
                        hours_old = (datetime.now() - latest_time).total_seconds() / 3600
                        metrics[f'{table}_data_age_hours'] = hours_old
                    else:
                        metrics[f'{table}_data_age_hours'] = 999  # 表示数据缺失
                except:
                    metrics[f'{table}_data_age_hours'] = 999
            
            # API健康状态
            try:
                from local_api_collector import LocalAPICollector
                collector = LocalAPICollector()
                api_healthy = collector.test_api_connection()
                metrics['api_health_status'] = 1.0 if api_healthy else 0.0
            except:
                metrics['api_health_status'] = 0.0
            
            # 同步状态
            try:
                sync_report = self.sync_manager.get_sync_status_report()
                metrics['sync_health_score'] = 1.0 if sync_report['summary']['sync_health'] == 'healthy' else 0.0
                metrics['synced_tables_count'] = sync_report['summary']['synced_tables']
                metrics['failed_tables_count'] = sync_report['summary']['failed_tables']
            except:
                metrics['sync_health_score'] = 0.0
                metrics['synced_tables_count'] = 0
                metrics['failed_tables_count'] = 0
            
            return metrics
            
        except Exception as e:
            logger.error(f"收集数据指标失败: {e}")
            return {}
    
    def store_metrics(self, metrics: Dict[str, float]):
        """存储监控指标"""
        try:
            timestamp = datetime.now().isoformat()
            
            for metric_name, value in metrics.items():
                self.db.execute_update("""
                    INSERT INTO monitoring_metrics (timestamp, metric_name, value, tags)
                    VALUES (?, ?, ?, ?)
                """, (timestamp, metric_name, value, json.dumps({})))
            
            # 保留最近7天的指标数据
            cutoff_time = (datetime.now() - timedelta(days=7)).isoformat()
            self.db.execute_update("""
                DELETE FROM monitoring_metrics WHERE timestamp < ?
            """, (cutoff_time,))
            
        except Exception as e:
            logger.error(f"存储监控指标失败: {e}")
    
    def check_alert_conditions(self, metrics: Dict[str, float]) -> List[Alert]:
        """检查告警条件"""
        alerts = []
        thresholds = self.alert_config['thresholds']
        
        try:
            # 检查数据新鲜度
            for metric_name, value in metrics.items():
                if metric_name.endswith('_data_age_hours'):
                    table_name = metric_name.replace('_data_age_hours', '')
                    if value > thresholds['data_freshness_hours']:
                        alerts.append(Alert(
                            id=f"data_freshness_{table_name}_{int(time.time())}",
                            alert_type='data_quality',
                            severity='high' if value > 24 else 'medium',
                            title=f"数据新鲜度告警: {table_name}",
                            message=f"表 {table_name} 数据已过期 {value:.1f} 小时",
                            source='monitoring_system',
                            created_at=datetime.now().isoformat(),
                            metadata={'table_name': table_name, 'age_hours': value}
                        ))
            
            # 检查API健康状态
            if metrics.get('api_health_status', 0) == 0:
                alerts.append(Alert(
                    id=f"api_health_{int(time.time())}",
                    alert_type='system_health',
                    severity='critical',
                    title="API连接异常",
                    message="PC28上游API连接失败",
                    source='monitoring_system',
                    created_at=datetime.now().isoformat()
                ))
            
            # 检查同步状态
            if metrics.get('sync_health_score', 0) == 0:
                failed_count = metrics.get('failed_tables_count', 0)
                alerts.append(Alert(
                    id=f"sync_failure_{int(time.time())}",
                    alert_type='sync_failure',
                    severity='high',
                    title="数据同步异常",
                    message=f"有 {failed_count} 个表同步失败",
                    source='monitoring_system',
                    created_at=datetime.now().isoformat(),
                    metadata={'failed_count': failed_count}
                ))
            
            # 检查系统资源
            if 'memory_usage_percent' in metrics:
                memory_usage = metrics['memory_usage_percent']
                if memory_usage > thresholds['memory_usage_percent']:
                    alerts.append(Alert(
                        id=f"memory_usage_{int(time.time())}",
                        alert_type='system_health',
                        severity='medium',
                        title="内存使用率过高",
                        message=f"内存使用率: {memory_usage:.1f}%",
                        source='monitoring_system',
                        created_at=datetime.now().isoformat(),
                        metadata={'memory_usage': memory_usage}
                    ))
            
            if 'disk_usage_percent' in metrics:
                disk_usage = metrics['disk_usage_percent']
                if disk_usage > thresholds['disk_usage_percent']:
                    alerts.append(Alert(
                        id=f"disk_usage_{int(time.time())}",
                        alert_type='system_health',
                        severity='medium',
                        title="磁盘使用率过高",
                        message=f"磁盘使用率: {disk_usage:.1f}%",
                        source='monitoring_system',
                        created_at=datetime.now().isoformat(),
                        metadata={'disk_usage': disk_usage}
                    ))
            
            return alerts
            
        except Exception as e:
            logger.error(f"检查告警条件失败: {e}")
            return []
    
    def create_alert(self, alert: Alert):
        """创建告警"""
        try:
            # 检查是否已存在相同类型的活跃告警（防止重复告警）
            existing = self.db.execute_query("""
                SELECT id FROM alerts 
                WHERE alert_type = ? AND source = ? AND status = 'active'
                AND created_at > ?
            """, (alert.alert_type, alert.source, 
                  (datetime.now() - timedelta(minutes=self.alert_config['alert_cooldown_minutes'])).isoformat()))
            
            if existing:
                logger.debug(f"告警已存在，跳过: {alert.title}")
                return
            
            # 存储告警
            self.db.execute_update("""
                INSERT INTO alerts (id, alert_type, severity, title, message, source, created_at, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (alert.id, alert.alert_type, alert.severity, alert.title, alert.message,
                  alert.source, alert.created_at, alert.status, json.dumps(alert.metadata or {})))
            
            # 发送通知
            self._send_alert_notification(alert)
            
            logger.warning(f"创建告警: {alert.title} - {alert.message}")
            
        except Exception as e:
            logger.error(f"创建告警失败: {e}")
    
    def resolve_alert(self, alert_id: str, resolution_message: str = ""):
        """解决告警"""
        try:
            self.db.execute_update("""
                UPDATE alerts 
                SET status = 'resolved', resolved_at = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), alert_id))
            
            logger.info(f"告警已解决: {alert_id} - {resolution_message}")
            
        except Exception as e:
            logger.error(f"解决告警失败: {e}")
    
    def _send_alert_notification(self, alert: Alert):
        """发送告警通知"""
        try:
            # 邮件通知
            if self.alert_config['notification']['email_enabled']:
                self._send_email_notification(alert)
            
            # Webhook通知
            if self.alert_config['notification']['webhook_enabled']:
                self._send_webhook_notification(alert)
                
        except Exception as e:
            logger.error(f"发送告警通知失败: {e}")
    
    def _send_email_notification(self, alert: Alert):
        """发送邮件通知"""
        try:
            config = self.alert_config['notification']
            
            msg = MIMEMultipart()
            msg['From'] = config['email_username']
            msg['To'] = ', '.join(config['email_recipients'])
            msg['Subject'] = f"[PC28告警] {alert.title}"
            
            body = f"""
告警详情:
- 类型: {alert.alert_type}
- 严重程度: {alert.severity}
- 标题: {alert.title}
- 消息: {alert.message}
- 来源: {alert.source}
- 时间: {alert.created_at}

请及时处理。
            """
            
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            server = smtplib.SMTP(config['email_smtp_server'], config['email_smtp_port'])
            server.starttls()
            server.login(config['email_username'], config['email_password'])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"邮件告警发送成功: {alert.title}")
            
        except Exception as e:
            logger.error(f"发送邮件告警失败: {e}")
    
    def _send_webhook_notification(self, alert: Alert):
        """发送Webhook通知"""
        try:
            import requests
            
            config = self.alert_config['notification']
            
            payload = {
                'alert_id': alert.id,
                'alert_type': alert.alert_type,
                'severity': alert.severity,
                'title': alert.title,
                'message': alert.message,
                'source': alert.source,
                'created_at': alert.created_at,
                'metadata': alert.metadata
            }
            
            response = requests.post(config['webhook_url'], json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Webhook告警发送成功: {alert.title}")
            
        except Exception as e:
            logger.error(f"发送Webhook告警失败: {e}")
    
    def start_monitoring(self):
        """启动监控"""
        if self.is_monitoring:
            logger.warning("监控已在运行中")
            return
        
        self.is_monitoring = True
        
        def monitoring_loop():
            logger.info("监控系统启动")
            
            while self.is_monitoring:
                try:
                    # 收集指标
                    system_metrics = self.collect_system_metrics()
                    data_metrics = self.collect_data_metrics()
                    
                    all_metrics = {**system_metrics, **data_metrics}
                    
                    # 存储指标
                    self.store_metrics(all_metrics)
                    
                    # 检查告警条件
                    alerts = self.check_alert_conditions(all_metrics)
                    
                    # 创建告警
                    for alert in alerts:
                        self.create_alert(alert)
                    
                    # 更新监控状态
                    self._update_monitoring_status(all_metrics)
                    
                    # 等待下次监控
                    time.sleep(self.alert_config['monitoring_interval_seconds'])
                    
                except Exception as e:
                    logger.error(f"监控循环异常: {e}")
                    time.sleep(60)  # 异常时等待1分钟后重试
            
            logger.info("监控系统停止")
        
        self.monitor_thread = threading.Thread(target=monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info("监控系统已启动")
    
    def stop_monitoring(self):
        """停止监控"""
        self.is_monitoring = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=10)
        
        logger.info("监控系统已停止")
    
    def _update_monitoring_status(self, metrics: Dict[str, float]):
        """更新监控状态"""
        try:
            timestamp = datetime.now().isoformat()
            
            # 更新各组件状态
            components = {
                'api_service': 'healthy' if metrics.get('api_health_status', 0) == 1 else 'unhealthy',
                'data_sync': 'healthy' if metrics.get('sync_health_score', 0) == 1 else 'unhealthy',
                'system_resources': 'healthy' if metrics.get('memory_usage_percent', 0) < 85 else 'degraded'
            }
            
            for component, status in components.items():
                self.db.execute_update("""
                    INSERT OR REPLACE INTO monitoring_status (component, status, last_check, details)
                    VALUES (?, ?, ?, ?)
                """, (component, status, timestamp, json.dumps(metrics)))
            
        except Exception as e:
            logger.error(f"更新监控状态失败: {e}")
    
    def get_monitoring_dashboard(self) -> Dict[str, Any]:
        """获取监控仪表板数据"""
        try:
            # 获取最新指标
            latest_metrics = self.db.execute_query("""
                SELECT metric_name, value, timestamp
                FROM monitoring_metrics
                WHERE timestamp >= ?
                ORDER BY timestamp DESC
            """, ((datetime.now() - timedelta(hours=1)).isoformat(),))
            
            # 获取活跃告警
            active_alerts = self.db.execute_query("""
                SELECT * FROM alerts
                WHERE status = 'active'
                ORDER BY created_at DESC
            """)
            
            # 获取组件状态
            component_status = self.db.execute_query("""
                SELECT * FROM monitoring_status
                ORDER BY last_check DESC
            """)
            
            # 统计信息
            alert_stats = self.db.execute_query("""
                SELECT 
                    severity,
                    COUNT(*) as count
                FROM alerts
                WHERE status = 'active'
                GROUP BY severity
            """)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'monitoring_active': self.is_monitoring,
                'latest_metrics': latest_metrics,
                'active_alerts': active_alerts,
                'component_status': component_status,
                'alert_statistics': alert_stats,
                'system_health': self._calculate_system_health(latest_metrics, active_alerts)
            }
            
        except Exception as e:
            logger.error(f"获取监控仪表板失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _calculate_system_health(self, metrics: List[Dict], alerts: List[Dict]) -> str:
        """计算系统健康状态"""
        try:
            # 检查是否有严重告警
            critical_alerts = [a for a in alerts if a['severity'] == 'critical']
            if critical_alerts:
                return 'critical'
            
            high_alerts = [a for a in alerts if a['severity'] == 'high']
            if high_alerts:
                return 'degraded'
            
            medium_alerts = [a for a in alerts if a['severity'] == 'medium']
            if len(medium_alerts) > 3:
                return 'degraded'
            
            return 'healthy'
            
        except Exception as e:
            logger.error(f"计算系统健康状态失败: {e}")
            return 'unknown'
    
    def generate_monitoring_report(self, hours: int = 24) -> Dict[str, Any]:
        """生成监控报告"""
        try:
            start_time = (datetime.now() - timedelta(hours=hours)).isoformat()
            
            # 获取指标历史
            metrics_history = self.db.execute_query("""
                SELECT * FROM monitoring_metrics
                WHERE timestamp >= ?
                ORDER BY timestamp
            """, (start_time,))
            
            # 获取告警历史
            alerts_history = self.db.execute_query("""
                SELECT * FROM alerts
                WHERE created_at >= ?
                ORDER BY created_at
            """, (start_time,))
            
            # 统计分析
            total_alerts = len(alerts_history)
            resolved_alerts = len([a for a in alerts_history if a['status'] == 'resolved'])
            active_alerts = len([a for a in alerts_history if a['status'] == 'active'])
            
            # 按类型统计告警
            alert_by_type = {}
            for alert in alerts_history:
                alert_type = alert['alert_type']
                alert_by_type[alert_type] = alert_by_type.get(alert_type, 0) + 1
            
            return {
                'report_period_hours': hours,
                'generated_at': datetime.now().isoformat(),
                'summary': {
                    'total_alerts': total_alerts,
                    'resolved_alerts': resolved_alerts,
                    'active_alerts': active_alerts,
                    'resolution_rate': resolved_alerts / total_alerts if total_alerts > 0 else 0
                },
                'alert_breakdown': alert_by_type,
                'metrics_collected': len(metrics_history),
                'system_uptime': self._calculate_uptime(metrics_history),
                'recommendations': self._generate_recommendations(alerts_history, metrics_history)
            }
            
        except Exception as e:
            logger.error(f"生成监控报告失败: {e}")
            return {
                'generated_at': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _calculate_uptime(self, metrics: List[Dict]) -> float:
        """计算系统正常运行时间百分比"""
        try:
            if not metrics:
                return 0.0
            
            # 统计API健康状态
            api_metrics = [m for m in metrics if m['metric_name'] == 'api_health_status']
            if not api_metrics:
                return 0.0
            
            healthy_count = len([m for m in api_metrics if m['value'] == 1.0])
            uptime_percentage = (healthy_count / len(api_metrics)) * 100
            
            return uptime_percentage
            
        except Exception as e:
            logger.error(f"计算正常运行时间失败: {e}")
            return 0.0
    
    def _generate_recommendations(self, alerts: List[Dict], metrics: List[Dict]) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        try:
            # 分析告警模式
            frequent_alerts = {}
            for alert in alerts:
                alert_type = alert['alert_type']
                frequent_alerts[alert_type] = frequent_alerts.get(alert_type, 0) + 1
            
            # 生成建议
            if frequent_alerts.get('data_quality', 0) > 5:
                recommendations.append("数据质量告警频繁，建议检查数据采集流程")
            
            if frequent_alerts.get('sync_failure', 0) > 3:
                recommendations.append("同步失败告警较多，建议检查网络连接和云端配置")
            
            if frequent_alerts.get('system_health', 0) > 5:
                recommendations.append("系统健康告警频繁，建议优化资源配置")
            
            # 分析指标趋势
            memory_metrics = [m for m in metrics if m['metric_name'] == 'memory_usage_percent']
            if memory_metrics:
                avg_memory = sum(m['value'] for m in memory_metrics) / len(memory_metrics)
                if avg_memory > 70:
                    recommendations.append("平均内存使用率较高，建议增加内存或优化程序")
            
            if not recommendations:
                recommendations.append("系统运行良好，继续保持监控")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"生成改进建议失败: {e}")
            return ["无法生成建议，请检查系统状态"]

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28监控和告警系统')
    parser.add_argument('--action', choices=['start', 'stop', 'status', 'dashboard', 'report'], 
                       default='dashboard', help='执行动作')
    parser.add_argument('--hours', type=int, default=24, help='报告时间范围（小时）')
    
    args = parser.parse_args()
    
    monitoring_system = MonitoringAlertingSystem()
    
    if args.action == 'start':
        monitoring_system.start_monitoring()
        print("监控系统已启动，按 Ctrl+C 停止...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            monitoring_system.stop_monitoring()
            print("监控系统已停止")
    
    elif args.action == 'stop':
        monitoring_system.stop_monitoring()
        print("监控系统已停止")
    
    elif args.action == 'dashboard':
        dashboard = monitoring_system.get_monitoring_dashboard()
        print(f"监控仪表板: {json.dumps(dashboard, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'report':
        report = monitoring_system.generate_monitoring_report(args.hours)
        print(f"监控报告: {json.dumps(report, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'status':
        dashboard = monitoring_system.get_monitoring_dashboard()
        print(f"监控状态: {'运行中' if dashboard['monitoring_active'] else '已停止'}")
        print(f"系统健康: {dashboard['system_health']}")
        print(f"活跃告警: {len(dashboard['active_alerts'])}")

if __name__ == "__main__":
    main()