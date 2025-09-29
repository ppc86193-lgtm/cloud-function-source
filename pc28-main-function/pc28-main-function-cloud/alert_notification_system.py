#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
告警和通知系统
监控PC28系统异常并发送告警通知
"""

import os
import sys
import json
import logging
import smtplib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from enum import Enum

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

class AlertLevel(Enum):
    """告警级别"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"

class AlertType(Enum):
    """告警类型"""
    SYSTEM = "system"
    DATA_QUALITY = "data_quality"
    API = "api"
    PERFORMANCE = "performance"
    SECURITY = "security"

@dataclass
class Alert:
    """告警信息"""
    id: str
    level: AlertLevel
    type: AlertType
    title: str
    message: str
    source: str
    timestamp: datetime
    metadata: Dict[str, Any] = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        data['level'] = self.level.value
        data['type'] = self.type.value
        data['timestamp'] = self.timestamp.isoformat()
        if self.resolved_at:
            data['resolved_at'] = self.resolved_at.isoformat()
        return data

class NotificationChannel:
    """通知渠道基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.enabled = config.get('enabled', False)
    
    def send(self, alert: Alert) -> bool:
        """发送通知"""
        raise NotImplementedError

class EmailNotification(NotificationChannel):
    """邮件通知"""
    
    def send(self, alert: Alert) -> bool:
        """发送邮件通知"""
        if not self.enabled:
            return False
        
        try:
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = self.config['from_email']
            msg['To'] = ', '.join(self.config['to_emails'])
            msg['Subject'] = Header(f"[{alert.level.value.upper()}] {alert.title}", 'utf-8')
            
            # 邮件内容
            body = self._format_email_body(alert)
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            
            # 发送邮件
            server = smtplib.SMTP(self.config['smtp_host'], self.config['smtp_port'])
            if self.config.get('use_tls', True):
                server.starttls()
            
            if self.config.get('username') and self.config.get('password'):
                server.login(self.config['username'], self.config['password'])
            
            server.send_message(msg)
            server.quit()
            
            return True
            
        except Exception as e:
            logging.error(f"邮件发送失败: {e}")
            return False
    
    def _format_email_body(self, alert: Alert) -> str:
        """格式化邮件内容"""
        level_colors = {
            AlertLevel.INFO: '#17a2b8',
            AlertLevel.WARNING: '#ffc107',
            AlertLevel.CRITICAL: '#dc3545',
            AlertLevel.EMERGENCY: '#6f42c1'
        }
        
        color = level_colors.get(alert.level, '#6c757d')
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; margin: 20px;">
            <div style="border-left: 4px solid {color}; padding-left: 20px; margin-bottom: 20px;">
                <h2 style="color: {color}; margin-top: 0;">
                    [{alert.level.value.upper()}] {alert.title}
                </h2>
            </div>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <p><strong>告警时间:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>告警来源:</strong> {alert.source}</p>
                <p><strong>告警类型:</strong> {alert.type.value}</p>
            </div>
            
            <div style="margin-bottom: 20px;">
                <h3>详细信息:</h3>
                <p style="background-color: #ffffff; padding: 15px; border: 1px solid #dee2e6; border-radius: 5px;">
                    {alert.message.replace(chr(10), '<br>')}
                </p>
            </div>
            
            {self._format_metadata(alert.metadata) if alert.metadata else ''}
            
            <hr style="margin: 30px 0;">
            <p style="color: #6c757d; font-size: 12px;">
                此邮件由PC28系统监控自动发送，请勿回复。
            </p>
        </body>
        </html>
        """
    
    def _format_metadata(self, metadata: Dict[str, Any]) -> str:
        """格式化元数据"""
        if not metadata:
            return ""
        
        html = "<div style='margin-bottom: 20px;'><h3>附加信息:</h3><ul>"
        for key, value in metadata.items():
            html += f"<li><strong>{key}:</strong> {value}</li>"
        html += "</ul></div>"
        return html

class WebhookNotification(NotificationChannel):
    """Webhook通知"""
    
    def send(self, alert: Alert) -> bool:
        """发送Webhook通知"""
        if not self.enabled:
            return False
        
        try:
            payload = {
                'alert': alert.to_dict(),
                'timestamp': datetime.now().isoformat()
            }
            
            headers = {'Content-Type': 'application/json'}
            if self.config.get('auth_token'):
                headers['Authorization'] = f"Bearer {self.config['auth_token']}"
            
            response = requests.post(
                self.config['url'],
                json=payload,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            return True
            
        except Exception as e:
            logging.error(f"Webhook发送失败: {e}")
            return False

class SlackNotification(NotificationChannel):
    """Slack通知"""
    
    def send(self, alert: Alert) -> bool:
        """发送Slack通知"""
        if not self.enabled:
            return False
        
        try:
            color_map = {
                AlertLevel.INFO: 'good',
                AlertLevel.WARNING: 'warning',
                AlertLevel.CRITICAL: 'danger',
                AlertLevel.EMERGENCY: '#6f42c1'
            }
            
            payload = {
                'text': f"[{alert.level.value.upper()}] {alert.title}",
                'attachments': [{
                    'color': color_map.get(alert.level, 'good'),
                    'fields': [
                        {
                            'title': '告警时间',
                            'value': alert.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                            'short': True
                        },
                        {
                            'title': '告警来源',
                            'value': alert.source,
                            'short': True
                        },
                        {
                            'title': '告警类型',
                            'value': alert.type.value,
                            'short': True
                        },
                        {
                            'title': '详细信息',
                            'value': alert.message,
                            'short': False
                        }
                    ]
                }]
            }
            
            response = requests.post(
                self.config['webhook_url'],
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            return True
            
        except Exception as e:
            logging.error(f"Slack发送失败: {e}")
            return False

class AlertNotificationSystem:
    """告警通知系统"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config', 'alert_config.json')
        self.config = self._load_config()
        
        # 设置日志
        self._setup_logging()
        
        # 初始化通知渠道
        self.channels = self._init_channels()
        
        # 告警存储
        self.alerts = []
        self.alert_history_file = os.path.join(os.path.dirname(__file__), 'logs', 'alert_history.json')
        
        # 告警规则
        self.rules = self.config.get('rules', {})
        
        # 静默规则
        self.silence_rules = []
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            # 创建默认配置
            default_config = self._create_default_config()
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, ensure_ascii=False, indent=2)
            return default_config
        except Exception as e:
            logging.error(f"配置文件加载失败: {e}")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """创建默认配置"""
        return {
            'channels': {
                'email': {
                    'enabled': False,
                    'smtp_host': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'use_tls': True,
                    'from_email': 'your-email@gmail.com',
                    'username': 'your-email@gmail.com',
                    'password': 'your-app-password',
                    'to_emails': ['admin@example.com']
                },
                'webhook': {
                    'enabled': False,
                    'url': 'https://your-webhook-url.com/alerts',
                    'auth_token': 'your-auth-token'
                },
                'slack': {
                    'enabled': False,
                    'webhook_url': 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
                }
            },
            'rules': {
                'rate_limiting': {
                    'max_alerts_per_hour': 10,
                    'max_alerts_per_day': 100
                },
                'escalation': {
                    'critical_repeat_minutes': 30,
                    'emergency_repeat_minutes': 15
                }
            },
            'thresholds': {
                'api_response_time_ms': 5000,
                'error_rate_percentage': 5.0,
                'data_freshness_hours': 2,
                'system_cpu_percentage': 80,
                'system_memory_percentage': 85
            }
        }
    
    def _setup_logging(self):
        """设置日志"""
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'alert_system_{datetime.now().strftime("%Y%m%d")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def _init_channels(self) -> Dict[str, NotificationChannel]:
        """初始化通知渠道"""
        channels = {}
        
        channel_config = self.config.get('channels', {})
        
        if 'email' in channel_config:
            channels['email'] = EmailNotification(channel_config['email'])
        
        if 'webhook' in channel_config:
            channels['webhook'] = WebhookNotification(channel_config['webhook'])
        
        if 'slack' in channel_config:
            channels['slack'] = SlackNotification(channel_config['slack'])
        
        return channels
    
    def create_alert(self, level: AlertLevel, alert_type: AlertType, title: str, 
                    message: str, source: str, metadata: Dict[str, Any] = None) -> Alert:
        """创建告警"""
        alert = Alert(
            id=f"{source}_{alert_type.value}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.alerts)}",
            level=level,
            type=alert_type,
            title=title,
            message=message,
            source=source,
            timestamp=datetime.now(),
            metadata=metadata or {}
        )
        
        return alert
    
    def send_alert(self, alert: Alert, channels: List[str] = None) -> Dict[str, bool]:
        """发送告警"""
        # 检查是否被静默
        if self._is_silenced(alert):
            self.logger.info(f"告警被静默: {alert.id}")
            return {}
        
        # 检查频率限制
        if not self._check_rate_limit(alert):
            self.logger.warning(f"告警频率超限: {alert.id}")
            return {}
        
        # 添加到告警列表
        self.alerts.append(alert)
        
        # 保存到历史记录
        self._save_alert_history(alert)
        
        # 发送通知
        results = {}
        target_channels = channels or list(self.channels.keys())
        
        for channel_name in target_channels:
            if channel_name in self.channels:
                try:
                    success = self.channels[channel_name].send(alert)
                    results[channel_name] = success
                    
                    if success:
                        self.logger.info(f"告警发送成功 [{channel_name}]: {alert.id}")
                    else:
                        self.logger.error(f"告警发送失败 [{channel_name}]: {alert.id}")
                        
                except Exception as e:
                    self.logger.error(f"告警发送异常 [{channel_name}]: {alert.id} - {e}")
                    results[channel_name] = False
        
        return results
    
    def _is_silenced(self, alert: Alert) -> bool:
        """检查告警是否被静默"""
        for rule in self.silence_rules:
            if self._match_silence_rule(alert, rule):
                return True
        return False
    
    def _match_silence_rule(self, alert: Alert, rule: Dict[str, Any]) -> bool:
        """匹配静默规则"""
        # 检查时间范围
        if 'start_time' in rule and 'end_time' in rule:
            start_time = datetime.fromisoformat(rule['start_time'])
            end_time = datetime.fromisoformat(rule['end_time'])
            if not (start_time <= alert.timestamp <= end_time):
                return False
        
        # 检查告警级别
        if 'levels' in rule and alert.level.value not in rule['levels']:
            return False
        
        # 检查告警类型
        if 'types' in rule and alert.type.value not in rule['types']:
            return False
        
        # 检查来源
        if 'sources' in rule and alert.source not in rule['sources']:
            return False
        
        return True
    
    def _check_rate_limit(self, alert: Alert) -> bool:
        """检查频率限制"""
        rules = self.rules.get('rate_limiting', {})
        
        now = datetime.now()
        hour_ago = now - timedelta(hours=1)
        day_ago = now - timedelta(days=1)
        
        # 统计最近的告警
        recent_alerts = [a for a in self.alerts if a.timestamp >= hour_ago]
        daily_alerts = [a for a in self.alerts if a.timestamp >= day_ago]
        
        # 检查小时限制
        max_per_hour = rules.get('max_alerts_per_hour', 10)
        if len(recent_alerts) >= max_per_hour:
            return False
        
        # 检查日限制
        max_per_day = rules.get('max_alerts_per_day', 100)
        if len(daily_alerts) >= max_per_day:
            return False
        
        return True
    
    def _save_alert_history(self, alert: Alert):
        """保存告警历史"""
        try:
            # 读取现有历史
            history = []
            if os.path.exists(self.alert_history_file):
                with open(self.alert_history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            
            # 添加新告警
            history.append(alert.to_dict())
            
            # 保持最近1000条记录
            if len(history) > 1000:
                history = history[-1000:]
            
            # 保存
            os.makedirs(os.path.dirname(self.alert_history_file), exist_ok=True)
            with open(self.alert_history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.error(f"保存告警历史失败: {e}")
    
    def add_silence_rule(self, rule: Dict[str, Any]):
        """添加静默规则"""
        self.silence_rules.append(rule)
        self.logger.info(f"添加静默规则: {rule}")
    
    def remove_silence_rule(self, rule_id: str):
        """移除静默规则"""
        self.silence_rules = [r for r in self.silence_rules if r.get('id') != rule_id]
        self.logger.info(f"移除静默规则: {rule_id}")
    
    def resolve_alert(self, alert_id: str):
        """解决告警"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.resolved = True
                alert.resolved_at = datetime.now()
                self.logger.info(f"告警已解决: {alert_id}")
                break
    
    def get_active_alerts(self) -> List[Alert]:
        """获取活跃告警"""
        return [alert for alert in self.alerts if not alert.resolved]
    
    def get_alert_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """获取告警统计"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_alerts = [a for a in self.alerts if a.timestamp >= cutoff_time]
        
        stats = {
            'total_alerts': len(recent_alerts),
            'active_alerts': len([a for a in recent_alerts if not a.resolved]),
            'resolved_alerts': len([a for a in recent_alerts if a.resolved]),
            'by_level': {},
            'by_type': {},
            'by_source': {}
        }
        
        # 按级别统计
        for level in AlertLevel:
            count = len([a for a in recent_alerts if a.level == level])
            stats['by_level'][level.value] = count
        
        # 按类型统计
        for alert_type in AlertType:
            count = len([a for a in recent_alerts if a.type == alert_type])
            stats['by_type'][alert_type.value] = count
        
        # 按来源统计
        sources = set(a.source for a in recent_alerts)
        for source in sources:
            count = len([a for a in recent_alerts if a.source == source])
            stats['by_source'][source] = count
        
        return stats
    
    def test_channels(self) -> Dict[str, bool]:
        """测试通知渠道"""
        test_alert = self.create_alert(
            level=AlertLevel.INFO,
            alert_type=AlertType.SYSTEM,
            title="通知渠道测试",
            message="这是一条测试消息，用于验证通知渠道是否正常工作。",
            source="alert_system_test",
            metadata={'test': True}
        )
        
        results = {}
        for channel_name, channel in self.channels.items():
            if channel.enabled:
                try:
                    success = channel.send(test_alert)
                    results[channel_name] = success
                    print(f"{'✅' if success else '❌'} {channel_name}: {'成功' if success else '失败'}")
                except Exception as e:
                    results[channel_name] = False
                    print(f"❌ {channel_name}: 异常 - {e}")
            else:
                results[channel_name] = False
                print(f"⚪ {channel_name}: 未启用")
        
        return results

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28 告警通知系统')
    parser.add_argument('command', choices=[
        'test', 'stats', 'active', 'send'
    ], help='命令')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--hours', type=int, default=24, help='统计时间范围（小时）')
    
    # 发送告警参数
    parser.add_argument('--level', choices=['info', 'warning', 'critical', 'emergency'], help='告警级别')
    parser.add_argument('--type', choices=['system', 'data_quality', 'api', 'performance', 'security'], help='告警类型')
    parser.add_argument('--title', type=str, help='告警标题')
    parser.add_argument('--message', type=str, help='告警消息')
    parser.add_argument('--source', type=str, help='告警来源')
    
    args = parser.parse_args()
    
    # 初始化系统
    alert_system = AlertNotificationSystem(args.config)
    
    try:
        if args.command == 'test':
            print("\n=== 测试通知渠道 ===")
            results = alert_system.test_channels()
            
        elif args.command == 'stats':
            print(f"\n=== 告警统计 (最近{args.hours}小时) ===")
            stats = alert_system.get_alert_statistics(args.hours)
            print(json.dumps(stats, ensure_ascii=False, indent=2))
            
        elif args.command == 'active':
            print("\n=== 活跃告警 ===")
            active_alerts = alert_system.get_active_alerts()
            if active_alerts:
                for alert in active_alerts:
                    print(f"[{alert.level.value.upper()}] {alert.title} - {alert.source} ({alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')})")
            else:
                print("✅ 没有活跃告警")
                
        elif args.command == 'send':
            if not all([args.level, args.type, args.title, args.message, args.source]):
                print("❌ 发送告警需要提供: --level, --type, --title, --message, --source")
                return
            
            alert = alert_system.create_alert(
                level=AlertLevel(args.level),
                alert_type=AlertType(args.type),
                title=args.title,
                message=args.message,
                source=args.source
            )
            
            results = alert_system.send_alert(alert)
            print(f"告警发送结果: {results}")
    
    except KeyboardInterrupt:
        print("\n告警系统已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()