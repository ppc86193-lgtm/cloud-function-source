#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
告警通知器适配器
为数据质量检查系统提供告警通知功能
"""

import os
import sys
from typing import Dict, Any, Optional

# 添加当前目录到路径
sys.path.append(os.path.dirname(__file__))

from alert_notification_system import AlertNotificationSystem, AlertLevel, AlertType

class AlertNotifier:
    """告警通知器适配器"""
    
    def __init__(self, config_path: str = None):
        """初始化告警通知器"""
        self.alert_system = AlertNotificationSystem(config_path)
    
    def send_alert(self, title: str, message: str, severity: str = 'info', 
                   category: str = 'system', source: str = 'data_quality_checker',
                   metadata: Dict[str, Any] = None) -> bool:
        """发送告警
        
        Args:
            title: 告警标题
            message: 告警消息
            severity: 严重程度 ('info', 'warning', 'critical', 'emergency')
            category: 告警类别 ('system', 'data_quality', 'api', 'performance', 'security')
            source: 告警来源
            metadata: 额外元数据
            
        Returns:
            bool: 发送是否成功
        """
        try:
            # 映射严重程度
            level_mapping = {
                'info': AlertLevel.INFO,
                'warning': AlertLevel.WARNING,
                'critical': AlertLevel.CRITICAL,
                'emergency': AlertLevel.EMERGENCY
            }
            
            # 映射告警类型
            type_mapping = {
                'system': AlertType.SYSTEM,
                'data_quality': AlertType.DATA_QUALITY,
                'api': AlertType.API,
                'performance': AlertType.PERFORMANCE,
                'security': AlertType.SECURITY
            }
            
            alert_level = level_mapping.get(severity.lower(), AlertLevel.INFO)
            alert_type = type_mapping.get(category.lower(), AlertType.SYSTEM)
            
            # 创建告警
            alert = self.alert_system.create_alert(
                level=alert_level,
                alert_type=alert_type,
                title=title,
                message=message,
                source=source,
                metadata=metadata
            )
            
            # 发送告警
            results = self.alert_system.send_alert(alert)
            
            # 如果至少有一个渠道发送成功，则认为成功
            return any(results.values()) if results else True
            
        except Exception as e:
            print(f"发送告警失败: {e}")
            return False
    
    def get_alert_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """获取告警统计"""
        return self.alert_system.get_alert_statistics(hours)
    
    def get_active_alerts(self):
        """获取活跃告警"""
        return self.alert_system.get_active_alerts()
    
    def resolve_alert(self, alert_id: str):
        """解决告警"""
        self.alert_system.resolve_alert(alert_id)