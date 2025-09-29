#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PC28 错误处理和告警机制"""

import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass

class AlertLevel(Enum):
    """告警级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ErrorEvent:
    """错误事件数据类"""
    timestamp: str
    level: AlertLevel
    component: str
    error_type: str
    message: str
    details: Optional[Dict[str, Any]] = None
    stack_trace: Optional[str] = None

class ErrorHandler:
    """错误处理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = self._setup_logger()
        self.error_history: List[ErrorEvent] = []
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('error_handler')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def handle_api_error(self, error: Exception, api_type: str, context: Dict[str, Any] = None):
        """处理API错误"""
        error_event = ErrorEvent(
            timestamp=datetime.now().isoformat(),
            level=AlertLevel.ERROR,
            component=f"api_{api_type}",
            error_type=type(error).__name__,
            message=str(error),
            details=context,
            stack_trace=traceback.format_exc()
        )
        
        self._process_error_event(error_event)
    
    def handle_data_quality_issue(self, issue_type: str, details: Dict[str, Any]):
        """处理数据质量问题"""
        level = AlertLevel.WARNING if details.get('severity', 'medium') == 'low' else AlertLevel.ERROR
        
        error_event = ErrorEvent(
            timestamp=datetime.now().isoformat(),
            level=level,
            component="data_quality",
            error_type=issue_type,
            message=f"数据质量问题: {issue_type}",
            details=details
        )
        
        self._process_error_event(error_event)
    
    def handle_system_error(self, error: Exception, component: str, context: Dict[str, Any] = None):
        """处理系统错误"""
        error_event = ErrorEvent(
            timestamp=datetime.now().isoformat(),
            level=AlertLevel.CRITICAL,
            component=component,
            error_type=type(error).__name__,
            message=str(error),
            details=context,
            stack_trace=traceback.format_exc()
        )
        
        self._process_error_event(error_event)
    
    def handle_rate_limit_error(self, api_type: str, retry_after: Optional[int] = None):
        """处理API限制错误"""
        details = {'api_type': api_type}
        if retry_after:
            details['retry_after_seconds'] = retry_after
            
        error_event = ErrorEvent(
            timestamp=datetime.now().isoformat(),
            level=AlertLevel.WARNING,
            component=f"api_{api_type}",
            error_type="RateLimitError",
            message=f"API调用频率限制: {api_type}",
            details=details
        )
        
        self._process_error_event(error_event)
    
    def _process_error_event(self, event: ErrorEvent):
        """处理错误事件"""
        # 记录到历史
        self.error_history.append(event)
        
        # 记录日志
        self._log_error_event(event)
        
        # 发送告警
        if self._should_send_alert(event):
            self._send_alert(event)
    
    def _log_error_event(self, event: ErrorEvent):
        """记录错误事件到日志"""
        log_message = f"[{event.component}] {event.error_type}: {event.message}"
        
        if event.level == AlertLevel.INFO:
            self.logger.info(log_message)
        elif event.level == AlertLevel.WARNING:
            self.logger.warning(log_message)
        elif event.level == AlertLevel.ERROR:
            self.logger.error(log_message)
        elif event.level == AlertLevel.CRITICAL:
            self.logger.critical(log_message)
            
        # 记录详细信息
        if event.details:
            self.logger.debug(f"错误详情: {json.dumps(event.details, ensure_ascii=False, indent=2)}")
            
        if event.stack_trace:
            self.logger.debug(f"堆栈跟踪:\n{event.stack_trace}")
    
    def _should_send_alert(self, event: ErrorEvent) -> bool:
        """判断是否应该发送告警"""
        # 关键错误总是发送告警
        if event.level == AlertLevel.CRITICAL:
            return True
            
        # 检查错误频率，避免告警风暴
        recent_errors = [
            e for e in self.error_history[-10:]
            if e.component == event.component and e.error_type == event.error_type
        ]
        
        # 如果同类错误在短时间内频繁出现，只发送第一次告警
        if len(recent_errors) > 3:
            return False
            
        # ERROR级别的错误发送告警
        if event.level == AlertLevel.ERROR:
            return True
            
        return False
    
    def _send_alert(self, event: ErrorEvent):
        """发送告警"""
        try:
            alert_message = self._format_alert_message(event)
            
            # 发送到Google Cloud Logging
            self._send_to_cloud_logging(event, alert_message)
            
            # 如果配置了Telegram，发送到Telegram
            if self._has_telegram_config():
                self._send_to_telegram(alert_message)
                
        except Exception as e:
            self.logger.error(f"发送告警失败: {e}")
    
    def _format_alert_message(self, event: ErrorEvent) -> str:
        """格式化告警消息"""
        emoji_map = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨"
        }
        
        emoji = emoji_map.get(event.level, "❓")
        
        message = f"{emoji} PC28 系统告警\n\n"
        message += f"级别: {event.level.value.upper()}\n"
        message += f"组件: {event.component}\n"
        message += f"错误类型: {event.error_type}\n"
        message += f"消息: {event.message}\n"
        message += f"时间: {event.timestamp}\n"
        
        if event.details:
            message += f"\n详情:\n{json.dumps(event.details, ensure_ascii=False, indent=2)}\n"
            
        return message
    
    def _send_to_cloud_logging(self, event: ErrorEvent, message: str):
        """发送到Google Cloud Logging"""
        # 使用结构化日志记录
        log_entry = {
            'severity': event.level.value.upper(),
            'component': event.component,
            'error_type': event.error_type,
            'message': event.message,
            'timestamp': event.timestamp,
            'details': event.details,
            'alert_message': message
        }
        
        # 记录结构化日志
        self.logger.info(json.dumps(log_entry, ensure_ascii=False))
    
    def _has_telegram_config(self) -> bool:
        """检查是否配置了Telegram"""
        return (
            'telegram' in self.config and
            'bot_token' in self.config['telegram'] and
            'chat_id' in self.config['telegram']
        )
    
    def _send_to_telegram(self, message: str):
        """发送消息到Telegram"""
        try:
            import requests
            
            telegram_config = self.config['telegram']
            bot_token = telegram_config['bot_token']
            chat_id = telegram_config['chat_id']
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.logger.info("Telegram告警发送成功")
            
        except Exception as e:
            self.logger.error(f"Telegram告警发送失败: {e}")
    
    def get_error_summary(self, hours: int = 24) -> Dict[str, Any]:
        """获取错误摘要"""
        from datetime import timedelta
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_errors = [
            error for error in self.error_history
            if datetime.fromisoformat(error.timestamp) > cutoff_time
        ]
        
        if not recent_errors:
            return {'period_hours': hours, 'total_errors': 0, 'summary': 'No errors in the specified period'}
        
        # 按级别统计
        level_counts = {}
        for level in AlertLevel:
            level_counts[level.value] = len([e for e in recent_errors if e.level == level])
        
        # 按组件统计
        component_counts = {}
        for error in recent_errors:
            component_counts[error.component] = component_counts.get(error.component, 0) + 1
        
        # 按错误类型统计
        error_type_counts = {}
        for error in recent_errors:
            error_type_counts[error.error_type] = error_type_counts.get(error.error_type, 0) + 1
        
        return {
            'period_hours': hours,
            'total_errors': len(recent_errors),
            'by_level': level_counts,
            'by_component': component_counts,
            'by_error_type': error_type_counts,
            'most_recent_error': {
                'timestamp': recent_errors[-1].timestamp,
                'component': recent_errors[-1].component,
                'message': recent_errors[-1].message
            } if recent_errors else None
        }
    
    def clear_old_errors(self, days: int = 7):
        """清理旧的错误记录"""
        from datetime import timedelta
        
        cutoff_time = datetime.now() - timedelta(days=days)
        self.error_history = [
            error for error in self.error_history
            if datetime.fromisoformat(error.timestamp) > cutoff_time
        ]
        
        self.logger.info(f"已清理{days}天前的错误记录")

# 全局错误处理装饰器
def handle_errors(component: str, error_handler: ErrorHandler):
    """错误处理装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_handler.handle_system_error(e, component, {
                    'function': func.__name__,
                    'args': str(args)[:200],  # 限制长度
                    'kwargs': str(kwargs)[:200]
                })
                raise
        return wrapper
    return decorator