#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 告警集成模块
集成告警系统与监控组件，实现自动化告警触发
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alert_notification_system import AlertNotificationSystem, AlertLevel, AlertType
from data_quality_checker import DataQualityChecker
from api_monitor import APIMonitor
from monitoring_dashboard import MonitoringDashboard

@dataclass
class AlertThreshold:
    """告警阈值配置"""
    metric_name: str
    threshold_value: float
    comparison: str  # 'gt', 'lt', 'eq', 'gte', 'lte'
    alert_level: AlertLevel
    alert_type: AlertType
    description: str

class AlertIntegration:
    """告警集成管理器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """初始化告警集成"""
        self.config = self._load_config(config_path)
        self.alert_system = AlertNotificationSystem(config_path)
        self.logger = self._setup_logging()
        
        # 初始化监控组件
        self.data_quality_checker = None
        self.api_monitor = None
        self.monitoring_dashboard = None
        
        # 加载告警阈值
        self.thresholds = self._load_alert_thresholds()
        
        # 告警历史记录
        self.alert_history = []
        
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """加载配置"""
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        # 默认配置
        return {
            'bigquery': {
                'project_id': 'your-project-id',
                'dataset_id': 'pc28_data'
            },
            'upstream_api': {
                'appid': '45928',
                'secret_key': 'ca9edbfee35c22a0d6c4cf6722506af0'
            },
            'monitoring': {
                'check_interval_minutes': 5,
                'alert_cooldown_minutes': 30
            }
        }
    
    def _setup_logging(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger('alert_integration')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # 文件处理器
            os.makedirs('logs', exist_ok=True)
            file_handler = logging.FileHandler('logs/alert_integration.log', encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # 格式化器
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def _load_alert_thresholds(self) -> List[AlertThreshold]:
        """加载告警阈值配置"""
        thresholds = []
        
        # BigQuery数据质量阈值
        bq_config = self.config.get('rules', {}).get('bigquery_monitoring', {})
        dq_thresholds = bq_config.get('data_quality_thresholds', {})
        
        thresholds.extend([
            AlertThreshold(
                'data_completeness', 
                dq_thresholds.get('completeness_min', 0.95),
                'lt', AlertLevel.WARNING, AlertType.DATA_QUALITY,
                '数据完整性低于阈值'
            ),
            AlertThreshold(
                'data_consistency', 
                dq_thresholds.get('consistency_min', 0.90),
                'lt', AlertLevel.WARNING, AlertType.DATA_QUALITY,
                '数据一致性低于阈值'
            ),
            AlertThreshold(
                'data_accuracy', 
                dq_thresholds.get('accuracy_min', 0.95),
                'lt', AlertLevel.CRITICAL, AlertType.DATA_QUALITY,
                '数据准确性低于阈值'
            )
        ])
        
        # API性能阈值
        api_thresholds = bq_config.get('api_performance_thresholds', {})
        thresholds.extend([
            AlertThreshold(
                'api_response_time', 
                api_thresholds.get('response_time_max_ms', 5000),
                'gt', AlertLevel.WARNING, AlertType.PERFORMANCE,
                'API响应时间过长'
            ),
            AlertThreshold(
                'api_success_rate', 
                api_thresholds.get('success_rate_min', 0.95),
                'lt', AlertLevel.CRITICAL, AlertType.API,
                'API成功率低于阈值'
            ),
            AlertThreshold(
                'api_error_rate', 
                api_thresholds.get('error_rate_max', 0.05),
                'gt', AlertLevel.WARNING, AlertType.API,
                'API错误率过高'
            )
        ])
        
        return thresholds
    
    def initialize_monitors(self):
        """初始化监控组件"""
        try:
            # 初始化数据质量检查器
            self.data_quality_checker = DataQualityChecker()
            self.logger.info("数据质量检查器初始化成功")
            
            # 初始化API监控器
            self.api_monitor = APIMonitor(self.config)
            self.logger.info("API监控器初始化成功")
            
            # 初始化监控仪表板
            self.monitoring_dashboard = MonitoringDashboard(self.config)
            self.logger.info("监控仪表板初始化成功")
            
        except Exception as e:
            self.logger.error(f"监控组件初始化失败: {e}")
            raise
    
    def check_data_quality_alerts(self) -> List[Dict[str, Any]]:
        """检查数据质量告警"""
        alerts = []
        
        if not self.data_quality_checker:
            return alerts
        
        try:
            # 获取数据质量摘要
            quality_summary = self.data_quality_checker.get_quality_summary()
            
            # 检查各项指标
            for threshold in self.thresholds:
                if threshold.alert_type != AlertType.DATA_QUALITY:
                    continue
                
                metric_value = quality_summary.get(threshold.metric_name.replace('data_', ''), 0)
                
                if self._check_threshold(metric_value, threshold):
                    alert = self.alert_system.create_alert(
                        level=threshold.alert_level,
                        alert_type=threshold.alert_type,
                        title=f"数据质量告警: {threshold.description}",
                        message=f"{threshold.metric_name}: {metric_value:.3f} (阈值: {threshold.threshold_value})",
                        source="data_quality_monitor",
                        metadata={
                            'metric': threshold.metric_name,
                            'value': metric_value,
                            'threshold': threshold.threshold_value,
                            'quality_summary': quality_summary
                        }
                    )
                    alerts.append(alert)
                    
        except Exception as e:
            self.logger.error(f"数据质量告警检查失败: {e}")
            
        return alerts
    
    def check_api_performance_alerts(self) -> List[Dict[str, Any]]:
        """检查API性能告警"""
        alerts = []
        
        if not self.api_monitor:
            return alerts
        
        try:
            # 检查实时API健康状态
            realtime_health = self.api_monitor.check_realtime_api_health()
            historical_health = self.api_monitor.check_historical_api_health()
            
            # 检查API响应时间
            for api_type, health_data in [('realtime', realtime_health), ('historical', historical_health)]:
                if not health_data.get('healthy', True):
                    alert = self.alert_system.create_alert(
                        level=AlertLevel.CRITICAL,
                        alert_type=AlertType.API,
                        title=f"{api_type.upper()} API健康检查失败",
                        message=f"API状态: {health_data.get('status', 'unknown')}, 错误: {health_data.get('error', 'N/A')}",
                        source=f"{api_type}_api_monitor",
                        metadata=health_data
                    )
                    alerts.append(alert)
                
                # 检查响应时间
                response_time = health_data.get('response_time', 0)
                if response_time > 0:
                    for threshold in self.thresholds:
                        if threshold.metric_name == 'api_response_time':
                            if self._check_threshold(response_time, threshold):
                                alert = self.alert_system.create_alert(
                                    level=threshold.alert_level,
                                    alert_type=threshold.alert_type,
                                    title=f"{api_type.upper()} API响应时间告警",
                                    message=f"响应时间: {response_time}ms (阈值: {threshold.threshold_value}ms)",
                                    source=f"{api_type}_api_monitor",
                                    metadata={
                                        'api_type': api_type,
                                        'response_time': response_time,
                                        'threshold': threshold.threshold_value
                                    }
                                )
                                alerts.append(alert)
                            break
                    
        except Exception as e:
            self.logger.error(f"API性能告警检查失败: {e}")
            
        return alerts
    
    def check_system_alerts(self) -> List[Dict[str, Any]]:
        """检查系统告警"""
        alerts = []
        
        if not self.monitoring_dashboard:
            return alerts
        
        try:
            # 获取仪表板数据
            dashboard_data = self.monitoring_dashboard.get_dashboard_data()
            
            # 检查系统健康状态
            system_health = dashboard_data.get('system_health', {})
            if not system_health.get('overall_healthy', True):
                alert = self.alert_system.create_alert(
                    level=AlertLevel.CRITICAL,
                    alert_type=AlertType.SYSTEM,
                    title="系统健康检查失败",
                    message=f"系统状态异常，详情: {system_health.get('issues', [])}",
                    source="system_monitor",
                    metadata=system_health
                )
                alerts.append(alert)
            
            # 检查性能指标
            performance = dashboard_data.get('performance', {})
            uptime_percentage = performance.get('uptime_percentage', 100)
            if uptime_percentage < 99.0:
                alert = self.alert_system.create_alert(
                    level=AlertLevel.WARNING,
                    alert_type=AlertType.PERFORMANCE,
                    title="系统可用性告警",
                    message=f"系统可用性: {uptime_percentage:.2f}% (低于99%)",
                    source="system_monitor",
                    metadata={'uptime_percentage': uptime_percentage}
                )
                alerts.append(alert)
                
        except Exception as e:
            self.logger.error(f"系统告警检查失败: {e}")
            
        return alerts
    
    def _check_threshold(self, value: float, threshold: AlertThreshold) -> bool:
        """检查是否超过阈值"""
        if threshold.comparison == 'gt':
            return value > threshold.threshold_value
        elif threshold.comparison == 'lt':
            return value < threshold.threshold_value
        elif threshold.comparison == 'gte':
            return value >= threshold.threshold_value
        elif threshold.comparison == 'lte':
            return value <= threshold.threshold_value
        elif threshold.comparison == 'eq':
            return value == threshold.threshold_value
        return False
    
    def run_alert_check(self) -> Dict[str, Any]:
        """运行完整的告警检查"""
        self.logger.info("开始告警检查...")
        
        all_alerts = []
        check_results = {
            'timestamp': datetime.now().isoformat(),
            'data_quality_alerts': 0,
            'api_performance_alerts': 0,
            'system_alerts': 0,
            'total_alerts': 0,
            'alerts_sent': 0,
            'errors': []
        }
        
        try:
            # 检查数据质量告警
            dq_alerts = self.check_data_quality_alerts()
            all_alerts.extend(dq_alerts)
            check_results['data_quality_alerts'] = len(dq_alerts)
            
            # 检查API性能告警
            api_alerts = self.check_api_performance_alerts()
            all_alerts.extend(api_alerts)
            check_results['api_performance_alerts'] = len(api_alerts)
            
            # 检查系统告警
            sys_alerts = self.check_system_alerts()
            all_alerts.extend(sys_alerts)
            check_results['system_alerts'] = len(sys_alerts)
            
            check_results['total_alerts'] = len(all_alerts)
            
            # 发送告警
            alerts_sent = 0
            for alert in all_alerts:
                try:
                    results = self.alert_system.send_alert(alert)
                    if any(results.values()):
                        alerts_sent += 1
                except Exception as e:
                    check_results['errors'].append(f"发送告警失败: {e}")
            
            check_results['alerts_sent'] = alerts_sent
            
            self.logger.info(f"告警检查完成: 发现{len(all_alerts)}个告警，成功发送{alerts_sent}个")
            
        except Exception as e:
            error_msg = f"告警检查过程中发生错误: {e}"
            self.logger.error(error_msg)
            check_results['errors'].append(error_msg)
        
        return check_results
    
    def start_continuous_monitoring(self, interval_minutes: int = 5):
        """启动持续监控"""
        import time
        
        self.logger.info(f"启动持续监控，检查间隔: {interval_minutes}分钟")
        
        try:
            while True:
                check_results = self.run_alert_check()
                
                # 记录检查结果
                self.alert_history.append(check_results)
                
                # 保持最近100次检查记录
                if len(self.alert_history) > 100:
                    self.alert_history = self.alert_history[-100:]
                
                # 等待下次检查
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            self.logger.info("监控已停止")
        except Exception as e:
            self.logger.error(f"持续监控异常: {e}")
            raise

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28 告警集成系统')
    parser.add_argument('command', choices=[
        'check', 'monitor', 'test', 'status'
    ], help='命令')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--interval', type=int, default=5, help='监控间隔（分钟）')
    
    args = parser.parse_args()
    
    # 初始化告警集成
    alert_integration = AlertIntegration(args.config)
    alert_integration.initialize_monitors()
    
    try:
        if args.command == 'check':
            print("\n=== 执行告警检查 ===")
            results = alert_integration.run_alert_check()
            print(json.dumps(results, ensure_ascii=False, indent=2))
            
        elif args.command == 'monitor':
            print(f"\n=== 启动持续监控 (间隔: {args.interval}分钟) ===")
            alert_integration.start_continuous_monitoring(args.interval)
            
        elif args.command == 'test':
            print("\n=== 测试告警系统 ===")
            results = alert_integration.alert_system.test_channels()
            print(f"测试结果: {results}")
            
        elif args.command == 'status':
            print("\n=== 告警系统状态 ===")
            stats = alert_integration.alert_system.get_alert_statistics()
            active_alerts = alert_integration.alert_system.get_active_alerts()
            
            print(f"告警统计: {json.dumps(stats, ensure_ascii=False, indent=2)}")
            print(f"活跃告警数量: {len(active_alerts)}")
    
    except KeyboardInterrupt:
        print("\n告警集成系统已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()