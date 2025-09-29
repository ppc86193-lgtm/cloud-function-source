#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
监控仪表板模块
提供API状态、数据质量和系统健康的综合监控视图
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict

from python.api_monitor import APIMonitor, APIHealthStatus
from python.data_quality_monitor import DataQualityMonitor, DataQualityMetrics
from python.error_handler import ErrorHandler

@dataclass
class SystemHealthStatus:
    """系统健康状态"""
    timestamp: datetime
    overall_status: str  # healthy, degraded, unhealthy
    api_status: Dict[str, str]
    data_quality_status: Dict[str, str]
    error_rate: float
    uptime_percentage: float
    active_alerts: List[str]
    performance_metrics: Dict[str, float]

class MonitoringDashboard:
    """监控仪表板"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 初始化监控组件
        self.api_monitor = APIMonitor(config)
        self.data_quality_monitor = DataQualityMonitor(config)
        self.error_handler = ErrorHandler(config)
        
        # 系统状态历史
        self.health_history = []
        self.max_history = 1000
        
        # 性能指标
        self.performance_metrics = {
            'api_response_times': defaultdict(list),
            'data_processing_times': [],
            'error_counts': defaultdict(int),
            'success_counts': defaultdict(int)
        }
    
    def run_comprehensive_health_check(self) -> SystemHealthStatus:
        """运行综合健康检查"""
        timestamp = datetime.now()
        
        # 1. API健康检查
        api_health_results = self.api_monitor.run_health_check()
        api_status = {}
        api_healthy = True
        
        for api_type, health_status in api_health_results.items():
            api_status[api_type] = health_status.status
            if health_status.status != 'healthy':
                api_healthy = False
        
        # 2. 数据质量检查
        data_quality_summary = self.data_quality_monitor.get_quality_summary(hours=1)
        data_quality_status = {}
        data_quality_healthy = True
        
        for source, summary in data_quality_summary.items():
            data_quality_status[source] = summary['status']
            if summary['status'] != 'good':
                data_quality_healthy = False
        
        # 3. 错误率统计
        error_summary = self.error_handler.get_error_summary(hours=1)
        total_requests = sum(self.performance_metrics['success_counts'].values()) + sum(self.performance_metrics['error_counts'].values())
        total_errors = sum(self.performance_metrics['error_counts'].values())
        error_rate = (total_errors / total_requests) if total_requests > 0 else 0
        
        # 4. 计算运行时间百分比
        uptime_percentage = self._calculate_uptime_percentage()
        
        # 5. 收集活跃告警
        active_alerts = []
        
        # API告警
        for api_type, health_status in api_health_results.items():
            if health_status.status != 'healthy':
                active_alerts.append(f"API {api_type} 状态异常: {health_status.error_message}")
        
        # 数据质量告警
        for source, summary in data_quality_summary.items():
            if summary['status'] == 'critical':
                active_alerts.append(f"数据质量严重问题: {source} (评分: {summary['latest_score']:.3f})")
            elif summary['status'] == 'warning':
                active_alerts.append(f"数据质量警告: {source} (评分: {summary['latest_score']:.3f})")
        
        # 错误率告警
        if error_rate > 0.1:  # 错误率超过10%
            active_alerts.append(f"系统错误率过高: {error_rate:.1%}")
        
        # 6. 性能指标
        performance_metrics = {
            'avg_api_response_time': self._calculate_avg_response_time(),
            'avg_data_processing_time': sum(self.performance_metrics['data_processing_times'][-10:]) / min(10, len(self.performance_metrics['data_processing_times'])) if self.performance_metrics['data_processing_times'] else 0,
            'requests_per_hour': self._calculate_requests_per_hour(),
            'error_rate': error_rate
        }
        
        # 7. 确定总体状态
        if api_healthy and data_quality_healthy and error_rate < 0.05 and len(active_alerts) == 0:
            overall_status = 'healthy'
        elif api_healthy and error_rate < 0.1 and len([a for a in active_alerts if 'critical' in a.lower()]) == 0:
            overall_status = 'degraded'
        else:
            overall_status = 'unhealthy'
        
        # 创建健康状态对象
        health_status = SystemHealthStatus(
            timestamp=timestamp,
            overall_status=overall_status,
            api_status=api_status,
            data_quality_status=data_quality_status,
            error_rate=error_rate,
            uptime_percentage=uptime_percentage,
            active_alerts=active_alerts,
            performance_metrics=performance_metrics
        )
        
        # 记录健康状态
        self._record_health_status(health_status)
        
        return health_status
    
    def _calculate_uptime_percentage(self) -> float:
        """计算运行时间百分比"""
        if not self.health_history:
            return 100.0
        
        # 计算最近24小时的运行时间
        now = datetime.now()
        cutoff_time = now - timedelta(hours=24)
        
        recent_checks = [
            check for check in self.health_history 
            if check.timestamp >= cutoff_time
        ]
        
        if not recent_checks:
            return 100.0
        
        healthy_checks = sum(1 for check in recent_checks if check.overall_status == 'healthy')
        return (healthy_checks / len(recent_checks)) * 100
    
    def start_monitoring(self):
        """启动监控服务"""
        self.logger.info("启动监控仪表板服务")
        # 这里可以添加定期监控逻辑，比如启动后台线程
        # 目前只是一个占位方法，确保系统启动时不会报错
        pass
    
    def stop_monitoring(self):
        """停止监控服务"""
        self.logger.info("停止监控仪表板服务")
        pass
    
    def _calculate_avg_response_time(self) -> float:
        """计算平均API响应时间"""
        all_times = []
        for api_type, times in self.performance_metrics['api_response_times'].items():
            all_times.extend(times[-10:])  # 取最近10次
        
        return sum(all_times) / len(all_times) if all_times else 0
    
    def _calculate_requests_per_hour(self) -> float:
        """计算每小时请求数"""
        total_requests = sum(self.performance_metrics['success_counts'].values()) + sum(self.performance_metrics['error_counts'].values())
        return total_requests  # 简化计算，实际应该基于时间窗口
    
    def _record_health_status(self, health_status: SystemHealthStatus):
        """记录健康状态"""
        self.health_history.append(health_status)
        
        # 保持历史记录在合理范围内
        if len(self.health_history) > self.max_history:
            self.health_history = self.health_history[-self.max_history:]
        
        # 记录日志
        self.logger.info(f"系统健康检查完成 - 状态: {health_status.overall_status}, "
                        f"错误率: {health_status.error_rate:.1%}, "
                        f"运行时间: {health_status.uptime_percentage:.1f}%, "
                        f"活跃告警: {len(health_status.active_alerts)}")
        
        if health_status.active_alerts:
            self.logger.warning(f"活跃告警: {'; '.join(health_status.active_alerts)}")
    
    def record_api_performance(self, api_type: str, response_time_ms: float, success: bool):
        """记录API性能指标"""
        self.performance_metrics['api_response_times'][api_type].append(response_time_ms)
        
        # 保持最近的记录
        if len(self.performance_metrics['api_response_times'][api_type]) > 100:
            self.performance_metrics['api_response_times'][api_type] = self.performance_metrics['api_response_times'][api_type][-100:]
        
        if success:
            self.performance_metrics['success_counts'][api_type] += 1
        else:
            self.performance_metrics['error_counts'][api_type] += 1
    
    def record_data_processing_time(self, processing_time_ms: float):
        """记录数据处理时间"""
        self.performance_metrics['data_processing_times'].append(processing_time_ms)
        
        # 保持最近的记录
        if len(self.performance_metrics['data_processing_times']) > 100:
            self.performance_metrics['data_processing_times'] = self.performance_metrics['data_processing_times'][-100:]
    
    def get_dashboard_data(self, hours: int = 24) -> Dict[str, Any]:
        """获取仪表板数据"""
        # 运行健康检查
        current_health = self.run_comprehensive_health_check()
        
        # 获取历史趋势
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_history = [h for h in self.health_history if h.timestamp >= cutoff_time]
        
        # 计算趋势数据
        status_trend = []
        error_rate_trend = []
        uptime_trend = []
        
        for h in recent_history[-24:]:  # 最近24个数据点
            status_trend.append({
                'timestamp': h.timestamp.isoformat(),
                'status': h.overall_status
            })
            error_rate_trend.append({
                'timestamp': h.timestamp.isoformat(),
                'error_rate': h.error_rate
            })
            uptime_trend.append({
                'timestamp': h.timestamp.isoformat(),
                'uptime': h.uptime_percentage
            })
        
        # API状态统计
        api_stats = {}
        for api_type in ['realtime', 'historical']:
            success_count = self.performance_metrics['success_counts'].get(api_type, 0)
            error_count = self.performance_metrics['error_counts'].get(api_type, 0)
            total_count = success_count + error_count
            
            api_stats[api_type] = {
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': (success_count / total_count) if total_count > 0 else 0,
                'avg_response_time': sum(self.performance_metrics['api_response_times'][api_type][-10:]) / min(10, len(self.performance_metrics['api_response_times'][api_type])) if self.performance_metrics['api_response_times'][api_type] else 0
            }
        
        return {
            'current_status': asdict(current_health),
            'trends': {
                'status': status_trend,
                'error_rate': error_rate_trend,
                'uptime': uptime_trend
            },
            'api_statistics': api_stats,
            'data_quality_summary': self.data_quality_monitor.get_quality_summary(hours),
            'error_summary': self.error_handler.get_error_summary(hours),
            'system_metrics': {
                'total_health_checks': len(self.health_history),
                'avg_uptime_24h': sum(h.uptime_percentage for h in recent_history) / len(recent_history) if recent_history else 100,
                'total_alerts_24h': sum(len(h.active_alerts) for h in recent_history),
                'avg_error_rate_24h': sum(h.error_rate for h in recent_history) / len(recent_history) if recent_history else 0
            }
        }
    
    def generate_health_report(self, hours: int = 24) -> str:
        """生成健康报告"""
        dashboard_data = self.get_dashboard_data(hours)
        current_status = dashboard_data['current_status']
        system_metrics = dashboard_data['system_metrics']
        
        report = f"""
=== PC28 系统健康报告 ===
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
报告周期: 最近 {hours} 小时

【总体状态】
系统状态: {current_status['overall_status'].upper()}
运行时间: {current_status['uptime_percentage']:.1f}%
错误率: {current_status['error_rate']:.1%}
活跃告警: {len(current_status['active_alerts'])} 个

【API状态】
"""
        
        for api_type, status in current_status['api_status'].items():
            api_stats = dashboard_data['api_statistics'].get(api_type, {})
            report += f"- {api_type.upper()}: {status.upper()}"
            if api_stats:
                report += f" (成功率: {api_stats['success_rate']:.1%}, 平均响应时间: {api_stats['avg_response_time']:.0f}ms)"
            report += "\n"
        
        report += "\n【数据质量】\n"
        for source, summary in dashboard_data['data_quality_summary'].items():
            report += f"- {source.upper()}: {summary['status'].upper()} (评分: {summary['latest_score']:.3f})\n"
        
        if current_status['active_alerts']:
            report += "\n【活跃告警】\n"
            for alert in current_status['active_alerts']:
                report += f"- {alert}\n"
        
        report += f"""
【性能指标】
- 平均API响应时间: {current_status['performance_metrics']['avg_api_response_time']:.0f}ms
- 平均数据处理时间: {current_status['performance_metrics']['avg_data_processing_time']:.0f}ms
- 每小时请求数: {current_status['performance_metrics']['requests_per_hour']:.0f}

【24小时统计】
- 平均运行时间: {system_metrics['avg_uptime_24h']:.1f}%
- 总告警数: {system_metrics['total_alerts_24h']}
- 平均错误率: {system_metrics['avg_error_rate_24h']:.1%}
- 健康检查次数: {system_metrics['total_health_checks']}
"""
        
        return report
    
    def export_metrics_json(self, hours: int = 24) -> str:
        """导出指标为JSON格式"""
        dashboard_data = self.get_dashboard_data(hours)
        return json.dumps(dashboard_data, ensure_ascii=False, indent=2, default=str)