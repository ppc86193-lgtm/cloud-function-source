#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统监控和运行状态检查模块
实现持续监控系统运行状态、数据质量检查和性能指标跟踪
"""

import asyncio
import json
import logging
import psutil
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """系统性能指标"""
    timestamp: str
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_io: Dict[str, int]
    process_count: int
    load_average: List[float]
    
@dataclass
class DataQualityMetrics:
    """数据质量指标"""
    timestamp: str
    total_records: int
    valid_records: int
    invalid_records: int
    completeness_rate: float
    accuracy_rate: float
    timeliness_score: float
    consistency_score: float
    
@dataclass
class ServiceStatus:
    """服务状态"""
    service_name: str
    status: str  # running, stopped, error
    uptime: float
    last_check: str
    error_count: int
    response_time: float
    
@dataclass
class AlertRule:
    """告警规则"""
    name: str
    metric_type: str  # system, data_quality, service
    threshold: float
    operator: str  # >, <, >=, <=, ==
    severity: str  # critical, warning, info
    enabled: bool = True
    
@dataclass
class Alert:
    """告警信息"""
    id: str
    rule_name: str
    message: str
    severity: str
    timestamp: str
    resolved: bool = False
    
class SystemMonitor:
    """系统监控器"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._get_default_config()
        self.is_monitoring = False
        self.metrics_history: List[SystemMetrics] = []
        self.data_quality_history: List[DataQualityMetrics] = []
        self.service_statuses: Dict[str, ServiceStatus] = {}
        self.alert_rules: List[AlertRule] = self._init_alert_rules()
        self.active_alerts: List[Alert] = []
        self.alert_counter = 0
        
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'monitor_interval': 30,  # 监控间隔（秒）
            'history_retention': 24 * 60 * 60,  # 历史数据保留时间（秒）
            'alert_cooldown': 300,  # 告警冷却时间（秒）
            'services_to_monitor': [
                'pc28_api_service',
                'backfill_service',
                'notification_service',
                'cache_service'
            ],
            'data_quality_thresholds': {
                'completeness_min': 95.0,
                'accuracy_min': 98.0,
                'timeliness_min': 90.0
            }
        }
        
    def _init_alert_rules(self) -> List[AlertRule]:
        """初始化告警规则"""
        return [
            AlertRule('high_cpu_usage', 'system', 80.0, '>', 'warning'),
            AlertRule('high_memory_usage', 'system', 85.0, '>', 'warning'),
            AlertRule('low_disk_space', 'system', 90.0, '>', 'critical'),
            AlertRule('low_data_completeness', 'data_quality', 95.0, '<', 'warning'),
            AlertRule('low_data_accuracy', 'data_quality', 98.0, '<', 'critical'),
            AlertRule('service_down', 'service', 0, '==', 'critical'),
            AlertRule('high_response_time', 'service', 5000, '>', 'warning'),  # 5秒
        ]
        
    async def start_monitoring(self):
        """开始监控"""
        if self.is_monitoring:
            logger.warning("监控已在运行中")
            return
            
        self.is_monitoring = True
        logger.info("开始系统监控...")
        
        # 启动监控任务
        tasks = [
            asyncio.create_task(self._monitor_system_metrics()),
            asyncio.create_task(self._monitor_data_quality()),
            asyncio.create_task(self._monitor_services()),
            asyncio.create_task(self._process_alerts()),
            asyncio.create_task(self._cleanup_history())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"监控过程中发生错误: {e}")
        finally:
            self.is_monitoring = False
            
    async def stop_monitoring(self):
        """停止监控"""
        self.is_monitoring = False
        logger.info("停止系统监控")
        
    async def _monitor_system_metrics(self):
        """监控系统指标"""
        while self.is_monitoring:
            try:
                # 获取系统指标
                cpu_usage = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                network = psutil.net_io_counters()
                load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else [0, 0, 0]
                
                metrics = SystemMetrics(
                    timestamp=datetime.now().isoformat(),
                    cpu_usage=cpu_usage,
                    memory_usage=memory.percent,
                    disk_usage=disk.percent,
                    network_io={
                        'bytes_sent': network.bytes_sent,
                        'bytes_recv': network.bytes_recv
                    },
                    process_count=len(psutil.pids()),
                    load_average=list(load_avg)
                )
                
                self.metrics_history.append(metrics)
                
                # 检查系统告警
                await self._check_system_alerts(metrics)
                
                logger.debug(f"系统指标更新: CPU={cpu_usage:.1f}%, 内存={memory.percent:.1f}%")
                
            except Exception as e:
                logger.error(f"获取系统指标失败: {e}")
                
            await asyncio.sleep(self.config['monitor_interval'])
            
    async def _monitor_data_quality(self):
        """监控数据质量"""
        while self.is_monitoring:
            try:
                # 模拟数据质量检查
                # 在实际应用中，这里应该连接到数据库或数据源
                total_records = 1000
                valid_records = 980
                invalid_records = total_records - valid_records
                
                quality_metrics = DataQualityMetrics(
                    timestamp=datetime.now().isoformat(),
                    total_records=total_records,
                    valid_records=valid_records,
                    invalid_records=invalid_records,
                    completeness_rate=96.5,
                    accuracy_rate=98.2,
                    timeliness_score=94.8,
                    consistency_score=97.1
                )
                
                self.data_quality_history.append(quality_metrics)
                
                # 检查数据质量告警
                await self._check_data_quality_alerts(quality_metrics)
                
                logger.debug(f"数据质量更新: 完整性={quality_metrics.completeness_rate:.1f}%")
                
            except Exception as e:
                logger.error(f"数据质量检查失败: {e}")
                
            await asyncio.sleep(self.config['monitor_interval'] * 2)  # 数据质量检查频率较低
            
    async def _monitor_services(self):
        """监控服务状态"""
        while self.is_monitoring:
            try:
                for service_name in self.config['services_to_monitor']:
                    start_time = time.time()
                    
                    # 模拟服务健康检查
                    # 在实际应用中，这里应该调用服务的健康检查接口
                    is_running = self._check_service_health(service_name)
                    response_time = (time.time() - start_time) * 1000  # 毫秒
                    
                    status = ServiceStatus(
                        service_name=service_name,
                        status='running' if is_running else 'stopped',
                        uptime=time.time() - start_time,
                        last_check=datetime.now().isoformat(),
                        error_count=0 if is_running else 1,
                        response_time=response_time
                    )
                    
                    self.service_statuses[service_name] = status
                    
                    # 检查服务告警
                    await self._check_service_alerts(status)
                    
                logger.debug(f"服务状态更新: {len(self.service_statuses)} 个服务")
                
            except Exception as e:
                logger.error(f"服务监控失败: {e}")
                
            await asyncio.sleep(self.config['monitor_interval'])
            
    def _check_service_health(self, service_name: str) -> bool:
        """检查服务健康状态（模拟）"""
        # 模拟服务状态检查
        import random
        return random.random() > 0.1  # 90%的概率服务正常
        
    async def _check_system_alerts(self, metrics: SystemMetrics):
        """检查系统告警"""
        for rule in self.alert_rules:
            if rule.metric_type != 'system' or not rule.enabled:
                continue
                
            value = getattr(metrics, rule.name.split('_')[1] + '_usage', 0)
            if self._evaluate_condition(value, rule.threshold, rule.operator):
                await self._trigger_alert(rule, f"系统{rule.name}: {value:.1f}%")
                
    async def _check_data_quality_alerts(self, metrics: DataQualityMetrics):
        """检查数据质量告警"""
        for rule in self.alert_rules:
            if rule.metric_type != 'data_quality' or not rule.enabled:
                continue
                
            if 'completeness' in rule.name:
                value = metrics.completeness_rate
            elif 'accuracy' in rule.name:
                value = metrics.accuracy_rate
            else:
                continue
                
            if self._evaluate_condition(value, rule.threshold, rule.operator):
                await self._trigger_alert(rule, f"数据质量{rule.name}: {value:.1f}%")
                
    async def _check_service_alerts(self, status: ServiceStatus):
        """检查服务告警"""
        for rule in self.alert_rules:
            if rule.metric_type != 'service' or not rule.enabled:
                continue
                
            if 'service_down' in rule.name and status.status != 'running':
                await self._trigger_alert(rule, f"服务 {status.service_name} 停止运行")
            elif 'response_time' in rule.name and status.response_time > rule.threshold:
                await self._trigger_alert(rule, f"服务 {status.service_name} 响应时间过长: {status.response_time:.0f}ms")
                
    def _evaluate_condition(self, value: float, threshold: float, operator: str) -> bool:
        """评估告警条件"""
        if operator == '>':
            return value > threshold
        elif operator == '<':
            return value < threshold
        elif operator == '>=':
            return value >= threshold
        elif operator == '<=':
            return value <= threshold
        elif operator == '==':
            return value == threshold
        return False
        
    async def _trigger_alert(self, rule: AlertRule, message: str):
        """触发告警"""
        alert_id = f"alert_{self.alert_counter}"
        self.alert_counter += 1
        
        alert = Alert(
            id=alert_id,
            rule_name=rule.name,
            message=message,
            severity=rule.severity,
            timestamp=datetime.now().isoformat()
        )
        
        self.active_alerts.append(alert)
        logger.warning(f"告警触发 [{rule.severity.upper()}]: {message}")
        
    async def _process_alerts(self):
        """处理告警"""
        while self.is_monitoring:
            try:
                # 清理已解决的告警
                self.active_alerts = [alert for alert in self.active_alerts if not alert.resolved]
                
                # 发送告警通知（这里可以集成邮件、短信、钉钉等通知方式）
                for alert in self.active_alerts:
                    if alert.severity == 'critical':
                        logger.critical(f"严重告警: {alert.message}")
                    elif alert.severity == 'warning':
                        logger.warning(f"警告告警: {alert.message}")
                        
            except Exception as e:
                logger.error(f"告警处理失败: {e}")
                
            await asyncio.sleep(60)  # 每分钟处理一次告警
            
    async def _cleanup_history(self):
        """清理历史数据"""
        while self.is_monitoring:
            try:
                cutoff_time = datetime.now() - timedelta(seconds=self.config['history_retention'])
                cutoff_str = cutoff_time.isoformat()
                
                # 清理系统指标历史
                self.metrics_history = [
                    m for m in self.metrics_history 
                    if m.timestamp > cutoff_str
                ]
                
                # 清理数据质量历史
                self.data_quality_history = [
                    m for m in self.data_quality_history 
                    if m.timestamp > cutoff_str
                ]
                
                logger.debug(f"历史数据清理完成: 系统指标={len(self.metrics_history)}, 数据质量={len(self.data_quality_history)}")
                
            except Exception as e:
                logger.error(f"历史数据清理失败: {e}")
                
            await asyncio.sleep(3600)  # 每小时清理一次
            
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态概览"""
        latest_metrics = self.metrics_history[-1] if self.metrics_history else None
        latest_quality = self.data_quality_history[-1] if self.data_quality_history else None
        
        return {
            'monitoring_status': 'running' if self.is_monitoring else 'stopped',
            'last_update': datetime.now().isoformat(),
            'system_metrics': asdict(latest_metrics) if latest_metrics else None,
            'data_quality': asdict(latest_quality) if latest_quality else None,
            'service_statuses': {name: asdict(status) for name, status in self.service_statuses.items()},
            'active_alerts': [asdict(alert) for alert in self.active_alerts],
            'alert_summary': {
                'total': len(self.active_alerts),
                'critical': len([a for a in self.active_alerts if a.severity == 'critical']),
                'warning': len([a for a in self.active_alerts if a.severity == 'warning'])
            }
        }
        
    def export_status_report(self, filepath: Optional[str] = None) -> str:
        """导出状态报告"""
        if not filepath:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = f"system_status_report_{timestamp}.json"
            
        status = self.get_system_status()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(status, f, ensure_ascii=False, indent=2)
            
        logger.info(f"状态报告已导出: {filepath}")
        return filepath
        
    def get_performance_summary(self) -> Dict[str, Any]:
        """获取性能摘要"""
        if not self.metrics_history:
            return {'error': '暂无性能数据'}
            
        recent_metrics = self.metrics_history[-10:]  # 最近10次记录
        
        avg_cpu = sum(m.cpu_usage for m in recent_metrics) / len(recent_metrics)
        avg_memory = sum(m.memory_usage for m in recent_metrics) / len(recent_metrics)
        avg_disk = sum(m.disk_usage for m in recent_metrics) / len(recent_metrics)
        
        return {
            'period': f'最近 {len(recent_metrics)} 次监控',
            'average_cpu_usage': round(avg_cpu, 2),
            'average_memory_usage': round(avg_memory, 2),
            'average_disk_usage': round(avg_disk, 2),
            'total_processes': recent_metrics[-1].process_count if recent_metrics else 0,
            'load_average': recent_metrics[-1].load_average if recent_metrics else [0, 0, 0]
        }

async def main():
    """主函数 - 系统监控测试"""
    print("=== PC28系统监控测试 ===")
    
    # 创建监控器
    monitor = SystemMonitor()
    
    try:
        # 启动监控（运行30秒用于测试）
        print("启动系统监控...")
        monitor_task = asyncio.create_task(monitor.start_monitoring())
        
        # 等待一段时间收集数据
        await asyncio.sleep(10)
        
        # 获取状态概览
        print("\n=== 系统状态概览 ===")
        status = monitor.get_system_status()
        print(f"监控状态: {status['monitoring_status']}")
        print(f"最后更新: {status['last_update']}")
        
        if status['system_metrics']:
            metrics = status['system_metrics']
            print(f"CPU使用率: {metrics['cpu_usage']:.1f}%")
            print(f"内存使用率: {metrics['memory_usage']:.1f}%")
            print(f"磁盘使用率: {metrics['disk_usage']:.1f}%")
            
        print(f"\n活跃告警: {status['alert_summary']['total']} 个")
        print(f"  - 严重: {status['alert_summary']['critical']} 个")
        print(f"  - 警告: {status['alert_summary']['warning']} 个")
        
        # 服务状态
        print("\n=== 服务状态 ===")
        for service_name, service_status in status['service_statuses'].items():
            status_icon = "✅" if service_status['status'] == 'running' else "❌"
            print(f"{status_icon} {service_name}: {service_status['status']} (响应时间: {service_status['response_time']:.0f}ms)")
            
        # 性能摘要
        print("\n=== 性能摘要 ===")
        perf_summary = monitor.get_performance_summary()
        if 'error' not in perf_summary:
            print(f"平均CPU使用率: {perf_summary['average_cpu_usage']}%")
            print(f"平均内存使用率: {perf_summary['average_memory_usage']}%")
            print(f"平均磁盘使用率: {perf_summary['average_disk_usage']}%")
            print(f"进程数量: {perf_summary['total_processes']}")
            
        # 导出状态报告
        report_file = monitor.export_status_report()
        print(f"\n状态报告已导出: {report_file}")
        
        # 停止监控
        await monitor.stop_monitoring()
        monitor_task.cancel()
        
    except Exception as e:
        logger.error(f"监控测试失败: {e}")
        await monitor.stop_monitoring()
        
    print("\n=== 监控测试完成 ===")

if __name__ == "__main__":
    asyncio.run(main())