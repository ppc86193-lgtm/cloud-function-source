#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
并发参数调优工具
动态监控和调整PC28系统的并发参数以优化性能
"""

import os
import sys
import json
import time
import logging
import threading
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
import statistics

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

from python.api_monitor import APIMonitor
from python.bigquery_client_adapter import BQClient

@dataclass
class PerformanceMetrics:
    """性能指标"""
    timestamp: datetime
    api_response_time: float  # API响应时间(ms)
    api_success_rate: float   # API成功率(%)
    requests_per_second: float  # 每秒请求数
    cpu_usage: float         # CPU使用率(%)
    memory_usage: float      # 内存使用率(%)
    active_threads: int      # 活跃线程数
    queue_size: int         # 队列大小
    error_rate: float       # 错误率(%)
    throughput: float       # 吞吐量(records/sec)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data

@dataclass
class ConcurrencyConfig:
    """并发配置"""
    max_workers: int = 10
    queue_size: int = 1000
    batch_size: int = 100
    request_timeout: int = 30
    retry_attempts: int = 3
    rate_limit_per_second: float = 10.0
    connection_pool_size: int = 20
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConcurrencyConfig':
        """从字典创建"""
        return cls(**data)

class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.metrics_history = deque(maxlen=window_size)
        self.lock = threading.Lock()
    
    def record_metrics(self, metrics: PerformanceMetrics):
        """记录性能指标"""
        with self.lock:
            self.metrics_history.append(metrics)
    
    def get_recent_metrics(self, minutes: int = 5) -> List[PerformanceMetrics]:
        """获取最近的性能指标"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        with self.lock:
            return [m for m in self.metrics_history if m.timestamp >= cutoff_time]
    
    def get_average_metrics(self, minutes: int = 5) -> Optional[Dict[str, float]]:
        """获取平均性能指标"""
        recent_metrics = self.get_recent_metrics(minutes)
        if not recent_metrics:
            return None
        
        return {
            'api_response_time': statistics.mean(m.api_response_time for m in recent_metrics),
            'api_success_rate': statistics.mean(m.api_success_rate for m in recent_metrics),
            'requests_per_second': statistics.mean(m.requests_per_second for m in recent_metrics),
            'cpu_usage': statistics.mean(m.cpu_usage for m in recent_metrics),
            'memory_usage': statistics.mean(m.memory_usage for m in recent_metrics),
            'active_threads': statistics.mean(m.active_threads for m in recent_metrics),
            'error_rate': statistics.mean(m.error_rate for m in recent_metrics),
            'throughput': statistics.mean(m.throughput for m in recent_metrics)
        }

class ConcurrencyTuner:
    """并发参数调优器"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config', 'concurrency_config.json')
        self.config = self._load_config()
        
        # 设置日志
        self._setup_logging()
        
        # 当前并发配置
        self.current_config = ConcurrencyConfig.from_dict(self.config.get('current', {}))
        
        # 性能监控
        self.performance_monitor = PerformanceMonitor()
        
        # 当前性能指标
        self.current_metrics = {
            'cpu_usage': 0.0,
            'memory_usage': 0.0,
            'api_response_time': 0.0,
            'api_success_rate': 100.0,
            'requests_per_second': 0.0,
            'error_rate': 0.0,
            'active_threads': 0,
            'throughput': 0.0
        }
        
        # 调优规则
        self.tuning_rules = self.config.get('tuning_rules', {})
        
        # 调优历史
        self.tuning_history = []
        
        # 监控线程
        self.monitoring_thread = None
        self.monitoring_active = False
        
        # API监控器
        try:
            self.api_monitor = APIMonitor(self.config.get('api_config', {}))
        except Exception as e:
            self.logger.warning(f"API监控器初始化失败: {e}")
            self.api_monitor = None
        
        self.logger.info("并发调优器初始化完成")
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """获取当前性能指标"""
        return self.current_metrics
    
    def get_tuning_recommendations(self) -> List[Dict[str, Any]]:
        """获取调优建议"""
        recommendations = []
        
        # 基于当前指标生成建议
        if self.current_metrics['cpu_usage'] > self.tuning_rules.get('cpu_threshold_high', 80.0):
            recommendations.append({
                'type': 'reduce_workers',
                'reason': 'CPU使用率过高',
                'current_value': self.current_config.max_workers,
                'suggested_value': max(1, int(self.current_config.max_workers * 0.8))
            })
        
        if self.current_metrics['error_rate'] > self.tuning_rules.get('error_rate_threshold', 5.0):
            recommendations.append({
                'type': 'increase_timeout',
                'reason': '错误率过高',
                'current_value': self.current_config.request_timeout,
                'suggested_value': min(120, self.current_config.request_timeout + 10)
            })
        
        return recommendations
    
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
            'current': {
                'max_workers': 10,
                'queue_size': 1000,
                'batch_size': 100,
                'request_timeout': 30,
                'retry_attempts': 3,
                'rate_limit_per_second': 10.0,
                'connection_pool_size': 20
            },
            'limits': {
                'max_workers': {'min': 1, 'max': 50},
                'queue_size': {'min': 100, 'max': 10000},
                'batch_size': {'min': 10, 'max': 1000},
                'request_timeout': {'min': 5, 'max': 120},
                'retry_attempts': {'min': 1, 'max': 10},
                'rate_limit_per_second': {'min': 1.0, 'max': 100.0},
                'connection_pool_size': {'min': 5, 'max': 100}
            },
            'tuning_rules': {
                'cpu_threshold_high': 80.0,
                'cpu_threshold_low': 30.0,
                'memory_threshold_high': 85.0,
                'response_time_threshold': 5000.0,
                'error_rate_threshold': 5.0,
                'success_rate_threshold': 95.0,
                'adjustment_factor': 0.2,
                'min_observation_minutes': 2,
                'tuning_interval_minutes': 5
            },
            'monitoring': {
                'enabled': True,
                'interval_seconds': 30,
                'metrics_retention_hours': 24
            }
        }
    
    def _setup_logging(self):
        """设置日志"""
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'concurrency_tuner_{datetime.now().strftime("%Y%m%d")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def start_monitoring(self):
        """启动性能监控"""
        if self.monitoring_active:
            self.logger.warning("监控已经在运行")
            return
        
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        
        self.logger.info("性能监控已启动")
    
    def stop_monitoring(self):
        """停止性能监控"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        
        self.logger.info("性能监控已停止")
    
    def _monitoring_loop(self):
        """监控循环"""
        interval = self.config.get('monitoring', {}).get('interval_seconds', 30)
        tuning_interval = self.tuning_rules.get('tuning_interval_minutes', 5) * 60
        last_tuning_time = 0
        
        while self.monitoring_active:
            try:
                # 收集性能指标
                metrics = self._collect_performance_metrics()
                if metrics:
                    self.performance_monitor.record_metrics(metrics)
                
                # 检查是否需要调优
                current_time = time.time()
                if current_time - last_tuning_time >= tuning_interval:
                    self._auto_tune_parameters()
                    last_tuning_time = current_time
                
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"监控循环异常: {e}")
                time.sleep(interval)
    
    def _collect_performance_metrics(self) -> Optional[PerformanceMetrics]:
        """收集性能指标"""
        try:
            # 系统指标
            cpu_usage = psutil.cpu_percent(interval=1)
            memory_usage = psutil.virtual_memory().percent
            
            # API指标
            api_response_time = 0.0
            api_success_rate = 100.0
            requests_per_second = 0.0
            error_rate = 0.0
            
            if self.api_monitor:
                api_stats = self.api_monitor.get_statistics()
                api_response_time = api_stats.get('avg_response_time', 0.0)
                api_success_rate = api_stats.get('success_rate', 100.0)
                requests_per_second = api_stats.get('requests_per_second', 0.0)
                error_rate = 100.0 - api_success_rate
            
            # 线程指标
            active_threads = threading.active_count()
            
            # 创建性能指标对象
            metrics = PerformanceMetrics(
                timestamp=datetime.now(),
                api_response_time=api_response_time,
                api_success_rate=api_success_rate,
                requests_per_second=requests_per_second,
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                active_threads=active_threads,
                queue_size=0,  # 需要从实际队列获取
                error_rate=error_rate,
                throughput=requests_per_second  # 简化处理
            )
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"收集性能指标失败: {e}")
            return None
    
    def _auto_tune_parameters(self):
        """自动调优参数"""
        try:
            # 获取最近的平均指标
            min_observation_minutes = self.tuning_rules.get('min_observation_minutes', 2)
            avg_metrics = self.performance_monitor.get_average_metrics(min_observation_minutes)
            
            if not avg_metrics:
                self.logger.info("没有足够的性能数据进行调优")
                return
            
            # 分析性能并生成调优建议
            recommendations = self._analyze_performance(avg_metrics)
            
            if recommendations:
                self.logger.info(f"生成调优建议: {recommendations}")
                
                # 应用调优建议
                new_config = self._apply_recommendations(recommendations)
                
                if new_config != self.current_config:
                    self._update_configuration(new_config)
                    
                    # 记录调优历史
                    self.tuning_history.append({
                        'timestamp': datetime.now().isoformat(),
                        'old_config': self.current_config.to_dict(),
                        'new_config': new_config.to_dict(),
                        'metrics': avg_metrics,
                        'recommendations': recommendations
                    })
                    
                    self.current_config = new_config
                    self.logger.info("参数调优完成")
            
        except Exception as e:
            self.logger.error(f"自动调优失败: {e}")
    
    def _analyze_performance(self, metrics: Dict[str, float]) -> List[Dict[str, Any]]:
        """分析性能并生成建议"""
        recommendations = []
        
        # CPU使用率分析
        cpu_usage = metrics.get('cpu_usage', 0)
        cpu_high_threshold = self.tuning_rules.get('cpu_threshold_high', 80)
        cpu_low_threshold = self.tuning_rules.get('cpu_threshold_low', 30)
        
        if cpu_usage > cpu_high_threshold:
            recommendations.append({
                'parameter': 'max_workers',
                'action': 'decrease',
                'reason': f'CPU使用率过高 ({cpu_usage:.1f}%)',
                'priority': 'high'
            })
        elif cpu_usage < cpu_low_threshold:
            recommendations.append({
                'parameter': 'max_workers',
                'action': 'increase',
                'reason': f'CPU使用率较低 ({cpu_usage:.1f}%)，可以增加并发',
                'priority': 'medium'
            })
        
        # 内存使用率分析
        memory_usage = metrics.get('memory_usage', 0)
        memory_threshold = self.tuning_rules.get('memory_threshold_high', 85)
        
        if memory_usage > memory_threshold:
            recommendations.append({
                'parameter': 'batch_size',
                'action': 'decrease',
                'reason': f'内存使用率过高 ({memory_usage:.1f}%)',
                'priority': 'high'
            })
        
        # API响应时间分析
        response_time = metrics.get('api_response_time', 0)
        response_time_threshold = self.tuning_rules.get('response_time_threshold', 5000)
        
        if response_time > response_time_threshold:
            recommendations.append({
                'parameter': 'request_timeout',
                'action': 'increase',
                'reason': f'API响应时间过长 ({response_time:.0f}ms)',
                'priority': 'medium'
            })
            recommendations.append({
                'parameter': 'max_workers',
                'action': 'decrease',
                'reason': '减少并发以降低API压力',
                'priority': 'medium'
            })
        
        # 错误率分析
        error_rate = metrics.get('error_rate', 0)
        error_rate_threshold = self.tuning_rules.get('error_rate_threshold', 5)
        
        if error_rate > error_rate_threshold:
            recommendations.append({
                'parameter': 'retry_attempts',
                'action': 'increase',
                'reason': f'错误率过高 ({error_rate:.1f}%)',
                'priority': 'high'
            })
            recommendations.append({
                'parameter': 'rate_limit_per_second',
                'action': 'decrease',
                'reason': '降低请求频率以减少错误',
                'priority': 'medium'
            })
        
        # 成功率分析
        success_rate = metrics.get('api_success_rate', 100)
        success_rate_threshold = self.tuning_rules.get('success_rate_threshold', 95)
        
        if success_rate < success_rate_threshold:
            recommendations.append({
                'parameter': 'connection_pool_size',
                'action': 'increase',
                'reason': f'API成功率较低 ({success_rate:.1f}%)',
                'priority': 'medium'
            })
        
        return recommendations
    
    def _apply_recommendations(self, recommendations: List[Dict[str, Any]]) -> ConcurrencyConfig:
        """应用调优建议"""
        new_config = ConcurrencyConfig.from_dict(self.current_config.to_dict())
        adjustment_factor = self.tuning_rules.get('adjustment_factor', 0.2)
        limits = self.config.get('limits', {})
        
        # 按优先级排序
        recommendations.sort(key=lambda x: {'high': 3, 'medium': 2, 'low': 1}.get(x['priority'], 1), reverse=True)
        
        for rec in recommendations:
            parameter = rec['parameter']
            action = rec['action']
            
            if not hasattr(new_config, parameter):
                continue
            
            current_value = getattr(new_config, parameter)
            param_limits = limits.get(parameter, {})
            min_value = param_limits.get('min', 1)
            max_value = param_limits.get('max', 1000)
            
            if action == 'increase':
                if isinstance(current_value, int):
                    new_value = min(max_value, int(current_value * (1 + adjustment_factor)))
                else:
                    new_value = min(max_value, current_value * (1 + adjustment_factor))
            elif action == 'decrease':
                if isinstance(current_value, int):
                    new_value = max(min_value, int(current_value * (1 - adjustment_factor)))
                else:
                    new_value = max(min_value, current_value * (1 - adjustment_factor))
            else:
                continue
            
            setattr(new_config, parameter, new_value)
            self.logger.info(f"调整参数 {parameter}: {current_value} -> {new_value} ({rec['reason']})")
        
        return new_config
    
    def _update_configuration(self, new_config: ConcurrencyConfig):
        """更新配置"""
        try:
            # 更新配置文件
            self.config['current'] = new_config.to_dict()
            
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
            
            self.logger.info("配置文件已更新")
            
        except Exception as e:
            self.logger.error(f"更新配置失败: {e}")
    
    def manual_tune(self, parameter: str, value: Any) -> bool:
        """手动调优参数"""
        try:
            if not hasattr(self.current_config, parameter):
                self.logger.error(f"未知参数: {parameter}")
                return False
            
            # 检查参数限制
            limits = self.config.get('limits', {}).get(parameter, {})
            min_value = limits.get('min')
            max_value = limits.get('max')
            
            if min_value is not None and value < min_value:
                self.logger.error(f"参数值 {value} 小于最小值 {min_value}")
                return False
            
            if max_value is not None and value > max_value:
                self.logger.error(f"参数值 {value} 大于最大值 {max_value}")
                return False
            
            # 更新参数
            old_value = getattr(self.current_config, parameter)
            setattr(self.current_config, parameter, value)
            
            # 保存配置
            self._update_configuration(self.current_config)
            
            # 记录调优历史
            self.tuning_history.append({
                'timestamp': datetime.now().isoformat(),
                'type': 'manual',
                'parameter': parameter,
                'old_value': old_value,
                'new_value': value
            })
            
            self.logger.info(f"手动调优 {parameter}: {old_value} -> {value}")
            return True
            
        except Exception as e:
            self.logger.error(f"手动调优失败: {e}")
            return False
    
    def get_current_config(self) -> Dict[str, Any]:
        """获取当前配置"""
        return self.current_config.to_dict()
    
    def get_performance_report(self, hours: int = 1) -> Dict[str, Any]:
        """获取性能报告"""
        recent_metrics = self.performance_monitor.get_recent_metrics(hours * 60)
        
        if not recent_metrics:
            return {'error': '没有性能数据'}
        
        # 计算统计信息
        response_times = [m.api_response_time for m in recent_metrics]
        cpu_usages = [m.cpu_usage for m in recent_metrics]
        memory_usages = [m.memory_usage for m in recent_metrics]
        success_rates = [m.api_success_rate for m in recent_metrics]
        
        return {
            'period': f'最近{hours}小时',
            'total_samples': len(recent_metrics),
            'api_response_time': {
                'avg': statistics.mean(response_times),
                'min': min(response_times),
                'max': max(response_times),
                'median': statistics.median(response_times)
            },
            'cpu_usage': {
                'avg': statistics.mean(cpu_usages),
                'min': min(cpu_usages),
                'max': max(cpu_usages)
            },
            'memory_usage': {
                'avg': statistics.mean(memory_usages),
                'min': min(memory_usages),
                'max': max(memory_usages)
            },
            'api_success_rate': {
                'avg': statistics.mean(success_rates),
                'min': min(success_rates),
                'max': max(success_rates)
            },
            'current_config': self.get_current_config(),
            'recent_tuning': self.tuning_history[-5:] if self.tuning_history else []
        }
    
    def export_tuning_history(self) -> str:
        """导出调优历史"""
        return json.dumps({
            'export_time': datetime.now().isoformat(),
            'current_config': self.get_current_config(),
            'tuning_history': self.tuning_history
        }, ensure_ascii=False, indent=2)

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28 并发参数调优工具')
    parser.add_argument('command', choices=[
        'start', 'stop', 'status', 'tune', 'report', 'export', 'config'
    ], help='命令')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--parameter', type=str, help='调优参数名')
    parser.add_argument('--value', type=str, help='参数值')
    parser.add_argument('--hours', type=int, default=1, help='报告时间范围（小时）')
    
    args = parser.parse_args()
    
    # 初始化调优器
    tuner = ConcurrencyTuner(args.config)
    
    try:
        if args.command == 'start':
            print("启动并发参数调优监控...")
            tuner.start_monitoring()
            print("监控已启动，按 Ctrl+C 停止")
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n停止监控...")
                tuner.stop_monitoring()
                
        elif args.command == 'stop':
            tuner.stop_monitoring()
            print("监控已停止")
            
        elif args.command == 'status':
            print("\n=== 当前配置 ===")
            config = tuner.get_current_config()
            for key, value in config.items():
                print(f"{key}: {value}")
                
        elif args.command == 'tune':
            if not args.parameter or not args.value:
                print("❌ 手动调优需要提供 --parameter 和 --value")
                return
            
            # 尝试转换值类型
            try:
                if '.' in args.value:
                    value = float(args.value)
                else:
                    value = int(args.value)
            except ValueError:
                value = args.value
            
            success = tuner.manual_tune(args.parameter, value)
            if success:
                print(f"✅ 参数调优成功: {args.parameter} = {value}")
            else:
                print(f"❌ 参数调优失败")
                
        elif args.command == 'report':
            print(f"\n=== 性能报告 (最近{args.hours}小时) ===")
            report = tuner.get_performance_report(args.hours)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            
        elif args.command == 'export':
            history_json = tuner.export_tuning_history()
            
            export_file = os.path.join(
                os.path.dirname(__file__), 
                'logs', 
                f'tuning_history_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            
            os.makedirs(os.path.dirname(export_file), exist_ok=True)
            with open(export_file, 'w', encoding='utf-8') as f:
                f.write(history_json)
            
            print(f"✅ 调优历史已导出: {export_file}")
            
        elif args.command == 'config':
            print("\n=== 配置文件路径 ===")
            print(f"配置文件: {tuner.config_path}")
            print("\n=== 当前配置 ===")
            print(json.dumps(tuner.config, ensure_ascii=False, indent=2))
    
    except KeyboardInterrupt:
        print("\n并发调优工具已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()