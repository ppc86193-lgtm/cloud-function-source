#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统性能优化和并发参数调整模块
实现动态性能调优、资源管理和并发参数自适应调整
"""

import asyncio
import json
import logging
import psutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """性能指标"""
    timestamp: str
    cpu_usage: float
    memory_usage: float
    disk_io_read: int
    disk_io_write: int
    network_io_sent: int
    network_io_recv: int
    thread_count: int
    process_count: int
    response_time: float
    throughput: float
    error_rate: float
    
@dataclass
class ConcurrencyConfig:
    """并发配置"""
    max_workers: int
    thread_pool_size: int
    process_pool_size: int
    connection_pool_size: int
    batch_size: int
    queue_size: int
    timeout: float
    retry_attempts: int
    
@dataclass
class OptimizationRule:
    """优化规则"""
    name: str
    condition: str  # 触发条件
    action: str     # 优化动作
    priority: int   # 优先级
    enabled: bool = True
    
@dataclass
class OptimizationResult:
    """优化结果"""
    rule_name: str
    action_taken: str
    before_metrics: Dict[str, Any]
    after_metrics: Dict[str, Any]
    improvement: float
    timestamp: str
    
class PerformanceOptimizer:
    """性能优化器"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._get_default_config()
        self.current_concurrency = self._get_initial_concurrency_config()
        self.metrics_history: List[PerformanceMetrics] = []
        self.optimization_rules = self._init_optimization_rules()
        self.optimization_history: List[OptimizationResult] = []
        self.is_optimizing = False
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        self.process_pool: Optional[ProcessPoolExecutor] = None
        self._lock = threading.Lock()
        
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'optimization_interval': 60,  # 优化检查间隔（秒）
            'metrics_window': 300,        # 指标窗口时间（秒）
            'min_improvement_threshold': 5.0,  # 最小改进阈值（%）
            'max_workers_limit': psutil.cpu_count() * 4,
            'memory_usage_threshold': 80.0,
            'cpu_usage_threshold': 70.0,
            'response_time_threshold': 1000.0,  # 毫秒
            'error_rate_threshold': 5.0,  # %
        }
        
    def _get_initial_concurrency_config(self) -> ConcurrencyConfig:
        """获取初始并发配置"""
        cpu_count = psutil.cpu_count()
        return ConcurrencyConfig(
            max_workers=cpu_count * 2,
            thread_pool_size=cpu_count * 4,
            process_pool_size=cpu_count,
            connection_pool_size=20,
            batch_size=100,
            queue_size=1000,
            timeout=30.0,
            retry_attempts=3
        )
        
    def _init_optimization_rules(self) -> List[OptimizationRule]:
        """初始化优化规则"""
        return [
            OptimizationRule('increase_workers_high_cpu', 'cpu_usage > 80 and response_time > 1000', 'increase_workers', 1),
            OptimizationRule('decrease_workers_low_cpu', 'cpu_usage < 30 and memory_usage > 70', 'decrease_workers', 2),
            OptimizationRule('increase_batch_size', 'throughput < 100 and cpu_usage < 50', 'increase_batch_size', 3),
            OptimizationRule('decrease_batch_size', 'memory_usage > 85', 'decrease_batch_size', 1),
            OptimizationRule('adjust_timeout', 'error_rate > 5', 'increase_timeout', 2),
            OptimizationRule('optimize_connection_pool', 'response_time > 2000', 'increase_connection_pool', 2),
        ]
        
    async def start_optimization(self):
        """开始性能优化"""
        if self.is_optimizing:
            logger.warning("性能优化已在运行中")
            return
            
        self.is_optimizing = True
        logger.info("开始性能优化...")
        
        # 初始化线程池和进程池
        self._initialize_pools()
        
        try:
            while self.is_optimizing:
                # 收集性能指标
                metrics = await self._collect_performance_metrics()
                self.metrics_history.append(metrics)
                
                # 执行优化
                await self._execute_optimizations(metrics)
                
                # 清理历史数据
                self._cleanup_metrics_history()
                
                await asyncio.sleep(self.config['optimization_interval'])
                
        except Exception as e:
            logger.error(f"性能优化过程中发生错误: {e}")
        finally:
            self._cleanup_pools()
            self.is_optimizing = False
            
    async def stop_optimization(self):
        """停止性能优化"""
        self.is_optimizing = False
        logger.info("停止性能优化")
        
    def _initialize_pools(self):
        """初始化线程池和进程池"""
        with self._lock:
            if self.thread_pool:
                self.thread_pool.shutdown(wait=False)
            if self.process_pool:
                self.process_pool.shutdown(wait=False)
                
            self.thread_pool = ThreadPoolExecutor(
                max_workers=self.current_concurrency.thread_pool_size
            )
            self.process_pool = ProcessPoolExecutor(
                max_workers=self.current_concurrency.process_pool_size
            )
            
        logger.info(f"线程池初始化: {self.current_concurrency.thread_pool_size} 线程")
        logger.info(f"进程池初始化: {self.current_concurrency.process_pool_size} 进程")
        
    def _cleanup_pools(self):
        """清理线程池和进程池"""
        with self._lock:
            if self.thread_pool:
                self.thread_pool.shutdown(wait=True)
                self.thread_pool = None
            if self.process_pool:
                self.process_pool.shutdown(wait=True)
                self.process_pool = None
                
    async def _collect_performance_metrics(self) -> PerformanceMetrics:
        """收集性能指标"""
        # 系统指标
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk_io = psutil.disk_io_counters()
        network_io = psutil.net_io_counters()
        
        # 模拟应用指标
        response_time = self._simulate_response_time()
        throughput = self._simulate_throughput()
        error_rate = self._simulate_error_rate()
        
        return PerformanceMetrics(
            timestamp=datetime.now().isoformat(),
            cpu_usage=cpu_usage,
            memory_usage=memory.percent,
            disk_io_read=disk_io.read_bytes if disk_io else 0,
            disk_io_write=disk_io.write_bytes if disk_io else 0,
            network_io_sent=network_io.bytes_sent if network_io else 0,
            network_io_recv=network_io.bytes_recv if network_io else 0,
            thread_count=threading.active_count(),
            process_count=len(psutil.pids()),
            response_time=response_time,
            throughput=throughput,
            error_rate=error_rate
        )
        
    def _simulate_response_time(self) -> float:
        """模拟响应时间（毫秒）"""
        import random
        base_time = 500
        # 根据当前负载调整响应时间
        cpu_factor = psutil.cpu_percent() / 100
        memory_factor = psutil.virtual_memory().percent / 100
        return base_time * (1 + cpu_factor * 0.5 + memory_factor * 0.3) + random.uniform(-50, 100)
        
    def _simulate_throughput(self) -> float:
        """模拟吞吐量（请求/秒）"""
        import random
        base_throughput = 200
        # 根据当前配置调整吞吐量
        worker_factor = self.current_concurrency.max_workers / 10
        batch_factor = self.current_concurrency.batch_size / 100
        return base_throughput * worker_factor * batch_factor + random.uniform(-20, 50)
        
    def _simulate_error_rate(self) -> float:
        """模拟错误率（%）"""
        import random
        base_error_rate = 2.0
        # 高负载时错误率增加
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        if cpu_usage > 80 or memory_usage > 85:
            base_error_rate *= 2
        return max(0, base_error_rate + random.uniform(-1, 2))
        
    async def _execute_optimizations(self, current_metrics: PerformanceMetrics):
        """执行优化"""
        # 按优先级排序规则
        sorted_rules = sorted(self.optimization_rules, key=lambda r: r.priority)
        
        for rule in sorted_rules:
            if not rule.enabled:
                continue
                
            if self._evaluate_rule_condition(rule, current_metrics):
                logger.info(f"触发优化规则: {rule.name}")
                
                # 记录优化前的指标
                before_metrics = asdict(current_metrics)
                
                # 执行优化动作
                success = await self._execute_optimization_action(rule.action)
                
                if success:
                    # 等待一段时间让优化生效
                    await asyncio.sleep(10)
                    
                    # 收集优化后的指标
                    after_metrics_obj = await self._collect_performance_metrics()
                    after_metrics = asdict(after_metrics_obj)
                    
                    # 计算改进程度
                    improvement = self._calculate_improvement(before_metrics, after_metrics)
                    
                    # 记录优化结果
                    result = OptimizationResult(
                        rule_name=rule.name,
                        action_taken=rule.action,
                        before_metrics=before_metrics,
                        after_metrics=after_metrics,
                        improvement=improvement,
                        timestamp=datetime.now().isoformat()
                    )
                    
                    self.optimization_history.append(result)
                    
                    logger.info(f"优化完成: {rule.action}, 改进: {improvement:.1f}%")
                    
                    # 如果改进不明显，回滚优化
                    if improvement < self.config['min_improvement_threshold']:
                        logger.warning(f"优化效果不佳，考虑回滚: {rule.action}")
                        await self._rollback_optimization(rule.action)
                        
                break  # 一次只执行一个优化规则
                
    def _evaluate_rule_condition(self, rule: OptimizationRule, metrics: PerformanceMetrics) -> bool:
        """评估规则条件"""
        try:
            # 构建评估上下文
            context = {
                'cpu_usage': metrics.cpu_usage,
                'memory_usage': metrics.memory_usage,
                'response_time': metrics.response_time,
                'throughput': metrics.throughput,
                'error_rate': metrics.error_rate,
                'thread_count': metrics.thread_count
            }
            
            # 评估条件表达式
            return eval(rule.condition, {"__builtins__": {}}, context)
            
        except Exception as e:
            logger.error(f"规则条件评估失败 {rule.name}: {e}")
            return False
            
    async def _execute_optimization_action(self, action: str) -> bool:
        """执行优化动作"""
        try:
            if action == 'increase_workers':
                new_workers = min(
                    self.current_concurrency.max_workers + 2,
                    self.config['max_workers_limit']
                )
                self.current_concurrency.max_workers = new_workers
                self.current_concurrency.thread_pool_size = new_workers * 2
                self._initialize_pools()
                
            elif action == 'decrease_workers':
                new_workers = max(self.current_concurrency.max_workers - 2, 2)
                self.current_concurrency.max_workers = new_workers
                self.current_concurrency.thread_pool_size = new_workers * 2
                self._initialize_pools()
                
            elif action == 'increase_batch_size':
                self.current_concurrency.batch_size = min(
                    self.current_concurrency.batch_size + 50, 500
                )
                
            elif action == 'decrease_batch_size':
                self.current_concurrency.batch_size = max(
                    self.current_concurrency.batch_size - 50, 10
                )
                
            elif action == 'increase_timeout':
                self.current_concurrency.timeout = min(
                    self.current_concurrency.timeout + 10, 120
                )
                
            elif action == 'increase_connection_pool':
                self.current_concurrency.connection_pool_size = min(
                    self.current_concurrency.connection_pool_size + 5, 100
                )
                
            else:
                logger.warning(f"未知的优化动作: {action}")
                return False
                
            logger.info(f"执行优化动作: {action}")
            return True
            
        except Exception as e:
            logger.error(f"执行优化动作失败 {action}: {e}")
            return False
            
    async def _rollback_optimization(self, action: str):
        """回滚优化"""
        try:
            # 实现回滚逻辑（这里简化处理）
            if 'increase' in action:
                rollback_action = action.replace('increase', 'decrease')
            elif 'decrease' in action:
                rollback_action = action.replace('decrease', 'increase')
            else:
                return
                
            await self._execute_optimization_action(rollback_action)
            logger.info(f"回滚优化: {action} -> {rollback_action}")
            
        except Exception as e:
            logger.error(f"回滚优化失败 {action}: {e}")
            
    def _calculate_improvement(self, before: Dict, after: Dict) -> float:
        """计算改进程度"""
        try:
            # 综合多个指标计算改进程度
            response_time_improvement = (
                (before['response_time'] - after['response_time']) / before['response_time'] * 100
            )
            throughput_improvement = (
                (after['throughput'] - before['throughput']) / before['throughput'] * 100
            )
            error_rate_improvement = (
                (before['error_rate'] - after['error_rate']) / max(before['error_rate'], 0.1) * 100
            )
            
            # 加权平均
            total_improvement = (
                response_time_improvement * 0.4 +
                throughput_improvement * 0.4 +
                error_rate_improvement * 0.2
            )
            
            return total_improvement
            
        except Exception as e:
            logger.error(f"计算改进程度失败: {e}")
            return 0.0
            
    def _cleanup_metrics_history(self):
        """清理指标历史"""
        cutoff_time = datetime.now() - timedelta(seconds=self.config['metrics_window'])
        cutoff_str = cutoff_time.isoformat()
        
        self.metrics_history = [
            m for m in self.metrics_history 
            if m.timestamp > cutoff_str
        ]
        
    def get_current_config(self) -> Dict[str, Any]:
        """获取当前配置"""
        return {
            'concurrency_config': asdict(self.current_concurrency),
            'optimization_status': 'running' if self.is_optimizing else 'stopped',
            'metrics_count': len(self.metrics_history),
            'optimization_count': len(self.optimization_history),
            'last_update': datetime.now().isoformat()
        }
        
    def get_optimization_report(self) -> Dict[str, Any]:
        """获取优化报告"""
        if not self.optimization_history:
            return {'message': '暂无优化历史'}
            
        recent_optimizations = self.optimization_history[-10:]  # 最近10次优化
        
        total_improvement = sum(opt.improvement for opt in recent_optimizations)
        avg_improvement = total_improvement / len(recent_optimizations)
        
        successful_optimizations = [opt for opt in recent_optimizations if opt.improvement > 0]
        success_rate = len(successful_optimizations) / len(recent_optimizations) * 100
        
        return {
            'period': f'最近 {len(recent_optimizations)} 次优化',
            'total_optimizations': len(recent_optimizations),
            'successful_optimizations': len(successful_optimizations),
            'success_rate': round(success_rate, 2),
            'average_improvement': round(avg_improvement, 2),
            'best_optimization': max(recent_optimizations, key=lambda x: x.improvement).rule_name,
            'current_config': asdict(self.current_concurrency)
        }
        
    def export_optimization_report(self, filepath: Optional[str] = None) -> str:
        """导出优化报告"""
        if not filepath:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = f"optimization_report_{timestamp}.json"
            
        report = {
            'summary': self.get_optimization_report(),
            'current_config': self.get_current_config(),
            'optimization_history': [asdict(opt) for opt in self.optimization_history[-20:]],
            'metrics_history': [asdict(m) for m in self.metrics_history[-50:]]
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
            
        logger.info(f"优化报告已导出: {filepath}")
        return filepath
        
    async def execute_task_with_optimization(self, task_func: Callable, *args, **kwargs):
        """使用优化后的配置执行任务"""
        if not self.thread_pool:
            self._initialize_pools()
            
        try:
            # 使用线程池执行任务
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.thread_pool, task_func, *args, **kwargs
            )
            return result
            
        except Exception as e:
            logger.error(f"任务执行失败: {e}")
            raise
            
async def main():
    """主函数 - 性能优化测试"""
    print("=== PC28性能优化测试 ===")
    
    # 创建性能优化器
    optimizer = PerformanceOptimizer()
    
    try:
        # 启动优化（运行30秒用于测试）
        print("启动性能优化...")
        optimization_task = asyncio.create_task(optimizer.start_optimization())
        
        # 等待一段时间让优化运行
        await asyncio.sleep(15)
        
        # 获取当前配置
        print("\n=== 当前配置 ===")
        config = optimizer.get_current_config()
        concurrency = config['concurrency_config']
        print(f"最大工作线程: {concurrency['max_workers']}")
        print(f"线程池大小: {concurrency['thread_pool_size']}")
        print(f"进程池大小: {concurrency['process_pool_size']}")
        print(f"批处理大小: {concurrency['batch_size']}")
        print(f"连接池大小: {concurrency['connection_pool_size']}")
        print(f"超时时间: {concurrency['timeout']}秒")
        
        # 获取优化报告
        print("\n=== 优化报告 ===")
        report = optimizer.get_optimization_report()
        if 'message' not in report:
            print(f"优化次数: {report['total_optimizations']}")
            print(f"成功次数: {report['successful_optimizations']}")
            print(f"成功率: {report['success_rate']}%")
            print(f"平均改进: {report['average_improvement']}%")
            print(f"最佳优化: {report['best_optimization']}")
        else:
            print(report['message'])
            
        # 模拟任务执行
        print("\n=== 任务执行测试 ===")
        def sample_task(task_id):
            time.sleep(0.1)  # 模拟任务处理时间
            return f"任务 {task_id} 完成"
            
        start_time = time.time()
        tasks = []
        for i in range(10):
            task = optimizer.execute_task_with_optimization(sample_task, i)
            tasks.append(task)
            
        results = await asyncio.gather(*tasks)
        execution_time = time.time() - start_time
        
        print(f"执行 {len(results)} 个任务耗时: {execution_time:.2f}秒")
        print(f"平均每个任务: {execution_time/len(results)*1000:.1f}毫秒")
        
        # 导出优化报告
        report_file = optimizer.export_optimization_report()
        print(f"\n优化报告已导出: {report_file}")
        
        # 停止优化
        await optimizer.stop_optimization()
        optimization_task.cancel()
        
    except Exception as e:
        logger.error(f"性能优化测试失败: {e}")
        await optimizer.stop_optimization()
        
    print("\n=== 性能优化测试完成 ===")

if __name__ == "__main__":
    asyncio.run(main())