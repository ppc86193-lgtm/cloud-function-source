#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能分析模块
提供性能监控、内存分析、CPU分析、IO分析等功能
"""

import time
import psutil
import gc
import sys
import threading
import tracemalloc
import cProfile
import pstats
import io
import logging
from typing import Dict, List, Tuple, Optional, Any, Callable
from collections import defaultdict, deque
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import wraps

from models import PerformanceProfile, MemoryUsageMetrics, CPUUsageMetrics, IOMetrics

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self, sampling_interval: float = 0.1):
        self.sampling_interval = sampling_interval
        self.is_monitoring = False
        self.monitor_thread = None
        self.performance_data = deque(maxlen=1000)  # 保留最近1000个采样点
        self.start_time = None
        self.process = psutil.Process()
    
    def start_monitoring(self):
        """开始性能监控"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.start_time = time.time()
        self.performance_data.clear()
        
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info("性能监控已启动")
    
    def stop_monitoring(self) -> Dict[str, Any]:
        """停止性能监控并返回结果"""
        if not self.is_monitoring:
            return {}
        
        self.is_monitoring = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
        
        # 分析收集的数据
        analysis_result = self._analyze_performance_data()
        
        logger.info("性能监控已停止")
        return analysis_result
    
    def _monitor_loop(self):
        """监控循环"""
        while self.is_monitoring:
            try:
                # 收集性能数据
                timestamp = time.time()
                cpu_percent = self.process.cpu_percent()
                memory_info = self.process.memory_info()
                io_counters = self.process.io_counters() if hasattr(self.process, 'io_counters') else None
                
                data_point = {
                    'timestamp': timestamp,
                    'cpu_percent': cpu_percent,
                    'memory_rss': memory_info.rss / 1024 / 1024,  # MB
                    'memory_vms': memory_info.vms / 1024 / 1024,  # MB
                    'io_read_bytes': io_counters.read_bytes if io_counters else 0,
                    'io_write_bytes': io_counters.write_bytes if io_counters else 0,
                    'io_read_count': io_counters.read_count if io_counters else 0,
                    'io_write_count': io_counters.write_count if io_counters else 0
                }
                
                self.performance_data.append(data_point)
                
            except Exception as e:
                logger.error(f"监控数据收集出错: {e}")
            
            time.sleep(self.sampling_interval)
    
    def _analyze_performance_data(self) -> Dict[str, Any]:
        """分析性能数据"""
        if not self.performance_data:
            return {}
        
        data_list = list(self.performance_data)
        
        # CPU分析
        cpu_values = [d['cpu_percent'] for d in data_list]
        cpu_analysis = {
            'average': sum(cpu_values) / len(cpu_values),
            'peak': max(cpu_values),
            'min': min(cpu_values),
            'samples': len(cpu_values)
        }
        
        # 内存分析
        memory_rss_values = [d['memory_rss'] for d in data_list]
        memory_analysis = {
            'peak_rss': max(memory_rss_values),
            'average_rss': sum(memory_rss_values) / len(memory_rss_values),
            'min_rss': min(memory_rss_values)
        }
        
        # IO分析
        if data_list:
            first_io = data_list[0]
            last_io = data_list[-1]
            
            io_analysis = {
                'total_read_bytes': last_io['io_read_bytes'] - first_io['io_read_bytes'],
                'total_write_bytes': last_io['io_write_bytes'] - first_io['io_write_bytes'],
                'total_read_count': last_io['io_read_count'] - first_io['io_read_count'],
                'total_write_count': last_io['io_write_count'] - first_io['io_write_count']
            }
        else:
            io_analysis = {
                'total_read_bytes': 0,
                'total_write_bytes': 0,
                'total_read_count': 0,
                'total_write_count': 0
            }
        
        # 时间分析
        total_time = data_list[-1]['timestamp'] - data_list[0]['timestamp'] if len(data_list) > 1 else 0
        
        return {
            'total_time': total_time,
            'cpu_analysis': cpu_analysis,
            'memory_analysis': memory_analysis,
            'io_analysis': io_analysis,
            'sample_count': len(data_list)
        }

class MemoryProfiler:
    """内存分析器"""
    
    def __init__(self):
        self.tracemalloc_started = False
        self.baseline_snapshot = None
    
    def start_memory_tracing(self):
        """开始内存跟踪"""
        if not self.tracemalloc_started:
            tracemalloc.start()
            self.tracemalloc_started = True
            self.baseline_snapshot = tracemalloc.take_snapshot()
            logger.info("内存跟踪已启动")
    
    def stop_memory_tracing(self) -> Dict[str, Any]:
        """停止内存跟踪并分析结果"""
        if not self.tracemalloc_started:
            return {}
        
        current_snapshot = tracemalloc.take_snapshot()
        tracemalloc.stop()
        self.tracemalloc_started = False
        
        # 分析内存使用
        analysis = self._analyze_memory_snapshot(current_snapshot)
        
        # 比较基线
        if self.baseline_snapshot:
            comparison = self._compare_snapshots(self.baseline_snapshot, current_snapshot)
            analysis['comparison'] = comparison
        
        logger.info("内存跟踪已停止")
        return analysis
    
    def _analyze_memory_snapshot(self, snapshot) -> Dict[str, Any]:
        """分析内存快照"""
        top_stats = snapshot.statistics('lineno')
        
        analysis = {
            'total_memory': sum(stat.size for stat in top_stats),
            'total_blocks': sum(stat.count for stat in top_stats),
            'top_allocations': [],
            'memory_by_file': defaultdict(int),
            'memory_by_function': defaultdict(int)
        }
        
        # 分析前10个最大内存分配
        for i, stat in enumerate(top_stats[:10]):
            analysis['top_allocations'].append({
                'rank': i + 1,
                'size_mb': stat.size / 1024 / 1024,
                'count': stat.count,
                'filename': stat.traceback.format()[0] if stat.traceback else 'unknown',
                'line': stat.traceback[0].lineno if stat.traceback else 0
            })
        
        # 按文件统计内存使用
        for stat in top_stats:
            if stat.traceback:
                filename = stat.traceback[0].filename
                analysis['memory_by_file'][filename] += stat.size
        
        return analysis
    
    def _compare_snapshots(self, baseline, current) -> Dict[str, Any]:
        """比较两个内存快照"""
        top_stats = current.compare_to(baseline, 'lineno')
        
        comparison = {
            'memory_growth': [],
            'new_allocations': [],
            'total_growth': 0
        }
        
        for stat in top_stats[:10]:
            if stat.size_diff > 0:
                comparison['memory_growth'].append({
                    'size_diff_mb': stat.size_diff / 1024 / 1024,
                    'count_diff': stat.count_diff,
                    'filename': stat.traceback.format()[0] if stat.traceback else 'unknown'
                })
        
        comparison['total_growth'] = sum(stat.size_diff for stat in top_stats) / 1024 / 1024
        
        return comparison
    
    def analyze_memory_patterns(self, file_path: str) -> List[Dict[str, Any]]:
        """分析代码中的内存使用模式"""
        patterns = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                
                # 检查大列表创建
                if 'list(' in stripped and 'range(' in stripped:
                    patterns.append({
                        'type': 'large_list_creation',
                        'line': i,
                        'code': stripped,
                        'severity': 'medium',
                        'message': '可能创建大列表，考虑使用生成器'
                    })
                
                # 检查全局变量
                if stripped.startswith('global '):
                    patterns.append({
                        'type': 'global_variable',
                        'line': i,
                        'code': stripped,
                        'severity': 'low',
                        'message': '全局变量可能导致内存泄漏'
                    })
                
                # 检查循环中的列表追加
                if ('for ' in stripped or 'while ' in stripped) and '.append(' in stripped:
                    patterns.append({
                        'type': 'loop_append',
                        'line': i,
                        'code': stripped,
                        'severity': 'medium',
                        'message': '循环中频繁追加可能影响性能'
                    })
                
                # 检查字符串拼接
                if '+=' in stripped and ('str' in stripped or '"' in stripped or "'" in stripped):
                    patterns.append({
                        'type': 'string_concatenation',
                        'line': i,
                        'code': stripped,
                        'severity': 'medium',
                        'message': '字符串拼接效率较低，考虑使用join()'
                    })
        
        except Exception as e:
            logger.error(f"分析内存模式时出错 {file_path}: {e}")
        
        return patterns

class CPUProfiler:
    """CPU性能分析器"""
    
    def __init__(self):
        self.profiler = None
        self.profile_data = None
    
    @contextmanager
    def profile_context(self):
        """CPU性能分析上下文管理器"""
        self.start_profiling()
        try:
            yield
        finally:
            self.stop_profiling()
    
    def start_profiling(self):
        """开始CPU性能分析"""
        self.profiler = cProfile.Profile()
        self.profiler.enable()
        logger.info("CPU性能分析已启动")
    
    def stop_profiling(self) -> Dict[str, Any]:
        """停止CPU性能分析"""
        if not self.profiler:
            return {}
        
        self.profiler.disable()
        
        # 分析结果
        s = io.StringIO()
        ps = pstats.Stats(self.profiler, stream=s)
        ps.sort_stats('cumulative')
        ps.print_stats(20)  # 显示前20个函数
        
        profile_output = s.getvalue()
        
        # 解析性能数据
        analysis = self._parse_profile_data(ps)
        analysis['raw_output'] = profile_output
        
        logger.info("CPU性能分析已停止")
        return analysis
    
    def _parse_profile_data(self, stats: pstats.Stats) -> Dict[str, Any]:
        """解析性能分析数据"""
        analysis = {
            'total_calls': stats.total_calls,
            'total_time': stats.total_tt,
            'top_functions': [],
            'hotspots': [],
            'bottlenecks': []
        }
        
        # 获取统计数据
        stats_dict = stats.stats
        
        # 按累计时间排序的前10个函数
        sorted_functions = sorted(stats_dict.items(), key=lambda x: x[1][3], reverse=True)[:10]
        
        for (filename, lineno, function_name), (cc, nc, tt, ct) in sorted_functions:
            func_info = {
                'function': function_name,
                'filename': filename,
                'line': lineno,
                'call_count': cc,
                'total_time': tt,
                'cumulative_time': ct,
                'time_per_call': tt / cc if cc > 0 else 0,
                'percentage': (ct / analysis['total_time']) * 100 if analysis['total_time'] > 0 else 0
            }
            
            analysis['top_functions'].append(func_info)
            
            # 识别热点（占用时间超过5%的函数）
            if func_info['percentage'] > 5:
                analysis['hotspots'].append(func_info)
            
            # 识别瓶颈（调用次数多且耗时的函数）
            if cc > 100 and ct > 0.1:
                analysis['bottlenecks'].append(func_info)
        
        return analysis
    
    def analyze_cpu_intensive_operations(self, file_path: str) -> List[Dict[str, Any]]:
        """分析CPU密集型操作"""
        operations = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                
                # 检查嵌套循环
                if self._count_nested_loops(lines, i-1) > 2:
                    operations.append({
                        'type': 'nested_loops',
                        'line': i,
                        'code': stripped,
                        'severity': 'high',
                        'message': '深度嵌套循环可能导致性能问题'
                    })
                
                # 检查递归调用
                if 'def ' in stripped:
                    func_name = stripped.split('(')[0].replace('def ', '').strip()
                    if func_name in content[content.find(stripped):]:  # 简单的递归检测
                        operations.append({
                            'type': 'recursion',
                            'line': i,
                            'code': stripped,
                            'severity': 'medium',
                            'message': '递归调用需要注意栈溢出风险'
                        })
                
                # 检查复杂的正则表达式
                if 're.' in stripped and ('compile' in stripped or 'search' in stripped or 'match' in stripped):
                    operations.append({
                        'type': 'regex_operation',
                        'line': i,
                        'code': stripped,
                        'severity': 'medium',
                        'message': '复杂正则表达式可能影响性能'
                    })
                
                # 检查文件IO操作
                if any(op in stripped for op in ['open(', 'read()', 'write(', 'readlines()']):
                    operations.append({
                        'type': 'file_io',
                        'line': i,
                        'code': stripped,
                        'severity': 'low',
                        'message': 'IO操作可能成为性能瓶颈'
                    })
        
        except Exception as e:
            logger.error(f"分析CPU密集型操作时出错 {file_path}: {e}")
        
        return operations
    
    def _count_nested_loops(self, lines: List[str], start_line: int) -> int:
        """计算嵌套循环深度"""
        depth = 0
        max_depth = 0
        
        for i in range(start_line, min(start_line + 20, len(lines))):
            line = lines[i].strip()
            
            if line.startswith('for ') or line.startswith('while '):
                depth += 1
                max_depth = max(max_depth, depth)
            elif line and not line.startswith(' ') and depth > 0:
                depth = 0
        
        return max_depth

class IOProfiler:
    """IO性能分析器"""
    
    def __init__(self):
        self.io_operations = []
        self.start_time = None
    
    def start_io_monitoring(self):
        """开始IO监控"""
        self.io_operations.clear()
        self.start_time = time.time()
        logger.info("IO监控已启动")
    
    def stop_io_monitoring(self) -> Dict[str, Any]:
        """停止IO监控"""
        if not self.start_time:
            return {}
        
        total_time = time.time() - self.start_time
        
        analysis = {
            'total_time': total_time,
            'operation_count': len(self.io_operations),
            'operations': self.io_operations,
            'summary': self._analyze_io_operations()
        }
        
        logger.info("IO监控已停止")
        return analysis
    
    def record_io_operation(self, operation_type: str, file_path: str, size: int, duration: float):
        """记录IO操作"""
        operation = {
            'type': operation_type,
            'file_path': file_path,
            'size': size,
            'duration': duration,
            'timestamp': time.time(),
            'throughput': size / duration if duration > 0 else 0
        }
        
        self.io_operations.append(operation)
    
    def _analyze_io_operations(self) -> Dict[str, Any]:
        """分析IO操作"""
        if not self.io_operations:
            return {}
        
        total_size = sum(op['size'] for op in self.io_operations)
        total_duration = sum(op['duration'] for op in self.io_operations)
        
        read_ops = [op for op in self.io_operations if op['type'] == 'read']
        write_ops = [op for op in self.io_operations if op['type'] == 'write']
        
        analysis = {
            'total_size': total_size,
            'total_duration': total_duration,
            'average_throughput': total_size / total_duration if total_duration > 0 else 0,
            'read_operations': len(read_ops),
            'write_operations': len(write_ops),
            'read_size': sum(op['size'] for op in read_ops),
            'write_size': sum(op['size'] for op in write_ops),
            'slowest_operations': sorted(self.io_operations, key=lambda x: x['duration'], reverse=True)[:5]
        }
        
        return analysis

class PerformanceProfiler:
    """综合性能分析器"""
    
    def __init__(self):
        self.monitor = PerformanceMonitor()
        self.memory_profiler = MemoryProfiler()
        self.cpu_profiler = CPUProfiler()
        self.io_profiler = IOProfiler()
    
    @contextmanager
    def profile_execution(self, component_id: str):
        """性能分析上下文管理器"""
        # 开始所有监控
        self.monitor.start_monitoring()
        self.memory_profiler.start_memory_tracing()
        self.cpu_profiler.start_profiling()
        self.io_profiler.start_io_monitoring()
        
        start_time = time.time()
        
        try:
            yield
        finally:
            # 停止所有监控并收集结果
            execution_time = time.time() - start_time
            
            monitor_result = self.monitor.stop_monitoring()
            memory_result = self.memory_profiler.stop_memory_tracing()
            cpu_result = self.cpu_profiler.stop_profiling()
            io_result = self.io_profiler.stop_io_monitoring()
            
            # 生成性能报告
            self._generate_performance_report(
                component_id, execution_time, monitor_result, 
                memory_result, cpu_result, io_result
            )
    
    def _generate_performance_report(self, component_id: str, execution_time: float,
                                   monitor_result: Dict, memory_result: Dict,
                                   cpu_result: Dict, io_result: Dict) -> PerformanceProfile:
        """生成性能报告"""
        # 提取关键指标
        memory_peak = monitor_result.get('memory_analysis', {}).get('peak_rss', 0)
        memory_average = monitor_result.get('memory_analysis', {}).get('average_rss', 0)
        cpu_usage = monitor_result.get('cpu_analysis', {}).get('average', 0)
        io_operations = io_result.get('operation_count', 0)
        
        # 提取函数调用信息
        function_calls = {}
        if 'top_functions' in cpu_result:
            for func in cpu_result['top_functions']:
                function_calls[func['function']] = func['call_count']
        
        # 提取热点信息
        hotspots = []
        if 'hotspots' in cpu_result:
            for hotspot in cpu_result['hotspots']:
                hotspots.append((hotspot['function'], hotspot['percentage']))
        
        # 提取瓶颈信息
        bottlenecks = []
        if 'bottlenecks' in cpu_result:
            bottlenecks = [b['function'] for b in cpu_result['bottlenecks']]
        
        profile = PerformanceProfile(
            component_id=component_id,
            execution_time=execution_time,
            memory_peak=memory_peak,
            memory_average=memory_average,
            cpu_usage=cpu_usage,
            io_operations=io_operations,
            function_calls=function_calls,
            hotspots=hotspots,
            bottlenecks=bottlenecks
        )
        
        logger.info(f"性能分析完成: {component_id}, 执行时间: {execution_time:.2f}s")
        return profile
    
    def analyze_file_performance(self, file_path: str) -> PerformanceProfile:
        """分析文件性能特征"""
        # 获取静态分析结果
        memory_patterns = self.memory_profiler.analyze_memory_patterns(file_path)
        cpu_operations = self.cpu_profiler.analyze_cpu_intensive_operations(file_path)
        
        # 生成优化建议
        recommendations = []
        
        # 基于内存模式的建议
        for pattern in memory_patterns:
            if pattern['type'] == 'large_list_creation':
                recommendations.append({
                    'type': 'memory_optimization',
                    'priority': 'medium',
                    'description': '使用生成器替代大列表创建',
                    'line': pattern['line']
                })
        
        # 基于CPU操作的建议
        for operation in cpu_operations:
            if operation['type'] == 'nested_loops':
                recommendations.append({
                    'type': 'algorithm_optimization',
                    'priority': 'high',
                    'description': '优化嵌套循环算法复杂度',
                    'line': operation['line']
                })
        
        # 创建PerformanceProfile对象
        profile = PerformanceProfile(
            component_id=file_path,
            execution_time=0.0,  # 静态分析，无实际执行时间
            memory_peak=0.0,     # 静态分析，无实际内存使用
            memory_average=0.0,  # 静态分析，无实际内存使用
            cpu_usage=0.0,       # 静态分析，无实际CPU使用
            io_operations=0,     # 静态分析，无实际IO操作
            function_calls={},   # 静态分析，无实际函数调用统计
            hotspots=[],         # 静态分析，无实际热点
            bottlenecks=[]       # 静态分析，无实际瓶颈
        )
        
        return profile

def profile_function(func: Callable) -> Callable:
    """函数性能分析装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        profiler = PerformanceProfiler()
        
        with profiler.profile_execution(func.__name__):
            result = func(*args, **kwargs)
        
        return result
    
    return wrapper

def time_function(func: Callable) -> Callable:
    """函数执行时间测量装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        execution_time = end_time - start_time
        logger.info(f"函数 {func.__name__} 执行时间: {execution_time:.4f}秒")
        
        return result
    
    return wrapper