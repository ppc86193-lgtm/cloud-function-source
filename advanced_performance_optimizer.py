#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强的Python性能优化系统
基于现有component_updater.py，提供深度性能分析、智能优化建议、风险评估和自动化优化功能
"""

import ast
import sqlite3
import json
import logging
import threading
import time
import os
import hashlib
import subprocess
import shutil
import sys
import re
import traceback
import psutil
import gc
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any, Callable, Tuple, Set
from pathlib import Path
import importlib
from collections import defaultdict, Counter
from functools import wraps
import cProfile
import pstats
import io
from contextlib import contextmanager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ComplexityMetrics:
    """代码复杂度指标"""
    component_id: str
    cyclomatic_complexity: int
    cognitive_complexity: int
    nesting_depth: int
    function_count: int
    class_count: int
    line_count: int
    comment_ratio: float
    duplicate_lines: int

@dataclass
class PerformanceProfile:
    """性能分析结果"""
    component_id: str
    execution_time: float
    memory_peak: float
    memory_average: float
    cpu_usage: float
    io_operations: int
    function_calls: Dict[str, int]
    hotspots: List[Tuple[str, float]]  # (function_name, time_percentage)
    bottlenecks: List[str]

@dataclass
class OptimizationSuggestion:
    """优化建议"""
    suggestion_id: str
    component_id: str
    category: str  # 'data_structure', 'algorithm', 'memory', 'io', 'concurrency'
    priority: str  # 'critical', 'high', 'medium', 'low'
    description: str
    code_location: Tuple[int, int]  # (start_line, end_line)
    original_code: str
    suggested_code: str
    estimated_improvement: float  # 预期性能提升百分比
    risk_level: str  # 'safe', 'moderate', 'risky'
    auto_applicable: bool
    reasoning: str
    type: str
    impact: str  # low, medium, high
    effort: str  # low, medium, high
    code_example: str
    risk_assessment: 'RiskAssessment' = None

@dataclass
class RiskAssessment:
    """风险评估"""
    risk_id: str
    component_id: str
    risk_level: str  # 'low', 'medium', 'high', 'critical'
    risk_factors: List[str]
    impact_analysis: str
    mitigation_strategies: List[str]
    requires_manual_review: bool
    backup_required: bool
    rollback_plan: str

class CodeAnalyzer:
    """代码分析器"""
    
    def __init__(self):
        self.ast_cache = {}
    
    def analyze_complexity(self, file_path: str) -> ComplexityMetrics:
        """分析代码复杂度"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # 计算各种复杂度指标
            cyclomatic = self._calculate_cyclomatic_complexity(tree)
            cognitive = self._calculate_cognitive_complexity(tree)
            nesting = self._calculate_max_nesting_depth(tree)
            
            # 统计函数和类数量
            function_count = len([node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)])
            class_count = len([node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)])
            
            lines = content.split('\n')
            line_count = len(lines)
            
            # 计算注释比例
            comment_lines = len([line for line in lines if line.strip().startswith('#')])
            comment_ratio = comment_lines / line_count if line_count > 0 else 0
            
            # 检测重复代码
            duplicate_lines = self._detect_duplicate_lines(lines)
            
            return ComplexityMetrics(
                component_id=file_path,
                cyclomatic_complexity=cyclomatic,
                cognitive_complexity=cognitive,
                nesting_depth=nesting,
                function_count=function_count,
                class_count=class_count,
                line_count=line_count,
                comment_ratio=comment_ratio,
                duplicate_lines=duplicate_lines
            )
        
        except Exception as e:
            logger.error(f"分析复杂度时出错 {file_path}: {e}")
            return ComplexityMetrics(file_path, 0, 0, 0, 0, 0, 0, 0.0, 0)
    
    def _calculate_cyclomatic_complexity(self, tree: ast.AST) -> int:
        """计算圈复杂度"""
        complexity = 1  # 基础复杂度
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.If, ast.While, ast.For, ast.AsyncFor)):
                complexity += 1
            elif isinstance(node, ast.ExceptHandler):
                complexity += 1
            elif isinstance(node, ast.BoolOp):
                complexity += len(node.values) - 1
        
        return complexity
    
    def _calculate_cognitive_complexity(self, tree: ast.AST) -> int:
        """计算认知复杂度"""
        complexity = 0
        nesting_level = 0
        
        def visit_node(node, level=0):
            nonlocal complexity
            
            if isinstance(node, (ast.If, ast.While, ast.For, ast.AsyncFor)):
                complexity += 1 + level
                level += 1
            elif isinstance(node, ast.ExceptHandler):
                complexity += 1 + level
            elif isinstance(node, ast.BoolOp):
                complexity += len(node.values) - 1
            
            for child in ast.iter_child_nodes(node):
                visit_node(child, level)
        
        visit_node(tree)
        return complexity
    
    def _calculate_max_nesting_depth(self, tree: ast.AST) -> int:
        """计算最大嵌套深度"""
        max_depth = 0
        
        def visit_node(node, depth=0):
            nonlocal max_depth
            max_depth = max(max_depth, depth)
            
            if isinstance(node, (ast.If, ast.While, ast.For, ast.AsyncFor, ast.With, ast.AsyncWith, ast.Try)):
                depth += 1
            
            for child in ast.iter_child_nodes(node):
                visit_node(child, depth)
        
        visit_node(tree)
        return max_depth
    
    def _detect_duplicate_lines(self, lines: List[str]) -> int:
        """检测重复代码行"""
        line_counts = Counter()
        
        for line in lines:
            stripped = line.strip()
            if len(stripped) > 10 and not stripped.startswith('#'):  # 忽略短行和注释
                line_counts[stripped] += 1
        
        return sum(count - 1 for count in line_counts.values() if count > 1)

class PerformanceProfiler:
    """性能分析器"""
    
    def __init__(self):
        self.active_profiles = {}
        self.memory_tracker = MemoryTracker()
        self.cpu_analyzer = CPUAnalyzer()
        self.io_monitor = IOMonitor()
    
    @contextmanager
    def profile_execution(self, component_id: str):
        """性能分析上下文管理器"""
        profiler = cProfile.Profile()
        process = psutil.Process()
        
        # 记录开始状态
        start_time = time.time()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_cpu = process.cpu_percent()
        
        # 启动详细监控
        self.memory_tracker.start_tracking()
        self.cpu_analyzer.start_monitoring()
        self.io_monitor.start_monitoring()
        
        profiler.enable()
        
        try:
            yield profiler
        finally:
            profiler.disable()
            
            # 停止监控
            memory_stats = self.memory_tracker.stop_tracking()
            cpu_stats = self.cpu_analyzer.stop_monitoring()
            io_stats = self.io_monitor.stop_monitoring()
            
            # 记录结束状态
            end_time = time.time()
            end_memory = process.memory_info().rss / 1024 / 1024  # MB
            end_cpu = process.cpu_percent()
            
            # 分析性能数据
            profile_data = self._analyze_profile_data(profiler)
            
            # 创建详细性能报告
            performance_profile = PerformanceProfile(
                component_id=component_id,
                execution_time=end_time - start_time,
                memory_peak=memory_stats.get('peak_usage', end_memory),
                memory_average=memory_stats.get('average_usage', (start_memory + end_memory) / 2),
                cpu_usage=cpu_stats.get('average_usage', (start_cpu + end_cpu) / 2),
                io_operations=io_stats.get('total_operations', 0),
                function_calls=profile_data['function_calls'],
                hotspots=profile_data['hotspots'],
                bottlenecks=profile_data['bottlenecks']
            )
            
            self.active_profiles[component_id] = performance_profile
    
    def _analyze_profile_data(self, profiler: cProfile.Profile) -> Dict[str, Any]:
        """分析性能数据"""
        s = io.StringIO()
        stats = pstats.Stats(profiler, stream=s)
        stats.sort_stats('cumulative')
        
        # 获取函数调用统计
        function_calls = {}
        hotspots = []
        bottlenecks = []
        
        for func, (cc, nc, tt, ct, callers) in stats.stats.items():
            func_name = f"{func[0]}:{func[1]}({func[2]})"
            function_calls[func_name] = nc
            
            # 识别热点函数（执行时间占比高）
            if ct > 0.01:  # 超过10ms的函数
                hotspots.append((func_name, ct))
            
            # 识别瓶颈（调用次数多且耗时）
            if nc > 100 and ct > 0.005:
                bottlenecks.append(func_name)
        
        # 按时间排序热点
        hotspots.sort(key=lambda x: x[1], reverse=True)
        hotspots = hotspots[:10]  # 取前10个
        
        return {
            'function_calls': function_calls,
            'hotspots': hotspots,
            'bottlenecks': bottlenecks
        }
    
    def analyze_memory_patterns(self, file_path: str) -> Dict[str, Any]:
        """分析内存使用模式"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            memory_issues = []
            
            # 检查内存密集型操作
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    # 检查大列表创建
                    if (isinstance(node.func, ast.Name) and node.func.id == 'list' and
                        len(node.args) > 0):
                        memory_issues.append({
                            'type': 'large_list_creation',
                            'line': node.lineno,
                            'severity': 'medium',
                            'description': '可能创建大列表，考虑使用生成器'
                        })
                    
                    # 检查字符串拼接
                    elif (isinstance(node.func, ast.Attribute) and 
                          isinstance(node.func.value, ast.Name) and
                          node.func.attr == 'join'):
                        memory_issues.append({
                            'type': 'string_concatenation',
                            'line': node.lineno,
                            'severity': 'low',
                            'description': '使用join()进行字符串拼接，性能良好'
                        })
                
                # 检查全局变量
                elif isinstance(node, ast.Global):
                    memory_issues.append({
                        'type': 'global_variable',
                        'line': node.lineno,
                        'severity': 'medium',
                        'description': '全局变量可能导致内存长期占用'
                    })
            
            return {
                'memory_issues': memory_issues,
                'total_issues': len(memory_issues),
                'high_severity_count': len([i for i in memory_issues if i['severity'] == 'high']),
                'medium_severity_count': len([i for i in memory_issues if i['severity'] == 'medium']),
                'low_severity_count': len([i for i in memory_issues if i['severity'] == 'low'])
            }
        
        except Exception as e:
            logger.error(f"内存模式分析失败 {file_path}: {e}")
            return {'memory_issues': [], 'total_issues': 0}
    
    def analyze_cpu_intensive_operations(self, file_path: str) -> Dict[str, Any]:
        """分析CPU密集型操作"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            cpu_issues = []
            
            # 检查嵌套循环
            for node in ast.walk(tree):
                if isinstance(node, (ast.For, ast.While)):
                    nested_count = len([n for n in ast.walk(node) 
                                      if isinstance(n, (ast.For, ast.While)) and n != node])
                    
                    if nested_count >= 2:
                        cpu_issues.append({
                            'type': 'nested_loops',
                            'line': node.lineno,
                            'severity': 'high',
                            'nesting_level': nested_count,
                            'description': f'{nested_count}层嵌套循环，时间复杂度可能很高'
                        })
                    elif nested_count == 1:
                        cpu_issues.append({
                            'type': 'double_loop',
                            'line': node.lineno,
                            'severity': 'medium',
                            'nesting_level': nested_count,
                            'description': '双重循环，注意时间复杂度'
                        })
                
                # 检查递归函数
                elif isinstance(node, ast.FunctionDef):
                    for child in ast.walk(node):
                        if (isinstance(child, ast.Call) and 
                            isinstance(child.func, ast.Name) and 
                            child.func.id == node.name):
                            cpu_issues.append({
                                'type': 'recursion',
                                'line': node.lineno,
                                'severity': 'medium',
                                'description': f'递归函数 {node.name}，注意栈溢出风险'
                            })
                            break
            
            return {
                'cpu_issues': cpu_issues,
                'total_issues': len(cpu_issues),
                'complexity_score': sum(issue.get('nesting_level', 1) for issue in cpu_issues)
            }
        
        except Exception as e:
            logger.error(f"CPU分析失败 {file_path}: {e}")
            return {'cpu_issues': [], 'total_issues': 0}

class MemoryTracker:
    """内存跟踪器"""
    
    def __init__(self):
        self.tracking = False
        self.memory_samples = []
        self.start_memory = 0
    
    def start_tracking(self):
        """开始内存跟踪"""
        self.tracking = True
        self.memory_samples = []
        process = psutil.Process()
        self.start_memory = process.memory_info().rss / 1024 / 1024
        self.memory_samples.append(self.start_memory)
    
    def stop_tracking(self) -> Dict[str, float]:
        """停止内存跟踪并返回统计信息"""
        self.tracking = False
        
        if not self.memory_samples:
            return {'peak_usage': 0, 'average_usage': 0, 'growth': 0}
        
        process = psutil.Process()
        end_memory = process.memory_info().rss / 1024 / 1024
        self.memory_samples.append(end_memory)
        
        return {
            'peak_usage': max(self.memory_samples),
            'average_usage': sum(self.memory_samples) / len(self.memory_samples),
            'growth': end_memory - self.start_memory,
            'samples_count': len(self.memory_samples)
        }

class CPUAnalyzer:
    """CPU分析器"""
    
    def __init__(self):
        self.monitoring = False
        self.cpu_samples = []
    
    def start_monitoring(self):
        """开始CPU监控"""
        self.monitoring = True
        self.cpu_samples = []
        # 初始CPU读数
        psutil.cpu_percent(interval=None)
    
    def stop_monitoring(self) -> Dict[str, float]:
        """停止CPU监控并返回统计信息"""
        self.monitoring = False
        
        # 获取最终CPU使用率
        final_cpu = psutil.cpu_percent(interval=0.1)
        self.cpu_samples.append(final_cpu)
        
        if not self.cpu_samples:
            return {'average_usage': 0, 'peak_usage': 0}
        
        return {
            'average_usage': sum(self.cpu_samples) / len(self.cpu_samples),
            'peak_usage': max(self.cpu_samples),
            'samples_count': len(self.cpu_samples)
        }

class IOMonitor:
    """I/O监控器"""
    
    def __init__(self):
        self.monitoring = False
        self.io_operations = 0
        self.start_io_counters = None
    
    def start_monitoring(self):
        """开始I/O监控"""
        self.monitoring = True
        self.io_operations = 0
        try:
            process = psutil.Process()
            self.start_io_counters = process.io_counters()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            self.start_io_counters = None
    
    def stop_monitoring(self) -> Dict[str, int]:
        """停止I/O监控并返回统计信息"""
        self.monitoring = False
        
        if self.start_io_counters is None:
            return {'total_operations': 0, 'read_operations': 0, 'write_operations': 0}
        
        try:
            process = psutil.Process()
            end_io_counters = process.io_counters()
            
            read_ops = end_io_counters.read_count - self.start_io_counters.read_count
            write_ops = end_io_counters.write_count - self.start_io_counters.write_count
            
            return {
                'total_operations': read_ops + write_ops,
                'read_operations': read_ops,
                'write_operations': write_ops,
                'read_bytes': end_io_counters.read_bytes - self.start_io_counters.read_bytes,
                'write_bytes': end_io_counters.write_bytes - self.start_io_counters.write_bytes
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return {'total_operations': 0, 'read_operations': 0, 'write_operations': 0}

class RiskAssessmentEngine:
    """风险评估引擎"""
    
    def __init__(self):
        self.risk_rules = self._load_risk_rules()
    
    def assess_optimization_risk(self, file_path: str, optimization_type: str, 
                               complexity_metrics: ComplexityMetrics) -> RiskAssessment:
        """评估优化风险"""
        risk_factors = []
        risk_level = "low"
        
        # 文件大小风险
        try:
            file_size = os.path.getsize(file_path) / 1024  # KB
            if file_size > 100:
                risk_factors.append("大文件修改风险")
                risk_level = "medium"
        except OSError:
            pass
        
        # 复杂度风险
        if complexity_metrics.cyclomatic_complexity > 15:
            risk_factors.append("高复杂度代码修改风险")
            risk_level = "high"
        elif complexity_metrics.cyclomatic_complexity > 10:
            risk_factors.append("中等复杂度代码修改风险")
            if risk_level == "low":
                risk_level = "medium"
        
        # 优化类型风险
        high_risk_optimizations = [
            "algorithm_change", "data_structure_change", 
            "concurrency_optimization", "memory_management"
        ]
        
        if optimization_type in high_risk_optimizations:
            risk_factors.append(f"高风险优化类型: {optimization_type}")
            risk_level = "high"
        
        # 函数数量风险
        if complexity_metrics.function_count > 20:
            risk_factors.append("大量函数可能受影响")
            if risk_level == "low":
                risk_level = "medium"
        
        return RiskAssessment(
            risk_id=f"risk_{hash(file_path + optimization_type)}",
            component_id=file_path,
            risk_level=risk_level,
            risk_factors=risk_factors,
            impact_analysis=f"优化类型 {optimization_type} 对文件 {file_path} 的影响评估",
            mitigation_strategies=self._generate_mitigation_strategies(risk_level),
            requires_manual_review=risk_level in ["medium", "high"],
            backup_required=risk_level == "high",
            rollback_plan=self._generate_rollback_plan(file_path, risk_level)
        )
    
    def _load_risk_rules(self) -> Dict[str, Any]:
        """加载风险评估规则"""
        return {
            "file_size_threshold": 100,  # KB
            "complexity_threshold": 10,
            "high_risk_operations": [
                "algorithm_change", "data_structure_change",
                "concurrency_optimization", "memory_management"
            ]
        }
    
    def _generate_rollback_plan(self, file_path: str, risk_level: str) -> str:
        """生成回滚计划"""
        if risk_level == "high":
            return f"1. 创建 {file_path} 的完整备份\n2. 记录所有修改细节\n3. 准备自动回滚脚本\n4. 设置性能监控检查点"
        elif risk_level == "medium":
            return f"1. 创建 {file_path} 的备份\n2. 记录主要修改\n3. 准备手动回滚步骤"
        else:
            return "低风险操作，标准版本控制即可"
    
    def _generate_mitigation_strategies(self, risk_level: str) -> List[str]:
        """生成缓解策略"""
        if risk_level == "high":
            return ["创建完整备份", "分阶段实施", "增加测试覆盖", "性能监控"]
        elif risk_level == "medium":
            return ["创建备份", "代码审查", "基本测试"]
        else:
            return ["标准测试"]

class BackupManager:
    """备份管理器"""
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = backup_dir
        os.makedirs(backup_dir, exist_ok=True)
    
    def create_backup(self, file_path: str) -> str:
        """创建文件备份"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.basename(file_path)
            backup_filename = f"{filename}.backup_{timestamp}"
            backup_path = os.path.join(self.backup_dir, backup_filename)
            
            shutil.copy2(file_path, backup_path)
            logger.info(f"备份创建成功: {backup_path}")
            
            return backup_path
        
        except Exception as e:
            logger.error(f"备份创建失败 {file_path}: {e}")
            raise
    
    def restore_backup(self, original_path: str, backup_path: str) -> bool:
        """从备份恢复文件"""
        try:
            if not os.path.exists(backup_path):
                logger.error(f"备份文件不存在: {backup_path}")
                return False
            
            shutil.copy2(backup_path, original_path)
            logger.info(f"文件恢复成功: {original_path}")
            return True
        
        except Exception as e:
            logger.error(f"文件恢复失败: {e}")
            return False
    
    def list_backups(self, file_pattern: str = None) -> List[str]:
        """列出备份文件"""
        try:
            backups = []
            for filename in os.listdir(self.backup_dir):
                if filename.endswith('.backup_'):
                    continue
                if '.backup_' in filename:
                    if file_pattern is None or file_pattern in filename:
                        backups.append(os.path.join(self.backup_dir, filename))
            
            return sorted(backups, key=os.path.getmtime, reverse=True)
        
        except Exception as e:
            logger.error(f"列出备份失败: {e}")
            return []

class OptimizationEngine:
    """优化引擎"""
    
    def __init__(self):
        self.analyzers = {
            'data_structure': self._analyze_data_structures,
            'algorithm': self._analyze_algorithms,
            'memory': self._analyze_memory_usage,
            'io': self._analyze_io_operations,
            'concurrency': self._analyze_concurrency
        }
        self.risk_engine = RiskAssessmentEngine()
        self.backup_manager = BackupManager()
    
    def generate_suggestions(self, file_path: str, complexity_metrics: ComplexityMetrics, 
                           performance_profile: PerformanceProfile) -> List[OptimizationSuggestion]:
        """生成优化建议"""
        suggestions = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            lines = content.split('\n')
            
            # 运行各种分析器
            for category, analyzer in self.analyzers.items():
                category_suggestions = analyzer(file_path, tree, lines, complexity_metrics, performance_profile)
                suggestions.extend(category_suggestions)
        
        except Exception as e:
            logger.error(f"生成优化建议时出错 {file_path}: {e}")
        
        return suggestions
    
    def _analyze_data_structures(self, file_path: str, tree: ast.AST, lines: List[str], 
                               complexity_metrics: ComplexityMetrics, 
                               performance_profile: PerformanceProfile) -> List[OptimizationSuggestion]:
        """分析数据结构使用"""
        suggestions = []
        
        # 检查低效的数据结构使用
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # 检查list()和range()的组合
                if (isinstance(node.func, ast.Name) and node.func.id == 'list' and
                    len(node.args) > 0 and isinstance(node.args[0], ast.Call) and
                    isinstance(node.args[0].func, ast.Name) and node.args[0].func.id == 'range'):
                    
                    suggestions.append(OptimizationSuggestion(
                        suggestion_id=f"ds_{len(suggestions)}",
                        component_id=file_path,
                        category='data_structure',
                        priority='medium',
                        description="使用生成器替代list(range())以节省内存",
                        code_location=(node.lineno, node.lineno),
                        original_code=lines[node.lineno-1].strip() if node.lineno <= len(lines) else "",
                        suggested_code="# 使用生成器: range() 或 (i for i in range(...))",
                        estimated_improvement=15.0,
                        risk_level='safe',
                        auto_applicable=True,
                        reasoning="生成器按需生成元素，减少内存占用",
                        type='memory_optimization',
                        impact='medium',
                        effort='low',
                        code_example="# 替换: list(range(1000))\n# 使用: range(1000) 或 (i for i in range(1000))"
                    ))
        
        return suggestions
    
    def _analyze_algorithms(self, file_path: str, tree: ast.AST, lines: List[str], 
                          complexity_metrics: ComplexityMetrics, 
                          performance_profile: PerformanceProfile) -> List[OptimizationSuggestion]:
        """分析算法效率"""
        suggestions = []
        
        # 检查嵌套循环
        for node in ast.walk(tree):
            if isinstance(node, (ast.For, ast.While)):
                nested_loops = [n for n in ast.walk(node) if isinstance(n, (ast.For, ast.While)) and n != node]
                if len(nested_loops) >= 2:
                    suggestions.append(OptimizationSuggestion(
                         suggestion_id=f"algo_{len(suggestions)}",
                         component_id=file_path,
                         category='algorithm',
                         priority='high',
                         description="检测到深度嵌套循环，考虑算法优化",
                         code_location=(node.lineno, node.lineno + 5),
                         original_code="# 嵌套循环代码",
                         suggested_code="# 考虑使用更高效的算法或数据结构",
                         estimated_improvement=50.0,
                         risk_level='moderate',
                         auto_applicable=False,
                         reasoning="嵌套循环可能导致O(n²)或更高的时间复杂度",
                         type='algorithm_optimization',
                         impact='high',
                         effort='high',
                         code_example="# 考虑使用字典、集合或更高效的算法\n# 例如：使用哈希表替代嵌套查找"
                     ))
        
        return suggestions
    
    def _analyze_memory_usage(self, file_path: str, tree: ast.AST, lines: List[str], 
                            complexity_metrics: ComplexityMetrics, 
                            performance_profile: PerformanceProfile) -> List[OptimizationSuggestion]:
        """分析内存使用"""
        suggestions = []
        
        # 检查全局变量
        global_vars = [node for node in ast.walk(tree) if isinstance(node, ast.Global)]
        if len(global_vars) > 5:
            suggestions.append(OptimizationSuggestion(
                suggestion_id=f"mem_{len(suggestions)}",
                component_id=file_path,
                category='memory',
                priority='medium',
                description="过多全局变量可能导致内存泄漏",
                code_location=(1, len(lines)),
                original_code="# 全局变量声明",
                suggested_code="# 考虑使用类或模块级别的封装",
                estimated_improvement=20.0,
                risk_level='safe',
                auto_applicable=False,
                reasoning="全局变量在程序生命周期内持续占用内存",
                type='memory_optimization',
                impact='medium',
                effort='medium',
                code_example="# 替换全局变量:\n# 使用类属性或模块级配置对象"
            ))
        
        return suggestions
    
    def _analyze_io_operations(self, file_path: str, tree: ast.AST, lines: List[str], 
                             complexity_metrics: ComplexityMetrics, 
                             performance_profile: PerformanceProfile) -> List[OptimizationSuggestion]:
        """分析I/O操作"""
        suggestions = []
        
        # 检查文件操作
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'open':
                # 检查是否使用了with语句
                suggestions.append(OptimizationSuggestion(
                    suggestion_id=f"io_{len(suggestions)}",
                    component_id=file_path,
                    category='io',
                    priority='medium',
                    description="建议使用with语句进行文件操作",
                    code_location=(node.lineno, node.lineno),
                    original_code=lines[node.lineno-1].strip() if node.lineno <= len(lines) else "",
                    suggested_code="with open(...) as f:",
                    estimated_improvement=10.0,
                    risk_level='safe',
                    auto_applicable=True,
                    reasoning="with语句确保文件正确关闭，避免资源泄漏",
                    type='io_optimization',
                    impact='low',
                    effort='low',
                    code_example="# 替换: f = open('file.txt')\n# 使用: with open('file.txt') as f:"
                ))
        
        return suggestions
    
    def _analyze_concurrency(self, file_path: str, tree: ast.AST, lines: List[str], 
                           complexity_metrics: ComplexityMetrics, 
                           performance_profile: PerformanceProfile) -> List[OptimizationSuggestion]:
        """分析并发优化机会"""
        suggestions = []
        
        # 检查CPU密集型循环
        for node in ast.walk(tree):
            if isinstance(node, ast.For):
                # 简单启发式：长循环可能适合并行化
                if complexity_metrics.cyclomatic_complexity > 10:
                    suggestions.append(OptimizationSuggestion(
                        suggestion_id=f"conc_{len(suggestions)}",
                        component_id=file_path,
                        category='concurrency',
                        priority='low',
                        description="考虑使用多进程或多线程优化CPU密集型操作",
                        code_location=(node.lineno, node.lineno + 3),
                        original_code="# CPU密集型循环",
                        suggested_code="# 考虑使用multiprocessing.Pool或concurrent.futures",
                        estimated_improvement=30.0,
                        risk_level='risky',
                        auto_applicable=False,
                        reasoning="并行处理可以利用多核CPU提升性能",
                        type='concurrency_optimization',
                        impact='high',
                        effort='high',
                        code_example="# 使用多进程池:\nfrom multiprocessing import Pool\nwith Pool() as pool:\n    results = pool.map(func, data)"
                    ))
        
        return suggestions

class RealTimeMonitor:
    """实时性能监控器"""
    
    def __init__(self):
        self.monitoring_active = False
        self.performance_data = []
        self.thresholds = {
            'memory_mb': 500,
            'cpu_percent': 80,
            'execution_time_ms': 1000
        }
        self.alerts = []
    
    def start_monitoring(self, component_id: str):
        """开始实时监控"""
        self.monitoring_active = True
        self.component_id = component_id
        self.start_time = time.time()
        
        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info(f"开始监控组件: {component_id}")
    
    def stop_monitoring(self) -> Dict[str, Any]:
        """停止监控并返回报告"""
        self.monitoring_active = False
        
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=1.0)
        
        return self._generate_monitoring_report()
    
    def _monitor_loop(self):
        """监控循环"""
        while self.monitoring_active:
            try:
                # 收集性能数据
                data_point = self._collect_performance_data()
                self.performance_data.append(data_point)
                
                # 检查阈值
                self._check_thresholds(data_point)
                
                time.sleep(0.1)  # 100ms间隔
                
            except Exception as e:
                logger.error(f"监控循环错误: {e}")
                break
    
    def _collect_performance_data(self) -> Dict[str, Any]:
        """收集性能数据"""
        try:
            process = psutil.Process()
            
            return {
                'timestamp': time.time(),
                'memory_mb': process.memory_info().rss / 1024 / 1024,
                'cpu_percent': process.cpu_percent(),
                'memory_percent': process.memory_percent(),
                'num_threads': process.num_threads(),
                'io_counters': process.io_counters()._asdict() if hasattr(process, 'io_counters') else {}
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return {
                'timestamp': time.time(),
                'memory_mb': 0,
                'cpu_percent': 0,
                'memory_percent': 0,
                'num_threads': 0,
                'io_counters': {}
            }
    
    def _check_thresholds(self, data_point: Dict[str, Any]):
        """检查性能阈值"""
        alerts = []
        
        if data_point['memory_mb'] > self.thresholds['memory_mb']:
            alerts.append({
                'type': 'memory_high',
                'message': f"内存使用过高: {data_point['memory_mb']:.1f}MB",
                'severity': 'warning',
                'timestamp': data_point['timestamp']
            })
        
        if data_point['cpu_percent'] > self.thresholds['cpu_percent']:
            alerts.append({
                'type': 'cpu_high',
                'message': f"CPU使用过高: {data_point['cpu_percent']:.1f}%",
                'severity': 'warning',
                'timestamp': data_point['timestamp']
            })
        
        self.alerts.extend(alerts)
    
    def _generate_monitoring_report(self) -> Dict[str, Any]:
        """生成监控报告"""
        if not self.performance_data:
            return {'error': '无监控数据'}
        
        memory_values = [d['memory_mb'] for d in self.performance_data]
        cpu_values = [d['cpu_percent'] for d in self.performance_data if d['cpu_percent'] > 0]
        
        return {
            'component_id': self.component_id,
            'monitoring_duration': time.time() - self.start_time,
            'data_points': len(self.performance_data),
            'memory_stats': {
                'peak': max(memory_values) if memory_values else 0,
                'average': sum(memory_values) / len(memory_values) if memory_values else 0,
                'min': min(memory_values) if memory_values else 0
            },
            'cpu_stats': {
                'peak': max(cpu_values) if cpu_values else 0,
                'average': sum(cpu_values) / len(cpu_values) if cpu_values else 0
            },
            'alerts': self.alerts,
            'bottlenecks': self._identify_bottlenecks()
        }
    
    def _identify_bottlenecks(self) -> List[Dict[str, Any]]:
        """识别性能瓶颈"""
        bottlenecks = []
        
        if not self.performance_data:
            return bottlenecks
        
        # 内存瓶颈检测
        memory_values = [d['memory_mb'] for d in self.performance_data]
        if memory_values:
            memory_growth = max(memory_values) - min(memory_values)
            if memory_growth > 100:  # 100MB增长
                bottlenecks.append({
                    'type': 'memory_leak',
                    'description': f'检测到内存增长 {memory_growth:.1f}MB',
                    'severity': 'high'
                })
        
        # CPU瓶颈检测
        cpu_values = [d['cpu_percent'] for d in self.performance_data if d['cpu_percent'] > 0]
        if cpu_values:
            high_cpu_count = len([c for c in cpu_values if c > 80])
            if high_cpu_count > len(cpu_values) * 0.5:  # 超过50%的时间CPU高负载
                bottlenecks.append({
                    'type': 'cpu_intensive',
                    'description': f'CPU高负载时间占比 {high_cpu_count/len(cpu_values)*100:.1f}%',
                    'severity': 'medium'
                })
        
        return bottlenecks

class PerformanceTrendAnalyzer:
    """性能趋势分析器"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def analyze_trends(self, component_id: str, days: int = 7) -> Dict[str, Any]:
        """分析性能趋势"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 获取历史性能数据
                cursor.execute("""
                    SELECT timestamp, execution_time, memory_peak, cpu_usage
                    FROM performance_metrics 
                    WHERE component_id = ? AND timestamp > datetime('now', '-{} days')
                    ORDER BY timestamp
                """.format(days), (component_id,))
                
                data = cursor.fetchall()
            
            if not data:
                return {'error': '无历史数据'}
            
            # 分析趋势
            timestamps = [row[0] for row in data]
            execution_times = [row[1] for row in data]
            memory_peaks = [row[2] for row in data]
            cpu_usages = [row[3] for row in data]
            
            return {
                'component_id': component_id,
                'data_points': len(data),
                'time_range': f'{timestamps[0]} to {timestamps[-1]}',
                'execution_time_trend': self._calculate_trend(execution_times),
                'memory_trend': self._calculate_trend(memory_peaks),
                'cpu_trend': self._calculate_trend(cpu_usages),
                'performance_score': self._calculate_performance_score(execution_times, memory_peaks, cpu_usages),
                'recommendations': self._generate_trend_recommendations(execution_times, memory_peaks, cpu_usages)
            }
        
        except Exception as e:
            logger.error(f"趋势分析失败: {e}")
            return {'error': str(e)}
    
    def _calculate_trend(self, values: List[float]) -> Dict[str, Any]:
        """计算趋势"""
        if len(values) < 2:
            return {'trend': 'insufficient_data'}
        
        # 简单线性趋势计算
        x = list(range(len(values)))
        n = len(values)
        
        sum_x = sum(x)
        sum_y = sum(values)
        sum_xy = sum(x[i] * values[i] for i in range(n))
        sum_x2 = sum(xi * xi for xi in x)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        
        trend_direction = 'improving' if slope < 0 else 'degrading' if slope > 0 else 'stable'
        
        return {
            'trend': trend_direction,
            'slope': slope,
            'current_value': values[-1],
            'average_value': sum(values) / len(values),
            'change_rate': abs(slope) / (sum(values) / len(values)) * 100 if sum(values) > 0 else 0
        }
    
    def _calculate_performance_score(self, execution_times: List[float], 
                                   memory_peaks: List[float], cpu_usages: List[float]) -> float:
        """计算性能评分"""
        # 基于最近的性能数据计算评分 (0-100)
        recent_data = 5  # 最近5次数据
        
        recent_exec = execution_times[-recent_data:] if len(execution_times) >= recent_data else execution_times
        recent_memory = memory_peaks[-recent_data:] if len(memory_peaks) >= recent_data else memory_peaks
        recent_cpu = cpu_usages[-recent_data:] if len(cpu_usages) >= recent_data else cpu_usages
        
        # 执行时间评分 (越低越好)
        exec_score = max(0, 100 - (sum(recent_exec) / len(recent_exec)) * 10)
        
        # 内存使用评分 (越低越好)
        memory_score = max(0, 100 - (sum(recent_memory) / len(recent_memory)) / 10)
        
        # CPU使用评分 (越低越好)
        cpu_score = max(0, 100 - (sum(recent_cpu) / len(recent_cpu)))
        
        # 综合评分
        return (exec_score * 0.4 + memory_score * 0.3 + cpu_score * 0.3)
    
    def _generate_trend_recommendations(self, execution_times: List[float],
                                      memory_peaks: List[float], cpu_usages: List[float]) -> List[str]:
        """基于趋势生成建议"""
        recommendations = []
        
        exec_trend = self._calculate_trend(execution_times)
        memory_trend = self._calculate_trend(memory_peaks)
        cpu_trend = self._calculate_trend(cpu_usages)
        
        if exec_trend['trend'] == 'degrading':
            recommendations.append("执行时间呈上升趋势，建议进行算法优化")
        
        if memory_trend['trend'] == 'degrading':
            recommendations.append("内存使用呈上升趋势，检查是否存在内存泄漏")
        
        if cpu_trend['trend'] == 'degrading':
            recommendations.append("CPU使用率上升，考虑优化计算密集型操作")
        
        if not recommendations:
            recommendations.append("性能趋势稳定，继续保持当前优化策略")
        
        return recommendations

class AutomatedOptimizer:
    """自动化优化应用器"""
    
    def __init__(self, backup_manager: 'BackupManager'):
        self.backup_manager = backup_manager
        self.applied_optimizations = []
    
    def apply_safe_optimizations(self, file_path: str, suggestions: List[OptimizationSuggestion]) -> Dict[str, Any]:
        """应用安全的优化建议"""
        results = {
            'applied': [],
            'skipped': [],
            'errors': []
        }
        
        # 创建备份
        backup_path = self.backup_manager.create_backup(file_path)
        if not backup_path:
            return {'error': '无法创建备份文件'}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            modified_content = original_content
            
            # 按风险等级排序，优先应用低风险优化
            safe_suggestions = [s for s in suggestions if s.risk_level in ['low', 'medium']]
            safe_suggestions.sort(key=lambda x: {'low': 0, 'medium': 1}.get(x.risk_level, 2))
            
            for suggestion in safe_suggestions:
                try:
                    if suggestion.category == 'import_optimization':
                        modified_content = self._optimize_imports(modified_content)
                        results['applied'].append(suggestion.description)
                    
                    elif suggestion.category == 'loop_optimization':
                        modified_content = self._optimize_loops(modified_content)
                        results['applied'].append(suggestion.description)
                    
                    elif suggestion.category == 'string_optimization':
                        modified_content = self._optimize_strings(modified_content)
                        results['applied'].append(suggestion.description)
                    
                    elif suggestion.category == 'variable_naming':
                        modified_content = self._normalize_variable_names(modified_content)
                        results['applied'].append(suggestion.description)
                    
                    else:
                        results['skipped'].append(f"未支持的优化类型: {suggestion.category}")
                
                except Exception as e:
                    results['errors'].append(f"应用优化失败 {suggestion.description}: {str(e)}")
            
            # 写入优化后的内容
            if modified_content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(modified_content)
                
                # 验证语法
                if not self._validate_syntax(file_path):
                    # 语法错误，回滚
                    self.backup_manager.restore_backup(backup_path, file_path)
                    results['errors'].append("语法验证失败，已回滚更改")
                else:
                    self.applied_optimizations.append({
                        'file_path': file_path,
                        'backup_path': backup_path,
                        'timestamp': time.time(),
                        'optimizations': results['applied']
                    })
            
            return results
        
        except Exception as e:
            # 发生错误，回滚
            self.backup_manager.restore_backup(backup_path, file_path)
            return {'error': f'优化过程中发生错误: {str(e)}'}
    
    def _optimize_imports(self, content: str) -> str:
        """优化导入语句"""
        lines = content.split('\n')
        import_lines = []
        other_lines = []
        
        # 分离导入语句和其他代码
        in_imports = True
        for line in lines:
            stripped = line.strip()
            if stripped.startswith(('import ', 'from ')) and in_imports:
                import_lines.append(line)
            elif stripped == '' and in_imports:
                import_lines.append(line)
            else:
                if stripped != '':
                    in_imports = False
                other_lines.append(line)
        
        # 排序和去重导入
        if import_lines:
            # 分类导入
            stdlib_imports = []
            third_party_imports = []
            local_imports = []
            
            for line in import_lines:
                stripped = line.strip()
                if not stripped or stripped.startswith('#'):
                    continue
                
                if stripped.startswith('from .') or stripped.startswith('import .'):
                    local_imports.append(line)
                elif any(stdlib in stripped for stdlib in ['os', 'sys', 'time', 'datetime', 'json', 'sqlite3', 'threading']):
                    stdlib_imports.append(line)
                else:
                    third_party_imports.append(line)
            
            # 重新组织导入
            organized_imports = []
            if stdlib_imports:
                organized_imports.extend(sorted(set(stdlib_imports)))
                organized_imports.append('')
            if third_party_imports:
                organized_imports.extend(sorted(set(third_party_imports)))
                organized_imports.append('')
            if local_imports:
                organized_imports.extend(sorted(set(local_imports)))
                organized_imports.append('')
            
            return '\n'.join(organized_imports + other_lines)
        
        return content
    
    def _optimize_loops(self, content: str) -> str:
        """优化循环结构"""
        import re
        
        # 优化 range(len()) 模式
        pattern = r'for\s+(\w+)\s+in\s+range\(len\(([^)]+)\)\):'
        replacement = r'for \1, item in enumerate(\2):'
        content = re.sub(pattern, replacement, content)
        
        # 优化简单的列表构建循环为列表推导式
        # 这是一个简化的实现，实际应用中需要更复杂的AST分析
        lines = content.split('\n')
        optimized_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            # 检测简单的列表构建模式
            if (line.endswith('= []') and i + 3 < len(lines) and 
                lines[i + 1].strip().startswith('for ') and
                lines[i + 2].strip().startswith('    ') and
                '.append(' in lines[i + 2]):
                
                # 尝试转换为列表推导式
                var_name = line.split('=')[0].strip()
                for_line = lines[i + 1].strip()
                append_line = lines[i + 2].strip()
                
                if append_line.startswith(f'    {var_name}.append('):
                    append_content = append_line[append_line.find('append(') + 7:-1]
                    optimized_lines.append(f"{var_name} = [{append_content} {for_line[4:]}]")
                    i += 3
                    continue
            
            optimized_lines.append(lines[i])
            i += 1
        
        return '\n'.join(optimized_lines)
    
    def _optimize_strings(self, content: str) -> str:
        """优化字符串操作"""
        import re
        
        # 优化字符串连接
        # 查找 += 字符串连接模式
        pattern = r'(\w+)\s*\+=\s*["\'][^"\']*.?["\']'
        matches = re.findall(pattern, content)
        
        for var_name in set(matches):
            # 建议使用 join() 或 f-string
            # 这里只是标记，实际实现需要更复杂的分析
            pass
        
        return content
    
    def _normalize_variable_names(self, content: str) -> str:
        """规范化变量命名"""
        import re
        
        # 这是一个简化的实现
        # 实际应用中需要使用AST进行更精确的变量重命名
        
        # 转换驼峰命名为下划线命名（仅限于明显的情况）
        camel_case_pattern = r'\b([a-z]+)([A-Z][a-z]+)+\b'
        
        def camel_to_snake(match):
            name = match.group(0)
            # 只转换明显的变量名，避免转换类名或其他标识符
            if name[0].islower():
                return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
            return name
        
        content = re.sub(camel_case_pattern, camel_to_snake, content)
        
        return content
    
    def _validate_syntax(self, file_path: str) -> bool:
        """验证Python文件语法"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            compile(content, file_path, 'exec')
            return True
        
        except SyntaxError as e:
            logger.error(f"语法错误 {file_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"验证失败 {file_path}: {e}")
            return False
    
    def rollback_optimization(self, file_path: str) -> bool:
        """回滚指定文件的优化"""
        for opt in self.applied_optimizations:
            if opt['file_path'] == file_path:
                success = self.backup_manager.restore_backup(opt['backup_path'], file_path)
                if success:
                    self.applied_optimizations.remove(opt)
                    logger.info(f"已回滚文件优化: {file_path}")
                return success
        
        return False
    
    def get_optimization_history(self) -> List[Dict[str, Any]]:
        """获取优化历史"""
        return self.applied_optimizations.copy()

# InteractiveOptimizationInterface类已移除，使用简化的main函数

class AdvancedPerformanceOptimizer:
    """增强的性能优化器主类"""
    
    def __init__(self, db_path: str = "advanced_optimizer.db", project_root: str = "."):
        self.db_path = db_path
        self.project_root = Path(project_root).resolve()
        self.backup_dir = self.project_root / "optimization_backups"
        self.backup_dir.mkdir(exist_ok=True)
        
        # 初始化组件
        self.code_analyzer = CodeAnalyzer()
        self.performance_profiler = PerformanceProfiler()
        self.optimization_engine = OptimizationEngine()
        
        # 缓存
        self.analysis_cache = {}
        self.suggestion_cache = {}
        
        self._init_database()
    
    def _init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            # 复杂度指标表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS complexity_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    component_id TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    cyclomatic_complexity INTEGER,
                    cognitive_complexity INTEGER,
                    nesting_depth INTEGER,
                    function_count INTEGER,
                    class_count INTEGER,
                    line_count INTEGER,
                    comment_ratio REAL,
                    duplicate_lines INTEGER
                )
            """)
            
            # 性能分析表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    component_id TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    execution_time REAL,
                    memory_peak REAL,
                    memory_average REAL,
                    cpu_usage REAL,
                    io_operations INTEGER,
                    hotspots TEXT,
                    bottlenecks TEXT
                )
            """)
            
            # 优化建议表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS optimization_suggestions (
                    suggestion_id TEXT PRIMARY KEY,
                    component_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    description TEXT NOT NULL,
                    code_location TEXT,
                    original_code TEXT,
                    suggested_code TEXT,
                    estimated_improvement REAL,
                    risk_level TEXT,
                    auto_applicable BOOLEAN,
                    reasoning TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    applied_at DATETIME
                )
            """)
            
            # 风险评估表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS risk_assessments (
                    risk_id TEXT PRIMARY KEY,
                    component_id TEXT NOT NULL,
                    risk_level TEXT NOT NULL,
                    risk_factors TEXT,
                    impact_analysis TEXT,
                    mitigation_strategies TEXT,
                    requires_manual_review BOOLEAN,
                    backup_required BOOLEAN,
                    rollback_plan TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
    
    def analyze_component(self, file_path: str) -> Tuple[ComplexityMetrics, Optional[PerformanceProfile]]:
        """分析组件性能"""
        logger.info(f"开始分析组件: {file_path}")
        
        # 复杂度分析
        complexity_metrics = self.code_analyzer.analyze_complexity(file_path)
        
        # 性能分析（如果是Python文件）
        performance_profile = None
        if file_path.endswith('.py'):
            try:
                # 这里可以添加实际的性能测试逻辑
                # 目前创建一个模拟的性能配置文件
                performance_profile = PerformanceProfile(
                    component_id=file_path,
                    execution_time=0.0,
                    memory_peak=0.0,
                    memory_average=0.0,
                    cpu_usage=0.0,
                    io_operations=0,
                    function_calls={},
                    hotspots=[],
                    bottlenecks=[]
                )
            except Exception as e:
                logger.error(f"性能分析失败 {file_path}: {e}")
        
        # 保存分析结果
        self._save_analysis_results(file_path, complexity_metrics, performance_profile)
        
        return complexity_metrics, performance_profile
    
    def _save_analysis_results(self, file_path: str, complexity_metrics: ComplexityMetrics, 
                             performance_profile: Optional[PerformanceProfile]):
        """保存分析结果到数据库"""
        with sqlite3.connect(self.db_path) as conn:
            # 保存复杂度指标
            conn.execute("""
                INSERT INTO complexity_metrics (
                    component_id, cyclomatic_complexity, cognitive_complexity, nesting_depth,
                    function_count, class_count, line_count, comment_ratio, duplicate_lines
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                file_path, complexity_metrics.cyclomatic_complexity, complexity_metrics.cognitive_complexity,
                complexity_metrics.nesting_depth, complexity_metrics.function_count, complexity_metrics.class_count,
                complexity_metrics.line_count, complexity_metrics.comment_ratio, complexity_metrics.duplicate_lines
            ))
            
            # 保存性能分析结果
            if performance_profile:
                conn.execute("""
                    INSERT INTO performance_profiles (
                        component_id, execution_time, memory_peak, memory_average, cpu_usage,
                        io_operations, hotspots, bottlenecks
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    performance_profile.component_id, performance_profile.execution_time,
                    performance_profile.memory_peak, performance_profile.memory_average,
                    performance_profile.cpu_usage, performance_profile.io_operations,
                    json.dumps(performance_profile.hotspots), json.dumps(performance_profile.bottlenecks)
                ))
            
            conn.commit()
    
    def generate_optimization_suggestions(self, file_path: str) -> List[OptimizationSuggestion]:
        """生成优化建议"""
        # 首先分析组件
        complexity_metrics, performance_profile = self.analyze_component(file_path)
        
        # 如果没有性能配置文件，创建一个默认的
        if performance_profile is None:
            performance_profile = PerformanceProfile(
                component_id=file_path,
                execution_time=0.0,
                memory_peak=0.0,
                memory_average=0.0,
                cpu_usage=0.0,
                io_operations=0,
                function_calls={},
                hotspots=[],
                bottlenecks=[]
            )
        
        # 使用优化引擎生成建议
        suggestions = self.optimization_engine.generate_suggestions(
            file_path, complexity_metrics, performance_profile
        )
        
        # 保存建议到数据库
        self._save_suggestions(suggestions)
        
        return suggestions
    
    def _save_suggestions(self, suggestions: List[OptimizationSuggestion]):
        """保存优化建议到数据库"""
        with sqlite3.connect(self.db_path) as conn:
            for suggestion in suggestions:
                conn.execute("""
                    INSERT OR REPLACE INTO optimization_suggestions (
                        suggestion_id, component_id, category, priority, description,
                        code_location, original_code, suggested_code, estimated_improvement,
                        risk_level, auto_applicable, reasoning
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    suggestion.suggestion_id, suggestion.component_id, suggestion.category,
                    suggestion.priority, suggestion.description, str(suggestion.code_location),
                    suggestion.original_code, suggestion.suggested_code, suggestion.estimated_improvement,
                    suggestion.risk_level, suggestion.auto_applicable, suggestion.reasoning
                ))
            conn.commit()

def main():
    """主函数 - 交互式优化界面"""
    optimizer = AdvancedPerformanceOptimizer()
    
    print("=== 增强的Python性能优化系统 ===")
    print("1. 分析单个文件")
    print("2. 分析整个项目")
    print("3. 查看优化建议")
    print("4. 应用优化")
    print("5. 性能监控")
    print("0. 退出")
    
    while True:
        try:
            choice = input("\n请选择操作: ").strip()
            
            if choice == '0':
                break
            elif choice == '1':
                file_path = input("请输入文件路径: ").strip()
                if os.path.exists(file_path):
                    complexity, performance = optimizer.analyze_component(file_path)
                    print(f"\n复杂度分析结果:")
                    print(f"  圈复杂度: {complexity.cyclomatic_complexity}")
                    print(f"  认知复杂度: {complexity.cognitive_complexity}")
                    print(f"  最大嵌套深度: {complexity.nesting_depth}")
                    print(f"  函数数量: {complexity.function_count}")
                    print(f"  类数量: {complexity.class_count}")
                    print(f"  代码行数: {complexity.line_count}")
                    print(f"  注释比例: {complexity.comment_ratio:.2%}")
                    print(f"  重复代码行: {complexity.duplicate_lines}")
                else:
                    print("文件不存在!")
            else:
                print("功能开发中...")
        
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"操作出错: {e}")
    
    print("\n感谢使用增强的Python性能优化系统!")

if __name__ == "__main__":
    main()