#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能优化系统数据模型
包含所有数据类定义和类型声明
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime

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
    
    def get_complexity_score(self) -> float:
        """计算综合复杂度评分"""
        # 权重化计算复杂度评分
        score = (
            self.cyclomatic_complexity * 0.3 +
            self.cognitive_complexity * 0.4 +
            self.nesting_depth * 0.2 +
            (self.duplicate_lines / max(self.line_count, 1)) * 100 * 0.1
        )
        return min(score, 100.0)  # 限制最大值为100
    
    def get_maintainability_index(self) -> float:
        """计算可维护性指数"""
        # 基于复杂度和注释比例计算可维护性
        base_score = 100
        complexity_penalty = self.get_complexity_score() * 0.5
        comment_bonus = self.comment_ratio * 20
        
        return max(0, base_score - complexity_penalty + comment_bonus)

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
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def get_performance_score(self) -> float:
        """计算性能评分"""
        # 基于执行时间、内存使用和CPU使用率计算评分
        time_score = max(0, 100 - self.execution_time * 10)
        memory_score = max(0, 100 - self.memory_peak / 10)
        cpu_score = max(0, 100 - self.cpu_usage)
        
        return (time_score + memory_score + cpu_score) / 3

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
    confidence_score: float = 0.0  # 风险评估的置信度
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def get_risk_score(self) -> int:
        """获取数值化的风险评分"""
        risk_scores = {
            'low': 1,
            'medium': 2,
            'high': 3,
            'critical': 4
        }
        return risk_scores.get(self.risk_level.lower(), 0)

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
    risk_assessment: Optional[RiskAssessment] = None
    status: str = 'pending'  # 'pending', 'applied', 'rejected', 'failed'
    created_at: Optional[datetime] = None
    applied_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def get_priority_score(self) -> int:
        """获取数值化的优先级评分"""
        priority_scores = {
            'critical': 4,
            'high': 3,
            'medium': 2,
            'low': 1
        }
        return priority_scores.get(self.priority.lower(), 0)
    
    def get_effort_score(self) -> int:
        """获取数值化的工作量评分"""
        effort_scores = {
            'low': 1,
            'medium': 2,
            'high': 3
        }
        return effort_scores.get(self.effort.lower(), 0)
    
    def get_roi_score(self) -> float:
        """计算投资回报率评分"""
        if self.get_effort_score() == 0:
            return 0.0
        return self.estimated_improvement / self.get_effort_score()

@dataclass
class MemoryUsageMetrics:
    """内存使用指标"""
    peak_usage: float  # MB
    average_usage: float  # MB
    allocation_count: int
    deallocation_count: int
    memory_leaks: List[str]
    gc_collections: int
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def get_memory_efficiency(self) -> float:
        """计算内存使用效率"""
        if self.peak_usage == 0:
            return 100.0
        return min(100.0, (self.average_usage / self.peak_usage) * 100)

@dataclass
class CPUUsageMetrics:
    """CPU使用指标"""
    average_usage: float  # 百分比
    peak_usage: float  # 百分比
    user_time: float  # 秒
    system_time: float  # 秒
    context_switches: int
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def get_cpu_efficiency(self) -> float:
        """计算CPU使用效率"""
        if self.peak_usage == 0:
            return 100.0
        return min(100.0, 100 - self.average_usage)

@dataclass
class IOMetrics:
    """IO操作指标"""
    read_operations: int
    write_operations: int
    read_bytes: int
    write_bytes: int
    read_time: float  # 秒
    write_time: float  # 秒
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def get_total_operations(self) -> int:
        """获取总IO操作数"""
        return self.read_operations + self.write_operations
    
    def get_total_bytes(self) -> int:
        """获取总字节数"""
        return self.read_bytes + self.write_bytes
    
    def get_io_efficiency(self) -> float:
        """计算IO效率"""
        total_time = self.read_time + self.write_time
        if total_time == 0:
            return 100.0
        
        total_bytes = self.get_total_bytes()
        if total_bytes == 0:
            return 0.0
        
        # MB/s
        throughput = (total_bytes / (1024 * 1024)) / total_time
        return min(100.0, throughput * 10)  # 简化的效率计算

@dataclass
class OptimizationResult:
    """优化结果"""
    suggestion_id: str
    component_id: str
    success: bool
    error_message: Optional[str] = None
    before_metrics: Optional[PerformanceProfile] = None
    after_metrics: Optional[PerformanceProfile] = None
    actual_improvement: Optional[float] = None
    backup_path: Optional[str] = None
    applied_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.applied_at is None:
            self.applied_at = datetime.now()
    
    def calculate_actual_improvement(self) -> float:
        """计算实际性能改进"""
        if not self.before_metrics or not self.after_metrics:
            return 0.0
        
        before_score = self.before_metrics.get_performance_score()
        after_score = self.after_metrics.get_performance_score()
        
        if before_score == 0:
            return 0.0
        
        improvement = ((after_score - before_score) / before_score) * 100
        self.actual_improvement = improvement
        return improvement

@dataclass
class ComponentAnalysisReport:
    """组件分析报告"""
    component_id: str
    complexity_metrics: ComplexityMetrics
    performance_profile: Optional[PerformanceProfile]
    optimization_suggestions: List[OptimizationSuggestion]
    risk_assessments: List[RiskAssessment]
    memory_metrics: Optional[MemoryUsageMetrics] = None
    cpu_metrics: Optional[CPUUsageMetrics] = None
    io_metrics: Optional[IOMetrics] = None
    overall_score: float = 0.0
    analysis_timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.analysis_timestamp is None:
            self.analysis_timestamp = datetime.now()
        self.calculate_overall_score()
    
    def calculate_overall_score(self):
        """计算综合评分"""
        scores = []
        
        # 复杂度评分（越低越好）
        complexity_score = max(0, 100 - self.complexity_metrics.get_complexity_score())
        scores.append(complexity_score * 0.3)
        
        # 性能评分
        if self.performance_profile:
            performance_score = self.performance_profile.get_performance_score()
            scores.append(performance_score * 0.4)
        
        # 内存效率评分
        if self.memory_metrics:
            memory_score = self.memory_metrics.get_memory_efficiency()
            scores.append(memory_score * 0.2)
        
        # CPU效率评分
        if self.cpu_metrics:
            cpu_score = self.cpu_metrics.get_cpu_efficiency()
            scores.append(cpu_score * 0.1)
        
        self.overall_score = sum(scores) if scores else 0.0
    
    def get_high_priority_suggestions(self) -> List[OptimizationSuggestion]:
        """获取高优先级建议"""
        return [s for s in self.optimization_suggestions if s.priority in ['critical', 'high']]
    
    def get_safe_suggestions(self) -> List[OptimizationSuggestion]:
        """获取安全的优化建议"""
        return [s for s in self.optimization_suggestions if s.risk_level == 'safe' and s.auto_applicable]
    
    def get_risky_suggestions(self) -> List[OptimizationSuggestion]:
        """获取高风险建议"""
        return [s for s in self.optimization_suggestions if s.risk_level in ['moderate', 'risky'] or not s.auto_applicable]

# 类型别名
AnalysisResults = Dict[str, ComponentAnalysisReport]
OptimizationHistory = List[OptimizationResult]
MetricsTimeSeries = List[Tuple[datetime, PerformanceProfile]]