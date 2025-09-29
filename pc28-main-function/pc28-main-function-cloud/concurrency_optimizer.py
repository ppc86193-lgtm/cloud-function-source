#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
并发参数动态调整系统
根据业务需求、系统负载和性能指标自动优化并发参数
"""

import sqlite3
import json
import logging
import threading
import time
import os
import psutil
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any, Callable
from pathlib import Path
import statistics
import math

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """系统指标"""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_io_read: float
    disk_io_write: float
    network_io_sent: float
    network_io_recv: float
    load_average: float
    active_connections: int
    response_time: float
    throughput: float
    error_rate: float

@dataclass
class ConcurrencyConfig:
    """并发配置"""
    config_id: str
    service_name: str
    max_workers: int
    queue_size: int
    timeout_seconds: int
    batch_size: int
    retry_attempts: int
    connection_pool_size: int
    thread_pool_size: int
    process_pool_size: int
    rate_limit_per_second: float
    circuit_breaker_threshold: int
    created_at: datetime
    updated_at: datetime
    is_active: bool

@dataclass
class PerformanceProfile:
    """性能配置文件"""
    profile_name: str
    target_cpu_usage: float
    target_memory_usage: float
    target_response_time: float
    target_throughput: float
    max_error_rate: float
    optimization_strategy: str  # 'throughput', 'latency', 'balanced', 'resource_conservative'
    priority_weight: float

@dataclass
class OptimizationResult:
    """优化结果"""
    optimization_id: str
    timestamp: datetime
    service_name: str
    old_config: ConcurrencyConfig
    new_config: ConcurrencyConfig
    performance_improvement: float
    resource_efficiency: float
    recommendation_reason: str
    confidence_score: float
    applied: bool

@dataclass
class BusinessRule:
    """业务规则"""
    rule_id: str
    rule_name: str
    condition: str  # 'time_based', 'load_based', 'event_based'
    trigger_condition: Dict[str, Any]
    target_config: Dict[str, Any]
    priority: int
    enabled: bool
    description: str

class ConcurrencyOptimizer:
    """并发参数动态调整系统"""
    
    def __init__(self, db_path: str = "concurrency_optimizer.db"):
        self.db_path = db_path
        self.optimization_active = False
        self.check_interval = 60  # 1分钟检查一次
        self.optimizer_thread = None
        
        # 性能配置文件
        self.performance_profiles = {}
        
        # 业务规则
        self.business_rules = {}
        
        # 当前配置
        self.current_configs = {}
        
        # 性能历史
        self.performance_history = []
        self.max_history_size = 1000
        
        # 优化策略
        self.optimization_strategies = {
            'throughput': self._optimize_for_throughput,
            'latency': self._optimize_for_latency,
            'balanced': self._optimize_balanced,
            'resource_conservative': self._optimize_resource_conservative
        }
        
        # 机器学习模型参数
        self.learning_rate = 0.1
        self.momentum = 0.9
        self.adaptation_threshold = 0.05
        
        self._init_database()
        self._setup_default_profiles()
        self._setup_default_rules()
    
    def _init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            # 系统指标表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS system_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    cpu_usage REAL NOT NULL,
                    memory_usage REAL NOT NULL,
                    disk_io_read REAL DEFAULT 0,
                    disk_io_write REAL DEFAULT 0,
                    network_io_sent REAL DEFAULT 0,
                    network_io_recv REAL DEFAULT 0,
                    load_average REAL DEFAULT 0,
                    active_connections INTEGER DEFAULT 0,
                    response_time REAL DEFAULT 0,
                    throughput REAL DEFAULT 0,
                    error_rate REAL DEFAULT 0
                )
            """)
            
            # 并发配置表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS concurrency_configs (
                    config_id TEXT PRIMARY KEY,
                    service_name TEXT NOT NULL,
                    max_workers INTEGER NOT NULL,
                    queue_size INTEGER NOT NULL,
                    timeout_seconds INTEGER NOT NULL,
                    batch_size INTEGER NOT NULL,
                    retry_attempts INTEGER NOT NULL,
                    connection_pool_size INTEGER NOT NULL,
                    thread_pool_size INTEGER NOT NULL,
                    process_pool_size INTEGER NOT NULL,
                    rate_limit_per_second REAL NOT NULL,
                    circuit_breaker_threshold INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            """)
            
            # 性能配置文件表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_profiles (
                    profile_name TEXT PRIMARY KEY,
                    target_cpu_usage REAL NOT NULL,
                    target_memory_usage REAL NOT NULL,
                    target_response_time REAL NOT NULL,
                    target_throughput REAL NOT NULL,
                    max_error_rate REAL NOT NULL,
                    optimization_strategy TEXT NOT NULL,
                    priority_weight REAL NOT NULL
                )
            """)
            
            # 优化结果表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS optimization_results (
                    optimization_id TEXT PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    service_name TEXT NOT NULL,
                    old_config TEXT NOT NULL,
                    new_config TEXT NOT NULL,
                    performance_improvement REAL NOT NULL,
                    resource_efficiency REAL NOT NULL,
                    recommendation_reason TEXT NOT NULL,
                    confidence_score REAL NOT NULL,
                    applied BOOLEAN DEFAULT 0
                )
            """)
            
            # 业务规则表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS business_rules (
                    rule_id TEXT PRIMARY KEY,
                    rule_name TEXT NOT NULL,
                    condition TEXT NOT NULL,
                    trigger_condition TEXT NOT NULL,
                    target_config TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    enabled BOOLEAN DEFAULT 1,
                    description TEXT
                )
            """)
            
            conn.commit()
    
    def _setup_default_profiles(self):
        """设置默认性能配置文件"""
        default_profiles = [
            PerformanceProfile(
                profile_name="high_throughput",
                target_cpu_usage=85.0,
                target_memory_usage=80.0,
                target_response_time=500.0,
                target_throughput=1000.0,
                max_error_rate=1.0,
                optimization_strategy="throughput",
                priority_weight=1.0
            ),
            PerformanceProfile(
                profile_name="low_latency",
                target_cpu_usage=70.0,
                target_memory_usage=60.0,
                target_response_time=100.0,
                target_throughput=500.0,
                max_error_rate=0.5,
                optimization_strategy="latency",
                priority_weight=1.2
            ),
            PerformanceProfile(
                profile_name="balanced",
                target_cpu_usage=75.0,
                target_memory_usage=70.0,
                target_response_time=200.0,
                target_throughput=750.0,
                max_error_rate=0.8,
                optimization_strategy="balanced",
                priority_weight=1.0
            ),
            PerformanceProfile(
                profile_name="resource_conservative",
                target_cpu_usage=60.0,
                target_memory_usage=50.0,
                target_response_time=300.0,
                target_throughput=300.0,
                max_error_rate=0.3,
                optimization_strategy="resource_conservative",
                priority_weight=0.8
            )
        ]
        
        for profile in default_profiles:
            self.add_performance_profile(profile)
    
    def _setup_default_rules(self):
        """设置默认业务规则"""
        default_rules = [
            BusinessRule(
                rule_id="peak_hours",
                rule_name="高峰时段优化",
                condition="time_based",
                trigger_condition={"hours": [9, 10, 11, 14, 15, 16, 20, 21]},
                target_config={"profile": "high_throughput", "max_workers_multiplier": 1.5},
                priority=1,
                enabled=True,
                description="在业务高峰时段提高并发能力"
            ),
            BusinessRule(
                rule_id="low_traffic",
                rule_name="低流量时段优化",
                condition="time_based",
                trigger_condition={"hours": [0, 1, 2, 3, 4, 5, 6]},
                target_config={"profile": "resource_conservative", "max_workers_multiplier": 0.5},
                priority=2,
                enabled=True,
                description="在低流量时段节约资源"
            ),
            BusinessRule(
                rule_id="high_cpu_load",
                rule_name="高CPU负载保护",
                condition="load_based",
                trigger_condition={"cpu_usage": {"threshold": 90.0, "duration": 300}},
                target_config={"profile": "resource_conservative", "max_workers_multiplier": 0.7},
                priority=0,
                enabled=True,
                description="CPU负载过高时降低并发度"
            ),
            BusinessRule(
                rule_id="high_error_rate",
                rule_name="高错误率保护",
                condition="load_based",
                trigger_condition={"error_rate": {"threshold": 5.0, "duration": 180}},
                target_config={"profile": "low_latency", "max_workers_multiplier": 0.6},
                priority=0,
                enabled=True,
                description="错误率过高时降低负载"
            )
        ]
        
        for rule in default_rules:
            self.add_business_rule(rule)
    
    def add_performance_profile(self, profile: PerformanceProfile):
        """添加性能配置文件"""
        self.performance_profiles[profile.profile_name] = profile
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO performance_profiles 
                (profile_name, target_cpu_usage, target_memory_usage, target_response_time,
                 target_throughput, max_error_rate, optimization_strategy, priority_weight)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                profile.profile_name, profile.target_cpu_usage, profile.target_memory_usage,
                profile.target_response_time, profile.target_throughput, profile.max_error_rate,
                profile.optimization_strategy, profile.priority_weight
            ))
            conn.commit()
    
    def add_business_rule(self, rule: BusinessRule):
        """添加业务规则"""
        self.business_rules[rule.rule_id] = rule
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO business_rules 
                (rule_id, rule_name, condition, trigger_condition, target_config, 
                 priority, enabled, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.rule_id, rule.rule_name, rule.condition,
                json.dumps(rule.trigger_condition), json.dumps(rule.target_config),
                rule.priority, rule.enabled, rule.description
            ))
            conn.commit()
    
    def collect_system_metrics(self) -> SystemMetrics:
        """收集系统指标"""
        try:
            # 使用psutil收集系统指标
            cpu_usage = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # 磁盘IO
            disk_io = psutil.disk_io_counters()
            disk_io_read = disk_io.read_bytes if disk_io else 0
            disk_io_write = disk_io.write_bytes if disk_io else 0
            
            # 网络IO
            network_io = psutil.net_io_counters()
            network_io_sent = network_io.bytes_sent if network_io else 0
            network_io_recv = network_io.bytes_recv if network_io else 0
            
            # 负载平均值
            load_average = psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else cpu_usage / 100
            
            # 模拟应用指标
            import random
            active_connections = random.randint(50, 500)
            response_time = random.uniform(50, 300)
            throughput = random.uniform(100, 1000)
            error_rate = random.uniform(0, 2)
            
        except ImportError:
            # 如果没有psutil，使用模拟数据
            import random
            cpu_usage = random.uniform(30, 80)
            memory_usage = random.uniform(40, 70)
            disk_io_read = random.uniform(1000000, 10000000)
            disk_io_write = random.uniform(500000, 5000000)
            network_io_sent = random.uniform(1000000, 10000000)
            network_io_recv = random.uniform(2000000, 20000000)
            load_average = random.uniform(0.5, 2.0)
            active_connections = random.randint(50, 500)
            response_time = random.uniform(50, 300)
            throughput = random.uniform(100, 1000)
            error_rate = random.uniform(0, 2)
        
        return SystemMetrics(
            timestamp=datetime.now(),
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            disk_io_read=disk_io_read,
            disk_io_write=disk_io_write,
            network_io_sent=network_io_sent,
            network_io_recv=network_io_recv,
            load_average=load_average,
            active_connections=active_connections,
            response_time=response_time,
            throughput=throughput,
            error_rate=error_rate
        )
    
    def save_metrics(self, metrics: SystemMetrics):
        """保存系统指标"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO system_metrics 
                (timestamp, cpu_usage, memory_usage, disk_io_read, disk_io_write,
                 network_io_sent, network_io_recv, load_average, active_connections,
                 response_time, throughput, error_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                metrics.timestamp, metrics.cpu_usage, metrics.memory_usage,
                metrics.disk_io_read, metrics.disk_io_write, metrics.network_io_sent,
                metrics.network_io_recv, metrics.load_average, metrics.active_connections,
                metrics.response_time, metrics.throughput, metrics.error_rate
            ))
            conn.commit()
        
        # 添加到历史记录
        self.performance_history.append(metrics)
        if len(self.performance_history) > self.max_history_size:
            self.performance_history.pop(0)
    
    def evaluate_business_rules(self, metrics: SystemMetrics) -> Optional[str]:
        """评估业务规则"""
        current_hour = datetime.now().hour
        
        # 按优先级排序规则
        sorted_rules = sorted(self.business_rules.values(), key=lambda r: r.priority)
        
        for rule in sorted_rules:
            if not rule.enabled:
                continue
            
            triggered = False
            
            if rule.condition == "time_based":
                if "hours" in rule.trigger_condition:
                    if current_hour in rule.trigger_condition["hours"]:
                        triggered = True
            
            elif rule.condition == "load_based":
                if "cpu_usage" in rule.trigger_condition:
                    cpu_config = rule.trigger_condition["cpu_usage"]
                    if metrics.cpu_usage > cpu_config["threshold"]:
                        # 检查持续时间（简化实现）
                        triggered = True
                
                if "error_rate" in rule.trigger_condition:
                    error_config = rule.trigger_condition["error_rate"]
                    if metrics.error_rate > error_config["threshold"]:
                        triggered = True
            
            if triggered:
                logger.info(f"业务规则触发: {rule.rule_name}")
                return rule.rule_id
        
        return None
    
    def get_current_config(self, service_name: str) -> Optional[ConcurrencyConfig]:
        """获取当前配置"""
        if service_name in self.current_configs:
            return self.current_configs[service_name]
        
        # 从数据库加载
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute("""
                SELECT * FROM concurrency_configs 
                WHERE service_name = ? AND is_active = 1
                ORDER BY updated_at DESC LIMIT 1
            """, (service_name,)).fetchone()
            
            if result:
                config = ConcurrencyConfig(
                    config_id=result[0],
                    service_name=result[1],
                    max_workers=result[2],
                    queue_size=result[3],
                    timeout_seconds=result[4],
                    batch_size=result[5],
                    retry_attempts=result[6],
                    connection_pool_size=result[7],
                    thread_pool_size=result[8],
                    process_pool_size=result[9],
                    rate_limit_per_second=result[10],
                    circuit_breaker_threshold=result[11],
                    created_at=datetime.fromisoformat(result[12]),
                    updated_at=datetime.fromisoformat(result[13]),
                    is_active=bool(result[14])
                )
                self.current_configs[service_name] = config
                return config
        
        # 返回默认配置
        default_config = ConcurrencyConfig(
            config_id=f"default_{service_name}",
            service_name=service_name,
            max_workers=10,
            queue_size=100,
            timeout_seconds=30,
            batch_size=10,
            retry_attempts=3,
            connection_pool_size=20,
            thread_pool_size=10,
            process_pool_size=4,
            rate_limit_per_second=100.0,
            circuit_breaker_threshold=10,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            is_active=True
        )
        
        self.save_config(default_config)
        return default_config
    
    def save_config(self, config: ConcurrencyConfig):
        """保存配置"""
        self.current_configs[config.service_name] = config
        
        with sqlite3.connect(self.db_path) as conn:
            # 先将旧配置设为非活跃
            conn.execute(
                "UPDATE concurrency_configs SET is_active = 0 WHERE service_name = ?",
                (config.service_name,)
            )
            
            # 插入新配置
            conn.execute("""
                INSERT INTO concurrency_configs 
                (config_id, service_name, max_workers, queue_size, timeout_seconds,
                 batch_size, retry_attempts, connection_pool_size, thread_pool_size,
                 process_pool_size, rate_limit_per_second, circuit_breaker_threshold,
                 created_at, updated_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                config.config_id, config.service_name, config.max_workers, config.queue_size,
                config.timeout_seconds, config.batch_size, config.retry_attempts,
                config.connection_pool_size, config.thread_pool_size, config.process_pool_size,
                config.rate_limit_per_second, config.circuit_breaker_threshold,
                config.created_at, config.updated_at, config.is_active
            ))
            conn.commit()
    
    def _optimize_for_throughput(self, current_config: ConcurrencyConfig, metrics: SystemMetrics, profile: PerformanceProfile) -> ConcurrencyConfig:
        """吞吐量优化策略"""
        new_config = ConcurrencyConfig(
            config_id=f"opt_throughput_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            service_name=current_config.service_name,
            max_workers=min(current_config.max_workers + 2, 50),
            queue_size=max(current_config.queue_size * 2, 1000),
            timeout_seconds=max(current_config.timeout_seconds + 10, 60),
            batch_size=min(current_config.batch_size + 5, 100),
            retry_attempts=current_config.retry_attempts,
            connection_pool_size=min(current_config.connection_pool_size + 5, 100),
            thread_pool_size=min(current_config.thread_pool_size + 2, 20),
            process_pool_size=current_config.process_pool_size,
            rate_limit_per_second=current_config.rate_limit_per_second * 1.2,
            circuit_breaker_threshold=current_config.circuit_breaker_threshold + 2,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            is_active=True
        )
        return new_config
    
    def _optimize_for_latency(self, current_config: ConcurrencyConfig, metrics: SystemMetrics, profile: PerformanceProfile) -> ConcurrencyConfig:
        """延迟优化策略"""
        new_config = ConcurrencyConfig(
            config_id=f"opt_latency_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            service_name=current_config.service_name,
            max_workers=max(current_config.max_workers - 1, 5),
            queue_size=max(current_config.queue_size // 2, 50),
            timeout_seconds=max(current_config.timeout_seconds - 5, 10),
            batch_size=max(current_config.batch_size - 2, 5),
            retry_attempts=min(current_config.retry_attempts + 1, 5),
            connection_pool_size=current_config.connection_pool_size,
            thread_pool_size=max(current_config.thread_pool_size - 1, 5),
            process_pool_size=current_config.process_pool_size,
            rate_limit_per_second=current_config.rate_limit_per_second * 0.8,
            circuit_breaker_threshold=max(current_config.circuit_breaker_threshold - 1, 5),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            is_active=True
        )
        return new_config
    
    def _optimize_balanced(self, current_config: ConcurrencyConfig, metrics: SystemMetrics, profile: PerformanceProfile) -> ConcurrencyConfig:
        """平衡优化策略"""
        # 根据当前性能调整
        worker_adjustment = 0
        if metrics.response_time > profile.target_response_time:
            worker_adjustment = -1
        elif metrics.throughput < profile.target_throughput:
            worker_adjustment = 1
        
        new_config = ConcurrencyConfig(
            config_id=f"opt_balanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            service_name=current_config.service_name,
            max_workers=max(min(current_config.max_workers + worker_adjustment, 30), 5),
            queue_size=current_config.queue_size,
            timeout_seconds=current_config.timeout_seconds,
            batch_size=current_config.batch_size,
            retry_attempts=current_config.retry_attempts,
            connection_pool_size=current_config.connection_pool_size,
            thread_pool_size=current_config.thread_pool_size,
            process_pool_size=current_config.process_pool_size,
            rate_limit_per_second=current_config.rate_limit_per_second,
            circuit_breaker_threshold=current_config.circuit_breaker_threshold,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            is_active=True
        )
        return new_config
    
    def _optimize_resource_conservative(self, current_config: ConcurrencyConfig, metrics: SystemMetrics, profile: PerformanceProfile) -> ConcurrencyConfig:
        """资源保守优化策略"""
        new_config = ConcurrencyConfig(
            config_id=f"opt_conservative_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            service_name=current_config.service_name,
            max_workers=max(current_config.max_workers - 2, 3),
            queue_size=max(current_config.queue_size // 2, 20),
            timeout_seconds=max(current_config.timeout_seconds - 10, 15),
            batch_size=max(current_config.batch_size - 3, 3),
            retry_attempts=max(current_config.retry_attempts - 1, 2),
            connection_pool_size=max(current_config.connection_pool_size - 5, 10),
            thread_pool_size=max(current_config.thread_pool_size - 2, 3),
            process_pool_size=max(current_config.process_pool_size - 1, 2),
            rate_limit_per_second=current_config.rate_limit_per_second * 0.7,
            circuit_breaker_threshold=max(current_config.circuit_breaker_threshold - 2, 3),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            is_active=True
        )
        return new_config
    
    def optimize_concurrency(self, service_name: str, metrics: SystemMetrics) -> Optional[OptimizationResult]:
        """优化并发参数"""
        # 获取当前配置
        current_config = self.get_current_config(service_name)
        if not current_config:
            logger.error(f"无法获取服务 {service_name} 的当前配置")
            return None
        
        # 评估业务规则
        triggered_rule = self.evaluate_business_rules(metrics)
        
        # 选择性能配置文件
        profile_name = "balanced"  # 默认
        if triggered_rule and triggered_rule in self.business_rules:
            rule = self.business_rules[triggered_rule]
            if "profile" in rule.target_config:
                profile_name = rule.target_config["profile"]
        
        profile = self.performance_profiles.get(profile_name)
        if not profile:
            logger.error(f"未找到性能配置文件: {profile_name}")
            return None
        
        # 应用优化策略
        strategy_func = self.optimization_strategies.get(profile.optimization_strategy)
        if not strategy_func:
            logger.error(f"未找到优化策略: {profile.optimization_strategy}")
            return None
        
        new_config = strategy_func(current_config, metrics, profile)
        
        # 应用业务规则的配置调整
        if triggered_rule and triggered_rule in self.business_rules:
            rule = self.business_rules[triggered_rule]
            if "max_workers_multiplier" in rule.target_config:
                multiplier = rule.target_config["max_workers_multiplier"]
                new_config.max_workers = int(new_config.max_workers * multiplier)
                new_config.max_workers = max(new_config.max_workers, 1)
        
        # 计算性能改进预期
        performance_improvement = self._calculate_performance_improvement(current_config, new_config, metrics, profile)
        
        # 计算资源效率
        resource_efficiency = self._calculate_resource_efficiency(new_config, metrics)
        
        # 生成推荐原因
        recommendation_reason = self._generate_recommendation_reason(current_config, new_config, metrics, profile, triggered_rule)
        
        # 计算置信度
        confidence_score = self._calculate_confidence_score(metrics, profile)
        
        optimization_result = OptimizationResult(
            optimization_id=f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}",
            timestamp=datetime.now(),
            service_name=service_name,
            old_config=current_config,
            new_config=new_config,
            performance_improvement=performance_improvement,
            resource_efficiency=resource_efficiency,
            recommendation_reason=recommendation_reason,
            confidence_score=confidence_score,
            applied=False
        )
        
        # 保存优化结果
        self._save_optimization_result(optimization_result)
        
        return optimization_result
    
    def _calculate_performance_improvement(self, old_config: ConcurrencyConfig, new_config: ConcurrencyConfig, metrics: SystemMetrics, profile: PerformanceProfile) -> float:
        """计算性能改进预期"""
        # 简化的性能改进计算
        worker_ratio = new_config.max_workers / old_config.max_workers
        queue_ratio = new_config.queue_size / old_config.queue_size
        
        # 基于策略的改进预期
        if profile.optimization_strategy == "throughput":
            improvement = (worker_ratio - 1) * 0.3 + (queue_ratio - 1) * 0.2
        elif profile.optimization_strategy == "latency":
            improvement = (1 - worker_ratio) * 0.2 + (1 - queue_ratio) * 0.1
        else:
            improvement = abs(worker_ratio - 1) * 0.1
        
        return max(min(improvement, 1.0), -0.5)
    
    def _calculate_resource_efficiency(self, config: ConcurrencyConfig, metrics: SystemMetrics) -> float:
        """计算资源效率"""
        # 基于当前资源使用情况计算效率
        cpu_efficiency = 1.0 - (metrics.cpu_usage / 100.0)
        memory_efficiency = 1.0 - (metrics.memory_usage / 100.0)
        
        # 配置复杂度惩罚
        complexity_penalty = (config.max_workers + config.queue_size / 100) / 100
        
        efficiency = (cpu_efficiency + memory_efficiency) / 2 - complexity_penalty
        return max(min(efficiency, 1.0), 0.0)
    
    def _generate_recommendation_reason(self, old_config: ConcurrencyConfig, new_config: ConcurrencyConfig, metrics: SystemMetrics, profile: PerformanceProfile, triggered_rule: Optional[str]) -> str:
        """生成推荐原因"""
        reasons = []
        
        if triggered_rule:
            rule = self.business_rules.get(triggered_rule)
            if rule:
                reasons.append(f"触发业务规则: {rule.rule_name}")
        
        if new_config.max_workers > old_config.max_workers:
            reasons.append(f"增加工作线程数 ({old_config.max_workers} -> {new_config.max_workers}) 以提高吞吐量")
        elif new_config.max_workers < old_config.max_workers:
            reasons.append(f"减少工作线程数 ({old_config.max_workers} -> {new_config.max_workers}) 以降低资源消耗")
        
        if metrics.cpu_usage > 80:
            reasons.append("CPU使用率过高，需要优化资源配置")
        
        if metrics.response_time > profile.target_response_time:
            reasons.append(f"响应时间 ({metrics.response_time:.1f}ms) 超过目标 ({profile.target_response_time:.1f}ms)")
        
        if metrics.error_rate > profile.max_error_rate:
            reasons.append(f"错误率 ({metrics.error_rate:.2f}%) 超过阈值 ({profile.max_error_rate:.2f}%)")
        
        return "; ".join(reasons) if reasons else "基于当前性能指标的常规优化"
    
    def _calculate_confidence_score(self, metrics: SystemMetrics, profile: PerformanceProfile) -> float:
        """计算置信度分数"""
        # 基于历史数据的置信度计算
        if len(self.performance_history) < 5:
            return 0.5  # 数据不足时的默认置信度
        
        # 计算指标稳定性
        recent_metrics = self.performance_history[-5:]
        cpu_variance = statistics.variance([m.cpu_usage for m in recent_metrics])
        response_time_variance = statistics.variance([m.response_time for m in recent_metrics])
        
        # 稳定性越高，置信度越高
        stability_score = 1.0 / (1.0 + cpu_variance / 100 + response_time_variance / 1000)
        
        # 与目标的偏差
        cpu_deviation = abs(metrics.cpu_usage - profile.target_cpu_usage) / profile.target_cpu_usage
        response_deviation = abs(metrics.response_time - profile.target_response_time) / profile.target_response_time
        
        deviation_score = 1.0 / (1.0 + cpu_deviation + response_deviation)
        
        confidence = (stability_score + deviation_score) / 2
        return max(min(confidence, 1.0), 0.1)
    
    def _save_optimization_result(self, result: OptimizationResult):
        """保存优化结果"""
        # 转换datetime对象为字符串
        def convert_datetime(obj):
            if isinstance(obj, dict):
                return {k: convert_datetime(v) for k, v in obj.items()}
            elif isinstance(obj, datetime):
                return obj.isoformat()
            else:
                return obj
        
        old_config_dict = convert_datetime(asdict(result.old_config))
        new_config_dict = convert_datetime(asdict(result.new_config))
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO optimization_results 
                (optimization_id, timestamp, service_name, old_config, new_config,
                 performance_improvement, resource_efficiency, recommendation_reason,
                 confidence_score, applied)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result.optimization_id, result.timestamp, result.service_name,
                json.dumps(old_config_dict), json.dumps(new_config_dict),
                result.performance_improvement, result.resource_efficiency,
                result.recommendation_reason, result.confidence_score, result.applied
            ))
            conn.commit()
    
    def apply_optimization(self, optimization_id: str) -> bool:
        """应用优化建议"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                result_data = conn.execute(
                    "SELECT * FROM optimization_results WHERE optimization_id = ?",
                    (optimization_id,)
                ).fetchone()
                
                if not result_data:
                    logger.error(f"未找到优化结果: {optimization_id}")
                    return False
                
                new_config_data = json.loads(result_data[4])
                new_config = ConcurrencyConfig(**new_config_data)
                
                # 保存新配置
                self.save_config(new_config)
                
                # 标记为已应用
                conn.execute(
                    "UPDATE optimization_results SET applied = 1 WHERE optimization_id = ?",
                    (optimization_id,)
                )
                conn.commit()
                
                logger.info(f"已应用优化配置: {optimization_id}")
                return True
        
        except Exception as e:
            logger.error(f"应用优化配置时出错: {e}")
            return False
    
    def start_optimization(self):
        """启动自动优化"""
        if self.optimization_active:
            logger.warning("并发优化已经在运行中")
            return
        
        self.optimization_active = True
        self.optimizer_thread = threading.Thread(target=self._optimization_loop, daemon=True)
        self.optimizer_thread.start()
        logger.info("并发参数自动优化已启动")
    
    def stop_optimization(self):
        """停止优化"""
        self.optimization_active = False
        if self.optimizer_thread:
            self.optimizer_thread.join(timeout=5)
        logger.info("并发参数自动优化已停止")
    
    def _optimization_loop(self):
        """优化循环"""
        services = ["lottery_api", "data_validator", "deduplication_service"]  # 示例服务
        
        while self.optimization_active:
            try:
                # 收集系统指标
                metrics = self.collect_system_metrics()
                self.save_metrics(metrics)
                
                logger.info(f"系统指标 - CPU: {metrics.cpu_usage:.1f}%, 内存: {metrics.memory_usage:.1f}%, 响应时间: {metrics.response_time:.1f}ms")
                
                # 为每个服务优化配置
                for service_name in services:
                    optimization_result = self.optimize_concurrency(service_name, metrics)
                    
                    if optimization_result:
                        logger.info(f"服务 {service_name} 优化建议:")
                        logger.info(f"  - 性能改进预期: {optimization_result.performance_improvement:.2%}")
                        logger.info(f"  - 资源效率: {optimization_result.resource_efficiency:.2%}")
                        logger.info(f"  - 置信度: {optimization_result.confidence_score:.2%}")
                        logger.info(f"  - 原因: {optimization_result.recommendation_reason}")
                        
                        # 自动应用高置信度的优化
                        if optimization_result.confidence_score > 0.7 and optimization_result.performance_improvement > 0.1:
                            logger.info(f"自动应用优化: {optimization_result.optimization_id}")
                            self.apply_optimization(optimization_result.optimization_id)
            
            except Exception as e:
                logger.error(f"优化过程中出错: {e}")
            
            # 等待下次优化
            for _ in range(self.check_interval):
                if not self.optimization_active:
                    break
                time.sleep(1)
    
    def get_optimization_history(self, service_name: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """获取优化历史"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if service_name:
                    results = conn.execute("""
                        SELECT optimization_id, timestamp, service_name, performance_improvement,
                               resource_efficiency, recommendation_reason, confidence_score, applied
                        FROM optimization_results 
                        WHERE service_name = ?
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    """, (service_name, limit)).fetchall()
                else:
                    results = conn.execute("""
                        SELECT optimization_id, timestamp, service_name, performance_improvement,
                               resource_efficiency, recommendation_reason, confidence_score, applied
                        FROM optimization_results 
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    """, (limit,)).fetchall()
                
                return [
                    {
                        "optimization_id": result[0],
                        "timestamp": result[1],
                        "service_name": result[2],
                        "performance_improvement": result[3],
                        "resource_efficiency": result[4],
                        "recommendation_reason": result[5],
                        "confidence_score": result[6],
                        "applied": bool(result[7])
                    } for result in results
                ]
        
        except Exception as e:
            logger.error(f"获取优化历史时出错: {e}")
            return []
    
    def cleanup_old_data(self, days: int = 30):
        """清理旧数据"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM system_metrics WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM optimization_results WHERE timestamp < ?", (cutoff_date,))
            conn.execute("UPDATE concurrency_configs SET is_active = 0 WHERE updated_at < ?", (cutoff_date,))
            conn.commit()
        
        logger.info(f"已清理 {days} 天前的旧数据")

def main():
    """测试并发参数动态调整系统"""
    print("=== 并发参数动态调整系统测试 ===")
    
    # 创建优化器实例
    optimizer = ConcurrencyOptimizer()
    
    # 收集系统指标
    print("\n收集系统指标...")
    metrics = optimizer.collect_system_metrics()
    optimizer.save_metrics(metrics)
    
    print(f"CPU使用率: {metrics.cpu_usage:.1f}%")
    print(f"内存使用率: {metrics.memory_usage:.1f}%")
    print(f"响应时间: {metrics.response_time:.1f}ms")
    print(f"吞吐量: {metrics.throughput:.1f} req/s")
    print(f"错误率: {metrics.error_rate:.2f}%")
    
    # 测试优化建议
    print("\n生成优化建议...")
    services = ["lottery_api", "data_validator", "deduplication_service"]
    
    for service_name in services:
        print(f"\n--- 服务: {service_name} ---")
        
        # 获取当前配置
        current_config = optimizer.get_current_config(service_name)
        print(f"当前配置: 工作线程={current_config.max_workers}, 队列大小={current_config.queue_size}")
        
        # 生成优化建议
        optimization_result = optimizer.optimize_concurrency(service_name, metrics)
        
        if optimization_result:
            print(f"优化建议:")
            print(f"  - 新工作线程数: {optimization_result.new_config.max_workers}")
            print(f"  - 新队列大小: {optimization_result.new_config.queue_size}")
            print(f"  - 性能改进预期: {optimization_result.performance_improvement:.2%}")
            print(f"  - 资源效率: {optimization_result.resource_efficiency:.2%}")
            print(f"  - 置信度: {optimization_result.confidence_score:.2%}")
            print(f"  - 推荐原因: {optimization_result.recommendation_reason}")
            
            # 应用优化（测试）
            if optimization_result.confidence_score > 0.5:
                print(f"  - 应用优化: {optimizer.apply_optimization(optimization_result.optimization_id)}")
    
    # 获取优化历史
    print("\n优化历史:")
    history = optimizer.get_optimization_history(limit=5)
    for record in history:
        print(f"  - {record['timestamp']}: {record['service_name']} - 改进: {record['performance_improvement']:.2%} (已应用: {record['applied']})")
    
    # 启动短期自动优化测试
    print("\n启动自动优化测试 (30秒)...")
    optimizer.check_interval = 10  # 10秒检查一次
    optimizer.start_optimization()
    
    time.sleep(30)
    
    optimizer.stop_optimization()
    
    print("\n=== 并发参数动态调整系统测试完成 ===")

if __name__ == "__main__":
    main()