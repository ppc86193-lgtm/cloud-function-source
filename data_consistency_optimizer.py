#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28数据一致性优化器
确保实时获取与历史回填数据的一致性
符合PROJECT_RULES.md性能监控要求
"""

import json
import time
import hashlib
import threading
import logging
import psutil
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(name)s|%(funcName)s:%(lineno)d|%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DataSource(Enum):
    """数据源枚举"""
    REALTIME = "realtime"      # 实时数据源
    BACKFILL = "backfill"      # 历史回填数据源
    MANUAL = "manual"          # 手动数据源
    CACHE = "cache"            # 缓存数据源

class ConsistencyStatus(Enum):
    """一致性状态枚举"""
    CONSISTENT = "consistent"      # 一致
    INCONSISTENT = "inconsistent"  # 不一致
    MISSING = "missing"           # 缺失
    DUPLICATE = "duplicate"       # 重复
    CORRUPTED = "corrupted"       # 损坏
    PENDING = "pending"           # 待处理

@dataclass
class DataRecord:
    """数据记录"""
    draw_id: str
    draw_time: str
    draw_number: str
    source: DataSource
    timestamp: datetime
    checksum: str = ""
    
    def __post_init__(self):
        """计算数据校验和"""
        if not self.checksum:
            data_str = f"{self.draw_id}|{self.draw_time}|{self.draw_number}"
            self.checksum = hashlib.sha256(data_str.encode()).hexdigest()[:16]

@dataclass
class ConsistencyIssue:
    """一致性问题"""
    issue_id: str
    issue_type: str
    severity: str  # low, medium, high, critical
    description: str
    affected_records: List[str]
    detected_time: datetime
    resolved: bool = False
    resolution_time: Optional[datetime] = None
    resolution_method: str = ""

@dataclass
class ConsistencyMetrics:
    """一致性指标"""
    total_records_checked: int = 0
    consistent_records: int = 0
    inconsistent_records: int = 0
    missing_records: int = 0
    duplicate_records: int = 0
    corrupted_records: int = 0
    
    # 性能指标
    check_duration_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # 修复指标
    issues_detected: int = 0
    issues_resolved: int = 0
    auto_fix_success_rate: float = 0.0
    
    @property
    def consistency_rate(self) -> float:
        """一致性率"""
        if self.total_records_checked == 0:
            return 0.0
        return self.consistent_records / self.total_records_checked

    @property
    def issue_resolution_rate(self) -> float:
        """问题解决率"""
        if self.issues_detected == 0:
            return 0.0
        return self.issues_resolved / self.issues_detected

@dataclass
class PerformanceThreshold:
    """性能阈值配置"""
    max_memory_usage_mb: int = 256
    max_cpu_usage_percent: float = 70.0
    max_check_duration_seconds: float = 300.0
    max_batch_size: int = 1000
    performance_check_interval: int = 30

class DataConsistencyOptimizer:
    """数据一致性优化器 - 符合PROJECT_RULES.md要求"""
    
    def __init__(self, realtime_source, backfill_source, config: PerformanceThreshold = None):
        self.realtime_source = realtime_source
        self.backfill_source = backfill_source
        self.config = config or PerformanceThreshold()
        
        # 状态管理
        self.is_running = False
        self.start_time = datetime.now()
        
        # 数据存储
        self.realtime_data: Dict[str, DataRecord] = {}
        self.backfill_data: Dict[str, DataRecord] = {}
        self.consistency_issues: List[ConsistencyIssue] = []
        
        # 性能监控
        self.metrics = ConsistencyMetrics()
        self.performance_alerts = []
        self.last_performance_check = datetime.now()
        
        # 线程管理
        self.monitor_thread = None
        self.consistency_thread = None
        self.performance_thread = None
        self.auto_fix_thread = None
        
        # 缓存管理
        self.data_cache = {}
        self.cache_ttl = 600  # 10分钟TTL
        
        # 回调函数
        self.issue_callbacks = []
        self.resolution_callbacks = []
        self.performance_callbacks = []
        
        # 线程锁
        self.data_lock = threading.RLock()
        self.metrics_lock = threading.RLock()
        
        logger.info("DataConsistencyOptimizer初始化完成")

    def start_monitoring(self) -> bool:
        """启动一致性监控"""
        if self.is_running:
            logger.warning("一致性监控已在运行中")
            return False
        
        try:
            self.is_running = True
            self.start_time = datetime.now()
            
            # 启动监控线程
            self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.consistency_thread = threading.Thread(target=self._consistency_check_loop, daemon=True)
            self.performance_thread = threading.Thread(target=self._performance_monitor_loop, daemon=True)
            self.auto_fix_thread = threading.Thread(target=self._auto_fix_loop, daemon=True)
            
            self.monitor_thread.start()
            self.consistency_thread.start()
            self.performance_thread.start()
            self.auto_fix_thread.start()
            
            logger.info("数据一致性监控启动成功")
            return True
            
        except Exception as e:
            logger.error(f"启动一致性监控失败: {e}")
            self.is_running = False
            return False

    def stop_monitoring(self):
        """停止一致性监控"""
        logger.info("正在停止数据一致性监控...")
        self.is_running = False
        
        # 等待线程结束
        for thread in [self.monitor_thread, self.consistency_thread, 
                      self.performance_thread, self.auto_fix_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("数据一致性监控已停止")

    def _monitoring_loop(self):
        """主监控循环"""
        logger.info("开始数据监控循环")
        
        while self.is_running:
            try:
                start_time = time.time()
                
                # 获取实时数据
                realtime_data = self._fetch_realtime_data()
                if realtime_data:
                    self._update_realtime_cache(realtime_data)
                
                # 获取回填数据
                backfill_data = self._fetch_backfill_data()
                if backfill_data:
                    self._update_backfill_cache(backfill_data)
                
                # 更新性能指标
                duration = time.time() - start_time
                with self.metrics_lock:
                    self.metrics.check_duration_seconds = duration
                
                # 动态调整检查间隔
                interval = self._calculate_check_interval()
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                time.sleep(60)

    def _consistency_check_loop(self):
        """一致性检查循环"""
        logger.info("开始一致性检查循环")
        
        while self.is_running:
            try:
                # 执行一致性检查
                issues = self._perform_consistency_check()
                
                if issues:
                    logger.info(f"发现 {len(issues)} 个一致性问题")
                    self._handle_consistency_issues(issues)
                
                # 每5分钟执行一次完整检查
                time.sleep(300)
                
            except Exception as e:
                logger.error(f"一致性检查异常: {e}")
                time.sleep(300)

    def _performance_monitor_loop(self):
        """性能监控循环"""
        logger.info("开始性能监控循环")
        
        while self.is_running:
            try:
                self._check_performance_metrics()
                time.sleep(self.config.performance_check_interval)
                
            except Exception as e:
                logger.error(f"性能监控异常: {e}")
                time.sleep(60)

    def _auto_fix_loop(self):
        """自动修复循环"""
        logger.info("开始自动修复循环")
        
        while self.is_running:
            try:
                # 处理待修复的问题
                unresolved_issues = [
                    issue for issue in self.consistency_issues 
                    if not issue.resolved
                ]
                
                for issue in unresolved_issues:
                    if self._can_auto_fix(issue):
                        success = self._attempt_auto_fix(issue)
                        if success:
                            logger.info(f"自动修复成功: {issue.issue_id}")
                        else:
                            logger.warning(f"自动修复失败: {issue.issue_id}")
                
                # 每2分钟尝试一次自动修复
                time.sleep(120)
                
            except Exception as e:
                logger.error(f"自动修复异常: {e}")
                time.sleep(120)

    def _check_performance_metrics(self):
        """检查性能指标"""
        try:
            # 获取系统资源使用情况
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            cpu_percent = process.cpu_percent()
            
            # 更新性能指标
            with self.metrics_lock:
                self.metrics.memory_usage_mb = memory_mb
                self.metrics.cpu_usage_percent = cpu_percent
            
            # 检查性能阈值
            alerts = []
            
            if memory_mb > self.config.max_memory_usage_mb:
                alerts.append({
                    "type": "memory_usage",
                    "severity": "high",
                    "message": f"内存使用过高: {memory_mb:.1f}MB > {self.config.max_memory_usage_mb}MB",
                    "value": memory_mb,
                    "threshold": self.config.max_memory_usage_mb
                })
            
            if cpu_percent > self.config.max_cpu_usage_percent:
                alerts.append({
                    "type": "cpu_usage",
                    "severity": "high",
                    "message": f"CPU使用率过高: {cpu_percent:.1f}% > {self.config.max_cpu_usage_percent}%",
                    "value": cpu_percent,
                    "threshold": self.config.max_cpu_usage_percent
                })
            
            if self.metrics.check_duration_seconds > self.config.max_check_duration_seconds:
                alerts.append({
                    "type": "check_duration",
                    "severity": "medium",
                    "message": f"检查耗时过长: {self.metrics.check_duration_seconds:.1f}s > {self.config.max_check_duration_seconds}s",
                    "value": self.metrics.check_duration_seconds,
                    "threshold": self.config.max_check_duration_seconds
                })
            
            # 处理性能告警
            for alert in alerts:
                self._handle_performance_alert(alert)
                
            # 自动优化措施
            if memory_mb > self.config.max_memory_usage_mb * 0.8:
                self._cleanup_memory()
                
        except Exception as e:
            logger.error(f"性能检查失败: {e}")

    def _handle_performance_alert(self, alert: Dict[str, Any]):
        """处理性能告警"""
        self.performance_alerts.append({
            **alert,
            "timestamp": datetime.now().isoformat()
        })
        
        # 保持最近50个告警
        if len(self.performance_alerts) > 50:
            self.performance_alerts = self.performance_alerts[-50:]
        
        logger.warning(f"性能告警: {alert['message']}")
        
        # 触发告警回调
        for callback in self.performance_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"性能告警回调失败: {e}")

    def _cleanup_memory(self):
        """清理内存"""
        try:
            # 清理过期缓存
            current_time = time.time()
            expired_keys = [
                key for key, (data, timestamp) in self.data_cache.items()
                if current_time - timestamp > self.cache_ttl
            ]
            
            for key in expired_keys:
                del self.data_cache[key]
            
            # 清理旧的一致性问题记录
            cutoff_time = datetime.now() - timedelta(hours=24)
            self.consistency_issues = [
                issue for issue in self.consistency_issues
                if issue.detected_time > cutoff_time or not issue.resolved
            ]
            
            # 强制垃圾回收
            gc.collect()
            
            logger.info(f"内存清理完成，清理了{len(expired_keys)}个缓存项")
            
        except Exception as e:
            logger.error(f"内存清理失败: {e}")

    def _perform_consistency_check(self) -> List[ConsistencyIssue]:
        """执行一致性检查"""
        issues = []
        
        try:
            with self.data_lock:
                # 检查缺失数据
                missing_issues = self._check_missing_data()
                issues.extend(missing_issues)
                
                # 检查重复数据
                duplicate_issues = self._check_duplicate_data()
                issues.extend(duplicate_issues)
                
                # 检查数据不一致
                inconsistent_issues = self._check_data_inconsistency()
                issues.extend(inconsistent_issues)
                
                # 检查数据损坏
                corrupted_issues = self._check_data_corruption()
                issues.extend(corrupted_issues)
            
            # 更新指标
            with self.metrics_lock:
                self.metrics.issues_detected += len(issues)
                self.metrics.total_records_checked = len(self.realtime_data) + len(self.backfill_data)
            
            return issues
            
        except Exception as e:
            logger.error(f"一致性检查失败: {e}")
            return []

    def _check_missing_data(self) -> List[ConsistencyIssue]:
        """检查缺失数据"""
        issues = []
        
        # 检查实时数据中缺失的回填数据
        realtime_ids = set(self.realtime_data.keys())
        backfill_ids = set(self.backfill_data.keys())
        
        missing_in_backfill = realtime_ids - backfill_ids
        missing_in_realtime = backfill_ids - realtime_ids
        
        if missing_in_backfill:
            issue = ConsistencyIssue(
                issue_id=f"missing_backfill_{int(time.time())}",
                issue_type="missing_data",
                severity="medium",
                description=f"回填数据中缺失 {len(missing_in_backfill)} 条记录",
                affected_records=list(missing_in_backfill),
                detected_time=datetime.now()
            )
            issues.append(issue)
        
        if missing_in_realtime:
            issue = ConsistencyIssue(
                issue_id=f"missing_realtime_{int(time.time())}",
                issue_type="missing_data",
                severity="high",
                description=f"实时数据中缺失 {len(missing_in_realtime)} 条记录",
                affected_records=list(missing_in_realtime),
                detected_time=datetime.now()
            )
            issues.append(issue)
        
        return issues

    def _check_duplicate_data(self) -> List[ConsistencyIssue]:
        """检查重复数据"""
        issues = []
        
        # 检查实时数据中的重复
        realtime_checksums = {}
        for draw_id, record in self.realtime_data.items():
            checksum = record.checksum
            if checksum in realtime_checksums:
                issue = ConsistencyIssue(
                    issue_id=f"duplicate_realtime_{int(time.time())}_{draw_id}",
                    issue_type="duplicate_data",
                    severity="medium",
                    description=f"实时数据中发现重复记录: {draw_id}",
                    affected_records=[draw_id, realtime_checksums[checksum]],
                    detected_time=datetime.now()
                )
                issues.append(issue)
            else:
                realtime_checksums[checksum] = draw_id
        
        return issues

    def _check_data_inconsistency(self) -> List[ConsistencyIssue]:
        """检查数据不一致"""
        issues = []
        
        # 比较相同draw_id的数据
        common_ids = set(self.realtime_data.keys()) & set(self.backfill_data.keys())
        
        for draw_id in common_ids:
            realtime_record = self.realtime_data[draw_id]
            backfill_record = self.backfill_data[draw_id]
            
            if realtime_record.checksum != backfill_record.checksum:
                issue = ConsistencyIssue(
                    issue_id=f"inconsistent_{int(time.time())}_{draw_id}",
                    issue_type="data_inconsistency",
                    severity="high",
                    description=f"数据不一致: {draw_id} (实时: {realtime_record.checksum}, 回填: {backfill_record.checksum})",
                    affected_records=[draw_id],
                    detected_time=datetime.now()
                )
                issues.append(issue)
        
        return issues

    def _check_data_corruption(self) -> List[ConsistencyIssue]:
        """检查数据损坏"""
        issues = []
        
        # 检查数据完整性
        for source_name, data_dict in [("realtime", self.realtime_data), ("backfill", self.backfill_data)]:
            for draw_id, record in data_dict.items():
                # 重新计算校验和验证数据完整性
                expected_checksum = hashlib.sha256(
                    f"{record.draw_id}|{record.draw_time}|{record.draw_number}".encode()
                ).hexdigest()[:16]
                
                if record.checksum != expected_checksum:
                    issue = ConsistencyIssue(
                        issue_id=f"corrupted_{source_name}_{int(time.time())}_{draw_id}",
                        issue_type="data_corruption",
                        severity="critical",
                        description=f"{source_name}数据损坏: {draw_id}",
                        affected_records=[draw_id],
                        detected_time=datetime.now()
                    )
                    issues.append(issue)
        
        return issues

    def _can_auto_fix(self, issue: ConsistencyIssue) -> bool:
        """判断是否可以自动修复"""
        auto_fixable_types = ["missing_data", "duplicate_data"]
        return issue.issue_type in auto_fixable_types and issue.severity != "critical"

    def _attempt_auto_fix(self, issue: ConsistencyIssue) -> bool:
        """尝试自动修复"""
        try:
            if issue.issue_type == "missing_data":
                return self._fix_missing_data(issue)
            elif issue.issue_type == "duplicate_data":
                return self._fix_duplicate_data(issue)
            else:
                return False
                
        except Exception as e:
            logger.error(f"自动修复失败: {e}")
            return False

    def _fix_missing_data(self, issue: ConsistencyIssue) -> bool:
        """修复缺失数据"""
        try:
            # 从另一个数据源补充缺失数据
            for draw_id in issue.affected_records:
                if draw_id in self.realtime_data and draw_id not in self.backfill_data:
                    # 从实时数据补充到回填数据
                    self.backfill_data[draw_id] = self.realtime_data[draw_id]
                elif draw_id in self.backfill_data and draw_id not in self.realtime_data:
                    # 从回填数据补充到实时数据
                    self.realtime_data[draw_id] = self.backfill_data[draw_id]
            
            # 标记问题已解决
            issue.resolved = True
            issue.resolution_time = datetime.now()
            issue.resolution_method = "auto_fix_missing_data"
            
            with self.metrics_lock:
                self.metrics.issues_resolved += 1
            
            return True
            
        except Exception as e:
            logger.error(f"修复缺失数据失败: {e}")
            return False

    def _fix_duplicate_data(self, issue: ConsistencyIssue) -> bool:
        """修复重复数据"""
        try:
            # 保留最新的记录，删除重复的
            if len(issue.affected_records) >= 2:
                records_to_check = []
                for draw_id in issue.affected_records:
                    if draw_id in self.realtime_data:
                        records_to_check.append((draw_id, self.realtime_data[draw_id]))
                
                if len(records_to_check) >= 2:
                    # 按时间戳排序，保留最新的
                    records_to_check.sort(key=lambda x: x[1].timestamp, reverse=True)
                    
                    # 删除重复记录
                    for draw_id, _ in records_to_check[1:]:
                        if draw_id in self.realtime_data:
                            del self.realtime_data[draw_id]
            
            # 标记问题已解决
            issue.resolved = True
            issue.resolution_time = datetime.now()
            issue.resolution_method = "auto_fix_duplicate_data"
            
            with self.metrics_lock:
                self.metrics.issues_resolved += 1
            
            return True
            
        except Exception as e:
            logger.error(f"修复重复数据失败: {e}")
            return False

    def get_consistency_report(self) -> Dict[str, Any]:
        """获取一致性报告"""
        with self.metrics_lock:
            return {
                "metrics": asdict(self.metrics),
                "uptime": str(datetime.now() - self.start_time),
                "active_issues": len([issue for issue in self.consistency_issues if not issue.resolved]),
                "resolved_issues": len([issue for issue in self.consistency_issues if issue.resolved]),
                "recent_issues": [
                    {
                        "id": issue.issue_id,
                        "type": issue.issue_type,
                        "severity": issue.severity,
                        "description": issue.description,
                        "detected_time": issue.detected_time.isoformat(),
                        "resolved": issue.resolved,
                        "resolution_time": issue.resolution_time.isoformat() if issue.resolution_time else None
                    }
                    for issue in self.consistency_issues[-10:]  # 最近10个问题
                ],
                "performance_alerts": self.performance_alerts[-5:],  # 最近5个性能告警
                "cache_stats": {
                    "realtime_records": len(self.realtime_data),
                    "backfill_records": len(self.backfill_data),
                    "cache_size": len(self.data_cache)
                }
            }

    def add_issue_callback(self, callback):
        """添加问题回调"""
        self.issue_callbacks.append(callback)

    def add_resolution_callback(self, callback):
        """添加解决回调"""
        self.resolution_callbacks.append(callback)

    def add_performance_callback(self, callback):
        """添加性能回调"""
        self.performance_callbacks.append(callback)