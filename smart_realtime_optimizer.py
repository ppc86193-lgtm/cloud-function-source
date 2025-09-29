#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28智能实时开奖优化器
基于下期开奖时间实现智能轮询和预测性获取
符合PROJECT_RULES.md性能监控要求
"""

import json
import time
import threading
import logging
import psutil
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(name)s|%(funcName)s:%(lineno)d|%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class PollingMode(Enum):
    """轮询模式枚举"""
    IDLE = "idle"              # 空闲模式：300秒间隔
    NORMAL = "normal"          # 正常模式：60秒间隔
    APPROACHING = "approaching" # 临近模式：30秒间隔
    CRITICAL = "critical"      # 关键模式：5秒间隔
    IMMEDIATE = "immediate"    # 即时模式：1秒间隔

@dataclass
class PollingConfig:
    """轮询配置"""
    idle_interval: int = 300       # 空闲间隔（秒）
    normal_interval: int = 60      # 正常间隔（秒）
    approaching_interval: int = 30 # 临近间隔（秒）
    critical_interval: int = 5     # 关键间隔（秒）
    immediate_interval: int = 1    # 即时间隔（秒）
    
    # 阈值设置
    idle_threshold: int = 600      # 空闲阈值（10分钟）
    approaching_threshold: int = 600 # 临近阈值（10分钟）
    critical_threshold: int = 120  # 关键阈值（2分钟）
    immediate_threshold: int = 30  # 即时阈值（30秒）
    
    # 性能监控配置
    max_memory_usage: int = 512    # 最大内存使用（MB）
    max_cpu_usage: float = 80.0    # 最大CPU使用率（%）
    performance_check_interval: int = 60  # 性能检查间隔（秒）

@dataclass
class DrawPrediction:
    """开奖预测信息"""
    next_draw_id: str
    next_draw_time: str
    countdown_seconds: int
    estimated_draw_time: datetime
    confidence_level: float  # 预测置信度 0-1
    prediction_accuracy: float = 0.0  # 预测准确率
    
@dataclass
class OptimizationMetrics:
    """优化指标"""
    total_requests: int = 0
    successful_predictions: int = 0
    failed_predictions: int = 0
    average_response_time: float = 0.0
    cache_hit_rate: float = 0.0
    data_freshness_score: float = 0.0
    polling_efficiency: float = 0.0
    
    # 性能监控指标
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    network_latency_ms: float = 0.0
    error_rate: float = 0.0
    uptime_seconds: int = 0

@dataclass
class PerformanceAlert:
    """性能告警"""
    alert_type: str
    severity: str  # low, medium, high, critical
    message: str
    timestamp: datetime
    metric_value: float
    threshold: float

class SmartRealtimeOptimizer:
    """智能实时优化器 - 符合PROJECT_RULES.md要求"""
    
    def __init__(self, api_system, config: PollingConfig = None):
        self.api_system = api_system
        self.config = config or PollingConfig()
        
        # 状态管理
        self.is_running = False
        self.current_mode = PollingMode.NORMAL
        self.current_prediction = None
        self.last_prediction = None  # 添加缺失的last_prediction属性
        self.last_draw_data = None
        
        # 缓存管理
        self.data_cache = {}
        self.prediction_cache = {}
        self.cache_ttl = 300  # 5分钟TTL
        
        # 性能监控
        self.start_time = datetime.now()
        self.metrics = OptimizationMetrics()
        self.performance_alerts = []
        self.last_performance_check = datetime.now()
        self.metrics_lock = threading.Lock()  # 添加缺失的metrics_lock
        
        # 线程管理
        self.optimization_thread = None
        self.prediction_thread = None
        self.cache_cleanup_thread = None
        self.performance_monitor_thread = None
        
        # 回调函数
        self.new_draw_callbacks = []
        self.prediction_callbacks = []
        self.performance_alert_callbacks = []
        
        logger.info("SmartRealtimeOptimizer初始化完成")

    def start_optimization(self) -> bool:
        """启动优化器"""
        if self.is_running:
            logger.warning("优化器已在运行中")
            return False
        
        try:
            self.is_running = True
            self.start_time = datetime.now()
            
            # 启动各个监控线程
            self.optimization_thread = threading.Thread(target=self._optimization_loop, daemon=True)
            # 暂时注释掉不存在的方法
            # self.prediction_thread = threading.Thread(target=self._prediction_loop, daemon=True)
            # self.cache_cleanup_thread = threading.Thread(target=self._cache_cleanup_loop, daemon=True)
            self.performance_monitor_thread = threading.Thread(target=self._performance_monitor_loop, daemon=True)
            
            self.optimization_thread.start()
            # 暂时注释掉不存在的方法
            # self.prediction_thread.start()
            # self.cache_cleanup_thread.start()
            self.performance_monitor_thread.start()
            
            logger.info("智能实时优化器启动成功")
            return True
            
        except Exception as e:
            logger.error(f"启动优化器失败: {e}")
            self.is_running = False
            return False

    def stop_optimization(self):
        """停止优化器"""
        logger.info("正在停止智能实时优化器...")
        self.is_running = False
        
        # 等待线程结束
        for thread in [self.optimization_thread, self.prediction_thread, 
                      self.cache_cleanup_thread, self.performance_monitor_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("智能实时优化器已停止")

    def _optimization_loop(self):
        """主优化循环"""
        logger.info("开始优化循环")
        
        while self.is_running:
            try:
                start_time = time.time()
                
                # 获取优化数据 - 使用实际存在的API方法
                data = self.api_system.get_current_lottery_data()
                
                if data:
                    self._process_new_data(data)
                
                # 更新性能指标
                response_time = time.time() - start_time
                self._update_metrics(response_time)
                
                # 动态调整轮询间隔
                interval = self._determine_polling_interval()
                
                logger.debug(f"当前模式: {self.current_mode.value}, 间隔: {interval}秒")
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"优化循环异常: {e}")
                self.metrics.error_rate += 1
                time.sleep(self.config.normal_interval)

    def _performance_monitor_loop(self):
        """性能监控循环"""
        logger.info("开始性能监控循环")
        
        while self.is_running:
            try:
                self._check_system_performance()
                time.sleep(self.config.performance_check_interval)
                
            except Exception as e:
                logger.error(f"性能监控异常: {e}")
                time.sleep(60)

    def _check_system_performance(self):
        """检查系统性能"""
        try:
            # 获取系统资源使用情况
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            cpu_percent = process.cpu_percent()
            
            # 更新性能指标
            self.metrics.memory_usage_mb = memory_mb
            self.metrics.cpu_usage_percent = cpu_percent
            self.metrics.uptime_seconds = int((datetime.now() - self.start_time).total_seconds())
            
            # 检查性能阈值
            alerts = []
            
            if memory_mb > self.config.max_memory_usage:
                alerts.append(PerformanceAlert(
                    alert_type="memory_usage",
                    severity="high",
                    message=f"内存使用过高: {memory_mb:.1f}MB > {self.config.max_memory_usage}MB",
                    timestamp=datetime.now(),
                    metric_value=memory_mb,
                    threshold=self.config.max_memory_usage
                ))
            
            if cpu_percent > self.config.max_cpu_usage:
                alerts.append(PerformanceAlert(
                    alert_type="cpu_usage",
                    severity="high",
                    message=f"CPU使用率过高: {cpu_percent:.1f}% > {self.config.max_cpu_usage}%",
                    timestamp=datetime.now(),
                    metric_value=cpu_percent,
                    threshold=self.config.max_cpu_usage
                ))
            
            # 处理性能告警
            for alert in alerts:
                self._handle_performance_alert(alert)
                
            # 定期清理内存
            if memory_mb > self.config.max_memory_usage * 0.8:
                self._cleanup_memory()
                
        except Exception as e:
            logger.error(f"性能检查失败: {e}")

    def _handle_performance_alert(self, alert: PerformanceAlert):
        """处理性能告警"""
        self.performance_alerts.append(alert)
        
        # 保持最近100个告警
        if len(self.performance_alerts) > 100:
            self.performance_alerts = self.performance_alerts[-100:]
        
        logger.warning(f"性能告警: {alert.message}")
        
        # 触发告警回调
        for callback in self.performance_alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"性能告警回调失败: {e}")
        
        # 自动优化措施
        if alert.severity in ["high", "critical"]:
            self._apply_performance_optimization(alert)

    def _apply_performance_optimization(self, alert: PerformanceAlert):
        """应用性能优化措施"""
        try:
            if alert.alert_type == "memory_usage":
                # 清理缓存
                self._cleanup_memory()
                # 调整轮询间隔
                if self.current_mode in [PollingMode.CRITICAL, PollingMode.IMMEDIATE]:
                    self.current_mode = PollingMode.APPROACHING
                    logger.info("因内存压力调整轮询模式为APPROACHING")
                    
            elif alert.alert_type == "cpu_usage":
                # 增加轮询间隔
                if self.current_mode == PollingMode.IMMEDIATE:
                    self.current_mode = PollingMode.CRITICAL
                elif self.current_mode == PollingMode.CRITICAL:
                    self.current_mode = PollingMode.APPROACHING
                logger.info(f"因CPU压力调整轮询模式为{self.current_mode.value}")
                
        except Exception as e:
            logger.error(f"应用性能优化失败: {e}")

    def _cleanup_memory(self):
        """清理内存"""
        try:
            # 清理过期缓存
            current_time = time.time()
            
            # 清理数据缓存
            expired_keys = [
                key for key, (data, timestamp) in self.data_cache.items()
                if current_time - timestamp > self.cache_ttl
            ]
            for key in expired_keys:
                del self.data_cache[key]
            
            # 清理预测缓存
            expired_keys = [
                key for key, (prediction, timestamp) in self.prediction_cache.items()
                if current_time - timestamp > self.cache_ttl
            ]
            for key in expired_keys:
                del self.prediction_cache[key]
            
            # 强制垃圾回收
            gc.collect()
            
            logger.info(f"内存清理完成，清理了{len(expired_keys)}个缓存项")
            
        except Exception as e:
            logger.error(f"内存清理失败: {e}")

    def _determine_polling_interval(self) -> float:
        """确定轮询间隔"""
        if not self.current_prediction:
            return self.config.normal_interval
        
        countdown = self.current_prediction.countdown_seconds
        
        # 根据倒计时动态调整
        if countdown > self.config.idle_threshold:
            self.current_mode = PollingMode.IDLE
            return self.config.idle_interval
        elif countdown > self.config.approaching_threshold:
            self.current_mode = PollingMode.NORMAL
            return self.config.normal_interval
        elif countdown > self.config.critical_threshold:
            self.current_mode = PollingMode.APPROACHING
            return self.config.approaching_interval
        elif countdown >= self.config.immediate_threshold:
            self.current_mode = PollingMode.CRITICAL
            return self.config.critical_interval
        else:
            self.current_mode = PollingMode.IMMEDIATE
            return self.config.immediate_interval

    def _process_new_data(self, data: List):
        """处理新数据"""
        try:
            for record in data:
                # 检查是否为新开奖
                if self._is_new_draw(record):
                    logger.info(f"发现新开奖: {record.draw_id}")
                    self.last_draw_time = datetime.now()
                    
                    # 验证预测准确性
                    if self.last_prediction:
                        self._validate_prediction(record)
                    
        except Exception as e:
            logger.error(f"处理新数据失败: {e}")
            
    def _is_new_draw(self, record) -> bool:
        """检查是否为新开奖"""
        # 这里可以实现更复杂的新开奖检测逻辑
        return True  # 简化实现
        
    def _update_prediction(self, data: List):
        """更新开奖预测"""
        try:
            if not data:
                return
                
            latest_record = data[0]  # 假设第一条是最新的
            
            # 提取预测信息
            if hasattr(latest_record, 'next_draw_time') and latest_record.next_draw_time:
                next_time = datetime.fromisoformat(latest_record.next_draw_time.replace('Z', '+00:00'))
                countdown = int((next_time - datetime.now()).total_seconds())
                
                prediction = DrawPrediction(
                    next_draw_id=getattr(latest_record, 'next_draw_id', ''),
                    next_draw_time=latest_record.next_draw_time,
                    countdown_seconds=max(0, countdown),
                    estimated_draw_time=next_time,
                    confidence_level=0.9  # 基于API数据的高置信度
                )
                
                self.last_prediction = prediction
                
        except Exception as e:
            logger.error(f"更新预测失败: {e}")
            
    def _generate_draw_prediction(self) -> Optional[DrawPrediction]:
        """生成开奖预测"""
        try:
            # 如果有最新预测且仍然有效，直接返回
            if self.last_prediction:
                if self.last_prediction.countdown_seconds > 0:
                    # 更新倒计时
                    elapsed = (datetime.now() - self.last_prediction.estimated_draw_time).total_seconds()
                    remaining = max(0, self.last_prediction.countdown_seconds + elapsed)
                    
                    if remaining > 0:
                        updated_prediction = DrawPrediction(
                            next_draw_id=self.last_prediction.next_draw_id,
                            next_draw_time=self.last_prediction.next_draw_time,
                            countdown_seconds=int(remaining),
                            estimated_draw_time=self.last_prediction.estimated_draw_time,
                            confidence_level=self.last_prediction.confidence_level
                        )
                        return updated_prediction
            
            # 基于历史模式生成预测（简化实现）
            if self.last_draw_time:
                # 假设每5分钟开奖一次
                next_estimated = self.last_draw_time + timedelta(minutes=5)
                countdown = int((next_estimated - datetime.now()).total_seconds())
                
                if countdown > 0:
                    return DrawPrediction(
                        next_draw_id="predicted",
                        next_draw_time=next_estimated.isoformat(),
                        countdown_seconds=countdown,
                        estimated_draw_time=next_estimated,
                        confidence_level=0.7  # 基于模式的中等置信度
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"生成预测失败: {e}")
            return None
            
    def _adjust_polling_mode(self):
        """动态调整轮询模式"""
        try:
            if not self.last_prediction:
                self.current_mode = PollingMode.NORMAL
                return
                
            countdown = self.last_prediction.countdown_seconds
            
            # 根据倒计时调整模式 - 修正阈值逻辑
            if countdown > self.config.idle_threshold:
                self.current_mode = PollingMode.IDLE
            elif countdown > self.config.approaching_threshold:
                self.current_mode = PollingMode.NORMAL
            elif countdown > self.config.critical_threshold:
                self.current_mode = PollingMode.APPROACHING
            elif countdown >= self.config.immediate_threshold:  # 修改为>=，30秒时应该是CRITICAL
                self.current_mode = PollingMode.CRITICAL
            else:
                self.current_mode = PollingMode.IMMEDIATE
                
            logger.debug(f"轮询模式调整为: {self.current_mode.value}")
            
        except Exception as e:
            logger.error(f"调整轮询模式失败: {e}")
            
    def _validate_prediction(self, actual_record):
        """验证预测准确性"""
        try:
            if not self.last_prediction:
                return
                
            # 计算预测误差
            predicted_time = self.last_prediction.estimated_draw_time
            actual_time = datetime.now()  # 简化，实际应该从记录中获取
            
            error_seconds = abs((actual_time - predicted_time).total_seconds())
            
            # 更新预测指标
            with self.metrics_lock:
                if error_seconds <= 60:  # 1分钟内算成功
                    self.metrics.successful_predictions += 1
                else:
                    self.metrics.failed_predictions += 1
                    
            logger.info(f"预测验证: 误差 {error_seconds:.1f} 秒")
            
        except Exception as e:
            logger.error(f"验证预测失败: {e}")
            
    def _update_metrics(self, response_time: float):
        """更新指标"""
        with self.metrics_lock:
            self.metrics.total_requests += 1
            
            # 更新平均响应时间
            if self.metrics.average_response_time == 0:
                self.metrics.average_response_time = response_time
            else:
                self.metrics.average_response_time = (
                    self.metrics.average_response_time * 0.9 + response_time * 0.1
                )
                
            # 计算轮询效率
            if self.metrics.total_requests > 0:
                success_rate = (
                    self.metrics.successful_predictions / 
                    max(1, self.metrics.successful_predictions + self.metrics.failed_predictions)
                )
                self.metrics.polling_efficiency = success_rate * self.metrics.cache_hit_rate
                
    def add_new_draw_callback(self, callback: Callable):
        """添加新开奖回调"""
        self.on_new_draw_callbacks.append(callback)
        
    def add_prediction_callback(self, callback: Callable):
        """添加预测回调"""
        self.on_prediction_callbacks.append(callback)
        
    def get_current_prediction(self) -> Optional[DrawPrediction]:
        """获取当前预测"""
        return self.last_prediction
        
    def get_optimization_metrics(self) -> Dict[str, Any]:
        """获取优化指标"""
        with self.metrics_lock:
            return asdict(self.metrics)
            
    def get_current_mode(self) -> PollingMode:
        """获取当前轮询模式"""
        return self.current_mode

# 使用示例
if __name__ == "__main__":
    
    # 创建API系统 - 移除RealAPIDataSystem引用，改为使用云端数据源
    # api_config = APIConfig()
    # api_system = RealAPIDataSystem(api_config)
    
    # 创建优化器
    optimizer = SmartRealtimeOptimizer()  # 使用默认初始化
    
    # 添加回调函数
    def on_new_draw(data):
        print(f"新开奖回调: {len(data)} 条记录")
        
    def on_prediction(prediction):
        print(f"预测回调: 下期 {prediction.next_draw_id}, 倒计时 {prediction.countdown_seconds} 秒")
        
    optimizer.add_new_draw_callback(on_new_draw)
    optimizer.add_prediction_callback(on_prediction)
    
    # 启动优化
    optimizer.start_optimization()
    
    try:
        # 运行监控
        while True:
            time.sleep(30)
            
            # 显示当前状态
            prediction = optimizer.get_current_prediction()
            if prediction:
                print(f"当前预测: {prediction.next_draw_id}, 倒计时: {prediction.countdown_seconds}秒")
                
            metrics = optimizer.get_optimization_metrics()
            print(f"优化指标: 请求 {metrics['total_requests']}, 效率 {metrics['polling_efficiency']:.2%}")
            print(f"当前模式: {optimizer.get_current_mode().value}")
            print("-" * 50)
            
    except KeyboardInterrupt:
        print("停止优化器...")
        optimizer.stop_optimization()

    def add_performance_alert_callback(self, callback: Callable):
        """添加性能告警回调"""
        self.performance_alert_callbacks.append(callback)

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        return {
            "metrics": asdict(self.metrics),
            "current_mode": self.current_mode.value,
            "uptime": str(datetime.now() - self.start_time),
            "recent_alerts": [
                {
                    "type": alert.alert_type,
                    "severity": alert.severity,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "value": alert.metric_value,
                    "threshold": alert.threshold
                }
                for alert in self.performance_alerts[-10:]  # 最近10个告警
            ],
            "cache_stats": {
                "data_cache_size": len(self.data_cache),
                "prediction_cache_size": len(self.prediction_cache),
                "cache_hit_rate": self.metrics.cache_hit_rate
            }
        }