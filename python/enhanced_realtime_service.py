#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28增强版实时开奖服务
最大化API字段利用率，实现实时推送、缓存和通知功能
"""

import json
import time
import logging
import threading
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, asdict
from enum import Enum
import sqlite3
import hashlib
from concurrent.futures import ThreadPoolExecutor
import queue


# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NotificationType(Enum):
    """通知类型枚举"""
    NEW_DRAW = "new_draw"  # 新开奖
    DRAW_UPDATE = "draw_update"  # 开奖更新
    NEXT_DRAW_INFO = "next_draw_info"  # 下期信息
    ERROR = "error"  # 错误通知
    SYSTEM_STATUS = "system_status"  # 系统状态

class CacheLevel(Enum):
    """缓存级别枚举"""
    MEMORY = "memory"  # 内存缓存
    PERSISTENT = "persistent"  # 持久化缓存
    DISTRIBUTED = "distributed"  # 分布式缓存

@dataclass
class EnhancedDrawData:
    """增强版开奖数据结构，最大化利用API字段"""
    # 基础字段
    draw_id: str
    timestamp: str
    date: str
    result_numbers: List[int]
    result_sum: int
    result_digits: List[int]
    
    # 扩展字段（已优化，移除未使用字段）
    # current_time: Optional[str] = None  # curtime字段 - 已移除
    # short_issue: Optional[str] = None  # 短期号 - 已移除
    award_time: Optional[str] = None  # 开奖时间
    next_issue: Optional[str] = None  # 下期期号
    next_time: Optional[str] = None  # 下期时间
    
    # 计算字段
    big_small: Optional[str] = None  # 大小
    odd_even: Optional[str] = None  # 单双
    dragon_tiger: Optional[str] = None  # 龙虎
    
    # 元数据
    data_source: str = "realtime"  # 数据来源
    fetch_timestamp: Optional[str] = None  # 获取时间戳
    data_hash: Optional[str] = None  # 数据哈希
    validation_status: str = "pending"  # 验证状态
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.fetch_timestamp:
            self.fetch_timestamp = datetime.now(timezone.utc).isoformat()
        
        # 计算数据哈希
        self.data_hash = self._calculate_hash()
        
        # 计算扩展属性
        self._calculate_extended_properties()
    
    def _calculate_hash(self) -> str:
        """计算数据哈希"""
        hash_data = f"{self.draw_id}_{self.timestamp}_{self.result_numbers}"
        return hashlib.md5(hash_data.encode()).hexdigest()
    
    def _calculate_extended_properties(self):
        """计算扩展属性"""
        if self.result_sum:
            # 大小判断
            self.big_small = "大" if self.result_sum >= 14 else "小"
            
            # 单双判断
            self.odd_even = "单" if self.result_sum % 2 == 1 else "双"
        
        if len(self.result_numbers) >= 3:
            # 龙虎判断（第一个数字vs最后一个数字）
            first = self.result_numbers[0]
            last = self.result_numbers[-1]
            if first > last:
                self.dragon_tiger = "龙"
            elif first < last:
                self.dragon_tiger = "虎"
            else:
                self.dragon_tiger = "和"

@dataclass
class RealtimeMetrics:
    """实时监控指标"""
    total_draws_fetched: int = 0
    successful_fetches: int = 0
    failed_fetches: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    notifications_sent: int = 0
    average_response_time: float = 0.0
    last_fetch_time: Optional[str] = None
    system_uptime: float = 0.0
    error_rate: float = 0.0

class EnhancedRealtimeService:
    """
    增强版实时开奖服务
    """
    
    def __init__(self, appid: str = "45928", secret_key: str = "ca9edbfee35c22a0d6c4cf6722506af0", config: Dict = None):
        """
        初始化服务
        
        Args:
            appid: 应用ID
            secret_key: 密钥
            config: 配置字典
        """
        self.config = config or {}
        self.api_client = PC28UpstreamAPI(appid, secret_key)
        self.db_path = "realtime_cache.db"
        
        # 从配置中获取参数
        realtime_settings = self.config.get('realtime_settings', {})
        self.fetch_interval = self.config.get('realtime_fetch_interval', 30)
        self.cache_ttl = 300  # 缓存TTL（秒）
        self.max_cache_size = realtime_settings.get('cache_size', 1000)
        self.notification_queue_size = 100
        
        # 运行状态
        self.is_running = False
        self.start_time = None
        
        # 缓存系统
        self.memory_cache: Dict[str, EnhancedDrawData] = {}
        self.cache_timestamps: Dict[str, float] = {}
        self.cache_lock = threading.Lock()
        
        # 通知系统
        self.notification_callbacks: List[Callable] = []
        self.notification_queue = queue.Queue(maxsize=self.notification_queue_size)
        
        # 监控指标
        self.metrics = RealtimeMetrics()
        self.metrics_lock = threading.Lock()
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=3)
        
        # 数据验证
        self.last_known_draw: Optional[EnhancedDrawData] = None
        self.duplicate_detection: Set[str] = set()
        
        # 初始化数据库
        self._init_database()
        
    def _init_database(self):
        """初始化缓存数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 创建实时数据缓存表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS realtime_cache (
                        draw_id TEXT PRIMARY KEY,
                        timestamp TEXT,
                        date TEXT,
                        result_numbers TEXT,
                        result_sum INTEGER,
                        result_digits TEXT,
                        -- current_time TEXT,  -- 已移除 - 未使用字段
                        -- short_issue TEXT,   -- 已移除 - 未使用字段
                        award_time TEXT,
                        next_issue TEXT,
                        next_time TEXT,
                        big_small TEXT,
                        odd_even TEXT,
                        dragon_tiger TEXT,
                        data_source TEXT,
                        fetch_timestamp TEXT,
                        data_hash TEXT,
                        validation_status TEXT,
                        created_at TEXT
                    )
                """)
                
                # 创建通知记录表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS notification_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        notification_type TEXT,
                        draw_id TEXT,
                        content TEXT,
                        sent_at TEXT,
                        status TEXT
                    )
                """)
                
                # 创建监控指标表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS metrics_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        total_draws_fetched INTEGER,
                        successful_fetches INTEGER,
                        failed_fetches INTEGER,
                        cache_hits INTEGER,
                        cache_misses INTEGER,
                        notifications_sent INTEGER,
                        average_response_time REAL,
                        error_rate REAL
                    )
                """)
                
                conn.commit()
                logger.info("实时服务数据库初始化完成")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def start_realtime_monitoring(self):
        """
        启动实时监控
        """
        if self.is_running:
            logger.warning("实时监控已在运行")
            return
        
        self.is_running = True
        self.start_time = time.time()
        
        logger.info("启动实时开奖监控服务")
        
        # 启动主监控线程
        threading.Thread(target=self._monitoring_loop, daemon=True).start()
        
        # 启动通知处理线程
        threading.Thread(target=self._notification_processor, daemon=True).start()
        
        # 启动缓存清理线程
        threading.Thread(target=self._cache_cleanup_loop, daemon=True).start()
        
        # 启动指标收集线程
        threading.Thread(target=self._metrics_collector, daemon=True).start()
    
    def stop_realtime_monitoring(self):
        """
        停止实时监控
        """
        self.is_running = False
        logger.info("停止实时开奖监控服务")
    
    def _monitoring_loop(self):
        """
        主监控循环
        """
        logger.info("实时监控循环启动")
        
        while self.is_running:
            try:
                start_time = time.time()
                
                # 获取实时数据
                draw_data = self._fetch_enhanced_realtime_data()
                
                if draw_data:
                    # 检查是否为新数据
                    if self._is_new_draw(draw_data):
                        logger.info(f"发现新开奖: {draw_data.draw_id}")
                        
                        # 缓存数据
                        self._cache_draw_data(draw_data)
                        
                        # 发送通知
                        self._send_notification(NotificationType.NEW_DRAW, draw_data)
                        
                        # 更新最后已知开奖
                        self.last_known_draw = draw_data
                    
                    # 更新指标
                    with self.metrics_lock:
                        self.metrics.successful_fetches += 1
                        self.metrics.total_draws_fetched += 1
                        
                        # 更新平均响应时间
                        response_time = time.time() - start_time
                        if self.metrics.average_response_time == 0:
                            self.metrics.average_response_time = response_time
                        else:
                            self.metrics.average_response_time = (
                                self.metrics.average_response_time * 0.9 + response_time * 0.1
                            )
                        
                        self.metrics.last_fetch_time = datetime.now(timezone.utc).isoformat()
                
                else:
                    # 获取失败
                    with self.metrics_lock:
                        self.metrics.failed_fetches += 1
                        self.metrics.total_draws_fetched += 1
                
                # 等待下次获取
                time.sleep(self.fetch_interval)
                
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                
                with self.metrics_lock:
                    self.metrics.failed_fetches += 1
                
                time.sleep(self.fetch_interval)
    
    def _fetch_enhanced_realtime_data(self) -> Optional[EnhancedDrawData]:
        """
        获取增强版实时数据，最大化利用API字段
        
        Returns:
            增强版开奖数据
        """
        try:
            # 获取原始数据
            raw_data = self.api_client.get_realtime_lottery()
            
            if raw_data.get('codeid') != 10000:
                logger.warning(f"API返回错误: {raw_data.get('message')}")
                return None
            
            retdata = raw_data.get('retdata', {})
            current_data = retdata.get('curent', {})
            next_data = retdata.get('next', {})
            
            if not current_data:
                logger.warning("无当前开奖数据")
                return None
            
            # 解析基础数据
            parsed_data = self.api_client.parse_lottery_data(raw_data)
            if not parsed_data:
                return None
            
            base_data = parsed_data[0]  # 取第一条数据
            
            # 创建增强版数据结构
            enhanced_data = EnhancedDrawData(
                draw_id=base_data.get('draw_id'),
                timestamp=base_data.get('timestamp'),
                date=base_data.get('date'),
                result_numbers=base_data.get('result_numbers', []),
                result_sum=base_data.get('result_sum', 0),
                result_digits=base_data.get('result_digits', []),
                
                # 扩展字段（已优化，移除未使用字段）
                # current_time=raw_data.get('curtime'),  # 已移除 - 未使用字段
                # short_issue=current_data.get('short_issue'),  # 已移除 - 未使用字段
                award_time=current_data.get('award_time'),
                next_issue=next_data.get('next_issue'),
                next_time=next_data.get('next_time'),
                
                data_source="realtime_enhanced"
            )
            
            # 数据验证
            enhanced_data.validation_status = self._validate_draw_data(enhanced_data)
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"获取增强实时数据失败: {e}")
            return None
    
    def _validate_draw_data(self, draw_data: EnhancedDrawData) -> str:
        """
        验证开奖数据
        
        Args:
            draw_data: 开奖数据
            
        Returns:
            验证状态
        """
        try:
            # 基础验证
            if not draw_data.draw_id or not draw_data.result_numbers:
                return "invalid_basic"
            
            # 数字范围验证
            if not all(0 <= num <= 9 for num in draw_data.result_numbers):
                return "invalid_range"
            
            # 数字数量验证
            if len(draw_data.result_numbers) != 3:
                return "invalid_count"
            
            # 和值验证
            expected_sum = sum(draw_data.result_numbers)
            if draw_data.result_sum != expected_sum:
                return "invalid_sum"
            
            # 时间验证
            if draw_data.timestamp:
                try:
                    datetime.fromisoformat(draw_data.timestamp.replace('Z', '+00:00'))
                except ValueError:
                    return "invalid_timestamp"
            
            # 重复检测
            if draw_data.data_hash in self.duplicate_detection:
                return "duplicate"
            
            return "valid"
            
        except Exception as e:
            logger.error(f"数据验证失败: {e}")
            return "validation_error"
    
    def _is_new_draw(self, draw_data: EnhancedDrawData) -> bool:
        """
        检查是否为新开奖
        
        Args:
            draw_data: 开奖数据
            
        Returns:
            是否为新开奖
        """
        # 检查是否已缓存
        if self._get_cached_draw(draw_data.draw_id):
            return False
        
        # 检查是否重复
        if draw_data.data_hash in self.duplicate_detection:
            return False
        
        # 与最后已知开奖比较
        if self.last_known_draw:
            if draw_data.draw_id == self.last_known_draw.draw_id:
                return False
        
        return True
    
    def _cache_draw_data(self, draw_data: EnhancedDrawData):
        """
        缓存开奖数据
        
        Args:
            draw_data: 开奖数据
        """
        with self.cache_lock:
            # 内存缓存
            self.memory_cache[draw_data.draw_id] = draw_data
            self.cache_timestamps[draw_data.draw_id] = time.time()
            
            # 添加到重复检测集合
            self.duplicate_detection.add(draw_data.data_hash)
            
            # 限制重复检测集合大小
            if len(self.duplicate_detection) > 1000:
                # 移除最旧的一半
                old_hashes = list(self.duplicate_detection)[:500]
                self.duplicate_detection -= set(old_hashes)
        
        # 持久化缓存
        self._save_to_persistent_cache(draw_data)
        
        logger.debug(f"缓存开奖数据: {draw_data.draw_id}")
    
    def _save_to_persistent_cache(self, draw_data: EnhancedDrawData):
        """
        保存到持久化缓存
        
        Args:
            draw_data: 开奖数据
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO realtime_cache 
                    (draw_id, timestamp, date, result_numbers, result_sum, result_digits,
                     award_time, next_issue, next_time,
                     big_small, odd_even, dragon_tiger, data_source, fetch_timestamp,
                     data_hash, validation_status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    draw_data.draw_id, draw_data.timestamp, draw_data.date,
                    json.dumps(draw_data.result_numbers), draw_data.result_sum,
                    json.dumps(draw_data.result_digits), draw_data.award_time, draw_data.next_issue,
                    draw_data.next_time, draw_data.big_small, draw_data.odd_even,
                    draw_data.dragon_tiger, draw_data.data_source, draw_data.fetch_timestamp,
                    draw_data.data_hash, draw_data.validation_status,
                    datetime.now(timezone.utc).isoformat()
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存持久化缓存失败: {e}")
    
    def _get_cached_draw(self, draw_id: str) -> Optional[EnhancedDrawData]:
        """
        获取缓存的开奖数据
        
        Args:
            draw_id: 开奖ID
            
        Returns:
            缓存的开奖数据
        """
        with self.cache_lock:
            # 检查内存缓存
            if draw_id in self.memory_cache:
                # 检查是否过期
                cache_time = self.cache_timestamps.get(draw_id, 0)
                if time.time() - cache_time < self.cache_ttl:
                    with self.metrics_lock:
                        self.metrics.cache_hits += 1
                    return self.memory_cache[draw_id]
                else:
                    # 过期，移除
                    del self.memory_cache[draw_id]
                    del self.cache_timestamps[draw_id]
        
        # 检查持久化缓存
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM realtime_cache WHERE draw_id = ?",
                    (draw_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    with self.metrics_lock:
                        self.metrics.cache_hits += 1
                    
                    # 重建数据对象（简化版）
                    return EnhancedDrawData(
                        draw_id=row[0],
                        timestamp=row[1],
                        date=row[2],
                        result_numbers=json.loads(row[3]) if row[3] else [],
                        result_sum=row[4] or 0,
                        result_digits=json.loads(row[5]) if row[5] else []
                    )
        
        except Exception as e:
            logger.error(f"获取持久化缓存失败: {e}")
        
        with self.metrics_lock:
            self.metrics.cache_misses += 1
        
        return None
    
    def add_notification_callback(self, callback: Callable[[NotificationType, Any], None]):
        """
        添加通知回调函数
        
        Args:
            callback: 回调函数
        """
        self.notification_callbacks.append(callback)
        logger.info(f"添加通知回调，当前回调数量: {len(self.notification_callbacks)}")
    
    def _send_notification(self, notification_type: NotificationType, data: Any):
        """
        发送通知
        
        Args:
            notification_type: 通知类型
            data: 通知数据
        """
        try:
            notification = {
                'type': notification_type.value,
                'data': data,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # 添加到通知队列
            if not self.notification_queue.full():
                self.notification_queue.put(notification)
            else:
                logger.warning("通知队列已满，丢弃通知")
            
        except Exception as e:
            logger.error(f"发送通知失败: {e}")
    
    def _notification_processor(self):
        """
        通知处理器
        """
        logger.info("通知处理器启动")
        
        while self.is_running:
            try:
                # 获取通知
                notification = self.notification_queue.get(timeout=1)
                
                # 调用所有回调函数
                for callback in self.notification_callbacks:
                    try:
                        callback(notification['type'], notification['data'])
                    except Exception as e:
                        logger.error(f"通知回调执行失败: {e}")
                
                # 记录通知
                self._log_notification(notification)
                
                with self.metrics_lock:
                    self.metrics.notifications_sent += 1
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"通知处理异常: {e}")
    
    def _log_notification(self, notification: Dict[str, Any]):
        """
        记录通知日志
        
        Args:
            notification: 通知信息
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                draw_id = None
                if isinstance(notification['data'], EnhancedDrawData):
                    draw_id = notification['data'].draw_id
                
                cursor.execute("""
                    INSERT INTO notification_log 
                    (notification_type, draw_id, content, sent_at, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    notification['type'],
                    draw_id,
                    json.dumps(notification, default=str),
                    notification['timestamp'],
                    'sent'
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"记录通知日志失败: {e}")
    
    def _cache_cleanup_loop(self):
        """
        缓存清理循环
        """
        logger.info("缓存清理器启动")
        
        while self.is_running:
            try:
                with self.cache_lock:
                    current_time = time.time()
                    expired_keys = []
                    
                    # 查找过期的缓存
                    for draw_id, cache_time in self.cache_timestamps.items():
                        if current_time - cache_time > self.cache_ttl:
                            expired_keys.append(draw_id)
                    
                    # 移除过期缓存
                    for key in expired_keys:
                        if key in self.memory_cache:
                            del self.memory_cache[key]
                        if key in self.cache_timestamps:
                            del self.cache_timestamps[key]
                    
                    if expired_keys:
                        logger.debug(f"清理过期缓存: {len(expired_keys)} 条")
                    
                    # 限制缓存大小
                    if len(self.memory_cache) > self.max_cache_size:
                        # 移除最旧的缓存
                        sorted_items = sorted(
                            self.cache_timestamps.items(),
                            key=lambda x: x[1]
                        )
                        
                        remove_count = len(self.memory_cache) - self.max_cache_size
                        for i in range(remove_count):
                            key = sorted_items[i][0]
                            if key in self.memory_cache:
                                del self.memory_cache[key]
                            if key in self.cache_timestamps:
                                del self.cache_timestamps[key]
                        
                        logger.debug(f"清理超量缓存: {remove_count} 条")
                
                # 每5分钟清理一次
                time.sleep(300)
                
            except Exception as e:
                logger.error(f"缓存清理异常: {e}")
                time.sleep(60)
    
    def _metrics_collector(self):
        """
        指标收集器
        """
        logger.info("指标收集器启动")
        
        while self.is_running:
            try:
                with self.metrics_lock:
                    # 计算错误率
                    if self.metrics.total_draws_fetched > 0:
                        self.metrics.error_rate = (
                            self.metrics.failed_fetches / self.metrics.total_draws_fetched
                        ) * 100
                    
                    # 计算系统运行时间
                    if self.start_time:
                        self.metrics.system_uptime = time.time() - self.start_time
                    
                    # 保存指标到数据库
                    self._save_metrics()
                
                # 每分钟收集一次
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"指标收集异常: {e}")
                time.sleep(60)
    
    def _save_metrics(self):
        """
        保存监控指标
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO metrics_log 
                    (timestamp, total_draws_fetched, successful_fetches, failed_fetches,
                     cache_hits, cache_misses, notifications_sent, average_response_time, error_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now(timezone.utc).isoformat(),
                    self.metrics.total_draws_fetched,
                    self.metrics.successful_fetches,
                    self.metrics.failed_fetches,
                    self.metrics.cache_hits,
                    self.metrics.cache_misses,
                    self.metrics.notifications_sent,
                    self.metrics.average_response_time,
                    self.metrics.error_rate
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存指标失败: {e}")
    
    def get_realtime_metrics(self) -> Dict[str, Any]:
        """
        获取实时监控指标
        
        Returns:
            监控指标字典
        """
        with self.metrics_lock:
            return asdict(self.metrics)
    
    def get_latest_draw(self) -> Optional[EnhancedDrawData]:
        """
        获取最新开奖数据
        
        Returns:
            最新开奖数据
        """
        return self.last_known_draw
    
    def get_next_draw_info(self) -> Optional[Dict[str, Any]]:
        """
        获取下期开奖信息
        
        Returns:
            下期开奖信息
        """
        if self.last_known_draw:
            return {
                'next_issue': self.last_known_draw.next_issue,
                'next_time': self.last_known_draw.next_time,
                'estimated_time': self._estimate_next_draw_time()
            }
        return None
    
    def fetch_current_draw(self) -> Optional[Dict[str, Any]]:
        """
        获取当前开奖数据（兼容接口）
        
        Returns:
            当前开奖数据
        """
        enhanced_data = self._fetch_enhanced_realtime_data()
        if enhanced_data:
            return {
                'draw_id': enhanced_data.draw_id,
                'timestamp': enhanced_data.timestamp,
                'date': enhanced_data.date,
                'result_numbers': enhanced_data.result_numbers,
                'result_sum': enhanced_data.result_sum,
                'result_digits': enhanced_data.result_digits,
                'validation_status': enhanced_data.validation_status
            }
        return None
    
    def get_cached_draw(self, draw_id: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存的开奖数据（公共接口）
        
        Args:
            draw_id: 开奖ID
            
        Returns:
            缓存的开奖数据
        """
        cached_data = self._get_cached_draw(draw_id)
        if cached_data:
            return {
                'draw_id': cached_data.draw_id,
                'timestamp': cached_data.timestamp,
                'date': cached_data.date,
                'result_numbers': cached_data.result_numbers,
                'result_sum': cached_data.result_sum,
                'result_digits': cached_data.result_digits,
                'validation_status': cached_data.validation_status
            }
        return None
    
    def _estimate_next_draw_time(self) -> Optional[str]:
        """
        估算下期开奖时间
        
        Returns:
            估算的开奖时间
        """
        try:
            if self.last_known_draw and self.last_known_draw.next_time:
                return self.last_known_draw.next_time
            
            # 如果没有下期时间，基于当前时间估算（假设每5分钟一期）
            next_time = datetime.now(timezone.utc) + timedelta(minutes=5)
            return next_time.isoformat()
            
        except Exception as e:
            logger.error(f"估算下期开奖时间失败: {e}")
            return None

# 使用示例
if __name__ == "__main__":
    # 创建增强实时服务
    realtime_service = EnhancedRealtimeService()
    
    # 添加通知回调
    def notification_handler(notification_type: str, data: Any):
        if notification_type == NotificationType.NEW_DRAW.value:
            print(f"新开奖通知: {data.draw_id} - {data.result_numbers}")
            print(f"大小: {data.big_small}, 单双: {data.odd_even}, 龙虎: {data.dragon_tiger}")
        elif notification_type == NotificationType.ERROR.value:
            print(f"错误通知: {data}")
    
    realtime_service.add_notification_callback(notification_handler)
    
    # 启动实时监控
    realtime_service.start_realtime_monitoring()
    
    try:
        # 运行一段时间
        while True:
            time.sleep(10)
            
            # 显示监控指标
            metrics = realtime_service.get_realtime_metrics()
            print(f"监控指标: 成功 {metrics['successful_fetches']}, 失败 {metrics['failed_fetches']}, 错误率 {metrics['error_rate']:.2f}%")
            
            # 显示最新开奖
            latest = realtime_service.get_latest_draw()
            if latest:
                print(f"最新开奖: {latest.draw_id} - {latest.result_numbers} (和值: {latest.result_sum})")
            
            # 显示下期信息
            next_info = realtime_service.get_next_draw_info()
            if next_info:
                print(f"下期信息: {next_info['next_issue']} - {next_info['next_time']}")
    
    except KeyboardInterrupt:
        print("停止监控...")
        realtime_service.stop_realtime_monitoring()