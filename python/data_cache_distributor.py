#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28数据缓存和分发系统
负责数据缓存、实时分发和性能优化
"""

import json
import time
import threading
import logging
from typing import Dict, List, Any, Optional, Callable, Set
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
from api_field_optimization import OptimizedLotteryData
from realtime_notification_system import RealtimeNotificationSystem, NotificationEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheLevel(Enum):
    """缓存级别"""
    L1_MEMORY = "l1_memory"      # 内存缓存（最快）
    L2_REDIS = "l2_redis"        # Redis缓存（中等）
    L3_DATABASE = "l3_database"  # 数据库缓存（最慢）

class DataType(Enum):
    """数据类型"""
    REALTIME = "realtime"        # 实时数据
    HISTORICAL = "historical"    # 历史数据
    STATISTICS = "statistics"    # 统计数据
    METADATA = "metadata"        # 元数据

@dataclass
class CacheEntry:
    """缓存条目"""
    key: str
    data: Any
    data_type: DataType
    timestamp: float
    ttl: float  # 生存时间（秒）
    access_count: int = 0
    last_access: float = 0
    size_bytes: int = 0

@dataclass
class CacheStats:
    """缓存统计"""
    total_entries: int
    total_size_bytes: int
    hit_count: int
    miss_count: int
    eviction_count: int
    hit_rate: float
    memory_usage_mb: float
    avg_access_time_ms: float

@dataclass
class DistributionTarget:
    """分发目标"""
    target_id: str
    target_type: str  # websocket, http, file, database
    endpoint: str
    filters: Dict[str, Any]  # 数据过滤条件
    callback: Optional[Callable] = None
    is_active: bool = True
    last_update: float = 0
    error_count: int = 0

class DataCacheManager:
    """数据缓存管理器"""
    
    def __init__(self, max_memory_mb: int = 100, default_ttl: int = 300):
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.default_ttl = default_ttl
        
        # 多级缓存存储
        self.l1_cache: Dict[str, CacheEntry] = {}  # 内存缓存
        self.cache_index: Dict[DataType, Set[str]] = defaultdict(set)  # 类型索引
        self.access_history: deque = deque(maxlen=1000)  # 访问历史
        
        # 缓存统计
        self.stats = CacheStats(
            total_entries=0,
            total_size_bytes=0,
            hit_count=0,
            miss_count=0,
            eviction_count=0,
            hit_rate=0.0,
            memory_usage_mb=0.0,
            avg_access_time_ms=0.0
        )
        
        # 线程锁
        self.cache_lock = threading.RLock()
        
        # 后台清理线程
        self.cleanup_thread = threading.Thread(target=self._background_cleanup, daemon=True)
        self.cleanup_thread.start()
        
        logger.info(f"数据缓存管理器初始化完成，最大内存: {max_memory_mb}MB")
    
    def put(self, key: str, data: Any, data_type: DataType, ttl: Optional[int] = None) -> bool:
        """存储数据到缓存"""
        if ttl is None:
            ttl = self.default_ttl
        
        try:
            with self.cache_lock:
                # 计算数据大小
                data_json = json.dumps(data, ensure_ascii=False) if not isinstance(data, str) else data
                size_bytes = len(data_json.encode('utf-8'))
                
                # 检查内存限制
                if self.stats.total_size_bytes + size_bytes > self.max_memory_bytes:
                    self._evict_lru_entries(size_bytes)
                
                # 创建缓存条目
                current_time = time.time()
                entry = CacheEntry(
                    key=key,
                    data=data,
                    data_type=data_type,
                    timestamp=current_time,
                    ttl=ttl,
                    access_count=0,
                    last_access=current_time,
                    size_bytes=size_bytes
                )
                
                # 如果key已存在，先移除旧条目
                if key in self.l1_cache:
                    old_entry = self.l1_cache[key]
                    self.stats.total_size_bytes -= old_entry.size_bytes
                    self.cache_index[old_entry.data_type].discard(key)
                else:
                    self.stats.total_entries += 1
                
                # 存储新条目
                self.l1_cache[key] = entry
                self.cache_index[data_type].add(key)
                self.stats.total_size_bytes += size_bytes
                self.stats.memory_usage_mb = self.stats.total_size_bytes / (1024 * 1024)
                
                logger.debug(f"缓存存储成功: {key} ({size_bytes} bytes)")
                return True
                
        except Exception as e:
            logger.error(f"缓存存储失败: {key}, 错误: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """从缓存获取数据"""
        start_time = time.time()
        
        try:
            with self.cache_lock:
                if key not in self.l1_cache:
                    self.stats.miss_count += 1
                    self._update_hit_rate()
                    return None
                
                entry = self.l1_cache[key]
                current_time = time.time()
                
                # 检查TTL
                if current_time - entry.timestamp > entry.ttl:
                    self._remove_entry(key)
                    self.stats.miss_count += 1
                    self._update_hit_rate()
                    return None
                
                # 更新访问统计
                entry.access_count += 1
                entry.last_access = current_time
                self.stats.hit_count += 1
                self._update_hit_rate()
                
                # 记录访问历史
                access_time_ms = (time.time() - start_time) * 1000
                self.access_history.append(access_time_ms)
                self._update_avg_access_time()
                
                logger.debug(f"缓存命中: {key}")
                return entry.data
                
        except Exception as e:
            logger.error(f"缓存获取失败: {key}, 错误: {e}")
            self.stats.miss_count += 1
            self._update_hit_rate()
            return None
    
    def get_by_type(self, data_type: DataType, limit: int = 100) -> List[Any]:
        """按类型获取数据"""
        try:
            with self.cache_lock:
                keys = list(self.cache_index[data_type])[:limit]
                results = []
                
                for key in keys:
                    data = self.get(key)
                    if data is not None:
                        results.append(data)
                
                return results
                
        except Exception as e:
            logger.error(f"按类型获取缓存失败: {data_type}, 错误: {e}")
            return []
    
    def remove(self, key: str) -> bool:
        """移除缓存条目"""
        try:
            with self.cache_lock:
                return self._remove_entry(key)
        except Exception as e:
            logger.error(f"移除缓存失败: {key}, 错误: {e}")
            return False
    
    def clear_by_type(self, data_type: DataType) -> int:
        """按类型清空缓存"""
        try:
            with self.cache_lock:
                keys_to_remove = list(self.cache_index[data_type])
                removed_count = 0
                
                for key in keys_to_remove:
                    if self._remove_entry(key):
                        removed_count += 1
                
                logger.info(f"清空缓存类型 {data_type}: {removed_count} 条记录")
                return removed_count
                
        except Exception as e:
            logger.error(f"按类型清空缓存失败: {data_type}, 错误: {e}")
            return 0
    
    def _remove_entry(self, key: str) -> bool:
        """内部移除条目方法"""
        if key in self.l1_cache:
            entry = self.l1_cache[key]
            del self.l1_cache[key]
            self.cache_index[entry.data_type].discard(key)
            self.stats.total_entries -= 1
            self.stats.total_size_bytes -= entry.size_bytes
            self.stats.memory_usage_mb = self.stats.total_size_bytes / (1024 * 1024)
            return True
        return False
    
    def _evict_lru_entries(self, required_bytes: int):
        """LRU淘汰策略"""
        # 按最后访问时间排序
        entries_by_access = sorted(
            self.l1_cache.items(),
            key=lambda x: x[1].last_access
        )
        
        freed_bytes = 0
        evicted_count = 0
        
        for key, entry in entries_by_access:
            if freed_bytes >= required_bytes:
                break
            
            freed_bytes += entry.size_bytes
            self._remove_entry(key)
            evicted_count += 1
        
        self.stats.eviction_count += evicted_count
        logger.info(f"LRU淘汰: {evicted_count} 条记录，释放 {freed_bytes} 字节")
    
    def _background_cleanup(self):
        """后台清理过期条目"""
        while True:
            try:
                time.sleep(60)  # 每分钟清理一次
                
                with self.cache_lock:
                    current_time = time.time()
                    expired_keys = []
                    
                    for key, entry in self.l1_cache.items():
                        if current_time - entry.timestamp > entry.ttl:
                            expired_keys.append(key)
                    
                    for key in expired_keys:
                        self._remove_entry(key)
                    
                    if expired_keys:
                        logger.info(f"后台清理过期条目: {len(expired_keys)} 条")
                        
            except Exception as e:
                logger.error(f"后台清理异常: {e}")
    
    def _update_hit_rate(self):
        """更新命中率"""
        total_requests = self.stats.hit_count + self.stats.miss_count
        if total_requests > 0:
            self.stats.hit_rate = (self.stats.hit_count / total_requests) * 100
    
    def _update_avg_access_time(self):
        """更新平均访问时间"""
        if self.access_history:
            self.stats.avg_access_time_ms = sum(self.access_history) / len(self.access_history)
    
    def get_stats(self) -> CacheStats:
        """获取缓存统计"""
        with self.cache_lock:
            return CacheStats(
                total_entries=self.stats.total_entries,
                total_size_bytes=self.stats.total_size_bytes,
                hit_count=self.stats.hit_count,
                miss_count=self.stats.miss_count,
                eviction_count=self.stats.eviction_count,
                hit_rate=self.stats.hit_rate,
                memory_usage_mb=self.stats.memory_usage_mb,
                avg_access_time_ms=self.stats.avg_access_time_ms
            )

class DataDistributor:
    """数据分发器"""
    
    def __init__(self, cache_manager: DataCacheManager):
        self.cache_manager = cache_manager
        self.notification_system = RealtimeNotificationSystem()
        
        # 分发目标
        self.distribution_targets: Dict[str, DistributionTarget] = {}
        
        # 分发队列
        self.distribution_queue: deque = deque(maxlen=10000)
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="distributor")
        
        # 分发统计
        self.distribution_stats = {
            'total_distributions': 0,
            'successful_distributions': 0,
            'failed_distributions': 0,
            'active_targets': 0,
            'queue_size': 0
        }
        
        # 分发线程
        self.distribution_thread = threading.Thread(target=self._distribution_worker, daemon=True)
        self.distribution_thread.start()
        
        logger.info("数据分发器初始化完成")
    
    def register_target(self, target: DistributionTarget) -> bool:
        """注册分发目标"""
        try:
            self.distribution_targets[target.target_id] = target
            self.distribution_stats['active_targets'] = len([t for t in self.distribution_targets.values() if t.is_active])
            logger.info(f"注册分发目标: {target.target_id} ({target.target_type})")
            return True
        except Exception as e:
            logger.error(f"注册分发目标失败: {target.target_id}, 错误: {e}")
            return False
    
    def unregister_target(self, target_id: str) -> bool:
        """注销分发目标"""
        try:
            if target_id in self.distribution_targets:
                del self.distribution_targets[target_id]
                self.distribution_stats['active_targets'] = len([t for t in self.distribution_targets.values() if t.is_active])
                logger.info(f"注销分发目标: {target_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"注销分发目标失败: {target_id}, 错误: {e}")
            return False
    
    def distribute_data(self, data: OptimizedLotteryData, data_type: DataType) -> bool:
        """分发数据"""
        try:
            # 生成缓存键
            cache_key = self._generate_cache_key(data, data_type)
            
            # 存储到缓存
            self.cache_manager.put(cache_key, asdict(data), data_type)
            
            # 添加到分发队列
            distribution_item = {
                'data': data,
                'data_type': data_type,
                'timestamp': time.time(),
                'cache_key': cache_key
            }
            
            self.distribution_queue.append(distribution_item)
            self.distribution_stats['queue_size'] = len(self.distribution_queue)
            
            # 发送实时通知
            event = NotificationEvent(
                event_type="new_data",
                data=asdict(data),
                timestamp=datetime.now(timezone(timedelta(hours=8))).isoformat(),
                metadata={'data_type': data_type.value, 'cache_key': cache_key}
            )
            self.notification_system.queue_notification(event)
            
            logger.debug(f"数据分发排队: {cache_key}")
            return True
            
        except Exception as e:
            logger.error(f"数据分发失败: {e}")
            return False
    
    def _generate_cache_key(self, data: OptimizedLotteryData, data_type: DataType) -> str:
        """生成缓存键"""
        key_data = f"{data_type.value}_{data.draw_id}_{data.timestamp}"
        return hashlib.md5(key_data.encode()).hexdigest()[:16]
    
    def _distribution_worker(self):
        """分发工作线程"""
        while True:
            try:
                if not self.distribution_queue:
                    time.sleep(0.1)
                    continue
                
                # 获取分发项
                distribution_item = self.distribution_queue.popleft()
                self.distribution_stats['queue_size'] = len(self.distribution_queue)
                
                # 并行分发到所有目标
                futures = []
                for target in self.distribution_targets.values():
                    if target.is_active and self._should_distribute_to_target(distribution_item, target):
                        future = self.executor.submit(self._distribute_to_target, distribution_item, target)
                        futures.append(future)
                
                # 等待所有分发完成
                successful_count = 0
                for future in futures:
                    try:
                        if future.result(timeout=5):  # 5秒超时
                            successful_count += 1
                    except Exception as e:
                        logger.error(f"分发任务异常: {e}")
                
                # 更新统计
                self.distribution_stats['total_distributions'] += 1
                if successful_count > 0:
                    self.distribution_stats['successful_distributions'] += 1
                else:
                    self.distribution_stats['failed_distributions'] += 1
                
            except Exception as e:
                logger.error(f"分发工作线程异常: {e}")
                time.sleep(1)
    
    def _should_distribute_to_target(self, distribution_item: Dict, target: DistributionTarget) -> bool:
        """判断是否应该分发到目标"""
        try:
            # 检查过滤条件
            if target.filters:
                data = distribution_item['data']
                for filter_key, filter_value in target.filters.items():
                    if hasattr(data, filter_key):
                        actual_value = getattr(data, filter_key)
                        if actual_value != filter_value:
                            return False
            
            return True
            
        except Exception as e:
            logger.error(f"过滤检查异常: {e}")
            return False
    
    def _distribute_to_target(self, distribution_item: Dict, target: DistributionTarget) -> bool:
        """分发到特定目标"""
        try:
            data = distribution_item['data']
            
            if target.target_type == "console":
                # 控制台输出
                print(f"[{target.target_id}] 新数据: 期号={data.draw_id}, 号码={data.numbers}, 和值={data.result_sum}")
                
            elif target.target_type == "file":
                # 文件输出
                with open(target.endpoint, 'a', encoding='utf-8') as f:
                    f.write(f"{json.dumps(asdict(data), ensure_ascii=False)}\n")
                
            elif target.target_type == "callback" and target.callback:
                # 回调函数
                target.callback(data)
                
            elif target.target_type == "websocket":
                # WebSocket分发（模拟）
                logger.info(f"WebSocket分发到 {target.endpoint}: {data.draw_id}")
                
            elif target.target_type == "http":
                # HTTP POST分发（模拟）
                logger.info(f"HTTP分发到 {target.endpoint}: {data.draw_id}")
            
            # 更新目标状态
            target.last_update = time.time()
            target.error_count = 0
            
            return True
            
        except Exception as e:
            logger.error(f"分发到目标失败 {target.target_id}: {e}")
            target.error_count += 1
            
            # 如果错误次数过多，暂时禁用目标
            if target.error_count >= 5:
                target.is_active = False
                logger.warning(f"目标 {target.target_id} 因错误过多被禁用")
            
            return False
    
    def get_distribution_stats(self) -> Dict[str, Any]:
        """获取分发统计"""
        return {
            'distribution_stats': self.distribution_stats.copy(),
            'cache_stats': asdict(self.cache_manager.get_stats()),
            'active_targets': [
                {
                    'target_id': target.target_id,
                    'target_type': target.target_type,
                    'is_active': target.is_active,
                    'error_count': target.error_count,
                    'last_update': target.last_update
                }
                for target in self.distribution_targets.values()
            ]
        }

def main():
    """测试数据缓存和分发系统"""
    try:
        print("=== PC28数据缓存和分发系统测试 ===")
        
        # 初始化系统
        cache_manager = DataCacheManager(max_memory_mb=50)
        distributor = DataDistributor(cache_manager)
        
        # 注册分发目标
        console_target = DistributionTarget(
            target_id="console_output",
            target_type="console",
            endpoint="console",
            filters={}
        )
        
        file_target = DistributionTarget(
            target_id="file_output",
            target_type="file",
            endpoint="distributed_data.log",
            filters={'big_small': '大'}  # 只分发大数
        )
        
        distributor.register_target(console_target)
        distributor.register_target(file_target)
        
        # 创建测试数据
        test_data = [
            OptimizedLotteryData(
                draw_id="3339001",
                timestamp="2025-09-25 10:00:00",
                numbers=[1, 2, 3],
                result_sum=6,
                big_small="小",
                odd_even="双",
                dragon_tiger="虎"
            ),
            OptimizedLotteryData(
                draw_id="3339002",
                timestamp="2025-09-25 10:03:00",
                numbers=[9, 8, 7],
                result_sum=24,
                big_small="大",
                odd_even="双",
                dragon_tiger="龙"
            ),
            OptimizedLotteryData(
                draw_id="3339003",
                timestamp="2025-09-25 10:06:00",
                numbers=[5, 5, 5],
                result_sum=15,
                big_small="大",
                odd_even="单",
                dragon_tiger="和"
            )
        ]
        
        # 分发数据
        print("\n开始数据分发...")
        for data in test_data:
            success = distributor.distribute_data(data, DataType.REALTIME)
            print(f"分发数据 {data.draw_id}: {'✅' if success else '❌'}")
        
        # 等待分发完成
        time.sleep(2)
        
        # 测试缓存功能
        print("\n=== 缓存测试 ===")
        
        # 直接缓存测试
        cache_manager.put("test_key", {"test": "data"}, DataType.METADATA)
        cached_data = cache_manager.get("test_key")
        print(f"缓存测试: {'✅' if cached_data else '❌'}")
        
        # 按类型获取
        realtime_data = cache_manager.get_by_type(DataType.REALTIME, limit=5)
        print(f"实时数据缓存: {len(realtime_data)} 条记录")
        
        # 显示统计信息
        print("\n=== 系统统计 ===")
        stats = distributor.get_distribution_stats()
        
        print("分发统计:")
        for key, value in stats['distribution_stats'].items():
            print(f"  {key}: {value}")
        
        print("\n缓存统计:")
        cache_stats = stats['cache_stats']
        print(f"  总条目数: {cache_stats['total_entries']}")
        print(f"  内存使用: {cache_stats['memory_usage_mb']:.2f} MB")
        print(f"  命中率: {cache_stats['hit_rate']:.2f}%")
        print(f"  平均访问时间: {cache_stats['avg_access_time_ms']:.2f} ms")
        
        print("\n活跃目标:")
        for target in stats['active_targets']:
            print(f"  {target['target_id']} ({target['target_type']}): {'✅' if target['is_active'] else '❌'}")
        
        print("\n=== 测试完成 ===")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")

if __name__ == "__main__":
    main()