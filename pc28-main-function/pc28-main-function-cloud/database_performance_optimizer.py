#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库写入性能优化和并发控制机制
防止数据污染，优化写入性能
"""

import sqlite3
import threading
import time
import queue
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Callable
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import logging
from collections import defaultdict, deque

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class WriteRequest:
    """写入请求"""
    data: Dict[str, Any]
    table_name: str
    priority: int = 1  # 1-10, 10为最高优先级
    callback: Optional[Callable] = None
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

@dataclass
class WriteResult:
    """写入结果"""
    success: bool
    record_id: Optional[str]
    error_message: Optional[str]
    execution_time: float
    retry_count: int = 0

@dataclass
class PerformanceMetrics:
    """性能指标"""
    total_writes: int = 0
    successful_writes: int = 0
    failed_writes: int = 0
    average_write_time: float = 0.0
    peak_write_time: float = 0.0
    queue_size: int = 0
    active_connections: int = 0
    cache_hit_rate: float = 0.0
    throughput_per_second: float = 0.0

@dataclass
class ConnectionPoolConfig:
    """连接池配置"""
    max_connections: int = 10
    min_connections: int = 2
    connection_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 0.1
    batch_size: int = 100
    queue_timeout: int = 5

class ConnectionPool:
    """数据库连接池"""
    
    def __init__(self, db_path: str, config: ConnectionPoolConfig):
        self.db_path = db_path
        self.config = config
        self.connections = queue.Queue(maxsize=config.max_connections)
        self.active_connections = 0
        self.lock = threading.Lock()
        
        # 初始化最小连接数
        for _ in range(config.min_connections):
            conn = self._create_connection()
            if conn:
                self.connections.put(conn)
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """创建数据库连接"""
        try:
            conn = sqlite3.connect(
                self.db_path, 
                timeout=self.config.connection_timeout,
                check_same_thread=False
            )
            conn.execute("PRAGMA journal_mode=WAL")  # 启用WAL模式提高并发性能
            conn.execute("PRAGMA synchronous=NORMAL")  # 平衡性能和安全性
            conn.execute("PRAGMA cache_size=10000")  # 增加缓存大小
            conn.execute("PRAGMA temp_store=MEMORY")  # 临时表存储在内存中
            return conn
        except Exception as e:
            logger.error(f"创建数据库连接失败: {e}")
            return None
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接"""
        conn = None
        try:
            # 尝试从池中获取连接
            try:
                conn = self.connections.get(timeout=self.config.queue_timeout)
            except queue.Empty:
                # 如果池中没有连接，创建新连接
                with self.lock:
                    if self.active_connections < self.config.max_connections:
                        conn = self._create_connection()
                        if conn:
                            self.active_connections += 1
                    else:
                        # 等待连接可用
                        conn = self.connections.get(timeout=self.config.queue_timeout)
            
            if conn is None:
                raise Exception("无法获取数据库连接")
            
            yield conn
            
        except Exception as e:
            logger.error(f"数据库连接错误: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                try:
                    # 将连接返回池中
                    self.connections.put(conn, timeout=1)
                except queue.Full:
                    # 如果池已满，关闭连接
                    conn.close()
                    with self.lock:
                        self.active_connections -= 1
    
    def close_all(self):
        """关闭所有连接"""
        while not self.connections.empty():
            try:
                conn = self.connections.get_nowait()
                conn.close()
            except queue.Empty:
                break
        self.active_connections = 0

class WriteCache:
    """写入缓存"""
    
    def __init__(self, max_size: int = 1000, ttl: int = 300):
        self.max_size = max_size
        self.ttl = ttl
        self.cache = {}
        self.access_times = deque()
        self.lock = threading.RLock()
    
    def _generate_key(self, data: Dict[str, Any]) -> str:
        """生成缓存键"""
        return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
    
    def get(self, data: Dict[str, Any]) -> Optional[WriteResult]:
        """获取缓存结果"""
        key = self._generate_key(data)
        with self.lock:
            if key in self.cache:
                result, timestamp = self.cache[key]
                if time.time() - timestamp < self.ttl:
                    return result
                else:
                    del self.cache[key]
        return None
    
    def put(self, data: Dict[str, Any], result: WriteResult):
        """存储缓存结果"""
        key = self._generate_key(data)
        with self.lock:
            # 清理过期缓存
            self._cleanup_expired()
            
            # 如果缓存已满，删除最旧的条目
            if len(self.cache) >= self.max_size:
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
            
            self.cache[key] = (result, time.time())
    
    def _cleanup_expired(self):
        """清理过期缓存"""
        current_time = time.time()
        expired_keys = [
            key for key, (_, timestamp) in self.cache.items()
            if current_time - timestamp >= self.ttl
        ]
        for key in expired_keys:
            del self.cache[key]
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        with self.lock:
            return {
                "cache_size": len(self.cache),
                "max_size": self.max_size,
                "ttl": self.ttl
            }

class DatabasePerformanceOptimizer:
    """数据库性能优化器"""
    
    def __init__(self, db_path: str = "optimized_lottery.db", config: Optional[ConnectionPoolConfig] = None):
        self.db_path = db_path
        self.config = config or ConnectionPoolConfig()
        self.connection_pool = ConnectionPool(db_path, self.config)
        self.write_cache = WriteCache()
        
        # 写入队列和工作线程
        self.write_queue = queue.PriorityQueue()
        self.batch_queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_connections)
        
        # 性能指标
        self.metrics = PerformanceMetrics()
        self.metrics_lock = threading.Lock()
        self.write_times = deque(maxlen=1000)  # 保存最近1000次写入时间
        
        # 控制标志
        self.running = True
        self.batch_thread = None
        
        # 初始化数据库
        self.init_database()
        
        # 启动批处理线程
        self.start_batch_processor()
    
    def init_database(self):
        """初始化数据库表"""
        with self.connection_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # 创建开奖数据表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lottery_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id TEXT UNIQUE NOT NULL,
                    numbers TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    source TEXT NOT NULL,
                    data_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            
            # 创建性能监控表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_type TEXT NOT NULL,
                    execution_time REAL NOT NULL,
                    success BOOLEAN NOT NULL,
                    error_message TEXT,
                    timestamp TEXT NOT NULL
                )
            """)
            
            # 创建索引优化查询性能
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_draw_id ON lottery_data(draw_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON lottery_data(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_hash ON lottery_data(data_hash)")
            
            conn.commit()
    
    def start_batch_processor(self):
        """启动批处理线程"""
        self.batch_thread = threading.Thread(target=self._batch_processor, daemon=True)
        self.batch_thread.start()
    
    def _batch_processor(self):
        """批处理器"""
        batch = []
        last_flush = time.time()
        
        while self.running:
            try:
                # 收集批处理数据
                try:
                    _, request = self.write_queue.get(timeout=1)
                    batch.append(request)
                except queue.Empty:
                    pass
                
                # 检查是否需要刷新批处理
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.config.batch_size or
                    (batch and current_time - last_flush > 1.0)  # 1秒超时
                )
                
                if should_flush and batch:
                    self._execute_batch(batch)
                    batch = []
                    last_flush = current_time
                    
            except Exception as e:
                logger.error(f"批处理器错误: {e}")
                time.sleep(0.1)
    
    def _execute_batch(self, requests: List[WriteRequest]):
        """执行批处理写入"""
        start_time = time.time()
        
        try:
            with self.connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # 按表名分组
                table_groups = defaultdict(list)
                for request in requests:
                    table_groups[request.table_name].append(request)
                
                # 批量插入每个表
                for table_name, table_requests in table_groups.items():
                    if table_name == "lottery_data":
                        self._batch_insert_lottery_data(cursor, table_requests)
                    elif table_name == "performance_logs":
                        self._batch_insert_performance_logs(cursor, table_requests)
                
                conn.commit()
                
                # 更新性能指标
                execution_time = time.time() - start_time
                with self.metrics_lock:
                    self.metrics.successful_writes += len(requests)
                    self.write_times.append(execution_time)
                    self._update_metrics()
                
                # 执行回调
                for request in requests:
                    if request.callback:
                        try:
                            request.callback(WriteResult(
                                success=True,
                                record_id=None,
                                error_message=None,
                                execution_time=execution_time
                            ))
                        except Exception as e:
                            logger.error(f"回调执行错误: {e}")
                            
        except Exception as e:
            logger.error(f"批处理执行错误: {e}")
            
            # 更新失败指标
            with self.metrics_lock:
                self.metrics.failed_writes += len(requests)
                self._update_metrics()
            
            # 执行错误回调
            for request in requests:
                if request.callback:
                    try:
                        request.callback(WriteResult(
                            success=False,
                            record_id=None,
                            error_message=str(e),
                            execution_time=time.time() - start_time
                        ))
                    except Exception as callback_error:
                        logger.error(f"错误回调执行失败: {callback_error}")
    
    def _batch_insert_lottery_data(self, cursor: sqlite3.Cursor, requests: List[WriteRequest]):
        """批量插入开奖数据"""
        values = []
        for request in requests:
            data = request.data
            data_hash = hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
            
            values.append((
                data.get('draw_id'),
                json.dumps(data.get('numbers', [])),
                data.get('timestamp'),
                data.get('source', 'unknown'),
                data_hash,
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
        
        cursor.executemany("""
            INSERT OR REPLACE INTO lottery_data 
            (draw_id, numbers, timestamp, source, data_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, values)
    
    def _batch_insert_performance_logs(self, cursor: sqlite3.Cursor, requests: List[WriteRequest]):
        """批量插入性能日志"""
        values = []
        for request in requests:
            data = request.data
            values.append((
                data.get('operation_type'),
                data.get('execution_time'),
                data.get('success'),
                data.get('error_message'),
                datetime.now().isoformat()
            ))
        
        cursor.executemany("""
            INSERT INTO performance_logs 
            (operation_type, execution_time, success, error_message, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, values)
    
    def _update_metrics(self):
        """更新性能指标"""
        self.metrics.total_writes = self.metrics.successful_writes + self.metrics.failed_writes
        
        if self.write_times:
            self.metrics.average_write_time = sum(self.write_times) / len(self.write_times)
            self.metrics.peak_write_time = max(self.write_times)
        
        self.metrics.queue_size = self.write_queue.qsize()
        self.metrics.active_connections = self.connection_pool.active_connections
        
        # 计算吞吐量（每秒写入数）
        if self.write_times and len(self.write_times) > 1:
            time_span = sum(self.write_times)
            if time_span > 0:
                self.metrics.throughput_per_second = len(self.write_times) / time_span
    
    def write_data(self, data: Dict[str, Any], table_name: str = "lottery_data", 
                   priority: int = 1, callback: Optional[Callable] = None) -> bool:
        """写入数据"""
        try:
            # 检查缓存
            cached_result = self.write_cache.get(data)
            if cached_result and cached_result.success:
                if callback:
                    callback(cached_result)
                return True
            
            # 创建写入请求
            request = WriteRequest(
                data=data,
                table_name=table_name,
                priority=priority,
                callback=callback
            )
            
            # 添加到队列（优先级队列，数字越小优先级越高）
            self.write_queue.put((-priority, request))
            
            return True
            
        except Exception as e:
            logger.error(f"写入数据失败: {e}")
            if callback:
                callback(WriteResult(
                    success=False,
                    record_id=None,
                    error_message=str(e),
                    execution_time=0
                ))
            return False
    
    def write_batch(self, data_list: List[Dict[str, Any]], table_name: str = "lottery_data", 
                    priority: int = 1) -> List[bool]:
        """批量写入数据"""
        results = []
        for data in data_list:
            result = self.write_data(data, table_name, priority)
            results.append(result)
        return results
    
    def get_performance_metrics(self) -> PerformanceMetrics:
        """获取性能指标"""
        with self.metrics_lock:
            self._update_metrics()
            return PerformanceMetrics(
                total_writes=self.metrics.total_writes,
                successful_writes=self.metrics.successful_writes,
                failed_writes=self.metrics.failed_writes,
                average_write_time=self.metrics.average_write_time,
                peak_write_time=self.metrics.peak_write_time,
                queue_size=self.metrics.queue_size,
                active_connections=self.metrics.active_connections,
                cache_hit_rate=self.metrics.cache_hit_rate,
                throughput_per_second=self.metrics.throughput_per_second
            )
    
    def optimize_performance(self) -> Dict[str, Any]:
        """性能优化建议"""
        metrics = self.get_performance_metrics()
        recommendations = []
        
        # 分析写入性能
        if metrics.average_write_time > 0.1:
            recommendations.append("写入时间较长，建议增加批处理大小")
        
        if metrics.queue_size > 100:
            recommendations.append("写入队列积压，建议增加工作线程数")
        
        if metrics.failed_writes / max(metrics.total_writes, 1) > 0.05:
            recommendations.append("写入失败率较高，建议检查数据库连接和数据质量")
        
        if metrics.active_connections >= self.config.max_connections * 0.8:
            recommendations.append("连接池使用率较高，建议增加最大连接数")
        
        return {
            "current_metrics": asdict(metrics),
            "recommendations": recommendations,
            "optimization_score": self._calculate_optimization_score(metrics)
        }
    
    def _calculate_optimization_score(self, metrics: PerformanceMetrics) -> float:
        """计算优化分数"""
        score = 100.0
        
        # 写入成功率
        if metrics.total_writes > 0:
            success_rate = metrics.successful_writes / metrics.total_writes
            score *= success_rate
        
        # 写入速度
        if metrics.average_write_time > 0.05:
            score *= max(0.5, 1 - (metrics.average_write_time - 0.05) / 0.1)
        
        # 队列积压
        if metrics.queue_size > 50:
            score *= max(0.7, 1 - (metrics.queue_size - 50) / 100)
        
        return min(100.0, max(0.0, score))
    
    def shutdown(self):
        """关闭优化器"""
        self.running = False
        
        # 等待批处理线程结束
        if self.batch_thread and self.batch_thread.is_alive():
            self.batch_thread.join(timeout=5)
        
        # 关闭线程池
        self.executor.shutdown(wait=True)
        
        # 关闭连接池
        self.connection_pool.close_all()

def main():
    """测试数据库性能优化器"""
    optimizer = DatabasePerformanceOptimizer()
    
    print("=== 数据库性能优化测试 ===")
    
    # 测试数据
    test_data = [
        {
            "draw_id": f"20241201{i:03d}",
            "numbers": [1, 3, 7, 2, 8],
            "timestamp": f"2024-12-01T10:{i:02d}:00Z",
            "source": "performance_test"
        }
        for i in range(100)
    ]
    
    # 性能测试
    start_time = time.time()
    
    print(f"\n开始写入 {len(test_data)} 条记录...")
    results = optimizer.write_batch(test_data)
    
    # 等待写入完成
    time.sleep(2)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"写入完成，总耗时: {total_time:.2f}秒")
    print(f"平均每条记录: {total_time/len(test_data)*1000:.2f}毫秒")
    
    # 获取性能指标
    metrics = optimizer.get_performance_metrics()
    print(f"\n性能指标:")
    print(f"  总写入数: {metrics.total_writes}")
    print(f"  成功写入: {metrics.successful_writes}")
    print(f"  失败写入: {metrics.failed_writes}")
    print(f"  平均写入时间: {metrics.average_write_time*1000:.2f}毫秒")
    print(f"  峰值写入时间: {metrics.peak_write_time*1000:.2f}毫秒")
    print(f"  队列大小: {metrics.queue_size}")
    print(f"  活跃连接数: {metrics.active_connections}")
    print(f"  吞吐量: {metrics.throughput_per_second:.2f}条/秒")
    
    # 获取优化建议
    optimization = optimizer.optimize_performance()
    print(f"\n优化建议:")
    print(f"  优化分数: {optimization['optimization_score']:.2f}%")
    for recommendation in optimization['recommendations']:
        print(f"  - {recommendation}")
    
    # 关闭优化器
    optimizer.shutdown()
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()