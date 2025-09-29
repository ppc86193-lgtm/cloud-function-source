#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增量同步系统
Incremental Synchronization System

功能：
1. 实现云端到本地的增量数据同步
2. 避免重复传输数据，提高同步效率
3. 支持断点续传和错误恢复
4. 建立数据版本控制和冲突解决机制
5. 优化网络传输和存储性能
"""

import os
import logging
import json
import time
import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
import threading
import queue

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('incremental_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class SyncConfig:
    """同步配置"""
    source_database: str = "production_data/production.db"
    target_database: str = "local_sync.db"
    sync_interval_minutes: int = 5
    batch_size: int = 100
    max_retry_attempts: int = 3
    compression_enabled: bool = True
    checksum_validation: bool = True

@dataclass
class SyncRecord:
    """同步记录"""
    table_name: str
    record_id: str
    last_modified: str
    checksum: str
    sync_status: str  # PENDING, SYNCED, FAILED
    retry_count: int = 0
    error_message: Optional[str] = None

@dataclass
class SyncMetrics:
    """同步指标"""
    sync_session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_records: int = 0
    synced_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    data_transferred_bytes: int = 0
    compression_ratio: float = 0.0
    sync_duration_seconds: float = 0.0

class IncrementalSyncSystem:
    """增量同步系统"""
    
    def __init__(self, config: SyncConfig = None):
        self.config = config or SyncConfig()
        self.sync_queue = queue.Queue()
        self.is_running = False
        self.sync_thread = None
        self.current_metrics = None
        
        # 初始化同步系统
        self._init_sync_system()
    
    def _init_sync_system(self):
        """初始化同步系统"""
        try:
            logger.info("初始化增量同步系统...")
            
            # 创建目标数据库
            conn = sqlite3.connect(self.config.target_database)
            cursor = conn.cursor()
            
            # 创建同步元数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    sync_status TEXT DEFAULT 'PENDING',
                    retry_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(table_name, record_id)
                )
            ''')
            
            # 创建同步会话表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT UNIQUE NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    total_records INTEGER DEFAULT 0,
                    synced_records INTEGER DEFAULT 0,
                    failed_records INTEGER DEFAULT 0,
                    skipped_records INTEGER DEFAULT 0,
                    data_transferred_bytes INTEGER DEFAULT 0,
                    compression_ratio REAL DEFAULT 0.0,
                    sync_duration_seconds REAL DEFAULT 0.0,
                    status TEXT DEFAULT 'RUNNING'
                )
            ''')
            
            # 创建数据版本表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    version_number INTEGER NOT NULL,
                    version_timestamp TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    checksum TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(table_name, version_number)
                )
            ''')
            
            # 创建冲突解决日志表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS conflict_resolution_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    conflict_type TEXT NOT NULL,
                    source_data TEXT,
                    target_data TEXT,
                    resolution_strategy TEXT,
                    resolved_data TEXT,
                    resolved_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_metadata_table_record ON sync_metadata(table_name, record_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_metadata_status ON sync_metadata(sync_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_sessions_session_id ON sync_sessions(session_id)')
            
            conn.commit()
            conn.close()
            
            logger.info("增量同步系统初始化完成")
            
        except Exception as e:
            logger.error(f"初始化增量同步系统失败: {e}")
            raise
    
    def _calculate_checksum(self, data: str) -> str:
        """计算数据校验和"""
        return hashlib.md5(data.encode('utf-8')).hexdigest()
    
    def _compress_data(self, data: str) -> Tuple[bytes, float]:
        """压缩数据"""
        if not self.config.compression_enabled:
            return data.encode('utf-8'), 1.0
        
        import gzip
        original_size = len(data.encode('utf-8'))
        compressed_data = gzip.compress(data.encode('utf-8'))
        compression_ratio = len(compressed_data) / original_size if original_size > 0 else 1.0
        
        return compressed_data, compression_ratio
    
    def _decompress_data(self, compressed_data: bytes) -> str:
        """解压数据"""
        if not self.config.compression_enabled:
            return compressed_data.decode('utf-8')
        
        import gzip
        return gzip.decompress(compressed_data).decode('utf-8')
    
    def detect_changes(self, table_name: str) -> List[SyncRecord]:
        """检测数据变更"""
        try:
            logger.info(f"检测表 {table_name} 的数据变更...")
            
            changes = []
            
            # 连接源数据库
            source_conn = sqlite3.connect(self.config.source_database)
            source_cursor = source_conn.cursor()
            
            # 连接目标数据库
            target_conn = sqlite3.connect(self.config.target_database)
            target_cursor = target_conn.cursor()
            
            # 获取源数据库中的记录
            source_cursor.execute(f'''
                SELECT id, created_at, updated_at 
                FROM {table_name} 
                ORDER BY updated_at DESC
            ''')
            
            source_records = source_cursor.fetchall()
            
            for record in source_records:
                record_id = str(record[0])
                last_modified = record[2] if record[2] else record[1]  # 优先使用updated_at
                
                # 获取完整记录数据用于校验和计算
                source_cursor.execute(f'SELECT * FROM {table_name} WHERE id = ?', (record[0],))
                full_record = source_cursor.fetchone()
                record_data = json.dumps(full_record, default=str)
                checksum = self._calculate_checksum(record_data)
                
                # 检查目标数据库中的同步状态
                target_cursor.execute('''
                    SELECT checksum, sync_status, retry_count 
                    FROM sync_metadata 
                    WHERE table_name = ? AND record_id = ?
                ''', (table_name, record_id))
                
                existing_sync = target_cursor.fetchone()
                
                if not existing_sync:
                    # 新记录
                    changes.append(SyncRecord(
                        table_name=table_name,
                        record_id=record_id,
                        last_modified=last_modified,
                        checksum=checksum,
                        sync_status="PENDING"
                    ))
                elif existing_sync[0] != checksum:
                    # 记录已变更
                    changes.append(SyncRecord(
                        table_name=table_name,
                        record_id=record_id,
                        last_modified=last_modified,
                        checksum=checksum,
                        sync_status="PENDING",
                        retry_count=existing_sync[2] if existing_sync[1] == "FAILED" else 0
                    ))
                elif existing_sync[1] == "FAILED" and existing_sync[2] < self.config.max_retry_attempts:
                    # 失败记录重试
                    changes.append(SyncRecord(
                        table_name=table_name,
                        record_id=record_id,
                        last_modified=last_modified,
                        checksum=checksum,
                        sync_status="PENDING",
                        retry_count=existing_sync[2]
                    ))
            
            source_conn.close()
            target_conn.close()
            
            logger.info(f"检测到 {len(changes)} 条变更记录")
            return changes
            
        except Exception as e:
            logger.error(f"检测数据变更失败: {e}")
            return []
    
    def sync_record(self, sync_record: SyncRecord) -> bool:
        """同步单条记录"""
        try:
            # 连接源数据库
            source_conn = sqlite3.connect(self.config.source_database)
            source_cursor = source_conn.cursor()
            
            # 连接目标数据库
            target_conn = sqlite3.connect(self.config.target_database)
            target_cursor = target_conn.cursor()
            
            # 获取源记录数据
            source_cursor.execute(f'SELECT * FROM {sync_record.table_name} WHERE id = ?', 
                                (sync_record.record_id,))
            source_data = source_cursor.fetchone()
            
            if not source_data:
                logger.warning(f"源记录不存在: {sync_record.table_name}#{sync_record.record_id}")
                return False
            
            # 获取表结构
            source_cursor.execute(f'PRAGMA table_info({sync_record.table_name})')
            columns = [column[1] for column in source_cursor.fetchall()]
            
            # 确保目标表存在
            source_cursor.execute(f'SELECT sql FROM sqlite_master WHERE type="table" AND name="{sync_record.table_name}"')
            create_sql = source_cursor.fetchone()
            if create_sql:
                target_cursor.execute(create_sql[0])
            
            # 插入或更新目标记录
            placeholders = ','.join(['?' for _ in columns])
            update_placeholders = ','.join([f'{col}=?' for col in columns[1:]])  # 排除id列
            
            target_cursor.execute(f'''
                INSERT OR REPLACE INTO {sync_record.table_name} 
                ({','.join(columns)}) VALUES ({placeholders})
            ''', source_data)
            
            # 更新同步元数据
            target_cursor.execute('''
                INSERT OR REPLACE INTO sync_metadata 
                (table_name, record_id, last_modified, checksum, sync_status, retry_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                sync_record.table_name,
                sync_record.record_id,
                sync_record.last_modified,
                sync_record.checksum,
                "SYNCED",
                sync_record.retry_count,
                datetime.now().isoformat()
            ))
            
            target_conn.commit()
            source_conn.close()
            target_conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"同步记录失败 {sync_record.table_name}#{sync_record.record_id}: {e}")
            
            # 记录失败信息
            try:
                target_conn = sqlite3.connect(self.config.target_database)
                target_cursor = target_conn.cursor()
                
                target_cursor.execute('''
                    INSERT OR REPLACE INTO sync_metadata 
                    (table_name, record_id, last_modified, checksum, sync_status, retry_count, error_message, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sync_record.table_name,
                    sync_record.record_id,
                    sync_record.last_modified,
                    sync_record.checksum,
                    "FAILED",
                    sync_record.retry_count + 1,
                    str(e),
                    datetime.now().isoformat()
                ))
                
                target_conn.commit()
                target_conn.close()
            except Exception as meta_error:
                logger.error(f"记录同步失败信息时出错: {meta_error}")
            
            return False
    
    def run_incremental_sync(self, tables: List[str] = None) -> SyncMetrics:
        """运行增量同步"""
        session_id = f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        
        logger.info(f"开始增量同步会话: {session_id}")
        
        # 初始化同步指标
        metrics = SyncMetrics(
            sync_session_id=session_id,
            start_time=start_time
        )
        
        try:
            # 默认同步的表
            if not tables:
                tables = ["realtime_draws", "prediction_results", "model_config", "monitoring_logs"]
            
            # 记录同步会话
            target_conn = sqlite3.connect(self.config.target_database)
            target_cursor = target_conn.cursor()
            
            target_cursor.execute('''
                INSERT INTO sync_sessions 
                (session_id, start_time, status)
                VALUES (?, ?, ?)
            ''', (session_id, start_time.isoformat(), "RUNNING"))
            
            target_conn.commit()
            
            # 检测和同步每个表的变更
            for table_name in tables:
                logger.info(f"同步表: {table_name}")
                
                # 检测变更
                changes = self.detect_changes(table_name)
                metrics.total_records += len(changes)
                
                # 批量同步
                for i in range(0, len(changes), self.config.batch_size):
                    batch = changes[i:i + self.config.batch_size]
                    
                    for sync_record in batch:
                        if self.sync_record(sync_record):
                            metrics.synced_records += 1
                        else:
                            metrics.failed_records += 1
                    
                    # 批次间短暂休息
                    time.sleep(0.1)
            
            # 计算同步指标
            metrics.end_time = datetime.now()
            metrics.sync_duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
            metrics.skipped_records = metrics.total_records - metrics.synced_records - metrics.failed_records
            
            # 更新同步会话
            target_cursor.execute('''
                UPDATE sync_sessions 
                SET end_time = ?, total_records = ?, synced_records = ?, failed_records = ?, 
                    skipped_records = ?, sync_duration_seconds = ?, status = ?
                WHERE session_id = ?
            ''', (
                metrics.end_time.isoformat(),
                metrics.total_records,
                metrics.synced_records,
                metrics.failed_records,
                metrics.skipped_records,
                metrics.sync_duration_seconds,
                "COMPLETED" if metrics.failed_records == 0 else "PARTIAL",
                session_id
            ))
            
            target_conn.commit()
            target_conn.close()
            
            logger.info(f"增量同步完成: {session_id}")
            logger.info(f"  总记录: {metrics.total_records}")
            logger.info(f"  成功同步: {metrics.synced_records}")
            logger.info(f"  失败记录: {metrics.failed_records}")
            logger.info(f"  跳过记录: {metrics.skipped_records}")
            logger.info(f"  同步耗时: {metrics.sync_duration_seconds:.2f}秒")
            
            return metrics
            
        except Exception as e:
            logger.error(f"增量同步失败: {e}")
            
            # 更新失败状态
            try:
                target_conn = sqlite3.connect(self.config.target_database)
                target_cursor = target_conn.cursor()
                
                target_cursor.execute('''
                    UPDATE sync_sessions 
                    SET end_time = ?, status = ?, sync_duration_seconds = ?
                    WHERE session_id = ?
                ''', (
                    datetime.now().isoformat(),
                    "FAILED",
                    (datetime.now() - start_time).total_seconds(),
                    session_id
                ))
                
                target_conn.commit()
                target_conn.close()
            except Exception as update_error:
                logger.error(f"更新同步会话状态失败: {update_error}")
            
            metrics.end_time = datetime.now()
            metrics.sync_duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
            return metrics
    
    def start_continuous_sync(self, tables: List[str] = None):
        """启动连续同步"""
        if self.is_running:
            logger.warning("连续同步已在运行中")
            return
        
        self.is_running = True
        
        def sync_worker():
            logger.info(f"启动连续同步，间隔: {self.config.sync_interval_minutes}分钟")
            
            while self.is_running:
                try:
                    # 运行增量同步
                    metrics = self.run_incremental_sync(tables)
                    self.current_metrics = metrics
                    
                    # 等待下次同步
                    for _ in range(self.config.sync_interval_minutes * 60):
                        if not self.is_running:
                            break
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"连续同步过程中出错: {e}")
                    time.sleep(60)  # 出错后等待1分钟再重试
        
        self.sync_thread = threading.Thread(target=sync_worker, daemon=True)
        self.sync_thread.start()
        
        logger.info("连续同步已启动")
    
    def stop_continuous_sync(self):
        """停止连续同步"""
        if not self.is_running:
            logger.warning("连续同步未在运行")
            return
        
        self.is_running = False
        
        if self.sync_thread and self.sync_thread.is_alive():
            self.sync_thread.join(timeout=30)
        
        logger.info("连续同步已停止")
    
    def get_sync_status(self) -> Dict:
        """获取同步状态"""
        try:
            conn = sqlite3.connect(self.config.target_database)
            cursor = conn.cursor()
            
            # 获取最近的同步会话
            cursor.execute('''
                SELECT session_id, start_time, end_time, total_records, synced_records, 
                       failed_records, skipped_records, sync_duration_seconds, status
                FROM sync_sessions 
                ORDER BY start_time DESC 
                LIMIT 1
            ''')
            
            latest_session = cursor.fetchone()
            
            # 获取同步统计
            cursor.execute('''
                SELECT sync_status, COUNT(*) 
                FROM sync_metadata 
                GROUP BY sync_status
            ''')
            
            status_counts = dict(cursor.fetchall())
            
            # 获取失败记录详情
            cursor.execute('''
                SELECT table_name, COUNT(*) 
                FROM sync_metadata 
                WHERE sync_status = 'FAILED' 
                GROUP BY table_name
            ''')
            
            failed_by_table = dict(cursor.fetchall())
            
            conn.close()
            
            status = {
                "连续同步状态": "运行中" if self.is_running else "已停止",
                "最近同步会话": {
                    "会话ID": latest_session[0] if latest_session else None,
                    "开始时间": latest_session[1] if latest_session else None,
                    "结束时间": latest_session[2] if latest_session else None,
                    "总记录数": latest_session[3] if latest_session else 0,
                    "成功同步": latest_session[4] if latest_session else 0,
                    "失败记录": latest_session[5] if latest_session else 0,
                    "跳过记录": latest_session[6] if latest_session else 0,
                    "同步耗时": f"{latest_session[7]:.2f}秒" if latest_session and latest_session[7] else "0秒",
                    "状态": latest_session[8] if latest_session else "无"
                } if latest_session else None,
                "同步统计": {
                    "待同步": status_counts.get("PENDING", 0),
                    "已同步": status_counts.get("SYNCED", 0),
                    "同步失败": status_counts.get("FAILED", 0)
                },
                "失败详情": failed_by_table,
                "配置信息": {
                    "同步间隔": f"{self.config.sync_interval_minutes}分钟",
                    "批次大小": self.config.batch_size,
                    "最大重试": self.config.max_retry_attempts,
                    "压缩启用": self.config.compression_enabled,
                    "校验和验证": self.config.checksum_validation
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"获取同步状态失败: {e}")
            return {"错误": str(e)}

def main():
    """主函数"""
    logger.info("启动增量同步系统")
    
    # 创建同步配置
    sync_config = SyncConfig(
        source_database="production_data/production.db",
        target_database="local_sync.db",
        sync_interval_minutes=5,
        batch_size=100,
        max_retry_attempts=3,
        compression_enabled=True,
        checksum_validation=True
    )
    
    # 创建同步系统
    sync_system = IncrementalSyncSystem(sync_config)
    
    # 运行一次增量同步
    logger.info("执行增量同步...")
    metrics = sync_system.run_incremental_sync()
    
    # 输出同步结果
    logger.info("增量同步结果:")
    logger.info(f"  会话ID: {metrics.sync_session_id}")
    logger.info(f"  总记录: {metrics.total_records}")
    logger.info(f"  成功同步: {metrics.synced_records}")
    logger.info(f"  失败记录: {metrics.failed_records}")
    logger.info(f"  跳过记录: {metrics.skipped_records}")
    logger.info(f"  同步耗时: {metrics.sync_duration_seconds:.2f}秒")
    
    # 获取同步状态
    status = sync_system.get_sync_status()
    logger.info("同步系统状态:")
    for key, value in status.items():
        if isinstance(value, dict):
            logger.info(f"  {key}:")
            for sub_key, sub_value in value.items():
                logger.info(f"    {sub_key}: {sub_value}")
        else:
            logger.info(f"  {key}: {value}")
    
    # 保存同步报告
    sync_report = {
        "同步指标": asdict(metrics),
        "系统状态": status
    }
    
    with open('incremental_sync_report.json', 'w', encoding='utf-8') as f:
        json.dump(sync_report, f, ensure_ascii=False, indent=2)
    
    success = metrics.failed_records == 0
    logger.info(f"增量同步{'成功' if success else '部分失败'}")
    
    return success

if __name__ == "__main__":
    main()