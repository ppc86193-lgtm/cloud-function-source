#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据去重系统
实现基于多维度的数据去重机制，防止重复数据写入
"""

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import defaultdict
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DuplicateRecord:
    """重复记录信息"""
    original_hash: str
    duplicate_hash: str
    original_timestamp: datetime
    duplicate_timestamp: datetime
    duplicate_fields: List[str]
    similarity_score: float

@dataclass
class DeduplicationStats:
    """去重统计信息"""
    total_processed: int = 0
    duplicates_found: int = 0
    duplicates_blocked: int = 0
    unique_records: int = 0
    deduplication_rate: float = 0.0
    processing_time: float = 0.0

@dataclass
class DeduplicationRule:
    """去重规则配置"""
    rule_name: str
    key_fields: List[str]  # 用于生成唯一键的字段
    time_window: int  # 时间窗口（秒）
    similarity_threshold: float  # 相似度阈值
    enabled: bool = True

class DataDeduplicationSystem:
    """数据去重系统"""
    
    def __init__(self, db_path: str = "deduplication.db"):
        self.db_path = db_path
        self.lock = threading.RLock()
        self.memory_cache: Dict[str, Dict] = {}
        self.hash_index: Dict[str, str] = {}  # hash -> record_id
        self.time_index: Dict[str, List[Tuple[datetime, str]]] = defaultdict(list)  # date -> [(time, hash)]
        
        # 默认去重规则
        self.rules = [
            DeduplicationRule(
                rule_name="draw_id_based",
                key_fields=["draw_id", "issue"],
                time_window=3600,  # 1小时
                similarity_threshold=0.95
            ),
            DeduplicationRule(
                rule_name="timestamp_based",
                key_fields=["timestamp", "draw_time"],
                time_window=300,   # 5分钟
                similarity_threshold=0.90
            ),
            DeduplicationRule(
                rule_name="content_based",
                key_fields=["numbers", "sum_value", "big_small", "odd_even"],
                time_window=1800,  # 30分钟
                similarity_threshold=0.85
            )
        ]
        
        self.stats = DeduplicationStats()
        self._init_database()
    
    def _init_database(self):
        """初始化数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS record_hashes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        record_hash TEXT UNIQUE NOT NULL,
                        record_id TEXT NOT NULL,
                        key_fields TEXT NOT NULL,
                        content_hash TEXT NOT NULL,
                        timestamp DATETIME NOT NULL,
                        rule_name TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS duplicate_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        original_hash TEXT NOT NULL,
                        duplicate_hash TEXT NOT NULL,
                        original_timestamp DATETIME NOT NULL,
                        duplicate_timestamp DATETIME NOT NULL,
                        duplicate_fields TEXT NOT NULL,
                        similarity_score REAL NOT NULL,
                        rule_name TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_record_hash ON record_hashes(record_hash)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_timestamp ON record_hashes(timestamp)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_content_hash ON record_hashes(content_hash)
                """)
                
                conn.commit()
                logger.info("数据库初始化完成")
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def generate_record_hash(self, record: Dict, rule: DeduplicationRule) -> str:
        """生成记录哈希值"""
        try:
            # 提取关键字段
            key_data = {}
            for field in rule.key_fields:
                if field in record:
                    value = record[field]
                    # 标准化数值类型
                    if isinstance(value, (int, float)):
                        key_data[field] = str(value)
                    elif isinstance(value, list):
                        key_data[field] = ','.join(map(str, sorted(value)))
                    else:
                        key_data[field] = str(value).strip().lower()
            
            # 生成哈希
            key_string = json.dumps(key_data, sort_keys=True, ensure_ascii=False)
            return hashlib.md5(key_string.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"生成记录哈希失败: {e}")
            return ""
    
    def generate_content_hash(self, record: Dict) -> str:
        """生成内容哈希值"""
        try:
            # 排除时间戳等变化字段
            content_data = {k: v for k, v in record.items() 
                          if k not in ['timestamp', 'created_at', 'updated_at', 'id']}
            content_string = json.dumps(content_data, sort_keys=True, ensure_ascii=False)
            return hashlib.sha256(content_string.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"生成内容哈希失败: {e}")
            return ""
    
    def calculate_similarity(self, record1: Dict, record2: Dict) -> float:
        """计算两条记录的相似度"""
        try:
            # 获取共同字段
            common_fields = set(record1.keys()) & set(record2.keys())
            if not common_fields:
                return 0.0
            
            matches = 0
            total = len(common_fields)
            
            for field in common_fields:
                val1, val2 = record1[field], record2[field]
                
                # 数值比较
                if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                    if abs(val1 - val2) < 0.001:  # 浮点数精度
                        matches += 1
                # 列表比较
                elif isinstance(val1, list) and isinstance(val2, list):
                    if sorted(val1) == sorted(val2):
                        matches += 1
                # 字符串比较
                else:
                    if str(val1).strip().lower() == str(val2).strip().lower():
                        matches += 1
            
            return matches / total
        except Exception as e:
            logger.error(f"计算相似度失败: {e}")
            return 0.0
    
    def is_duplicate(self, record: Dict, record_id: str) -> Tuple[bool, Optional[DuplicateRecord]]:
        """检查是否为重复记录"""
        start_time = time.time()
        
        try:
            with self.lock:
                current_time = datetime.now()
                
                for rule in self.rules:
                    if not rule.enabled:
                        continue
                    
                    # 生成记录哈希
                    record_hash = self.generate_record_hash(record, rule)
                    if not record_hash:
                        continue
                    
                    # 检查内存缓存
                    if record_hash in self.memory_cache:
                        cached_record = self.memory_cache[record_hash]
                        similarity = self.calculate_similarity(record, cached_record)
                        
                        if similarity >= rule.similarity_threshold:
                            duplicate_record = DuplicateRecord(
                                original_hash=record_hash,
                                duplicate_hash=self.generate_content_hash(record),
                                original_timestamp=cached_record.get('timestamp', current_time),
                                duplicate_timestamp=current_time,
                                duplicate_fields=rule.key_fields,
                                similarity_score=similarity
                            )
                            
                            self._record_duplicate(duplicate_record, rule.rule_name)
                            return True, duplicate_record
                    
                    # 检查数据库
                    with sqlite3.connect(self.db_path) as conn:
                        cursor = conn.execute("""
                            SELECT record_id, content_hash, timestamp 
                            FROM record_hashes 
                            WHERE record_hash = ? AND rule_name = ?
                            AND timestamp > ?
                        """, (
                            record_hash, 
                            rule.rule_name,
                            current_time - timedelta(seconds=rule.time_window)
                        ))
                        
                        existing = cursor.fetchone()
                        if existing:
                            # 找到潜在重复，进行详细比较
                            similarity = 0.95  # 基于哈希匹配的高相似度
                            
                            if similarity >= rule.similarity_threshold:
                                duplicate_record = DuplicateRecord(
                                    original_hash=record_hash,
                                    duplicate_hash=self.generate_content_hash(record),
                                    original_timestamp=datetime.fromisoformat(existing[2]),
                                    duplicate_timestamp=current_time,
                                    duplicate_fields=rule.key_fields,
                                    similarity_score=similarity
                                )
                                
                                self._record_duplicate(duplicate_record, rule.rule_name)
                                return True, duplicate_record
                
                # 记录新的唯一记录
                self._record_unique(record, record_id)
                return False, None
                
        except Exception as e:
            logger.error(f"重复检查失败: {e}")
            return False, None
        finally:
            self.stats.processing_time += time.time() - start_time
    
    def _record_unique(self, record: Dict, record_id: str):
        """记录唯一记录"""
        try:
            current_time = datetime.now()
            
            for rule in self.rules:
                if not rule.enabled:
                    continue
                
                record_hash = self.generate_record_hash(record, rule)
                content_hash = self.generate_content_hash(record)
                
                if record_hash:
                    # 更新内存缓存
                    self.memory_cache[record_hash] = record.copy()
                    self.hash_index[record_hash] = record_id
                    
                    # 更新数据库
                    with sqlite3.connect(self.db_path) as conn:
                        conn.execute("""
                            INSERT OR REPLACE INTO record_hashes 
                            (record_hash, record_id, key_fields, content_hash, timestamp, rule_name)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            record_hash,
                            record_id,
                            ','.join(rule.key_fields),
                            content_hash,
                            current_time,
                            rule.rule_name
                        ))
                        conn.commit()
            
            self.stats.unique_records += 1
            
        except Exception as e:
            logger.error(f"记录唯一记录失败: {e}")
    
    def _record_duplicate(self, duplicate_record: DuplicateRecord, rule_name: str):
        """记录重复记录"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO duplicate_records 
                    (original_hash, duplicate_hash, original_timestamp, duplicate_timestamp, 
                     duplicate_fields, similarity_score, rule_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    duplicate_record.original_hash,
                    duplicate_record.duplicate_hash,
                    duplicate_record.original_timestamp,
                    duplicate_record.duplicate_timestamp,
                    ','.join(duplicate_record.duplicate_fields),
                    duplicate_record.similarity_score,
                    rule_name
                ))
                conn.commit()
            
            self.stats.duplicates_found += 1
            
        except Exception as e:
            logger.error(f"记录重复记录失败: {e}")
    
    def process_record(self, record: Dict, record_id: str) -> bool:
        """处理单条记录，返回是否应该写入"""
        try:
            self.stats.total_processed += 1
            
            is_dup, dup_info = self.is_duplicate(record, record_id)
            
            if is_dup:
                self.stats.duplicates_blocked += 1
                logger.info(f"阻止重复记录写入: {record_id}, 相似度: {dup_info.similarity_score:.2f}")
                return False
            else:
                logger.debug(f"允许记录写入: {record_id}")
                return True
                
        except Exception as e:
            logger.error(f"处理记录失败: {e}")
            return True  # 出错时允许写入，避免数据丢失
    
    def batch_process(self, records: List[Tuple[Dict, str]]) -> List[Tuple[Dict, str]]:
        """批量处理记录"""
        unique_records = []
        
        for record, record_id in records:
            if self.process_record(record, record_id):
                unique_records.append((record, record_id))
        
        return unique_records
    
    def cleanup_old_records(self, days: int = 30):
        """清理旧记录"""
        try:
            cutoff_time = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                # 清理旧的哈希记录
                cursor = conn.execute("""
                    DELETE FROM record_hashes WHERE timestamp < ?
                """, (cutoff_time,))
                deleted_hashes = cursor.rowcount
                
                # 清理旧的重复记录
                cursor = conn.execute("""
                    DELETE FROM duplicate_records WHERE created_at < ?
                """, (cutoff_time,))
                deleted_duplicates = cursor.rowcount
                
                conn.commit()
                
            # 清理内存缓存
            with self.lock:
                keys_to_remove = []
                for key, record in self.memory_cache.items():
                    if record.get('timestamp', datetime.now()) < cutoff_time:
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    del self.memory_cache[key]
                    if key in self.hash_index:
                        del self.hash_index[key]
            
            logger.info(f"清理完成: 删除 {deleted_hashes} 条哈希记录, {deleted_duplicates} 条重复记录")
            
        except Exception as e:
            logger.error(f"清理旧记录失败: {e}")
    
    def get_statistics(self) -> DeduplicationStats:
        """获取去重统计信息"""
        if self.stats.total_processed > 0:
            self.stats.deduplication_rate = (self.stats.duplicates_blocked / self.stats.total_processed) * 100
        
        return self.stats
    
    def get_duplicate_report(self, days: int = 7) -> Dict:
        """获取重复记录报告"""
        try:
            cutoff_time = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                # 按规则统计重复
                cursor = conn.execute("""
                    SELECT rule_name, COUNT(*) as count, AVG(similarity_score) as avg_similarity
                    FROM duplicate_records 
                    WHERE created_at > ?
                    GROUP BY rule_name
                """, (cutoff_time,))
                
                rule_stats = {}
                for row in cursor.fetchall():
                    rule_stats[row[0]] = {
                        'count': row[1],
                        'avg_similarity': round(row[2], 3)
                    }
                
                # 获取最近的重复记录
                cursor = conn.execute("""
                    SELECT * FROM duplicate_records 
                    WHERE created_at > ?
                    ORDER BY created_at DESC
                    LIMIT 10
                """, (cutoff_time,))
                
                recent_duplicates = []
                for row in cursor.fetchall():
                    recent_duplicates.append({
                        'original_hash': row[1],
                        'duplicate_hash': row[2],
                        'similarity_score': row[6],
                        'rule_name': row[7],
                        'created_at': row[8]
                    })
            
            return {
                'period_days': days,
                'rule_statistics': rule_stats,
                'recent_duplicates': recent_duplicates,
                'total_stats': asdict(self.get_statistics())
            }
            
        except Exception as e:
            logger.error(f"获取重复报告失败: {e}")
            return {}

def main():
    """测试数据去重系统"""
    print("=== 数据去重系统测试 ===")
    
    # 初始化去重系统
    dedup_system = DataDeduplicationSystem()
    
    # 测试数据
    test_records = [
        {
            'draw_id': 'PC28_20241219_001',
            'issue': '001',
            'numbers': [8, 15, 22],
            'sum_value': 45,
            'big_small': 'big',
            'odd_even': 'odd',
            'timestamp': datetime.now()
        },
        {
            'draw_id': 'PC28_20241219_001',  # 重复的draw_id
            'issue': '001',
            'numbers': [8, 15, 22],
            'sum_value': 45,
            'big_small': 'big',
            'odd_even': 'odd',
            'timestamp': datetime.now()
        },
        {
            'draw_id': 'PC28_20241219_002',
            'issue': '002',
            'numbers': [3, 12, 28],
            'sum_value': 43,
            'big_small': 'big',
            'odd_even': 'odd',
            'timestamp': datetime.now()
        }
    ]
    
    print("\n1. 测试记录去重:")
    for i, record in enumerate(test_records):
        record_id = f"test_record_{i+1}"
        should_write = dedup_system.process_record(record, record_id)
        print(f"记录 {record_id}: {'允许写入' if should_write else '重复阻止'}")
    
    print("\n2. 批量处理测试:")
    batch_records = [(record, f"batch_{i}") for i, record in enumerate(test_records)]
    unique_records = dedup_system.batch_process(batch_records)
    print(f"批量处理: 输入 {len(batch_records)} 条，输出 {len(unique_records)} 条唯一记录")
    
    print("\n3. 统计信息:")
    stats = dedup_system.get_statistics()
    print(f"总处理: {stats.total_processed}")
    print(f"发现重复: {stats.duplicates_found}")
    print(f"阻止重复: {stats.duplicates_blocked}")
    print(f"唯一记录: {stats.unique_records}")
    print(f"去重率: {stats.deduplication_rate:.2f}%")
    print(f"处理时间: {stats.processing_time:.4f}秒")
    
    print("\n4. 重复报告:")
    report = dedup_system.get_duplicate_report()
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    
    print("\n=== 数据去重系统测试完成 ===")

if __name__ == "__main__":
    main()