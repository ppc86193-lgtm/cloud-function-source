#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
历史数据完整性保护机制
确保历史数据的安全性、一致性和完整性
"""

import json
import sqlite3
import hashlib
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
import logging
import os
import shutil
from pathlib import Path
import gzip

# 导入相关系统
from real_api_data_system import LotteryRecord

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DataIntegrityCheck:
    """数据完整性检查结果"""
    check_id: str
    check_time: datetime
    table_name: str
    total_records: int
    corrupted_records: int
    missing_records: int
    duplicate_records: int
    hash_mismatches: int
    integrity_score: float
    issues: List[str]
    recommendations: List[str]

@dataclass
class BackupInfo:
    """备份信息"""
    backup_id: str
    backup_time: datetime
    backup_type: str  # 'full', 'incremental', 'differential'
    file_path: str
    file_size: int
    record_count: int
    checksum: str
    compression: bool
    status: str  # 'completed', 'failed', 'in_progress'

@dataclass
class RecoveryPlan:
    """恢复计划"""
    plan_id: str
    target_date: datetime
    recovery_type: str  # 'full', 'partial', 'point_in_time'
    backup_sources: List[str]
    estimated_records: int
    estimated_time: int  # 秒
    dependencies: List[str]
    validation_rules: List[str]

@dataclass
class ProtectionRule:
    """保护规则"""
    rule_id: str
    rule_name: str
    rule_type: str  # 'backup', 'validation', 'access_control', 'retention'
    enabled: bool
    parameters: Dict[str, Any]
    schedule: Optional[str] = None  # cron表达式
    priority: int = 1

class HistoricalDataProtection:
    """历史数据保护系统"""
    
    def __init__(self, db_path: str = "lottery_data.db", backup_dir: str = "backups"):
        self.db_path = db_path
        self.backup_dir = Path(backup_dir)
        self.protection_db = "data_protection.db"
        self.lock = threading.RLock()
        
        # 创建备份目录
        self.backup_dir.mkdir(exist_ok=True)
        
        # 保护规则
        self.protection_rules = self._init_protection_rules()
        
        # 统计信息
        self.protection_stats = {
            'total_backups': 0,
            'successful_backups': 0,
            'failed_backups': 0,
            'total_integrity_checks': 0,
            'integrity_issues_found': 0,
            'data_recovery_operations': 0
        }
        
        self._init_protection_database()
    
    def _init_protection_database(self):
        """初始化保护数据库"""
        try:
            with sqlite3.connect(self.protection_db) as conn:
                # 完整性检查记录
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS integrity_checks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        check_id TEXT UNIQUE NOT NULL,
                        check_time DATETIME NOT NULL,
                        table_name TEXT NOT NULL,
                        total_records INTEGER NOT NULL,
                        corrupted_records INTEGER NOT NULL,
                        missing_records INTEGER NOT NULL,
                        duplicate_records INTEGER NOT NULL,
                        hash_mismatches INTEGER NOT NULL,
                        integrity_score REAL NOT NULL,
                        issues TEXT,
                        recommendations TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 备份记录
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS backup_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backup_id TEXT UNIQUE NOT NULL,
                        backup_time DATETIME NOT NULL,
                        backup_type TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        file_size INTEGER NOT NULL,
                        record_count INTEGER NOT NULL,
                        checksum TEXT NOT NULL,
                        compression BOOLEAN NOT NULL,
                        status TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 数据哈希记录（用于完整性验证）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS data_hashes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        record_id TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        data_hash TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 恢复操作记录
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS recovery_operations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        operation_id TEXT UNIQUE NOT NULL,
                        operation_time DATETIME NOT NULL,
                        recovery_type TEXT NOT NULL,
                        source_backup TEXT NOT NULL,
                        target_table TEXT NOT NULL,
                        records_recovered INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        error_message TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建索引
                conn.execute("CREATE INDEX IF NOT EXISTS idx_check_time ON integrity_checks(check_time)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_backup_time ON backup_records(backup_time)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_data_hash ON data_hashes(record_id, table_name)")
                
                conn.commit()
                logger.info("数据保护数据库初始化完成")
        except Exception as e:
            logger.error(f"数据保护数据库初始化失败: {e}")
            raise
    
    def _init_protection_rules(self) -> List[ProtectionRule]:
        """初始化保护规则"""
        return [
            # 备份规则
            ProtectionRule(
                rule_id="daily_full_backup",
                rule_name="每日全量备份",
                rule_type="backup",
                enabled=True,
                parameters={
                    'backup_type': 'full',
                    'compression': True,
                    'retention_days': 30
                },
                schedule="0 2 * * *",  # 每天凌晨2点
                priority=1
            ),
            ProtectionRule(
                rule_id="hourly_incremental_backup",
                rule_name="每小时增量备份",
                rule_type="backup",
                enabled=True,
                parameters={
                    'backup_type': 'incremental',
                    'compression': True,
                    'retention_hours': 72
                },
                schedule="0 * * * *",  # 每小时
                priority=2
            ),
            
            # 验证规则
            ProtectionRule(
                rule_id="integrity_check_daily",
                rule_name="每日完整性检查",
                rule_type="validation",
                enabled=True,
                parameters={
                    'check_type': 'full',
                    'hash_verification': True,
                    'duplicate_detection': True
                },
                schedule="0 3 * * *",  # 每天凌晨3点
                priority=1
            ),
            
            # 访问控制规则
            ProtectionRule(
                rule_id="read_only_historical",
                rule_name="历史数据只读保护",
                rule_type="access_control",
                enabled=True,
                parameters={
                    'protect_days_old': 7,  # 7天前的数据设为只读
                    'allow_admin_write': True
                },
                priority=1
            ),
            
            # 数据保留规则
            ProtectionRule(
                rule_id="long_term_retention",
                rule_name="长期数据保留",
                rule_type="retention",
                enabled=True,
                parameters={
                    'archive_days': 365,  # 1年后归档
                    'delete_days': 2555,  # 7年后删除
                    'archive_compression': True
                },
                priority=3
            )
        ]
    
    def create_backup(self, backup_type: str = "full", compression: bool = True) -> BackupInfo:
        """创建数据备份"""
        try:
            self.protection_stats['total_backups'] += 1
            
            backup_id = f"backup_{backup_type}_{int(time.time())}"
            backup_time = datetime.now()
            
            # 确定备份文件路径
            file_extension = ".gz" if compression else ".sql"
            backup_file = self.backup_dir / f"{backup_id}{file_extension}"
            
            logger.info(f"开始创建{backup_type}备份: {backup_id}")
            
            # 获取数据
            records = self._get_backup_data(backup_type)
            record_count = len(records)
            
            # 生成SQL备份内容
            sql_content = self._generate_backup_sql(records)
            
            # 写入文件（可选压缩）
            if compression:
                with gzip.open(backup_file, 'wt', encoding='utf-8') as f:
                    f.write(sql_content)
            else:
                with open(backup_file, 'w', encoding='utf-8') as f:
                    f.write(sql_content)
            
            # 计算文件大小和校验和
            file_size = backup_file.stat().st_size
            checksum = self._calculate_file_checksum(backup_file)
            
            # 创建备份信息
            backup_info = BackupInfo(
                backup_id=backup_id,
                backup_time=backup_time,
                backup_type=backup_type,
                file_path=str(backup_file),
                file_size=file_size,
                record_count=record_count,
                checksum=checksum,
                compression=compression,
                status="completed"
            )
            
            # 保存备份记录
            self._save_backup_record(backup_info)
            
            # 更新统计
            self.protection_stats['successful_backups'] += 1
            
            logger.info(f"备份创建完成: {backup_id}, 记录数: {record_count}, 文件大小: {file_size} bytes")
            
            return backup_info
            
        except Exception as e:
            logger.error(f"创建备份失败: {e}")
            self.protection_stats['failed_backups'] += 1
            
            # 创建失败的备份信息
            return BackupInfo(
                backup_id=backup_id if 'backup_id' in locals() else f"failed_{int(time.time())}",
                backup_time=datetime.now(),
                backup_type=backup_type,
                file_path="",
                file_size=0,
                record_count=0,
                checksum="",
                compression=compression,
                status="failed"
            )
    
    def _get_backup_data(self, backup_type: str) -> List[LotteryRecord]:
        """获取备份数据"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if backup_type == "full":
                    # 全量备份
                    cursor = conn.execute("""
                        SELECT * FROM lottery_records ORDER BY timestamp
                    """)
                elif backup_type == "incremental":
                    # 增量备份（最近1小时的数据）
                    cutoff_time = datetime.now() - timedelta(hours=1)
                    cursor = conn.execute("""
                        SELECT * FROM lottery_records 
                        WHERE timestamp > ? ORDER BY timestamp
                    """, (cutoff_time.isoformat(),))
                elif backup_type == "differential":
                    # 差异备份（最近24小时的数据）
                    cutoff_time = datetime.now() - timedelta(days=1)
                    cursor = conn.execute("""
                        SELECT * FROM lottery_records 
                        WHERE timestamp > ? ORDER BY timestamp
                    """, (cutoff_time.isoformat(),))
                else:
                    raise ValueError(f"不支持的备份类型: {backup_type}")
                
                rows = cursor.fetchall()
                
                # 转换为LotteryRecord对象
                records = []
                for row in rows:
                    try:
                        numbers = json.loads(row[2]) if row[2] else []
                        timestamp = datetime.fromisoformat(row[7]) if row[7] else datetime.now()
                        
                        record = LotteryRecord(
                            draw_id=row[0],
                            issue=row[1],
                            numbers=numbers,
                            sum_value=row[3],
                            big_small=row[4],
                            odd_even=row[5],
                            dragon_tiger=row[6],
                            timestamp=timestamp
                        )
                        records.append(record)
                    except Exception as e:
                        logger.warning(f"解析记录失败: {e}, 跳过记录: {row[0]}")
                        continue
                
                return records
                
        except Exception as e:
            logger.error(f"获取备份数据失败: {e}")
            return []
    
    def _generate_backup_sql(self, records: List[LotteryRecord]) -> str:
        """生成备份SQL"""
        try:
            sql_lines = [
                "-- PC28历史数据备份",
                f"-- 备份时间: {datetime.now().isoformat()}",
                f"-- 记录数量: {len(records)}",
                "",
                "-- 创建表结构",
                "CREATE TABLE IF NOT EXISTS lottery_records (",
                "    draw_id TEXT PRIMARY KEY,",
                "    issue TEXT NOT NULL,",
                "    numbers TEXT NOT NULL,",
                "    sum_value INTEGER NOT NULL,",
                "    big_small TEXT NOT NULL,",
                "    odd_even TEXT NOT NULL,",
                "    dragon_tiger TEXT NOT NULL,",
                "    timestamp DATETIME NOT NULL",
                ");",
                "",
                "-- 插入数据",
                "BEGIN TRANSACTION;"
            ]
            
            for record in records:
                numbers_json = json.dumps(record.numbers)
                timestamp_str = record.timestamp.isoformat()
                
                sql_line = f"INSERT OR REPLACE INTO lottery_records VALUES ('{record.draw_id}', '{record.issue}', '{numbers_json}', {record.sum_value}, '{record.big_small}', '{record.odd_even}', '{record.dragon_tiger}', '{timestamp_str}');"
                sql_lines.append(sql_line)
            
            sql_lines.extend([
                "COMMIT;",
                "",
                "-- 备份完成"
            ])
            
            return "\n".join(sql_lines)
            
        except Exception as e:
            logger.error(f"生成备份SQL失败: {e}")
            return ""
    
    def _calculate_file_checksum(self, file_path: Path) -> str:
        """计算文件校验和"""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"计算文件校验和失败: {e}")
            return ""
    
    def _save_backup_record(self, backup_info: BackupInfo):
        """保存备份记录"""
        try:
            with sqlite3.connect(self.protection_db) as conn:
                conn.execute("""
                    INSERT INTO backup_records 
                    (backup_id, backup_time, backup_type, file_path, file_size,
                     record_count, checksum, compression, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    backup_info.backup_id,
                    backup_info.backup_time,
                    backup_info.backup_type,
                    backup_info.file_path,
                    backup_info.file_size,
                    backup_info.record_count,
                    backup_info.checksum,
                    backup_info.compression,
                    backup_info.status
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存备份记录失败: {e}")
    
    def verify_data_integrity(self, table_name: str = "lottery_records") -> DataIntegrityCheck:
        """验证数据完整性"""
        try:
            self.protection_stats['total_integrity_checks'] += 1
            
            check_id = f"integrity_{int(time.time())}"
            check_time = datetime.now()
            
            logger.info(f"开始数据完整性检查: {check_id}")
            
            issues = []
            recommendations = []
            
            with sqlite3.connect(self.db_path) as conn:
                # 获取总记录数
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_records = cursor.fetchone()[0]
                
                # 检查损坏记录（字段为空或格式错误）
                corrupted_records = self._check_corrupted_records(conn, table_name)
                
                # 检查重复记录
                duplicate_records = self._check_duplicate_records(conn, table_name)
                
                # 检查缺失记录（基于期号连续性）
                missing_records = self._check_missing_records(conn, table_name)
                
                # 检查哈希不匹配
                hash_mismatches = self._check_hash_integrity(conn, table_name)
                
                # 计算完整性得分
                integrity_score = self._calculate_integrity_score(
                    total_records, corrupted_records, missing_records, 
                    duplicate_records, hash_mismatches
                )
                
                # 生成问题和建议
                if corrupted_records > 0:
                    issues.append(f"发现{corrupted_records}条损坏记录")
                    recommendations.append("修复或删除损坏的记录")
                
                if duplicate_records > 0:
                    issues.append(f"发现{duplicate_records}条重复记录")
                    recommendations.append("清理重复记录")
                
                if missing_records > 0:
                    issues.append(f"发现{missing_records}条缺失记录")
                    recommendations.append("从备份或上游API补充缺失数据")
                
                if hash_mismatches > 0:
                    issues.append(f"发现{hash_mismatches}条哈希不匹配记录")
                    recommendations.append("重新计算并更新数据哈希")
                
                if integrity_score >= 95:
                    recommendations.append("数据完整性良好，继续保持")
                elif integrity_score >= 80:
                    recommendations.append("数据完整性一般，建议定期检查")
                else:
                    recommendations.append("数据完整性较差，需要立即修复")
            
            # 创建检查结果
            integrity_check = DataIntegrityCheck(
                check_id=check_id,
                check_time=check_time,
                table_name=table_name,
                total_records=total_records,
                corrupted_records=corrupted_records,
                missing_records=missing_records,
                duplicate_records=duplicate_records,
                hash_mismatches=hash_mismatches,
                integrity_score=integrity_score,
                issues=issues,
                recommendations=recommendations
            )
            
            # 保存检查结果
            self._save_integrity_check(integrity_check)
            
            # 更新统计
            if issues:
                self.protection_stats['integrity_issues_found'] += len(issues)
            
            logger.info(f"完整性检查完成: {check_id}, 得分: {integrity_score:.2f}")
            
            return integrity_check
            
        except Exception as e:
            logger.error(f"数据完整性检查失败: {e}")
            self.protection_stats['integrity_issues_found'] += 1
            raise
    
    def _check_corrupted_records(self, conn: sqlite3.Connection, table_name: str) -> int:
        """检查损坏记录"""
        try:
            cursor = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE draw_id IS NULL OR draw_id = '' 
                   OR numbers IS NULL OR numbers = '' 
                   OR timestamp IS NULL OR timestamp = ''
            """)
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"检查损坏记录失败: {e}")
            return 0
    
    def _check_duplicate_records(self, conn: sqlite3.Connection, table_name: str) -> int:
        """检查重复记录"""
        try:
            cursor = conn.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT draw_id) FROM {table_name}
            """)
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"检查重复记录失败: {e}")
            return 0
    
    def _check_missing_records(self, conn: sqlite3.Connection, table_name: str) -> int:
        """检查缺失记录（基于期号连续性）"""
        try:
            # 获取所有数字期号
            cursor = conn.execute(f"""
                SELECT issue FROM {table_name} 
                WHERE issue REGEXP '^[0-9]+$' 
                ORDER BY CAST(issue AS INTEGER)
            """)
            
            issues = [int(row[0]) for row in cursor.fetchall()]
            
            if len(issues) < 2:
                return 0
            
            # 计算应有的期号范围
            min_issue = min(issues)
            max_issue = max(issues)
            expected_count = max_issue - min_issue + 1
            actual_count = len(issues)
            
            return max(0, expected_count - actual_count)
            
        except Exception as e:
            logger.error(f"检查缺失记录失败: {e}")
            return 0
    
    def _check_hash_integrity(self, conn: sqlite3.Connection, table_name: str) -> int:
        """检查哈希完整性"""
        try:
            # 获取所有记录和对应的哈希
            cursor = conn.execute(f"""
                SELECT lr.draw_id, lr.numbers, lr.sum_value, lr.timestamp, dh.data_hash
                FROM {table_name} lr
                LEFT JOIN data_hashes dh ON lr.draw_id = dh.record_id AND dh.table_name = ?
            """, (table_name,))
            
            mismatches = 0
            
            for row in cursor.fetchall():
                draw_id, numbers, sum_value, timestamp, stored_hash = row
                
                # 计算当前数据的哈希
                data_str = f"{draw_id}|{numbers}|{sum_value}|{timestamp}"
                current_hash = hashlib.md5(data_str.encode()).hexdigest()
                
                if stored_hash and stored_hash != current_hash:
                    mismatches += 1
                elif not stored_hash:
                    # 如果没有存储哈希，创建一个
                    self._store_data_hash(draw_id, table_name, current_hash)
            
            return mismatches
            
        except Exception as e:
            logger.error(f"检查哈希完整性失败: {e}")
            return 0
    
    def _store_data_hash(self, record_id: str, table_name: str, data_hash: str):
        """存储数据哈希"""
        try:
            with sqlite3.connect(self.protection_db) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO data_hashes 
                    (record_id, table_name, data_hash, updated_at)
                    VALUES (?, ?, ?, ?)
                """, (record_id, table_name, data_hash, datetime.now()))
                conn.commit()
        except Exception as e:
            logger.error(f"存储数据哈希失败: {e}")
    
    def _calculate_integrity_score(self, total: int, corrupted: int, missing: int, 
                                 duplicates: int, hash_mismatches: int) -> float:
        """计算完整性得分"""
        if total == 0:
            return 0.0
        
        # 计算各项扣分
        corruption_penalty = (corrupted / total) * 40  # 损坏记录扣40分
        missing_penalty = (missing / total) * 30       # 缺失记录扣30分
        duplicate_penalty = (duplicates / total) * 20  # 重复记录扣20分
        hash_penalty = (hash_mismatches / total) * 10  # 哈希不匹配扣10分
        
        # 计算最终得分
        score = 100 - corruption_penalty - missing_penalty - duplicate_penalty - hash_penalty
        
        return max(0.0, min(100.0, score))
    
    def _save_integrity_check(self, check: DataIntegrityCheck):
        """保存完整性检查结果"""
        try:
            with sqlite3.connect(self.protection_db) as conn:
                conn.execute("""
                    INSERT INTO integrity_checks 
                    (check_id, check_time, table_name, total_records, corrupted_records,
                     missing_records, duplicate_records, hash_mismatches, integrity_score,
                     issues, recommendations)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    check.check_id,
                    check.check_time,
                    check.table_name,
                    check.total_records,
                    check.corrupted_records,
                    check.missing_records,
                    check.duplicate_records,
                    check.hash_mismatches,
                    check.integrity_score,
                    json.dumps(check.issues, ensure_ascii=False),
                    json.dumps(check.recommendations, ensure_ascii=False)
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存完整性检查结果失败: {e}")
    
    def restore_from_backup(self, backup_id: str, target_table: str = "lottery_records") -> bool:
        """从备份恢复数据"""
        try:
            self.protection_stats['data_recovery_operations'] += 1
            
            logger.info(f"开始从备份恢复数据: {backup_id}")
            
            # 获取备份信息
            backup_info = self._get_backup_info(backup_id)
            if not backup_info:
                logger.error(f"未找到备份: {backup_id}")
                return False
            
            # 验证备份文件
            backup_file = Path(backup_info.file_path)
            if not backup_file.exists():
                logger.error(f"备份文件不存在: {backup_file}")
                return False
            
            # 验证校验和
            current_checksum = self._calculate_file_checksum(backup_file)
            if current_checksum != backup_info.checksum:
                logger.error(f"备份文件校验和不匹配: {current_checksum} != {backup_info.checksum}")
                return False
            
            # 读取备份内容
            if backup_info.compression:
                with gzip.open(backup_file, 'rt', encoding='utf-8') as f:
                    sql_content = f.read()
            else:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
            
            # 执行恢复
            with sqlite3.connect(self.db_path) as conn:
                # 备份当前数据（以防恢复失败）
                temp_backup = self.create_backup("emergency")
                
                try:
                    # 清空目标表
                    conn.execute(f"DELETE FROM {target_table}")
                    
                    # 执行恢复SQL
                    conn.executescript(sql_content)
                    
                    conn.commit()
                    
                    # 记录恢复操作
                    self._record_recovery_operation(
                        backup_id, target_table, backup_info.record_count, "success"
                    )
                    
                    logger.info(f"数据恢复成功: {backup_info.record_count} 条记录")
                    return True
                    
                except Exception as e:
                    # 恢复失败，回滚
                    conn.rollback()
                    logger.error(f"数据恢复失败，正在回滚: {e}")
                    
                    # 记录失败操作
                    self._record_recovery_operation(
                        backup_id, target_table, 0, "failed", str(e)
                    )
                    
                    return False
            
        except Exception as e:
            logger.error(f"恢复数据失败: {e}")
            return False
    
    def _get_backup_info(self, backup_id: str) -> Optional[BackupInfo]:
        """获取备份信息"""
        try:
            with sqlite3.connect(self.protection_db) as conn:
                cursor = conn.execute("""
                    SELECT backup_id, backup_time, backup_type, file_path, file_size,
                           record_count, checksum, compression, status
                    FROM backup_records WHERE backup_id = ?
                """, (backup_id,))
                
                row = cursor.fetchone()
                if row:
                    return BackupInfo(
                        backup_id=row[0],
                        backup_time=datetime.fromisoformat(row[1]),
                        backup_type=row[2],
                        file_path=row[3],
                        file_size=row[4],
                        record_count=row[5],
                        checksum=row[6],
                        compression=bool(row[7]),
                        status=row[8]
                    )
                return None
        except Exception as e:
            logger.error(f"获取备份信息失败: {e}")
            return None
    
    def _record_recovery_operation(self, backup_id: str, target_table: str, 
                                 records_recovered: int, status: str, error_message: str = None):
        """记录恢复操作"""
        try:
            operation_id = f"recovery_{int(time.time())}"
            
            with sqlite3.connect(self.protection_db) as conn:
                conn.execute("""
                    INSERT INTO recovery_operations 
                    (operation_id, operation_time, recovery_type, source_backup,
                     target_table, records_recovered, status, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    operation_id,
                    datetime.now(),
                    "backup_restore",
                    backup_id,
                    target_table,
                    records_recovered,
                    status,
                    error_message
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"记录恢复操作失败: {e}")
    
    def cleanup_old_backups(self, retention_days: int = 30):
        """清理旧备份"""
        try:
            cutoff_time = datetime.now() - timedelta(days=retention_days)
            
            with sqlite3.connect(self.protection_db) as conn:
                # 获取需要清理的备份
                cursor = conn.execute("""
                    SELECT backup_id, file_path FROM backup_records 
                    WHERE backup_time < ? AND status = 'completed'
                """, (cutoff_time,))
                
                old_backups = cursor.fetchall()
                
                cleaned_count = 0
                for backup_id, file_path in old_backups:
                    try:
                        # 删除文件
                        backup_file = Path(file_path)
                        if backup_file.exists():
                            backup_file.unlink()
                        
                        # 更新数据库记录
                        conn.execute("""
                            UPDATE backup_records SET status = 'deleted' 
                            WHERE backup_id = ?
                        """, (backup_id,))
                        
                        cleaned_count += 1
                        
                    except Exception as e:
                        logger.warning(f"清理备份失败 {backup_id}: {e}")
                
                conn.commit()
                
                logger.info(f"清理完成，删除了 {cleaned_count} 个旧备份")
                
        except Exception as e:
            logger.error(f"清理旧备份失败: {e}")
    
    def get_protection_summary(self) -> Dict[str, Any]:
        """获取保护摘要"""
        try:
            with sqlite3.connect(self.protection_db) as conn:
                # 备份统计
                cursor = conn.execute("""
                    SELECT COUNT(*), SUM(file_size), SUM(record_count)
                    FROM backup_records WHERE status = 'completed'
                """)
                backup_stats = cursor.fetchone()
                
                # 最近的完整性检查
                cursor = conn.execute("""
                    SELECT integrity_score, check_time FROM integrity_checks 
                    ORDER BY check_time DESC LIMIT 1
                """)
                latest_check = cursor.fetchone()
                
                # 恢复操作统计
                cursor = conn.execute("""
                    SELECT COUNT(*), SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)
                    FROM recovery_operations
                """)
                recovery_stats = cursor.fetchone()
                
                return {
                    'backup_summary': {
                        'total_backups': backup_stats[0] or 0,
                        'total_size_bytes': backup_stats[1] or 0,
                        'total_records_backed_up': backup_stats[2] or 0
                    },
                    'integrity_summary': {
                        'latest_score': latest_check[0] if latest_check else 0,
                        'last_check_time': latest_check[1] if latest_check else None
                    },
                    'recovery_summary': {
                        'total_operations': recovery_stats[0] or 0,
                        'successful_operations': recovery_stats[1] or 0
                    },
                    'protection_stats': self.protection_stats.copy()
                }
                
        except Exception as e:
            logger.error(f"获取保护摘要失败: {e}")
            return {}

def main():
    """测试历史数据保护系统"""
    print("=== 历史数据保护系统测试 ===")
    
    # 初始化保护系统
    protection = HistoricalDataProtection()
    
    print("\n1. 创建全量备份:")
    backup_info = protection.create_backup("full", compression=True)
    print(f"备份ID: {backup_info.backup_id}")
    print(f"备份状态: {backup_info.status}")
    print(f"记录数量: {backup_info.record_count}")
    print(f"文件大小: {backup_info.file_size} bytes")
    print(f"校验和: {backup_info.checksum[:16]}...")
    
    print("\n2. 数据完整性检查:")
    integrity_check = protection.verify_data_integrity()
    print(f"检查ID: {integrity_check.check_id}")
    print(f"总记录数: {integrity_check.total_records}")
    print(f"完整性得分: {integrity_check.integrity_score:.2f}")
    print(f"损坏记录: {integrity_check.corrupted_records}")
    print(f"重复记录: {integrity_check.duplicate_records}")
    print(f"缺失记录: {integrity_check.missing_records}")
    
    if integrity_check.issues:
        print("\n发现的问题:")
        for issue in integrity_check.issues:
            print(f"- {issue}")
    
    if integrity_check.recommendations:
        print("\n改进建议:")
        for rec in integrity_check.recommendations:
            print(f"- {rec}")
    
    print("\n3. 创建增量备份:")
    incremental_backup = protection.create_backup("incremental", compression=True)
    print(f"增量备份ID: {incremental_backup.backup_id}")
    print(f"增量备份状态: {incremental_backup.status}")
    print(f"增量记录数: {incremental_backup.record_count}")
    
    print("\n4. 保护摘要:")
    summary = protection.get_protection_summary()
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    
    print("\n5. 清理旧备份（测试模式，保留时间设为0天）:")
    protection.cleanup_old_backups(retention_days=0)
    
    print("\n=== 历史数据保护系统测试完成 ===")

if __name__ == "__main__":
    main()