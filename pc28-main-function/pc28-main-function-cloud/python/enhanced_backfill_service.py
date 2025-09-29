#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28增强版智能回填服务
实现智能缺失检测、增量/全量回填、进度跟踪和异常处理
"""

import json
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
import sqlite3
import os


# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BackfillMode(Enum):
    """回填模式枚举"""
    INCREMENTAL = "incremental"  # 增量回填
    FULL = "full"  # 全量回填
    SMART = "smart"  # 智能回填（自动检测缺失）

class BackfillStatus(Enum):
    """回填状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"

@dataclass
class BackfillTask:
    """回填任务数据类"""
    task_id: str
    mode: BackfillMode
    start_date: str
    end_date: str
    status: BackfillStatus
    progress: float = 0.0
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    created_at: datetime = None
    started_at: datetime = None
    completed_at: datetime = None
    error_message: str = None

@dataclass
class DataGap:
    """数据缺失信息"""
    start_date: str
    end_date: str
    missing_count: int
    gap_type: str  # 'continuous', 'scattered'

class EnhancedBackfillService:
    """
    增强版智能回填服务
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
        self.db_path = "backfill_tracking.db"
        
        # 从配置中获取参数
        backfill_settings = self.config.get('backfill_settings', {})
        self.max_workers = backfill_settings.get('max_concurrent_tasks', 5)
        self.request_delay = 1.0  # 请求间隔（秒）
        self.retry_attempts = backfill_settings.get('retry_attempts', 3)
        self.batch_size = backfill_settings.get('batch_size', 100)
        
        self.active_tasks: Dict[str, BackfillTask] = {}
        self.lock = threading.Lock()
        
        # 初始化数据库
        self._init_database()
        
    def _init_database(self):
        """初始化跟踪数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 创建任务跟踪表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS backfill_tasks (
                        task_id TEXT PRIMARY KEY,
                        mode TEXT NOT NULL,
                        start_date TEXT NOT NULL,
                        end_date TEXT NOT NULL,
                        status TEXT NOT NULL,
                        progress REAL DEFAULT 0.0,
                        total_records INTEGER DEFAULT 0,
                        processed_records INTEGER DEFAULT 0,
                        failed_records INTEGER DEFAULT 0,
                        created_at TEXT,
                        started_at TEXT,
                        completed_at TEXT,
                        error_message TEXT
                    )
                """)
                
                # 创建数据记录表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS backfill_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id TEXT,
                        draw_id TEXT,
                        timestamp TEXT,
                        date TEXT,
                        status TEXT,
                        retry_count INTEGER DEFAULT 0,
                        error_message TEXT,
                        created_at TEXT,
                        FOREIGN KEY (task_id) REFERENCES backfill_tasks (task_id)
                    )
                """)
                
                # 创建数据缺失跟踪表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS data_gaps (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        start_date TEXT,
                        end_date TEXT,
                        missing_count INTEGER,
                        gap_type TEXT,
                        detected_at TEXT,
                        resolved_at TEXT
                    )
                """)
                
                conn.commit()
                logger.info("数据库初始化完成")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def detect_data_gaps(self, start_date: str, end_date: str) -> List[DataGap]:
        """
        检测指定日期范围内的数据缺失
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            数据缺失列表
        """
        logger.info(f"开始检测数据缺失: {start_date} 到 {end_date}")
        gaps = []
        
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            current_dt = start_dt
            missing_dates = []
            
            while current_dt <= end_dt:
                date_str = current_dt.strftime("%Y-%m-%d")
                
                # 检查该日期是否有数据
                if not self._has_data_for_date(date_str):
                    missing_dates.append(date_str)
                    logger.debug(f"发现缺失日期: {date_str}")
                
                current_dt += timedelta(days=1)
            
            if missing_dates:
                # 分析缺失模式
                gaps = self._analyze_missing_patterns(missing_dates)
                
                # 保存缺失信息到数据库
                self._save_data_gaps(gaps)
                
                logger.info(f"检测到 {len(gaps)} 个数据缺失区间，总计 {len(missing_dates)} 天")
            else:
                logger.info("未发现数据缺失")
            
            return gaps
            
        except Exception as e:
            logger.error(f"数据缺失检测失败: {e}")
            return []
    
    def _has_data_for_date(self, date: str) -> bool:
        """
        检查指定日期是否有数据
        
        Args:
            date: 日期字符串 (YYYY-MM-DD)
            
        Returns:
            是否有数据
        """
        try:
            # 尝试获取该日期的数据
            raw_data = self.api_client.get_history_lottery(date=date, limit=1)
            
            if raw_data.get('codeid') == 10000 and raw_data.get('retdata'):
                return len(raw_data['retdata']) > 0
            
            return False
            
        except Exception as e:
            logger.debug(f"检查日期 {date} 数据时出错: {e}")
            return False
    
    def _analyze_missing_patterns(self, missing_dates: List[str]) -> List[DataGap]:
        """
        分析缺失日期的模式
        
        Args:
            missing_dates: 缺失日期列表
            
        Returns:
            数据缺失区间列表
        """
        if not missing_dates:
            return []
        
        gaps = []
        missing_dates.sort()
        
        start_date = missing_dates[0]
        end_date = missing_dates[0]
        count = 1
        
        for i in range(1, len(missing_dates)):
            current_date = datetime.strptime(missing_dates[i], "%Y-%m-%d")
            prev_date = datetime.strptime(missing_dates[i-1], "%Y-%m-%d")
            
            # 检查是否连续
            if (current_date - prev_date).days == 1:
                end_date = missing_dates[i]
                count += 1
            else:
                # 创建一个缺失区间
                gap_type = "continuous" if count > 1 else "scattered"
                gaps.append(DataGap(
                    start_date=start_date,
                    end_date=end_date,
                    missing_count=count,
                    gap_type=gap_type
                ))
                
                # 开始新的区间
                start_date = missing_dates[i]
                end_date = missing_dates[i]
                count = 1
        
        # 添加最后一个区间
        gap_type = "continuous" if count > 1 else "scattered"
        gaps.append(DataGap(
            start_date=start_date,
            end_date=end_date,
            missing_count=count,
            gap_type=gap_type
        ))
        
        return gaps
    
    def _save_data_gaps(self, gaps: List[DataGap]):
        """
        保存数据缺失信息到数据库
        
        Args:
            gaps: 数据缺失列表
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for gap in gaps:
                    cursor.execute("""
                        INSERT INTO data_gaps 
                        (start_date, end_date, missing_count, gap_type, detected_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        gap.start_date,
                        gap.end_date,
                        gap.missing_count,
                        gap.gap_type,
                        datetime.now(timezone.utc).isoformat()
                    ))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存数据缺失信息失败: {e}")
    
    def create_backfill_task(self, mode: BackfillMode, start_date: str, end_date: str) -> str:
        """
        创建回填任务
        
        Args:
            mode: 回填模式
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            任务ID
        """
        task_id = f"backfill_{int(time.time())}_{mode.value}"
        
        task = BackfillTask(
            task_id=task_id,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            status=BackfillStatus.PENDING,
            created_at=datetime.now(timezone.utc)
        )
        
        # 保存任务到数据库
        self._save_task(task)
        
        with self.lock:
            self.active_tasks[task_id] = task
        
        logger.info(f"创建回填任务: {task_id} ({mode.value}: {start_date} 到 {end_date})")
        return task_id
    
    def start_backfill_task(self, task_id: str) -> bool:
        """
        启动回填任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否启动成功
        """
        with self.lock:
            if task_id not in self.active_tasks:
                logger.error(f"任务不存在: {task_id}")
                return False
            
            task = self.active_tasks[task_id]
            if task.status != BackfillStatus.PENDING:
                logger.error(f"任务状态不允许启动: {task.status}")
                return False
            
            task.status = BackfillStatus.RUNNING
            task.started_at = datetime.now(timezone.utc)
        
        # 在后台线程中执行回填
        threading.Thread(
            target=self._execute_backfill_task,
            args=(task_id,),
            daemon=True
        ).start()
        
        logger.info(f"启动回填任务: {task_id}")
        return True
    
    def _execute_backfill_task(self, task_id: str):
        """
        执行回填任务
        
        Args:
            task_id: 任务ID
        """
        try:
            task = self.active_tasks[task_id]
            
            if task.mode == BackfillMode.SMART:
                # 智能回填：先检测缺失，再回填
                gaps = self.detect_data_gaps(task.start_date, task.end_date)
                if gaps:
                    self._backfill_gaps(task_id, gaps)
                else:
                    logger.info(f"任务 {task_id}: 未发现数据缺失")
                    task.status = BackfillStatus.COMPLETED
            else:
                # 增量或全量回填
                self._backfill_date_range(task_id, task.start_date, task.end_date)
            
            # 更新任务状态
            with self.lock:
                if task.status == BackfillStatus.RUNNING:
                    task.status = BackfillStatus.COMPLETED
                    task.completed_at = datetime.now(timezone.utc)
                    task.progress = 100.0
            
            self._update_task(task)
            logger.info(f"回填任务完成: {task_id}")
            
        except Exception as e:
            logger.error(f"回填任务执行失败 {task_id}: {e}")
            
            with self.lock:
                task = self.active_tasks[task_id]
                task.status = BackfillStatus.FAILED
                task.error_message = str(e)
                task.completed_at = datetime.now(timezone.utc)
            
            self._update_task(task)
    
    def _backfill_gaps(self, task_id: str, gaps: List[DataGap]):
        """
        回填数据缺失区间
        
        Args:
            task_id: 任务ID
            gaps: 数据缺失列表
        """
        task = self.active_tasks[task_id]
        total_days = sum(gap.missing_count for gap in gaps)
        processed_days = 0
        
        logger.info(f"任务 {task_id}: 开始回填 {len(gaps)} 个缺失区间，总计 {total_days} 天")
        
        for gap in gaps:
            if task.status != BackfillStatus.RUNNING:
                break
            
            logger.info(f"回填区间: {gap.start_date} 到 {gap.end_date} ({gap.missing_count} 天)")
            
            # 回填该区间
            start_dt = datetime.strptime(gap.start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(gap.end_date, "%Y-%m-%d")
            
            current_dt = start_dt
            while current_dt <= end_dt:
                if task.status != BackfillStatus.RUNNING:
                    break
                
                date_str = current_dt.strftime("%Y-%m-%d")
                success = self._backfill_single_date(task_id, date_str)
                
                if success:
                    task.processed_records += 1
                else:
                    task.failed_records += 1
                
                processed_days += 1
                task.progress = (processed_days / total_days) * 100
                
                current_dt += timedelta(days=1)
                time.sleep(self.request_delay)
            
            # 更新任务进度
            self._update_task(task)
    
    def _backfill_date_range(self, task_id: str, start_date: str, end_date: str):
        """
        回填指定日期范围
        
        Args:
            task_id: 任务ID
            start_date: 开始日期
            end_date: 结束日期
        """
        task = self.active_tasks[task_id]
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        total_days = (end_dt - start_dt).days + 1
        processed_days = 0
        
        logger.info(f"任务 {task_id}: 开始回填日期范围 {start_date} 到 {end_date} ({total_days} 天)")
        
        current_dt = start_dt
        while current_dt <= end_dt:
            if task.status != BackfillStatus.RUNNING:
                break
            
            date_str = current_dt.strftime("%Y-%m-%d")
            success = self._backfill_single_date(task_id, date_str)
            
            if success:
                task.processed_records += 1
            else:
                task.failed_records += 1
            
            processed_days += 1
            task.progress = (processed_days / total_days) * 100
            
            current_dt += timedelta(days=1)
            time.sleep(self.request_delay)
            
            # 定期更新任务进度
            if processed_days % 10 == 0:
                self._update_task(task)
    
    def _backfill_single_date(self, task_id: str, date: str) -> bool:
        """
        回填单个日期的数据
        
        Args:
            task_id: 任务ID
            date: 日期字符串
            
        Returns:
            是否成功
        """
        for attempt in range(self.retry_attempts):
            try:
                logger.debug(f"回填日期 {date} (尝试 {attempt + 1}/{self.retry_attempts})")
                
                # 获取该日期的历史数据
                raw_data = self.api_client.get_history_lottery(date=date, limit=100)
                
                if raw_data.get('codeid') != 10000:
                    logger.warning(f"API返回错误: {raw_data.get('message')}")
                    continue
                
                # 解析数据
                parsed_data = self.api_client.parse_lottery_data(raw_data)
                
                if parsed_data:
                    # 记录成功的数据
                    for data in parsed_data:
                        self._save_backfill_record(
                            task_id, data.get('draw_id'), data.get('timestamp'), date, 'success'
                        )
                    
                    logger.debug(f"成功回填 {date}: {len(parsed_data)} 条记录")
                    return True
                else:
                    logger.warning(f"日期 {date} 无有效数据")
                    return True  # 无数据也算成功
                
            except Exception as e:
                logger.warning(f"回填日期 {date} 失败 (尝试 {attempt + 1}): {e}")
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(2 ** attempt)  # 指数退避
        
        # 记录失败
        self._save_backfill_record(task_id, None, None, date, 'failed')
        return False
    
    def _save_task(self, task: BackfillTask):
        """保存任务到数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO backfill_tasks 
                    (task_id, mode, start_date, end_date, status, progress, 
                     total_records, processed_records, failed_records, 
                     created_at, started_at, completed_at, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task.task_id, task.mode.value, task.start_date, task.end_date,
                    task.status.value, task.progress, task.total_records,
                    task.processed_records, task.failed_records,
                    task.created_at.isoformat() if task.created_at else None,
                    task.started_at.isoformat() if task.started_at else None,
                    task.completed_at.isoformat() if task.completed_at else None,
                    task.error_message
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存任务失败: {e}")
    
    def _update_task(self, task: BackfillTask):
        """更新任务状态"""
        self._save_task(task)
    
    def _save_backfill_record(self, task_id: str, draw_id: str, timestamp: str, date: str, status: str):
        """保存回填记录"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO backfill_records 
                    (task_id, draw_id, timestamp, date, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    task_id, draw_id, timestamp, date, status,
                    datetime.now(timezone.utc).isoformat()
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存回填记录失败: {e}")
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态信息
        """
        with self.lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                return {
                    'task_id': task.task_id,
                    'mode': task.mode.value,
                    'start_date': task.start_date,
                    'end_date': task.end_date,
                    'status': task.status.value,
                    'progress': task.progress,
                    'total_records': task.total_records,
                    'processed_records': task.processed_records,
                    'failed_records': task.failed_records,
                    'created_at': task.created_at.isoformat() if task.created_at else None,
                    'started_at': task.started_at.isoformat() if task.started_at else None,
                    'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                    'error_message': task.error_message
                }
        
        return None
    
    def list_active_tasks(self) -> List[Dict[str, Any]]:
        """
        列出所有活跃任务
        
        Returns:
            任务列表
        """
        with self.lock:
            return [self.get_task_status(task_id) for task_id in self.active_tasks.keys()]
    
    def pause_task(self, task_id: str) -> bool:
        """
        暂停任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功
        """
        with self.lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                if task.status == BackfillStatus.RUNNING:
                    task.status = BackfillStatus.PAUSED
                    self._update_task(task)
                    logger.info(f"任务已暂停: {task_id}")
                    return True
        
        return False
    
    def resume_task(self, task_id: str) -> bool:
        """
        恢复任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功
        """
        with self.lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                if task.status == BackfillStatus.PAUSED:
                    task.status = BackfillStatus.RUNNING
                    self._update_task(task)
                    logger.info(f"任务已恢复: {task_id}")
                    return True
        
        return False
    
    def get_task_progress_report(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务详细进度报告
        
        Args:
            task_id: 任务ID
            
        Returns:
            详细进度报告
        """
        task_status = self.get_task_status(task_id)
        if not task_status:
            return None
        
        # 获取回填记录统计
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 统计成功和失败的记录
                cursor.execute("""
                    SELECT status, COUNT(*) as count
                    FROM backfill_records 
                    WHERE task_id = ?
                    GROUP BY status
                """, (task_id,))
                
                record_stats = {row[0]: row[1] for row in cursor.fetchall()}
                
                # 获取最近的回填记录
                cursor.execute("""
                    SELECT date, status, COUNT(*) as count
                    FROM backfill_records 
                    WHERE task_id = ?
                    GROUP BY date, status
                    ORDER BY date DESC
                    LIMIT 10
                """, (task_id,))
                
                recent_records = cursor.fetchall()
                
        except Exception as e:
            logger.error(f"获取回填记录统计失败: {e}")
            record_stats = {}
            recent_records = []
        
        # 计算执行时间
        execution_time = None
        if task_status['started_at']:
            start_time = datetime.fromisoformat(task_status['started_at'].replace('Z', '+00:00'))
            if task_status['completed_at']:
                end_time = datetime.fromisoformat(task_status['completed_at'].replace('Z', '+00:00'))
                execution_time = (end_time - start_time).total_seconds()
            else:
                execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        # 计算成功率
        total_attempts = sum(record_stats.values())
        success_rate = (record_stats.get('success', 0) / total_attempts * 100) if total_attempts > 0 else 0
        
        return {
            **task_status,
            'record_statistics': record_stats,
            'recent_records': [
                {'date': record[0], 'status': record[1], 'count': record[2]}
                for record in recent_records
            ],
            'execution_time_seconds': execution_time,
            'success_rate': round(success_rate, 2),
            'estimated_completion': self._estimate_completion_time(task_status)
        }
    
    def _estimate_completion_time(self, task_status: Dict[str, Any]) -> Optional[str]:
        """
        估算任务完成时间
        
        Args:
            task_status: 任务状态
            
        Returns:
            预计完成时间
        """
        if task_status['status'] in ['completed', 'failed']:
            return None
        
        if not task_status['started_at'] or task_status['progress'] <= 0:
            return None
        
        try:
            start_time = datetime.fromisoformat(task_status['started_at'].replace('Z', '+00:00'))
            elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # 基于当前进度估算剩余时间
            progress_ratio = task_status['progress'] / 100.0
            if progress_ratio > 0:
                total_estimated_time = elapsed_time / progress_ratio
                remaining_time = total_estimated_time - elapsed_time
                
                estimated_completion = datetime.now(timezone.utc) + timedelta(seconds=remaining_time)
                return estimated_completion.isoformat()
        
        except Exception as e:
            logger.error(f"估算完成时间失败: {e}")
        
        return None
    
    def generate_summary_report(self) -> Dict[str, Any]:
        """
        生成回填服务总体报告
        
        Returns:
            总体报告
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 统计任务状态
                cursor.execute("""
                    SELECT status, COUNT(*) as count
                    FROM backfill_tasks
                    GROUP BY status
                """)
                task_stats = {row[0]: row[1] for row in cursor.fetchall()}
                
                # 统计回填记录
                cursor.execute("""
                    SELECT status, COUNT(*) as count
                    FROM backfill_records
                    GROUP BY status
                """)
                record_stats = {row[0]: row[1] for row in cursor.fetchall()}
                
                # 获取最近的任务
                cursor.execute("""
                    SELECT task_id, mode, status, progress, created_at
                    FROM backfill_tasks
                    ORDER BY created_at DESC
                    LIMIT 5
                """)
                recent_tasks = [
                    {
                        'task_id': row[0],
                        'mode': row[1],
                        'status': row[2],
                        'progress': row[3],
                        'created_at': row[4]
                    }
                    for row in cursor.fetchall()
                ]
                
        except Exception as e:
            logger.error(f"生成总体报告失败: {e}")
            task_stats = {}
            record_stats = {}
            recent_tasks = []
        
        # 计算总体成功率
        total_records = sum(record_stats.values())
        success_rate = (record_stats.get('success', 0) / total_records * 100) if total_records > 0 else 0
        
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'active_tasks_count': len(self.active_tasks),
            'task_statistics': task_stats,
            'record_statistics': record_stats,
            'overall_success_rate': round(success_rate, 2),
            'recent_tasks': recent_tasks,
            'system_status': 'healthy' if len(self.active_tasks) < 10 else 'busy'
        }

# 使用示例
if __name__ == "__main__":
    # 创建增强回填服务
    backfill_service = EnhancedBackfillService()
    
    # 智能回填示例
    task_id = backfill_service.create_backfill_task(
        mode=BackfillMode.SMART,
        start_date="2025-09-20",
        end_date="2025-09-25"
    )
    
    print(f"创建智能回填任务: {task_id}")
    
    # 启动任务
    if backfill_service.start_backfill_task(task_id):
        print("任务启动成功")
        
        # 监控任务进度
        while True:
            status = backfill_service.get_task_status(task_id)
            if status:
                print(f"任务进度: {status['progress']:.1f}% ({status['status']})")
                
                if status['status'] in ['completed', 'failed']:
                    print(f"任务结束: {status['status']}")
                    if status['error_message']:
                        print(f"错误信息: {status['error_message']}")
                    break
            
            time.sleep(5)
    else:
        print("任务启动失败")