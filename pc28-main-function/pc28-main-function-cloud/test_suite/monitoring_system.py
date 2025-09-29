#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统监控和工作任务管理系统
实现自动化监控、问题检测、任务队列和一键修复功能
"""

import os
import sys
import json
import time
import logging
import subprocess
import threading
import schedule
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import sqlite3

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'python'))

# 导入测试和修复系统
from pc28_data_flow_test import PC28DataFlowTester
from data_repair_system import PC28DataRepairSystem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskPriority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class WorkTask:
    """工作任务数据类"""
    task_id: str
    task_name: str
    task_type: str  # 'test', 'repair', 'monitor', 'alert'
    priority: TaskPriority
    status: TaskStatus
    description: str
    created_at: str
    scheduled_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None
    retry_count: int = 0
    max_retries: int = 3

class PC28MonitoringSystem:
    """PC28监控系统"""
    
    def __init__(self, db_path: str = "/Users/a606/cloud_function_source/test_suite/monitoring.db"):
        self.db_path = db_path
        self.tester = PC28DataFlowTester()
        self.repair_system = PC28DataRepairSystem()
        self.task_queue = []
        self.lock = threading.Lock()
        self.running = False
        self.worker_thread = None
        
        # 初始化数据库
        self._init_database()
        
        # 设置定时任务
        self._setup_scheduled_tasks()
    
    def _init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS work_tasks (
                    task_id TEXT PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    status TEXT NOT NULL,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    scheduled_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    error_message TEXT,
                    result_data TEXT,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS monitoring_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    log_level TEXT NOT NULL,
                    component TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS system_metrics (
                    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    metric_unit TEXT,
                    tags TEXT
                )
            """)
            
            conn.commit()
    
    def _setup_scheduled_tasks(self):
        """设置定时任务"""
        # 每5分钟运行一次数据流测试
        schedule.every(5).minutes.do(self._schedule_data_flow_test)
        
        # 每15分钟运行一次自动修复
        schedule.every(15).minutes.do(self._schedule_auto_repair)
        
        # 每小时生成一次监控报告
        schedule.every().hour.do(self._schedule_monitoring_report)
        
        # 每天清理旧日志
        schedule.every().day.at("02:00").do(self._schedule_log_cleanup)
    
    def add_task(self, task: WorkTask) -> bool:
        """添加任务到队列"""
        try:
            with self.lock:
                # 保存到数据库
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        INSERT OR REPLACE INTO work_tasks 
                        (task_id, task_name, task_type, priority, status, description, 
                         created_at, scheduled_at, started_at, completed_at, error_message, 
                         result_data, retry_count, max_retries)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        task.task_id, task.task_name, task.task_type, 
                        task.priority.value, task.status.value, task.description,
                        task.created_at, task.scheduled_at, task.started_at, 
                        task.completed_at, task.error_message,
                        json.dumps(task.result_data) if task.result_data else None,
                        task.retry_count, task.max_retries
                    ))
                    conn.commit()
                
                # 添加到内存队列
                self.task_queue.append(task)
                self.task_queue.sort(key=lambda x: (
                    {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}[x.priority.value],
                    x.created_at
                ))
                
                logger.info(f"任务已添加: {task.task_name} (优先级: {task.priority.value})")
                return True
                
        except Exception as e:
            logger.error(f"添加任务失败: {e}")
            return False
    
    def get_pending_tasks(self) -> List[WorkTask]:
        """获取待处理任务"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT * FROM work_tasks 
                    WHERE status = ? 
                    ORDER BY 
                        CASE priority 
                            WHEN 'critical' THEN 0 
                            WHEN 'high' THEN 1 
                            WHEN 'medium' THEN 2 
                            WHEN 'low' THEN 3 
                        END,
                        created_at
                """, (TaskStatus.PENDING.value,))
                
                tasks = []
                for row in cursor.fetchall():
                    task = WorkTask(
                        task_id=row[0],
                        task_name=row[1],
                        task_type=row[2],
                        priority=TaskPriority(row[3]),
                        status=TaskStatus(row[4]),
                        description=row[5],
                        created_at=row[6],
                        scheduled_at=row[7],
                        started_at=row[8],
                        completed_at=row[9],
                        error_message=row[10],
                        result_data=json.loads(row[11]) if row[11] else None,
                        retry_count=row[12],
                        max_retries=row[13]
                    )
                    tasks.append(task)
                
                return tasks
                
        except Exception as e:
            logger.error(f"获取待处理任务失败: {e}")
            return []
    
    def execute_task(self, task: WorkTask) -> bool:
        """执行任务"""
        try:
            # 更新任务状态
            task.status = TaskStatus.IN_PROGRESS
            task.started_at = datetime.now().isoformat()
            self._update_task_in_db(task)
            
            logger.info(f"开始执行任务: {task.task_name}")
            
            success = False
            result_data = None
            
            if task.task_type == "test":
                success, result_data = self._execute_test_task(task)
            elif task.task_type == "repair":
                success, result_data = self._execute_repair_task(task)
            elif task.task_type == "monitor":
                success, result_data = self._execute_monitor_task(task)
            elif task.task_type == "alert":
                success, result_data = self._execute_alert_task(task)
            else:
                logger.warning(f"未知任务类型: {task.task_type}")
                success = False
            
            # 更新任务结果
            if success:
                task.status = TaskStatus.COMPLETED
                task.completed_at = datetime.now().isoformat()
                task.result_data = result_data
                logger.info(f"任务执行成功: {task.task_name}")
            else:
                task.retry_count += 1
                if task.retry_count >= task.max_retries:
                    task.status = TaskStatus.FAILED
                    task.completed_at = datetime.now().isoformat()
                    logger.error(f"任务执行失败，已达最大重试次数: {task.task_name}")
                else:
                    task.status = TaskStatus.PENDING
                    logger.warning(f"任务执行失败，将重试 ({task.retry_count}/{task.max_retries}): {task.task_name}")
            
            self._update_task_in_db(task)
            return success
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error_message = str(e)
            task.completed_at = datetime.now().isoformat()
            self._update_task_in_db(task)
            logger.error(f"执行任务异常: {task.task_name}, {e}")
            return False
    
    def _execute_test_task(self, task: WorkTask) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """执行测试任务"""
        try:
            if "data_flow" in task.description.lower():
                summary = self.tester.run_full_test_suite()
                success = summary['failed'] == 0
                return success, summary
            else:
                return False, {"error": "未知的测试类型"}
                
        except Exception as e:
            logger.error(f"执行测试任务异常: {e}")
            return False, {"error": str(e)}
    
    def _execute_repair_task(self, task: WorkTask) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """执行修复任务"""
        try:
            summary = self.repair_system.run_auto_repair()
            success = summary['failed'] == 0
            return success, summary
            
        except Exception as e:
            logger.error(f"执行修复任务异常: {e}")
            return False, {"error": str(e)}
    
    def _execute_monitor_task(self, task: WorkTask) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """执行监控任务"""
        try:
            # 收集系统指标
            metrics = self._collect_system_metrics()
            
            # 保存指标到数据库
            self._save_metrics(metrics)
            
            return True, {"metrics": metrics}
            
        except Exception as e:
            logger.error(f"执行监控任务异常: {e}")
            return False, {"error": str(e)}
    
    def _execute_alert_task(self, task: WorkTask) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """执行告警任务"""
        try:
            # 这里可以集成Telegram、邮件等告警方式
            logger.info(f"发送告警: {task.description}")
            return True, {"alert_sent": True}
            
        except Exception as e:
            logger.error(f"执行告警任务异常: {e}")
            return False, {"error": str(e)}
    
    def _collect_system_metrics(self) -> Dict[str, Any]:
        """收集系统指标"""
        metrics = {}
        
        try:
            # 数据表行数统计
            tables = [
                "cloud_pred_today_norm",
                "signal_pool",
                "p_cloud_today_v",
                "signal_pool_union_v3"
            ]
            
            for table in tables:
                query = f"SELECT COUNT(*) as count FROM `wprojectl.pc28_lab.{table}`"
                success, result = self.repair_system.run_bq_query(query)
                if success and result:
                    metrics[f"{table}_count"] = int(result[0]['count'])
                else:
                    metrics[f"{table}_count"] = -1
            
            # 系统健康状态
            test_summary = self.tester.run_full_test_suite()
            metrics["test_success_rate"] = test_summary['success_rate']
            metrics["test_passed"] = test_summary['passed']
            metrics["test_failed"] = test_summary['failed']
            
        except Exception as e:
            logger.error(f"收集系统指标异常: {e}")
            metrics["error"] = str(e)
        
        return metrics
    
    def _save_metrics(self, metrics: Dict[str, Any]):
        """保存指标到数据库"""
        try:
            timestamp = datetime.now().isoformat()
            
            with sqlite3.connect(self.db_path) as conn:
                for metric_name, metric_value in metrics.items():
                    if isinstance(metric_value, (int, float)):
                        conn.execute("""
                            INSERT INTO system_metrics 
                            (timestamp, metric_name, metric_value, metric_unit, tags)
                            VALUES (?, ?, ?, ?, ?)
                        """, (timestamp, metric_name, metric_value, "", ""))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存指标异常: {e}")
    
    def _update_task_in_db(self, task: WorkTask):
        """更新数据库中的任务"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE work_tasks SET
                        status = ?, started_at = ?, completed_at = ?,
                        error_message = ?, result_data = ?, retry_count = ?
                    WHERE task_id = ?
                """, (
                    task.status.value, task.started_at, task.completed_at,
                    task.error_message, 
                    json.dumps(task.result_data) if task.result_data else None,
                    task.retry_count, task.task_id
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"更新任务数据库异常: {e}")
    
    def _schedule_data_flow_test(self):
        """调度数据流测试"""
        task = WorkTask(
            task_id=f"test_data_flow_{int(time.time())}",
            task_name="定时数据流测试",
            task_type="test",
            priority=TaskPriority.MEDIUM,
            status=TaskStatus.PENDING,
            description="定时执行数据流完整性测试",
            created_at=datetime.now().isoformat()
        )
        self.add_task(task)
    
    def _schedule_auto_repair(self):
        """调度自动修复"""
        task = WorkTask(
            task_id=f"repair_auto_{int(time.time())}",
            task_name="定时自动修复",
            task_type="repair",
            priority=TaskPriority.HIGH,
            status=TaskStatus.PENDING,
            description="定时执行自动修复流程",
            created_at=datetime.now().isoformat()
        )
        self.add_task(task)
    
    def _schedule_monitoring_report(self):
        """调度监控报告"""
        task = WorkTask(
            task_id=f"monitor_report_{int(time.time())}",
            task_name="定时监控报告",
            task_type="monitor",
            priority=TaskPriority.LOW,
            status=TaskStatus.PENDING,
            description="定时生成系统监控报告",
            created_at=datetime.now().isoformat()
        )
        self.add_task(task)
    
    def _schedule_log_cleanup(self):
        """调度日志清理"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 删除7天前的日志
                cutoff_date = (datetime.now() - timedelta(days=7)).isoformat()
                conn.execute("DELETE FROM monitoring_logs WHERE timestamp < ?", (cutoff_date,))
                conn.execute("DELETE FROM system_metrics WHERE timestamp < ?", (cutoff_date,))
                conn.commit()
                
            logger.info("日志清理完成")
            
        except Exception as e:
            logger.error(f"日志清理异常: {e}")
    
    def start_monitoring(self):
        """启动监控系统"""
        if self.running:
            logger.warning("监控系统已在运行")
            return
        
        self.running = True
        logger.info("启动PC28监控系统...")
        
        # 启动工作线程
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        
        # 启动调度线程
        schedule_thread = threading.Thread(target=self._schedule_loop, daemon=True)
        schedule_thread.start()
        
        logger.info("监控系统已启动")
    
    def stop_monitoring(self):
        """停止监控系统"""
        self.running = False
        logger.info("监控系统已停止")
    
    def _worker_loop(self):
        """工作线程循环"""
        while self.running:
            try:
                # 获取待处理任务
                pending_tasks = self.get_pending_tasks()
                
                for task in pending_tasks:
                    if not self.running:
                        break
                    
                    # 检查是否到了执行时间
                    if task.scheduled_at:
                        scheduled_time = datetime.fromisoformat(task.scheduled_at)
                        if datetime.now() < scheduled_time:
                            continue
                    
                    # 执行任务
                    self.execute_task(task)
                
                # 休眠一段时间
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"工作线程异常: {e}")
                time.sleep(30)
    
    def _schedule_loop(self):
        """调度线程循环"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
                
            except Exception as e:
                logger.error(f"调度线程异常: {e}")
                time.sleep(60)
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 任务统计
                cursor = conn.execute("""
                    SELECT status, COUNT(*) as count 
                    FROM work_tasks 
                    WHERE created_at > datetime('now', '-24 hours')
                    GROUP BY status
                """)
                task_stats = {row[0]: row[1] for row in cursor.fetchall()}
                
                # 最新指标
                cursor = conn.execute("""
                    SELECT metric_name, metric_value 
                    FROM system_metrics 
                    WHERE timestamp > datetime('now', '-1 hour')
                    ORDER BY timestamp DESC
                    LIMIT 20
                """)
                latest_metrics = {row[0]: row[1] for row in cursor.fetchall()}
                
                return {
                    "monitoring_active": self.running,
                    "task_queue_size": len(self.task_queue),
                    "task_stats_24h": task_stats,
                    "latest_metrics": latest_metrics,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"获取系统状态异常: {e}")
            return {"error": str(e)}

def main():
    """主函数"""
    monitoring_system = PC28MonitoringSystem()
    
    try:
        # 启动监控系统
        monitoring_system.start_monitoring()
        
        # 添加初始测试任务
        initial_task = WorkTask(
            task_id=f"initial_test_{int(time.time())}",
            task_name="初始数据流测试",
            task_type="test",
            priority=TaskPriority.HIGH,
            status=TaskStatus.PENDING,
            description="系统启动后的初始数据流完整性测试",
            created_at=datetime.now().isoformat()
        )
        monitoring_system.add_task(initial_task)
        
        print("PC28监控系统已启动")
        print("按 Ctrl+C 停止监控")
        
        # 主循环
        while True:
            time.sleep(30)
            status = monitoring_system.get_system_status()
            print(f"\n系统状态: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"监控状态: {'运行中' if status['monitoring_active'] else '已停止'}")
            print(f"任务队列: {status['task_queue_size']} 个待处理")
            
            if status.get('task_stats_24h'):
                print("24小时任务统计:")
                for status_name, count in status['task_stats_24h'].items():
                    print(f"  {status_name}: {count}")
            
    except KeyboardInterrupt:
        print("\n正在停止监控系统...")
        monitoring_system.stop_monitoring()
        print("监控系统已停止")
    
    except Exception as e:
        logger.error(f"监控系统异常: {e}")
        monitoring_system.stop_monitoring()

if __name__ == "__main__":
    main()