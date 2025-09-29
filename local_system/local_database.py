#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28本地SQLite数据库系统
模拟BigQuery环境，支持所有表和视图的本地化存储
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalDatabase:
    """本地SQLite数据库管理器"""
    
    def __init__(self, db_path: str = "local_system/pc28_local.db"):
        """初始化本地数据库"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
        self._init_database()
    
    def _init_database(self):
        """初始化数据库连接和表结构"""
        try:
            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.row_factory = sqlite3.Row  # 支持字典式访问
            
            # 启用外键约束
            self.conn.execute("PRAGMA foreign_keys = ON")
            
            # 创建所有表结构
            self._create_tables()
            
            logger.info(f"本地数据库初始化完成: {self.db_path}")
            
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def _create_tables(self):
        """创建所有表结构"""
        
        # 1. 原始数据表
        tables_sql = {
            # 云端预测数据表
            "cloud_pred_today_norm": """
                CREATE TABLE IF NOT EXISTS cloud_pred_today_norm (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    period TEXT,
                    market TEXT,
                    pick TEXT,
                    p_win REAL,
                    source TEXT DEFAULT 'cloud_api',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    data_date TEXT,
                    UNIQUE(draw_id, market, pick)
                )
            """,
            
            # 地图预测清洗数据表
            "p_map_clean_merged_dedup_v": """
                CREATE TABLE IF NOT EXISTS p_map_clean_merged_dedup_v (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    period TEXT,
                    market TEXT,
                    pick TEXT,
                    p_win REAL,
                    source TEXT DEFAULT 'map_api',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    data_date TEXT,
                    UNIQUE(draw_id, market, pick)
                )
            """,
            
            # 大小预测清洗数据表
            "p_size_clean_merged_dedup_v": """
                CREATE TABLE IF NOT EXISTS p_size_clean_merged_dedup_v (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    period TEXT,
                    market TEXT,
                    pick TEXT,
                    p_win REAL,
                    source TEXT DEFAULT 'size_api',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    data_date TEXT,
                    UNIQUE(draw_id, market, pick)
                )
            """,
            
            # 运行时参数表
            "runtime_params": """
                CREATE TABLE IF NOT EXISTS runtime_params (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market TEXT NOT NULL UNIQUE,
                    p_min_base REAL NOT NULL,
                    ev_min REAL NOT NULL,
                    max_kelly REAL NOT NULL,
                    target_acc REAL NOT NULL,
                    target_cov REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            # 信号池表
            "signal_pool_union_v3": """
                CREATE TABLE IF NOT EXISTS signal_pool_union_v3 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    period TEXT,
                    market TEXT,
                    pick TEXT,
                    p_win REAL,
                    source TEXT,
                    vote_ratio REAL,
                    pick_zh TEXT,
                    day_id_cst TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(draw_id, market, pick, source)
                )
            """,
            
            # 决策候选表
            "lab_push_candidates_v2": """
                CREATE TABLE IF NOT EXISTS lab_push_candidates_v2 (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    period TEXT,
                    market TEXT,
                    pick TEXT,
                    p_win REAL,
                    ev REAL,
                    kelly_frac REAL,
                    source TEXT,
                    vote_ratio REAL,
                    pick_zh TEXT,
                    day_id_cst TEXT,
                    draw_id TEXT
                )
            """,
            
            # 系统状态表
            "system_status": """
                CREATE TABLE IF NOT EXISTS system_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    component TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_check TEXT DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT,
                    details TEXT,
                    metadata TEXT,
                    UNIQUE(component)
                )
            """,
            
            # 修复日志表
            "repair_logs": """
                CREATE TABLE IF NOT EXISTS repair_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    component TEXT NOT NULL,
                    issue_type TEXT NOT NULL,
                    repair_action TEXT NOT NULL,
                    action_taken TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details TEXT,
                    execution_time_ms INTEGER
                )
            """,
            
            # 同步状态表
            "sync_status": """
                CREATE TABLE IF NOT EXISTS sync_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL UNIQUE,
                    last_sync TEXT DEFAULT CURRENT_TIMESTAMP,
                    sync_status TEXT NOT NULL,
                    records_synced INTEGER DEFAULT 0,
                    error_message TEXT
                )
            """
        }
        
        # 执行创建表语句
        for table_name, sql in tables_sql.items():
            try:
                self.conn.execute(sql)
                logger.debug(f"创建表: {table_name}")
            except Exception as e:
                logger.error(f"创建表 {table_name} 失败: {e}")
                raise
        
        self.conn.commit()
        
        # 创建索引
        self._create_indexes()
        
        # 初始化运行时参数
        self._init_runtime_params()
    
    def _create_indexes(self):
        """创建索引优化查询性能"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_cloud_pred_draw_id ON cloud_pred_today_norm(draw_id)",
            "CREATE INDEX IF NOT EXISTS idx_cloud_pred_data_date ON cloud_pred_today_norm(data_date)",
            "CREATE INDEX IF NOT EXISTS idx_cloud_pred_market ON cloud_pred_today_norm(market)",
            
            "CREATE INDEX IF NOT EXISTS idx_map_clean_draw_id ON p_map_clean_merged_dedup_v(draw_id)",
            "CREATE INDEX IF NOT EXISTS idx_map_clean_data_date ON p_map_clean_merged_dedup_v(data_date)",
            
            "CREATE INDEX IF NOT EXISTS idx_size_clean_draw_id ON p_size_clean_merged_dedup_v(draw_id)",
            "CREATE INDEX IF NOT EXISTS idx_size_clean_data_date ON p_size_clean_merged_dedup_v(data_date)",
            
            "CREATE INDEX IF NOT EXISTS idx_signal_pool_draw_id ON signal_pool_union_v3(draw_id)",
            "CREATE INDEX IF NOT EXISTS idx_signal_pool_market ON signal_pool_union_v3(market)",
            "CREATE INDEX IF NOT EXISTS idx_signal_pool_day_id ON signal_pool_union_v3(day_id_cst)",
            
            "CREATE INDEX IF NOT EXISTS idx_candidates_market ON lab_push_candidates_v2(market)",
            "CREATE INDEX IF NOT EXISTS idx_candidates_day_id ON lab_push_candidates_v2(day_id_cst)",
            
            "CREATE INDEX IF NOT EXISTS idx_repair_logs_timestamp ON repair_logs(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_repair_logs_component ON repair_logs(component)"
        ]
        
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"创建索引失败: {e}")
        
        self.conn.commit()
    
    def _init_runtime_params(self):
        """初始化运行时参数"""
        default_params = [
            ('oe', 0.56, 1.0e-6, 0.05, 0.8, 0.5),
            ('size', 0.56, 1.0e-6, 0.05, 0.8, 0.5),
            ('pc28', 0.55, 1.0e-6, 0.05, 0.8, 0.5)
        ]
        
        for market, p_min_base, ev_min, max_kelly, target_acc, target_cov in default_params:
            try:
                self.conn.execute("""
                    INSERT OR IGNORE INTO runtime_params 
                    (market, p_min_base, ev_min, max_kelly, target_acc, target_cov)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (market, p_min_base, ev_min, max_kelly, target_acc, target_cov))
            except Exception as e:
                logger.warning(f"初始化运行时参数失败: {e}")
        
        self.conn.commit()
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """执行查询并返回结果"""
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            raise
    
    def execute_update(self, query: str, params: Optional[Tuple] = None) -> int:
        """执行更新操作并返回影响行数"""
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            self.conn.commit()
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"更新执行失败: {e}")
            self.conn.rollback()
            raise
    
    def bulk_insert(self, table_name: str, data: List[Dict], replace: bool = False) -> int:
        """批量插入数据"""
        if not data:
            return 0
        
        try:
            # 获取字段名
            columns = list(data[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)
            
            # 构建SQL语句
            action = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
            sql = f"{action} INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # 准备数据
            values = [[row.get(col) for col in columns] for row in data]
            
            cursor = self.conn.cursor()
            cursor.executemany(sql, values)
            self.conn.commit()
            
            inserted_count = cursor.rowcount
            logger.info(f"批量插入 {table_name}: {inserted_count} 行")
            return inserted_count
            
        except Exception as e:
            logger.error(f"批量插入失败: {e}")
            self.conn.rollback()
            raise
    
    def get_table_count(self, table_name: str, where_clause: str = "") -> int:
        """获取表行数"""
        try:
            sql = f"SELECT COUNT(*) as count FROM {table_name}"
            if where_clause:
                sql += f" WHERE {where_clause}"
            
            result = self.execute_query(sql)
            return result[0]['count'] if result else 0
            
        except Exception as e:
            logger.error(f"获取表行数失败: {e}")
            return 0
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            # 获取各表数据量
            tables = [
                'cloud_pred_today_norm',
                'p_map_clean_merged_dedup_v', 
                'p_size_clean_merged_dedup_v',
                'signal_pool_union_v3',
                'lab_push_candidates_v2'
            ]
            
            status = {
                'timestamp': datetime.now().isoformat(),
                'tables': {},
                'total_records': 0
            }
            
            for table in tables:
                count = self.get_table_count(table)
                status['tables'][table] = {
                    'count': count,
                    'status': 'healthy' if count > 0 else 'empty'
                }
                status['total_records'] += count
            
            return status
            
        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")
            return {'error': str(e)}
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False

def get_local_db() -> LocalDatabase:
    """获取本地数据库实例"""
    return LocalDatabase()

if __name__ == "__main__":
    # 测试数据库初始化
    db = LocalDatabase()
    
    # 检查系统状态
    status = db.get_system_status()
    print(f"系统状态: {json.dumps(status, indent=2, ensure_ascii=False)}")
    
    # 测试插入数据
    test_data = [{
        'draw_id': 'test_001',
        'timestamp': datetime.now().isoformat(),
        'period': '202501',
        'market': 'pc28',
        'pick': 'big',
        'p_win': 0.65,
        'data_date': datetime.now().strftime('%Y-%m-%d')
    }]
    
    inserted = db.bulk_insert('cloud_pred_today_norm', test_data)
    print(f"插入测试数据: {inserted} 行")
    
    # 查询测试
    results = db.execute_query("SELECT * FROM cloud_pred_today_norm LIMIT 5")
    print(f"查询结果: {json.dumps(results, indent=2, ensure_ascii=False)}")
    
    db.close()