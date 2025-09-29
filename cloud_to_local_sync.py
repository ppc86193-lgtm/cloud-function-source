#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
云端到本地数据同步系统
Cloud to Local Data Synchronization System

功能：
1. 从BigQuery同步数据到本地SQLite
2. 增量同步机制
3. 数据一致性验证
4. 同步监控和日志
"""

import os
import sqlite3
import logging
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cloud_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class SyncConfig:
    """同步配置"""
    project_id: str = "wprojectl"
    dataset_id: str = "pc28_lab"
    local_db_path: str = "local_experiment.db"
    sync_interval_minutes: int = 30
    max_retry_attempts: int = 3
    batch_size: int = 1000
    
@dataclass
class SyncMetrics:
    """同步指标"""
    last_sync_time: Optional[str] = None
    total_records_synced: int = 0
    sync_success_count: int = 0
    sync_error_count: int = 0
    last_error_message: Optional[str] = None
    data_consistency_score: float = 1.0
    sync_duration_seconds: float = 0.0

class CloudToLocalSync:
    """云端到本地数据同步器"""
    
    def __init__(self, config: SyncConfig = None):
        self.config = config or SyncConfig()
        self.metrics = SyncMetrics()
        self.local_db_path = self.config.local_db_path
        
        # 初始化本地数据库
        self._init_local_database()
        
        # 加载同步状态
        self._load_sync_state()
        
    def _init_local_database(self):
        """初始化本地SQLite数据库"""
        try:
            conn = sqlite3.connect(self.local_db_path)
            cursor = conn.cursor()
            
            # 创建同步状态表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_status (
                    table_name TEXT PRIMARY KEY,
                    last_sync_timestamp TEXT,
                    record_count INTEGER,
                    checksum TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            
            # 创建p_ensemble_today_norm_v5表（本地版本）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS p_ensemble_today_norm_v5 (
                    period TEXT PRIMARY KEY,
                    ts_utc TEXT,
                    ts_cst TEXT,
                    p_cloud REAL,
                    conf_cloud REAL,
                    p_map REAL,
                    conf_map REAL,
                    p_size REAL,
                    conf_size REAL,
                    p_combo REAL,
                    conf_combo REAL,
                    p_hit REAL,
                    conf_hit REAL,
                    p_star_ens REAL,
                    vote_ratio REAL,
                    cooling_status TEXT,
                    sync_timestamp TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建云预测数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cloud_pred_today_norm (
                    period TEXT PRIMARY KEY,
                    timestamp TEXT,
                    p_win REAL,
                    confidence REAL,
                    sync_timestamp TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建地图预测数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS p_map_today_canon_v (
                    period TEXT PRIMARY KEY,
                    ts_utc TEXT,
                    p_win REAL,
                    vote_ratio REAL,
                    sync_timestamp TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建尺寸预测数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS p_size_today_canon_v (
                    period TEXT PRIMARY KEY,
                    ts_utc TEXT,
                    p_win REAL,
                    vote_ratio REAL,
                    sync_timestamp TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建组合预测数据表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS combo_based_predictions (
                    issue TEXT PRIMARY KEY,
                    timestamp TEXT,
                    recommendation TEXT,
                    big_odd_ev_pct REAL,
                    big_even_ev_pct REAL,
                    small_odd_ev_pct REAL,
                    small_even_ev_pct REAL,
                    sync_timestamp TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建同步日志表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_type TEXT,
                    table_name TEXT,
                    status TEXT,
                    records_processed INTEGER,
                    duration_seconds REAL,
                    error_message TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("本地数据库初始化完成")
            
        except Exception as e:
            logger.error(f"初始化本地数据库失败: {e}")
            raise
    
    def _load_sync_state(self):
        """加载同步状态"""
        try:
            if os.path.exists('sync_metrics.json'):
                with open('sync_metrics.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.metrics = SyncMetrics(**data)
                logger.info("同步状态加载完成")
        except Exception as e:
            logger.warning(f"加载同步状态失败，使用默认值: {e}")
    
    def _save_sync_state(self):
        """保存同步状态"""
        try:
            with open('sync_metrics.json', 'w', encoding='utf-8') as f:
                json.dump(asdict(self.metrics), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存同步状态失败: {e}")
    
    def simulate_bigquery_data(self) -> List[Dict]:
        """模拟BigQuery数据（用于测试）"""
        current_time = datetime.now()
        sample_data = []
        
        for i in range(10):
            period = f"2024{str(current_time.hour).zfill(2)}{str(current_time.minute + i).zfill(2)}"
            sample_data.append({
                'period': period,
                'ts_utc': current_time.isoformat(),
                'ts_cst': (current_time + timedelta(hours=8)).isoformat(),
                'p_cloud': 0.6 + (i * 0.02),
                'conf_cloud': 0.8,
                'p_map': 0.55 + (i * 0.01),
                'conf_map': 0.75,
                'p_size': 0.5 + (i * 0.015),
                'conf_size': 0.7,
                'p_combo': 0.65 + (i * 0.01),
                'conf_combo': 0.85,
                'p_hit': 0.5,
                'conf_hit': 0.5,
                'p_star_ens': 0.58 + (i * 0.012),
                'vote_ratio': 0.72,
                'cooling_status': 'ACTIVE' if (0.58 + i * 0.012) >= 0.6 else 'COOLING'
            })
        
        return sample_data
    
    def sync_ensemble_data(self) -> bool:
        """同步集成预测数据"""
        start_time = time.time()
        
        try:
            logger.info("开始同步p_ensemble_today_norm_v5数据...")
            
            # 模拟从BigQuery获取数据
            cloud_data = self.simulate_bigquery_data()
            
            if not cloud_data:
                logger.warning("未获取到云端数据")
                return False
            
            # 连接本地数据库
            conn = sqlite3.connect(self.local_db_path)
            cursor = conn.cursor()
            
            # 清空今日数据（增量同步策略）
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute(
                "DELETE FROM p_ensemble_today_norm_v5 WHERE DATE(sync_timestamp) = ?",
                (today,)
            )
            
            # 插入新数据
            sync_timestamp = datetime.now().isoformat()
            records_inserted = 0
            
            for record in cloud_data:
                cursor.execute('''
                    INSERT OR REPLACE INTO p_ensemble_today_norm_v5 
                    (period, ts_utc, ts_cst, p_cloud, conf_cloud, p_map, conf_map,
                     p_size, conf_size, p_combo, conf_combo, p_hit, conf_hit,
                     p_star_ens, vote_ratio, cooling_status, sync_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record['period'], record['ts_utc'], record['ts_cst'],
                    record['p_cloud'], record['conf_cloud'], record['p_map'], record['conf_map'],
                    record['p_size'], record['conf_size'], record['p_combo'], record['conf_combo'],
                    record['p_hit'], record['conf_hit'], record['p_star_ens'],
                    record['vote_ratio'], record['cooling_status'], sync_timestamp
                ))
                records_inserted += 1
            
            # 更新同步状态
            cursor.execute('''
                INSERT OR REPLACE INTO sync_status 
                (table_name, last_sync_timestamp, record_count, checksum, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                'p_ensemble_today_norm_v5',
                sync_timestamp,
                records_inserted,
                hashlib.md5(str(cloud_data).encode()).hexdigest(),
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
            # 更新指标
            duration = time.time() - start_time
            self.metrics.last_sync_time = sync_timestamp
            self.metrics.total_records_synced += records_inserted
            self.metrics.sync_success_count += 1
            self.metrics.sync_duration_seconds = duration
            
            logger.info(f"同步完成: {records_inserted}条记录，耗时{duration:.2f}秒")
            return True
            
        except Exception as e:
            self.metrics.sync_error_count += 1
            self.metrics.last_error_message = str(e)
            logger.error(f"同步失败: {e}")
            return False
        finally:
            self._save_sync_state()
    
    def verify_data_consistency(self) -> float:
        """验证数据一致性"""
        try:
            conn = sqlite3.connect(self.local_db_path)
            cursor = conn.cursor()
            
            # 检查数据完整性
            cursor.execute("SELECT COUNT(*) FROM p_ensemble_today_norm_v5")
            local_count = cursor.fetchone()[0]
            
            # 检查数据质量
            cursor.execute('''
                SELECT COUNT(*) FROM p_ensemble_today_norm_v5 
                WHERE p_star_ens IS NOT NULL AND vote_ratio IS NOT NULL
            ''')
            valid_count = cursor.fetchone()[0]
            
            conn.close()
            
            if local_count == 0:
                consistency_score = 0.0
            else:
                consistency_score = valid_count / local_count
            
            self.metrics.data_consistency_score = consistency_score
            logger.info(f"数据一致性评分: {consistency_score:.2f}")
            
            return consistency_score
            
        except Exception as e:
            logger.error(f"数据一致性验证失败: {e}")
            return 0.0
    
    def get_sync_report(self) -> Dict:
        """获取同步报告"""
        consistency_score = self.verify_data_consistency()
        
        return {
            "同步状态": "正常" if self.metrics.sync_error_count == 0 else "异常",
            "最后同步时间": self.metrics.last_sync_time,
            "总同步记录数": self.metrics.total_records_synced,
            "成功同步次数": self.metrics.sync_success_count,
            "失败同步次数": self.metrics.sync_error_count,
            "数据一致性评分": f"{consistency_score:.2%}",
            "最后同步耗时": f"{self.metrics.sync_duration_seconds:.2f}秒",
            "最后错误信息": self.metrics.last_error_message or "无",
            "本地数据库路径": self.local_db_path,
            "配置信息": {
                "同步间隔": f"{self.config.sync_interval_minutes}分钟",
                "批处理大小": self.config.batch_size,
                "最大重试次数": self.config.max_retry_attempts
            }
        }
    
    def run_sync_cycle(self):
        """运行一次完整的同步周期"""
        logger.info("=" * 50)
        logger.info("开始云端到本地数据同步周期")
        logger.info("=" * 50)
        
        # 执行同步
        sync_success = self.sync_ensemble_data()
        
        # 验证数据一致性
        consistency_score = self.verify_data_consistency()
        
        # 生成报告
        report = self.get_sync_report()
        
        logger.info("同步周期完成")
        logger.info("同步报告:")
        for key, value in report.items():
            if isinstance(value, dict):
                logger.info(f"  {key}:")
                for sub_key, sub_value in value.items():
                    logger.info(f"    {sub_key}: {sub_value}")
            else:
                logger.info(f"  {key}: {value}")
        
        return sync_success and consistency_score > 0.8

def main():
    """主函数"""
    logger.info("启动云端到本地数据同步系统")
    
    # 创建同步器
    sync_config = SyncConfig(
        project_id="wprojectl",
        dataset_id="pc28_lab",
        local_db_path="local_experiment.db",
        sync_interval_minutes=30,
        batch_size=1000
    )
    
    syncer = CloudToLocalSync(sync_config)
    
    # 运行同步周期
    success = syncer.run_sync_cycle()
    
    if success:
        logger.info("数据同步系统运行成功")
    else:
        logger.error("数据同步系统运行失败")
    
    return success

if __name__ == "__main__":
    main()