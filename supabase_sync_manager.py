"""
Supabase 数据同步管理器
支持 PC28 系统到 Supabase 的实时和批量数据同步
"""

import os
import logging
import sqlite3
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

from supabase_config import get_supabase_client, supabase_manager
from data_type_mapper import data_type_mapper

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupabaseSyncManager:
    """Supabase 数据同步管理器"""
    
    # 核心数据表配置
    CORE_TABLES = {
        'lab_push_candidates_v2': {
            'primary_key': 'draw_id',
            'timestamp_column': 'created_at',
            'batch_size': 1000,
            'sync_mode': 'incremental'
        },
        'cloud_pred_today_norm': {
            'primary_key': 'draw_id',
            'timestamp_column': 'created_at',
            'batch_size': 1000,
            'sync_mode': 'incremental'
        },
        'signal_pool_union_v3': {
            'primary_key': 'signal_id',
            'timestamp_column': 'last_seen',
            'batch_size': 500,
            'sync_mode': 'incremental'
        },
        'p_size_clean_merged_dedup_v': {
            'primary_key': 'size_category',
            'timestamp_column': 'last_updated',
            'batch_size': 100,
            'sync_mode': 'full'
        },
        'draws_14w_dedup_v': {
            'primary_key': 'draw_id',
            'timestamp_column': 'created_at',
            'batch_size': 2000,
            'sync_mode': 'incremental'
        },
        'score_ledger': {
            'primary_key': 'draw_id',
            'timestamp_column': 'evaluation_date',
            'batch_size': 1000,
            'sync_mode': 'incremental'
        }
    }
    
    def __init__(self, sqlite_db_path: Optional[str] = None):
        self.sqlite_db_path = sqlite_db_path or os.getenv('SQLITE_DB_PATH', 'pc28_data.db')
        self.supabase_client = get_supabase_client(use_service_role=True)
        self.sync_stats = {
            'total_syncs': 0,
            'successful_syncs': 0,
            'failed_syncs': 0,
            'total_records_synced': 0,
            'sync_history': []
        }
        
        # 同步配置
        self.max_workers = int(os.getenv('SYNC_MAX_WORKERS', '4'))
        self.sync_timeout = int(os.getenv('SYNC_TIMEOUT_SECONDS', '300'))
        self.retry_attempts = int(os.getenv('SYNC_RETRY_ATTEMPTS', '3'))
        
    def get_sqlite_connection(self) -> sqlite3.Connection:
        """获取 SQLite 数据库连接"""
        try:
            conn = sqlite3.connect(self.sqlite_db_path)
            conn.row_factory = sqlite3.Row  # 使结果可以按列名访问
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise
    
    def get_table_schema(self, table_name: str, conn: sqlite3.Connection) -> Dict[str, str]:
        """
        获取表的 schema 信息
        
        Args:
            table_name: 表名
            conn: SQLite 连接
            
        Returns:
            列名到数据类型的映射
        """
        try:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            schema = {}
            for column in columns:
                column_name = column[1]
                column_type = column[2]
                schema[column_name] = column_type
            
            logger.debug(f"Retrieved schema for table {table_name}: {schema}")
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return {}
    
    def get_last_sync_timestamp(self, table_name: str) -> Optional[datetime]:
        """
        获取表的最后同步时间戳
        
        Args:
            table_name: 表名
            
        Returns:
            最后同步时间戳
        """
        try:
            response = self.supabase_client.table('sync_status') \
                .select('last_sync_timestamp') \
                .eq('table_name', table_name) \
                .order('created_at', desc=True) \
                .limit(1) \
                .execute()
            
            if response.data:
                timestamp_str = response.data[0]['last_sync_timestamp']
                if timestamp_str:
                    return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get last sync timestamp for {table_name}: {e}")
            return None
    
    def update_sync_status(self, table_name: str, status: str, 
                          records_synced: int = 0, error_message: str = None,
                          sync_duration: float = 0.0, sync_mode: str = 'incremental'):
        """
        更新同步状态
        
        Args:
            table_name: 表名
            status: 同步状态
            records_synced: 同步的记录数
            error_message: 错误消息
            sync_duration: 同步持续时间（秒）
            sync_mode: 同步模式
        """
        try:
            sync_data = {
                'table_name': table_name,
                'last_sync_timestamp': datetime.now().isoformat(),
                'sync_mode': sync_mode,
                'records_synced': records_synced,
                'sync_duration_seconds': sync_duration,
                'status': status,
                'error_message': error_message
            }
            
            self.supabase_client.table('sync_status').insert(sync_data).execute()
            logger.info(f"Updated sync status for {table_name}: {status}")
            
        except Exception as e:
            logger.error(f"Failed to update sync status for {table_name}: {e}")
    
    def create_data_hash(self, data: Dict[str, Any]) -> str:
        """
        为数据行创建哈希值，用于检测变更
        
        Args:
            data: 数据行
            
        Returns:
            数据哈希值
        """
        # 排除元数据字段
        filtered_data = {k: v for k, v in data.items() 
                        if k not in ['id', 'created_at', 'updated_at', 'sync_source', 'sync_timestamp']}
        
        data_str = json.dumps(filtered_data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def sync_table_incremental(self, table_name: str) -> Tuple[bool, int, str]:
        """
        增量同步表数据
        
        Args:
            table_name: 表名
            
        Returns:
            (成功标志, 同步记录数, 错误消息)
        """
        start_time = time.time()
        
        try:
            table_config = self.CORE_TABLES.get(table_name, {})
            batch_size = table_config.get('batch_size', 1000)
            timestamp_column = table_config.get('timestamp_column', 'created_at')
            
            # 获取最后同步时间戳
            last_sync = self.get_last_sync_timestamp(table_name)
            
            # 构建查询条件
            with self.get_sqlite_connection() as conn:
                if last_sync:
                    query = f"""
                    SELECT * FROM {table_name} 
                    WHERE {timestamp_column} > ? 
                    ORDER BY {timestamp_column}
                    """
                    cursor = conn.execute(query, (last_sync.isoformat(),))
                else:
                    query = f"SELECT * FROM {table_name} ORDER BY {timestamp_column}"
                    cursor = conn.execute(query)
                
                # 获取表结构
                schema = self.get_table_schema(table_name, conn)
                
                # 批量处理数据
                records_synced = 0
                batch_data = []
                
                for row in cursor:
                    # 转换为字典
                    row_dict = dict(row)
                    
                    # 数据类型转换
                    converted_row = {}
                    for column, value in row_dict.items():
                        if column in schema:
                            postgres_type = data_type_mapper.map_sqlite_type(schema[column])
                            converted_row[column] = data_type_mapper.convert_value(value, postgres_type)
                        else:
                            converted_row[column] = value
                    
                    # 添加同步元数据
                    converted_row['sync_source'] = 'sqlite'
                    converted_row['sync_timestamp'] = datetime.now().isoformat()
                    
                    batch_data.append(converted_row)
                    
                    # 批量插入
                    if len(batch_data) >= batch_size:
                        self._insert_batch(table_name, batch_data)
                        records_synced += len(batch_data)
                        batch_data = []
                        logger.info(f"Synced {records_synced} records for {table_name}")
                
                # 处理剩余数据
                if batch_data:
                    self._insert_batch(table_name, batch_data)
                    records_synced += len(batch_data)
            
            # 更新同步状态
            sync_duration = time.time() - start_time
            self.update_sync_status(table_name, 'completed', records_synced, 
                                  sync_duration=sync_duration, sync_mode='incremental')
            
            logger.info(f"Incremental sync completed for {table_name}: {records_synced} records in {sync_duration:.2f}s")
            return True, records_synced, None
            
        except Exception as e:
            error_msg = f"Incremental sync failed for {table_name}: {str(e)}"
            logger.error(error_msg)
            
            sync_duration = time.time() - start_time
            self.update_sync_status(table_name, 'failed', 0, error_msg, 
                                  sync_duration=sync_duration, sync_mode='incremental')
            
            return False, 0, error_msg
    
    def sync_table_full(self, table_name: str) -> Tuple[bool, int, str]:
        """
        全量同步表数据
        
        Args:
            table_name: 表名
            
        Returns:
            (成功标志, 同步记录数, 错误消息)
        """
        start_time = time.time()
        
        try:
            table_config = self.CORE_TABLES.get(table_name, {})
            batch_size = table_config.get('batch_size', 1000)
            
            # 清空目标表
            self.supabase_client.table(table_name).delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
            logger.info(f"Cleared existing data in {table_name}")
            
            # 从 SQLite 读取所有数据
            with self.get_sqlite_connection() as conn:
                query = f"SELECT * FROM {table_name}"
                cursor = conn.execute(query)
                
                # 获取表结构
                schema = self.get_table_schema(table_name, conn)
                
                # 批量处理数据
                records_synced = 0
                batch_data = []
                
                for row in cursor:
                    # 转换为字典
                    row_dict = dict(row)
                    
                    # 数据类型转换
                    converted_row = {}
                    for column, value in row_dict.items():
                        if column in schema:
                            postgres_type = data_type_mapper.map_sqlite_type(schema[column])
                            converted_row[column] = data_type_mapper.convert_value(value, postgres_type)
                        else:
                            converted_row[column] = value
                    
                    # 添加同步元数据
                    converted_row['sync_source'] = 'sqlite'
                    converted_row['sync_timestamp'] = datetime.now().isoformat()
                    
                    batch_data.append(converted_row)
                    
                    # 批量插入
                    if len(batch_data) >= batch_size:
                        self._insert_batch(table_name, batch_data)
                        records_synced += len(batch_data)
                        batch_data = []
                        logger.info(f"Synced {records_synced} records for {table_name}")
                
                # 处理剩余数据
                if batch_data:
                    self._insert_batch(table_name, batch_data)
                    records_synced += len(batch_data)
            
            # 更新同步状态
            sync_duration = time.time() - start_time
            self.update_sync_status(table_name, 'completed', records_synced, 
                                  sync_duration=sync_duration, sync_mode='full')
            
            logger.info(f"Full sync completed for {table_name}: {records_synced} records in {sync_duration:.2f}s")
            return True, records_synced, None
            
        except Exception as e:
            error_msg = f"Full sync failed for {table_name}: {str(e)}"
            logger.error(error_msg)
            
            sync_duration = time.time() - start_time
            self.update_sync_status(table_name, 'failed', 0, error_msg, 
                                  sync_duration=sync_duration, sync_mode='full')
            
            return False, 0, error_msg
    
    def _insert_batch(self, table_name: str, batch_data: List[Dict[str, Any]]):
        """
        批量插入数据到 Supabase
        
        Args:
            table_name: 表名
            batch_data: 批量数据
        """
        try:
            # 使用 upsert 来处理重复数据
            response = self.supabase_client.table(table_name).upsert(batch_data).execute()
            
            if not response.data:
                logger.warning(f"No data returned from upsert for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to insert batch for {table_name}: {e}")
            # 尝试逐条插入以识别问题记录
            self._insert_records_individually(table_name, batch_data)
    
    def _insert_records_individually(self, table_name: str, records: List[Dict[str, Any]]):
        """
        逐条插入记录，用于错误恢复
        
        Args:
            table_name: 表名
            records: 记录列表
        """
        successful_inserts = 0
        failed_inserts = 0
        
        for record in records:
            try:
                self.supabase_client.table(table_name).upsert([record]).execute()
                successful_inserts += 1
            except Exception as e:
                failed_inserts += 1
                logger.error(f"Failed to insert individual record in {table_name}: {e}")
                logger.debug(f"Problematic record: {record}")
        
        logger.info(f"Individual insert results for {table_name}: {successful_inserts} successful, {failed_inserts} failed")
    
    def sync_all_tables(self, sync_mode: str = 'auto') -> Dict[str, Any]:
        """
        同步所有核心表
        
        Args:
            sync_mode: 同步模式 ('auto', 'incremental', 'full')
            
        Returns:
            同步结果摘要
        """
        start_time = time.time()
        results = {
            'start_time': datetime.now().isoformat(),
            'sync_mode': sync_mode,
            'table_results': {},
            'total_records_synced': 0,
            'successful_tables': 0,
            'failed_tables': 0
        }
        
        logger.info(f"Starting sync for all tables with mode: {sync_mode}")
        
        # 使用线程池并行同步
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_table = {}
            
            for table_name, config in self.CORE_TABLES.items():
                # 确定同步模式
                if sync_mode == 'auto':
                    table_sync_mode = config.get('sync_mode', 'incremental')
                else:
                    table_sync_mode = sync_mode
                
                # 提交同步任务
                if table_sync_mode == 'full':
                    future = executor.submit(self.sync_table_full, table_name)
                else:
                    future = executor.submit(self.sync_table_incremental, table_name)
                
                future_to_table[future] = table_name
            
            # 收集结果
            for future in as_completed(future_to_table, timeout=self.sync_timeout):
                table_name = future_to_table[future]
                
                try:
                    success, records_synced, error_msg = future.result()
                    
                    results['table_results'][table_name] = {
                        'success': success,
                        'records_synced': records_synced,
                        'error_message': error_msg
                    }
                    
                    if success:
                        results['successful_tables'] += 1
                        results['total_records_synced'] += records_synced
                    else:
                        results['failed_tables'] += 1
                    
                except Exception as e:
                    error_msg = f"Sync task failed for {table_name}: {str(e)}"
                    logger.error(error_msg)
                    
                    results['table_results'][table_name] = {
                        'success': False,
                        'records_synced': 0,
                        'error_message': error_msg
                    }
                    results['failed_tables'] += 1
        
        # 完成同步
        results['end_time'] = datetime.now().isoformat()
        results['duration_seconds'] = time.time() - start_time
        
        # 更新统计信息
        self.sync_stats['total_syncs'] += 1
        if results['failed_tables'] == 0:
            self.sync_stats['successful_syncs'] += 1
        else:
            self.sync_stats['failed_syncs'] += 1
        
        self.sync_stats['total_records_synced'] += results['total_records_synced']
        self.sync_stats['sync_history'].append(results)
        
        # 保留最近 10 次同步历史
        if len(self.sync_stats['sync_history']) > 10:
            self.sync_stats['sync_history'] = self.sync_stats['sync_history'][-10:]
        
        logger.info(f"Sync completed: {results['successful_tables']} successful, {results['failed_tables']} failed, "
                   f"{results['total_records_synced']} total records in {results['duration_seconds']:.2f}s")
        
        return results
    
    def get_sync_stats(self) -> Dict[str, Any]:
        """获取同步统计信息"""
        return self.sync_stats.copy()
    
    def validate_sync_integrity(self, table_name: str) -> Dict[str, Any]:
        """
        验证同步数据完整性
        
        Args:
            table_name: 表名
            
        Returns:
            验证结果
        """
        try:
            # 获取 SQLite 记录数
            with self.get_sqlite_connection() as conn:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                sqlite_count = cursor.fetchone()[0]
            
            # 获取 Supabase 记录数
            response = self.supabase_client.table(table_name).select('id', count='exact').execute()
            supabase_count = response.count
            
            # 计算差异
            difference = abs(sqlite_count - supabase_count)
            integrity_score = 1.0 - (difference / max(sqlite_count, 1))
            
            result = {
                'table_name': table_name,
                'sqlite_count': sqlite_count,
                'supabase_count': supabase_count,
                'difference': difference,
                'integrity_score': integrity_score,
                'status': 'passed' if integrity_score >= 0.95 else 'failed',
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Integrity check for {table_name}: {result['status']} (score: {integrity_score:.3f})")
            return result
            
        except Exception as e:
            error_msg = f"Integrity validation failed for {table_name}: {str(e)}"
            logger.error(error_msg)
            
            return {
                'table_name': table_name,
                'status': 'error',
                'error_message': error_msg,
                'timestamp': datetime.now().isoformat()
            }

# 全局同步管理器实例
sync_manager = SupabaseSyncManager()

def sync_table(table_name: str, sync_mode: str = 'incremental') -> Tuple[bool, int, str]:
    """
    同步单个表的便捷函数
    
    Args:
        table_name: 表名
        sync_mode: 同步模式
        
    Returns:
        (成功标志, 同步记录数, 错误消息)
    """
    if sync_mode == 'full':
        return sync_manager.sync_table_full(table_name)
    else:
        return sync_manager.sync_table_incremental(table_name)

def sync_all_tables(sync_mode: str = 'auto') -> Dict[str, Any]:
    """
    同步所有表的便捷函数
    
    Args:
        sync_mode: 同步模式
        
    Returns:
        同步结果摘要
    """
    return sync_manager.sync_all_tables(sync_mode)

if __name__ == "__main__":
    # 测试同步管理器
    print("Testing Supabase Sync Manager...")
    
    try:
        manager = SupabaseSyncManager()
        
        # 测试连接
        if supabase_manager.test_connection()['status'] == 'success':
            print("✅ Supabase connection successful")
            
            # 运行同步测试
            print("\n🔄 Running sync test...")
            results = manager.sync_all_tables('incremental')
            
            print(f"Sync results:")
            print(f"  Successful tables: {results['successful_tables']}")
            print(f"  Failed tables: {results['failed_tables']}")
            print(f"  Total records synced: {results['total_records_synced']}")
            print(f"  Duration: {results['duration_seconds']:.2f}s")
            
            # 显示统计信息
            stats = manager.get_sync_stats()
            print(f"\nSync statistics: {stats}")
            
        else:
            print("❌ Supabase connection failed")
            
    except Exception as e:
        print(f"❌ Sync manager test failed: {e}")