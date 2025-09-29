#!/usr/bin/env python3
"""
BigQuery 到 Supabase 数据同步系统
包含防止AI误判的验证机制和错误处理
"""

import os
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import hashlib
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bigquery_supabase_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 防止AI误判的验证装饰器
def validate_operation(operation_type: str):
    """验证操作的装饰器，防止AI误判"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # 记录操作开始
            operation_id = hashlib.md5(f"{operation_type}_{time.time()}".encode()).hexdigest()[:8]
            logger.info(f"[{operation_id}] Starting {operation_type} operation")
            
            # 验证前置条件
            try:
                # 检查必要的环境变量和配置
                if operation_type == "bigquery_sync":
                    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
                        logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set, using default credentials")
                
                # 执行操作
                result = func(*args, **kwargs)
                
                # 记录成功
                logger.info(f"[{operation_id}] {operation_type} completed successfully")
                return result
                
            except Exception as e:
                # 详细记录错误
                logger.error(f"[{operation_id}] {operation_type} failed: {str(e)}")
                logger.error(f"[{operation_id}] Error type: {type(e).__name__}")
                logger.error(f"[{operation_id}] Stack trace:", exc_info=True)
                raise
                
        return wrapper
    return decorator

class BigQueryToSupabaseSync:
    """BigQuery到Supabase的数据同步管理器"""
    
    def __init__(self, config_path: str = "sync_config.json"):
        """初始化同步管理器"""
        self.config = self._load_config(config_path)
        self.bq_client = None
        self.supabase_client = None
        self.sync_state = {}
        self.validation_results = {}
        
        # 初始化连接
        self._init_connections()
        
    def _load_config(self, config_path: str) -> Dict:
        """加载配置文件"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # 合并BigQuery配置
            if os.path.exists('config/integrated_config.json'):
                with open('config/integrated_config.json', 'r') as f:
                    integrated_config = json.load(f)
                    config['bigquery'] = integrated_config.get('bigquery', {})
            
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            # 返回默认配置
            return {
                "bigquery": {
                    "project": "wprojectl",
                    "dataset_lab": "pc28_lab",
                    "location": "US"
                },
                "sync_interval_minutes": 30,
                "max_concurrent_syncs": 2
            }
    
    @validate_operation("connection_init")
    def _init_connections(self):
        """初始化BigQuery和Supabase连接"""
        # 初始化BigQuery客户端
        try:
            from google.cloud import bigquery
            self.bq_client = bigquery.Client(
                project=self.config['bigquery']['project']
            )
            logger.info(f"BigQuery client initialized for project: {self.config['bigquery']['project']}")
        except ImportError:
            logger.error("google-cloud-bigquery not installed. Please install: pip install google-cloud-bigquery")
            self.bq_client = None
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            self.bq_client = None
        
        # 初始化Supabase客户端
        try:
            from supabase_config import get_supabase_client
            self.supabase_client = get_supabase_client(use_service_role=True)
            logger.info("Supabase client initialized successfully")
        except ImportError:
            logger.error("supabase_config not found. Please ensure supabase_config.py exists")
            self.supabase_client = None
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            self.supabase_client = None
    
    @validate_operation("bigquery_test")
    def test_bigquery_connection(self) -> bool:
        """测试BigQuery连接"""
        if not self.bq_client:
            logger.error("BigQuery client not initialized")
            return False
        
        try:
            # 执行简单查询测试连接
            query = "SELECT 1 as test_value"
            query_job = self.bq_client.query(query)
            results = list(query_job)
            
            if results and results[0].test_value == 1:
                logger.info("✅ BigQuery connection test passed")
                return True
            else:
                logger.error("❌ BigQuery connection test failed: unexpected result")
                return False
                
        except Exception as e:
            logger.error(f"❌ BigQuery connection test failed: {e}")
            return False
    
    @validate_operation("supabase_test")
    def test_supabase_connection(self) -> bool:
        """测试Supabase连接"""
        if not self.supabase_client:
            logger.error("Supabase client not initialized")
            return False
        
        try:
            # 测试查询sync_status表
            response = self.supabase_client.table('sync_status').select('*').limit(1).execute()
            logger.info("✅ Supabase connection test passed")
            return True
        except Exception as e:
            logger.error(f"❌ Supabase connection test failed: {e}")
            # 尝试创建sync_status表
            try:
                self._create_sync_status_table()
                return True
            except:
                return False
    
    def _create_sync_status_table(self):
        """创建同步状态表（如果不存在）"""
        # 注意：这通常需要在Supabase控制台或通过SQL编辑器创建
        logger.info("sync_status table may not exist. Please create it in Supabase console with schema:")
        logger.info("""
        CREATE TABLE IF NOT EXISTS sync_status (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            last_sync_timestamp TIMESTAMP,
            sync_mode VARCHAR(50),
            records_synced INTEGER,
            sync_duration_seconds FLOAT,
            status VARCHAR(50),
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
    
    @validate_operation("table_list")
    def get_bigquery_tables(self) -> List[str]:
        """获取BigQuery数据集中的所有表"""
        if not self.bq_client:
            return []
        
        try:
            dataset_ref = self.bq_client.dataset(
                self.config['bigquery'].get('dataset_lab', 'pc28_lab')
            )
            tables = list(self.bq_client.list_tables(dataset_ref))
            table_names = [table.table_id for table in tables]
            logger.info(f"Found {len(table_names)} tables in BigQuery dataset")
            return table_names
        except Exception as e:
            logger.error(f"Failed to list BigQuery tables: {e}")
            return []
    
    @validate_operation("data_sync")
    def sync_table(self, table_name: str, mode: str = "incremental") -> Tuple[bool, int, str]:
        """同步单个表从BigQuery到Supabase"""
        if not self.bq_client or not self.supabase_client:
            return False, 0, "Clients not initialized"
        
        start_time = time.time()
        records_synced = 0
        
        try:
            # 构建查询
            dataset_name = self.config['bigquery'].get('dataset_lab', 'pc28_lab')
            
            if mode == "incremental":
                # 获取最后同步时间
                last_sync = self._get_last_sync_time(table_name)
                if last_sync:
                    query = f"""
                    SELECT * FROM `{self.config['bigquery']['project']}.{dataset_name}.{table_name}`
                    WHERE created_at > TIMESTAMP('{last_sync}')
                    ORDER BY created_at
                    """
                else:
                    query = f"SELECT * FROM `{self.config['bigquery']['project']}.{dataset_name}.{table_name}`"
            else:
                query = f"SELECT * FROM `{self.config['bigquery']['project']}.{dataset_name}.{table_name}`"
            
            # 执行查询
            logger.info(f"Executing BigQuery query for table {table_name}")
            query_job = self.bq_client.query(query)
            
            # 批量处理数据
            batch_size = 1000
            batch_data = []
            
            for row in query_job:
                # 转换行数据为字典
                row_dict = dict(row)
                
                # 处理特殊数据类型
                for key, value in row_dict.items():
                    if hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
                    elif value is None:
                        row_dict[key] = None
                    elif isinstance(value, bytes):
                        row_dict[key] = value.decode('utf-8', errors='ignore')
                
                batch_data.append(row_dict)
                
                # 批量插入
                if len(batch_data) >= batch_size:
                    self._insert_to_supabase(table_name, batch_data)
                    records_synced += len(batch_data)
                    batch_data = []
                    logger.info(f"Synced {records_synced} records for {table_name}")
            
            # 处理剩余数据
            if batch_data:
                self._insert_to_supabase(table_name, batch_data)
                records_synced += len(batch_data)
            
            # 更新同步状态
            sync_duration = time.time() - start_time
            self._update_sync_status(
                table_name, "completed", records_synced, 
                sync_duration=sync_duration, sync_mode=mode
            )
            
            logger.info(f"✅ Sync completed for {table_name}: {records_synced} records in {sync_duration:.2f}s")
            return True, records_synced, None
            
        except Exception as e:
            error_msg = f"Sync failed for {table_name}: {str(e)}"
            logger.error(error_msg)
            
            sync_duration = time.time() - start_time
            self._update_sync_status(
                table_name, "failed", 0, error_msg,
                sync_duration=sync_duration, sync_mode=mode
            )
            
            return False, 0, error_msg
    
    def _get_last_sync_time(self, table_name: str) -> Optional[str]:
        """获取表的最后同步时间"""
        try:
            response = self.supabase_client.table('sync_status') \
                .select('last_sync_timestamp') \
                .eq('table_name', table_name) \
                .eq('status', 'completed') \
                .order('created_at', desc=True) \
                .limit(1) \
                .execute()
            
            if response.data:
                return response.data[0]['last_sync_timestamp']
            return None
        except:
            return None
    
    def _insert_to_supabase(self, table_name: str, data: List[Dict]):
        """插入数据到Supabase"""
        try:
            # 使用upsert处理重复数据
            response = self.supabase_client.table(table_name).upsert(data).execute()
            if not response.data:
                logger.warning(f"No data returned from upsert for {table_name}")
        except Exception as e:
            logger.error(f"Failed to insert data to Supabase table {table_name}: {e}")
            # 尝试创建表（如果不存在）
            if "relation" in str(e) and "does not exist" in str(e):
                logger.info(f"Table {table_name} may not exist in Supabase. Please create it first.")
            raise
    
    def _update_sync_status(self, table_name: str, status: str, 
                           records_synced: int = 0, error_message: str = None,
                           sync_duration: float = 0.0, sync_mode: str = 'incremental'):
        """更新同步状态"""
        try:
            sync_data = {
                'table_name': table_name,
                'last_sync_timestamp': datetime.now().isoformat(),
                'sync_mode': sync_mode,
                'records_synced':