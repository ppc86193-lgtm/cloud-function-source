#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28云端同步管理器
实现本地数据与BigQuery云端的双向同步
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import threading
from dataclasses import dataclass, asdict
from google.cloud import bigquery
import pandas as pd

from local_database import get_local_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SyncTask:
    """同步任务数据类"""
    id: str
    table_name: str
    sync_type: str  # 'upload', 'download', 'bidirectional'
    direction: str  # 'local_to_cloud', 'cloud_to_local', 'both'
    status: str  # 'pending', 'running', 'completed', 'failed'
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    records_synced: int = 0
    error_message: Optional[str] = None

class CloudSyncManager:
    """云端同步管理器"""
    
    def __init__(self, project_id: str = "wprojectl", dataset_id: str = "pc28_lab"):
        """初始化同步管理器"""
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.db = get_local_db()
        self.bq_client = None
        self.sync_tasks = []
        self.is_syncing = False
        self.sync_thread = None
        
        # 初始化BigQuery客户端
        self._init_bigquery_client()
        
        # 同步配置
        self.sync_config = self._get_sync_config()
    
    def _init_bigquery_client(self):
        """初始化BigQuery客户端"""
        try:
            self.bq_client = bigquery.Client(project=self.project_id)
            logger.info("BigQuery客户端初始化成功")
        except Exception as e:
            logger.error(f"BigQuery客户端初始化失败: {e}")
            self.bq_client = None
    
    def _get_sync_config(self) -> Dict[str, Any]:
        """获取同步配置"""
        return {
            'tables': {
                # 原始数据表 - 双向同步
                'cloud_pred_today_norm': {
                    'sync_type': 'bidirectional',
                    'local_table': 'cloud_pred_today_norm',
                    'cloud_table': f'{self.project_id}.{self.dataset_id}.cloud_pred_today_norm',
                    'key_fields': ['draw_id', 'market', 'pick'],
                    'sync_interval_minutes': 30
                },
                
                # 信号池 - 本地到云端
                'signal_pool_union_v3': {
                    'sync_type': 'upload',
                    'local_table': 'signal_pool_union_v3',
                    'cloud_table': f'{self.project_id}.{self.dataset_id}.signal_pool_union_v3',
                    'key_fields': ['draw_id', 'market', 'pick', 'source'],
                    'sync_interval_minutes': 15
                },
                
                # 决策候选 - 本地到云端
                'lab_push_candidates_v2': {
                    'sync_type': 'upload',
                    'local_table': 'lab_push_candidates_v2',
                    'cloud_table': f'{self.project_id}.{self.dataset_id}.lab_push_candidates_v2',
                    'key_fields': ['id'],
                    'sync_interval_minutes': 10
                },
                
                # 运行时参数 - 双向同步
                'runtime_params': {
                    'sync_type': 'bidirectional',
                    'local_table': 'runtime_params',
                    'cloud_table': f'{self.project_id}.{self.dataset_id}.runtime_params',
                    'key_fields': ['market'],
                    'sync_interval_minutes': 60
                }
            },
            'batch_size': 1000,
            'max_retries': 3,
            'retry_delay_seconds': 30
        }
    
    def sync_table_to_cloud(self, table_name: str, incremental: bool = True) -> bool:
        """同步表到云端"""
        try:
            if not self.bq_client:
                logger.error("BigQuery客户端未初始化")
                return False
            
            config = self.sync_config['tables'].get(table_name)
            if not config:
                logger.error(f"未找到表同步配置: {table_name}")
                return False
            
            logger.info(f"开始同步表到云端: {table_name}")
            
            # 获取本地数据
            local_data = self._get_local_data(table_name, incremental)
            if not local_data:
                logger.info(f"表 {table_name} 无需同步的数据")
                return True
            
            # 转换为DataFrame
            df = pd.DataFrame(local_data)
            
            # 上传到BigQuery
            cloud_table = config['cloud_table']
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND if incremental else bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
            )
            
            job = self.bq_client.load_table_from_dataframe(df, cloud_table, job_config=job_config)
            job.result()  # 等待完成
            
            # 更新同步状态
            self._update_sync_status(table_name, 'completed', len(local_data))
            
            logger.info(f"表同步到云端成功: {table_name}, {len(local_data)} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"表同步到云端失败 {table_name}: {e}")
            self._update_sync_status(table_name, 'failed', 0, str(e))
            return False
    
    def sync_table_from_cloud(self, table_name: str, incremental: bool = True) -> bool:
        """从云端同步表"""
        try:
            if not self.bq_client:
                logger.error("BigQuery客户端未初始化")
                return False
            
            config = self.sync_config['tables'].get(table_name)
            if not config:
                logger.error(f"未找到表同步配置: {table_name}")
                return False
            
            logger.info(f"开始从云端同步表: {table_name}")
            
            # 构建查询
            cloud_table = config['cloud_table']
            
            if incremental:
                # 获取本地最新时间戳
                last_sync = self._get_last_sync_time(table_name)
                if last_sync:
                    query = f"""
                        SELECT * FROM `{cloud_table}`
                        WHERE created_at > TIMESTAMP('{last_sync}')
                        ORDER BY created_at
                    """
                else:
                    # 如果没有同步记录，获取最近24小时的数据
                    query = f"""
                        SELECT * FROM `{cloud_table}`
                        WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                        ORDER BY created_at
                    """
            else:
                query = f"SELECT * FROM `{cloud_table}` ORDER BY created_at"
            
            # 执行查询
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            # 转换为字典列表，处理数据类型转换
            cloud_data = []
            for row in results:
                row_dict = dict(row)
                # 处理特殊数据类型转换
                for key, value in row_dict.items():
                    if value is not None:
                        # 将datetime对象转换为字符串
                        if hasattr(value, 'strftime'):
                            row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                        # 将date对象转换为字符串
                        elif hasattr(value, 'isoformat') and hasattr(value, 'year'):
                            row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S') if hasattr(value, 'hour') else value.strftime('%Y-%m-%d') + ' 00:00:00'
                        # 将int转换为float（为了兼容SQLite的REAL类型）
                        elif isinstance(value, int) and key in ['period']:
                            row_dict[key] = float(value)
                        # 将Decimal转换为float
                        elif hasattr(value, '__float__'):
                            try:
                                row_dict[key] = float(value)
                            except (ValueError, TypeError):
                                row_dict[key] = str(value)
                cloud_data.append(row_dict)
            
            if not cloud_data:
                logger.info(f"云端表 {table_name} 无新数据")
                return True
            
            # 插入本地数据库
            inserted = self.db.bulk_insert(config['local_table'], cloud_data, replace=True)
            
            # 更新同步状态
            self._update_sync_status(table_name, 'completed', inserted)
            
            logger.info(f"从云端同步表成功: {table_name}, {inserted} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"从云端同步表失败 {table_name}: {e}")
            self._update_sync_status(table_name, 'failed', 0, str(e))
            return False
    
    def bidirectional_sync(self, table_name: str) -> bool:
        """双向同步表"""
        try:
            logger.info(f"开始双向同步表: {table_name}")
            
            # 先从云端同步到本地
            download_success = self.sync_table_from_cloud(table_name, incremental=True)
            
            # 再从本地同步到云端
            upload_success = self.sync_table_to_cloud(table_name, incremental=True)
            
            success = download_success and upload_success
            logger.info(f"双向同步表 {table_name}: {'成功' if success else '失败'}")
            return success
            
        except Exception as e:
            logger.error(f"双向同步表失败 {table_name}: {e}")
            return False
    
    def _get_local_data(self, table_name: str, incremental: bool = True) -> List[Dict]:
        """获取本地数据"""
        try:
            if incremental:
                # 获取最后同步时间
                last_sync = self._get_last_sync_time(table_name)
                if last_sync:
                    where_clause = f"created_at > '{last_sync}'"
                else:
                    # 如果没有同步记录，获取今天的数据
                    where_clause = f"data_date = '{datetime.now().strftime('%Y-%m-%d')}'"
                
                query = f"SELECT * FROM {table_name} WHERE {where_clause} ORDER BY created_at"
            else:
                query = f"SELECT * FROM {table_name} ORDER BY created_at"
            
            return self.db.execute_query(query)
            
        except Exception as e:
            logger.error(f"获取本地数据失败 {table_name}: {e}")
            return []
    
    def _get_last_sync_time(self, table_name: str) -> Optional[str]:
        """获取最后同步时间"""
        try:
            result = self.db.execute_query("""
                SELECT last_sync FROM sync_status 
                WHERE table_name = ? AND sync_status = 'completed'
                ORDER BY last_sync DESC LIMIT 1
            """, (table_name,))
            
            return result[0]['last_sync'] if result else None
            
        except Exception as e:
            logger.error(f"获取最后同步时间失败: {e}")
            return None
    
    def _update_sync_status(self, table_name: str, status: str, records_synced: int = 0, error_message: str = ""):
        """更新同步状态"""
        try:
            self.db.execute_update("""
                INSERT OR REPLACE INTO sync_status 
                (table_name, last_sync, sync_status, records_synced, error_message)
                VALUES (?, ?, ?, ?, ?)
            """, (table_name, datetime.now().isoformat(), status, records_synced, error_message))
            
        except Exception as e:
            logger.error(f"更新同步状态失败: {e}")
    
    def run_scheduled_sync(self) -> Dict[str, Any]:
        """运行定时同步"""
        try:
            logger.info("开始运行定时同步...")
            
            sync_results = {
                'timestamp': datetime.now().isoformat(),
                'tables': {},
                'total_synced': 0,
                'success_count': 0,
                'failed_count': 0
            }
            
            for table_name, config in self.sync_config['tables'].items():
                try:
                    logger.info(f"同步表: {table_name}")
                    
                    if config['sync_type'] == 'upload':
                        success = self.sync_table_to_cloud(table_name)
                    elif config['sync_type'] == 'download':
                        success = self.sync_table_from_cloud(table_name)
                    elif config['sync_type'] == 'bidirectional':
                        success = self.bidirectional_sync(table_name)
                    else:
                        logger.warning(f"未知同步类型: {config['sync_type']}")
                        success = False
                    
                    sync_results['tables'][table_name] = {
                        'success': success,
                        'sync_type': config['sync_type']
                    }
                    
                    if success:
                        sync_results['success_count'] += 1
                    else:
                        sync_results['failed_count'] += 1
                        
                except Exception as e:
                    logger.error(f"同步表异常 {table_name}: {e}")
                    sync_results['tables'][table_name] = {
                        'success': False,
                        'error': str(e)
                    }
                    sync_results['failed_count'] += 1
            
            logger.info(f"定时同步完成: {sync_results['success_count']} 成功, {sync_results['failed_count']} 失败")
            return sync_results
            
        except Exception as e:
            logger.error(f"定时同步失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'success': False,
                'error': str(e)
            }
    
    def start_auto_sync(self, interval_minutes: int = 15):
        """启动自动同步"""
        if self.is_syncing:
            logger.warning("自动同步已在运行中")
            return
        
        self.is_syncing = True
        
        def sync_loop():
            logger.info(f"自动同步启动，间隔: {interval_minutes} 分钟")
            
            while self.is_syncing:
                try:
                    # 运行定时同步
                    results = self.run_scheduled_sync()
                    
                    # 记录同步结果
                    logger.info(f"自动同步完成: {results.get('success_count', 0)} 成功")
                    
                    # 等待下次同步
                    time.sleep(interval_minutes * 60)
                    
                except Exception as e:
                    logger.error(f"自动同步循环异常: {e}")
                    time.sleep(60)  # 异常时等待1分钟后重试
            
            logger.info("自动同步停止")
        
        self.sync_thread = threading.Thread(target=sync_loop, daemon=True)
        self.sync_thread.start()
        
        logger.info("自动同步已启动")
    
    def stop_auto_sync(self):
        """停止自动同步"""
        self.is_syncing = False
        
        if self.sync_thread and self.sync_thread.is_alive():
            self.sync_thread.join(timeout=10)
        
        logger.info("自动同步已停止")
    
    def get_sync_status_report(self) -> Dict[str, Any]:
        """获取同步状态报告"""
        try:
            # 获取同步状态
            sync_status = self.db.execute_query("""
                SELECT * FROM sync_status 
                ORDER BY last_sync DESC
            """)
            
            # 统计信息
            total_tables = len(self.sync_config['tables'])
            synced_tables = len([s for s in sync_status if s['sync_status'] == 'completed'])
            failed_tables = len([s for s in sync_status if s['sync_status'] == 'failed'])
            
            return {
                'timestamp': datetime.now().isoformat(),
                'auto_sync_active': self.is_syncing,
                'bigquery_connected': self.bq_client is not None,
                'summary': {
                    'total_tables': total_tables,
                    'synced_tables': synced_tables,
                    'failed_tables': failed_tables,
                    'sync_health': 'healthy' if failed_tables == 0 else 'degraded'
                },
                'table_status': sync_status,
                'sync_config': self.sync_config
            }
            
        except Exception as e:
            logger.error(f"获取同步状态报告失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def force_full_sync(self) -> Dict[str, Any]:
        """强制全量同步"""
        try:
            logger.info("开始强制全量同步...")
            
            results = {
                'timestamp': datetime.now().isoformat(),
                'tables': {},
                'success_count': 0,
                'failed_count': 0
            }
            
            for table_name, config in self.sync_config['tables'].items():
                try:
                    logger.info(f"全量同步表: {table_name}")
                    
                    if config['sync_type'] == 'upload':
                        success = self.sync_table_to_cloud(table_name, incremental=False)
                    elif config['sync_type'] == 'download':
                        success = self.sync_table_from_cloud(table_name, incremental=False)
                    elif config['sync_type'] == 'bidirectional':
                        # 双向同步时，先下载再上传
                        download_success = self.sync_table_from_cloud(table_name, incremental=False)
                        upload_success = self.sync_table_to_cloud(table_name, incremental=False)
                        success = download_success and upload_success
                    else:
                        success = False
                    
                    results['tables'][table_name] = success
                    
                    if success:
                        results['success_count'] += 1
                    else:
                        results['failed_count'] += 1
                        
                except Exception as e:
                    logger.error(f"全量同步表异常 {table_name}: {e}")
                    results['tables'][table_name] = False
                    results['failed_count'] += 1
            
            logger.info(f"强制全量同步完成: {results['success_count']} 成功, {results['failed_count']} 失败")
            return results
            
        except Exception as e:
            logger.error(f"强制全量同步失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'success': False,
                'error': str(e)
            }
    
    def test_cloud_connectivity(self) -> bool:
        """测试云端连接"""
        try:
            if not self.bq_client:
                return False
            
            # 测试查询
            query = f"SELECT 1 as test_connection"
            query_job = self.bq_client.query(query)
            result = query_job.result()
            
            # 检查结果
            for row in result:
                if row.test_connection == 1:
                    logger.info("云端连接测试成功")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"云端连接测试失败: {e}")
            return False

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28云端同步管理器')
    parser.add_argument('--action', choices=['sync', 'upload', 'download', 'start', 'stop', 'status', 'test', 'full_sync'], 
                       default='status', help='执行动作')
    parser.add_argument('--table', help='指定表名')
    parser.add_argument('--interval', type=int, default=15, help='自动同步间隔（分钟）')
    
    args = parser.parse_args()
    
    sync_manager = CloudSyncManager()
    
    if args.action == 'test':
        connected = sync_manager.test_cloud_connectivity()
        print(f"云端连接测试: {'成功' if connected else '失败'}")
    
    elif args.action == 'upload':
        if args.table:
            success = sync_manager.sync_table_to_cloud(args.table)
            print(f"上传表 {args.table}: {'成功' if success else '失败'}")
        else:
            print("请指定表名 --table")
    
    elif args.action == 'download':
        if args.table:
            success = sync_manager.sync_table_from_cloud(args.table)
            print(f"下载表 {args.table}: {'成功' if success else '失败'}")
        else:
            print("请指定表名 --table")
    
    elif args.action == 'sync':
        results = sync_manager.run_scheduled_sync()
        print(f"定时同步结果: {json.dumps(results, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'full_sync':
        results = sync_manager.force_full_sync()
        print(f"全量同步结果: {json.dumps(results, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'start':
        sync_manager.start_auto_sync(args.interval)
        print(f"自动同步已启动，间隔: {args.interval} 分钟，按 Ctrl+C 停止...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            sync_manager.stop_auto_sync()
            print("自动同步已停止")
    
    elif args.action == 'stop':
        sync_manager.stop_auto_sync()
        print("自动同步已停止")
    
    elif args.action == 'status':
        report = sync_manager.get_sync_status_report()
        print(f"同步状态报告: {json.dumps(report, indent=2, ensure_ascii=False)}")

if __name__ == "__main__":
    main()