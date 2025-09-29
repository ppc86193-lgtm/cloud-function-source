#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 历史数据API接口模块
用于从Google Cloud或其他数据源获取历史开奖数据
版本: 1.0.0
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from google.cloud import bigquery
from google.cloud import storage
from google.auth import default

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HistoricalDataAPI:
    """历史数据API接口类"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        """初始化API接口
        
        Args:
            project_id: Google Cloud项目ID
            dataset_id: BigQuery数据集ID
        """
        self.project_id = project_id or os.getenv('PROJECT', 'wprojectl')
        self.dataset_id = dataset_id or os.getenv('DS_DRAW', 'pc２８')
        
        # 初始化Google Cloud客户端
        try:
            self.credentials, _ = default()
            self.bq_client = bigquery.Client(
                project=self.project_id,
                credentials=self.credentials
            )
            self.storage_client = storage.Client(
                project=self.project_id,
                credentials=self.credentials
            )
            logger.info(f"已连接到Google Cloud项目: {self.project_id}")
        except Exception as e:
            logger.error(f"Google Cloud认证失败: {e}")
            raise
    
    def get_data_gap(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """检查数据缺口
        
        Returns:
            Tuple[最新数据时间, 当前时间]
        """
        try:
            query = f"""
            SELECT 
                MAX(timestamp) as latest_timestamp,
                CURRENT_TIMESTAMP() as current_timestamp
            FROM `{self.project_id}.{self.dataset_id}.draws_14w_dedup_v`
            """
            
            result = self.bq_client.query(query).result()
            row = next(iter(result))
            
            latest_time = row.latest_timestamp
            current_time = row.current_timestamp
            
            logger.info(f"最新数据时间: {latest_time}")
            logger.info(f"当前时间: {current_time}")
            
            return latest_time, current_time
            
        except Exception as e:
            logger.error(f"检查数据缺口失败: {e}")
            return None, None
    
    def fetch_from_bigquery_history(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """从BigQuery历史表获取数据
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            历史数据列表
        """
        try:
            # 查询历史数据表（如果存在）
            query = f"""
            SELECT 
                issue,
                timestamp,
                result,
                sum_value,
                big_small,
                odd_even
            FROM `{self.project_id}.{self.dataset_id}.draws_history`
            WHERE timestamp BETWEEN @start_time AND @end_time
            ORDER BY timestamp ASC
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
                    bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
                ]
            )
            
            result = self.bq_client.query(query, job_config=job_config).result()
            
            data = []
            for row in result:
                data.append({
                    'issue': row.issue,
                    'timestamp': row.timestamp.isoformat(),
                    'result': row.result,
                    'sum_value': row.sum_value,
                    'big_small': row.big_small,
                    'odd_even': row.odd_even
                })
            
            logger.info(f"从BigQuery历史表获取到 {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.warning(f"从BigQuery历史表获取数据失败: {e}")
            return []
    
    def fetch_from_external_api(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """从外部API获取历史数据
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            历史数据列表
        """
        try:
            # 这里可以配置实际的外部API
            api_url = os.getenv('PC28_HISTORY_API_URL', '')
            api_key = os.getenv('PC28_HISTORY_API_KEY', '')
            
            if not api_url:
                logger.warning("未配置外部历史数据API")
                return []
            
            # 构建API请求
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'PC28-DataBackfill/1.0'
            }
            
            if api_key:
                headers['Authorization'] = f'Bearer {api_key}'
            
            params = {
                'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                'format': 'json'
            }
            
            logger.info(f"请求外部API: {api_url}")
            response = requests.get(
                api_url,
                headers=headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"从外部API获取到 {len(data.get('results', []))} 条记录")
                return data.get('results', [])
            else:
                logger.error(f"外部API请求失败: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"从外部API获取数据失败: {e}")
            return []
    
    def fetch_from_gcs_backup(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """从GCS备份文件获取数据
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            历史数据列表
        """
        try:
            bucket_name = os.getenv('GCS_BACKUP_BUCKET', '')
            if not bucket_name:
                logger.warning("未配置GCS备份存储桶")
                return []
            
            bucket = self.storage_client.bucket(bucket_name)
            
            # 按日期查找备份文件
            data = []
            current_date = start_time.date()
            end_date = end_time.date()
            
            while current_date <= end_date:
                blob_name = f"pc28_backup/{current_date.strftime('%Y/%m/%d')}/draws.json"
                blob = bucket.blob(blob_name)
                
                if blob.exists():
                    logger.info(f"找到备份文件: {blob_name}")
                    content = blob.download_as_text()
                    daily_data = json.loads(content)
                    
                    # 过滤时间范围内的数据
                    for record in daily_data:
                        record_time = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                        if start_time <= record_time <= end_time:
                            data.append(record)
                
                current_date += timedelta(days=1)
            
            logger.info(f"从GCS备份获取到 {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.error(f"从GCS备份获取数据失败: {e}")
            return []
    
    def get_historical_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """获取历史数据（多源策略）
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            历史数据列表
        """
        logger.info(f"获取历史数据: {start_time} 到 {end_time}")
        
        # 尝试多个数据源
        data_sources = [
            ("BigQuery历史表", self.fetch_from_bigquery_history),
            ("外部API", self.fetch_from_external_api),
            ("GCS备份", self.fetch_from_gcs_backup)
        ]
        
        for source_name, fetch_func in data_sources:
            try:
                logger.info(f"尝试从 {source_name} 获取数据")
                data = fetch_func(start_time, end_time)
                
                if data:
                    logger.info(f"成功从 {source_name} 获取到 {len(data)} 条记录")
                    return data
                else:
                    logger.warning(f"{source_name} 未返回数据")
                    
            except Exception as e:
                logger.error(f"从 {source_name} 获取数据失败: {e}")
                continue
        
        logger.warning("所有数据源都未能获取到历史数据")
        return []
    
    def insert_historical_data(self, data: List[Dict]) -> bool:
        """插入历史数据到BigQuery
        
        Args:
            data: 历史数据列表
            
        Returns:
            是否成功
        """
        if not data:
            logger.warning("没有数据需要插入")
            return True
        
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.draws_14w"
            table = self.bq_client.get_table(table_id)
            
            # 转换数据格式
            rows_to_insert = []
            for record in data:
                row = {
                    'issue': record.get('issue'),
                    'timestamp': record.get('timestamp'),
                    'result': record.get('result'),
                    'sum_value': record.get('sum_value'),
                    'big_small': record.get('big_small'),
                    'odd_even': record.get('odd_even')
                }
                rows_to_insert.append(row)
            
            # 插入数据
            errors = self.bq_client.insert_rows_json(table, rows_to_insert)
            
            if errors:
                logger.error(f"插入数据时发生错误: {errors}")
                return False
            else:
                logger.info(f"成功插入 {len(rows_to_insert)} 条历史数据")
                return True
                
        except Exception as e:
            logger.error(f"插入历史数据失败: {e}")
            return False

def main():
    """主函数 - 用于测试"""
    try:
        api = HistoricalDataAPI()
        
        # 检查数据缺口
        latest_time, current_time = api.get_data_gap()
        
        if latest_time and current_time:
            time_diff = current_time - latest_time
            
            if time_diff.total_seconds() > 7200:  # 超过2小时
                logger.info(f"检测到数据缺口: {time_diff}")
                
                # 获取历史数据
                historical_data = api.get_historical_data(latest_time, current_time)
                
                if historical_data:
                    # 插入数据
                    success = api.insert_historical_data(historical_data)
                    if success:
                        logger.info("历史数据回填完成")
                    else:
                        logger.error("历史数据回填失败")
                else:
                    logger.warning("未获取到历史数据")
            else:
                logger.info("数据时间正常，无需回填")
        else:
            logger.error("无法检查数据缺口")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()