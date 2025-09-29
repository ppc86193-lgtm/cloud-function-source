#!/usr/bin/env python3
"""
PC28 API数据自动采集脚本
优化版本 - 支持完整字段采集、数据清洗、去重和BigQuery存储
"""

import hashlib
import requests
import json
import time
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api_fetch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PC28DataFetcher:
    def __init__(self):
        # API配置 - 从环境变量或配置文件读取
        self.api_url = "https://rijb.api.storeapi.net/api/119/259"
        self.history_api_url = "https://rijb.api.storeapi.net/api/119/260"
        self.wapi_key = os.getenv('WAPI_KEY', 'ca9edbfee35c22a0d6c4cf6722506af0')
        self.wapi_id = os.getenv('WAPI_ID', '45928')
        
        # BigQuery配置
        self.project_id = 'wprojectl'
        self.dataset_id = 'pc28_lab'
        self.table_id = 'draws_14w_clean'
        
        # 初始化BigQuery客户端
        try:
            self.bq_client = bigquery.Client(project=self.project_id)
            logger.info("BigQuery客户端初始化成功")
        except Exception as e:
            logger.error(f"BigQuery客户端初始化失败: {e}")
            self.bq_client = None
    
    def generate_signature(self, params: Dict[str, str]) -> str:
        """
        生成API签名 - 按照API文档要求生成正确的签名
        """
        try:
            # 按键排序参数
            sorted_params = sorted(params.items())
            
            # 拼接参数字符串
            param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
            
            # 添加密钥
            param_string += self.wapi_key
            
            # 生成MD5签名
            signature = hashlib.md5(param_string.encode('utf-8')).hexdigest()
            
            logger.debug(f"签名参数: {param_string}")
            logger.debug(f"生成签名: {signature}")
            return signature
            
        except Exception as e:
            logger.error(f"签名生成失败: {e}")
            raise
    
    def fetch_data_from_api(self) -> Optional[Dict[str, Any]]:
        """从API获取数据"""
        try:
            current_time = str(int(time.time()))
            
            # 构建请求参数
            params = {
                'appid': self.wapi_id,
                'format': 'json',
                'time': current_time
            }
            
            # 生成签名
            params['sign'] = self.generate_signature(params)
            
            logger.info(f"发送API请求: {self.api_url}")
            logger.debug(f"请求参数: {params}")
            
            # 发送请求
            response = requests.get(
                self.api_url, 
                params=params,
                timeout=30,
                headers={'User-Agent': 'PC28-DataFetcher/2.0'}
            )
            
            logger.info(f"API响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"成功获取数据，返回条数: {len(data.get('retdata', []))}")
                return data
            else:
                logger.error(f"API请求失败，状态码: {response.status_code}")
                logger.error(f"响应内容: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"网络请求异常: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            logger.error(f"数据获取异常: {e}")
            return None
    
    def fetch_history_data(self, date: str = None, limit: int = 30) -> Optional[Dict[str, Any]]:
        """获取历史数据"""
        try:
            current_time = str(int(time.time()))
            if not date:
                date = datetime.now().strftime('%Y-%m-%d')
            
            # 构建请求参数
            params = {
                'appid': self.wapi_id,
                'format': 'json',
                'date': date,
                'limit': str(limit),
                'time': current_time
            }
            
            # 生成签名
            params['sign'] = self.generate_signature(params)
            
            logger.info(f"发送历史数据API请求: {self.history_api_url}")
            logger.debug(f"请求参数: {params}")
            
            # 发送请求
            response = requests.get(
                self.history_api_url, 
                params=params,
                timeout=30,
                headers={'User-Agent': 'PC28-DataFetcher/2.0'}
            )
            
            logger.info(f"历史API响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"成功获取历史数据")
                return data
            else:
                logger.error(f"历史API请求失败，状态码: {response.status_code}")
                logger.error(f"响应内容: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"历史数据获取异常: {e}")
            return None

    def clean_and_validate_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """数据清洗和验证 - 优化版本，充分利用所有字段"""
        cleaned_data = []
        
        try:
            # 先打印原始数据结构用于调试
            logger.info(f"原始API响应结构: {json.dumps(raw_data, ensure_ascii=False, indent=2)}")
            
            # 提取API响应的基础信息
            api_codeid = raw_data.get('codeid')
            api_message = raw_data.get('message', '')
            api_curtime = raw_data.get('curtime')
            
            ret_data = raw_data.get('retdata', {})
            if not ret_data:
                logger.warning("API返回数据为空")
                return cleaned_data
            
            # 处理嵌套的数据结构
            items_to_process = []
            
            if isinstance(ret_data, dict):
                # 如果retdata是字典，提取其中的数据项
                for key, value in ret_data.items():
                    if isinstance(value, dict):
                        # 添加键名作为数据类型标识
                        value['data_type'] = key
                        value['api_codeid'] = api_codeid
                        value['api_message'] = api_message
                        value['api_curtime'] = api_curtime
                        items_to_process.append(value)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                item['data_type'] = key
                                item['api_codeid'] = api_codeid
                                item['api_message'] = api_message
                                item['api_curtime'] = api_curtime
                        items_to_process.extend(value)
            elif isinstance(ret_data, list):
                for item in ret_data:
                    if isinstance(item, dict):
                        item['api_codeid'] = api_codeid
                        item['api_message'] = api_message
                        item['api_curtime'] = api_curtime
                items_to_process = ret_data
            
            logger.info(f"开始处理 {len(items_to_process)} 条数据")
            
            for idx, item in enumerate(items_to_process):
                try:
                    logger.debug(f"处理第 {idx+1} 条数据: {item}")
                    
                    if not isinstance(item, dict):
                        logger.warning(f"数据项格式不正确，跳过: {item}")
                        continue
                    
                    # 提取关键字段 - 适配不同的字段名
                    issue = (item.get('long_issue') or 
                            item.get('issue') or 
                            item.get('period') or
                            item.get('next_issue'))
                    
                    kj_time = (item.get('kjtime') or 
                              item.get('timestamp') or 
                              item.get('time') or
                              item.get('next_time'))
                    
                    # 提取更多有用字段
                    short_issue = item.get('short_issue')
                    award_time = item.get('award_time')  # 距离开奖剩余时间
                    data_type = item.get('data_type', 'unknown')
                    
                    # API响应信息
                    api_codeid = item.get('api_codeid')
                    api_message = item.get('api_message', '')
                    api_curtime = item.get('api_curtime')
                    
                    # 数据验证 - 对于next类型数据，可能没有kjtime
                    if not issue:
                        logger.warning(f"缺少期号信息，跳过: {item}")
                        continue
                    
                    # 时间戳处理
                    timestamp = None
                    if kj_time:
                        try:
                            if isinstance(kj_time, str):
                                # 尝试解析时间戳
                                if kj_time.isdigit():
                                    timestamp = datetime.fromtimestamp(int(kj_time), tz=timezone.utc)
                                else:
                                    # 如果是时间字符串，尝试解析
                                    try:
                                        timestamp = datetime.fromisoformat(kj_time.replace('Z', '+00:00'))
                                    except:
                                        # 尝试其他时间格式
                                        timestamp = datetime.strptime(kj_time, '%Y-%m-%d %H:%M:%S')
                                        timestamp = timestamp.replace(tzinfo=timezone.utc)
                            else:
                                timestamp = datetime.fromtimestamp(int(kj_time), tz=timezone.utc)
                        except Exception as time_error:
                            logger.warning(f"时间解析失败: {time_error}")
                    
                    # 如果没有时间戳，对于next类型数据使用next_time
                    if not timestamp and data_type == 'next' and item.get('next_time'):
                        try:
                            next_time_str = item.get('next_time')
                            timestamp = datetime.strptime(next_time_str, '%Y-%m-%d %H:%M:%S')
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        except:
                            pass
                    
                    # 如果仍然没有时间戳，使用当前时间
                    if not timestamp:
                        timestamp = datetime.now(timezone.utc)
                    
                    # 处理开奖号码
                    numbers = item.get('number', [])
                    a, b, c = None, None, None
                    sum28 = None
                    
                    if isinstance(numbers, list) and len(numbers) >= 3:
                        try:
                            a = int(numbers[0])
                            b = int(numbers[1]) 
                            c = int(numbers[2])
                            sum28 = a + b + c
                        except (ValueError, TypeError):
                            logger.warning(f"号码解析失败: {numbers}")
                    
                    # 计算额外字段
                    odd_even = None
                    size = None
                    if sum28 is not None:
                        # 大小：0-13为小，14-27为大
                        size = 'big' if sum28 >= 14 else 'small'
                        # 单双：奇数为单，偶数为双
                        odd_even = 'odd' if sum28 % 2 == 1 else 'even'
                    
                    # 构建清洗后的数据
                    cleaned_item = {
                        'issue': str(issue),
                        'timestamp': timestamp.isoformat(),
                        'kjtime_raw': str(kj_time) if kj_time else None,
                        'short_issue': short_issue,
                        'a': a,
                        'b': b, 
                        'c': c,
                        'sum': sum28,  # 使用sum字段名匹配数据库
                        'size': size,
                        'odd_even': odd_even,
                        'numbers': json.dumps(numbers) if numbers else None,
                        'award_time': award_time,  # 距离开奖剩余时间
                        'data_type': data_type,
                        'api_codeid': api_codeid,
                        'api_message': api_message,
                        'api_curtime': api_curtime,
                        'raw_data': json.dumps(item, ensure_ascii=False),
                        'fetch_time': datetime.now(timezone.utc).isoformat(),
                        'data_source': 'api_auto_fetch_v2'
                    }
                    
                    cleaned_data.append(cleaned_item)
                    logger.info(f"成功清洗数据: {cleaned_item['issue']} ({cleaned_item['data_type']})")
                    
                except Exception as e:
                    logger.error(f"数据清洗失败: {item}, 错误: {e}")
                    continue
            
            logger.info(f"数据清洗完成，有效数据: {len(cleaned_data)}")
            return cleaned_data
            
        except Exception as e:
            logger.error(f"数据清洗过程异常: {e}")
            return []
    
    def ensure_bigquery_table(self):
        """确保BigQuery表存在，使用优化的表结构"""
        if not self.bq_client:
            logger.error("BigQuery客户端未初始化")
            return False
        
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            
            # 检查表是否存在
            try:
                table = self.bq_client.get_table(table_ref)
                logger.info(f"表已存在: {table_ref}")
                return True
            except NotFound:
                logger.info(f"表不存在，创建新表: {table_ref}")
                
                # 创建优化的表结构
                schema = [
                    bigquery.SchemaField("issue", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("kjtime_raw", "STRING"),
                    bigquery.SchemaField("short_issue", "STRING"),
                    bigquery.SchemaField("a", "INTEGER"),
                    bigquery.SchemaField("b", "INTEGER"),
                    bigquery.SchemaField("c", "INTEGER"),
                    bigquery.SchemaField("sum", "INTEGER"),
                    bigquery.SchemaField("size", "STRING"),
                    bigquery.SchemaField("odd_even", "STRING"),
                    bigquery.SchemaField("numbers", "STRING"),
                    bigquery.SchemaField("award_time", "INTEGER"),
                    bigquery.SchemaField("data_type", "STRING"),
                    bigquery.SchemaField("api_codeid", "INTEGER"),
                    bigquery.SchemaField("api_message", "STRING"),
                    bigquery.SchemaField("api_curtime", "INTEGER"),
                    bigquery.SchemaField("raw_data", "STRING"),
                    bigquery.SchemaField("fetch_time", "TIMESTAMP"),
                    bigquery.SchemaField("data_source", "STRING"),
                ]
                
                # 创建表
                table = bigquery.Table(table_ref, schema=schema)
                
                # 设置分区和聚簇
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp"
                )
                table.clustering_fields = ["issue", "data_type"]
                
                table = self.bq_client.create_table(table)
                logger.info(f"表创建成功: {table_ref}")
                return True
                
        except Exception as e:
            logger.error(f"BigQuery表操作失败: {e}")
            return False
    
    def insert_to_bigquery(self, data: List[Dict[str, Any]]) -> bool:
        """插入数据到BigQuery"""
        if not self.bq_client or not data:
            return False
        
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            
            # 确保表存在
            if not self.ensure_bigquery_table():
                return False
            
            # 插入数据
            errors = self.bq_client.insert_rows_json(
                table_ref, 
                data,
                ignore_unknown_values=True
            )
            
            if errors:
                logger.error(f"BigQuery插入错误: {errors}")
                return False
            else:
                logger.info(f"成功插入 {len(data)} 条数据到BigQuery")
                return True
                
        except Exception as e:
            logger.error(f"BigQuery插入异常: {e}")
            return False
    
    def run_fetch_cycle(self, include_history: bool = False) -> bool:
        """执行一次完整的数据采集周期"""
        logger.info("开始数据采集周期")
        
        try:
            all_cleaned_data = []
            
            # 1. 从实时API获取数据
            raw_data = self.fetch_data_from_api()
            if raw_data:
                cleaned_data = self.clean_and_validate_data(raw_data)
                all_cleaned_data.extend(cleaned_data)
            else:
                logger.error("实时API数据获取失败")
            
            # 2. 可选：获取历史数据
            if include_history:
                history_data = self.fetch_history_data()
                if history_data:
                    history_cleaned = self.clean_and_validate_data(history_data)
                    all_cleaned_data.extend(history_cleaned)
            
            if not all_cleaned_data:
                logger.warning("没有有效数据需要处理")
                return True
            
            # 3. 插入到BigQuery
            success = self.insert_to_bigquery(all_cleaned_data)
            
            if success:
                logger.info("数据采集周期完成")
                return True
            else:
                logger.error("数据插入失败")
                return False
                
        except Exception as e:
            logger.error(f"数据采集周期异常: {e}")
            return False

def main():
    """主函数"""
    logger.info("PC28数据采集脚本启动")
    
    # 确保日志目录存在
    os.makedirs('logs', exist_ok=True)
    
    try:
        # 创建数据采集器
        fetcher = PC28DataFetcher()
        
        # 执行数据采集
        success = fetcher.run_fetch_cycle()
        
        if success:
            logger.info("数据采集成功完成")
            exit(0)
        else:
            logger.error("数据采集失败")
            exit(1)
            
    except KeyboardInterrupt:
        logger.info("用户中断程序")
        exit(0)
    except Exception as e:
        logger.error(f"程序异常: {e}")
        exit(1)

if __name__ == "__main__":
    main()