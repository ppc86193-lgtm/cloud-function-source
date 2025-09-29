#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28历史数据回填服务
负责从上游API获取历史开奖数据并进行批量回填
"""

import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pc28_upstream_api import PC28UpstreamAPI

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoryBackfillService:
    """
    历史数据回填服务
    """
    
    def __init__(self, appid: str = "45928", secret_key: str = "ca9edbfee35c22a0d6c4cf6722506af0"):
        """
        初始化服务
        
        Args:
            appid: 应用ID
            secret_key: 密钥
        """
        self.api_client = PC28UpstreamAPI(appid, secret_key)
        self.backfill_data = []
        self.failed_dates = []
        
    def fetch_history_by_date(self, date: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        按日期获取历史开奖数据
        
        Args:
            date: 日期字符串 (YYYY-MM-DD)
            limit: 获取数量限制
            
        Returns:
            历史开奖数据列表
        """
        try:
            logger.info(f"获取 {date} 的历史开奖数据，限制: {limit}")
            raw_data = self.api_client.get_history_lottery(date=date, limit=limit)
            
            if raw_data.get('codeid') != 10000:
                logger.error(f"API返回错误: {raw_data.get('message')}")
                return []
            
            # 解析数据
            parsed_data = self.api_client.parse_lottery_data(raw_data)
            
            if parsed_data:
                logger.info(f"成功获取 {date} 的 {len(parsed_data)} 条历史数据")
                return parsed_data
            else:
                logger.warning(f"未获取到 {date} 的有效历史数据")
                return []
                
        except Exception as e:
            logger.error(f"获取 {date} 历史数据失败: {e}")
            self.failed_dates.append(date)
            return []
    
    def fetch_recent_history(self, days: int = 7, limit_per_day: int = 100) -> List[Dict[str, Any]]:
        """
        获取最近几天的历史数据
        
        Args:
            days: 获取天数
            limit_per_day: 每天的数据限制
            
        Returns:
            历史开奖数据列表
        """
        all_data = []
        today = datetime.now()
        
        logger.info(f"开始获取最近 {days} 天的历史数据")
        
        for i in range(days):
            target_date = today - timedelta(days=i)
            date_str = target_date.strftime("%Y-%m-%d")
            
            # 获取该日期的数据
            daily_data = self.fetch_history_by_date(date_str, limit_per_day)
            all_data.extend(daily_data)
            
            # 避免请求过于频繁
            time.sleep(1)
        
        logger.info(f"总共获取到 {len(all_data)} 条历史数据")
        return all_data
    
    def fetch_date_range(self, start_date: str, end_date: str, limit_per_day: int = 100) -> List[Dict[str, Any]]:
        """
        获取指定日期范围的历史数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            limit_per_day: 每天的数据限制
            
        Returns:
            历史开奖数据列表
        """
        all_data = []
        
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            if start_dt > end_dt:
                logger.error("开始日期不能晚于结束日期")
                return []
            
            logger.info(f"开始获取 {start_date} 到 {end_date} 的历史数据")
            
            current_dt = start_dt
            while current_dt <= end_dt:
                date_str = current_dt.strftime("%Y-%m-%d")
                
                # 获取该日期的数据
                daily_data = self.fetch_history_by_date(date_str, limit_per_day)
                all_data.extend(daily_data)
                
                # 移动到下一天
                current_dt += timedelta(days=1)
                
                # 避免请求过于频繁
                time.sleep(1)
            
            logger.info(f"日期范围获取完成，总共 {len(all_data)} 条数据")
            return all_data
            
        except ValueError as e:
            logger.error(f"日期格式错误: {e}")
            return []
        except Exception as e:
            logger.error(f"获取日期范围数据失败: {e}")
            return []
    
    def validate_history_data(self, data_list: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        验证历史数据的完整性和有效性
        
        Args:
            data_list: 历史数据列表
            
        Returns:
            (有效数据列表, 无效数据列表)
        """
        valid_data = []
        invalid_data = []
        
        for data in data_list:
            if self._validate_single_record(data):
                valid_data.append(data)
            else:
                invalid_data.append(data)
        
        logger.info(f"数据验证完成: 有效 {len(valid_data)} 条, 无效 {len(invalid_data)} 条")
        return valid_data, invalid_data
    
    def _validate_single_record(self, data: Dict[str, Any]) -> bool:
        """
        验证单条历史记录
        
        Args:
            data: 单条数据记录
            
        Returns:
            数据是否有效
        """
        required_fields = ['draw_id', 'timestamp', 'numbers', 'result_sum']
        
        # 检查必需字段
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.debug(f"记录缺少必需字段: {field}")
                return False
        
        # 验证开奖号码
        numbers = data.get('numbers', [])
        if not isinstance(numbers, list) or len(numbers) == 0:
            logger.debug("开奖号码格式无效")
            return False
        
        # 验证结果和
        result_sum = data.get('result_sum')
        if not isinstance(result_sum, (int, float)) or result_sum < 0:
            logger.debug("结果和无效")
            return False
        
        # 验证期号格式
        draw_id = data.get('draw_id')
        if not isinstance(draw_id, str) or len(draw_id) < 6:
            logger.debug("期号格式无效")
            return False
        
        return True
    
    def deduplicate_data(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        去除重复的历史数据
        
        Args:
            data_list: 历史数据列表
            
        Returns:
            去重后的数据列表
        """
        seen_draw_ids = set()
        unique_data = []
        
        for data in data_list:
            draw_id = data.get('draw_id')
            if draw_id and draw_id not in seen_draw_ids:
                seen_draw_ids.add(draw_id)
                unique_data.append(data)
        
        logger.info(f"去重完成: 原始 {len(data_list)} 条, 去重后 {len(unique_data)} 条")
        return unique_data
    
    def format_for_bigquery_batch(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        批量格式化数据以适配BigQuery表结构
        
        Args:
            data_list: 原始数据列表
            
        Returns:
            格式化后的数据列表
        """
        formatted_data = []
        
        for data in data_list:
            try:
                # 解析时间戳
                timestamp_str = data.get('timestamp')
                if timestamp_str:
                    # 假设时间格式为 "2021-10-09 10:25:30"
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                    # 转换为UTC时间戳
                    timestamp_utc = dt.replace(tzinfo=timezone.utc).isoformat()
                else:
                    timestamp_utc = datetime.now(timezone.utc).isoformat()
                
                formatted_item = {
                    'draw_id': str(data.get('draw_id')),
                    'timestamp': timestamp_utc,
                    'result_sum': int(data.get('result_sum', 0)),
                    'result_digits': json.dumps(data.get('numbers', [])),
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'source': 'upstream_api_history'
                }
                
                formatted_data.append(formatted_item)
                
            except Exception as e:
                logger.error(f"格式化数据失败: {e}, 数据: {data}")
                continue
        
        logger.info(f"批量格式化完成: {len(formatted_data)} 条数据")
        return formatted_data
    
    def run_backfill(self, days: int = 14, limit_per_day: int = 100) -> Dict[str, Any]:
        """
        执行历史数据回填
        
        Args:
            days: 回填天数
            limit_per_day: 每天的数据限制
            
        Returns:
            回填结果统计
        """
        logger.info(f"开始执行历史数据回填，回填 {days} 天的数据")
        
        start_time = time.time()
        
        # 获取历史数据
        raw_data = self.fetch_recent_history(days, limit_per_day)
        
        if not raw_data:
            logger.warning("未获取到任何历史数据")
            return {
                'success': False,
                'total_fetched': 0,
                'valid_count': 0,
                'invalid_count': 0,
                'unique_count': 0,
                'failed_dates': self.failed_dates,
                'duration': time.time() - start_time
            }
        
        # 验证数据
        valid_data, invalid_data = self.validate_history_data(raw_data)
        
        # 去重
        unique_data = self.deduplicate_data(valid_data)
        
        # 格式化数据
        formatted_data = self.format_for_bigquery_batch(unique_data)
        
        # 保存到缓存
        self.backfill_data = formatted_data
        
        duration = time.time() - start_time
        
        result = {
            'success': True,
            'total_fetched': len(raw_data),
            'valid_count': len(valid_data),
            'invalid_count': len(invalid_data),
            'unique_count': len(unique_data),
            'formatted_count': len(formatted_data),
            'failed_dates': self.failed_dates,
            'duration': duration
        }
        
        logger.info(f"历史数据回填完成: {json.dumps(result, indent=2)}")
        return result
    
    def get_backfill_data(self) -> List[Dict[str, Any]]:
        """
        获取回填的数据
        
        Returns:
            回填数据列表
        """
        return self.backfill_data.copy()
    
    def clear_backfill_data(self) -> None:
        """
        清空回填数据缓存
        """
        self.backfill_data.clear()
        self.failed_dates.clear()
        logger.info("回填数据缓存已清空")
    
    def export_to_json(self, filename: str) -> bool:
        """
        导出回填数据到JSON文件
        
        Args:
            filename: 文件名
            
        Returns:
            导出是否成功
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.backfill_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"回填数据已导出到: {filename}")
            return True
            
        except Exception as e:
            logger.error(f"导出数据失败: {e}")
            return False

# 使用示例
if __name__ == "__main__":
    # 创建回填服务
    service = HistoryBackfillService()
    
    # 执行回填
    result = service.run_backfill(days=7, limit_per_day=50)
    
    if result['success']:
        print(f"✅ 回填成功: {result['formatted_count']} 条数据")
        
        # 获取回填数据
        backfill_data = service.get_backfill_data()
        if backfill_data:
            print(f"回填数据示例: {json.dumps(backfill_data[0], indent=2, ensure_ascii=False)}")
        
        # 导出数据
        service.export_to_json("backfill_data.json")
    else:
        print("❌ 回填失败")
    
    # 测试指定日期范围回填
    print("\n测试日期范围回填...")
    range_data = service.fetch_date_range("2024-01-01", "2024-01-03", limit_per_day=20)
    print(f"日期范围数据: {len(range_data)} 条")