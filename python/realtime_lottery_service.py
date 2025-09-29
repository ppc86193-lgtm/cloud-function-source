#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28实时开奖数据获取服务
负责从上游API获取实时开奖数据并进行处理
"""

import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from pc28_upstream_api import PC28UpstreamAPI

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealtimeLotteryService:
    """
    实时开奖数据获取服务
    """
    
    def __init__(self, appid: str = "45928", secret_key: str = "ca9edbfee35c22a0d6c4cf6722506af0"):
        """
        初始化服务
        
        Args:
            appid: 应用ID
            secret_key: 密钥
        """
        self.api_client = PC28UpstreamAPI(appid, secret_key)
        self.last_draw_id = None
        self.data_cache = []
        
    def fetch_current_draw(self) -> Optional[Dict[str, Any]]:
        """
        获取当前开奖数据
        
        Returns:
            当前开奖数据或None
        """
        try:
            logger.info("获取当前开奖数据...")
            raw_data = self.api_client.get_realtime_lottery()
            
            if raw_data.get('codeid') != 10000:
                logger.error(f"API返回错误: {raw_data.get('message')}")
                return None
            
            # 解析数据
            parsed_data = self.api_client.parse_lottery_data(raw_data)
            
            if parsed_data:
                current_draw = parsed_data[0]
                logger.info(f"获取到开奖数据: 期号={current_draw.get('draw_id')}, 时间={current_draw.get('timestamp')}")
                return current_draw
            else:
                logger.warning("未获取到有效的开奖数据")
                return None
                
        except Exception as e:
            logger.error(f"获取当前开奖数据失败: {e}")
            return None
    
    def check_new_draw(self) -> Optional[Dict[str, Any]]:
        """
        检查是否有新的开奖数据
        
        Returns:
            新的开奖数据或None
        """
        current_draw = self.fetch_current_draw()
        
        if not current_draw:
            return None
        
        current_draw_id = current_draw.get('draw_id')
        
        # 检查是否为新数据
        if current_draw_id != self.last_draw_id:
            logger.info(f"发现新开奖数据: {current_draw_id}")
            self.last_draw_id = current_draw_id
            return current_draw
        else:
            logger.debug(f"开奖数据未更新: {current_draw_id}")
            return None
    
    def get_next_draw_info(self) -> Optional[Dict[str, Any]]:
        """
        获取下期开奖信息
        
        Returns:
            下期开奖信息或None
        """
        try:
            raw_data = self.api_client.get_realtime_lottery()
            
            if raw_data.get('codeid') != 10000:
                return None
            
            next_info = {
                'next_issue': raw_data.get('next_issue'),
                'next_time': raw_data.get('next_time'),
                'award_time': raw_data.get('award_time'),  # 距离开奖剩余时间(秒)
            }
            
            return next_info
            
        except Exception as e:
            logger.error(f"获取下期开奖信息失败: {e}")
            return None
    
    def validate_draw_data(self, draw_data: Dict[str, Any]) -> bool:
        """
        验证开奖数据的完整性和有效性
        
        Args:
            draw_data: 开奖数据
            
        Returns:
            数据是否有效
        """
        required_fields = ['draw_id', 'timestamp', 'numbers', 'result_sum']
        
        # 检查必需字段
        for field in required_fields:
            if field not in draw_data or draw_data[field] is None:
                logger.warning(f"开奖数据缺少必需字段: {field}")
                return False
        
        # 验证开奖号码
        numbers = draw_data.get('numbers', [])
        if not isinstance(numbers, list) or len(numbers) == 0:
            logger.warning("开奖号码格式无效")
            return False
        
        # 验证结果和
        result_sum = draw_data.get('result_sum')
        if not isinstance(result_sum, (int, float)) or result_sum < 0:
            logger.warning("结果和无效")
            return False
        
        # 验证期号格式
        draw_id = draw_data.get('draw_id')
        if not isinstance(draw_id, str) or len(draw_id) < 6:
            logger.warning("期号格式无效")
            return False
        
        logger.debug(f"开奖数据验证通过: {draw_id}")
        return True
    
    def format_for_bigquery(self, draw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化数据以适配BigQuery表结构
        
        Args:
            draw_data: 原始开奖数据
            
        Returns:
            格式化后的数据
        """
        try:
            # 解析时间戳
            timestamp_str = draw_data.get('timestamp')
            if timestamp_str:
                # 假设时间格式为 "2021-10-09 10:25:30"
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                # 转换为UTC时间戳
                timestamp_utc = dt.replace(tzinfo=timezone.utc).isoformat()
            else:
                timestamp_utc = datetime.now(timezone.utc).isoformat()
            
            formatted_data = {
                'draw_id': str(draw_data.get('draw_id')),
                'timestamp': timestamp_utc,
                'result_sum': int(draw_data.get('result_sum', 0)),
                'result_digits': json.dumps(draw_data.get('numbers', [])),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'source': 'upstream_api_realtime'
            }
            
            return formatted_data
            
        except Exception as e:
            logger.error(f"数据格式化失败: {e}")
            return {}
    
    def start_monitoring(self, interval: int = 60) -> None:
        """
        开始监控实时开奖数据
        
        Args:
            interval: 检查间隔(秒)
        """
        logger.info(f"开始监控实时开奖数据，检查间隔: {interval}秒")
        
        while True:
            try:
                # 检查新开奖数据
                new_draw = self.check_new_draw()
                
                if new_draw:
                    # 验证数据
                    if self.validate_draw_data(new_draw):
                        # 格式化数据
                        formatted_data = self.format_for_bigquery(new_draw)
                        
                        if formatted_data:
                            logger.info(f"新开奖数据已准备: {formatted_data['draw_id']}")
                            # 这里可以添加数据存储逻辑
                            self.data_cache.append(formatted_data)
                            
                            # 保持缓存大小
                            if len(self.data_cache) > 100:
                                self.data_cache = self.data_cache[-50:]
                        else:
                            logger.error("数据格式化失败")
                    else:
                        logger.error("数据验证失败")
                
                # 获取下期开奖信息
                next_info = self.get_next_draw_info()
                if next_info:
                    award_time = next_info.get('award_time')
                    if award_time and isinstance(award_time, int):
                        if award_time > 0:
                            logger.info(f"距离下期开奖还有 {award_time} 秒")
                        else:
                            logger.info("等待开奖中...")
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("监控已停止")
                break
            except Exception as e:
                logger.error(f"监控过程中发生错误: {e}")
                time.sleep(interval)
    
    def get_cached_data(self) -> List[Dict[str, Any]]:
        """
        获取缓存的开奖数据
        
        Returns:
            缓存的数据列表
        """
        return self.data_cache.copy()
    
    def clear_cache(self) -> None:
        """
        清空数据缓存
        """
        self.data_cache.clear()
        logger.info("数据缓存已清空")

# 使用示例
if __name__ == "__main__":
    # 创建实时服务
    service = RealtimeLotteryService()
    
    # 测试获取当前开奖数据
    current_draw = service.fetch_current_draw()
    if current_draw:
        print(f"当前开奖数据: {json.dumps(current_draw, indent=2, ensure_ascii=False)}")
        
        # 验证数据
        if service.validate_draw_data(current_draw):
            print("✅ 数据验证通过")
            
            # 格式化数据
            formatted = service.format_for_bigquery(current_draw)
            print(f"格式化数据: {json.dumps(formatted, indent=2, ensure_ascii=False)}")
        else:
            print("❌ 数据验证失败")
    
    # 获取下期开奖信息
    next_info = service.get_next_draw_info()
    if next_info:
        print(f"下期开奖信息: {json.dumps(next_info, indent=2, ensure_ascii=False)}")
    
    # 开始监控(取消注释以启动)
    # service.start_monitoring(interval=30)