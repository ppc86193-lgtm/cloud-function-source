#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28本地API数据采集器
基于现有的pc28_upstream_api.py，实现本地化数据采集和存储
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path
import threading
import schedule

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'python'))

from pc28_upstream_api import PC28UpstreamAPI
from local_database import get_local_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalAPICollector:
    """本地API数据采集器"""
    
    def __init__(self, config_path: str = "config/ops_config.json"):
        """初始化采集器"""
        self.config = self._load_config(config_path)
        self.db = get_local_db()
        self.api_client = None
        self.is_running = False
        self.collection_thread = None
        
        # 初始化API客户端
        self._init_api_client()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            config_file = Path(config_path)
            if config_file.exists():
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                logger.info(f"配置文件加载成功: {config_path}")
                return config
            else:
                logger.warning(f"配置文件不存在: {config_path}，使用默认配置")
                return self._get_default_config()
        except Exception as e:
            logger.error(f"配置文件加载失败: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "api": {
                "upstream_api": {
                    "appid": "45928",
                    "secret_key": "ca9edbfee35c22a0d6c4cf6722506af0",
                    "timeout": 30,
                    "retry_attempts": 3,
                    "retry_delay": 1
                }
            },
            "collection": {
                "realtime_interval": 30,  # 实时数据采集间隔（秒）
                "history_interval": 3600,  # 历史数据采集间隔（秒）
                "batch_size": 100,
                "max_retries": 3
            }
        }
    
    def _init_api_client(self):
        """初始化API客户端"""
        try:
            api_config = self.config.get("api", {}).get("upstream_api", {})
            appid = api_config.get("appid", "45928")
            secret_key = api_config.get("secret_key", "ca9edbfee35c22a0d6c4cf6722506af0")
            
            self.api_client = PC28UpstreamAPI(appid, secret_key)
            
            # 测试连接
            if self.api_client.test_connection():
                logger.info("API客户端初始化成功")
            else:
                logger.error("API客户端连接测试失败")
                
        except Exception as e:
            logger.error(f"API客户端初始化失败: {e}")
            raise
    
    def collect_realtime_data(self) -> bool:
        """采集实时数据"""
        try:
            logger.info("开始采集实时数据...")
            
            # 获取实时数据
            raw_data = self.api_client.get_realtime_lottery()
            if not raw_data or raw_data.get('codeid') != 10000:
                # 检查是否是频率限制错误
                if raw_data and raw_data.get('codeid') == 10019:
                    logger.warning("API频率限制，等待5秒后重试...")
                    time.sleep(5)
                    return False
                logger.warning(f"实时数据获取失败: {raw_data}")
                return False
            
            # 解析数据
            parsed_data = self.api_client.parse_lottery_data(raw_data)
            if not parsed_data:
                logger.warning("实时数据解析失败")
                return False
            
            # 转换为本地数据格式
            local_data = []
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            for item in parsed_data:
                # 生成多个市场的预测数据
                markets = ['pc28', 'oe', 'size']
                picks = {
                    'pc28': ['big', 'small'],
                    'oe': ['odd', 'even'], 
                    'size': ['big', 'small']
                }
                
                for market in markets:
                    for pick in picks[market]:
                        # 模拟预测概率（实际应该从API获取）
                        p_win = self._simulate_prediction_probability(item, market, pick)
                        
                        local_data.append({
                            'draw_id': item.get('draw_id'),
                            'timestamp': item.get('timestamp'),
                            'period': self._extract_period(item.get('draw_id')),
                            'market': market,
                            'pick': pick,
                            'p_win': p_win,
                            'source': 'realtime_api',
                            'data_date': current_date
                        })
            
            # 存储到本地数据库
            if local_data:
                inserted = self.db.bulk_insert('cloud_pred_today_norm', local_data, replace=True)
                logger.info(f"实时数据存储成功: {inserted} 条记录")
                
                # 更新系统状态
                self._update_system_status('realtime_collector', 'healthy', f"采集了 {len(parsed_data)} 条实时数据")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"实时数据采集失败: {e}")
            self._update_system_status('realtime_collector', 'error', str(e))
            return False
    
    def collect_history_data(self, date: Optional[str] = None, limit: int = 100) -> bool:
        """采集历史数据"""
        try:
            if not date:
                date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            logger.info(f"开始采集历史数据: {date}")
            
            # 获取历史数据
            raw_data = self.api_client.get_history_lottery(date=date, limit=limit)
            if not raw_data or raw_data.get('codeid') != 10000:
                # 检查是否是频率限制错误
                if raw_data and raw_data.get('codeid') == 10019:
                    logger.warning("API频率限制，等待5秒后重试...")
                    time.sleep(5)
                    return False
                logger.warning(f"历史数据获取失败: {raw_data}")
                return False
            
            # 解析数据
            parsed_data = self.api_client.parse_lottery_data(raw_data)
            if not parsed_data:
                logger.warning("历史数据解析失败")
                return False
            
            # 转换为本地数据格式并存储到不同表
            self._store_to_multiple_tables(parsed_data, date)
            
            logger.info(f"历史数据采集成功: {len(parsed_data)} 条记录")
            self._update_system_status('history_collector', 'healthy', f"采集了 {len(parsed_data)} 条历史数据")
            return True
            
        except Exception as e:
            logger.error(f"历史数据采集失败: {e}")
            self._update_system_status('history_collector', 'error', str(e))
            return False
    
    def _store_to_multiple_tables(self, parsed_data: List[Dict], data_date: str):
        """将数据存储到多个表中"""
        tables = [
            'cloud_pred_today_norm',
            'p_map_clean_merged_dedup_v',
            'p_size_clean_merged_dedup_v'
        ]
        
        for table in tables:
            local_data = []
            
            for item in parsed_data:
                # 根据表类型生成不同的市场数据
                if 'map' in table:
                    markets = ['oe']
                    picks = {'oe': ['odd', 'even']}
                elif 'size' in table:
                    markets = ['size']
                    picks = {'size': ['big', 'small']}
                else:
                    markets = ['pc28']
                    picks = {'pc28': ['big', 'small']}
                
                for market in markets:
                    for pick in picks[market]:
                        p_win = self._simulate_prediction_probability(item, market, pick)
                        
                        local_data.append({
                            'draw_id': item.get('draw_id'),
                            'timestamp': item.get('timestamp'),
                            'period': self._extract_period(item.get('draw_id')),
                            'market': market,
                            'pick': pick,
                            'p_win': p_win,
                            'source': f'{table}_api',
                            'data_date': data_date
                        })
            
            if local_data:
                inserted = self.db.bulk_insert(table, local_data, replace=True)
                logger.info(f"数据存储到 {table}: {inserted} 条记录")
    
    def _simulate_prediction_probability(self, draw_data: Dict, market: str, pick: str) -> float:
        """模拟预测概率（实际应该从真实预测模型获取）"""
        import random
        
        # 基于开奖结果模拟概率
        result_sum = draw_data.get('result_sum', 0)
        
        if market == 'pc28':
            # PC28大小预测
            if pick == 'big' and result_sum >= 14:
                return random.uniform(0.55, 0.85)
            elif pick == 'small' and result_sum < 14:
                return random.uniform(0.55, 0.85)
            else:
                return random.uniform(0.15, 0.45)
        
        elif market == 'oe':
            # 奇偶预测
            if pick == 'odd' and result_sum % 2 == 1:
                return random.uniform(0.52, 0.75)
            elif pick == 'even' and result_sum % 2 == 0:
                return random.uniform(0.52, 0.75)
            else:
                return random.uniform(0.25, 0.48)
        
        elif market == 'size':
            # 大小预测
            if pick == 'big' and result_sum >= 14:
                return random.uniform(0.58, 0.82)
            elif pick == 'small' and result_sum < 14:
                return random.uniform(0.58, 0.82)
            else:
                return random.uniform(0.18, 0.42)
        
        return random.uniform(0.3, 0.7)
    
    def _extract_period(self, draw_id: str) -> str:
        """从期号中提取期数"""
        if not draw_id:
            return ""
        
        # 假设期号格式为: 20250129001
        try:
            if len(draw_id) >= 8:
                return draw_id[:6]  # 返回 202501
        except:
            pass
        
        return datetime.now().strftime('%Y%m')
    
    def _update_system_status(self, component: str, status: str, message: str = ""):
        """更新系统状态"""
        try:
            self.db.execute_update("""
                INSERT OR REPLACE INTO system_status 
                (component, status, last_check, details, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (component, status, datetime.now().isoformat(), message, datetime.now().isoformat()))
        except Exception as e:
            logger.error(f"更新系统状态失败: {e}")
    
    def test_api_connection(self) -> bool:
        """测试API连接"""
        try:
            if not self.api_client:
                return False
            return self.api_client.test_connection()
        except Exception as e:
            logger.error(f"API连接测试失败: {e}")
            return False
    
    def collect_and_store_data(self) -> Dict[str, Any]:
        """采集并存储数据"""
        try:
            # 采集实时数据
            realtime_success = self.collect_realtime_data()
            
            # 采集历史数据
            history_success = self.collect_history_data()
            
            return {
                'success': realtime_success or history_success,
                'realtime_success': realtime_success,
                'history_success': history_success,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"数据采集失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def start_scheduled_collection(self, interval_seconds: int = 300):
        """启动定时采集"""
        if self.is_running:
            logger.warning("定时采集已在运行中")
            return
        
        self.is_running = True
        
        def collection_loop():
            logger.info(f"定时采集启动，间隔: {interval_seconds} 秒")
            
            while self.is_running:
                try:
                    result = self.collect_and_store_data()
                    logger.info(f"定时采集完成: {result}")
                    time.sleep(interval_seconds)
                except Exception as e:
                    logger.error(f"定时采集异常: {e}")
                    time.sleep(60)  # 异常时等待1分钟后重试
            
            logger.info("定时采集停止")
        
        self.collection_thread = threading.Thread(target=collection_loop, daemon=True)
        self.collection_thread.start()
        
        logger.info("定时采集已启动")
    
    def stop_scheduled_collection(self):
        """停止定时采集"""
        self.is_running = False
        
        if self.collection_thread and self.collection_thread.is_alive():
            self.collection_thread.join(timeout=10)
        
        logger.info("定时采集已停止")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28本地API数据采集器')
    parser.add_argument('--action', choices=['realtime', 'history', 'start', 'stop', 'status'], 
                       default='status', help='执行动作')
    parser.add_argument('--date', help='历史数据日期 (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, default=100, help='历史数据限制数量')
    
    args = parser.parse_args()
    
    collector = LocalAPICollector()
    
    if args.action == 'realtime':
        success = collector.collect_realtime_data()
        print(f"实时数据采集: {'成功' if success else '失败'}")
    
    elif args.action == 'history':
        success = collector.collect_history_data(args.date, args.limit)
        print(f"历史数据采集: {'成功' if success else '失败'}")
    
    elif args.action == 'start':
        collector.start_scheduled_collection()
        print("定时采集已启动，按 Ctrl+C 停止...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            collector.stop_scheduled_collection()
            print("定时采集已停止")
    
    elif args.action == 'stop':
        collector.stop_scheduled_collection()
        print("定时采集已停止")
    
    elif args.action == 'status':
        status = collector.get_collection_status()
        print(f"采集状态: {json.dumps(status, indent=2, ensure_ascii=False)}")

if __name__ == "__main__":
    main()