#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 上游API客户端模块
实现加拿大28实时开奖和历史数据获取功能
包含MD5签名验证逻辑
"""

import hashlib
import time
import requests
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PC28UpstreamAPI:
    """
    PC28上游API客户端
    支持实时开奖数据获取和历史数据回填
    """
    
    def __init__(self, appid: str = "45928", secret_key: str = "ca9edbfee35c22a0d6c4cf6722506af0"):
        """
        初始化API客户端
        
        Args:
            appid: 应用ID
            secret_key: 32位密钥
        """
        self.appid = appid
        self.secret_key = secret_key
        self.base_url = "https://rijb.api.storeapi.net/api/119"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PC28-Client/1.0',
            'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
        })
        
    def _generate_md5_sign(self, params: Dict[str, Any]) -> str:
        """
        生成MD5签名
        
        Args:
            params: 请求参数字典
            
        Returns:
            MD5签名字符串
        """
        # 过滤空值参数
        filtered_params = {k: v for k, v in params.items() if v is not None and v != ""}
        
        # 按字典序排序
        sorted_keys = sorted(filtered_params.keys())
        
        # 拼接字符串: key1value1key2value2...
        sign_string = ""
        for key in sorted_keys:
            sign_string += f"{key}{filtered_params[key]}"
        
        # 添加密钥
        sign_string += self.secret_key
        
        # MD5加密
        md5_hash = hashlib.md5(sign_string.encode('utf-8')).hexdigest()
        
        logger.debug(f"签名字符串: {sign_string}")
        logger.debug(f"MD5签名: {md5_hash}")
        
        return md5_hash
    
    def _make_request(self, endpoint: str, params: Dict[str, Any], method: str = "GET") -> Dict[str, Any]:
        """
        发送HTTP请求
        
        Args:
            endpoint: API端点
            params: 请求参数
            method: 请求方法 (GET/POST)
            
        Returns:
            响应数据字典
            
        Raises:
            requests.RequestException: 请求异常
            ValueError: 响应解析异常
        """
        # 添加时间戳
        if 'time' not in params:
            params['time'] = str(int(time.time()))
        
        # 生成签名
        params['sign'] = self._generate_md5_sign(params)
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params, timeout=30)
            else:
                response = self.session.post(url, data=params, timeout=30)
            
            response.raise_for_status()
            
            # 解析JSON响应
            data = response.json()
            
            # 检查API状态码
            if 'codeid' in data and data['codeid'] != 10000:
                logger.warning(f"API返回错误: {data.get('message', '未知错误')} (代码: {data.get('codeid')})")
            
            return data
            
        except requests.RequestException as e:
            logger.error(f"请求失败: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"响应解析失败: {e}")
            raise ValueError(f"无效的JSON响应: {response.text[:200]}")
    
    def get_realtime_lottery(self, format_type: str = "json") -> Dict[str, Any]:
        """
        获取实时开奖数据
        
        Args:
            format_type: 返回格式 (json/xml/jsonp)
            
        Returns:
            实时开奖数据字典
        """
        params = {
            'appid': self.appid,
            'format': format_type
        }
        
        logger.info("获取实时开奖数据...")
        return self._make_request("259", params)
    
    def get_history_lottery(self, date: Optional[str] = None, limit: int = 30, format_type: str = "json") -> Dict[str, Any]:
        """
        获取历史开奖数据
        
        Args:
            date: 指定日期 (格式: YYYY-MM-DD)
            limit: 返回数量限制
            format_type: 返回格式 (json/xml/jsonp)
            
        Returns:
            历史开奖数据字典
        """
        params = {
            'appid': self.appid,
            'format': format_type,
            'limit': str(limit)
        }
        
        if date:
            params['date'] = date
        
        logger.info(f"获取历史开奖数据 - 日期: {date or '全部'}, 限制: {limit}")
        return self._make_request("260", params)
    
    def parse_lottery_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        解析开奖数据为标准格式
        
        Args:
            raw_data: 原始API响应数据
            
        Returns:
            标准化的开奖数据列表
        """
        parsed_data = []
        
        try:
            # 检查响应状态
            if raw_data.get('codeid') != 10000:
                logger.warning(f"API响应异常: {raw_data.get('message')}")
                return parsed_data
            
            # 解析当前开奖数据 (实时数据结构)
            if 'retdata' in raw_data and isinstance(raw_data['retdata'], dict):
                retdata = raw_data['retdata']
                if 'curent' in retdata and retdata['curent']:
                    current_data = retdata['curent']
                    parsed_item = {
                        'draw_id': current_data.get('long_issue'),
                        'timestamp': current_data.get('kjtime'),
                        'numbers': current_data.get('number', []),
                        'result_sum': None,
                        'result_digits': None,
                        'source': 'upstream_api',
                        'created_at': datetime.now(timezone.utc).isoformat()
                    }
                    
                    # 计算结果和
                    if parsed_item['numbers']:
                        # 转换字符串数字为整数
                        numbers_int = [int(x) for x in parsed_item['numbers'] if str(x).isdigit()]
                        parsed_item['result_sum'] = sum(numbers_int)
                        parsed_item['result_digits'] = numbers_int
                        parsed_item['numbers'] = numbers_int
                    
                    parsed_data.append(parsed_item)
            
            # 解析历史数据列表
            if 'retdata' in raw_data and isinstance(raw_data['retdata'], list):
                for item in raw_data['retdata']:
                    parsed_item = {
                        'draw_id': item.get('long_issue'),
                        'timestamp': item.get('kjtime'),
                        'numbers': item.get('number', []),
                        'result_sum': None,
                        'result_digits': None,
                        'source': 'upstream_api',
                        'created_at': datetime.now(timezone.utc).isoformat()
                    }
                    
                    # 计算结果和
                    if parsed_item['numbers']:
                        # 转换字符串数字为整数
                        numbers_int = [int(x) for x in parsed_item['numbers'] if str(x).isdigit()]
                        parsed_item['result_sum'] = sum(numbers_int)
                        parsed_item['result_digits'] = numbers_int
                        parsed_item['numbers'] = numbers_int
                    
                    parsed_data.append(parsed_item)
            
            logger.info(f"解析完成，共 {len(parsed_data)} 条记录")
            return parsed_data
            
        except Exception as e:
            logger.error(f"数据解析失败: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        测试API连接
        
        Returns:
            连接是否成功
        """
        try:
            logger.info("测试API连接...")
            response = self.get_realtime_lottery()
            
            if response.get('codeid') == 10000:
                logger.info("API连接测试成功")
                return True
            else:
                logger.error(f"API连接测试失败: {response.get('message')}")
                return False
                
        except Exception as e:
            logger.error(f"API连接测试异常: {e}")
            return False

# 使用示例
if __name__ == "__main__":
    # 创建API客户端
    api_client = PC28UpstreamAPI()
    
    # 测试连接
    if api_client.test_connection():
        print("✅ API连接正常")
        
        # 获取实时数据
        realtime_data = api_client.get_realtime_lottery()
        print(f"实时数据: {json.dumps(realtime_data, indent=2, ensure_ascii=False)}")
        
        # 获取历史数据
        history_data = api_client.get_history_lottery(limit=5)
        print(f"历史数据: {json.dumps(history_data, indent=2, ensure_ascii=False)}")
        
        # 解析数据
        parsed_realtime = api_client.parse_lottery_data(realtime_data)
        parsed_history = api_client.parse_lottery_data(history_data)
        
        print(f"解析后实时数据: {json.dumps(parsed_realtime, indent=2, ensure_ascii=False)}")
        print(f"解析后历史数据: {json.dumps(parsed_history, indent=2, ensure_ascii=False)}")
    else:
        print("❌ API连接失败")