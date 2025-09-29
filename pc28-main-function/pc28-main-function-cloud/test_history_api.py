#!/usr/bin/env python3
"""
测试历史数据API并优化历史数据采集
"""

import os
import sys
import json
import time
import hashlib
import requests
from datetime import datetime, timezone

# 加载环境变量
from dotenv import load_dotenv
load_dotenv()

def generate_signature(params: dict, wapi_key: str) -> str:
    """生成API签名"""
    sorted_params = sorted(params.items())
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    param_string += wapi_key
    return hashlib.md5(param_string.encode('utf-8')).hexdigest()

def test_history_api_detailed():
    """详细测试历史数据API"""
    print("开始详细测试历史数据API...")
    
    # API配置
    api_url = os.getenv('PC28_HISTORY_API_URL', 'https://rijb.api.storeapi.net/api/119/260')
    wapi_key = os.getenv('WAPI_KEY', 'ca9edbfee35c22a0d6c4cf6722506af0')
    wapi_id = os.getenv('WAPI_ID', '45928')
    
    print(f"历史API URL: {api_url}")
    print(f"WAPI ID: {wapi_id}")
    print(f"WAPI KEY: {wapi_key[:8]}...")
    
    try:
        current_time = str(int(time.time()))
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 测试不同的参数组合
        test_cases = [
            {"date": today, "limit": "5", "description": "今日最新5条"},
            {"date": "2025-09-29", "limit": "10", "description": "指定日期10条"},
            {"limit": "3", "description": "不指定日期，最新3条"},
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n--- 测试用例 {i}: {test_case['description']} ---")
            
            params = {
                'appid': wapi_id,
                'format': 'json',
                'time': current_time
            }
            
            # 添加可选参数
            if 'date' in test_case:
                params['date'] = test_case['date']
            if 'limit' in test_case:
                params['limit'] = test_case['limit']
            
            params['sign'] = generate_signature(params, wapi_key)
            
            print(f"请求参数: {params}")
            
            response = requests.get(api_url, params=params, timeout=30)
            
            print(f"响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("历史API响应成功!")
                print(f"响应数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
                
                # 分析数据结构
                if 'retdata' in data:
                    retdata = data['retdata']
                    print(f"retdata类型: {type(retdata)}")
                    if isinstance(retdata, list):
                        print(f"历史数据条数: {len(retdata)}")
                        if retdata:
                            print(f"第一条数据结构: {json.dumps(retdata[0], ensure_ascii=False, indent=2)}")
                            # 分析字段
                            fields = set()
                            for item in retdata:
                                if isinstance(item, dict):
                                    fields.update(item.keys())
                            print(f"所有字段: {sorted(fields)}")
                    elif isinstance(retdata, dict):
                        print(f"retdata键: {list(retdata.keys())}")
                
            else:
                print(f"历史API请求失败: {response.status_code}")
                print(f"响应内容: {response.text}")
            
            time.sleep(1)  # 避免请求过快
            
    except Exception as e:
        print(f"测试异常: {e}")
        import traceback
        traceback.print_exc()

def test_history_data_processing():
    """测试历史数据处理"""
    print("\n开始测试历史数据处理...")
    
    try:
        # 导入本地脚本
        import api_auto_fetch
        
        # 创建采集器实例
        fetcher = api_auto_fetch.PC28DataFetcher()
        
        # 测试历史数据获取
        print("获取历史数据...")
        history_data = fetcher.fetch_history_data(limit=5)
        
        if history_data:
            print("历史数据获取成功!")
            print(f"原始历史数据: {json.dumps(history_data, ensure_ascii=False, indent=2)}")
            
            # 测试数据清洗
            print("清洗历史数据...")
            cleaned_data = fetcher.clean_and_validate_data(history_data)
            
            if cleaned_data:
                print(f"历史数据清洗成功，清洗后数据条数: {len(cleaned_data)}")
                for i, item in enumerate(cleaned_data):
                    print(f"清洗后历史数据 {i+1}: {json.dumps(item, ensure_ascii=False, indent=2)}")
            else:
                print("历史数据清洗失败或无有效数据")
        else:
            print("历史数据获取失败")
            
    except Exception as e:
        print(f"历史数据处理测试异常: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主函数"""
    print("=" * 60)
    print("PC28 历史数据API测试工具")
    print("=" * 60)
    
    # 检查环境变量
    print("环境变量检查:")
    print(f"WAPI_KEY: {'已设置' if os.getenv('WAPI_KEY') else '未设置'}")
    print(f"WAPI_ID: {'已设置' if os.getenv('WAPI_ID') else '未设置'}")
    print(f"PC28_HISTORY_API_URL: {'已设置' if os.getenv('PC28_HISTORY_API_URL') else '未设置'}")
    print()
    
    # 运行测试
    test_history_api_detailed()
    test_history_data_processing()
    
    print("\n" + "=" * 60)
    print("历史数据API测试完成")
    print("=" * 60)

if __name__ == "__main__":
    main()