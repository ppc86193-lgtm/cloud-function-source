#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试PC28上游API连接"""

import os
import sys
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 添加模块路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

from python.pc28_upstream_api import PC28UpstreamAPI

def test_api_connection():
    """测试API连接"""
    print("=== PC28 上游API连接测试 ===")
    
    # 从环境变量获取配置
    appid = os.getenv('PC28_APPID')
    secret_key = os.getenv('PC28_SECRET_KEY')
    
    if not appid or not secret_key:
        print("错误: 未找到API配置，请检查环境变量 PC28_APPID 和 PC28_SECRET_KEY")
        return False
    
    print(f"使用配置: appid={appid}, secret_key={'*' * len(secret_key)}")
    
    try:
        # 初始化API客户端
        api = PC28UpstreamAPI(appid, secret_key)
        
        # 测试实时开奖API
        print("\n1. 测试实时开奖API...")
        realtime_response = api.get_realtime_lottery()
        if realtime_response and realtime_response.get('codeid') == 10000:
            print(f"✓ 实时开奖API连接成功")
            retdata = realtime_response.get('retdata', {})
            if 'curent' in retdata and retdata['curent']:
                current = retdata['curent']
                print(f"  最新期号: {current.get('long_issue', 'N/A')}")
                print(f"  开奖时间: {current.get('kjtime', 'N/A')}")
                print(f"  开奖号码: {current.get('number', 'N/A')}")
            else:
                print("  当前开奖数据为空")
        else:
            print(f"✗ 实时开奖API连接失败: {realtime_response.get('message', '未知错误')}")
            return False
            
        # 测试历史数据API
        print("\n2. 测试历史数据API...")
        history_response = api.get_history_lottery(limit=5)
        if history_response and history_response.get('codeid') == 10000:
            print(f"✓ 历史数据API连接成功")
            retdata = history_response.get('retdata', [])
            if retdata:
                print(f"  返回数据条数: {len(retdata)}")
                first = retdata[0]
                print(f"  首条期号: {first.get('long_issue', 'N/A')}")
                print(f"  开奖时间: {first.get('kjtime', 'N/A')}")
                print(f"  开奖号码: {first.get('number', 'N/A')}")
            else:
                print("  历史数据为空")
        else:
            print(f"✗ 历史数据API连接失败: {history_response.get('message', '未知错误')}")
            return False
            
        print("\n=== API连接测试完成 ===")
        return True
        
    except Exception as e:
        print(f"✗ API连接测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_api_connection()
    sys.exit(0 if success else 1)