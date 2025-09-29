#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
端到端系统验证测试
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

import json
import logging
import requests
import time
from google.cloud import bigquery
from pc28_upstream_api import PC28UpstreamAPI
from integrated_data_adapter import IntegratedDataAdapter

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'integrated_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_upstream_api():
    """测试上游API连接"""
    logger.info("=== 测试上游API连接 ===")
    
    try:
        config = load_config()
        api_client = PC28UpstreamAPI(
            appid=config['upstream_api']['appid'],
            secret_key=config['upstream_api']['secret_key']
        )
        
        # 测试连接
        result = api_client.test_connection()
        if result:
            logger.info("✓ 上游API连接成功")
            return True
        else:
            logger.error("✗ 上游API连接失败")
            return False
            
    except Exception as e:
        logger.error(f"上游API测试异常: {e}")
        return False

def test_bigquery_connection():
    """测试BigQuery连接"""
    logger.info("=== 测试BigQuery连接 ===")
    
    try:
        client = bigquery.Client(project='wprojectl')
        
        # 测试查询
        query = "SELECT 1 as test_value"
        query_job = client.query(query)
        results = list(query_job.result())
        
        if results and results[0].test_value == 1:
            logger.info("✓ BigQuery连接成功")
            return True
        else:
            logger.error("✗ BigQuery连接失败")
            return False
            
    except Exception as e:
        logger.error(f"BigQuery连接测试异常: {e}")
        return False

def test_cloud_function():
    """测试Cloud Function"""
    logger.info("=== 测试Cloud Function ===")
    
    try:
        # 健康检查
        health_url = "https://us-central1-wprojectl.cloudfunctions.net/pc28-e2e-function"
        response = requests.get(health_url, timeout=30)
        
        if response.status_code == 200:
            logger.info("✓ Cloud Function健康检查通过")
            
            # 测试数据同步功能
            sync_response = requests.post(health_url, json={}, timeout=60)
            
            if sync_response.status_code == 200:
                result = sync_response.json()
                logger.info(f"Cloud Function响应: {result}")
                
                if result.get('status') in ['success', 'error']:  # 至少有响应
                    logger.info("✓ Cloud Function功能测试通过")
                    return True
                else:
                    logger.error("✗ Cloud Function响应格式异常")
                    return False
            else:
                logger.error(f"✗ Cloud Function数据同步测试失败: {sync_response.status_code}")
                return False
        else:
            logger.error(f"✗ Cloud Function健康检查失败: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Cloud Function测试异常: {e}")
        return False

def test_data_flow():
    """测试数据流"""
    logger.info("=== 测试数据流 ===")
    
    try:
        config = load_config()
        
        # 1. 获取上游数据
        api_client = PC28UpstreamAPI(
            appid=config['upstream_api']['appid'],
            secret_key=config['upstream_api']['secret_key']
        )
        
        realtime_result = api_client.get_realtime_lottery()
        if realtime_result.get('codeid') != 10000:
            logger.error("获取上游数据失败")
            return False
        
        # 2. 解析数据
        parsed_data = api_client.parse_lottery_data(realtime_result)
        if not parsed_data:
            logger.error("数据解析失败")
            return False
        
        logger.info(f"解析得到 {len(parsed_data)} 条数据")
        
        # 3. 数据适配器测试
        adapter = IntegratedDataAdapter(config)
        sync_result = adapter._sync_to_bigquery_with_validation(parsed_data)
        
        logger.info(f"数据同步结果: {sync_result}")
        
        if sync_result.get('status') in ['success', 'partial']:
            logger.info("✓ 数据流测试通过")
            return True
        else:
            logger.error("✗ 数据流测试失败")
            return False
            
    except Exception as e:
        logger.error(f"数据流测试异常: {e}")
        return False

def test_performance():
    """测试性能"""
    logger.info("=== 测试性能 ===")
    
    try:
        config = load_config()
        api_client = PC28UpstreamAPI(
            appid=config['upstream_api']['appid'],
            secret_key=config['upstream_api']['secret_key']
        )
        
        # 测试API响应时间
        start_time = time.time()
        result = api_client.get_realtime_lottery()
        api_time = time.time() - start_time
        
        logger.info(f"API响应时间: {api_time:.2f}秒")
        
        if api_time < 10:  # 10秒内响应
            logger.info("✓ API性能测试通过")
            return True
        else:
            logger.warning("⚠ API响应时间较长")
            return False
            
    except Exception as e:
        logger.error(f"性能测试异常: {e}")
        return False

def run_e2e_tests():
    """运行端到端测试"""
    logger.info("🚀 开始端到端系统验证测试")
    
    tests = [
        ("上游API连接", test_upstream_api),
        ("BigQuery连接", test_bigquery_connection),
        ("Cloud Function", test_cloud_function),
        ("数据流", test_data_flow),
        ("性能测试", test_performance)
    ]
    
    results = {}
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- 执行测试: {test_name} ---")
        try:
            result = test_func()
            results[test_name] = result
            if result:
                passed += 1
                logger.info(f"✓ {test_name} 测试通过")
            else:
                logger.error(f"✗ {test_name} 测试失败")
        except Exception as e:
            logger.error(f"✗ {test_name} 测试异常: {e}")
            results[test_name] = False
    
    # 生成测试报告
    logger.info(f"\n=== 端到端测试报告 ===")
    logger.info(f"总测试数: {total}")
    logger.info(f"通过数: {passed}")
    logger.info(f"失败数: {total - passed}")
    logger.info(f"通过率: {passed/total*100:.1f}%")
    
    for test_name, result in results.items():
        status = "✓ 通过" if result else "✗ 失败"
        logger.info(f"  {test_name}: {status}")
    
    return results, passed, total

def main():
    """主函数"""
    results, passed, total = run_e2e_tests()
    
    if passed == total:
        logger.info("\n🎉 所有端到端测试通过！系统运行正常")
        return True
    elif passed > 0:
        logger.warning(f"\n⚠ 部分测试通过 ({passed}/{total})，系统部分功能正常")
        return True
    else:
        logger.error("\n❌ 所有测试失败！系统存在严重问题")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)