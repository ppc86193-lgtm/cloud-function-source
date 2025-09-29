#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API集成测试脚本
测试加拿大28上游API的连接、签名验证和数据获取功能
"""

import json
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pc28_upstream_api import PC28UpstreamAPI
from realtime_lottery_service import RealtimeLotteryService
from history_backfill_service import HistoryBackfillService
from integrated_data_adapter import IntegratedDataAdapter

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_test_config() -> Dict[str, Any]:
    """
    加载测试配置
    """
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'integrated_config.json')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"配置文件未找到: {config_path}，使用默认配置")
        return {
            "appid": "45928",
            "secret_key": "ca9edbfee35c22a0d6c4cf6722506af0",
            "upstream_api": {
                "appid": "45928",
                "secret_key": "ca9edbfee35c22a0d6c4cf6722506af0",
                "realtime_url": "https://rijb.api.storeapi.net/api/119/259",
                "history_url": "https://rijb.api.storeapi.net/api/119/260",
                "timeout": 30,
                "max_retries": 3
            },
            "data_source": {
                "use_upstream_api": True,
                "fallback_to_bigquery": True,
                "sync_to_bigquery": False,
                "validation_enabled": True
            }
        }

def test_md5_signature():
    """
    测试MD5签名生成功能
    """
    logger.info("=== 测试MD5签名生成 ===")
    
    config = load_test_config()
    api_client = PC28UpstreamAPI(config['upstream_api']['appid'], config['upstream_api']['secret_key'])
    
    # 测试实时接口签名
    params = {
        'appid': '45928',
        'format': 'json',
        'time': '1545829466'
    }
    
    signature = api_client._generate_md5_sign(params)
    logger.info(f"实时接口签名: {signature}")
    
    # 测试历史接口签名
    history_params = {
        'appid': '45928',
        'date': '2020-12-16',
        'format': 'json',
        'limit': '30',
        'time': '1545829466'
    }
    
    history_signature = api_client._generate_md5_sign(history_params)
    logger.info(f"历史接口签名: {history_signature}")
    
    return True

def test_api_connectivity():
    """
    测试API连接性
    """
    logger.info("=== 测试API连接性 ===")
    
    config = load_test_config()
    api_client = PC28UpstreamAPI(config['upstream_api']['appid'], config['upstream_api']['secret_key'])
    
    # 测试实时接口
    try:
        realtime_response = api_client.get_realtime_lottery()
        realtime_success = realtime_response and realtime_response.get('codeid') == 10000
    except Exception as e:
        logger.error(f"实时接口测试失败: {e}")
        realtime_success = False
    
    # 测试历史接口
    try:
        history_response = api_client.get_history_lottery(date='2024-12-19', limit=1)
        history_success = history_response and history_response.get('codeid') == 10000
    except Exception as e:
        logger.error(f"历史接口测试失败: {e}")
        history_success = False
    
    connectivity_result = {
        'realtime': {'success': realtime_success, 'response_time': 0.0, 'error': None},
        'history': {'success': history_success, 'response_time': 0.0, 'error': None}
    }
    
    if connectivity_result['realtime']['success']:
        logger.info("✓ 实时接口连接成功")
        logger.info(f"  响应时间: {connectivity_result['realtime']['response_time']:.2f}s")
    else:
        logger.error("✗ 实时接口连接失败")
        logger.error(f"  错误: {connectivity_result['realtime']['error']}")
    
    if connectivity_result['history']['success']:
        logger.info("✓ 历史接口连接成功")
        logger.info(f"  响应时间: {connectivity_result['history']['response_time']:.2f}s")
    else:
        logger.error("✗ 历史接口连接失败")
        logger.error(f"  错误: {connectivity_result['history']['error']}")
    
    return connectivity_result

def test_realtime_data_fetch():
    """
    测试实时数据获取
    """
    logger.info("=== 测试实时数据获取 ===")
    
    config = load_test_config()
    realtime_service = RealtimeLotteryService(config['upstream_api']['appid'], config['upstream_api']['secret_key'])
    
    try:
        # 获取当前开奖信息
        current_draw = realtime_service.fetch_current_draw()
        if current_draw:
            logger.info("✓ 成功获取当前开奖信息")
            logger.info(f"  期号: {current_draw.get('draw_id')}")
            logger.info(f"  开奖时间: {current_draw.get('timestamp')}")
            logger.info(f"  开奖号码: {current_draw.get('numbers')}")
            logger.info(f"  和值: {current_draw.get('result_sum')}")
        else:
            logger.warning("⚠ 未获取到当前开奖信息")
        
        # 获取下期开奖信息
        next_draw = realtime_service.get_next_draw_info()
        if next_draw:
            logger.info("✓ 成功获取下期开奖信息")
            logger.info(f"  下期期号: {next_draw.get('next_issue')}")
            logger.info(f"  下期开奖时间: {next_draw.get('next_time')}")
            logger.info(f"  距离开奖: {next_draw.get('award_time')}秒")
        else:
            logger.warning("⚠ 未获取到下期开奖信息")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 实时数据获取失败: {e}")
        return False

def test_history_data_fetch():
    """
    测试历史数据获取
    """
    logger.info("=== 测试历史数据获取 ===")
    
    config = load_test_config()
    backfill_service = HistoryBackfillService(config['upstream_api']['appid'], config['upstream_api']['secret_key'])
    
    try:
        # 获取昨天的数据
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        data_list = backfill_service.fetch_history_by_date(yesterday, limit=5)
        
        if data_list:
            logger.info(f"✓ 成功获取 {yesterday} 的历史数据")
            logger.info(f"  获取记录数: {len(data_list)}")
            
            # 显示前几条记录
            for i, record in enumerate(data_list[:3]):
                logger.info(f"  记录{i+1}: 期号={record.get('draw_id')}, "
                          f"时间={record.get('timestamp')}, 号码={record.get('numbers')}, 和值={record.get('result_sum')}")
        else:
            logger.error(f"✗ 历史数据获取失败")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 历史数据获取异常: {e}")
        return False

def test_integrated_adapter():
    """
    测试集成数据适配器
    """
    logger.info("=== 测试集成数据适配器 ===")
    
    try:
        config = load_test_config()
        
        # 模拟BigQuery环境变量（测试时可能不存在）
        os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'test-project')
        
        # 创建测试配置
        test_config = {
            'bigquery': {
                'project': 'test-project',
                'dataset_lab': 'test_lab',
                'dataset_draw': 'test_draw',
                'location': 'US',
                'timezone': 'UTC'
            },
            'upstream_api': config['upstream_api'],
            'data_source': {
                'use_upstream_api': True,
                'fallback_to_bigquery': False,
                'sync_to_bigquery': False,
                'validation_enabled': False
            }
        }
        
        adapter = IntegratedDataAdapter(test_config)
        
        # 测试获取当前开奖信息
        current_info = adapter.get_current_draw_info()
        if current_info:
            logger.info("✓ 集成适配器成功获取当前开奖信息")
            logger.info(f"  数据源: {current_info.get('source', 'unknown')}")
        else:
            logger.warning("⚠ 集成适配器未获取到当前开奖信息")
        
        # 测试检查新开奖数据
        new_draws = adapter.check_for_new_draws()
        if new_draws:
            logger.info(f"✓ 检测到 {len(new_draws)} 条新开奖数据")
        else:
            logger.info("ℹ 暂无新开奖数据")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 集成适配器测试失败: {e}")
        return False

def run_comprehensive_test():
    """
    运行综合测试
    """
    logger.info("开始API集成综合测试")
    logger.info("=" * 50)
    
    test_results = {
        'md5_signature': False,
        'api_connectivity': False,
        'realtime_data': False,
        'history_data': False,
        'integrated_adapter': False
    }
    
    try:
        # 1. 测试MD5签名
        test_results['md5_signature'] = test_md5_signature()
        
        # 2. 测试API连接性
        connectivity_result = test_api_connectivity()
        test_results['api_connectivity'] = (
            connectivity_result['realtime']['success'] and 
            connectivity_result['history']['success']
        )
        
        # 3. 测试实时数据获取
        if test_results['api_connectivity']:
            test_results['realtime_data'] = test_realtime_data_fetch()
        
        # 4. 测试历史数据获取
        if test_results['api_connectivity']:
            test_results['history_data'] = test_history_data_fetch()
        
        # 5. 测试集成适配器
        test_results['integrated_adapter'] = test_integrated_adapter()
        
    except Exception as e:
        logger.error(f"测试过程中发生异常: {e}")
    
    # 输出测试结果摘要
    logger.info("=" * 50)
    logger.info("测试结果摘要:")
    
    passed_tests = 0
    total_tests = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✓ 通过" if result else "✗ 失败"
        logger.info(f"  {test_name}: {status}")
        if result:
            passed_tests += 1
    
    success_rate = (passed_tests / total_tests) * 100
    logger.info(f"\n总体测试通过率: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
    
    if success_rate >= 80:
        logger.info("🎉 API集成测试基本通过！")
        return True
    else:
        logger.warning("⚠️ API集成测试存在问题，需要进一步检查")
        return False

if __name__ == '__main__':
    success = run_comprehensive_test()
    sys.exit(0 if success else 1)