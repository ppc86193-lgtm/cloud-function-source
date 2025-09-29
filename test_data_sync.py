#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试数据同步到BigQuery
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

import json
import logging
from integrated_data_adapter import IntegratedDataAdapter
from pc28_upstream_api import PC28UpstreamAPI

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'integrated_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_api_data_sync():
    """测试API数据同步到BigQuery"""
    logger.info("=== 开始测试API数据同步 ===")
    
    try:
        # 加载配置
        config = load_config()
        logger.info("配置加载成功")
        
        # 创建API客户端
        api_client = PC28UpstreamAPI(
            appid=config['upstream_api']['appid'],
            secret_key=config['upstream_api']['secret_key']
        )
        
        # 测试API连接
        logger.info("测试API连接...")
        connection_result = api_client.test_connection()
        if not connection_result:
            logger.error(f"API连接失败")
            return False
        
        logger.info("API连接成功")
        
        # 获取实时数据
        logger.info("获取实时开奖数据...")
        realtime_result = api_client.get_realtime_lottery()
        if realtime_result.get('codeid') != 10000:
            logger.error(f"获取实时数据失败: {realtime_result}")
            return False
        
        # 解析数据
        parsed_data = api_client.parse_lottery_data(realtime_result)
        logger.info(f"解析得到 {len(parsed_data)} 条数据")
        
        if not parsed_data:
            logger.warning("没有可同步的数据")
            return True
        
        # 创建集成数据适配器
        logger.info("初始化集成数据适配器...")
        adapter = IntegratedDataAdapter(config)
        
        # 同步数据到BigQuery
        logger.info("开始同步数据到BigQuery...")
        sync_result = adapter._sync_to_bigquery_with_validation(parsed_data)
        
        logger.info(f"数据同步结果: {sync_result}")
        
        if sync_result.get('status') in ['success', 'partial']:
            logger.info("✓ 数据同步成功")
            return True
        else:
            logger.error("✗ 数据同步失败")
            return False
            
    except Exception as e:
        logger.error(f"测试过程中发生异常: {e}")
        return False

def main():
    """主函数"""
    logger.info("开始API数据同步测试")
    
    success = test_api_data_sync()
    
    if success:
        logger.info("🎉 API数据同步测试通过！")
    else:
        logger.error("❌ API数据同步测试失败！")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)