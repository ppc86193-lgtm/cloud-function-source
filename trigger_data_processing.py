#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
触发数据处理脚本
手动触发增强数据流转系统的实际数据处理功能
"""

import sys
import os
import time
import logging
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from enhanced_data_flow_system import EnhancedDataFlowSystem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def trigger_realtime_data_processing():
    """触发实时数据处理"""
    logger.info("开始触发实时数据处理...")
    
    # 创建系统实例
    flow_system = EnhancedDataFlowSystem()
    
    # 手动触发实时数据拉取
    try:
        flow_system._realtime_data_pull()
        logger.info("实时数据拉取完成")
    except Exception as e:
        logger.error(f"实时数据拉取失败: {e}")
    
    return flow_system

def trigger_historical_data_processing():
    """触发历史数据处理"""
    logger.info("开始触发历史数据处理...")
    
    # 创建系统实例
    flow_system = EnhancedDataFlowSystem()
    
    # 手动触发历史数据回填（最近3天）
    try:
        for i in range(3):
            date = datetime.now() - timedelta(days=i)
            flow_system._historical_data_backfill_for_date(date)
            logger.info(f"历史数据回填完成: {date.strftime('%Y-%m-%d')}")
    except Exception as e:
        logger.error(f"历史数据回填失败: {e}")
    
    return flow_system

def generate_processing_report(flow_system):
    """生成处理报告"""
    logger.info("生成数据处理报告...")
    
    # 获取系统状态
    status = flow_system.get_system_status()
    
    # 生成报告
    report = f"""
数据处理触发报告
================
触发时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

处理结果:
- 实时数据拉取: {status['metrics']['realtime_pulls']} 条
- 历史数据回填: {status['metrics']['backfill_records']} 条
- 字段利用率: {status['metrics']['field_utilization_rate']:.1f}%
- 处理速度提升: {status['metrics']['processing_speed_improvement']:.1f}%
- 存储空间节省: {status['metrics']['optimization_savings_mb']:.2f} MB

系统配置:
- 批处理大小: {status['batch_size']}
- 最大工作线程: {status['max_workers']}
- 优化模式: {'启用' if status['optimization_enabled'] else '禁用'}

数据库状态:
- 数据库路径: {status['database_path']}
- 系统运行时间: {status['system_uptime']:.2f} 秒
- 最后健康检查: {status['last_health_check']}

处理建议:
1. 数据处理已成功触发
2. 建议定期检查数据完整性
3. 监控系统性能指标
4. 根据需要调整批处理参数
    """.strip()
    
    print(report)
    
    # 保存报告
    report_file = f"data_processing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report)
    
    logger.info(f"报告已保存到: {report_file}")
    return report_file

def main():
    """主函数"""
    logger.info("开始触发数据处理...")
    
    # 1. 触发实时数据处理
    flow_system = trigger_realtime_data_processing()
    time.sleep(2)  # 等待处理完成
    
    # 2. 触发历史数据处理
    trigger_historical_data_processing()
    time.sleep(2)  # 等待处理完成
    
    # 3. 更新性能指标
    try:
        flow_system._update_performance_metrics()
        logger.info("性能指标更新完成")
    except Exception as e:
        logger.error(f"性能指标更新失败: {e}")
    
    # 4. 生成处理报告
    report_file = generate_processing_report(flow_system)
    
    logger.info("数据处理触发完成")
    return report_file

if __name__ == "__main__":
    main()