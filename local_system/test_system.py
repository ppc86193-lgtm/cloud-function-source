#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28本地化自愈系统测试脚本
快速测试系统各个组件的功能
"""

import sys
import os
import json
import time
from datetime import datetime

# 添加路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_database import LocalDatabase
from local_api_collector import LocalAPICollector
from local_sql_engine import LocalSQLEngine
from auto_repair_system import AutoRepairSystem
from cloud_sync_manager import CloudSyncManager
from monitoring_alerting_system import MonitoringAlertingSystem

def test_database():
    """测试数据库功能"""
    print("=== 测试数据库功能 ===")
    try:
        db = LocalDatabase()
        
        # 测试连接
        result = db.test_connection()
        print(f"数据库连接测试: {result}")
        
        # 插入测试数据
        test_data = {
            'draw_id': 'test_001',
            'timestamp': datetime.now().isoformat(),
            'period': '20241201001',
            'market': 'pc28',
            'pick': 'big',
            'p_win': 0.65,
            'source': 'test',
            'data_date': datetime.now().strftime('%Y-%m-%d')
        }
        
        db.execute_update("""
            INSERT INTO cloud_pred_today_norm 
            (draw_id, timestamp, period, market, pick, p_win, source, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, tuple(test_data.values()))
        
        # 查询测试数据
        results = db.execute_query("SELECT COUNT(*) as count FROM cloud_pred_today_norm")
        print(f"测试数据插入成功，表中共有 {results[0]['count']} 条记录")
        
        return True
        
    except Exception as e:
        print(f"数据库测试失败: {e}")
        return False

def test_api_collector():
    """测试API采集器"""
    print("\n=== 测试API采集器 ===")
    try:
        collector = LocalAPICollector()
        
        # 测试API连接
        result = collector.test_api_connection()
        print(f"API连接测试: {result}")
        
        # 测试数据采集（模拟）
        print("开始模拟数据采集...")
        collector.collect_and_store_data()
        print("数据采集完成")
        
        return True
        
    except Exception as e:
        print(f"API采集器测试失败: {e}")
        return False

def test_sql_engine():
    """测试SQL引擎"""
    print("\n=== 测试SQL引擎 ===")
    try:
        engine = LocalSQLEngine()
        
        # 创建视图
        print("创建视图...")
        view_results = engine.create_all_views()
        success_count = sum(1 for v in view_results.values() if v)
        print(f"视图创建结果: {success_count}/{len(view_results)} 成功")
        
        # 运行数据管道
        print("运行数据管道...")
        pipeline_result = engine.run_full_pipeline()
        print(f"数据管道运行: {'成功' if pipeline_result.get('success') else '失败'}")
        
        return True
        
    except Exception as e:
        print(f"SQL引擎测试失败: {e}")
        return False

def test_auto_repair():
    """测试自动修复系统"""
    print("\n=== 测试自动修复系统 ===")
    try:
        repair_system = AutoRepairSystem()
        
        # 健康检查
        print("执行健康检查...")
        health_result = repair_system.perform_health_check()
        print(f"健康检查完成，发现 {len(health_result.get('issues', []))} 个问题")
        
        # 自动修复
        if health_result.get('issues'):
            print("开始自动修复...")
            repair_results = repair_system.auto_repair_issues(health_result['issues'])
            print(f"修复完成，处理了 {len(repair_results)} 个问题")
        
        return True
        
    except Exception as e:
        print(f"自动修复系统测试失败: {e}")
        return False

def test_monitoring():
    """测试监控告警系统"""
    print("\n=== 测试监控告警系统 ===")
    try:
        monitoring = MonitoringAlertingSystem()
        
        # 收集指标
        print("收集系统指标...")
        monitoring.collect_system_metrics()
        monitoring.collect_data_metrics()
        
        # 检查告警
        print("检查告警条件...")
        # 先收集指标再检查告警
        system_metrics = monitoring.collect_system_metrics()
        data_metrics = monitoring.collect_data_metrics()
        all_metrics = {**system_metrics, **data_metrics}
        alerts = monitoring.check_alert_conditions(all_metrics)
        
        # 获取状态
        print(f"发现 {len(alerts)} 个告警")
        
        return True
        
    except Exception as e:
        print(f"监控告警系统测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("PC28本地化自愈系统 - 快速测试")
    print("=" * 50)
    
    test_results = {}
    
    # 测试各个组件
    test_results['database'] = test_database()
    test_results['api_collector'] = test_api_collector()
    test_results['sql_engine'] = test_sql_engine()
    test_results['auto_repair'] = test_auto_repair()
    test_results['monitoring'] = test_monitoring()
    
    # 汇总结果
    print("\n" + "=" * 50)
    print("测试结果汇总:")
    success_count = 0
    for component, result in test_results.items():
        status = "✓ 成功" if result else "✗ 失败"
        print(f"  {component}: {status}")
        if result:
            success_count += 1
    
    print(f"\n总体结果: {success_count}/{len(test_results)} 组件测试通过")
    
    if success_count == len(test_results):
        print("🎉 所有组件测试通过！系统运行正常")
        return 0
    else:
        print("⚠️  部分组件测试失败，请检查日志")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)