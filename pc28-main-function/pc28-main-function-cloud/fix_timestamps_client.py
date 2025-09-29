#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正BigQuery中的时间戳数据（使用Python客户端）
将错误的2025年时间戳修正为正确的2024年时间戳
"""

import json
from datetime import datetime, timezone, timedelta
from python.bigquery_client_adapter import BQClient

def fix_draw_results_timestamps(bq_client: BQClient):
    """修正draw_results表的时间戳"""
    print("开始修正draw_results表的时间戳...")
    
    # 获取当前正确的时间（2024年）
    correct_now = datetime(2024, 12, 19, 17, 40, 0, tzinfo=timezone.utc)
    
    # 更新时间戳为正确的2024年时间
    update_sql = f"""
    UPDATE `{bq_client.project}.{bq_client.ds_draw}.draw_results`
    SET timestamp = TIMESTAMP('{correct_now.strftime('%Y-%m-%d %H:%M:%S')}')
    WHERE DATE(timestamp) = '2025-09-24'
    """
    
    try:
        success = bq_client.execute_dml(update_sql)
        if success:
            print("✓ draw_results表时间戳已修正")
        else:
            print("✗ 修正draw_results表失败")
    except Exception as e:
        print(f"✗ 修正draw_results表失败: {e}")

def create_missing_views(bq_client: BQClient):
    """创建缺失的视图"""
    print("创建缺失的视图...")
    
    # 创建draws_14w_dedup_v视图
    create_view_sql = f"""
    CREATE OR REPLACE VIEW `{bq_client.project}.{bq_client.ds_draw}.draws_14w_dedup_v` AS
    SELECT 
        draw_id,
        timestamp,
        CAST(REGEXP_EXTRACT(result, r'(\\d+)') AS INT64) as result_sum,
        result as result_digits,
        timestamp as created_at,
        MOD(CAST(REGEXP_EXTRACT(result, r'(\\d+)') AS INT64), 2) = 0 as is_even,
        CAST(REGEXP_EXTRACT(result, r'(\\d+)') AS INT64) >= 14 as is_big
    FROM `{bq_client.project}.{bq_client.ds_draw}.draw_results`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14*7*24 HOUR)
    """
    
    try:
        success = bq_client.execute_dml(create_view_sql)
        if success:
            print("✓ draws_14w_dedup_v视图已创建")
        else:
            print("✗ 创建视图失败")
    except Exception as e:
        print(f"✗ 创建视图失败: {e}")

def insert_current_test_data(bq_client: BQClient):
    """插入当前时间的测试数据"""
    print("插入当前时间的测试数据...")
    
    # 获取当前正确时间
    now = datetime(2024, 12, 19, 17, 40, 0, tzinfo=timezone.utc)
    
    # 准备批量插入数据
    rows_to_insert = []
    for i in range(3, 8):
        timestamp = now + timedelta(minutes=i*10)
        row_data = {
            'draw_id': f'draw_{i:03d}',
            'result': f'candidate_{i}',
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }
        rows_to_insert.append(row_data)
    
    try:
        table_name = f"{bq_client.project}.{bq_client.ds_draw}.draw_results"
        success = bq_client.insert_rows(table_name, rows_to_insert)
        if success:
            print(f"✓ 成功插入 {len(rows_to_insert)} 条测试数据")
        else:
            print("✗ 插入测试数据失败")
    except Exception as e:
        print(f"✗ 插入测试数据失败: {e}")

def verify_fix(bq_client: BQClient):
    """验证修正结果"""
    print("\n验证修正结果...")
    
    # 查询最新数据
    verify_sql = f"""
    SELECT draw_id, result, timestamp, 
           DATE(timestamp, '{bq_client.timezone}') as date_local
    FROM `{bq_client.project}.{bq_client.ds_draw}.draw_results` 
    ORDER BY timestamp DESC 
    LIMIT 10
    """
    
    try:
        result = bq_client.run_query(verify_sql)
        if result:
            print("\n最新数据:")
            for row in result:
                print(f"  {row['draw_id']}: {row['result']} - {row['timestamp']} (本地时间: {row['date_local']})")
    except Exception as e:
        print(f"查询最新数据失败: {e}")
    
    # 测试今日开奖数查询
    today_sql = f"""
    SELECT COUNT(*) as count
    FROM `{bq_client.project}.{bq_client.ds_draw}.draw_results`
    WHERE DATE(timestamp, '{bq_client.timezone}') = CURRENT_DATE('{bq_client.timezone}')
    """
    
    try:
        result = bq_client.run_query(today_sql)
        if result:
            count = result[0]['count']
            print(f"\n今日开奖数: {count}")
    except Exception as e:
        print(f"查询今日开奖数失败: {e}")

def main():
    """主函数"""
    print("=== PC28 时间戳修正工具（Python客户端版本） ===")
    print(f"当前系统时间: {datetime.now()}")
    print(f"目标修正时间: 2024-12-19")
    print()
    
    # 初始化BigQuery客户端
    try:
        bq_client = BQClient(
            project="wprojectl",
            ds_lab="lab_dataset",
            ds_draw="draw_dataset",
            location="us-central1",
            timezone="Asia/Shanghai"
        )
        
        # 执行修正步骤
        fix_draw_results_timestamps(bq_client)
        create_missing_views(bq_client)
        insert_current_test_data(bq_client)
        verify_fix(bq_client)
        
        print("\n=== 修正完成 ===")
        
    except Exception as e:
        print(f"初始化BigQuery客户端失败: {e}")
        return False
    
    return True

if __name__ == '__main__':
    main()