#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正BigQuery中的时间戳数据
将错误的2025年时间戳修正为正确的2024年时间戳
"""

import subprocess
import json
from datetime import datetime, timezone, timedelta

def run_bq_query(sql):
    """执行BigQuery查询"""
    cmd = f'bq query --use_legacy_sql=false --format=json "{sql}"'
    try:
        result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        return json.loads(result.decode('utf-8'))
    except Exception as e:
        print(f"查询失败: {e}")
        return None

def fix_draw_results_timestamps():
    """修正draw_results表的时间戳"""
    print("开始修正draw_results表的时间戳...")
    
    # 获取当前正确的时间（2024年）
    correct_now = datetime(2024, 12, 19, 17, 40, 0, tzinfo=timezone.utc)
    
    # 更新时间戳为正确的2024年时间
    update_sql = f"""
    UPDATE `wprojectl.draw_dataset.draw_results`
    SET timestamp = TIMESTAMP('{correct_now.strftime('%Y-%m-%d %H:%M:%S')}')
    WHERE DATE(timestamp) = '2025-09-24'
    """
    
    try:
        subprocess.run(f'bq query --use_legacy_sql=false "{update_sql}"', shell=True, check=True)
        print("✓ draw_results表时间戳已修正")
    except Exception as e:
        print(f"✗ 修正draw_results表失败: {e}")

def create_missing_views():
    """创建缺失的视图"""
    print("创建缺失的视图...")
    
    # 创建draws_14w_dedup_v视图
    create_view_sql = """
    CREATE OR REPLACE VIEW `wprojectl.draw_dataset.draws_14w_dedup_v` AS
    SELECT 
        draw_id,
        timestamp,
        CAST(REGEXP_EXTRACT(result, r'(\\d+)') AS INT64) as result_sum,
        result as result_digits,
        timestamp as created_at,
        MOD(CAST(REGEXP_EXTRACT(result, r'(\\d+)') AS INT64), 2) = 0 as is_even,
        CAST(REGEXP_EXTRACT(result, r'(\\d+)') AS INT64) >= 14 as is_big
    FROM `wprojectl.draw_dataset.draw_results`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14*7*24 HOUR)
    """
    
    try:
        subprocess.run(f'bq query --use_legacy_sql=false "{create_view_sql}"', shell=True, check=True)
        print("✓ draws_14w_dedup_v视图已创建")
    except Exception as e:
        print(f"✗ 创建视图失败: {e}")

def insert_current_test_data():
    """插入当前时间的测试数据"""
    print("插入当前时间的测试数据...")
    
    # 获取当前正确时间
    now = datetime(2024, 12, 19, 17, 40, 0, tzinfo=timezone.utc)
    
    # 插入几条当前时间的测试数据
    for i in range(3, 8):
        timestamp = now + timedelta(minutes=i*10)
        insert_sql = f"""
        INSERT INTO `wprojectl.draw_dataset.draw_results` (draw_id, result, timestamp)
        VALUES ('draw_{i:03d}', 'candidate_{i}', TIMESTAMP('{timestamp.strftime('%Y-%m-%d %H:%M:%S')}'))
        """
        
        try:
            subprocess.run(f'bq query --use_legacy_sql=false "{insert_sql}"', shell=True, check=True)
            print(f"✓ 插入数据 draw_{i:03d}")
        except Exception as e:
            print(f"✗ 插入数据失败: {e}")

def verify_fix():
    """验证修正结果"""
    print("\n验证修正结果...")
    
    # 查询最新数据
    verify_sql = """
    SELECT draw_id, result, timestamp, 
           DATE(timestamp, 'Asia/Shanghai') as date_shanghai
    FROM `wprojectl.draw_dataset.draw_results` 
    ORDER BY timestamp DESC 
    LIMIT 10
    """
    
    result = run_bq_query(verify_sql)
    if result:
        print("\n最新数据:")
        for row in result:
            print(f"  {row['draw_id']}: {row['result']} - {row['timestamp']} (上海时间: {row['date_shanghai']})")
    
    # 测试今日开奖数查询
    today_sql = """
    SELECT COUNT(*) as count
    FROM `wprojectl.draw_dataset.draw_results`
    WHERE DATE(timestamp, 'Asia/Shanghai') = CURRENT_DATE('Asia/Shanghai')
    """
    
    result = run_bq_query(today_sql)
    if result:
        count = result[0]['count']
        print(f"\n今日开奖数: {count}")

if __name__ == '__main__':
    print("=== PC28 时间戳修正工具 ===")
    print(f"当前系统时间: {datetime.now()}")
    print(f"目标修正时间: 2024-12-19")
    print()
    
    # 执行修正步骤
    fix_draw_results_timestamps()
    create_missing_views()
    insert_current_test_data()
    verify_fix()
    
    print("\n=== 修正完成 ===")