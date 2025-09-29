#!/usr/bin/env python3
"""
填充示例数据到本地数据库表
为同步测试提供数据
"""

import sqlite3
import random
from datetime import datetime, timedelta
import json

def populate_sample_data():
    """填充示例数据到所有核心表"""
    
    conn = sqlite3.connect('pc28_data.db')
    cursor = conn.cursor()
    
    # 生成基础数据
    base_time = datetime.now() - timedelta(days=7)
    
    print("正在填充示例数据...")
    
    # 1. lab_push_candidates_v2
    print("填充 lab_push_candidates_v2 表...")
    for i in range(50):
        draw_id = f"draw_{20250929000 + i}"
        issue = f"20250929{i:03d}"
        numbers = f"{random.randint(0,9)},{random.randint(0,9)},{random.randint(0,9)}"
        sum_value = sum(int(x) for x in numbers.split(','))
        big_small = "大" if sum_value >= 14 else "小"
        odd_even = "奇" if sum_value % 2 == 1 else "偶"
        dragon_tiger = random.choice(["龙", "虎"])
        prediction_score = round(random.uniform(0.6, 0.95), 3)
        confidence_level = round(random.uniform(0.7, 0.9), 3)
        created_at = base_time + timedelta(minutes=i*3)
        
        cursor.execute("""
            INSERT OR REPLACE INTO lab_push_candidates_v2 
            (draw_id, issue, numbers, sum_value, big_small, odd_even, dragon_tiger, 
             prediction_score, confidence_level, algorithm_version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (draw_id, issue, numbers, sum_value, big_small, odd_even, dragon_tiger,
              prediction_score, confidence_level, "v2.1", created_at, created_at))
    
    # 2. cloud_pred_today_norm
    print("填充 cloud_pred_today_norm 表...")
    for i in range(40):
        draw_id = f"pred_{20250929000 + i}"
        issue = f"20250929{i:03d}"
        predicted_numbers = f"{random.randint(0,9)},{random.randint(0,9)},{random.randint(0,9)}"
        prediction_type = random.choice(["neural_network", "statistical", "hybrid"])
        confidence_score = round(random.uniform(0.65, 0.88), 3)
        normalization_factor = round(random.uniform(0.8, 1.2), 3)
        created_at = base_time + timedelta(minutes=i*4)
        
        cursor.execute("""
            INSERT OR REPLACE INTO cloud_pred_today_norm
            (draw_id, issue, predicted_numbers, prediction_type, confidence_score,
             normalization_factor, model_version, prediction_timestamp, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (draw_id, issue, predicted_numbers, prediction_type, confidence_score,
              normalization_factor, "cloud_v1.3", created_at, created_at, created_at))
    
    # 3. signal_pool_union_v3
    print("填充 signal_pool_union_v3 表...")
    for i in range(30):
        signal_id = f"signal_{i:04d}"
        signal_type = random.choice(["pattern", "frequency", "trend", "anomaly"])
        signal_strength = round(random.uniform(0.3, 0.9), 3)
        pattern_match = random.choice(["high", "medium", "low"])
        frequency_score = round(random.uniform(0.4, 0.8), 3)
        reliability_index = round(random.uniform(0.5, 0.95), 3)
        source_algorithm = random.choice(["algo_a", "algo_b", "algo_c"])
        last_seen = base_time + timedelta(minutes=i*5)
        
        cursor.execute("""
            INSERT OR REPLACE INTO signal_pool_union_v3
            (signal_id, signal_type, signal_strength, pattern_match, frequency_score,
             reliability_index, source_algorithm, last_seen, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (signal_id, signal_type, signal_strength, pattern_match, frequency_score,
              reliability_index, source_algorithm, last_seen, last_seen))
    
    # 4. p_size_clean_merged_dedup_v
    print("填充 p_size_clean_merged_dedup_v 表...")
    size_categories = ["small", "medium", "large", "extra_large"]
    for i, category in enumerate(size_categories):
        size_category = category
        category_name = f"分类_{category}"
        size_range = f"{i*5}-{(i+1)*5}"
        frequency_count = random.randint(10, 100)
        probability_score = round(random.uniform(0.1, 0.4), 3)
        trend_indicator = random.choice(["上升", "下降", "稳定"])
        last_updated = base_time + timedelta(hours=i)
        
        cursor.execute("""
            INSERT OR REPLACE INTO p_size_clean_merged_dedup_v
            (size_category, category_name, size_range, frequency_count, probability_score,
             trend_indicator, last_updated, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (size_category, category_name, size_range, frequency_count, probability_score,
              trend_indicator, last_updated, last_updated))
    
    # 5. draws_14w_dedup_v
    print("填充 draws_14w_dedup_v 表...")
    for i in range(60):
        draw_id = f"draw_14w_{20250929000 + i}"
        issue = f"20250929{i:03d}"
        numbers = f"{random.randint(0,9)},{random.randint(0,9)},{random.randint(0,9)}"
        sum_value = sum(int(x) for x in numbers.split(','))
        big_small = "大" if sum_value >= 14 else "小"
        odd_even = "奇" if sum_value % 2 == 1 else "偶"
        dragon_tiger = random.choice(["龙", "虎"])
        week_number = 39  # 2025年第39周
        day_of_week = (i % 7) + 1
        is_duplicate = random.choice([0, 1]) if i > 10 else 0
        dedup_hash = f"hash_{hash(numbers) % 10000}"
        created_at = base_time + timedelta(minutes=i*2)
        
        cursor.execute("""
            INSERT OR REPLACE INTO draws_14w_dedup_v
            (draw_id, issue, numbers, sum_value, big_small, odd_even, dragon_tiger,
             week_number, day_of_week, is_duplicate, dedup_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (draw_id, issue, numbers, sum_value, big_small, odd_even, dragon_tiger,
              week_number, day_of_week, is_duplicate, dedup_hash, created_at, created_at))
    
    # 6. score_ledger
    print("填充 score_ledger 表...")
    for i in range(45):
        draw_id = f"score_{20250929000 + i}"
        issue = f"20250929{i:03d}"
        actual_numbers = f"{random.randint(0,9)},{random.randint(0,9)},{random.randint(0,9)}"
        predicted_numbers = f"{random.randint(0,9)},{random.randint(0,9)},{random.randint(0,9)}"
        accuracy_score = round(random.uniform(0.4, 0.9), 3)
        prediction_method = random.choice(["ml_model", "statistical", "hybrid", "neural_net"])
        evaluation_date = base_time + timedelta(minutes=i*3)
        performance_metrics = json.dumps({
            "precision": round(random.uniform(0.6, 0.9), 3),
            "recall": round(random.uniform(0.5, 0.8), 3),
            "f1_score": round(random.uniform(0.55, 0.85), 3)
        })
        
        cursor.execute("""
            INSERT OR REPLACE INTO score_ledger
            (draw_id, issue, actual_numbers, predicted_numbers, accuracy_score,
             prediction_method, evaluation_date, performance_metrics, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (draw_id, issue, actual_numbers, predicted_numbers, accuracy_score,
              prediction_method, evaluation_date, performance_metrics, evaluation_date, evaluation_date))
    
    conn.commit()
    
    # 验证数据
    print("\n验证填充的数据:")
    tables = ['lab_push_candidates_v2', 'cloud_pred_today_norm', 'signal_pool_union_v3', 
              'p_size_clean_merged_dedup_v', 'draws_14w_dedup_v', 'score_ledger']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} 条记录")
    
    conn.close()
    print("\n✅ 示例数据填充完成!")

if __name__ == "__main__":
    populate_sample_data()