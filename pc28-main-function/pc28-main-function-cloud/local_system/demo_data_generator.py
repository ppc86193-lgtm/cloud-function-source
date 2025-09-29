#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28演示数据生成器
生成模拟数据以展示系统功能
"""

import sys
import os
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any

# 添加路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_database import LocalDatabase

class DemoDataGenerator:
    """演示数据生成器"""
    
    def __init__(self):
        """初始化生成器"""
        self.db = LocalDatabase()
        
    def generate_cloud_pred_data(self, count: int = 50) -> List[Dict]:
        """生成云端预测数据"""
        data = []
        base_time = datetime.now()
        
        markets = ['pc28']
        picks = ['big', 'small', 'odd', 'even']
        sources = ['model_v1', 'model_v2', 'ensemble']
        
        for i in range(count):
            # 生成时间序列
            timestamp = base_time - timedelta(minutes=i*5)
            period = f"{timestamp.strftime('%Y%m%d')}{str(i+1).zfill(3)}"
            
            data.append({
                'draw_id': f"draw_{timestamp.strftime('%Y%m%d')}_{str(i+1).zfill(3)}",
                'timestamp': timestamp.isoformat(),
                'period': period,
                'market': random.choice(markets),
                'pick': random.choice(picks),
                'p_win': round(random.uniform(0.45, 0.75), 4),
                'source': random.choice(sources),
                'data_date': timestamp.strftime('%Y-%m-%d')
            })
        
        return data
    
    def generate_map_data(self, count: int = 40) -> List[Dict]:
        """生成映射数据"""
        data = []
        base_time = datetime.now()
        
        picks = ['odd', 'even']
        sources = ['map_model_v1', 'map_ensemble']
        
        for i in range(count):
            timestamp = base_time - timedelta(minutes=i*6)
            period = f"{timestamp.strftime('%Y%m%d')}{str(i+1).zfill(3)}"
            
            data.append({
                'draw_id': f"draw_{timestamp.strftime('%Y%m%d')}_{str(i+1).zfill(3)}",
                'timestamp': timestamp.isoformat(),
                'period': period,
                'market': 'oe',
                'pick': random.choice(picks),
                'p_win': round(random.uniform(0.48, 0.72), 4),
                'source': random.choice(sources),
                'data_date': timestamp.strftime('%Y-%m-%d')
            })
        
        return data
    
    def generate_size_data(self, count: int = 40) -> List[Dict]:
        """生成大小数据"""
        data = []
        base_time = datetime.now()
        
        picks = ['big', 'small']
        sources = ['size_model_v1', 'size_ensemble']
        
        for i in range(count):
            timestamp = base_time - timedelta(minutes=i*7)
            period = f"{timestamp.strftime('%Y%m%d')}{str(i+1).zfill(3)}"
            
            data.append({
                'draw_id': f"draw_{timestamp.strftime('%Y%m%d')}_{str(i+1).zfill(3)}",
                'timestamp': timestamp.isoformat(),
                'period': period,
                'market': 'size',
                'pick': random.choice(picks),
                'p_win': round(random.uniform(0.46, 0.74), 4),
                'source': random.choice(sources),
                'data_date': timestamp.strftime('%Y-%m-%d')
            })
        
        return data
    
    def insert_demo_data(self):
        """插入演示数据到数据库"""
        try:
            print("开始生成演示数据...")
            
            # 生成云端预测数据
            print("生成云端预测数据...")
            cloud_data = self.generate_cloud_pred_data(50)
            for record in cloud_data:
                self.db.execute_update("""
                    INSERT OR REPLACE INTO cloud_pred_today_norm 
                    (draw_id, timestamp, period, market, pick, p_win, source, data_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, tuple(record.values()))
            
            # 生成映射数据
            print("生成映射数据...")
            map_data = self.generate_map_data(40)
            for record in map_data:
                self.db.execute_update("""
                    INSERT OR REPLACE INTO p_map_clean_merged_dedup_v 
                    (draw_id, timestamp, period, market, pick, p_win, source, data_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, tuple(record.values()))
            
            # 生成大小数据
            print("生成大小数据...")
            size_data = self.generate_size_data(40)
            for record in size_data:
                self.db.execute_update("""
                    INSERT OR REPLACE INTO p_size_clean_merged_dedup_v 
                    (draw_id, timestamp, period, market, pick, p_win, source, data_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, tuple(record.values()))
            
            print("演示数据生成完成！")
            
            # 显示统计信息
            self.show_data_stats()
            
            return True
            
        except Exception as e:
            print(f"生成演示数据失败: {e}")
            return False
    
    def show_data_stats(self):
        """显示数据统计信息"""
        try:
            tables = [
                'cloud_pred_today_norm',
                'p_map_clean_merged_dedup_v', 
                'p_size_clean_merged_dedup_v'
            ]
            
            print("\n数据统计信息:")
            print("-" * 50)
            
            for table in tables:
                count = self.db.get_table_count(table)
                today_count = self.db.get_table_count(table, f"data_date = '{datetime.now().strftime('%Y-%m-%d')}'")
                
                print(f"{table}:")
                print(f"  总记录数: {count}")
                print(f"  今日记录数: {today_count}")
                
                # 显示最新几条记录
                recent_records = self.db.execute_query(f"""
                    SELECT draw_id, market, pick, p_win, source 
                    FROM {table} 
                    ORDER BY timestamp DESC 
                    LIMIT 3
                """)
                
                if recent_records:
                    print("  最新记录:")
                    for record in recent_records:
                        print(f"    {record['draw_id']} | {record['market']} | {record['pick']} | {record['p_win']} | {record['source']}")
                print()
            
        except Exception as e:
            print(f"显示统计信息失败: {e}")
    
    def generate_signal_pool_data(self):
        """生成信号池数据"""
        try:
            print("生成信号池数据...")
            
            # 从现有数据生成信号池
            base_time = datetime.now()
            
            # 生成一些信号池记录
            for i in range(20):
                timestamp = base_time - timedelta(minutes=i*10)
                draw_id = f"draw_{timestamp.strftime('%Y%m%d')}_{str(i+1).zfill(3)}"
                
                # 随机选择市场和预测
                markets = ['oe', 'size']
                picks_map = {
                    'oe': ['odd', 'even'],
                    'size': ['big', 'small']
                }
                
                market = random.choice(markets)
                pick = random.choice(picks_map[market])
                
                # 中文映射
                pick_zh_map = {
                    'odd': '奇', 'even': '偶',
                    'big': '大', 'small': '小'
                }
                
                self.db.execute_update("""
                    INSERT OR REPLACE INTO signal_pool_union_v3 
                    (draw_id, ts_utc, period, market, pick, p_win, source, vote_ratio, pick_zh, day_id_cst)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    draw_id,
                    timestamp.isoformat(),
                    f"{timestamp.strftime('%Y%m%d')}{str(i+1).zfill(3)}",
                    market,
                    pick,
                    round(random.uniform(0.55, 0.75), 4),
                    'signal_generator',
                    1.0,
                    pick_zh_map[pick],
                    timestamp.strftime('%Y%m%d')
                ))
            
            print("信号池数据生成完成！")
            return True
            
        except Exception as e:
            print(f"生成信号池数据失败: {e}")
            return False
    
    def generate_candidates_data(self):
        """生成决策候选数据"""
        try:
            print("生成决策候选数据...")
            
            base_time = datetime.now()
            
            for i in range(15):
                timestamp = base_time - timedelta(minutes=i*15)
                draw_id = f"draw_{timestamp.strftime('%Y%m%d')}_{str(i+1).zfill(3)}"
                
                markets = ['oe', 'size']
                picks_map = {
                    'oe': ['odd', 'even'],
                    'size': ['big', 'small']
                }
                
                market = random.choice(markets)
                pick = random.choice(picks_map[market])
                p_win = round(random.uniform(0.58, 0.78), 4)
                
                # 计算EV和Kelly
                ev = round((p_win * 2.0 - 1.0) * p_win, 4)
                kelly_frac = round(min((p_win * 2.0 - 1.0) / 1.0, 0.05), 4)
                
                pick_zh_map = {
                    'odd': '奇', 'even': '偶',
                    'big': '大', 'small': '小'
                }
                
                candidate_id = f"{draw_id}_{market}_{pick}_{int(timestamp.timestamp())}"
                
                self.db.execute_update("""
                    INSERT OR REPLACE INTO lab_push_candidates_v2 
                    (id, created_at, ts_utc, period, market, pick, p_win, ev, kelly_frac, 
                     source, vote_ratio, pick_zh, day_id_cst, draw_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    candidate_id,
                    timestamp.isoformat(),
                    timestamp.isoformat(),
                    f"{timestamp.strftime('%Y%m%d')}{str(i+1).zfill(3)}",
                    market,
                    pick,
                    p_win,
                    ev,
                    kelly_frac,
                    'candidate_generator',
                    1.0,
                    pick_zh_map[pick],
                    timestamp.strftime('%Y%m%d'),
                    draw_id
                ))
            
            print("决策候选数据生成完成！")
            return True
            
        except Exception as e:
            print(f"生成决策候选数据失败: {e}")
            return False

def main():
    """主函数"""
    print("PC28演示数据生成器")
    print("=" * 50)
    
    generator = DemoDataGenerator()
    
    # 生成基础数据
    if not generator.insert_demo_data():
        print("基础数据生成失败")
        return 1
    
    # 生成信号池数据
    if not generator.generate_signal_pool_data():
        print("信号池数据生成失败")
        return 1
    
    # 生成决策候选数据
    if not generator.generate_candidates_data():
        print("决策候选数据生成失败")
        return 1
    
    print("\n🎉 所有演示数据生成完成！")
    print("现在可以运行系统测试来验证功能")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)