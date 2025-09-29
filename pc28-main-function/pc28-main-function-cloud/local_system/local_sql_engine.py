#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28本地SQL引擎
重建BigQuery视图逻辑到本地SQLite，支持所有视图的本地化计算
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import pandas as pd

from local_database import get_local_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalSQLEngine:
    """本地SQL引擎"""
    
    def __init__(self):
        """初始化SQL引擎"""
        self.db = get_local_db()
        self.view_definitions = self._get_view_definitions()
    
    def _get_view_definitions(self) -> Dict[str, str]:
        """获取所有视图定义"""
        return {
            # 1. 预测视图层
            "p_cloud_today_v": """
                SELECT 
                    draw_id,
                    timestamp,
                    period,
                    market,
                    pick,
                    p_win,
                    source,
                    created_at,
                    data_date
                FROM cloud_pred_today_norm
                WHERE data_date = date('now')
                  AND market = 'pc28'
            """,
            
            "p_map_today_v": """
                SELECT 
                    c.draw_id,
                    c.timestamp,
                    c.period,
                    'oe' as market,
                    c.pick,
                    c.p_win,
                    c.source,
                    c.created_at,
                    c.data_date
                FROM p_map_clean_merged_dedup_v c
                INNER JOIN (
                    SELECT draw_id FROM cloud_pred_today_norm 
                    WHERE data_date = date('now') AND market = 'pc28'
                ) cloud ON c.draw_id = cloud.draw_id
                WHERE c.data_date = date('now')
                  AND c.market = 'oe'
            """,
            
            "p_size_today_v": """
                SELECT 
                    s.draw_id,
                    s.timestamp,
                    s.period,
                    'size' as market,
                    s.pick,
                    -- 应用自适应权重调整
                    CASE 
                        WHEN s.p_win > 0.6 THEN s.p_win * 0.95  -- 高概率降权
                        WHEN s.p_win < 0.4 THEN s.p_win * 1.05  -- 低概率提权
                        ELSE s.p_win
                    END as p_win,
                    s.source,
                    s.created_at,
                    s.data_date
                FROM p_size_clean_merged_dedup_v s
                INNER JOIN (
                    SELECT draw_id FROM cloud_pred_today_norm 
                    WHERE data_date = date('now') AND market = 'pc28'
                ) cloud ON s.draw_id = cloud.draw_id
                WHERE s.data_date = date('now')
                  AND s.market = 'size'
            """,
            
            # 2. 标准化视图层
            "p_map_today_canon_v": """
                SELECT 
                    draw_id,
                    datetime('now') as ts_utc,
                    period,
                    market,
                    pick,
                    p_win,
                    source,
                    1.0 as vote_ratio,
                    CASE 
                        WHEN pick = 'odd' THEN '奇'
                        WHEN pick = 'even' THEN '偶'
                        ELSE pick
                    END as pick_zh,
                    strftime('%Y%m%d', date('now')) as day_id_cst
                FROM (
                    SELECT * FROM p_map_today_v
                ) map_data
                WHERE p_win IS NOT NULL
            """,
            
            "p_size_today_canon_v": """
                SELECT 
                    draw_id,
                    datetime('now') as ts_utc,
                    period,
                    market,
                    pick,
                    p_win,
                    source,
                    1.0 as vote_ratio,
                    CASE 
                        WHEN pick = 'big' THEN '大'
                        WHEN pick = 'small' THEN '小'
                        ELSE pick
                    END as pick_zh,
                    strftime('%Y%m%d', date('now')) as day_id_cst
                FROM (
                    SELECT * FROM p_size_today_v
                ) size_data
                WHERE p_win IS NOT NULL
            """,
            
            # 3. 信号池联合视图
            "signal_pool_union_v3_view": """
                SELECT * FROM (
                    SELECT 
                        draw_id,
                        ts_utc,
                        period,
                        market,
                        pick,
                        p_win,
                        source,
                        vote_ratio,
                        pick_zh,
                        day_id_cst
                    FROM (
                        SELECT * FROM p_map_today_canon_v
                    )
                    
                    UNION ALL
                    
                    SELECT 
                        draw_id,
                        ts_utc,
                        period,
                        market,
                        pick,
                        p_win,
                        source,
                        vote_ratio,
                        pick_zh,
                        day_id_cst
                    FROM (
                        SELECT * FROM p_size_today_canon_v
                    )
                ) combined
                ORDER BY draw_id, market, pick
            """,
            
            # 4. 决策候选视图
            "lab_push_candidates_v2_view": """
                SELECT 
                    printf('%s_%s_%s_%s', s.draw_id, s.market, s.pick, strftime('%s', 'now')) as id,
                    datetime('now') as created_at,
                    s.ts_utc,
                    s.period,
                    s.market,
                    s.pick,
                    s.p_win,
                    -- 计算期望值 EV
                    CASE 
                        WHEN s.p_win > r.p_min_base THEN 
                            (s.p_win * 2.0 - 1.0) * s.p_win  -- 简化EV计算
                        ELSE 0.0
                    END as ev,
                    -- 计算Kelly分数
                    CASE 
                        WHEN s.p_win > r.p_min_base THEN 
                            MIN((s.p_win * 2.0 - 1.0) / 1.0, r.max_kelly)  -- 简化Kelly计算
                        ELSE 0.0
                    END as kelly_frac,
                    s.source,
                    s.vote_ratio,
                    s.pick_zh,
                    s.day_id_cst,
                    s.draw_id
                FROM (
                    SELECT * FROM signal_pool_union_v3_view
                ) s
                INNER JOIN runtime_params r ON s.market = r.market
                WHERE s.day_id_cst = strftime('%Y%m%d', date('now'))
                  AND s.p_win >= r.p_min_base
                  AND (s.p_win * 2.0 - 1.0) * s.p_win > r.ev_min  -- EV过滤
                  AND s.market IN ('oe', 'size')  -- 只处理oe和size市场
            """
        }
    
    def create_view(self, view_name: str) -> bool:
        """创建视图"""
        try:
            if view_name not in self.view_definitions:
                logger.error(f"未找到视图定义: {view_name}")
                return False
            
            # 删除已存在的视图
            self.db.execute_update(f"DROP VIEW IF EXISTS {view_name}")
            
            # 创建新视图
            view_sql = f"CREATE VIEW {view_name} AS {self.view_definitions[view_name]}"
            self.db.execute_update(view_sql)
            
            logger.info(f"视图创建成功: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建视图失败 {view_name}: {e}")
            return False
    
    def create_all_views(self) -> Dict[str, bool]:
        """创建所有视图"""
        results = {}
        
        # 按依赖顺序创建视图
        view_order = [
            'p_cloud_today_v',
            'p_map_today_v', 
            'p_size_today_v',
            'p_map_today_canon_v',
            'p_size_today_canon_v',
            'signal_pool_union_v3_view',
            'lab_push_candidates_v2_view'
        ]
        
        for view_name in view_order:
            results[view_name] = self.create_view(view_name)
        
        return results
    
    def refresh_signal_pool(self) -> bool:
        """刷新信号池表"""
        try:
            logger.info("开始刷新信号池...")
            
            # 清空现有信号池数据
            self.db.execute_update("DELETE FROM signal_pool_union_v3")
            
            # 从视图中获取最新数据
            signal_data = self.db.execute_query("""
                SELECT 
                    draw_id,
                    ts_utc,
                    period,
                    market,
                    pick,
                    p_win,
                    source,
                    vote_ratio,
                    pick_zh,
                    day_id_cst
                FROM signal_pool_union_v3_view
            """)
            
            if signal_data:
                # 插入新数据
                inserted = self.db.bulk_insert('signal_pool_union_v3', signal_data, replace=True)
                logger.info(f"信号池刷新成功: {inserted} 条记录")
                return True
            else:
                logger.warning("信号池视图无数据")
                return False
                
        except Exception as e:
            logger.error(f"信号池刷新失败: {e}")
            return False
    
    def refresh_candidates(self) -> bool:
        """刷新决策候选表"""
        try:
            logger.info("开始刷新决策候选...")
            
            # 清空现有候选数据
            self.db.execute_update("DELETE FROM lab_push_candidates_v2")
            
            # 从视图中获取最新数据
            candidate_data = self.db.execute_query("""
                SELECT 
                    id,
                    created_at,
                    ts_utc,
                    period,
                    market,
                    pick,
                    p_win,
                    ev,
                    kelly_frac,
                    source,
                    vote_ratio,
                    pick_zh,
                    day_id_cst,
                    draw_id
                FROM lab_push_candidates_v2_view
            """)
            
            if candidate_data:
                # 插入新数据
                inserted = self.db.bulk_insert('lab_push_candidates_v2', candidate_data, replace=True)
                logger.info(f"决策候选刷新成功: {inserted} 条记录")
                return True
            else:
                logger.warning("决策候选视图无数据")
                return False
                
        except Exception as e:
            logger.error(f"决策候选刷新失败: {e}")
            return False
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """运行完整数据管道"""
        try:
            logger.info("开始运行完整数据管道...")
            
            pipeline_results = {
                'timestamp': datetime.now().isoformat(),
                'steps': {},
                'success': True,
                'total_time_ms': 0
            }
            
            start_time = datetime.now()
            
            # 1. 创建所有视图
            logger.info("步骤1: 创建视图")
            view_results = self.create_all_views()
            pipeline_results['steps']['create_views'] = view_results
            
            if not all(view_results.values()):
                pipeline_results['success'] = False
                logger.error("视图创建失败")
            
            # 2. 刷新信号池
            logger.info("步骤2: 刷新信号池")
            signal_success = self.refresh_signal_pool()
            pipeline_results['steps']['refresh_signal_pool'] = signal_success
            
            if not signal_success:
                pipeline_results['success'] = False
            
            # 3. 刷新决策候选
            logger.info("步骤3: 刷新决策候选")
            candidate_success = self.refresh_candidates()
            pipeline_results['steps']['refresh_candidates'] = candidate_success
            
            if not candidate_success:
                pipeline_results['success'] = False
            
            # 4. 统计结果
            logger.info("步骤4: 统计结果")
            stats = self._get_pipeline_stats()
            pipeline_results['stats'] = stats
            
            end_time = datetime.now()
            pipeline_results['total_time_ms'] = int((end_time - start_time).total_seconds() * 1000)
            
            logger.info(f"数据管道运行完成: {'成功' if pipeline_results['success'] else '失败'}")
            return pipeline_results
            
        except Exception as e:
            logger.error(f"数据管道运行失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'success': False,
                'error': str(e)
            }
    
    def _get_pipeline_stats(self) -> Dict[str, Any]:
        """获取管道统计信息"""
        try:
            tables = [
                'cloud_pred_today_norm',
                'p_map_clean_merged_dedup_v',
                'p_size_clean_merged_dedup_v', 
                'signal_pool_union_v3',
                'lab_push_candidates_v2'
            ]
            
            stats = {}
            total_records = 0
            
            for table in tables:
                count = self.db.get_table_count(table)
                # 对于没有data_date字段的表，只获取总数
                if table in ['signal_pool_union_v3', 'lab_push_candidates_v2']:
                    today_count = count  # 这些表没有data_date字段
                else:
                    today_count = self.db.get_table_count(table, f"data_date = '{datetime.now().strftime('%Y-%m-%d')}'")
                
                stats[table] = {
                    'total_count': count,
                    'today_count': today_count,
                    'status': 'healthy' if count > 0 else 'empty'
                }
                total_records += count
            
            # 视图统计
            view_stats = {}
            views = ['p_cloud_today_v', 'p_map_today_v', 'p_size_today_v']
            
            for view in views:
                try:
                    count = len(self.db.execute_query(f"SELECT COUNT(*) as count FROM {view}"))
                    view_stats[view] = {'count': count}
                except:
                    view_stats[view] = {'count': 0, 'error': 'view_not_exists'}
            
            return {
                'tables': stats,
                'views': view_stats,
                'total_records': total_records,
                'pipeline_health': 'healthy' if total_records > 0 else 'empty'
            }
            
        except Exception as e:
            logger.error(f"获取管道统计失败: {e}")
            return {'error': str(e)}
    
    def test_view_queries(self) -> Dict[str, Any]:
        """测试所有视图查询"""
        results = {}
        
        for view_name in self.view_definitions.keys():
            try:
                # 测试查询
                test_query = f"SELECT COUNT(*) as count FROM {view_name} LIMIT 1"
                result = self.db.execute_query(test_query)
                
                results[view_name] = {
                    'status': 'success',
                    'count': result[0]['count'] if result else 0
                }
                
            except Exception as e:
                results[view_name] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return results
    
    def get_data_lineage(self) -> Dict[str, Any]:
        """获取数据血缘关系"""
        return {
            'raw_data_layer': {
                'tables': ['cloud_pred_today_norm', 'p_map_clean_merged_dedup_v', 'p_size_clean_merged_dedup_v'],
                'description': '原始API数据存储层'
            },
            'prediction_layer': {
                'views': ['p_cloud_today_v', 'p_map_today_v', 'p_size_today_v'],
                'dependencies': ['cloud_pred_today_norm', 'p_map_clean_merged_dedup_v', 'p_size_clean_merged_dedup_v'],
                'description': '预测数据视图层'
            },
            'canonical_layer': {
                'views': ['p_map_today_canon_v', 'p_size_today_canon_v'],
                'dependencies': ['p_map_today_v', 'p_size_today_v'],
                'description': '标准化数据层'
            },
            'signal_pool_layer': {
                'tables': ['signal_pool_union_v3'],
                'views': ['signal_pool_union_v3_view'],
                'dependencies': ['p_map_today_canon_v', 'p_size_today_canon_v'],
                'description': '信号池层'
            },
            'decision_layer': {
                'tables': ['lab_push_candidates_v2'],
                'views': ['lab_push_candidates_v2_view'],
                'dependencies': ['signal_pool_union_v3', 'runtime_params'],
                'description': '决策候选层'
            }
        }

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28本地SQL引擎')
    parser.add_argument('--action', choices=['create_views', 'refresh_signal', 'refresh_candidates', 'run_pipeline', 'test_views', 'lineage'], 
                       default='run_pipeline', help='执行动作')
    
    args = parser.parse_args()
    
    engine = LocalSQLEngine()
    
    if args.action == 'create_views':
        results = engine.create_all_views()
        print(f"视图创建结果: {json.dumps(results, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'refresh_signal':
        success = engine.refresh_signal_pool()
        print(f"信号池刷新: {'成功' if success else '失败'}")
    
    elif args.action == 'refresh_candidates':
        success = engine.refresh_candidates()
        print(f"决策候选刷新: {'成功' if success else '失败'}")
    
    elif args.action == 'run_pipeline':
        results = engine.run_full_pipeline()
        print(f"数据管道运行结果: {json.dumps(results, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'test_views':
        results = engine.test_view_queries()
        print(f"视图测试结果: {json.dumps(results, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'lineage':
        lineage = engine.get_data_lineage()
        print(f"数据血缘关系: {json.dumps(lineage, indent=2, ensure_ascii=False)}")

if __name__ == "__main__":
    main()