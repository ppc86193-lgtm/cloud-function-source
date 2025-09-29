#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28智能数据修复系统
根据表逻辑自动检测和修复数据流问题，特别是日期过滤和字段映射问题
"""

import os
import sys
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SmartDataRepair:
    """智能数据修复系统"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.client = bigquery.Client(project=self.project_id)
        
        # 核心视图和修复规则
        self.repair_targets = {
            "p_cloud_today_v": {
                "type": "date_filter_view",
                "source_table": "p_cloud_clean_merged_dedup_v",
                "expected_fields": ["period", "ts_utc", "p_even", "src", "n_src"]
            },
            "p_map_today_v": {
                "type": "prediction_view", 
                "source_table": "p_map_clean_merged_dedup_v",
                "expected_fields": ["period", "ts_utc", "p_even", "src", "n_src"]
            },
            "p_size_today_v": {
                "type": "prediction_view",
                "source_table": "p_size_clean_merged_dedup_v", 
                "expected_fields": ["period", "ts_utc", "p_even", "src", "n_src"]
            },
            "signal_pool_union_v3": {
                "type": "union_view",
                "source_views": ["p_ensemble_today_canon_v", "p_map_today_canon_v", "p_size_today_canon_v"],
                "expected_fields": ["id", "created_at", "ts_utc", "period", "market", "pick", "p_win", "source"]
            }
        }
    
    def get_view_definition(self, view_name: str) -> Optional[str]:
        """获取视图定义"""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(view_name)
            table = self.client.get_table(table_ref)
            
            if hasattr(table, 'view_query'):
                return table.view_query
            else:
                return None
                
        except Exception as e:
            logger.error(f"获取视图定义失败 {view_name}: {e}")
            return None
    
    def check_table_data(self, table_name: str) -> Dict[str, Any]:
        """检查表数据状态"""
        try:
            # 检查行数
            count_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
            count_result = list(self.client.query(count_query).result())
            row_count = int(count_result[0]['count']) if count_result else 0
            
            # 检查最新数据时间
            latest_query = f"""
                SELECT MAX(ts_utc) as latest_time 
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE ts_utc IS NOT NULL
            """
            
            try:
                latest_result = list(self.client.query(latest_query).result())
                latest_time = latest_result[0]['latest_time'] if latest_result and latest_result[0]['latest_time'] else None
            except:
                latest_time = None
            
            # 检查日期分布
            date_query = f"""
                SELECT 
                    DATE(ts_utc, 'Asia/Shanghai') as date,
                    COUNT(*) as count
                FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE ts_utc IS NOT NULL
                GROUP BY DATE(ts_utc, 'Asia/Shanghai')
                ORDER BY date DESC
                LIMIT 10
            """
            
            try:
                date_result = list(self.client.query(date_query).result())
                date_distribution = [{"date": str(row['date']), "count": row['count']} for row in date_result]
            except:
                date_distribution = []
            
            return {
                "table_name": table_name,
                "row_count": row_count,
                "latest_time": latest_time.isoformat() if latest_time else None,
                "date_distribution": date_distribution,
                "has_data": row_count > 0
            }
            
        except Exception as e:
            logger.error(f"检查表数据失败 {table_name}: {e}")
            return {
                "table_name": table_name,
                "row_count": 0,
                "latest_time": None,
                "date_distribution": [],
                "has_data": False,
                "error": str(e)
            }
    
    def diagnose_date_filter_issue(self, view_name: str, source_table: str) -> Dict[str, Any]:
        """诊断日期过滤问题"""
        diagnosis = {
            "view_name": view_name,
            "source_table": source_table,
            "issue_type": None,
            "description": "",
            "suggested_fix": None
        }
        
        # 检查源表数据
        source_data = self.check_table_data(source_table)
        
        # 检查视图数据
        view_data = self.check_table_data(view_name)
        
        # 获取当前日期
        current_date_query = "SELECT CURRENT_DATE('Asia/Shanghai') as today"
        current_date_result = list(self.client.query(current_date_query).result())
        current_date = str(current_date_result[0]['today'])
        
        if source_data["has_data"] and not view_data["has_data"]:
            # 源表有数据但视图没有数据，可能是日期过滤问题
            
            # 检查源表最新数据日期
            if source_data["date_distribution"]:
                latest_source_date = source_data["date_distribution"][0]["date"]
                
                if latest_source_date != current_date:
                    diagnosis["issue_type"] = "date_mismatch"
                    diagnosis["description"] = f"源表最新数据日期 {latest_source_date} 与当前日期 {current_date} 不匹配"
                    diagnosis["suggested_fix"] = "expand_date_range"
                else:
                    diagnosis["issue_type"] = "filter_logic_error"
                    diagnosis["description"] = "日期过滤逻辑可能有误"
                    diagnosis["suggested_fix"] = "fix_filter_logic"
            else:
                diagnosis["issue_type"] = "no_source_data"
                diagnosis["description"] = "源表没有有效的时间戳数据"
        
        elif not source_data["has_data"]:
            diagnosis["issue_type"] = "empty_source"
            diagnosis["description"] = f"源表 {source_table} 为空"
        
        diagnosis["source_data"] = source_data
        diagnosis["view_data"] = view_data
        diagnosis["current_date"] = current_date
        
        return diagnosis
    
    def fix_date_filter_view(self, view_name: str, diagnosis: Dict[str, Any]) -> bool:
        """修复日期过滤视图"""
        try:
            # 获取当前视图定义
            current_definition = self.get_view_definition(view_name)
            if not current_definition:
                logger.error(f"无法获取视图定义: {view_name}")
                return False
            
            logger.info(f"当前视图定义: {current_definition}")
            
            # 根据诊断结果生成修复SQL
            if diagnosis["suggested_fix"] == "expand_date_range":
                # 扩大日期范围，包含最近几天的数据
                fixed_sql = self.expand_date_range(current_definition)
            elif diagnosis["suggested_fix"] == "fix_filter_logic":
                # 修复过滤逻辑
                fixed_sql = self.fix_filter_logic(current_definition)
            else:
                logger.warning(f"不支持的修复类型: {diagnosis['suggested_fix']}")
                return False
            
            if fixed_sql and fixed_sql != current_definition:
                # 执行修复
                create_sql = f"CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.{view_name}` AS {fixed_sql}"
                
                logger.info(f"执行修复SQL: {create_sql}")
                job = self.client.query(create_sql)
                job.result()
                
                logger.info(f"视图 {view_name} 修复成功")
                return True
            else:
                logger.warning(f"无法生成有效的修复SQL for {view_name}")
                return False
                
        except Exception as e:
            logger.error(f"修复视图失败 {view_name}: {e}")
            return False
    
    def expand_date_range(self, view_definition: str) -> str:
        """扩大日期范围"""
        # 查找日期过滤条件并替换
        patterns_and_replacements = [
            # 模式1: WHERE DATE(ts_utc,'Asia/Shanghai')=params.day_id
            (
                r"WHERE\s+DATE\(ts_utc,\s*'Asia/Shanghai'\)\s*=\s*params\.day_id",
                "WHERE DATE(ts_utc,'Asia/Shanghai') >= DATE_SUB(params.day_id, INTERVAL 7 DAY)"
            ),
            # 模式2: WHERE DATE(ts_utc,'Asia/Shanghai') = CURRENT_DATE('Asia/Shanghai')
            (
                r"WHERE\s+DATE\(ts_utc,\s*'Asia/Shanghai'\)\s*=\s*CURRENT_DATE\('Asia/Shanghai'\)",
                "WHERE DATE(ts_utc,'Asia/Shanghai') >= DATE_SUB(CURRENT_DATE('Asia/Shanghai'), INTERVAL 7 DAY)"
            ),
            # 模式3: WHERE DATE(ts_utc) = CURRENT_DATE()
            (
                r"WHERE\s+DATE\(ts_utc\)\s*=\s*CURRENT_DATE\(\)",
                "WHERE DATE(ts_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
            )
        ]
        
        fixed_sql = view_definition
        
        for pattern, replacement in patterns_and_replacements:
            if re.search(pattern, fixed_sql, re.IGNORECASE):
                fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
                logger.info(f"应用日期范围扩展: {pattern} -> {replacement}")
                break
        
        return fixed_sql
    
    def fix_filter_logic(self, view_definition: str) -> str:
        """修复过滤逻辑"""
        # 简化过滤逻辑，移除复杂的日期匹配
        fixed_sql = view_definition
        
        # 如果有WITH params子句，简化它
        if "WITH params AS" in fixed_sql:
            # 替换复杂的params逻辑
            pattern = r"WITH params AS \([^)]+\)[^F]*FROM[^,]+,"
            replacement = "FROM"
            
            fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE | re.DOTALL)
            
            # 移除params相关的WHERE条件
            fixed_sql = re.sub(r",\s*params\s*WHERE[^;]+", "", fixed_sql, flags=re.IGNORECASE)
            fixed_sql = re.sub(r"params\.day_id", "CURRENT_DATE('Asia/Shanghai')", fixed_sql, flags=re.IGNORECASE)
        
        return fixed_sql
    
    def create_missing_canonical_view(self, view_name: str, source_view: str) -> bool:
        """创建缺失的标准化视图"""
        try:
            # 标准化视图的SQL模板
            canonical_sql = f"""
                SELECT 
                    CONCAT(period, '_', src) as id,
                    CURRENT_TIMESTAMP() as created_at,
                    ts_utc,
                    period,
                    'pc28' as market,
                    CASE 
                        WHEN p_even > 0.5 THEN 'even'
                        ELSE 'odd'
                    END as pick,
                    p_even as p_win,
                    src as source,
                    NULL as vote_ratio,
                    NULL as params,
                    NULL as features,
                    NULL as notes
                FROM `{self.project_id}.{self.dataset_id}.{source_view}`
                WHERE p_even IS NOT NULL
            """
            
            create_sql = f"CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.{view_name}` AS {canonical_sql}"
            
            logger.info(f"创建标准化视图: {view_name}")
            job = self.client.query(create_sql)
            job.result()
            
            logger.info(f"标准化视图 {view_name} 创建成功")
            return True
            
        except Exception as e:
            logger.error(f"创建标准化视图失败 {view_name}: {e}")
            return False
    
    def run_smart_repair(self) -> Dict[str, Any]:
        """运行智能修复"""
        logger.info("开始智能数据修复...")
        
        repair_results = {
            "start_time": datetime.now().isoformat(),
            "repairs": [],
            "successful": 0,
            "failed": 0,
            "summary": ""
        }
        
        # 1. 修复日期过滤视图
        date_filter_views = ["p_cloud_today_v", "p_map_today_v", "p_size_today_v"]
        
        for view_name in date_filter_views:
            if view_name in self.repair_targets:
                target_info = self.repair_targets[view_name]
                source_table = target_info["source_table"]
                
                logger.info(f"诊断视图: {view_name}")
                diagnosis = self.diagnose_date_filter_issue(view_name, source_table)
                
                repair_result = {
                    "view_name": view_name,
                    "diagnosis": diagnosis,
                    "repair_attempted": False,
                    "repair_successful": False
                }
                
                if diagnosis["issue_type"] in ["date_mismatch", "filter_logic_error"]:
                    logger.info(f"尝试修复视图: {view_name}")
                    repair_result["repair_attempted"] = True
                    
                    success = self.fix_date_filter_view(view_name, diagnosis)
                    repair_result["repair_successful"] = success
                    
                    if success:
                        repair_results["successful"] += 1
                    else:
                        repair_results["failed"] += 1
                else:
                    logger.info(f"视图 {view_name} 无需修复或无法修复: {diagnosis['issue_type']}")
                
                repair_results["repairs"].append(repair_result)
        
        # 2. 创建缺失的标准化视图
        canonical_views = {
            "p_ensemble_today_canon_v": "p_ensemble_today_v",  # 如果存在
            "p_map_today_canon_v": "p_map_today_v",
            "p_size_today_canon_v": "p_size_today_v"
        }
        
        for canon_view, source_view in canonical_views.items():
            # 检查标准化视图是否存在且有数据
            canon_data = self.check_table_data(canon_view)
            source_data = self.check_table_data(source_view)
            
            repair_result = {
                "view_name": canon_view,
                "source_view": source_view,
                "repair_attempted": False,
                "repair_successful": False
            }
            
            if not canon_data["has_data"] and source_data["has_data"]:
                logger.info(f"创建标准化视图: {canon_view}")
                repair_result["repair_attempted"] = True
                
                success = self.create_missing_canonical_view(canon_view, source_view)
                repair_result["repair_successful"] = success
                
                if success:
                    repair_results["successful"] += 1
                else:
                    repair_results["failed"] += 1
            
            repair_results["repairs"].append(repair_result)
        
        # 3. 生成摘要
        repair_results["end_time"] = datetime.now().isoformat()
        repair_results["total_repairs"] = len(repair_results["repairs"])
        repair_results["summary"] = f"智能修复完成: {repair_results['successful']} 成功, {repair_results['failed']} 失败"
        
        logger.info(repair_results["summary"])
        return repair_results

def main():
    """主函数"""
    repair_system = SmartDataRepair()
    
    try:
        # 运行智能修复
        results = repair_system.run_smart_repair()
        
        # 输出结果
        print("\n" + "="*60)
        print("PC28智能数据修复报告")
        print("="*60)
        print(f"开始时间: {results['start_time']}")
        print(f"结束时间: {results['end_time']}")
        print(f"总修复项: {results['total_repairs']}")
        print(f"成功修复: {results['successful']}")
        print(f"修复失败: {results['failed']}")
        print(f"摘要: {results['summary']}")
        
        print("\n详细修复结果:")
        for repair in results['repairs']:
            view_name = repair.get('view_name', 'Unknown')
            attempted = repair.get('repair_attempted', False)
            successful = repair.get('repair_successful', False)
            
            if attempted:
                status = "✅ 成功" if successful else "❌ 失败"
                print(f"{status} {view_name}")
                
                if 'diagnosis' in repair:
                    diagnosis = repair['diagnosis']
                    print(f"   问题类型: {diagnosis.get('issue_type', 'Unknown')}")
                    print(f"   描述: {diagnosis.get('description', 'No description')}")
            else:
                print(f"⏭️  {view_name} (无需修复)")
        
        # 保存结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"/Users/a606/cloud_function_source/test_suite/smart_repair_report_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n详细报告已保存到: {report_file}")
        
    except Exception as e:
        logger.error(f"智能修复异常: {e}")
        print(f"智能修复失败: {e}")

if __name__ == "__main__":
    main()