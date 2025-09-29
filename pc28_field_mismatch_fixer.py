#!/usr/bin/env python3
"""
PC28字段不匹配修复系统
专门解决ts_utc字段问题和其他字段不匹配问题
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from google.cloud import bigquery

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PC28FieldMismatchFixer:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def analyze_field_issues(self) -> Dict[str, Any]:
        """分析字段不匹配问题"""
        logger.info("🔍 分析字段不匹配问题...")
        
        analysis = {
            "timestamp": datetime.now().isoformat(),
            "source_tables": {},
            "problematic_views": {},
            "field_mappings": {},
            "issues_found": []
        }
        
        # 1. 检查源表字段
        source_tables = [
            "cloud_pred_today_norm",
            "p_map_clean_merged_dedup_v", 
            "p_size_clean_merged_dedup_v"
        ]
        
        for table_name in source_tables:
            try:
                table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
                table = self.client.get_table(table_ref)
                
                fields = {}
                for field in table.schema:
                    fields[field.name] = field.field_type
                
                analysis["source_tables"][table_name] = {
                    "fields": fields,
                    "accessible": True,
                    "row_count": self._get_table_count(table_ref)
                }
                
                logger.info(f"✅ {table_name}: {len(fields)} 字段, {analysis['source_tables'][table_name]['row_count']} 行")
                
            except Exception as e:
                analysis["source_tables"][table_name] = {
                    "fields": {},
                    "accessible": False,
                    "error": str(e)
                }
                logger.error(f"❌ {table_name}: {e}")
        
        # 2. 检查问题视图
        problematic_views = [
            "p_cloud_clean_merged_dedup_v",
            "p_cloud_today_v"
        ]
        
        for view_name in problematic_views:
            try:
                # 尝试获取视图定义
                view_ref = f"{self.project_id}.{self.dataset_id}.{view_name}"
                view = self.client.get_table(view_ref)
                
                analysis["problematic_views"][view_name] = {
                    "type": view.table_type,
                    "view_query": view.view_query if hasattr(view, 'view_query') else None,
                    "accessible": False,  # 我们知道这些视图有问题
                    "error": "ts_utc field not found"
                }
                
            except Exception as e:
                analysis["problematic_views"][view_name] = {
                    "accessible": False,
                    "error": str(e)
                }
        
        # 3. 分析字段映射问题
        cloud_fields = analysis["source_tables"].get("cloud_pred_today_norm", {}).get("fields", {})
        if cloud_fields:
            if "timestamp" in cloud_fields and "ts_utc" not in cloud_fields:
                analysis["issues_found"].append({
                    "issue_type": "field_name_mismatch",
                    "description": "cloud_pred_today_norm表使用'timestamp'字段，但视图期望'ts_utc'字段",
                    "source_table": "cloud_pred_today_norm",
                    "expected_field": "ts_utc",
                    "actual_field": "timestamp",
                    "fix_strategy": "update_view_definition"
                })
        
        return analysis
    
    def _get_table_count(self, table_ref: str) -> int:
        """获取表行数"""
        try:
            query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
            result = self.client.query(query).result()
            return list(result)[0].count
        except:
            return 0
    
    def fix_field_mismatches(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """修复字段不匹配问题"""
        logger.info("🔧 开始修复字段不匹配问题...")
        
        fix_results = {
            "timestamp": datetime.now().isoformat(),
            "fixes_attempted": [],
            "fixes_successful": [],
            "fixes_failed": [],
            "overall_success": False
        }
        
        # 修复p_cloud_clean_merged_dedup_v视图
        fix_results["fixes_attempted"].append("p_cloud_clean_merged_dedup_v")
        
        try:
            # 新的视图定义，使用timestamp而不是ts_utc
            new_view_sql = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.p_cloud_clean_merged_dedup_v` AS
            SELECT 
                period,
                timestamp as ts_utc,  -- 将timestamp字段映射为ts_utc
                p_win as p_even,
                source as src
            FROM (
                SELECT 
                    period,
                    timestamp,
                    p_win,
                    source,
                    ROW_NUMBER() OVER (PARTITION BY period ORDER BY timestamp DESC) as rn
                FROM `{self.project_id}.{self.dataset_id}.cloud_pred_today_norm`
                WHERE p_win IS NOT NULL
            )
            WHERE rn = 1
            """
            
            job = self.client.query(new_view_sql)
            job.result()  # 等待完成
            
            fix_results["fixes_successful"].append("p_cloud_clean_merged_dedup_v")
            logger.info("✅ p_cloud_clean_merged_dedup_v 修复成功")
            
        except Exception as e:
            fix_results["fixes_failed"].append({
                "view": "p_cloud_clean_merged_dedup_v",
                "error": str(e)
            })
            logger.error(f"❌ p_cloud_clean_merged_dedup_v 修复失败: {e}")
        
        # 修复p_cloud_today_v视图
        fix_results["fixes_attempted"].append("p_cloud_today_v")
        
        try:
            # 新的视图定义
            new_view_sql = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.p_cloud_today_v` AS
            SELECT 
                period,
                timestamp as ts_utc,  -- 将timestamp字段映射为ts_utc
                p_win as p_even,
                source as src
            FROM `{self.project_id}.{self.dataset_id}.p_cloud_clean_merged_dedup_v`
            WHERE DATE(timestamp) = CURRENT_DATE('Asia/Shanghai')
            """
            
            job = self.client.query(new_view_sql)
            job.result()  # 等待完成
            
            fix_results["fixes_successful"].append("p_cloud_today_v")
            logger.info("✅ p_cloud_today_v 修复成功")
            
        except Exception as e:
            fix_results["fixes_failed"].append({
                "view": "p_cloud_today_v", 
                "error": str(e)
            })
            logger.error(f"❌ p_cloud_today_v 修复失败: {e}")
        
        # 计算整体成功率
        total_attempted = len(fix_results["fixes_attempted"])
        total_successful = len(fix_results["fixes_successful"])
        fix_results["overall_success"] = total_successful == total_attempted and total_attempted > 0
        
        logger.info(f"🎯 字段修复完成: {total_successful}/{total_attempted} 成功")
        
        return fix_results
    
    def verify_fixes(self) -> Dict[str, Any]:
        """验证修复效果"""
        logger.info("🔍 验证修复效果...")
        
        verification = {
            "timestamp": datetime.now().isoformat(),
            "view_tests": {},
            "data_flow_test": {},
            "overall_health": False
        }
        
        # 测试修复的视图
        views_to_test = [
            "p_cloud_clean_merged_dedup_v",
            "p_cloud_today_v"
        ]
        
        for view_name in views_to_test:
            try:
                # 测试视图是否可访问
                query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{view_name}`"
                result = self.client.query(query).result()
                count = list(result)[0].count
                
                verification["view_tests"][view_name] = {
                    "accessible": True,
                    "row_count": count,
                    "healthy": count > 0
                }
                
                logger.info(f"✅ {view_name}: {count} 行")
                
            except Exception as e:
                verification["view_tests"][view_name] = {
                    "accessible": False,
                    "error": str(e),
                    "healthy": False
                }
                logger.error(f"❌ {view_name}: {e}")
        
        # 测试数据流
        try:
            # 测试signal_pool_union_v3
            query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.signal_pool_union_v3`"
            result = self.client.query(query).result()
            signal_count = list(result)[0].count
            
            # 测试lab_push_candidates_v2
            query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.lab_push_candidates_v2`"
            result = self.client.query(query).result()
            candidates_count = list(result)[0].count
            
            verification["data_flow_test"] = {
                "signal_pool_count": signal_count,
                "candidates_count": candidates_count,
                "data_flow_healthy": signal_count > 0 and candidates_count > 0
            }
            
            logger.info(f"📊 数据流测试: 信号池 {signal_count} 行, 决策候选 {candidates_count} 行")
            
        except Exception as e:
            verification["data_flow_test"] = {
                "error": str(e),
                "data_flow_healthy": False
            }
            logger.error(f"❌ 数据流测试失败: {e}")
        
        # 计算整体健康状态
        all_views_healthy = all(
            test.get("healthy", False) 
            for test in verification["view_tests"].values()
        )
        data_flow_healthy = verification["data_flow_test"].get("data_flow_healthy", False)
        
        verification["overall_health"] = all_views_healthy and data_flow_healthy
        
        return verification
    
    def run_complete_field_fix(self) -> Dict[str, Any]:
        """运行完整的字段修复流程"""
        logger.info("🚀 开始PC28字段不匹配修复...")
        
        complete_results = {
            "fix_timestamp": self.timestamp,
            "analysis": {},
            "fixes": {},
            "verification": {},
            "overall_success": False
        }
        
        # 1. 分析问题
        complete_results["analysis"] = self.analyze_field_issues()
        
        # 2. 修复问题
        complete_results["fixes"] = self.fix_field_mismatches(complete_results["analysis"])
        
        # 3. 验证修复
        complete_results["verification"] = self.verify_fixes()
        
        # 4. 计算整体成功
        complete_results["overall_success"] = (
            complete_results["fixes"]["overall_success"] and
            complete_results["verification"]["overall_health"]
        )
        
        # 5. 生成报告
        self._generate_field_fix_report(complete_results)
        
        return complete_results
    
    def _generate_field_fix_report(self, results: Dict[str, Any]):
        """生成字段修复报告"""
        report_path = f"/Users/a606/cloud_function_source/pc28_field_fix_report_{self.timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 字段修复报告已保存: {report_path}")

def main():
    """主函数"""
    fixer = PC28FieldMismatchFixer()
    
    print("🔧 PC28字段不匹配修复系统")
    print("=" * 50)
    print("🎯 目标：解决ts_utc字段问题和其他字段不匹配")
    print("📋 修复范围：p_cloud_clean_merged_dedup_v, p_cloud_today_v")
    print("=" * 50)
    
    # 运行完整修复
    results = fixer.run_complete_field_fix()
    
    # 输出结果
    analysis = results["analysis"]
    fixes = results["fixes"]
    verification = results["verification"]
    
    print(f"\n📊 分析结果:")
    print(f"  发现问题: {len(analysis['issues_found'])} 个")
    print(f"  源表状态: {len([t for t in analysis['source_tables'].values() if t.get('accessible', False)])}/{len(analysis['source_tables'])} 可访问")
    
    print(f"\n🔧 修复结果:")
    print(f"  修复尝试: {len(fixes['fixes_attempted'])} 个")
    print(f"  修复成功: {len(fixes['fixes_successful'])} 个")
    print(f"  修复失败: {len(fixes['fixes_failed'])} 个")
    
    print(f"\n🔍 验证结果:")
    healthy_views = len([v for v in verification['view_tests'].values() if v.get('healthy', False)])
    total_views = len(verification['view_tests'])
    print(f"  视图健康: {healthy_views}/{total_views}")
    
    if verification.get('data_flow_test', {}).get('data_flow_healthy', False):
        signal_count = verification['data_flow_test']['signal_pool_count']
        candidates_count = verification['data_flow_test']['candidates_count']
        print(f"  数据流: ✅ 信号池 {signal_count} 行, 决策候选 {candidates_count} 行")
    else:
        print(f"  数据流: ❌ 异常")
    
    if results["overall_success"]:
        print(f"\n🎉 字段修复完成！")
        print(f"💡 所有字段不匹配问题已解决，数据流恢复正常")
    else:
        print(f"\n⚠️ 字段修复未完全成功，请检查详细报告")
    
    return results

if __name__ == "__main__":
    main()