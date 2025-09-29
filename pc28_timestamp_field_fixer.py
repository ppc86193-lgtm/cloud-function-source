#!/usr/bin/env python3
"""
PC28时间戳字段修复系统
专门解决timestamp字段类型和DATE函数问题
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from google.cloud import bigquery

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PC28TimestampFieldFixer:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def analyze_timestamp_issues(self) -> Dict[str, Any]:
        """分析时间戳字段问题"""
        logger.info("🔍 分析时间戳字段问题...")
        
        analysis = {
            "timestamp": datetime.now().isoformat(),
            "field_types": {},
            "issues_found": []
        }
        
        # 检查cloud_pred_today_norm表的timestamp字段类型
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.cloud_pred_today_norm"
            table = self.client.get_table(table_ref)
            
            for field in table.schema:
                if field.name == "timestamp":
                    analysis["field_types"]["cloud_pred_today_norm_timestamp"] = field.field_type
                    
                    if field.field_type == "STRING":
                        analysis["issues_found"].append({
                            "issue_type": "timestamp_type_mismatch",
                            "description": "timestamp字段是STRING类型，但DATE函数需要TIMESTAMP类型",
                            "table": "cloud_pred_today_norm",
                            "field": "timestamp",
                            "current_type": "STRING",
                            "expected_type": "TIMESTAMP",
                            "fix_strategy": "cast_to_timestamp"
                        })
                        
        except Exception as e:
            logger.error(f"分析timestamp字段失败: {e}")
            
        return analysis
    
    def fix_timestamp_views(self) -> Dict[str, Any]:
        """修复时间戳相关视图"""
        logger.info("🔧 开始修复时间戳相关视图...")
        
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
            # 新的视图定义，正确处理STRING类型的timestamp
            new_view_sql = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.p_cloud_clean_merged_dedup_v` AS
            SELECT 
                period,
                PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) as ts_utc,  -- 将STRING转换为TIMESTAMP
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
            # 新的视图定义，正确处理DATE函数
            new_view_sql = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.p_cloud_today_v` AS
            SELECT 
                period,
                ts_utc,
                p_even,
                src
            FROM `{self.project_id}.{self.dataset_id}.p_cloud_clean_merged_dedup_v`
            WHERE DATE(ts_utc) = CURRENT_DATE('Asia/Shanghai')
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
        
        logger.info(f"🎯 时间戳修复完成: {total_successful}/{total_attempted} 成功")
        
        return fix_results
    
    def verify_timestamp_fixes(self) -> Dict[str, Any]:
        """验证时间戳修复效果"""
        logger.info("🔍 验证时间戳修复效果...")
        
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
                # 测试视图是否可访问并获取样本数据
                query = f"""
                SELECT 
                    COUNT(*) as count,
                    MIN(ts_utc) as min_ts,
                    MAX(ts_utc) as max_ts
                FROM `{self.project_id}.{self.dataset_id}.{view_name}`
                """
                result = self.client.query(query).result()
                row = list(result)[0]
                
                verification["view_tests"][view_name] = {
                    "accessible": True,
                    "row_count": row.count,
                    "min_timestamp": str(row.min_ts) if row.min_ts else None,
                    "max_timestamp": str(row.max_ts) if row.max_ts else None,
                    "healthy": row.count > 0
                }
                
                logger.info(f"✅ {view_name}: {row.count} 行, 时间范围: {row.min_ts} ~ {row.max_ts}")
                
            except Exception as e:
                verification["view_tests"][view_name] = {
                    "accessible": False,
                    "error": str(e),
                    "healthy": False
                }
                logger.error(f"❌ {view_name}: {e}")
        
        # 测试完整数据流
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
    
    def run_complete_timestamp_fix(self) -> Dict[str, Any]:
        """运行完整的时间戳修复流程"""
        logger.info("🚀 开始PC28时间戳字段修复...")
        
        complete_results = {
            "fix_timestamp": self.timestamp,
            "analysis": {},
            "fixes": {},
            "verification": {},
            "overall_success": False
        }
        
        # 1. 分析问题
        complete_results["analysis"] = self.analyze_timestamp_issues()
        
        # 2. 修复问题
        complete_results["fixes"] = self.fix_timestamp_views()
        
        # 3. 验证修复
        complete_results["verification"] = self.verify_timestamp_fixes()
        
        # 4. 计算整体成功
        complete_results["overall_success"] = (
            complete_results["fixes"]["overall_success"] and
            complete_results["verification"]["overall_health"]
        )
        
        # 5. 生成报告
        self._generate_timestamp_fix_report(complete_results)
        
        return complete_results
    
    def _generate_timestamp_fix_report(self, results: Dict[str, Any]):
        """生成时间戳修复报告"""
        report_path = f"/Users/a606/cloud_function_source/pc28_timestamp_fix_report_{self.timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 时间戳修复报告已保存: {report_path}")

def main():
    """主函数"""
    fixer = PC28TimestampFieldFixer()
    
    print("🔧 PC28时间戳字段修复系统")
    print("=" * 50)
    print("🎯 目标：解决timestamp字段类型和DATE函数问题")
    print("📋 修复范围：p_cloud_clean_merged_dedup_v, p_cloud_today_v")
    print("=" * 50)
    
    # 运行完整修复
    results = fixer.run_complete_timestamp_fix()
    
    # 输出结果
    analysis = results["analysis"]
    fixes = results["fixes"]
    verification = results["verification"]
    
    print(f"\n📊 分析结果:")
    print(f"  发现问题: {len(analysis['issues_found'])} 个")
    for issue in analysis['issues_found']:
        print(f"    - {issue['description']}")
    
    print(f"\n🔧 修复结果:")
    print(f"  修复尝试: {len(fixes['fixes_attempted'])} 个")
    print(f"  修复成功: {len(fixes['fixes_successful'])} 个")
    print(f"  修复失败: {len(fixes['fixes_failed'])} 个")
    
    if fixes['fixes_failed']:
        print(f"  失败详情:")
        for failed in fixes['fixes_failed']:
            print(f"    - {failed['view']}: {failed['error'][:100]}...")
    
    print(f"\n🔍 验证结果:")
    healthy_views = len([v for v in verification['view_tests'].values() if v.get('healthy', False)])
    total_views = len(verification['view_tests'])
    print(f"  视图健康: {healthy_views}/{total_views}")
    
    for view_name, test in verification['view_tests'].items():
        if test.get('healthy', False):
            print(f"    ✅ {view_name}: {test['row_count']} 行")
        else:
            print(f"    ❌ {view_name}: {test.get('error', 'Unknown error')[:50]}...")
    
    if verification.get('data_flow_test', {}).get('data_flow_healthy', False):
        signal_count = verification['data_flow_test']['signal_pool_count']
        candidates_count = verification['data_flow_test']['candidates_count']
        print(f"  数据流: ✅ 信号池 {signal_count} 行, 决策候选 {candidates_count} 行")
    else:
        print(f"  数据流: ❌ 异常")
    
    if results["overall_success"]:
        print(f"\n🎉 时间戳修复完成！")
        print(f"💡 所有时间戳字段问题已解决，数据流恢复正常")
    else:
        print(f"\n⚠️ 时间戳修复未完全成功，请检查详细报告")
    
    return results

if __name__ == "__main__":
    main()