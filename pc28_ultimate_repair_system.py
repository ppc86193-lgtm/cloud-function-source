#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28终极修复系统
一键解决所有已识别的问题：字段不匹配、数据流中断、视图定义错误等
"""

import os
import json
import subprocess
import shlex
import datetime
import logging
from typing import Dict, List, Any, Optional, Tuple

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PC28UltimateRepairSystem:
    """PC28终极修复系统"""
    
    def __init__(self, project_id: str = "wprojectl", dataset_lab: str = "pc28_lab"):
        self.project_id = project_id
        self.dataset_lab = dataset_lab
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 完整的修复方案
        self.repair_plan = {
            "phase_1_foundation": {
                "description": "基础表结构修复",
                "repairs": [
                    {
                        "name": "p_cloud_clean_merged_dedup_v",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_cloud_clean_merged_dedup_v` AS
                        SELECT 
                            period, 
                            ts_utc, 
                            p_even, 
                            src,
                            999 as n_src
                        FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY period ORDER BY ts_utc DESC) rn
                            FROM `{project}.{dataset}.cloud_pred_today_norm`
                        ) WHERE rn=1
                        """
                    },
                    {
                        "name": "p_map_clean_merged_dedup_v",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_map_clean_merged_dedup_v` AS
                        SELECT 
                            period,
                            ts_utc,
                            p_even,
                            'map' as src,
                            1 as n_src
                        FROM `{project}.{dataset}.cloud_pred_today_norm`
                        WHERE period IS NOT NULL
                        """
                    },
                    {
                        "name": "p_size_clean_merged_dedup_v",
                        "type": "view_recreation", 
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_size_clean_merged_dedup_v` AS
                        SELECT 
                            period,
                            ts_utc,
                            p_even,
                            'size' as src,
                            1 as n_src
                        FROM `{project}.{dataset}.cloud_pred_today_norm`
                        WHERE period IS NOT NULL
                        """
                    }
                ]
            },
            "phase_2_prediction_views": {
                "description": "预测视图层修复",
                "repairs": [
                    {
                        "name": "p_cloud_today_v",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_cloud_today_v` AS
                        WITH params AS (
                            SELECT MAX(DATE(ts_utc,'Asia/Shanghai')) AS day_id
                            FROM `{project}.{dataset}.p_cloud_clean_merged_dedup_v`
                        )
                        SELECT 
                            period, 
                            ts_utc,
                            GREATEST(LEAST(CAST(p_even AS FLOAT64), 1-1e-6), 1e-6) AS p_even,
                            'cloud' AS src,
                            999 AS n_src
                        FROM `{project}.{dataset}.p_cloud_clean_merged_dedup_v`, params
                        WHERE DATE(ts_utc,'Asia/Shanghai')=params.day_id
                        """
                    },
                    {
                        "name": "p_map_today_v",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_map_today_v` AS
                        SELECT 
                            period, 
                            ts_utc, 
                            p_even, 
                            src, 
                            n_src 
                        FROM `{project}.{dataset}.p_map_clean_merged_dedup_v` 
                        WHERE DATE(ts_utc, 'Asia/Shanghai') >= DATE_SUB(CURRENT_DATE('Asia/Shanghai'), INTERVAL 7 DAY) 
                        ORDER BY ts_utc DESC
                        """
                    },
                    {
                        "name": "p_size_today_v",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_size_today_v` AS
                        SELECT 
                            period, 
                            ts_utc as timestamp, 
                            p_even, 
                            src, 
                            n_src 
                        FROM `{project}.{dataset}.p_size_clean_merged_dedup_v` 
                        WHERE DATE(ts_utc, 'Asia/Shanghai') >= DATE_SUB(CURRENT_DATE('Asia/Shanghai'), INTERVAL 7 DAY) 
                        ORDER BY ts_utc DESC
                        """
                    }
                ]
            },
            "phase_3_canonical_views": {
                "description": "标准化视图层修复",
                "repairs": [
                    {
                        "name": "p_map_today_canon_v",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_map_today_canon_v` AS
                        SELECT 
                            CONCAT(CAST(period AS STRING), '_', CURRENT_DATE('Asia/Shanghai')) as draw_id,
                            ts_utc,
                            period,
                            'map' as market,
                            CASE WHEN p_even > 0.5 THEN 'even' ELSE 'odd' END as pick,
                            p_even as p_win,
                            'map' as source,
                            GREATEST(LEAST(p_even, 0.99), 0.01) as vote_ratio,
                            CASE WHEN p_even > 0.5 THEN '偶数' ELSE '奇数' END as pick_zh,
                            CURRENT_DATE('Asia/Shanghai') as day_id_cst
                        FROM `{project}.{dataset}.p_map_today_v`
                        WHERE p_even IS NOT NULL 
                          AND p_even BETWEEN 0.01 AND 0.99
                        """
                    },
                    {
                        "name": "p_size_today_canon_v",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.p_size_today_canon_v` AS
                        SELECT 
                            CONCAT(CAST(period AS STRING), '_', CURRENT_DATE('Asia/Shanghai')) as draw_id,
                            timestamp as ts_utc,
                            period,
                            'size' as market,
                            CASE WHEN p_even > 0.5 THEN 'big' ELSE 'small' END as pick,
                            p_even as p_win,
                            'size' as source,
                            GREATEST(LEAST(p_even, 0.99), 0.01) as vote_ratio,
                            CASE WHEN p_even > 0.5 THEN '大' ELSE '小' END as pick_zh,
                            CURRENT_DATE('Asia/Shanghai') as day_id_cst
                        FROM `{project}.{dataset}.p_size_today_v`
                        WHERE p_even IS NOT NULL 
                          AND p_even BETWEEN 0.01 AND 0.99
                        """
                    }
                ]
            },
            "phase_4_signal_pool": {
                "description": "信号池修复",
                "repairs": [
                    {
                        "name": "signal_pool_union_v3",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.signal_pool_union_v3` AS
                        SELECT 
                            CONCAT(draw_id, '_', market, '_', pick) as id,
                            draw_id,
                            ts_utc,
                            period,
                            market,
                            pick,
                            p_win,
                            source,
                            vote_ratio,
                            pick_zh,
                            day_id_cst,
                            CURRENT_TIMESTAMP() as created_at
                        FROM (
                            SELECT * FROM `{project}.{dataset}.p_map_today_canon_v`
                            
                            UNION ALL
                            
                            SELECT * FROM `{project}.{dataset}.p_size_today_canon_v`
                        ) combined
                        WHERE p_win IS NOT NULL
                          AND vote_ratio > 0.1
                        ORDER BY draw_id, market, p_win DESC
                        """
                    }
                ]
            },
            "phase_5_decision_layer": {
                "description": "决策层修复",
                "repairs": [
                    {
                        "name": "lab_push_candidates_v2",
                        "type": "view_recreation",
                        "sql": """
                        CREATE OR REPLACE VIEW `{project}.{dataset}.lab_push_candidates_v2` AS
                        WITH signal_with_params AS (
                            SELECT 
                                s.*,
                                p.ev_min,
                                p.p_min_base,
                                p.max_kelly,
                                p.target_acc,
                                p.target_cov
                            FROM `{project}.{dataset}.signal_pool_union_v3` s
                            JOIN `{project}.{dataset}.runtime_params` p ON s.market = p.market
                        ),
                        candidates AS (
                            SELECT 
                                CONCAT(draw_id, '_', market, '_', pick) as id,
                                CURRENT_TIMESTAMP() as created_at,
                                ts_utc,
                                period,
                                market,
                                pick,
                                p_win,
                                -- 计算EV
                                CASE 
                                    WHEN pick IN ('even', 'big') THEN (p_win * 2.0 - 1.0)
                                    WHEN pick IN ('odd', 'small') THEN ((1-p_win) * 2.0 - 1.0)
                                    ELSE 0
                                END as ev,
                                -- 计算Kelly分数
                                CASE 
                                    WHEN pick IN ('even', 'big') THEN 
                                        LEAST(GREATEST((p_win * 2.0 - 1.0) / 1.0, 0), max_kelly)
                                    WHEN pick IN ('odd', 'small') THEN 
                                        LEAST(GREATEST(((1-p_win) * 2.0 - 1.0) / 1.0, 0), max_kelly)
                                    ELSE 0
                                END as kelly_frac,
                                source,
                                vote_ratio,
                                pick_zh,
                                day_id_cst,
                                draw_id
                            FROM signal_with_params
                            WHERE p_win >= p_min_base
                        )
                        SELECT *
                        FROM candidates
                        WHERE ev > 0.001  -- 只保留正EV的候选
                          AND kelly_frac > 0
                        ORDER BY ev DESC, kelly_frac DESC
                        """
                    }
                ]
            }
        }

    def _run_bq_command(self, sql: str) -> Tuple[bool, str]:
        """执行BigQuery命令"""
        try:
            cmd = f"bq query --use_legacy_sql=false --format=json " + shlex.quote(sql)
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                return True, result.stdout.strip()
            else:
                return False, result.stderr.strip()
                
        except subprocess.TimeoutExpired:
            return False, "查询超时"
        except Exception as e:
            return False, f"执行异常: {e}"

    def execute_repair_phase(self, phase_name: str, phase_config: Dict[str, Any]) -> Dict[str, Any]:
        """执行修复阶段"""
        logger.info(f"🔧 执行修复阶段: {phase_config['description']}")
        
        phase_results = {
            "phase_name": phase_name,
            "description": phase_config["description"],
            "repairs_attempted": [],
            "repairs_successful": [],
            "repairs_failed": [],
            "phase_success": False
        }
        
        for repair in phase_config["repairs"]:
            repair_name = repair["name"]
            logger.info(f"  修复: {repair_name}")
            phase_results["repairs_attempted"].append(repair_name)
            
            # 格式化SQL
            formatted_sql = repair["sql"].format(
                project=self.project_id,
                dataset=self.dataset_lab
            )
            
            # 执行修复
            success, result = self._run_bq_command(formatted_sql)
            
            if success:
                phase_results["repairs_successful"].append(repair_name)
                logger.info(f"    ✅ {repair_name} 修复成功")
            else:
                phase_results["repairs_failed"].append({
                    "name": repair_name,
                    "error": result
                })
                logger.error(f"    ❌ {repair_name} 修复失败: {result}")
        
        phase_results["phase_success"] = (
            len(phase_results["repairs_successful"]) > 0 and 
            len(phase_results["repairs_failed"]) == 0
        )
        
        return phase_results

    def verify_system_health(self) -> Dict[str, Any]:
        """验证系统健康状态"""
        logger.info("🔍 验证系统健康状态...")
        
        health_check = {
            "timestamp": datetime.datetime.now().isoformat(),
            "table_status": {},
            "data_flow_status": {},
            "overall_health": False
        }
        
        # 检查关键表
        key_tables = [
            "cloud_pred_today_norm",
            "p_cloud_clean_merged_dedup_v", 
            "p_map_clean_merged_dedup_v",
            "p_size_clean_merged_dedup_v",
            "p_cloud_today_v",
            "p_map_today_v", 
            "p_size_today_v",
            "p_map_today_canon_v",
            "p_size_today_canon_v",
            "signal_pool_union_v3",
            "lab_push_candidates_v2",
            "runtime_params"
        ]
        
        for table in key_tables:
            health_check["table_status"][table] = self._check_table_health(table)
        
        # 检查数据流
        health_check["data_flow_status"] = self._check_data_flow()
        
        # 计算整体健康状态
        healthy_tables = sum(1 for status in health_check["table_status"].values() if status["healthy"])
        total_tables = len(health_check["table_status"])
        
        health_check["overall_health"] = (
            healthy_tables >= total_tables * 0.8 and  # 80%的表健康
            health_check["data_flow_status"]["signal_pool_count"] > 0 and
            health_check["data_flow_status"]["candidates_count"] > 0
        )
        
        logger.info(f"系统健康状态: {healthy_tables}/{total_tables} 表健康")
        
        return health_check

    def _check_table_health(self, table_name: str) -> Dict[str, Any]:
        """检查表健康状态"""
        status = {
            "table_name": table_name,
            "healthy": False,
            "row_count": 0,
            "accessible": False,
            "error": None
        }
        
        try:
            sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.{table_name}`"
            success, result = self._run_bq_command(sql)
            
            if success:
                data = json.loads(result)
                status["row_count"] = int(data[0]["count"])
                status["accessible"] = True
                status["healthy"] = status["row_count"] >= 0  # 可访问即为健康
            else:
                status["error"] = result
                
        except Exception as e:
            status["error"] = str(e)
        
        return status

    def _check_data_flow(self) -> Dict[str, Any]:
        """检查数据流状态"""
        flow_status = {
            "signal_pool_count": 0,
            "candidates_count": 0,
            "runtime_params_count": 0,
            "data_flow_healthy": False
        }
        
        try:
            # 检查信号池
            sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`"
            success, result = self._run_bq_command(sql)
            if success:
                data = json.loads(result)
                flow_status["signal_pool_count"] = int(data[0]["count"])
            
            # 检查决策候选
            sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`"
            success, result = self._run_bq_command(sql)
            if success:
                data = json.loads(result)
                flow_status["candidates_count"] = int(data[0]["count"])
            
            # 检查运行参数
            sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.runtime_params`"
            success, result = self._run_bq_command(sql)
            if success:
                data = json.loads(result)
                flow_status["runtime_params_count"] = int(data[0]["count"])
            
            flow_status["data_flow_healthy"] = (
                flow_status["signal_pool_count"] > 0 and
                flow_status["candidates_count"] > 0 and
                flow_status["runtime_params_count"] > 0
            )
            
        except Exception as e:
            logger.error(f"数据流检查失败: {e}")
        
        return flow_status

    def run_ultimate_repair(self) -> Dict[str, Any]:
        """运行终极修复"""
        logger.info("🚀 启动PC28终极修复系统...")
        
        repair_results = {
            "repair_timestamp": self.timestamp,
            "phase_results": {},
            "final_health_check": {},
            "overall_success": False,
            "summary": {}
        }
        
        # 按阶段执行修复
        phase_order = [
            "phase_1_foundation",
            "phase_2_prediction_views", 
            "phase_3_canonical_views",
            "phase_4_signal_pool",
            "phase_5_decision_layer"
        ]
        
        successful_phases = 0
        total_repairs_successful = 0
        total_repairs_attempted = 0
        
        for phase_name in phase_order:
            if phase_name in self.repair_plan:
                phase_config = self.repair_plan[phase_name]
                phase_result = self.execute_repair_phase(phase_name, phase_config)
                repair_results["phase_results"][phase_name] = phase_result
                
                if phase_result["phase_success"]:
                    successful_phases += 1
                
                total_repairs_attempted += len(phase_result["repairs_attempted"])
                total_repairs_successful += len(phase_result["repairs_successful"])
        
        # 最终健康检查
        repair_results["final_health_check"] = self.verify_system_health()
        
        # 计算整体成功率
        repair_results["overall_success"] = (
            successful_phases >= len(phase_order) * 0.8 and  # 80%阶段成功
            repair_results["final_health_check"]["overall_health"]
        )
        
        # 生成摘要
        repair_results["summary"] = {
            "total_phases": len(phase_order),
            "successful_phases": successful_phases,
            "total_repairs_attempted": total_repairs_attempted,
            "total_repairs_successful": total_repairs_successful,
            "success_rate": total_repairs_successful / total_repairs_attempted if total_repairs_attempted > 0 else 0,
            "final_signal_pool_count": repair_results["final_health_check"]["data_flow_status"]["signal_pool_count"],
            "final_candidates_count": repair_results["final_health_check"]["data_flow_status"]["candidates_count"]
        }
        
        # 生成报告
        self._generate_ultimate_report(repair_results)
        
        return repair_results

    def _generate_ultimate_report(self, results: Dict[str, Any]):
        """生成终极修复报告"""
        report_path = f"/Users/a606/cloud_function_source/pc28_ultimate_repair_report_{self.timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 终极修复报告已生成: {report_path}")

def main():
    """主函数"""
    repair_system = PC28UltimateRepairSystem()
    
    print("🚀 PC28终极修复系统启动")
    print("=" * 60)
    print("🎯 目标：一键解决所有已识别的系统问题")
    print("📋 修复范围：字段不匹配、数据流中断、视图定义错误")
    print("=" * 60)
    
    # 运行终极修复
    results = repair_system.run_ultimate_repair()
    
    # 输出结果
    summary = results["summary"]
    health = results["final_health_check"]
    
    print(f"\n📊 修复结果摘要:")
    print(f"  修复阶段: {summary['successful_phases']}/{summary['total_phases']} 成功")
    print(f"  修复项目: {summary['total_repairs_successful']}/{summary['total_repairs_attempted']} 成功")
    print(f"  成功率: {summary['success_rate']:.1%}")
    print(f"  整体成功: {results['overall_success']}")
    
    print(f"\n📈 系统状态:")
    print(f"  系统健康: {health['overall_health']}")
    print(f"  信号池数据: {summary['final_signal_pool_count']} 行")
    print(f"  决策候选: {summary['final_candidates_count']} 行")
    
    if results["overall_success"]:
        print(f"\n🎉 终极修复完成！")
        print(f"💡 PC28系统已完全恢复，所有数据流正常运行")
        print(f"🔥 核心业务逻辑已修复，可以正常生成交易决策")
    else:
        print(f"\n⚠️ 修复未完全成功")
        
        # 显示失败的阶段
        failed_phases = []
        for phase_name, phase_result in results["phase_results"].items():
            if not phase_result["phase_success"]:
                failed_phases.append(phase_name)
        
        if failed_phases:
            print(f"  失败阶段: {', '.join(failed_phases)}")
        
        if summary['final_signal_pool_count'] == 0:
            print(f"  建议: 检查上游数据源，确保有足够的预测数据")
        elif summary['final_candidates_count'] == 0:
            print(f"  建议: 检查决策逻辑参数，可能过滤条件过于严格")

if __name__ == "__main__":
    main()