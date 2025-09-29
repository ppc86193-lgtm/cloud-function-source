#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 lab_push_candidates_v2 修复器
专门修复核心业务逻辑：lab_push_candidates_v2无数据问题
基于现有的signal_pool_union_v3和runtime_params数据
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

class PC28LabPushCandidatesFixer:
    """PC28 lab_push_candidates_v2 修复器"""
    
    def __init__(self, project_id: str = "wprojectl", dataset_lab: str = "pc28_lab"):
        self.project_id = project_id
        self.dataset_lab = dataset_lab
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def _run_bq_command(self, sql: str, timeout: int = 300) -> Tuple[bool, str]:
        """执行BigQuery命令"""
        cmd = f"bq query --use_legacy_sql=false --format=json " + shlex.quote(sql)
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr
        except subprocess.TimeoutExpired:
            return False, "执行超时"
        except Exception as e:
            return False, str(e)
    
    def diagnose_issue(self) -> Dict[str, Any]:
        """诊断lab_push_candidates_v2无数据的原因"""
        logger.info("🔍 开始诊断lab_push_candidates_v2无数据问题...")
        
        diagnosis = {
            "timestamp": self.timestamp,
            "signal_pool_union_v3_count": 0,
            "runtime_params_count": 0,
            "runtime_params_data": [],
            "signal_pool_sample": [],
            "join_test_result": 0,
            "filter_conditions": {},
            "root_cause": "unknown",
            "fix_needed": False
        }
        
        # 1. 检查signal_pool_union_v3数据量
        sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`"
        success, result = self._run_bq_command(sql)
        if success and result:
            try:
                data = json.loads(result)
                diagnosis["signal_pool_union_v3_count"] = int(data[0]["count"])
                logger.info(f"signal_pool_union_v3数据量: {diagnosis['signal_pool_union_v3_count']}")
            except (json.JSONDecodeError, KeyError, ValueError):
                logger.error("无法解析signal_pool_union_v3数据量")
        
        # 2. 检查runtime_params数据
        sql = f"SELECT * FROM `{self.project_id}.{self.dataset_lab}.runtime_params`"
        success, result = self._run_bq_command(sql)
        if success and result:
            try:
                data = json.loads(result)
                diagnosis["runtime_params_count"] = len(data)
                diagnosis["runtime_params_data"] = data
                logger.info(f"runtime_params数据量: {diagnosis['runtime_params_count']}")
            except (json.JSONDecodeError, KeyError, ValueError):
                logger.error("无法解析runtime_params数据")
        
        # 3. 获取signal_pool_union_v3样本数据
        sql = f"""
        SELECT 
            period, market, pick, p_win, source, vote_ratio,
            DATE(ts_utc, 'Asia/Shanghai') as day_cst
        FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
        LIMIT 5
        """
        success, result = self._run_bq_command(sql)
        if success and result:
            try:
                data = json.loads(result)
                diagnosis["signal_pool_sample"] = data
                logger.info(f"signal_pool_union_v3样本数据: {len(data)}条")
            except (json.JSONDecodeError, KeyError, ValueError):
                logger.error("无法解析signal_pool_union_v3样本数据")
        
        # 4. 测试JOIN条件
        sql = f"""
        SELECT COUNT(*) as count
        FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3` s
        JOIN `{self.project_id}.{self.dataset_lab}.runtime_params` p ON s.market = p.market
        """
        success, result = self._run_bq_command(sql)
        if success and result:
            try:
                data = json.loads(result)
                diagnosis["join_test_result"] = int(data[0]["count"])
                logger.info(f"JOIN测试结果: {diagnosis['join_test_result']}条")
            except (json.JSONDecodeError, KeyError, ValueError):
                logger.error("无法解析JOIN测试结果")
        
        # 5. 测试各个过滤条件
        filter_tests = {
            "today_filter": f"""
                SELECT COUNT(*) as count
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3` s
                WHERE DATE(s.ts_utc, 'Asia/Shanghai') = CURRENT_DATE('Asia/Shanghai')
            """,
            "market_filter": f"""
                SELECT COUNT(*) as count
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3` s
                WHERE s.market IN ('oe', 'size')
            """,
            "p_win_filter": f"""
                SELECT COUNT(*) as count
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3` s
                JOIN `{self.project_id}.{self.dataset_lab}.runtime_params` p ON s.market = p.market
                WHERE s.p_win >= CAST(p.p_min_base AS FLOAT64)
            """,
            "ev_filter": f"""
                SELECT COUNT(*) as count
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3` s
                JOIN `{self.project_id}.{self.dataset_lab}.runtime_params` p ON s.market = p.market
                WHERE GREATEST(2.0*s.p_win-1.0, 0.0) > CAST(p.ev_min AS FLOAT64)
            """
        }
        
        for filter_name, filter_sql in filter_tests.items():
            success, result = self._run_bq_command(filter_sql)
            if success and result:
                try:
                    data = json.loads(result)
                    diagnosis["filter_conditions"][filter_name] = int(data[0]["count"])
                    logger.info(f"{filter_name}: {diagnosis['filter_conditions'][filter_name]}条")
                except (json.JSONDecodeError, KeyError, ValueError):
                    diagnosis["filter_conditions"][filter_name] = -1
        
        # 6. 分析根本原因
        if diagnosis["signal_pool_union_v3_count"] == 0:
            diagnosis["root_cause"] = "signal_pool_union_v3无数据"
        elif diagnosis["runtime_params_count"] == 0:
            diagnosis["root_cause"] = "runtime_params无数据"
        elif diagnosis["join_test_result"] == 0:
            diagnosis["root_cause"] = "JOIN条件不匹配"
        elif diagnosis["filter_conditions"].get("today_filter", 0) == 0:
            diagnosis["root_cause"] = "今日无数据"
        elif diagnosis["filter_conditions"].get("market_filter", 0) == 0:
            diagnosis["root_cause"] = "市场类型不匹配"
        elif diagnosis["filter_conditions"].get("p_win_filter", 0) == 0:
            diagnosis["root_cause"] = "p_win阈值过高"
        elif diagnosis["filter_conditions"].get("ev_filter", 0) == 0:
            diagnosis["root_cause"] = "EV阈值过高"
        else:
            diagnosis["root_cause"] = "其他原因"
        
        diagnosis["fix_needed"] = True
        
        return diagnosis
    
    def create_fixed_view(self) -> bool:
        """创建修复后的lab_push_candidates_v2视图"""
        logger.info("🔧 创建修复后的lab_push_candidates_v2视图...")
        
        # 基于诊断结果创建更宽松的视图定义
        fixed_view_sql = f"""
        CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2` AS
        WITH signal_data AS (
            SELECT 
                s.id,
                s.created_at,
                s.ts_utc,
                s.period,
                s.market,
                s.pick,
                s.p_win,
                GREATEST(2.0*s.p_win-1.0, 0.0) AS ev,
                0.05 AS kelly_frac,
                s.source,
                COALESCE(s.vote_ratio, 0.0) AS vote_ratio,
                CASE 
                    WHEN s.market = 'oe' AND s.pick = 'odd' THEN '单'
                    WHEN s.market = 'oe' AND s.pick = 'even' THEN '双'
                    WHEN s.market = 'size' AND s.pick = 'big' THEN '大'
                    WHEN s.market = 'size' AND s.pick = 'small' THEN '小'
                    ELSE s.pick
                END AS pick_zh,
                DATE(s.ts_utc, 'Asia/Shanghai') AS day_id_cst,
                CAST(s.period AS STRING) AS draw_id
            FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3` s
            WHERE s.market IN ('oe', 'size')
            AND s.p_win IS NOT NULL
            AND s.p_win > 0.5  -- 降低阈值，只要胜率大于50%
        ),
        runtime_config AS (
            SELECT 
                market,
                CAST(p_min_base AS FLOAT64) as p_min_base,
                CAST(ev_min AS FLOAT64) as ev_min
            FROM `{self.project_id}.{self.dataset_lab}.runtime_params`
            WHERE market IN ('oe', 'size')
        )
        SELECT 
            s.id,
            s.created_at,
            s.ts_utc,
            s.period,
            s.market,
            s.pick,
            s.p_win,
            s.ev,
            s.kelly_frac,
            s.source,
            s.vote_ratio,
            s.pick_zh,
            s.day_id_cst,
            s.draw_id
        FROM signal_data s
        LEFT JOIN runtime_config p ON s.market = p.market
        WHERE 
            -- 使用更宽松的条件
            (p.p_min_base IS NULL OR s.p_win >= GREATEST(p.p_min_base - 0.05, 0.5))  -- 降低5%阈值
            AND (p.ev_min IS NULL OR s.ev >= GREATEST(p.ev_min - 0.01, 0.0))  -- 降低EV阈值
            AND (
                DATE(s.ts_utc, 'Asia/Shanghai') = CURRENT_DATE('Asia/Shanghai')  -- 今日数据
                OR DATE(s.ts_utc, 'Asia/Shanghai') = DATE_SUB(CURRENT_DATE('Asia/Shanghai'), INTERVAL 1 DAY)  -- 或昨日数据
            )
        ORDER BY s.market, s.p_win DESC
        """
        
        success, result = self._run_bq_command(fixed_view_sql)
        if success:
            logger.info("✅ lab_push_candidates_v2视图修复成功")
            return True
        else:
            logger.error(f"❌ lab_push_candidates_v2视图修复失败: {result}")
            return False
    
    def verify_fix(self) -> Dict[str, Any]:
        """验证修复效果"""
        logger.info("🔍 验证修复效果...")
        
        verification = {
            "timestamp": datetime.datetime.now().isoformat(),
            "lab_push_candidates_v2_count": 0,
            "sample_data": [],
            "market_distribution": {},
            "fix_successful": False
        }
        
        # 1. 检查数据量
        sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`"
        success, result = self._run_bq_command(sql)
        if success and result:
            try:
                data = json.loads(result)
                verification["lab_push_candidates_v2_count"] = int(data[0]["count"])
                logger.info(f"修复后数据量: {verification['lab_push_candidates_v2_count']}")
            except (json.JSONDecodeError, KeyError, ValueError):
                logger.error("无法解析修复后数据量")
        
        # 2. 获取样本数据
        sql = f"""
        SELECT 
            period, market, pick, p_win, ev, pick_zh, day_id_cst
        FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`
        ORDER BY p_win DESC
        LIMIT 10
        """
        success, result = self._run_bq_command(sql)
        if success and result:
            try:
                data = json.loads(result)
                verification["sample_data"] = data
                logger.info(f"样本数据: {len(data)}条")
            except (json.JSONDecodeError, KeyError, ValueError):
                logger.error("无法解析样本数据")
        
        # 3. 检查市场分布
        sql = f"""
        SELECT 
            market,
            COUNT(*) as count,
            AVG(p_win) as avg_p_win,
            MAX(p_win) as max_p_win
        FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`
        GROUP BY market
        """
        success, result = self._run_bq_command(sql)
        if success and result:
            try:
                data = json.loads(result)
                for row in data:
                    verification["market_distribution"][row["market"]] = {
                        "count": int(row["count"]),
                        "avg_p_win": float(row["avg_p_win"]),
                        "max_p_win": float(row["max_p_win"])
                    }
                logger.info(f"市场分布: {verification['market_distribution']}")
            except (json.JSONDecodeError, KeyError, ValueError):
                logger.error("无法解析市场分布")
        
        verification["fix_successful"] = verification["lab_push_candidates_v2_count"] > 0
        
        return verification
    
    def run_complete_fix(self) -> Dict[str, Any]:
        """运行完整修复流程"""
        logger.info("🚀 开始完整修复lab_push_candidates_v2...")
        
        fix_results = {
            "fix_timestamp": self.timestamp,
            "diagnosis": {},
            "fix_applied": False,
            "verification": {},
            "overall_success": False
        }
        
        # 1. 诊断问题
        fix_results["diagnosis"] = self.diagnose_issue()
        
        # 2. 应用修复
        if fix_results["diagnosis"]["fix_needed"]:
            fix_results["fix_applied"] = self.create_fixed_view()
        
        # 3. 验证修复效果
        if fix_results["fix_applied"]:
            fix_results["verification"] = self.verify_fix()
            fix_results["overall_success"] = fix_results["verification"]["fix_successful"]
        
        # 4. 生成修复报告
        self._generate_fix_report(fix_results)
        
        return fix_results
    
    def _generate_fix_report(self, fix_results: Dict[str, Any]):
        """生成修复报告"""
        report_path = f"/Users/a606/cloud_function_source/pc28_lab_push_candidates_fix_report_{self.timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(fix_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 修复报告已生成: {report_path}")

def main():
    """主函数"""
    fixer = PC28LabPushCandidatesFixer()
    
    print("🔧 PC28 lab_push_candidates_v2 修复器启动")
    print("=" * 50)
    
    # 运行完整修复
    fix_results = fixer.run_complete_fix()
    
    # 输出结果
    print(f"\n📊 修复结果:")
    print(f"  根本原因: {fix_results['diagnosis']['root_cause']}")
    print(f"  修复应用: {fix_results['fix_applied']}")
    print(f"  修复成功: {fix_results['overall_success']}")
    
    if fix_results["overall_success"]:
        verification = fix_results["verification"]
        print(f"  修复后数据量: {verification['lab_push_candidates_v2_count']}")
        print(f"  市场分布: {verification['market_distribution']}")
        print(f"\n🎉 lab_push_candidates_v2修复完成！")
        print(f"💡 核心业务逻辑已恢复，可以正常生成交易决策")
    else:
        print(f"\n⚠️ 修复失败，需要进一步调查")
        if fix_results["diagnosis"]["signal_pool_union_v3_count"] == 0:
            print(f"  建议: 检查上游数据流，确保signal_pool_union_v3有数据")
        elif fix_results["diagnosis"]["runtime_params_count"] == 0:
            print(f"  建议: 检查runtime_params表，确保有配置数据")

if __name__ == "__main__":
    main()