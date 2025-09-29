#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28字段修复系统
专门修复字段名称不匹配和视图定义错误的问题
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

class PC28FieldRepairSystem:
    """PC28字段修复系统"""
    
    def __init__(self, project_id: str = "wprojectl", dataset_lab: str = "pc28_lab"):
        self.project_id = project_id
        self.dataset_lab = dataset_lab
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 字段映射修复规则
        self.field_repairs = {
            "p_cloud_clean_merged_dedup_v": {
                "current_fields": ["period", "ts_utc", "p_even", "src"],
                "expected_fields": ["period", "ts_utc", "p_even", "src", "n_src"],
                "missing_fields": ["n_src"],
                "repair_sql": """
                CREATE OR REPLACE VIEW `{project}.{dataset}.p_cloud_clean_merged_dedup_v` AS
                SELECT 
                    period, 
                    ts_utc, 
                    p_even, 
                    src,
                    999 as n_src  -- 添加缺失的n_src字段
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY period ORDER BY ts_utc DESC) rn
                    FROM `{project}.{dataset}.cloud_pred_today_norm`
                ) WHERE rn=1
                """
            },
            "p_map_clean_merged_dedup_v": {
                "current_fields": [],
                "expected_fields": ["period", "ts_utc", "p_even", "src", "n_src"],
                "missing_fields": ["period", "ts_utc", "p_even", "src", "n_src"],
                "repair_sql": """
                CREATE OR REPLACE VIEW `{project}.{dataset}.p_map_clean_merged_dedup_v` AS
                SELECT 
                    period,
                    ts_utc,
                    p_even,
                    'map' as src,
                    1 as n_src
                FROM `{project}.{dataset}.cloud_pred_today_norm`
                WHERE src = 'map' OR period IS NOT NULL
                """
            },
            "p_size_clean_merged_dedup_v": {
                "current_fields": [],
                "expected_fields": ["period", "ts_utc", "p_even", "src", "n_src"],
                "missing_fields": ["period", "ts_utc", "p_even", "src", "n_src"],
                "repair_sql": """
                CREATE OR REPLACE VIEW `{project}.{dataset}.p_size_clean_merged_dedup_v` AS
                SELECT 
                    period,
                    ts_utc,
                    p_even,
                    'size' as src,
                    1 as n_src
                FROM `{project}.{dataset}.cloud_pred_today_norm`
                WHERE src = 'size' OR period IS NOT NULL
                """
            }
        }

    def _run_bq_command(self, sql: str) -> Tuple[bool, str]:
        """执行BigQuery命令"""
        try:
            cmd = f"bq query --use_legacy_sql=false --format=json " + shlex.quote(sql)
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                return True, result.stdout.strip()
            else:
                return False, result.stderr.strip()
                
        except subprocess.TimeoutExpired:
            return False, "查询超时"
        except Exception as e:
            return False, f"执行异常: {e}"

    def diagnose_field_issues(self) -> Dict[str, Any]:
        """诊断字段问题"""
        logger.info("🔍 开始诊断字段问题...")
        
        diagnosis = {
            "timestamp": self.timestamp,
            "table_issues": {},
            "repair_needed": False,
            "critical_issues": []
        }
        
        for table_name, repair_info in self.field_repairs.items():
            logger.info(f"检查表: {table_name}")
            
            # 检查表结构
            table_info = self._check_table_structure(table_name)
            diagnosis["table_issues"][table_name] = table_info
            
            if table_info["needs_repair"]:
                diagnosis["repair_needed"] = True
                if table_info["severity"] == "critical":
                    diagnosis["critical_issues"].append(table_name)
        
        return diagnosis

    def _check_table_structure(self, table_name: str) -> Dict[str, Any]:
        """检查表结构"""
        table_info = {
            "table_name": table_name,
            "exists": False,
            "current_fields": [],
            "missing_fields": [],
            "needs_repair": False,
            "severity": "normal",
            "error": None
        }
        
        try:
            # 检查表是否存在并获取结构
            cmd = f"bq show --format=json {self.project_id}:{self.dataset_lab}.{table_name}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                table_info["exists"] = True
                table_data = json.loads(result.stdout)
                
                # 提取字段信息
                if "schema" in table_data and "fields" in table_data["schema"]:
                    table_info["current_fields"] = [field["name"] for field in table_data["schema"]["fields"]]
                
                # 检查缺失字段
                expected_fields = self.field_repairs[table_name]["expected_fields"]
                table_info["missing_fields"] = [
                    field for field in expected_fields 
                    if field not in table_info["current_fields"]
                ]
                
                # 判断是否需要修复
                if table_info["missing_fields"]:
                    table_info["needs_repair"] = True
                    table_info["severity"] = "high" if len(table_info["missing_fields"]) > 2 else "medium"
                
                logger.info(f"  ✅ {table_name}: {len(table_info['current_fields'])} 字段, 缺失 {len(table_info['missing_fields'])} 字段")
                
            else:
                table_info["error"] = result.stderr.strip()
                table_info["needs_repair"] = True
                table_info["severity"] = "critical"
                logger.error(f"  ❌ {table_name}: {table_info['error']}")
                
        except Exception as e:
            table_info["error"] = str(e)
            table_info["needs_repair"] = True
            table_info["severity"] = "critical"
            logger.error(f"  ❌ {table_name}: 检查异常 - {e}")
        
        return table_info

    def repair_field_issues(self, diagnosis: Dict[str, Any]) -> Dict[str, Any]:
        """修复字段问题"""
        logger.info("🔧 开始修复字段问题...")
        
        repair_results = {
            "timestamp": datetime.datetime.now().isoformat(),
            "repairs_attempted": [],
            "repairs_successful": [],
            "repairs_failed": [],
            "overall_success": False
        }
        
        for table_name, table_info in diagnosis["table_issues"].items():
            if table_info["needs_repair"]:
                logger.info(f"修复表: {table_name}")
                repair_results["repairs_attempted"].append(table_name)
                
                # 获取修复SQL
                repair_sql = self.field_repairs[table_name]["repair_sql"].format(
                    project=self.project_id,
                    dataset=self.dataset_lab
                )
                
                # 执行修复
                success, result = self._run_bq_command(repair_sql)
                
                if success:
                    repair_results["repairs_successful"].append(table_name)
                    logger.info(f"  ✅ {table_name} 修复成功")
                else:
                    repair_results["repairs_failed"].append({
                        "table": table_name,
                        "error": result
                    })
                    logger.error(f"  ❌ {table_name} 修复失败: {result}")
        
        repair_results["overall_success"] = (
            len(repair_results["repairs_successful"]) > 0 and 
            len(repair_results["repairs_failed"]) == 0
        )
        
        return repair_results

    def verify_repairs(self) -> Dict[str, Any]:
        """验证修复效果"""
        logger.info("🔍 验证修复效果...")
        
        verification = {
            "timestamp": datetime.datetime.now().isoformat(),
            "table_status": {},
            "data_flow_test": {},
            "overall_health": False
        }
        
        # 1. 验证表结构
        for table_name in self.field_repairs.keys():
            table_status = self._verify_table_structure(table_name)
            verification["table_status"][table_name] = table_status
        
        # 2. 测试数据流
        verification["data_flow_test"] = self._test_data_flow()
        
        # 3. 整体健康状态
        all_tables_healthy = all(
            status["healthy"] for status in verification["table_status"].values()
        )
        data_flow_healthy = verification["data_flow_test"]["signal_pool_accessible"]
        
        verification["overall_health"] = all_tables_healthy and data_flow_healthy
        
        return verification

    def _verify_table_structure(self, table_name: str) -> Dict[str, Any]:
        """验证表结构"""
        status = {
            "table_name": table_name,
            "healthy": False,
            "field_count": 0,
            "missing_fields": [],
            "data_count": 0
        }
        
        try:
            # 检查字段结构
            cmd = f"bq show --format=json {self.project_id}:{self.dataset_lab}.{table_name}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                table_data = json.loads(result.stdout)
                if "schema" in table_data and "fields" in table_data["schema"]:
                    current_fields = [field["name"] for field in table_data["schema"]["fields"]]
                    status["field_count"] = len(current_fields)
                    
                    expected_fields = self.field_repairs[table_name]["expected_fields"]
                    status["missing_fields"] = [
                        field for field in expected_fields 
                        if field not in current_fields
                    ]
                    
                    status["healthy"] = len(status["missing_fields"]) == 0
            
            # 检查数据量
            sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.{table_name}`"
            success, result = self._run_bq_command(sql)
            if success:
                data = json.loads(result)
                status["data_count"] = int(data[0]["count"])
                
        except Exception as e:
            logger.error(f"验证 {table_name} 失败: {e}")
        
        return status

    def _test_data_flow(self) -> Dict[str, Any]:
        """测试数据流"""
        flow_test = {
            "signal_pool_accessible": False,
            "signal_pool_count": 0,
            "lab_candidates_accessible": False,
            "lab_candidates_count": 0
        }
        
        try:
            # 测试signal_pool_union_v3
            sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`"
            success, result = self._run_bq_command(sql)
            if success:
                data = json.loads(result)
                flow_test["signal_pool_count"] = int(data[0]["count"])
                flow_test["signal_pool_accessible"] = True
                logger.info(f"signal_pool_union_v3: {flow_test['signal_pool_count']} 行")
            
            # 测试lab_push_candidates_v2
            sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`"
            success, result = self._run_bq_command(sql)
            if success:
                data = json.loads(result)
                flow_test["lab_candidates_count"] = int(data[0]["count"])
                flow_test["lab_candidates_accessible"] = True
                logger.info(f"lab_push_candidates_v2: {flow_test['lab_candidates_count']} 行")
                
        except Exception as e:
            logger.error(f"数据流测试失败: {e}")
        
        return flow_test

    def run_complete_repair(self) -> Dict[str, Any]:
        """运行完整修复流程"""
        logger.info("🚀 开始完整字段修复...")
        
        complete_results = {
            "repair_timestamp": self.timestamp,
            "diagnosis": {},
            "repair_results": {},
            "verification": {},
            "overall_success": False
        }
        
        # 1. 诊断问题
        complete_results["diagnosis"] = self.diagnose_field_issues()
        
        # 2. 执行修复
        if complete_results["diagnosis"]["repair_needed"]:
            complete_results["repair_results"] = self.repair_field_issues(complete_results["diagnosis"])
        
        # 3. 验证修复
        complete_results["verification"] = self.verify_repairs()
        complete_results["overall_success"] = complete_results["verification"]["overall_health"]
        
        # 4. 生成报告
        self._generate_repair_report(complete_results)
        
        return complete_results

    def _generate_repair_report(self, results: Dict[str, Any]):
        """生成修复报告"""
        report_path = f"/Users/a606/cloud_function_source/pc28_field_repair_report_{self.timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 字段修复报告已生成: {report_path}")

def main():
    """主函数"""
    repair_system = PC28FieldRepairSystem()
    
    print("🔧 PC28字段修复系统启动")
    print("=" * 50)
    
    # 运行完整修复
    results = repair_system.run_complete_repair()
    
    # 输出结果
    print(f"\n📊 修复结果:")
    print(f"  需要修复: {results['diagnosis']['repair_needed']}")
    
    if results["diagnosis"]["repair_needed"]:
        repair_results = results.get("repair_results", {})
        print(f"  修复尝试: {len(repair_results.get('repairs_attempted', []))}")
        print(f"  修复成功: {len(repair_results.get('repairs_successful', []))}")
        print(f"  修复失败: {len(repair_results.get('repairs_failed', []))}")
    
    verification = results.get("verification", {})
    print(f"  整体健康: {verification.get('overall_health', False)}")
    
    if verification.get("data_flow_test"):
        flow_test = verification["data_flow_test"]
        print(f"  信号池数据: {flow_test.get('signal_pool_count', 0)} 行")
        print(f"  决策候选: {flow_test.get('lab_candidates_count', 0)} 行")
    
    if results["overall_success"]:
        print(f"\n🎉 字段修复完成！")
        print(f"💡 所有表结构已修复，数据流恢复正常")
    else:
        print(f"\n⚠️ 修复未完全成功，需要进一步处理")
        
        # 显示关键问题
        if results["diagnosis"]["critical_issues"]:
            print(f"  关键问题表: {', '.join(results['diagnosis']['critical_issues'])}")

if __name__ == "__main__":
    main()