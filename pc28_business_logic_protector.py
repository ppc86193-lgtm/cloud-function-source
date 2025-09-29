#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28业务逻辑保护系统
确保所有现有业务逻辑在优化过程中不丢失，实现完美闭环
基于数据库中的核心业务逻辑，提供第三方插件式的保护机制
"""
from __future__ import annotations
import os
import json
import subprocess
import shlex
import datetime
import logging
import sqlite3
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class BusinessLogicRule:
    """业务逻辑规则"""
    rule_id: str
    rule_name: str
    table_view: str
    logic_type: str  # 'data_validation', 'calculation', 'filtering', 'transformation'
    sql_pattern: str
    expected_behavior: str
    test_query: str
    critical_level: str  # 'critical', 'important', 'normal'
    created_at: str
    last_verified: Optional[str] = None

@dataclass
class LogicTestResult:
    """逻辑测试结果"""
    rule_id: str
    test_passed: bool
    actual_result: Any
    expected_result: Any
    error_message: Optional[str]
    test_timestamp: str

class PC28BusinessLogicProtector:
    """PC28业务逻辑保护器"""
    
    def __init__(self, project_id: str = "wprojectl", dataset_lab: str = "pc28_lab"):
        self.project_id = project_id
        self.dataset_lab = dataset_lab
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 初始化本地保护数据库
        self.protection_db_path = "/Users/a606/cloud_function_source/pc28_logic_protection.db"
        self._init_protection_database()
        
        # 加载核心业务逻辑规则
        self.business_rules = self._load_business_logic_rules()
        
    def _init_protection_database(self):
        """初始化保护数据库"""
        conn = sqlite3.connect(self.protection_db_path)
        cursor = conn.cursor()
        
        # 创建业务逻辑规则表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS business_logic_rules (
                rule_id TEXT PRIMARY KEY,
                rule_name TEXT NOT NULL,
                table_view TEXT NOT NULL,
                logic_type TEXT NOT NULL,
                sql_pattern TEXT NOT NULL,
                expected_behavior TEXT NOT NULL,
                test_query TEXT NOT NULL,
                critical_level TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_verified TEXT
            )
        """)
        
        # 创建测试结果表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logic_test_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT NOT NULL,
                test_passed BOOLEAN NOT NULL,
                actual_result TEXT,
                expected_result TEXT,
                error_message TEXT,
                test_timestamp TEXT NOT NULL,
                FOREIGN KEY (rule_id) REFERENCES business_logic_rules (rule_id)
            )
        """)
        
        # 创建逻辑变更历史表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logic_change_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT NOT NULL,
                change_type TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_reason TEXT,
                changed_by TEXT,
                change_timestamp TEXT NOT NULL,
                FOREIGN KEY (rule_id) REFERENCES business_logic_rules (rule_id)
            )
        """)
        
        conn.commit()
        conn.close()
        
        logger.info(f"保护数据库初始化完成: {self.protection_db_path}")
    
    def _load_business_logic_rules(self) -> List[BusinessLogicRule]:
        """加载核心业务逻辑规则"""
        rules = [
            # 1. 数据验证规则
            BusinessLogicRule(
                rule_id="DV001",
                rule_name="概率值有效性验证",
                table_view="cloud_pred_today_norm",
                logic_type="data_validation",
                sql_pattern="p_win BETWEEN 0.0 AND 1.0",
                expected_behavior="所有概率值必须在0-1之间",
                test_query=f"""
                SELECT COUNT(*) as invalid_count
                FROM `{self.project_id}.{self.dataset_lab}.cloud_pred_today_norm`
                WHERE p_win IS NOT NULL AND (p_win < 0.0 OR p_win > 1.0)
                """,
                critical_level="critical",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            BusinessLogicRule(
                rule_id="DV002", 
                rule_name="市场类型验证",
                table_view="signal_pool_union_v3",
                logic_type="data_validation",
                sql_pattern="market IN ('oe', 'size')",
                expected_behavior="市场类型只能是oe或size",
                test_query=f"""
                SELECT COUNT(*) as invalid_count
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
                WHERE market NOT IN ('oe', 'size')
                """,
                critical_level="critical",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            # 2. 计算逻辑规则
            BusinessLogicRule(
                rule_id="CL001",
                rule_name="EV计算逻辑",
                table_view="lab_push_candidates_v2",
                logic_type="calculation",
                sql_pattern="(p_win * 1.98 - 1.0) as ev",
                expected_behavior="EV = p_win * 赔率 - 1，赔率假设为1.98",
                test_query=f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN ABS((p_cloud * 1.98 - 1.0) - COALESCE(ev, 0)) > 0.001 THEN 1 END) as incorrect_ev_count
                FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`
                WHERE p_cloud IS NOT NULL
                """,
                critical_level="critical",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            BusinessLogicRule(
                rule_id="CL002",
                rule_name="Kelly分数计算",
                table_view="lab_push_candidates_v2", 
                logic_type="calculation",
                sql_pattern="(p_win * odds - 1.0) / (odds - 1.0)",
                expected_behavior="Kelly分数 = (p*odds - 1) / (odds - 1)",
                test_query=f"""
                SELECT 
                    COUNT(*) as total_count,
                    AVG(kelly_frac) as avg_kelly,
                    MAX(kelly_frac) as max_kelly
                FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`
                WHERE kelly_frac IS NOT NULL
                """,
                critical_level="important",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            # 3. 过滤逻辑规则
            BusinessLogicRule(
                rule_id="FL001",
                rule_name="正EV过滤",
                table_view="lab_push_candidates_v2",
                logic_type="filtering",
                sql_pattern="WHERE ev > 0",
                expected_behavior="只保留正期望值的候选",
                test_query=f"""
                SELECT 
                    COUNT(*) as total_candidates,
                    COUNT(CASE WHEN ev > 0 THEN 1 END) as positive_ev_count,
                    AVG(ev) as avg_ev
                FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`
                WHERE ev IS NOT NULL
                """,
                critical_level="critical",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            BusinessLogicRule(
                rule_id="FL002",
                rule_name="最小胜率过滤",
                table_view="signal_pool_union_v3",
                logic_type="filtering", 
                sql_pattern="WHERE p_win >= 0.5",
                expected_behavior="只保留胜率大于50%的信号",
                test_query=f"""
                SELECT 
                    COUNT(*) as total_signals,
                    COUNT(CASE WHEN p_win >= 0.5 THEN 1 END) as valid_signals,
                    AVG(p_win) as avg_p_win
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
                WHERE p_win IS NOT NULL
                """,
                critical_level="important",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            # 4. 数据转换规则
            BusinessLogicRule(
                rule_id="TR001",
                rule_name="投票权重计算",
                table_view="p_map_today_canon_v",
                logic_type="transformation",
                sql_pattern="CASE WHEN p_win >= 0.7 THEN 1.0 WHEN p_win >= 0.6 THEN 0.8 ELSE 0.3 END",
                expected_behavior="根据胜率分配投票权重：>=0.7为1.0，>=0.6为0.8，其他为0.3",
                test_query=f"""
                SELECT 
                    COUNT(*) as total_count,
                    AVG(vote_ratio) as avg_vote_ratio,
                    COUNT(CASE WHEN p_win >= 0.7 AND vote_ratio = 1.0 THEN 1 END) as high_confidence_correct,
                    COUNT(CASE WHEN p_win >= 0.6 AND p_win < 0.7 AND vote_ratio = 0.8 THEN 1 END) as medium_confidence_correct
                FROM `{self.project_id}.{self.dataset_lab}.p_map_today_canon_v`
                WHERE p_win IS NOT NULL AND vote_ratio IS NOT NULL
                """,
                critical_level="important",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            BusinessLogicRule(
                rule_id="TR002",
                rule_name="中文标签转换",
                table_view="signal_pool_union_v3",
                logic_type="transformation",
                sql_pattern="CASE WHEN market = 'oe' AND pick = 'even' THEN '偶数' WHEN market = 'oe' AND pick = 'odd' THEN '奇数' END",
                expected_behavior="奇偶市场的英文标签转换为中文",
                test_query=f"""
                SELECT 
                    market,
                    pick,
                    pick_zh,
                    COUNT(*) as count
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
                WHERE market = 'oe'
                GROUP BY market, pick, pick_zh
                """,
                critical_level="normal",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            # 5. 数据流完整性规则
            BusinessLogicRule(
                rule_id="DF001",
                rule_name="信号池数据完整性",
                table_view="signal_pool_union_v3",
                logic_type="data_validation",
                sql_pattern="UNION ALL between p_map_today_canon_v and p_size_today_canon_v",
                expected_behavior="信号池应包含地图和大小预测的所有数据",
                test_query=f"""
                SELECT 
                    'signal_pool' as source, COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
                UNION ALL
                SELECT 
                    'map_canon' as source, COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.p_map_today_canon_v`
                UNION ALL
                SELECT 
                    'size_canon' as source, COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.p_size_today_canon_v`
                """,
                critical_level="critical",
                created_at=datetime.datetime.now().isoformat()
            ),
            
            BusinessLogicRule(
                rule_id="DF002",
                rule_name="决策候选生成完整性",
                table_view="lab_push_candidates_v2",
                logic_type="data_validation",
                sql_pattern="Generated from signal_pool_union_v3 with positive EV",
                expected_behavior="决策候选应该从信号池中的正EV信号生成",
                test_query=f"""
                SELECT 
                    COUNT(*) as candidate_count,
                    COUNT(CASE WHEN ev > 0 THEN 1 END) as positive_ev_count,
                    MIN(ev) as min_ev,
                    MAX(ev) as max_ev
                FROM `{self.project_id}.{self.dataset_lab}.lab_push_candidates_v2`
                """,
                critical_level="critical",
                created_at=datetime.datetime.now().isoformat()
            )
        ]
        
        # 将规则保存到数据库
        self._save_rules_to_database(rules)
        
        return rules
    
    def _save_rules_to_database(self, rules: List[BusinessLogicRule]):
        """保存规则到数据库"""
        conn = sqlite3.connect(self.protection_db_path)
        cursor = conn.cursor()
        
        for rule in rules:
            cursor.execute("""
                INSERT OR REPLACE INTO business_logic_rules 
                (rule_id, rule_name, table_view, logic_type, sql_pattern, expected_behavior, test_query, critical_level, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.rule_id, rule.rule_name, rule.table_view, rule.logic_type,
                rule.sql_pattern, rule.expected_behavior, rule.test_query,
                rule.critical_level, rule.created_at
            ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"已保存 {len(rules)} 个业务逻辑规则到保护数据库")
    
    def _run_bq_command(self, sql: str, timeout: int = 300) -> Tuple[bool, str]:
        """执行BigQuery命令"""
        cmd = f"bq query --use_legacy_sql=false --format=json {shlex.quote(sql)}"
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
    
    def test_business_logic_rule(self, rule: BusinessLogicRule) -> LogicTestResult:
        """测试单个业务逻辑规则"""
        logger.info(f"测试业务逻辑规则: {rule.rule_name}")
        
        success, result = self._run_bq_command(rule.test_query)
        
        test_result = LogicTestResult(
            rule_id=rule.rule_id,
            test_passed=False,
            actual_result=None,
            expected_result=rule.expected_behavior,
            error_message=None,
            test_timestamp=datetime.datetime.now().isoformat()
        )
        
        if success and result:
            try:
                data = json.loads(result)
                test_result.actual_result = data
                
                # 根据规则类型判断测试是否通过
                if rule.logic_type == "data_validation":
                    if rule.rule_id in ["DV001", "DV002"]:
                        # 验证无效数据计数应为0
                        invalid_count = int(data[0].get("invalid_count", 0))
                        test_result.test_passed = invalid_count == 0
                        if invalid_count > 0:
                            test_result.error_message = f"发现 {invalid_count} 条无效数据"
                    elif rule.rule_id == "DF001":
                        # 验证数据流完整性
                        counts = {row["source"]: int(row["count"]) for row in data}
                        signal_count = counts.get("signal_pool", 0)
                        map_count = counts.get("map_canon", 0)
                        size_count = counts.get("size_canon", 0)
                        expected_total = map_count + size_count
                        test_result.test_passed = signal_count >= expected_total * 0.8  # 允许80%的数据保留率
                        if not test_result.test_passed:
                            test_result.error_message = f"信号池数据不完整: {signal_count}/{expected_total}"
                    elif rule.rule_id == "DF002":
                        # 验证决策候选生成
                        candidate_count = int(data[0].get("candidate_count", 0))
                        positive_ev_count = int(data[0].get("positive_ev_count", 0))
                        test_result.test_passed = candidate_count > 0 and positive_ev_count == candidate_count
                        if not test_result.test_passed:
                            test_result.error_message = f"决策候选生成异常: {candidate_count} 总数, {positive_ev_count} 正EV"
                
                elif rule.logic_type == "calculation":
                    if rule.rule_id == "CL001":
                        # 验证EV计算
                        total_count = int(data[0].get("total_count", 0))
                        incorrect_count = int(data[0].get("incorrect_ev_count", 0))
                        test_result.test_passed = total_count > 0 and incorrect_count == 0
                        if incorrect_count > 0:
                            test_result.error_message = f"EV计算错误: {incorrect_count}/{total_count}"
                    elif rule.rule_id == "CL002":
                        # 验证Kelly分数
                        avg_kelly = float(data[0].get("avg_kelly", 0))
                        max_kelly = float(data[0].get("max_kelly", 0))
                        test_result.test_passed = 0 <= avg_kelly <= 1 and 0 <= max_kelly <= 1
                        if not test_result.test_passed:
                            test_result.error_message = f"Kelly分数异常: avg={avg_kelly}, max={max_kelly}"
                
                elif rule.logic_type == "filtering":
                    if rule.rule_id == "FL001":
                        # 验证正EV过滤
                        total_candidates = int(data[0].get("total_candidates", 0))
                        positive_ev_count = int(data[0].get("positive_ev_count", 0))
                        avg_ev = float(data[0].get("avg_ev", 0))
                        test_result.test_passed = total_candidates == positive_ev_count and avg_ev > 0
                        if not test_result.test_passed:
                            test_result.error_message = f"EV过滤异常: {positive_ev_count}/{total_candidates}, avg_ev={avg_ev}"
                    elif rule.rule_id == "FL002":
                        # 验证胜率过滤
                        total_signals = int(data[0].get("total_signals", 0))
                        valid_signals = int(data[0].get("valid_signals", 0))
                        avg_p_win = float(data[0].get("avg_p_win", 0))
                        test_result.test_passed = total_signals == valid_signals and avg_p_win >= 0.5
                        if not test_result.test_passed:
                            test_result.error_message = f"胜率过滤异常: {valid_signals}/{total_signals}, avg_p_win={avg_p_win}"
                
                elif rule.logic_type == "transformation":
                    if rule.rule_id == "TR001":
                        # 验证投票权重计算
                        total_count = int(data[0].get("total_count", 0))
                        high_correct = int(data[0].get("high_confidence_correct", 0))
                        medium_correct = int(data[0].get("medium_confidence_correct", 0))
                        test_result.test_passed = total_count > 0 and (high_correct + medium_correct) > 0
                        if not test_result.test_passed:
                            test_result.error_message = f"投票权重计算异常: {high_correct + medium_correct}/{total_count}"
                    elif rule.rule_id == "TR002":
                        # 验证中文标签转换
                        has_chinese_labels = any(row.get("pick_zh") in ["奇数", "偶数"] for row in data)
                        test_result.test_passed = has_chinese_labels
                        if not test_result.test_passed:
                            test_result.error_message = "中文标签转换失败"
                
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                test_result.error_message = f"结果解析失败: {e}"
        else:
            test_result.error_message = result
        
        # 保存测试结果到数据库
        self._save_test_result(test_result)
        
        return test_result
    
    def _save_test_result(self, result: LogicTestResult):
        """保存测试结果到数据库"""
        conn = sqlite3.connect(self.protection_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO logic_test_results 
            (rule_id, test_passed, actual_result, expected_result, error_message, test_timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            result.rule_id, result.test_passed, 
            json.dumps(result.actual_result) if result.actual_result else None,
            result.expected_result, result.error_message, result.test_timestamp
        ))
        
        conn.commit()
        conn.close()
    
    def run_comprehensive_protection_test(self) -> Dict[str, Any]:
        """运行全面的保护测试"""
        logger.info("开始运行全面的业务逻辑保护测试...")
        
        test_results = {
            "test_timestamp": datetime.datetime.now().isoformat(),
            "total_rules": len(self.business_rules),
            "passed_rules": 0,
            "failed_rules": 0,
            "critical_failures": 0,
            "rule_results": {},
            "protection_status": "unknown"
        }
        
        for rule in self.business_rules:
            result = self.test_business_logic_rule(rule)
            
            test_results["rule_results"][rule.rule_id] = {
                "rule_name": rule.rule_name,
                "table_view": rule.table_view,
                "logic_type": rule.logic_type,
                "critical_level": rule.critical_level,
                "test_passed": result.test_passed,
                "error_message": result.error_message
            }
            
            if result.test_passed:
                test_results["passed_rules"] += 1
            else:
                test_results["failed_rules"] += 1
                if rule.critical_level == "critical":
                    test_results["critical_failures"] += 1
        
        # 评估整体保护状态
        if test_results["critical_failures"] == 0:
            if test_results["failed_rules"] == 0:
                test_results["protection_status"] = "excellent"
            elif test_results["failed_rules"] <= 2:
                test_results["protection_status"] = "good"
            else:
                test_results["protection_status"] = "needs_attention"
        else:
            test_results["protection_status"] = "critical_issues"
        
        # 生成保护报告
        self._generate_protection_report(test_results)
        
        return test_results
    
    def _generate_protection_report(self, test_results: Dict[str, Any]):
        """生成保护报告"""
        report_path = f"/Users/a606/cloud_function_source/pc28_business_logic_protection_report_{self.timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(test_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"业务逻辑保护报告已生成: {report_path}")
    
    def create_logic_backup_plugin(self) -> str:
        """创建业务逻辑备份插件"""
        logger.info("创建业务逻辑备份插件...")
        
        plugin_code = f'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28业务逻辑备份插件
自动备份和恢复所有核心业务逻辑
生成时间: {datetime.datetime.now().isoformat()}
"""

import json
import subprocess
import shlex
from typing import Dict, List, Any

class PC28LogicBackupPlugin:
    """PC28业务逻辑备份插件"""
    
    def __init__(self):
        self.project_id = "{self.project_id}"
        self.dataset_lab = "{self.dataset_lab}"
        self.backup_timestamp = "{self.timestamp}"
    
    def backup_all_views(self) -> Dict[str, str]:
        """备份所有视图定义"""
        views_to_backup = [
            "p_cloud_today_v",
            "p_map_today_v", 
            "p_size_today_v",
            "p_map_today_canon_v",
            "p_size_today_canon_v",
            "signal_pool_union_v3",
            "lab_push_candidates_v2"
        ]
        
        backup_definitions = {{}}
        
        for view_name in views_to_backup:
            sql = f"""
            SELECT view_definition
            FROM `{{self.project_id}}.{{self.dataset_lab}}.INFORMATION_SCHEMA.VIEWS`
            WHERE table_name = '{{view_name}}'
            """
            
            cmd = f"bq query --use_legacy_sql=false --format=json " + shlex.quote(sql)
            try:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    data = json.loads(result.stdout)
                    if data:
                        backup_definitions[view_name] = data[0]["view_definition"]
                        print(f"✅ 备份视图定义: {{view_name}}")
                    else:
                        print(f"⚠️ 视图不存在: {{view_name}}")
                else:
                    print(f"❌ 备份失败: {{view_name}} - {{result.stderr}}")
            except Exception as e:
                print(f"❌ 备份异常: {{view_name}} - {{e}}")
        
        # 保存备份文件
        backup_file = f"/Users/a606/cloud_function_source/pc28_view_definitions_backup_{{self.backup_timestamp}}.json"
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(backup_definitions, f, ensure_ascii=False, indent=2)
        
        print(f"📁 视图定义备份完成: {{backup_file}}")
        return backup_definitions
    
    def restore_view(self, view_name: str, view_definition: str) -> bool:
        """恢复单个视图"""
        sql = f"""
        CREATE OR REPLACE VIEW `{{self.project_id}}.{{self.dataset_lab}}.{{view_name}}` AS
        {{view_definition}}
        """
        
        cmd = f"bq query --use_legacy_sql=false " + shlex.quote(sql)
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ 恢复视图成功: {{view_name}}")
                return True
            else:
                print(f"❌ 恢复视图失败: {{view_name}} - {{result.stderr}}")
                return False
        except Exception as e:
            print(f"❌ 恢复视图异常: {{view_name}} - {{e}}")
            return False
    
    def validate_logic_integrity(self) -> Dict[str, bool]:
        """验证逻辑完整性"""
        validation_queries = {{
            "signal_pool_data": """
                SELECT COUNT(*) as count 
                FROM `{{self.project_id}}.{{self.dataset_lab}}.signal_pool_union_v3`
                WHERE day_id_cst = CURRENT_DATE('Asia/Shanghai')
            """,
            "decision_candidates": """
                SELECT COUNT(*) as count 
                FROM `{{self.project_id}}.{{self.dataset_lab}}.lab_push_candidates_v2`
                WHERE day_id_cst = CURRENT_DATE('Asia/Shanghai')
            """,
            "positive_ev_filter": """
                SELECT COUNT(*) as count 
                FROM `{{self.project_id}}.{{self.dataset_lab}}.lab_push_candidates_v2`
                WHERE ev > 0 AND day_id_cst = CURRENT_DATE('Asia/Shanghai')
            """
        }}
        
        validation_results = {{}}
        
        for test_name, sql in validation_queries.items():
            cmd = f"bq query --use_legacy_sql=false --format=json " + shlex.quote(sql)
            try:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    data = json.loads(result.stdout)
                    count = int(data[0]["count"]) if data else 0
                    validation_results[test_name] = count > 0
                    print(f"{'✅' if count > 0 else '❌'} " + test_name + f": {count} 条记录")
                else:
                    validation_results[test_name] = False
                    print(f"❌ " + test_name + ": 查询失败")
            except Exception as e:
                validation_results[test_name] = False
                print(f"❌ " + test_name + f": 异常 - {e}")
        
        return validation_results

if __name__ == "__main__":
    plugin = PC28LogicBackupPlugin()
    
    print("🔄 开始业务逻辑保护...")
    
    # 备份所有视图定义
    backup_definitions = plugin.backup_all_views()
    
    # 验证逻辑完整性
    validation_results = plugin.validate_logic_integrity()
    
    # 输出保护状态
    all_valid = all(validation_results.values())
    print(f"\\n{'🎉' if all_valid else '⚠️'} 业务逻辑保护完成")
    print(f"📊 备份视图数量: " + str(len(backup_definitions)))
    print(f"✅ 验证通过: " + str(sum(validation_results.values())) + "/" + str(len(validation_results)))
    
    if not all_valid:
        print("\\n⚠️ 发现问题的验证项:")
        for test_name, passed in validation_results.items():
            if not passed:
                print(f"  - " + test_name)
'''
        
        plugin_path = f"/Users/a606/cloud_function_source/pc28_logic_backup_plugin_{self.timestamp}.py"
        with open(plugin_path, 'w', encoding='utf-8') as f:
            f.write(plugin_code)
        
        # 设置执行权限
        os.chmod(plugin_path, 0o755)
        
        logger.info(f"业务逻辑备份插件已创建: {plugin_path}")
        return plugin_path

def main():
    """主函数"""
    protector = PC28BusinessLogicProtector()
    
    print("🛡️ PC28业务逻辑保护系统启动")
    print("=" * 50)
    
    # 运行全面保护测试
    test_results = protector.run_comprehensive_protection_test()
    
    # 创建备份插件
    plugin_path = protector.create_logic_backup_plugin()
    
    # 输出保护状态
    print(f"\\n📊 保护测试结果:")
    print(f"  总规则数: {test_results['total_rules']}")
    print(f"  通过规则: {test_results['passed_rules']}")
    print(f"  失败规则: {test_results['failed_rules']}")
    print(f"  关键失败: {test_results['critical_failures']}")
    print(f"  保护状态: {test_results['protection_status']}")
    
    if test_results['protection_status'] == 'excellent':
        print("\\n🎉 业务逻辑保护状态优秀！所有核心逻辑完整无损")
    elif test_results['protection_status'] == 'good':
        print("\\n✅ 业务逻辑保护状态良好，核心逻辑安全")
    elif test_results['protection_status'] == 'needs_attention':
        print("\\n⚠️ 业务逻辑需要关注，建议检查失败的规则")
    else:
        print("\\n❌ 发现关键业务逻辑问题，需要立即修复")
    
    print(f"\\n🔧 备份插件已创建: {plugin_path}")
    print("💡 可以运行插件进行业务逻辑备份和恢复")

if __name__ == "__main__":
    main()