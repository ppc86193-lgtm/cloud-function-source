#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28智能自动修复系统
基于表逻辑自动检测和修复字段缺失、名称不匹配等问题
"""

import os
import sys
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RepairAction:
    """修复动作"""
    action_type: str  # 'create_table', 'alter_table', 'create_view', 'update_view'
    target_table: str
    description: str
    sql_statement: str
    priority: int  # 1-5, 1最高
    dependencies: List[str] = None

class IntelligentAutoRepair:
    """智能自动修复系统"""
    
    def __init__(self, table_logic_file: str = None):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.client = bigquery.Client(project=self.project_id)
        
        # 加载表逻辑
        if table_logic_file:
            self.table_logic = self.load_table_logic(table_logic_file)
        else:
            # 查找最新的导出文件
            self.table_logic = self.load_latest_table_logic()
        
        # 修复规则配置
        self.repair_rules = {
            "field_mapping_rules": {
                # 常见字段名映射
                "p_even": ["p_win", "prob_even", "even_prob"],
                "p_big": ["prob_big", "big_prob"],
                "p_small": ["prob_small", "small_prob"],
                "ts_utc": ["timestamp", "time_utc", "created_at"],
                "period": ["period_id", "game_period", "issue"],
                "id": ["record_id", "row_id", "pk"]
            },
            "table_templates": {
                # 标准表结构模板
                "prediction_table": {
                    "fields": [
                        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "period", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "ts_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
                        {"name": "p_even", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "source", "type": "STRING", "mode": "NULLABLE"}
                    ]
                },
                "signal_table": {
                    "fields": [
                        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
                        {"name": "ts_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
                        {"name": "period", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "market", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "pick", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "p_win", "type": "FLOAT", "mode": "NULLABLE"},
                        {"name": "source", "type": "STRING", "mode": "NULLABLE"}
                    ]
                }
            }
        }
    
    def load_table_logic(self, file_path: str) -> Dict[str, Any]:
        """加载表逻辑文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载表逻辑文件失败: {e}")
            return {}
    
    def load_latest_table_logic(self) -> Dict[str, Any]:
        """加载最新的表逻辑导出文件"""
        try:
            import glob
            pattern = "/Users/a606/cloud_function_source/test_suite/table_logic_export_*.json"
            files = glob.glob(pattern)
            
            if files:
                # 按修改时间排序，取最新的
                latest_file = max(files, key=os.path.getmtime)
                logger.info(f"加载最新表逻辑文件: {latest_file}")
                return self.load_table_logic(latest_file)
            else:
                logger.warning("未找到表逻辑导出文件")
                return {}
                
        except Exception as e:
            logger.error(f"加载最新表逻辑文件失败: {e}")
            return {}
    
    def analyze_data_flow_issues(self) -> List[Dict[str, Any]]:
        """分析数据流问题"""
        issues = []
        
        if not self.table_logic or "tables" not in self.table_logic:
            issues.append({
                "type": "system_error",
                "severity": "critical",
                "description": "无法加载表逻辑信息",
                "table": None
            })
            return issues
        
        tables = self.table_logic["tables"]
        
        # 1. 检查空表问题
        for table_name, table_info in tables.items():
            if table_info["schema"] and table_info["schema"]["num_rows"] == 0:
                # 检查是否应该有数据
                if self.should_have_data(table_name, table_info):
                    issues.append({
                        "type": "empty_table",
                        "severity": "high",
                        "description": f"表 {table_name} 为空但应该包含数据",
                        "table": table_name,
                        "table_info": table_info
                    })
        
        # 2. 检查字段缺失问题
        field_issues = self.analyze_field_issues(tables)
        issues.extend(field_issues)
        
        # 3. 检查依赖关系问题
        dependency_issues = self.analyze_dependency_issues(tables)
        issues.extend(dependency_issues)
        
        return issues
    
    def should_have_data(self, table_name: str, table_info: Dict[str, Any]) -> bool:
        """判断表是否应该有数据"""
        # 基础数据表应该有数据
        if table_name in ["cloud_pred_today_norm"]:
            return True
        
        # 视图如果依赖的表有数据，则应该有数据
        if table_info["dependencies"]:
            for dep in table_info["dependencies"]:
                if dep in self.table_logic["tables"]:
                    dep_info = self.table_logic["tables"][dep]
                    if dep_info["schema"] and dep_info["schema"]["num_rows"] > 0:
                        return True
        
        return False
    
    def analyze_field_issues(self, tables: Dict[str, Any]) -> List[Dict[str, Any]]:
        """分析字段问题"""
        issues = []
        
        # 检查字段映射问题
        for table_name, table_info in tables.items():
            if not table_info["view_definition"]:
                continue
            
            view_def = table_info["view_definition"]["view_definition"]
            
            # 检查是否引用了不存在的字段
            referenced_fields = self.extract_referenced_fields(view_def)
            
            for dep_table in table_info["dependencies"]:
                if dep_table in tables:
                    dep_schema = tables[dep_table]["schema"]
                    if dep_schema:
                        available_fields = {f["name"] for f in dep_schema["fields"]}
                        
                        for ref_field in referenced_fields:
                            if ref_field not in available_fields:
                                # 尝试找到可能的映射
                                suggested_field = self.suggest_field_mapping(ref_field, available_fields)
                                
                                issues.append({
                                    "type": "missing_field",
                                    "severity": "high",
                                    "description": f"表 {table_name} 引用了不存在的字段 {ref_field}",
                                    "table": table_name,
                                    "missing_field": ref_field,
                                    "source_table": dep_table,
                                    "suggested_field": suggested_field,
                                    "available_fields": list(available_fields)
                                })
        
        return issues
    
    def analyze_dependency_issues(self, tables: Dict[str, Any]) -> List[Dict[str, Any]]:
        """分析依赖关系问题"""
        issues = []
        
        for table_name, table_info in tables.items():
            for dep_table in table_info["dependencies"]:
                if dep_table not in tables:
                    issues.append({
                        "type": "missing_dependency",
                        "severity": "critical",
                        "description": f"表 {table_name} 依赖的表 {dep_table} 不存在",
                        "table": table_name,
                        "missing_dependency": dep_table
                    })
        
        return issues
    
    def extract_referenced_fields(self, view_definition: str) -> List[str]:
        """从视图定义中提取引用的字段"""
        fields = []
        
        # 简单的字段提取逻辑
        # 匹配 table.field 或 field 模式
        patterns = [
            r'\b\w+\.\w+\b',  # table.field
            r'SELECT\s+([^FROM]+)',  # SELECT子句
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, view_definition, re.IGNORECASE)
            for match in matches:
                if '.' in match:
                    field_name = match.split('.')[-1]
                    fields.append(field_name)
        
        return list(set(fields))
    
    def suggest_field_mapping(self, missing_field: str, available_fields: set) -> Optional[str]:
        """建议字段映射"""
        # 检查映射规则
        for standard_field, alternatives in self.repair_rules["field_mapping_rules"].items():
            if missing_field in alternatives:
                if standard_field in available_fields:
                    return standard_field
        
        # 模糊匹配
        for available_field in available_fields:
            if self.field_similarity(missing_field, available_field) > 0.7:
                return available_field
        
        return None
    
    def field_similarity(self, field1: str, field2: str) -> float:
        """计算字段名相似度"""
        # 简单的相似度计算
        field1 = field1.lower()
        field2 = field2.lower()
        
        if field1 == field2:
            return 1.0
        
        # 检查包含关系
        if field1 in field2 or field2 in field1:
            return 0.8
        
        # 检查公共子串
        common_chars = set(field1) & set(field2)
        total_chars = set(field1) | set(field2)
        
        if total_chars:
            return len(common_chars) / len(total_chars)
        
        return 0.0
    
    def generate_repair_actions(self, issues: List[Dict[str, Any]]) -> List[RepairAction]:
        """生成修复动作"""
        actions = []
        
        for issue in issues:
            if issue["type"] == "empty_table":
                action = self.generate_empty_table_repair(issue)
                if action:
                    actions.append(action)
            
            elif issue["type"] == "missing_field":
                action = self.generate_field_mapping_repair(issue)
                if action:
                    actions.append(action)
            
            elif issue["type"] == "missing_dependency":
                action = self.generate_dependency_repair(issue)
                if action:
                    actions.append(action)
        
        # 按优先级排序
        actions.sort(key=lambda x: x.priority)
        return actions
    
    def generate_empty_table_repair(self, issue: Dict[str, Any]) -> Optional[RepairAction]:
        """生成空表修复动作"""
        table_name = issue["table"]
        table_info = issue["table_info"]
        
        if not table_info["view_definition"]:
            return None
        
        # 检查视图定义是否有问题
        view_def = table_info["view_definition"]["view_definition"]
        
        # 尝试修复日期过滤问题
        if "DATE(" in view_def and "Asia/Shanghai" in view_def:
            # 可能是日期过滤导致的空结果
            # 生成修复SQL，扩大日期范围
            fixed_sql = self.fix_date_filter(view_def)
            
            return RepairAction(
                action_type="update_view",
                target_table=table_name,
                description=f"修复 {table_name} 的日期过滤逻辑",
                sql_statement=f"CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.{table_name}` AS {fixed_sql}",
                priority=2
            )
        
        return None
    
    def generate_field_mapping_repair(self, issue: Dict[str, Any]) -> Optional[RepairAction]:
        """生成字段映射修复动作"""
        table_name = issue["table"]
        missing_field = issue["missing_field"]
        suggested_field = issue.get("suggested_field")
        
        if not suggested_field:
            return None
        
        # 获取当前视图定义
        table_info = self.table_logic["tables"][table_name]
        if not table_info["view_definition"]:
            return None
        
        view_def = table_info["view_definition"]["view_definition"]
        
        # 替换字段名
        fixed_sql = view_def.replace(missing_field, suggested_field)
        
        return RepairAction(
            action_type="update_view",
            target_table=table_name,
            description=f"修复 {table_name} 中的字段映射: {missing_field} -> {suggested_field}",
            sql_statement=f"CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.{table_name}` AS {fixed_sql}",
            priority=1
        )
    
    def generate_dependency_repair(self, issue: Dict[str, Any]) -> Optional[RepairAction]:
        """生成依赖修复动作"""
        missing_dep = issue["missing_dependency"]
        
        # 尝试创建缺失的表或视图
        if missing_dep in self.repair_rules["table_templates"]:
            template = self.repair_rules["table_templates"][missing_dep]
            
            # 生成创建表的SQL
            fields_sql = []
            for field in template["fields"]:
                mode = "NOT NULL" if field["mode"] == "REQUIRED" else ""
                fields_sql.append(f"{field['name']} {field['type']} {mode}")
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS `{self.project_id}.{self.dataset_id}.{missing_dep}` (
                    {', '.join(fields_sql)}
                )
            """
            
            return RepairAction(
                action_type="create_table",
                target_table=missing_dep,
                description=f"创建缺失的依赖表 {missing_dep}",
                sql_statement=create_sql,
                priority=1
            )
        
        return None
    
    def fix_date_filter(self, view_definition: str) -> str:
        """修复日期过滤逻辑"""
        # 将今天的过滤改为最近几天
        today_pattern = r"DATE\\(DATETIME\\(TIMESTAMP_TRUNC\\(CURRENT_TIMESTAMP\\(\\), DAY, 'Asia/Shanghai'\\)\\)\\)"
        
        # 替换为最近3天的数据
        replacement = """DATE(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'Asia/Shanghai'))) 
                        OR DATE(ts_utc, 'Asia/Shanghai') >= DATE_SUB(DATE(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'Asia/Shanghai'))), INTERVAL 3 DAY)"""
        
        fixed_sql = re.sub(today_pattern, replacement, view_definition)
        
        # 如果没有匹配到，尝试其他日期模式
        if fixed_sql == view_definition:
            # 查找其他日期过滤模式并修复
            date_patterns = [
                r"DATE\\(ts_utc, 'Asia/Shanghai'\\) = CURRENT_DATE\\('Asia/Shanghai'\\)",
                r"DATE\\(ts_utc\\) = CURRENT_DATE\\(\\)"
            ]
            
            for pattern in date_patterns:
                if re.search(pattern, view_definition):
                    fixed_sql = re.sub(pattern, 
                                     "DATE(ts_utc, 'Asia/Shanghai') >= DATE_SUB(CURRENT_DATE('Asia/Shanghai'), INTERVAL 3 DAY)", 
                                     view_definition)
                    break
        
        return fixed_sql
    
    def execute_repair_actions(self, actions: List[RepairAction]) -> Dict[str, Any]:
        """执行修复动作"""
        results = {
            "total_actions": len(actions),
            "successful": 0,
            "failed": 0,
            "results": []
        }
        
        for action in actions:
            try:
                logger.info(f"执行修复动作: {action.description}")
                
                # 执行SQL
                job = self.client.query(action.sql_statement)
                job.result()  # 等待完成
                
                results["successful"] += 1
                results["results"].append({
                    "action": action.description,
                    "status": "success",
                    "table": action.target_table
                })
                
                logger.info(f"修复成功: {action.description}")
                
            except Exception as e:
                results["failed"] += 1
                results["results"].append({
                    "action": action.description,
                    "status": "failed",
                    "table": action.target_table,
                    "error": str(e)
                })
                
                logger.error(f"修复失败: {action.description}, 错误: {e}")
        
        return results
    
    def run_intelligent_repair(self) -> Dict[str, Any]:
        """运行智能修复"""
        logger.info("开始智能自动修复...")
        
        start_time = datetime.now()
        
        # 1. 分析问题
        logger.info("分析数据流问题...")
        issues = self.analyze_data_flow_issues()
        
        if not issues:
            return {
                "status": "success",
                "message": "未发现需要修复的问题",
                "issues_found": 0,
                "actions_generated": 0,
                "actions_executed": 0,
                "successful_repairs": 0,
                "failed_repairs": 0,
                "execution_time": (datetime.now() - start_time).total_seconds()
            }
        
        logger.info(f"发现 {len(issues)} 个问题")
        
        # 2. 生成修复动作
        logger.info("生成修复动作...")
        actions = self.generate_repair_actions(issues)
        
        if not actions:
            return {
                "status": "warning",
                "message": "发现问题但无法生成修复动作",
                "issues_found": len(issues),
                "actions_executed": 0,
                "issues": issues,
                "execution_time": (datetime.now() - start_time).total_seconds()
            }
        
        logger.info(f"生成 {len(actions)} 个修复动作")
        
        # 3. 执行修复
        logger.info("执行修复动作...")
        repair_results = self.execute_repair_actions(actions)
        
        # 4. 汇总结果
        execution_time = (datetime.now() - start_time).total_seconds()
        
        summary = {
            "status": "completed",
            "message": f"智能修复完成: {repair_results['successful']}/{repair_results['total_actions']} 成功",
            "issues_found": len(issues),
            "actions_generated": len(actions),
            "actions_executed": repair_results["total_actions"],
            "successful_repairs": repair_results["successful"],
            "failed_repairs": repair_results["failed"],
            "execution_time": execution_time,
            "issues": issues,
            "repair_results": repair_results["results"]
        }
        
        logger.info(f"智能修复完成: {repair_results['successful']}/{repair_results['total_actions']} 成功")
        return summary

def main():
    """主函数"""
    repair_system = IntelligentAutoRepair()
    
    try:
        # 运行智能修复
        summary = repair_system.run_intelligent_repair()
        
        # 输出结果
        print("\n" + "="*60)
        print("PC28智能自动修复报告")
        print("="*60)
        print(f"状态: {summary['status']}")
        print(f"消息: {summary['message']}")
        print(f"发现问题: {summary['issues_found']} 个")
        print(f"生成动作: {summary['actions_generated']} 个")
        print(f"执行动作: {summary['actions_executed']} 个")
        print(f"成功修复: {summary['successful_repairs']} 个")
        print(f"修复失败: {summary['failed_repairs']} 个")
        print(f"执行时间: {summary['execution_time']:.2f} 秒")
        
        if summary.get('issues'):
            print("\n发现的问题:")
            for i, issue in enumerate(summary['issues'], 1):
                print(f"{i}. [{issue['severity']}] {issue['description']}")
        
        if summary.get('repair_results'):
            print("\n修复结果:")
            for result in summary['repair_results']:
                status = "✅" if result['status'] == 'success' else "❌"
                print(f"{status} {result['action']}")
                if result['status'] == 'failed':
                    print(f"   错误: {result.get('error', 'Unknown error')}")
        
        # 保存结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"/Users/a606/cloud_function_source/test_suite/intelligent_repair_report_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n详细报告已保存到: {report_file}")
        
    except Exception as e:
        logger.error(f"智能修复异常: {e}")
        print(f"智能修复失败: {e}")

if __name__ == "__main__":
    main()