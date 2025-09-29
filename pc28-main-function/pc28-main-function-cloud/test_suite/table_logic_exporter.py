#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28表逻辑导出器
导出所有表和视图的定义、字段映射、依赖关系
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TableLogicExporter:
    """表逻辑导出器"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.client = bigquery.Client(project=self.project_id)
        
        # 核心表和视图列表
        self.core_tables = [
            # 原始数据表
            "cloud_pred_today_norm",
            "p_cloud_clean_merged_dedup_v",
            
            # 预测视图
            "p_cloud_today_v",
            "p_map_today_v", 
            "p_size_today_v",
            
            # 标准化视图
            "p_ensemble_today_canon_v",
            "p_map_today_canon_v",
            "p_size_today_canon_v",
            
            # 信号池
            "signal_pool",
            "signal_pool_union_v3",
            
            # 决策层
            "lab_push_candidates_v2"
        ]
    
    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表结构"""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            table = self.client.get_table(table_ref)
            
            schema_info = {
                "table_name": table_name,
                "table_type": table.table_type,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "fields": []
            }
            
            for field in table.schema:
                field_info = {
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": field.description
                }
                schema_info["fields"].append(field_info)
            
            return schema_info
            
        except Exception as e:
            logger.error(f"获取表结构失败 {table_name}: {e}")
            return None
    
    def get_view_definition(self, view_name: str) -> Optional[Dict[str, Any]]:
        """获取视图定义"""
        try:
            # 使用INFORMATION_SCHEMA获取视图定义
            query = f"""
                SELECT 
                    table_name,
                    view_definition,
                    table_type
                FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.VIEWS`
                WHERE table_name = '{view_name}'
            """
            
            results = list(self.client.query(query).result())
            
            if results:
                result = results[0]
                return {
                    "view_name": result['table_name'],
                    "view_definition": result['view_definition'],
                    "table_type": result['table_type']
                }
            else:
                # 如果不是视图，尝试获取表信息
                return None
                
        except Exception as e:
            logger.error(f"获取视图定义失败 {view_name}: {e}")
            return None
    
    def parse_view_dependencies(self, view_definition: str) -> List[str]:
        """解析视图依赖的表"""
        dependencies = []
        
        # 正则表达式匹配FROM和JOIN子句中的表名
        patterns = [
            r'FROM\s+`?([^`\s]+)`?',
            r'JOIN\s+`?([^`\s]+)`?',
            r'LEFT\s+JOIN\s+`?([^`\s]+)`?',
            r'RIGHT\s+JOIN\s+`?([^`\s]+)`?',
            r'INNER\s+JOIN\s+`?([^`\s]+)`?'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, view_definition, re.IGNORECASE)
            for match in matches:
                # 清理表名，移除项目和数据集前缀
                table_name = match.split('.')[-1]
                if table_name not in dependencies and table_name != 'UNNEST':
                    dependencies.append(table_name)
        
        return dependencies
    
    def analyze_field_mappings(self, view_definition: str) -> Dict[str, Any]:
        """分析字段映射关系"""
        field_mappings = {
            "select_fields": [],
            "calculated_fields": [],
            "renamed_fields": [],
            "aggregated_fields": []
        }
        
        try:
            # 提取SELECT子句
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', view_definition, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1)
                
                # 分割字段
                fields = [f.strip() for f in select_clause.split(',')]
                
                for field in fields:
                    field = field.strip()
                    
                    # 检查是否有别名
                    if ' AS ' in field.upper():
                        parts = re.split(r'\s+AS\s+', field, flags=re.IGNORECASE)
                        if len(parts) == 2:
                            original = parts[0].strip()
                            alias = parts[1].strip()
                            field_mappings["renamed_fields"].append({
                                "original": original,
                                "alias": alias
                            })
                    
                    # 检查是否是计算字段
                    elif any(op in field.upper() for op in ['CASE', 'IF', 'COALESCE', 'CONCAT', '+', '-', '*', '/']):
                        field_mappings["calculated_fields"].append(field)
                    
                    # 检查是否是聚合字段
                    elif any(func in field.upper() for func in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'GROUP_CONCAT']):
                        field_mappings["aggregated_fields"].append(field)
                    
                    # 普通字段
                    else:
                        field_mappings["select_fields"].append(field)
            
        except Exception as e:
            logger.error(f"分析字段映射失败: {e}")
        
        return field_mappings
    
    def export_table_logic(self, table_name: str) -> Dict[str, Any]:
        """导出单个表的完整逻辑"""
        logger.info(f"导出表逻辑: {table_name}")
        
        table_logic = {
            "table_name": table_name,
            "export_time": datetime.now().isoformat(),
            "schema": None,
            "view_definition": None,
            "dependencies": [],
            "field_mappings": {},
            "issues": []
        }
        
        # 获取表结构
        schema = self.get_table_schema(table_name)
        if schema:
            table_logic["schema"] = schema
        else:
            table_logic["issues"].append("无法获取表结构")
        
        # 获取视图定义
        view_def = self.get_view_definition(table_name)
        if view_def:
            table_logic["view_definition"] = view_def
            
            # 分析依赖关系
            dependencies = self.parse_view_dependencies(view_def["view_definition"])
            table_logic["dependencies"] = dependencies
            
            # 分析字段映射
            field_mappings = self.analyze_field_mappings(view_def["view_definition"])
            table_logic["field_mappings"] = field_mappings
        
        return table_logic
    
    def export_all_tables(self) -> Dict[str, Any]:
        """导出所有表的逻辑"""
        logger.info("开始导出所有表逻辑...")
        
        export_result = {
            "export_time": datetime.now().isoformat(),
            "project_id": self.project_id,
            "dataset_id": self.dataset_id,
            "tables": {},
            "dependency_graph": {},
            "field_compatibility": {},
            "issues_summary": []
        }
        
        # 导出每个表
        for table_name in self.core_tables:
            table_logic = self.export_table_logic(table_name)
            export_result["tables"][table_name] = table_logic
            
            # 构建依赖图
            if table_logic["dependencies"]:
                export_result["dependency_graph"][table_name] = table_logic["dependencies"]
        
        # 分析字段兼容性
        export_result["field_compatibility"] = self.analyze_field_compatibility(export_result["tables"])
        
        # 汇总问题
        export_result["issues_summary"] = self.summarize_issues(export_result["tables"])
        
        logger.info("表逻辑导出完成")
        return export_result
    
    def analyze_field_compatibility(self, tables: Dict[str, Any]) -> Dict[str, Any]:
        """分析字段兼容性"""
        field_analysis = {
            "common_fields": {},
            "missing_fields": {},
            "type_mismatches": {},
            "naming_inconsistencies": []
        }
        
        # 收集所有字段
        all_fields = {}
        for table_name, table_info in tables.items():
            if table_info["schema"] and table_info["schema"]["fields"]:
                for field in table_info["schema"]["fields"]:
                    field_name = field["name"]
                    field_type = field["field_type"]
                    
                    if field_name not in all_fields:
                        all_fields[field_name] = {}
                    
                    all_fields[field_name][table_name] = {
                        "type": field_type,
                        "mode": field["mode"]
                    }
        
        # 分析常见字段
        for field_name, tables_info in all_fields.items():
            if len(tables_info) > 1:
                field_analysis["common_fields"][field_name] = tables_info
                
                # 检查类型不匹配
                types = set(info["type"] for info in tables_info.values())
                if len(types) > 1:
                    field_analysis["type_mismatches"][field_name] = tables_info
        
        # 检查缺失字段（基于依赖关系）
        for table_name, table_info in tables.items():
            if table_info["dependencies"]:
                for dep_table in table_info["dependencies"]:
                    if dep_table in tables:
                        # 比较字段
                        current_fields = set()
                        dep_fields = set()
                        
                        if table_info["schema"] and table_info["schema"]["fields"]:
                            current_fields = {f["name"] for f in table_info["schema"]["fields"]}
                        
                        if tables[dep_table]["schema"] and tables[dep_table]["schema"]["fields"]:
                            dep_fields = {f["name"] for f in tables[dep_table]["schema"]["fields"]}
                        
                        # 检查可能缺失的字段
                        if table_info["field_mappings"] and table_info["field_mappings"]["select_fields"]:
                            referenced_fields = set()
                            for field in table_info["field_mappings"]["select_fields"]:
                                # 简单提取字段名
                                field_name = field.split('.')[-1].strip('`"\'')
                                referenced_fields.add(field_name)
                            
                            missing = referenced_fields - dep_fields
                            if missing:
                                if table_name not in field_analysis["missing_fields"]:
                                    field_analysis["missing_fields"][table_name] = {}
                                field_analysis["missing_fields"][table_name][dep_table] = list(missing)
        
        return field_analysis
    
    def summarize_issues(self, tables: Dict[str, Any]) -> List[Dict[str, Any]]:
        """汇总问题"""
        issues = []
        
        for table_name, table_info in tables.items():
            # 表级别问题
            if table_info["issues"]:
                for issue in table_info["issues"]:
                    issues.append({
                        "table": table_name,
                        "type": "table_access",
                        "severity": "high",
                        "description": issue
                    })
            
            # 空表问题
            if table_info["schema"] and table_info["schema"]["num_rows"] == 0:
                issues.append({
                    "table": table_name,
                    "type": "empty_table",
                    "severity": "medium",
                    "description": f"表 {table_name} 为空"
                })
            
            # 依赖问题
            if table_info["dependencies"]:
                for dep in table_info["dependencies"]:
                    if dep not in tables:
                        issues.append({
                            "table": table_name,
                            "type": "missing_dependency",
                            "severity": "high",
                            "description": f"依赖表 {dep} 不存在或无法访问"
                        })
        
        return issues
    
    def save_export_result(self, export_result: Dict[str, Any], output_file: str = None) -> str:
        """保存导出结果"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"/Users/a606/cloud_function_source/test_suite/table_logic_export_{timestamp}.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"导出结果已保存到: {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"保存导出结果失败: {e}")
            return ""

def main():
    """主函数"""
    exporter = TableLogicExporter()
    
    try:
        # 导出所有表逻辑
        export_result = exporter.export_all_tables()
        
        # 保存结果
        output_file = exporter.save_export_result(export_result)
        
        # 输出摘要
        print("\n" + "="*60)
        print("PC28表逻辑导出报告")
        print("="*60)
        print(f"导出时间: {export_result['export_time']}")
        print(f"项目: {export_result['project_id']}.{export_result['dataset_id']}")
        print(f"导出表数: {len(export_result['tables'])}")
        
        print("\n表状态:")
        for table_name, table_info in export_result['tables'].items():
            status = "✅" if not table_info['issues'] else "❌"
            row_count = table_info['schema']['num_rows'] if table_info['schema'] else 0
            print(f"{status} {table_name}: {row_count} 行")
        
        print(f"\n依赖关系:")
        for table_name, deps in export_result['dependency_graph'].items():
            if deps:
                print(f"  {table_name} -> {', '.join(deps)}")
        
        print(f"\n字段兼容性:")
        compatibility = export_result['field_compatibility']
        print(f"  常见字段: {len(compatibility['common_fields'])}")
        print(f"  类型不匹配: {len(compatibility['type_mismatches'])}")
        print(f"  缺失字段: {len(compatibility['missing_fields'])}")
        
        print(f"\n问题汇总:")
        issues_by_severity = {}
        for issue in export_result['issues_summary']:
            severity = issue['severity']
            if severity not in issues_by_severity:
                issues_by_severity[severity] = 0
            issues_by_severity[severity] += 1
        
        for severity, count in issues_by_severity.items():
            print(f"  {severity}: {count} 个问题")
        
        print(f"\n详细结果已保存到: {output_file}")
        
    except Exception as e:
        logger.error(f"导出失败: {e}")
        print(f"导出失败: {e}")

if __name__ == "__main__":
    main()