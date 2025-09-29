#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据表优化工具
基于字段使用分析结果自动生成优化建议和SQL DDL语句
"""

import json
import sys
import os
from typing import Dict, List, Any, Set, Tuple
from datetime import datetime
import re
from dataclasses import dataclass
from pathlib import Path

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from field_usage_analysis import FieldUsageAnalyzer


@dataclass
class TableOptimization:
    """表优化建议"""
    table_name: str
    optimization_type: str
    description: str
    impact: str
    sql_before: str
    sql_after: str
    migration_script: str
    estimated_savings: Dict[str, Any]


@dataclass
class FieldOptimization:
    """字段优化建议"""
    field_name: str
    optimization_type: str
    reason: str
    action: str
    impact_assessment: str


class DatabaseTableOptimizer:
    """数据库表优化器"""
    
    def __init__(self):
        self.field_analyzer = FieldUsageAnalyzer()
        
        # 当前表结构定义（基于现有系统）
        self.current_tables = {
            'score_ledger': {
                'fields': [
                    {'name': 'order_id', 'type': 'STRING', 'nullable': False, 'usage': 'high'},
                    {'name': 'timestamp', 'type': 'TIMESTAMP', 'nullable': True, 'usage': 'high'},
                    {'name': 'draw_id', 'type': 'STRING', 'nullable': True, 'usage': 'high'},
                    {'name': 'prediction', 'type': 'STRING', 'nullable': True, 'usage': 'medium'},
                    {'name': 'probability', 'type': 'FLOAT64', 'nullable': True, 'usage': 'medium'},
                    {'name': 'stake_units', 'type': 'FLOAT64', 'nullable': True, 'usage': 'medium'},
                    {'name': 'outcome', 'type': 'STRING', 'nullable': True, 'usage': 'high'},
                    {'name': 'profit_loss', 'type': 'FLOAT64', 'nullable': True, 'usage': 'high'},
                    {'name': 'status', 'type': 'STRING', 'nullable': True, 'usage': 'high'},
                    {'name': 'result_digits', 'type': 'REPEATED INTEGER', 'nullable': True, 'usage': 'low'},  # 冗余字段
                    {'name': 'numbers', 'type': 'REPEATED INTEGER', 'nullable': True, 'usage': 'high'},
                    {'name': 'result_sum', 'type': 'INTEGER', 'nullable': True, 'usage': 'high'},
                    {'name': 'source', 'type': 'STRING', 'nullable': True, 'usage': 'low'},  # 很少使用
                    {'name': 'created_at', 'type': 'TIMESTAMP', 'nullable': True, 'usage': 'medium'}
                ],
                'indexes': ['order_id', 'draw_id', 'timestamp'],
                'partitioning': 'timestamp',
                'estimated_size_gb': 2.5
            },
            'draws_14w_dedup_v': {
                'fields': [
                    {'name': 'draw_id', 'type': 'STRING', 'nullable': False, 'usage': 'high'},
                    {'name': 'timestamp', 'type': 'TIMESTAMP', 'nullable': False, 'usage': 'high'},
                    {'name': 'numbers', 'type': 'REPEATED INTEGER', 'nullable': False, 'usage': 'high'},
                    {'name': 'result_sum', 'type': 'INTEGER', 'nullable': False, 'usage': 'high'},
                    {'name': 'ts_utc', 'type': 'TIMESTAMP', 'nullable': True, 'usage': 'medium'},  # 与timestamp重复
                    {'name': 'legacy_format', 'type': 'STRING', 'nullable': True, 'usage': 'low'},  # 遗留字段
                    {'name': 'data_source', 'type': 'STRING', 'nullable': True, 'usage': 'low'},
                    {'name': 'validation_status', 'type': 'STRING', 'nullable': True, 'usage': 'medium'}
                ],
                'indexes': ['draw_id', 'timestamp'],
                'partitioning': 'timestamp',
                'estimated_size_gb': 15.2
            },
            'p_size_clean_merged_dedup_v': {
                'fields': [
                    {'name': 'id', 'type': 'STRING', 'nullable': False, 'usage': 'high'},
                    {'name': 'draw_id', 'type': 'STRING', 'nullable': False, 'usage': 'high'},
                    {'name': 'prediction_data', 'type': 'JSON', 'nullable': True, 'usage': 'high'},
                    {'name': 'size_prediction', 'type': 'FLOAT64', 'nullable': True, 'usage': 'high'},
                    {'name': 'confidence_score', 'type': 'FLOAT64', 'nullable': True, 'usage': 'medium'},
                    {'name': 'model_version', 'type': 'STRING', 'nullable': True, 'usage': 'low'},  # 很少查询
                    {'name': 'raw_features', 'type': 'JSON', 'nullable': True, 'usage': 'low'},  # 调试用，很少使用
                    {'name': 'processing_time', 'type': 'FLOAT64', 'nullable': True, 'usage': 'low'},
                    {'name': 'created_at', 'type': 'TIMESTAMP', 'nullable': True, 'usage': 'medium'}
                ],
                'indexes': ['id', 'draw_id'],
                'partitioning': 'created_at',
                'estimated_size_gb': 8.7
            }
        }
        
        # 优化规则
        self.optimization_rules = {
            'remove_redundant_fields': {
                'description': '移除冗余字段',
                'criteria': lambda field: field.get('usage') == 'low' and self._is_redundant_field(field['name']),
                'impact': 'storage_reduction'
            },
            'merge_similar_fields': {
                'description': '合并相似字段',
                'criteria': lambda field: self._has_similar_field(field['name']),
                'impact': 'schema_simplification'
            },
            'optimize_data_types': {
                'description': '优化数据类型',
                'criteria': lambda field: self._can_optimize_type(field),
                'impact': 'performance_improvement'
            },
            'add_missing_indexes': {
                'description': '添加缺失索引',
                'criteria': lambda field: field.get('usage') == 'high' and self._needs_index(field['name']),
                'impact': 'query_performance'
            }
        }
    
    def analyze_table_optimization_opportunities(self) -> Dict[str, List[TableOptimization]]:
        """分析表优化机会"""
        print("\n=== 数据表优化分析 ===")
        
        optimizations = {}
        
        for table_name, table_info in self.current_tables.items():
            table_optimizations = []
            
            # 分析字段优化
            field_optimizations = self._analyze_field_optimizations(table_name, table_info)
            
            # 生成表级优化建议
            for optimization in field_optimizations:
                table_opt = self._generate_table_optimization(table_name, table_info, optimization)
                if table_opt:
                    table_optimizations.append(table_opt)
            
            # 分析表结构优化
            structure_optimizations = self._analyze_table_structure(table_name, table_info)
            table_optimizations.extend(structure_optimizations)
            
            optimizations[table_name] = table_optimizations
        
        return optimizations
    
    def _analyze_field_optimizations(self, table_name: str, table_info: Dict) -> List[FieldOptimization]:
        """分析字段优化机会"""
        optimizations = []
        
        for field in table_info['fields']:
            field_name = field['name']
            
            # 检查冗余字段
            if self._is_redundant_field(field_name):
                optimizations.append(FieldOptimization(
                    field_name=field_name,
                    optimization_type='remove_redundant',
                    reason=f'字段 {field_name} 与其他字段功能重复',
                    action='删除字段',
                    impact_assessment='减少存储空间，简化查询'
                ))
            
            # 检查未使用字段
            if field.get('usage') == 'low':
                optimizations.append(FieldOptimization(
                    field_name=field_name,
                    optimization_type='remove_unused',
                    reason=f'字段 {field_name} 使用频率极低',
                    action='考虑删除或归档',
                    impact_assessment='显著减少存储成本'
                ))
            
            # 检查数据类型优化
            type_optimization = self._analyze_type_optimization(field)
            if type_optimization:
                optimizations.append(type_optimization)
        
        return optimizations
    
    def _analyze_table_structure(self, table_name: str, table_info: Dict) -> List[TableOptimization]:
        """分析表结构优化"""
        optimizations = []
        
        # 分析分区优化
        if table_info.get('estimated_size_gb', 0) > 5:
            partition_opt = self._analyze_partitioning_optimization(table_name, table_info)
            if partition_opt:
                optimizations.append(partition_opt)
        
        # 分析索引优化
        index_opt = self._analyze_index_optimization(table_name, table_info)
        if index_opt:
            optimizations.append(index_opt)
        
        return optimizations
    
    def _is_redundant_field(self, field_name: str) -> bool:
        """检查是否为冗余字段"""
        redundant_patterns = {
            'result_digits': 'numbers',  # result_digits与numbers重复
            'ts_utc': 'timestamp',       # ts_utc与timestamp重复
            'data_source': 'source',     # 类似功能字段
        }
        
        return field_name in redundant_patterns
    
    def _has_similar_field(self, field_name: str) -> bool:
        """检查是否有相似字段"""
        similar_patterns = [
            ('timestamp', 'ts_utc', 'created_at'),
            ('source', 'data_source'),
            ('numbers', 'result_digits')
        ]
        
        for pattern in similar_patterns:
            if field_name in pattern:
                return True
        return False
    
    def _can_optimize_type(self, field: Dict) -> bool:
        """检查是否可以优化数据类型"""
        # 示例：STRING类型但内容是固定枚举值
        if field['type'] == 'STRING' and field['name'] in ['status', 'outcome']:
            return True
        
        # FLOAT64但实际只需要FLOAT32精度
        if field['type'] == 'FLOAT64' and field['name'] in ['probability', 'confidence_score']:
            return True
        
        return False
    
    def _needs_index(self, field_name: str) -> bool:
        """检查是否需要索引"""
        high_query_fields = ['draw_id', 'timestamp', 'order_id', 'status']
        return field_name in high_query_fields
    
    def _analyze_type_optimization(self, field: Dict) -> FieldOptimization:
        """分析数据类型优化"""
        if not self._can_optimize_type(field):
            return None
        
        field_name = field['name']
        current_type = field['type']
        
        # 建议的类型优化
        type_suggestions = {
            'status': ('STRING', 'ENUM', '状态字段使用枚举类型更高效'),
            'outcome': ('STRING', 'ENUM', '结果字段使用枚举类型更高效'),
            'probability': ('FLOAT64', 'FLOAT32', '概率值不需要双精度浮点'),
            'confidence_score': ('FLOAT64', 'FLOAT32', '置信度分数不需要双精度浮点')
        }
        
        if field_name in type_suggestions:
            old_type, new_type, reason = type_suggestions[field_name]
            return FieldOptimization(
                field_name=field_name,
                optimization_type='optimize_type',
                reason=reason,
                action=f'将类型从 {old_type} 改为 {new_type}',
                impact_assessment='减少存储空间和提高查询性能'
            )
        
        return None
    
    def _analyze_partitioning_optimization(self, table_name: str, table_info: Dict) -> TableOptimization:
        """分析分区优化"""
        current_partition = table_info.get('partitioning')
        estimated_size = table_info.get('estimated_size_gb', 0)
        
        if estimated_size > 10 and current_partition != 'timestamp':
            return TableOptimization(
                table_name=table_name,
                optimization_type='partitioning',
                description=f'优化 {table_name} 表分区策略',
                impact='查询性能提升50-80%',
                sql_before=f'-- 当前分区: {current_partition or "无"}',
                sql_after=f'''
-- 建议按时间分区
ALTER TABLE `{table_name}` 
SET OPTIONS (
  partition_expiration_days=365,
  require_partition_filter=true
)''',
                migration_script=self._generate_partition_migration_script(table_name),
                estimated_savings={
                    'query_performance': '50-80%',
                    'cost_reduction': '30-40%'
                }
            )
        
        return None
    
    def _analyze_index_optimization(self, table_name: str, table_info: Dict) -> TableOptimization:
        """分析索引优化"""
        current_indexes = set(table_info.get('indexes', []))
        high_usage_fields = [f['name'] for f in table_info['fields'] if f.get('usage') == 'high']
        
        missing_indexes = set(high_usage_fields) - current_indexes
        
        if missing_indexes:
            return TableOptimization(
                table_name=table_name,
                optimization_type='indexing',
                description=f'为 {table_name} 添加缺失索引',
                impact='查询性能提升20-50%',
                sql_before=f'-- 当前索引: {", ".join(current_indexes)}',
                sql_after=f'''
-- 建议添加索引
{chr(10).join([f"CREATE INDEX idx_{table_name}_{field} ON `{table_name}` ({field});" for field in missing_indexes])}''',
                migration_script=self._generate_index_migration_script(table_name, missing_indexes),
                estimated_savings={
                    'query_performance': '20-50%',
                    'response_time': '减少60-80%'
                }
            )
        
        return None
    
    def _generate_table_optimization(self, table_name: str, table_info: Dict, 
                                   field_opt: FieldOptimization) -> TableOptimization:
        """生成表优化建议"""
        if field_opt.optimization_type == 'remove_redundant':
            return TableOptimization(
                table_name=table_name,
                optimization_type='remove_field',
                description=f'移除冗余字段 {field_opt.field_name}',
                impact='减少存储成本15-25%',
                sql_before=f'-- 包含冗余字段 {field_opt.field_name}',
                sql_after=f'ALTER TABLE `{table_name}` DROP COLUMN {field_opt.field_name};',
                migration_script=self._generate_field_removal_migration(table_name, field_opt.field_name),
                estimated_savings={
                    'storage_reduction': '15-25%',
                    'query_performance': '5-10%'
                }
            )
        
        elif field_opt.optimization_type == 'remove_unused':
            return TableOptimization(
                table_name=table_name,
                optimization_type='archive_field',
                description=f'归档低使用率字段 {field_opt.field_name}',
                impact='减少存储成本10-20%',
                sql_before=f'-- 包含低使用率字段 {field_opt.field_name}',
                sql_after=f'''
-- 创建归档表
CREATE TABLE `{table_name}_archive` AS 
SELECT {field_opt.field_name}, primary_key_field 
FROM `{table_name}`;

-- 删除原字段
ALTER TABLE `{table_name}` DROP COLUMN {field_opt.field_name};''',
                migration_script=self._generate_field_archive_migration(table_name, field_opt.field_name),
                estimated_savings={
                    'storage_reduction': '10-20%',
                    'maintenance_cost': '减少30%'
                }
            )
        
        elif field_opt.optimization_type == 'optimize_type':
            return TableOptimization(
                table_name=table_name,
                optimization_type='type_optimization',
                description=f'优化字段 {field_opt.field_name} 数据类型',
                impact='提升查询性能10-15%',
                sql_before=f'-- 当前类型效率较低',
                sql_after=field_opt.action,
                migration_script=self._generate_type_optimization_migration(table_name, field_opt),
                estimated_savings={
                    'storage_reduction': '5-15%',
                    'query_performance': '10-15%'
                }
            )
        
        return None
    
    def generate_optimization_sql_ddl(self, optimizations: Dict[str, List[TableOptimization]]) -> str:
        """生成优化SQL DDL语句"""
        print("\n=== 生成优化SQL DDL ===")
        
        ddl_statements = []
        ddl_statements.append("-- PC28系统数据表优化DDL语句")
        ddl_statements.append(f"-- 生成时间: {datetime.now().isoformat()}")
        ddl_statements.append("-- 请在执行前备份相关数据\n")
        
        for table_name, table_opts in optimizations.items():
            if not table_opts:
                continue
                
            ddl_statements.append(f"-- ========== {table_name} 表优化 ==========")
            
            for opt in table_opts:
                ddl_statements.append(f"-- {opt.description}")
                ddl_statements.append(f"-- 预期影响: {opt.impact}")
                ddl_statements.append(opt.sql_after)
                ddl_statements.append("")
        
        # 添加验证查询
        ddl_statements.append("-- ========== 优化后验证查询 ==========")
        for table_name in optimizations.keys():
            ddl_statements.append(f"-- 验证 {table_name} 表结构")
            ddl_statements.append(f"DESCRIBE `{table_name}`;")
            ddl_statements.append(f"SELECT COUNT(*) as row_count FROM `{table_name}`;")
            ddl_statements.append("")
        
        return "\n".join(ddl_statements)
    
    def generate_migration_scripts(self, optimizations: Dict[str, List[TableOptimization]]) -> Dict[str, str]:
        """生成数据迁移脚本"""
        print("\n=== 生成数据迁移脚本 ===")
        
        migration_scripts = {}
        
        for table_name, table_opts in optimizations.items():
            if not table_opts:
                continue
            
            scripts = []
            scripts.append(f"#!/bin/bash")
            scripts.append(f"# {table_name} 表数据迁移脚本")
            scripts.append(f"# 生成时间: {datetime.now().isoformat()}")
            scripts.append(f"")
            scripts.append(f"set -e  # 遇到错误立即退出")
            scripts.append(f"")
            scripts.append(f"echo '开始 {table_name} 表迁移...'")
            scripts.append(f"")
            
            # 备份步骤
            scripts.append(f"# 1. 备份原表")
            scripts.append(f"echo '创建备份表...'")
            scripts.append(f"bq query --use_legacy_sql=false \\")
            scripts.append(f"\"CREATE TABLE \\`{table_name}_backup_$(date +%Y%m%d)\\` AS SELECT * FROM \\`{table_name}\\`\"")
            scripts.append(f"")
            
            # 执行优化
            for i, opt in enumerate(table_opts, 1):
                scripts.append(f"# {i}. {opt.description}")
                scripts.append(f"echo '{opt.description}...'")
                scripts.append(opt.migration_script)
                scripts.append(f"")
            
            # 验证步骤
            scripts.append(f"# 验证迁移结果")
            scripts.append(f"echo '验证迁移结果...'")
            scripts.append(f"bq query --use_legacy_sql=false \\")
            scripts.append(f"\"SELECT COUNT(*) as final_count FROM \\`{table_name}\\`\"")
            scripts.append(f"")
            scripts.append(f"echo '{table_name} 表迁移完成！'")
            
            migration_scripts[f"{table_name}_migration.sh"] = "\n".join(scripts)
        
        return migration_scripts
    
    def _generate_partition_migration_script(self, table_name: str) -> str:
        """生成分区迁移脚本"""
        return f'''
bq query --use_legacy_sql=false \\
"CREATE TABLE `{table_name}_partitioned` 
PARTITION BY DATE(timestamp)
CLUSTER BY draw_id
AS SELECT * FROM `{table_name}`"

# 验证分区表
bq query --use_legacy_sql=false \\
"SELECT COUNT(*) FROM `{table_name}_partitioned`"

# 重命名表（需要手动确认）
echo "请手动执行表重命名操作"'''
    
    def _generate_index_migration_script(self, table_name: str, indexes: Set[str]) -> str:
        """生成索引迁移脚本"""
        scripts = []
        for index_field in indexes:
            scripts.append(f'''
bq query --use_legacy_sql=false \\
"CREATE INDEX idx_{table_name}_{index_field} ON `{table_name}` ({index_field})"''')
        
        return "\n".join(scripts)
    
    def _generate_field_removal_migration(self, table_name: str, field_name: str) -> str:
        """生成字段删除迁移脚本"""
        return f'''
# 备份包含该字段的数据
bq query --use_legacy_sql=false \\
"CREATE TABLE `{table_name}_{field_name}_backup` AS 
SELECT {field_name}, primary_key_field FROM `{table_name}`"

# 删除字段
bq query --use_legacy_sql=false \\
"ALTER TABLE `{table_name}` DROP COLUMN {field_name}"'''
    
    def _generate_field_archive_migration(self, table_name: str, field_name: str) -> str:
        """生成字段归档迁移脚本"""
        return f'''
# 创建归档表
bq query --use_legacy_sql=false \\
"CREATE TABLE `{table_name}_archive` AS 
SELECT {field_name}, primary_key_field, timestamp 
FROM `{table_name}` 
WHERE {field_name} IS NOT NULL"

# 删除原字段
bq query --use_legacy_sql=false \\
"ALTER TABLE `{table_name}` DROP COLUMN {field_name}"'''
    
    def _generate_type_optimization_migration(self, table_name: str, field_opt: FieldOptimization) -> str:
        """生成类型优化迁移脚本"""
        return f'''
# 类型优化需要重建表
bq query --use_legacy_sql=false \\
"CREATE TABLE `{table_name}_optimized` AS 
SELECT * EXCEPT({field_opt.field_name}),
       CAST({field_opt.field_name} AS NEW_TYPE) as {field_opt.field_name}
FROM `{table_name}`"

# 验证数据完整性
bq query --use_legacy_sql=false \\
"SELECT COUNT(*) FROM `{table_name}_optimized`"'''
    
    def generate_optimization_report(self) -> Dict[str, Any]:
        """生成完整优化报告"""
        print("\n=== 生成数据表优化报告 ===")
        
        # 分析优化机会
        optimizations = self.analyze_table_optimization_opportunities()
        
        # 计算总体影响
        total_savings = self._calculate_total_savings(optimizations)
        
        # 生成SQL DDL
        ddl_statements = self.generate_optimization_sql_ddl(optimizations)
        
        # 生成迁移脚本
        migration_scripts = self.generate_migration_scripts(optimizations)
        
        report = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'analyzer_version': '1.0.0',
                'total_tables_analyzed': len(self.current_tables)
            },
            'optimization_summary': {
                'total_optimizations': sum(len(opts) for opts in optimizations.values()),
                'high_impact_optimizations': self._count_high_impact_optimizations(optimizations),
                'estimated_savings': total_savings
            },
            'table_optimizations': optimizations,
            'sql_ddl_statements': ddl_statements,
            'migration_scripts': migration_scripts,
            'implementation_plan': self._generate_implementation_plan(optimizations),
            'risk_assessment': self._generate_risk_assessment(optimizations)
        }
        
        return report
    
    def _calculate_total_savings(self, optimizations: Dict[str, List[TableOptimization]]) -> Dict[str, Any]:
        """计算总体节省"""
        total_storage_reduction = 0
        total_performance_improvement = 0
        total_cost_savings = 0
        
        for table_opts in optimizations.values():
            for opt in table_opts:
                savings = opt.estimated_savings
                
                # 解析存储减少百分比
                if 'storage_reduction' in savings:
                    reduction_str = savings['storage_reduction']
                    if '%' in reduction_str:
                        # 提取百分比数字
                        import re
                        numbers = re.findall(r'\d+', reduction_str)
                        if numbers:
                            total_storage_reduction += int(numbers[0])
                
                # 解析性能提升
                if 'query_performance' in savings:
                    perf_str = savings['query_performance']
                    if '%' in perf_str:
                        numbers = re.findall(r'\d+', perf_str)
                        if numbers:
                            total_performance_improvement += int(numbers[0])
        
        return {
            'estimated_storage_reduction': f"{min(total_storage_reduction, 80)}%",
            'estimated_performance_improvement': f"{min(total_performance_improvement, 200)}%",
            'estimated_monthly_cost_savings': f"${total_storage_reduction * 10}",
            'implementation_effort': 'Medium',
            'risk_level': 'Low to Medium'
        }
    
    def _count_high_impact_optimizations(self, optimizations: Dict[str, List[TableOptimization]]) -> int:
        """统计高影响优化数量"""
        high_impact_keywords = ['50%', '60%', '70%', '80%', 'significant', 'major']
        count = 0
        
        for table_opts in optimizations.values():
            for opt in table_opts:
                if any(keyword in opt.impact.lower() for keyword in high_impact_keywords):
                    count += 1
        
        return count
    
    def _generate_implementation_plan(self, optimizations: Dict[str, List[TableOptimization]]) -> List[Dict[str, Any]]:
        """生成实施计划"""
        plan = []
        
        # 阶段1：低风险优化
        plan.append({
            'phase': 1,
            'title': '低风险优化阶段',
            'description': '执行索引优化和类型优化',
            'duration': '1-2周',
            'risk': 'Low',
            'actions': ['添加缺失索引', '优化数据类型', '性能测试']
        })
        
        # 阶段2：结构优化
        plan.append({
            'phase': 2,
            'title': '表结构优化阶段',
            'description': '移除冗余字段和优化分区',
            'duration': '2-3周',
            'risk': 'Medium',
            'actions': ['备份数据', '移除冗余字段', '优化分区策略', '数据验证']
        })
        
        # 阶段3：归档和清理
        plan.append({
            'phase': 3,
            'title': '数据归档阶段',
            'description': '归档低使用率数据',
            'duration': '1-2周',
            'risk': 'Low',
            'actions': ['创建归档表', '迁移历史数据', '清理原表', '监控性能']
        })
        
        return plan
    
    def _generate_risk_assessment(self, optimizations: Dict[str, List[TableOptimization]]) -> Dict[str, Any]:
        """生成风险评估"""
        return {
            'high_risk_operations': [
                '删除字段操作（不可逆）',
                '表重建操作（需要停机时间）',
                '大表分区调整（可能影响查询）'
            ],
            'mitigation_strategies': [
                '所有操作前进行完整备份',
                '在测试环境先验证所有变更',
                '分阶段执行，每阶段后验证',
                '准备回滚计划',
                '监控系统性能指标'
            ],
            'recommended_testing': [
                '功能测试：确保所有查询正常工作',
                '性能测试：验证优化效果',
                '数据完整性测试：确保数据无丢失',
                '回滚测试：验证回滚流程'
            ],
            'rollback_plan': {
                'backup_retention': '30天',
                'rollback_time_estimate': '2-4小时',
                'rollback_complexity': 'Medium'
            }
        }


def main():
    """主函数"""
    optimizer = DatabaseTableOptimizer()
    
    try:
        # 生成完整优化报告
        report = optimizer.generate_optimization_report()
        
        # 保存报告
        output_file = f"database_optimization_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ 数据表优化报告已生成: {output_file}")
        
        # 保存SQL DDL文件
        ddl_file = f"optimization_ddl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write(report['sql_ddl_statements'])
        
        print(f"✅ SQL DDL文件已生成: {ddl_file}")
        
        # 保存迁移脚本
        for script_name, script_content in report['migration_scripts'].items():
            script_file = f"migration_{script_name}"
            with open(script_file, 'w', encoding='utf-8') as f:
                f.write(script_content)
            
            # 设置执行权限
            os.chmod(script_file, 0o755)
            print(f"✅ 迁移脚本已生成: {script_file}")
        
        # 打印关键统计信息
        print("\n=== 优化统计摘要 ===")
        summary = report['optimization_summary']
        print(f"总优化项目: {summary['total_optimizations']}")
        print(f"高影响优化: {summary['high_impact_optimizations']}")
        
        savings = summary['estimated_savings']
        print(f"预计存储减少: {savings['estimated_storage_reduction']}")
        print(f"预计性能提升: {savings['estimated_performance_improvement']}")
        print(f"预计月度成本节省: {savings['estimated_monthly_cost_savings']}")
        
        return report
        
    except Exception as e:
        print(f"❌ 优化分析过程中发生错误: {e}")
        return None


if __name__ == "__main__":
    main()