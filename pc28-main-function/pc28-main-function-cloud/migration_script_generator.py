#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据迁移脚本生成器
自动生成数据库迁移脚本，支持字段删除、表重构、数据归档等操作
"""

import json
import sys
import os
from typing import Dict, List, Any, Set, Tuple, Optional
from datetime import datetime, timedelta
import re
from dataclasses import dataclass
from pathlib import Path
import hashlib

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database_table_optimizer import DatabaseTableOptimizer, TableOptimization


@dataclass
class MigrationStep:
    """迁移步骤"""
    step_id: str
    step_type: str
    description: str
    sql_command: str
    rollback_command: str
    validation_query: str
    estimated_duration: str
    risk_level: str
    dependencies: List[str]


@dataclass
class MigrationPlan:
    """迁移计划"""
    plan_id: str
    plan_name: str
    description: str
    total_steps: int
    estimated_duration: str
    risk_assessment: str
    steps: List[MigrationStep]
    rollback_plan: str
    validation_checklist: List[str]


class MigrationScriptGenerator:
    """迁移脚本生成器"""
    
    def __init__(self):
        self.optimizer = DatabaseTableOptimizer()
        
        # 迁移模板
        self.migration_templates = {
            'field_removal': {
                'backup_template': '''
-- 备份包含待删除字段的数据
CREATE TABLE `{table_name}_{field_name}_backup_{timestamp}` AS
SELECT {field_name}, {primary_key_fields}
FROM `{table_name}`
WHERE {field_name} IS NOT NULL;
''',
                'removal_template': '''
-- 删除字段 {field_name}
ALTER TABLE `{table_name}` DROP COLUMN {field_name};
''',
                'validation_template': '''
-- 验证字段删除
SELECT column_name 
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table_name}' AND column_name = '{field_name}';
'''
            },
            'table_partitioning': {
                'create_partitioned_template': '''
-- 创建分区表
CREATE TABLE `{table_name}_partitioned`
PARTITION BY DATE({partition_field})
CLUSTER BY {cluster_fields}
AS SELECT * FROM `{table_name}`;
''',
                'swap_tables_template': '''
-- 交换表名（需要手动执行）
-- 1. 重命名原表
-- ALTER TABLE `{table_name}` RENAME TO `{table_name}_old`;
-- 2. 重命名新表
-- ALTER TABLE `{table_name}_partitioned` RENAME TO `{table_name}`;
''',
                'cleanup_template': '''
-- 清理旧表（确认数据正确后执行）
-- DROP TABLE `{table_name}_old`;
'''
            },
            'index_creation': {
                'create_index_template': '''
-- 创建索引 {index_name}
CREATE INDEX {index_name} ON `{table_name}` ({index_fields});
''',
                'validation_template': '''
-- 验证索引创建
SELECT index_name, table_name 
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.INDEXES`
WHERE table_name = '{table_name}' AND index_name = '{index_name}';
'''
            },
            'data_archiving': {
                'archive_template': '''
-- 创建归档表
CREATE TABLE `{archive_table_name}` AS
SELECT * FROM `{table_name}`
WHERE {archive_condition};
''',
                'cleanup_template': '''
-- 删除已归档数据
DELETE FROM `{table_name}`
WHERE {archive_condition};
''',
                'validation_template': '''
-- 验证归档结果
SELECT 
  (SELECT COUNT(*) FROM `{archive_table_name}`) as archived_count,
  (SELECT COUNT(*) FROM `{table_name}`) as remaining_count;
'''
            }
        }
        
        # 风险评估规则
        self.risk_rules = {
            'field_removal': 'HIGH',      # 不可逆操作
            'table_partitioning': 'MEDIUM', # 需要停机时间
            'index_creation': 'LOW',       # 可回滚
            'data_archiving': 'MEDIUM',    # 数据移动
            'type_optimization': 'HIGH'    # 可能数据丢失
        }
    
    def generate_migration_plan(self, optimizations: Dict[str, List[TableOptimization]]) -> MigrationPlan:
        """生成完整迁移计划"""
        print("\n=== 生成数据迁移计划 ===")
        
        plan_id = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 生成迁移步骤
        all_steps = []
        step_counter = 1
        
        for table_name, table_opts in optimizations.items():
            for opt in table_opts:
                steps = self._generate_optimization_steps(table_name, opt, step_counter)
                all_steps.extend(steps)
                step_counter += len(steps)
        
        # 排序步骤（按风险级别和依赖关系）
        sorted_steps = self._sort_migration_steps(all_steps)
        
        # 计算总体评估
        total_duration = self._calculate_total_duration(sorted_steps)
        risk_assessment = self._assess_overall_risk(sorted_steps)
        
        # 生成回滚计划
        rollback_plan = self._generate_rollback_plan(sorted_steps)
        
        # 生成验证清单
        validation_checklist = self._generate_validation_checklist(sorted_steps)
        
        return MigrationPlan(
            plan_id=plan_id,
            plan_name=f"PC28数据库优化迁移计划",
            description=f"基于字段使用分析的数据库优化迁移，包含{len(sorted_steps)}个步骤",
            total_steps=len(sorted_steps),
            estimated_duration=total_duration,
            risk_assessment=risk_assessment,
            steps=sorted_steps,
            rollback_plan=rollback_plan,
            validation_checklist=validation_checklist
        )
    
    def _generate_optimization_steps(self, table_name: str, optimization: TableOptimization, 
                                   start_step_id: int) -> List[MigrationStep]:
        """为单个优化生成迁移步骤"""
        steps = []
        
        if optimization.optimization_type == 'remove_field':
            steps.extend(self._generate_field_removal_steps(table_name, optimization, start_step_id))
        
        elif optimization.optimization_type == 'partitioning':
            steps.extend(self._generate_partitioning_steps(table_name, optimization, start_step_id))
        
        elif optimization.optimization_type == 'indexing':
            steps.extend(self._generate_indexing_steps(table_name, optimization, start_step_id))
        
        elif optimization.optimization_type == 'archive_field':
            steps.extend(self._generate_archiving_steps(table_name, optimization, start_step_id))
        
        return steps
    
    def _generate_field_removal_steps(self, table_name: str, optimization: TableOptimization, 
                                    start_step_id: int) -> List[MigrationStep]:
        """生成字段删除步骤"""
        steps = []
        
        # 从描述中提取字段名
        field_name = self._extract_field_name_from_description(optimization.description)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 步骤1: 备份数据
        backup_step = MigrationStep(
            step_id=f"step_{start_step_id:03d}",
            step_type="backup",
            description=f"备份 {table_name}.{field_name} 字段数据",
            sql_command=self.migration_templates['field_removal']['backup_template'].format(
                table_name=table_name,
                field_name=field_name,
                timestamp=timestamp,
                primary_key_fields=self._get_primary_key_fields(table_name)
            ).strip(),
            rollback_command=f"DROP TABLE `{table_name}_{field_name}_backup_{timestamp}`;",
            validation_query=f"SELECT COUNT(*) FROM `{table_name}_{field_name}_backup_{timestamp}`;",
            estimated_duration="5-10分钟",
            risk_level="LOW",
            dependencies=[]
        )
        steps.append(backup_step)
        
        # 步骤2: 删除字段
        removal_step = MigrationStep(
            step_id=f"step_{start_step_id+1:03d}",
            step_type="field_removal",
            description=f"删除 {table_name}.{field_name} 字段",
            sql_command=self.migration_templates['field_removal']['removal_template'].format(
                table_name=table_name,
                field_name=field_name
            ).strip(),
            rollback_command=f"-- 字段删除不可直接回滚，需要从备份恢复",
            validation_query=self.migration_templates['field_removal']['validation_template'].format(
                project_id="your_project_id",
                dataset_id="your_dataset_id",
                table_name=table_name,
                field_name=field_name
            ).strip(),
            estimated_duration="2-5分钟",
            risk_level="HIGH",
            dependencies=[backup_step.step_id]
        )
        steps.append(removal_step)
        
        return steps
    
    def _generate_partitioning_steps(self, table_name: str, optimization: TableOptimization, 
                                   start_step_id: int) -> List[MigrationStep]:
        """生成分区步骤"""
        steps = []
        
        # 步骤1: 创建分区表
        create_step = MigrationStep(
            step_id=f"step_{start_step_id:03d}",
            step_type="create_partitioned_table",
            description=f"创建 {table_name} 的分区表",
            sql_command=self.migration_templates['table_partitioning']['create_partitioned_template'].format(
                table_name=table_name,
                partition_field="timestamp",
                cluster_fields="draw_id"
            ).strip(),
            rollback_command=f"DROP TABLE `{table_name}_partitioned`;",
            validation_query=f"SELECT COUNT(*) FROM `{table_name}_partitioned`;",
            estimated_duration="30-60分钟",
            risk_level="MEDIUM",
            dependencies=[]
        )
        steps.append(create_step)
        
        # 步骤2: 表名交换（手动步骤）
        swap_step = MigrationStep(
            step_id=f"step_{start_step_id+1:03d}",
            step_type="table_swap",
            description=f"交换 {table_name} 表名（手动执行）",
            sql_command=self.migration_templates['table_partitioning']['swap_tables_template'].format(
                table_name=table_name
            ).strip(),
            rollback_command=f"-- 交换回原来的表名",
            validation_query=f"SELECT table_name FROM `INFORMATION_SCHEMA.TABLES` WHERE table_name = '{table_name}';",
            estimated_duration="5-10分钟",
            risk_level="HIGH",
            dependencies=[create_step.step_id]
        )
        steps.append(swap_step)
        
        return steps
    
    def _generate_indexing_steps(self, table_name: str, optimization: TableOptimization, 
                               start_step_id: int) -> List[MigrationStep]:
        """生成索引创建步骤"""
        steps = []
        
        # 从优化描述中提取索引信息
        index_fields = self._extract_index_fields_from_optimization(optimization)
        
        for i, field in enumerate(index_fields):
            index_name = f"idx_{table_name}_{field}"
            
            index_step = MigrationStep(
                step_id=f"step_{start_step_id+i:03d}",
                step_type="create_index",
                description=f"为 {table_name}.{field} 创建索引",
                sql_command=self.migration_templates['index_creation']['create_index_template'].format(
                    index_name=index_name,
                    table_name=table_name,
                    index_fields=field
                ).strip(),
                rollback_command=f"DROP INDEX {index_name};",
                validation_query=self.migration_templates['index_creation']['validation_template'].format(
                    project_id="your_project_id",
                    dataset_id="your_dataset_id",
                    table_name=table_name,
                    index_name=index_name
                ).strip(),
                estimated_duration="10-20分钟",
                risk_level="LOW",
                dependencies=[]
            )
            steps.append(index_step)
        
        return steps
    
    def _generate_archiving_steps(self, table_name: str, optimization: TableOptimization, 
                                start_step_id: int) -> List[MigrationStep]:
        """生成数据归档步骤"""
        steps = []
        
        field_name = self._extract_field_name_from_description(optimization.description)
        archive_table_name = f"{table_name}_{field_name}_archive"
        archive_condition = f"{field_name} IS NOT NULL"
        
        # 步骤1: 创建归档表
        archive_step = MigrationStep(
            step_id=f"step_{start_step_id:03d}",
            step_type="create_archive",
            description=f"创建 {field_name} 字段归档表",
            sql_command=self.migration_templates['data_archiving']['archive_template'].format(
                archive_table_name=archive_table_name,
                table_name=table_name,
                archive_condition=archive_condition
            ).strip(),
            rollback_command=f"DROP TABLE `{archive_table_name}`;",
            validation_query=self.migration_templates['data_archiving']['validation_template'].format(
                archive_table_name=archive_table_name,
                table_name=table_name
            ).strip(),
            estimated_duration="20-40分钟",
            risk_level="MEDIUM",
            dependencies=[]
        )
        steps.append(archive_step)
        
        # 步骤2: 删除原字段
        cleanup_step = MigrationStep(
            step_id=f"step_{start_step_id+1:03d}",
            step_type="field_removal",
            description=f"删除已归档的 {field_name} 字段",
            sql_command=f"ALTER TABLE `{table_name}` DROP COLUMN {field_name};",
            rollback_command=f"-- 需要从归档表恢复数据",
            validation_query=f"SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}' AND column_name = '{field_name}';",
            estimated_duration="5-10分钟",
            risk_level="HIGH",
            dependencies=[archive_step.step_id]
        )
        steps.append(cleanup_step)
        
        return steps
    
    def _sort_migration_steps(self, steps: List[MigrationStep]) -> List[MigrationStep]:
        """按依赖关系和风险级别排序迁移步骤"""
        # 简单的拓扑排序实现
        sorted_steps = []
        remaining_steps = steps.copy()
        
        while remaining_steps:
            # 找到没有未满足依赖的步骤
            ready_steps = []
            for step in remaining_steps:
                if all(dep_id in [s.step_id for s in sorted_steps] for dep_id in step.dependencies):
                    ready_steps.append(step)
            
            if not ready_steps:
                # 如果没有可执行的步骤，可能存在循环依赖
                ready_steps = [remaining_steps[0]]  # 强制选择一个
            
            # 按风险级别排序（LOW -> MEDIUM -> HIGH）
            risk_order = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
            ready_steps.sort(key=lambda x: risk_order.get(x.risk_level, 2))
            
            # 添加第一个就绪步骤
            next_step = ready_steps[0]
            sorted_steps.append(next_step)
            remaining_steps.remove(next_step)
        
        return sorted_steps
    
    def _calculate_total_duration(self, steps: List[MigrationStep]) -> str:
        """计算总迁移时间"""
        total_minutes = 0
        
        for step in steps:
            duration_str = step.estimated_duration
            # 提取分钟数（简单解析）
            if '分钟' in duration_str:
                numbers = re.findall(r'\d+', duration_str)
                if numbers:
                    # 取最大值作为估计
                    max_minutes = max(int(n) for n in numbers)
                    total_minutes += max_minutes
        
        hours = total_minutes // 60
        minutes = total_minutes % 60
        
        if hours > 0:
            return f"{hours}小时{minutes}分钟"
        else:
            return f"{minutes}分钟"
    
    def _assess_overall_risk(self, steps: List[MigrationStep]) -> str:
        """评估整体风险"""
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0}
        
        for step in steps:
            risk_counts[step.risk_level] += 1
        
        if risk_counts['HIGH'] > 0:
            return "HIGH - 包含不可逆操作，需要谨慎执行"
        elif risk_counts['MEDIUM'] > 2:
            return "MEDIUM - 包含多个中等风险操作"
        else:
            return "LOW - 主要为低风险操作"
    
    def _generate_rollback_plan(self, steps: List[MigrationStep]) -> str:
        """生成回滚计划"""
        rollback_steps = []
        rollback_steps.append("# 数据库迁移回滚计划")
        rollback_steps.append(f"# 生成时间: {datetime.now().isoformat()}")
        rollback_steps.append("")
        rollback_steps.append("## 回滚步骤（按逆序执行）")
        rollback_steps.append("")
        
        # 逆序回滚
        for step in reversed(steps):
            rollback_steps.append(f"### {step.step_id}: 回滚 {step.description}")
            rollback_steps.append(f"```sql")
            rollback_steps.append(step.rollback_command)
            rollback_steps.append(f"```")
            rollback_steps.append("")
        
        return "\n".join(rollback_steps)
    
    def _generate_validation_checklist(self, steps: List[MigrationStep]) -> List[str]:
        """生成验证清单"""
        checklist = []
        
        checklist.append("迁移前检查:")
        checklist.append("- [ ] 确认所有相关系统已停止写入")
        checklist.append("- [ ] 完成数据库完整备份")
        checklist.append("- [ ] 验证测试环境迁移成功")
        checklist.append("- [ ] 准备回滚计划和脚本")
        
        checklist.append("\n迁移过程检查:")
        for step in steps:
            checklist.append(f"- [ ] {step.step_id}: {step.description}")
            checklist.append(f"  - 执行验证查询: {step.validation_query[:50]}...")
        
        checklist.append("\n迁移后检查:")
        checklist.append("- [ ] 验证所有表结构正确")
        checklist.append("- [ ] 验证数据完整性")
        checklist.append("- [ ] 执行性能测试")
        checklist.append("- [ ] 验证应用程序功能")
        checklist.append("- [ ] 监控系统性能指标")
        
        return checklist
    
    def generate_migration_scripts(self, migration_plan: MigrationPlan) -> Dict[str, str]:
        """生成可执行的迁移脚本"""
        scripts = {}
        
        # 主迁移脚本
        main_script = self._generate_main_migration_script(migration_plan)
        scripts['main_migration.sh'] = main_script
        
        # 回滚脚本
        rollback_script = self._generate_rollback_script(migration_plan)
        scripts['rollback_migration.sh'] = rollback_script
        
        # 验证脚本
        validation_script = self._generate_validation_script(migration_plan)
        scripts['validate_migration.sh'] = validation_script
        
        # 预检查脚本
        precheck_script = self._generate_precheck_script(migration_plan)
        scripts['precheck_migration.sh'] = precheck_script
        
        return scripts
    
    def _generate_main_migration_script(self, plan: MigrationPlan) -> str:
        """生成主迁移脚本"""
        script_lines = []
        
        script_lines.append("#!/bin/bash")
        script_lines.append(f"# {plan.plan_name}")
        script_lines.append(f"# 生成时间: {datetime.now().isoformat()}")
        script_lines.append(f"# 预计执行时间: {plan.estimated_duration}")
        script_lines.append(f"# 风险评估: {plan.risk_assessment}")
        script_lines.append("")
        script_lines.append("set -e  # 遇到错误立即退出")
        script_lines.append("set -u  # 使用未定义变量时退出")
        script_lines.append("")
        script_lines.append("# 配置变量")
        script_lines.append("PROJECT_ID=${PROJECT_ID:-your_project_id}")
        script_lines.append("DATASET_ID=${DATASET_ID:-your_dataset_id}")
        script_lines.append("LOG_FILE=\"migration_$(date +%Y%m%d_%H%M%S).log\"")
        script_lines.append("")
        script_lines.append("# 日志函数")
        script_lines.append("log() {")
        script_lines.append("    echo \"[$(date '+%Y-%m-%d %H:%M:%S')] $1\" | tee -a \"$LOG_FILE\"")
        script_lines.append("}")
        script_lines.append("")
        script_lines.append("log \"开始数据库迁移: {plan.plan_name}\"")
        script_lines.append("")
        
        # 生成每个步骤
        for i, step in enumerate(plan.steps, 1):
            script_lines.append(f"# ========== 步骤 {i}/{plan.total_steps}: {step.description} ==========")
            script_lines.append(f"log \"执行步骤 {step.step_id}: {step.description}\"")
            script_lines.append("")
            
            # 添加SQL执行
            script_lines.append("bq query --use_legacy_sql=false \\")
            script_lines.append(f"  --project_id=\"$PROJECT_ID\" \\")
            script_lines.append(f"  \"{step.sql_command.replace(chr(10), ' ')}\"")
            script_lines.append("")
            
            # 添加验证
            if step.validation_query.strip():
                script_lines.append("# 验证步骤执行结果")
                script_lines.append("bq query --use_legacy_sql=false \\")
                script_lines.append(f"  --project_id=\"$PROJECT_ID\" \\")
                script_lines.append(f"  \"{step.validation_query.replace(chr(10), ' ')}\"")
                script_lines.append("")
            
            script_lines.append(f"log \"步骤 {step.step_id} 完成\"")
            script_lines.append("")
        
        script_lines.append("log \"数据库迁移完成！\"")
        script_lines.append("log \"请执行 validate_migration.sh 验证迁移结果\"")
        
        return "\n".join(script_lines)
    
    def _generate_rollback_script(self, plan: MigrationPlan) -> str:
        """生成回滚脚本"""
        script_lines = []
        
        script_lines.append("#!/bin/bash")
        script_lines.append(f"# {plan.plan_name} - 回滚脚本")
        script_lines.append(f"# 生成时间: {datetime.now().isoformat()}")
        script_lines.append("")
        script_lines.append("set -e")
        script_lines.append("")
        script_lines.append("echo \"警告: 即将执行数据库迁移回滚\"")
        script_lines.append("echo \"这将撤销所有迁移更改\"")
        script_lines.append("read -p \"确认继续? (yes/no): \" confirm")
        script_lines.append("if [ \"$confirm\" != \"yes\" ]; then")
        script_lines.append("    echo \"回滚已取消\"")
        script_lines.append("    exit 1")
        script_lines.append("fi")
        script_lines.append("")
        
        # 逆序执行回滚
        for i, step in enumerate(reversed(plan.steps), 1):
            if step.rollback_command.strip() and not step.rollback_command.startswith('--'):
                script_lines.append(f"echo \"回滚步骤 {i}: {step.description}\"")
                script_lines.append("bq query --use_legacy_sql=false \\")
                script_lines.append(f"  \"{step.rollback_command.replace(chr(10), ' ')}\"")
                script_lines.append("")
        
        script_lines.append("echo \"回滚完成\"")
        
        return "\n".join(script_lines)
    
    def _generate_validation_script(self, plan: MigrationPlan) -> str:
        """生成验证脚本"""
        script_lines = []
        
        script_lines.append("#!/bin/bash")
        script_lines.append(f"# {plan.plan_name} - 验证脚本")
        script_lines.append("")
        script_lines.append("echo \"开始验证迁移结果...\"")
        script_lines.append("")
        
        for step in plan.steps:
            if step.validation_query.strip():
                script_lines.append(f"echo \"验证: {step.description}\"")
                script_lines.append("bq query --use_legacy_sql=false \\")
                script_lines.append(f"  \"{step.validation_query.replace(chr(10), ' ')}\"")
                script_lines.append("")
        
        script_lines.append("echo \"验证完成\"")
        
        return "\n".join(script_lines)
    
    def _generate_precheck_script(self, plan: MigrationPlan) -> str:
        """生成预检查脚本"""
        script_lines = []
        
        script_lines.append("#!/bin/bash")
        script_lines.append(f"# {plan.plan_name} - 预检查脚本")
        script_lines.append("")
        script_lines.append("echo \"执行迁移前预检查...\"")
        script_lines.append("")
        script_lines.append("# 检查必要的环境变量")
        script_lines.append("if [ -z \"${PROJECT_ID:-}\" ]; then")
        script_lines.append("    echo \"错误: PROJECT_ID 环境变量未设置\"")
        script_lines.append("    exit 1")
        script_lines.append("fi")
        script_lines.append("")
        script_lines.append("# 检查BigQuery连接")
        script_lines.append("echo \"检查BigQuery连接...\"")
        script_lines.append("bq ls --project_id=\"$PROJECT_ID\" > /dev/null")
        script_lines.append("echo \"BigQuery连接正常\"")
        script_lines.append("")
        script_lines.append("# 检查表存在性")
        script_lines.append("echo \"检查目标表存在性...\"")
        
        # 从迁移步骤中提取表名
        table_names = set()
        for step in plan.steps:
            # 简单的表名提取
            if 'FROM `' in step.sql_command:
                matches = re.findall(r'FROM `([^`]+)`', step.sql_command)
                table_names.update(matches)
        
        for table_name in table_names:
            script_lines.append(f"bq show \"$PROJECT_ID:{table_name}\" > /dev/null || echo \"警告: 表 {table_name} 不存在\"")
        
        script_lines.append("")
        script_lines.append("echo \"预检查完成\"")
        
        return "\n".join(script_lines)
    
    # 辅助方法
    def _extract_field_name_from_description(self, description: str) -> str:
        """从描述中提取字段名"""
        # 简单的字段名提取逻辑
        if '字段' in description:
            words = description.split()
            for word in words:
                if word not in ['移除', '删除', '字段', '冗余', '归档']:
                    return word.strip()
        return 'unknown_field'
    
    def _get_primary_key_fields(self, table_name: str) -> str:
        """获取表的主键字段"""
        # 根据表名返回主键字段
        primary_keys = {
            'score_ledger': 'order_id',
            'draws_14w_dedup_v': 'draw_id',
            'p_size_clean_merged_dedup_v': 'id'
        }
        return primary_keys.get(table_name, 'id')
    
    def _extract_index_fields_from_optimization(self, optimization: TableOptimization) -> List[str]:
        """从优化建议中提取索引字段"""
        # 从SQL语句中提取字段名
        sql = optimization.sql_after
        fields = []
        
        # 查找CREATE INDEX语句中的字段
        matches = re.findall(r'CREATE INDEX.*?\((.*?)\)', sql, re.IGNORECASE)
        for match in matches:
            fields.extend([field.strip() for field in match.split(',')])
        
        return fields if fields else ['timestamp', 'draw_id']  # 默认字段


def main():
    """主函数"""
    generator = MigrationScriptGenerator()
    
    try:
        # 获取优化建议
        optimizations = generator.optimizer.analyze_table_optimization_opportunities()
        
        # 生成迁移计划
        migration_plan = generator.generate_migration_plan(optimizations)
        
        # 生成迁移脚本
        migration_scripts = generator.generate_migration_scripts(migration_plan)
        
        # 保存迁移计划
        plan_file = f"migration_plan_{migration_plan.plan_id}.json"
        plan_data = {
            'plan_id': migration_plan.plan_id,
            'plan_name': migration_plan.plan_name,
            'description': migration_plan.description,
            'total_steps': migration_plan.total_steps,
            'estimated_duration': migration_plan.estimated_duration,
            'risk_assessment': migration_plan.risk_assessment,
            'steps': [
                {
                    'step_id': step.step_id,
                    'step_type': step.step_type,
                    'description': step.description,
                    'sql_command': step.sql_command,
                    'rollback_command': step.rollback_command,
                    'validation_query': step.validation_query,
                    'estimated_duration': step.estimated_duration,
                    'risk_level': step.risk_level,
                    'dependencies': step.dependencies
                }
                for step in migration_plan.steps
            ],
            'rollback_plan': migration_plan.rollback_plan,
            'validation_checklist': migration_plan.validation_checklist
        }
        
        with open(plan_file, 'w', encoding='utf-8') as f:
            json.dump(plan_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ 迁移计划已生成: {plan_file}")
        
        # 保存迁移脚本
        for script_name, script_content in migration_scripts.items():
            with open(script_name, 'w', encoding='utf-8') as f:
                f.write(script_content)
            
            # 设置执行权限
            os.chmod(script_name, 0o755)
            print(f"✅ 迁移脚本已生成: {script_name}")
        
        # 打印迁移计划摘要
        print(f"\n=== 迁移计划摘要 ===")
        print(f"计划名称: {migration_plan.plan_name}")
        print(f"总步骤数: {migration_plan.total_steps}")
        print(f"预计时间: {migration_plan.estimated_duration}")
        print(f"风险评估: {migration_plan.risk_assessment}")
        
        print(f"\n=== 执行顺序 ===")
        print("1. 执行预检查: ./precheck_migration.sh")
        print("2. 执行迁移: ./main_migration.sh")
        print("3. 验证结果: ./validate_migration.sh")
        print("4. 如需回滚: ./rollback_migration.sh")
        
        return migration_plan
        
    except Exception as e:
        print(f"❌ 迁移脚本生成过程中发生错误: {e}")
        return None


if __name__ == "__main__":
    main()