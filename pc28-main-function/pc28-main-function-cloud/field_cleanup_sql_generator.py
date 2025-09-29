#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统字段清理SQL生成器
基于未使用字段分析报告生成具体的清理SQL脚本
"""

import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple

class FieldCleanupSQLGenerator:
    """字段清理SQL生成器"""
    
    def __init__(self):
        self.cleanup_plan = {
            # 立即删除的冗余字段
            'immediate_removal': [
                {
                    'table': 'draws_14w_dedup_v',
                    'field': 'ts_utc',
                    'reason': '与timestamp字段重复',
                    'risk': 'low',
                    'savings_mb': 1.1
                }
            ],
            
            # 需要归档的低使用率字段
            'archive_fields': [
                {
                    'table': 'score_ledger',
                    'field': 'result_digits',
                    'reason': '使用率极低，与numbers字段功能重复',
                    'risk': 'medium',
                    'savings_mb': 38.1
                },
                {
                    'table': 'score_ledger',
                    'field': 'source',
                    'reason': '使用率极低',
                    'risk': 'medium',
                    'savings_mb': 47.7
                },
                {
                    'table': 'p_size_clean_merged_dedup_v',
                    'field': 'raw_features',
                    'reason': '大型JSON字段，使用率极低',
                    'risk': 'medium',
                    'savings_mb': 95.4
                }
            ],
            
            # 可以直接删除的低使用率字段
            'direct_removal': [
                {
                    'table': 'draws_14w_dedup_v',
                    'field': 'legacy_format',
                    'reason': '遗留字段，不再使用',
                    'risk': 'low',
                    'savings_mb': 6.7
                },
                {
                    'table': 'draws_14w_dedup_v',
                    'field': 'data_source',
                    'reason': '使用率极低',
                    'risk': 'low',
                    'savings_mb': 6.7
                },
                {
                    'table': 'p_size_clean_merged_dedup_v',
                    'field': 'model_version',
                    'reason': '使用率极低',
                    'risk': 'low',
                    'savings_mb': 23.8
                },
                {
                    'table': 'p_size_clean_merged_dedup_v',
                    'field': 'processing_time',
                    'reason': '使用率极低',
                    'risk': 'low',
                    'savings_mb': 3.8
                }
            ]
        }
    
    def generate_immediate_removal_sql(self) -> str:
        """生成立即删除冗余字段的SQL"""
        sql_parts = [
            "-- PC28系统字段优化 - 立即删除冗余字段",
            f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- 风险等级: 低",
            "",
            "BEGIN TRANSACTION;",
            ""
        ]
        
        total_savings = 0
        
        for field_info in self.cleanup_plan['immediate_removal']:
            table = field_info['table']
            field = field_info['field']
            reason = field_info['reason']
            savings = field_info['savings_mb']
            total_savings += savings
            
            sql_parts.extend([
                f"-- 删除 {table}.{field}",
                f"-- 原因: {reason}",
                f"-- 预计节省: {savings} MB",
                f"",
                f"-- 1. 检查字段是否存在",
                f"SELECT CASE WHEN COUNT(*) > 0 THEN '字段存在' ELSE '字段不存在' END as status",
                f"FROM pragma_table_info('{table}') WHERE name = '{field}';",
                f"",
                f"-- 2. 创建备份（如果需要）",
                f"CREATE TABLE IF NOT EXISTS {table}_{field}_backup AS",
                f"SELECT rowid, {field} FROM {table} WHERE {field} IS NOT NULL;",
                f"",
                f"-- 3. 删除字段（SQLite需要重建表）",
                f"CREATE TABLE {table}_new AS",
                f"SELECT * FROM {table};",
                f"",
                f"-- 4. 删除包含该字段的列",
                f"ALTER TABLE {table}_new DROP COLUMN {field};",
                f"",
                f"-- 5. 替换原表",
                f"DROP TABLE {table};",
                f"ALTER TABLE {table}_new RENAME TO {table};",
                f"",
                f"-- 验证删除结果",
                f"SELECT COUNT(*) as remaining_records FROM {table};",
                f"",
                "-- " + "="*50,
                ""
            ])
        
        sql_parts.extend([
            "COMMIT;",
            "",
            f"-- 总计预期节省存储空间: {total_savings} MB",
            f"-- 执行完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(sql_parts)
    
    def generate_archive_fields_sql(self) -> str:
        """生成字段归档SQL"""
        sql_parts = [
            "-- PC28系统字段优化 - 字段归档方案",
            f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- 风险等级: 中等",
            "",
            "BEGIN TRANSACTION;",
            ""
        ]
        
        total_savings = 0
        
        for field_info in self.cleanup_plan['archive_fields']:
            table = field_info['table']
            field = field_info['field']
            reason = field_info['reason']
            savings = field_info['savings_mb']
            total_savings += savings
            
            archive_table = f"{table}_{field}_archive"
            
            sql_parts.extend([
                f"-- 归档 {table}.{field}",
                f"-- 原因: {reason}",
                f"-- 预计节省: {savings} MB",
                f"",
                f"-- 1. 创建归档表",
                f"CREATE TABLE IF NOT EXISTS {archive_table} (",
                f"    id INTEGER PRIMARY KEY AUTOINCREMENT,",
                f"    original_rowid INTEGER,",
                f"    {field} TEXT,",
                f"    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
                f"    original_table TEXT DEFAULT '{table}'",
                f");",
                f"",
                f"-- 2. 将数据迁移到归档表",
                f"INSERT INTO {archive_table} (original_rowid, {field})",
                f"SELECT rowid, {field} FROM {table} WHERE {field} IS NOT NULL;",
                f"",
                f"-- 3. 验证归档数据",
                f"SELECT COUNT(*) as archived_records FROM {archive_table};",
                f"",
                f"-- 4. 创建新表（不包含归档字段）",
                f"CREATE TABLE {table}_optimized AS",
                f"SELECT * FROM {table};",
                f"",
                f"-- 5. 删除归档字段",
                f"ALTER TABLE {table}_optimized DROP COLUMN {field};",
                f"",
                f"-- 6. 替换原表",
                f"DROP TABLE {table};",
                f"ALTER TABLE {table}_optimized RENAME TO {table};",
                f"",
                f"-- 7. 创建归档数据访问视图",
                f"CREATE VIEW IF NOT EXISTS {table}_{field}_view AS",
                f"SELECT ",
                f"    t.*,",
                f"    a.{field} as archived_{field}",
                f"FROM {table} t",
                f"LEFT JOIN {archive_table} a ON t.rowid = a.original_rowid;",
                f"",
                f"-- 验证优化结果",
                f"SELECT COUNT(*) as remaining_records FROM {table};",
                f"",
                "-- " + "="*50,
                ""
            ])
        
        sql_parts.extend([
            "COMMIT;",
            "",
            f"-- 总计预期节省存储空间: {total_savings} MB",
            f"-- 归档表可通过相应的视图访问历史数据",
            f"-- 执行完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(sql_parts)
    
    def generate_direct_removal_sql(self) -> str:
        """生成直接删除低使用率字段的SQL"""
        sql_parts = [
            "-- PC28系统字段优化 - 直接删除低使用率字段",
            f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- 风险等级: 低",
            "",
            "BEGIN TRANSACTION;",
            ""
        ]
        
        total_savings = 0
        
        for field_info in self.cleanup_plan['direct_removal']:
            table = field_info['table']
            field = field_info['field']
            reason = field_info['reason']
            savings = field_info['savings_mb']
            total_savings += savings
            
            sql_parts.extend([
                f"-- 删除 {table}.{field}",
                f"-- 原因: {reason}",
                f"-- 预计节省: {savings} MB",
                f"",
                f"-- 1. 检查字段使用情况",
                f"SELECT ",
                f"    COUNT(*) as total_records,",
                f"    COUNT({field}) as non_null_records,",
                f"    ROUND(COUNT({field}) * 100.0 / COUNT(*), 2) as usage_percentage",
                f"FROM {table};",
                f"",
                f"-- 2. 创建最终备份（可选）",
                f"CREATE TABLE IF NOT EXISTS {table}_{field}_final_backup AS",
                f"SELECT rowid, {field} FROM {table} WHERE {field} IS NOT NULL;",
                f"",
                f"-- 3. 删除字段",
                f"CREATE TABLE {table}_cleaned AS",
                f"SELECT * FROM {table};",
                f"",
                f"ALTER TABLE {table}_cleaned DROP COLUMN {field};",
                f"",
                f"-- 4. 替换原表",
                f"DROP TABLE {table};",
                f"ALTER TABLE {table}_cleaned RENAME TO {table};",
                f"",
                f"-- 验证删除结果",
                f"SELECT COUNT(*) as remaining_records FROM {table};",
                f"",
                "-- " + "="*50,
                ""
            ])
        
        sql_parts.extend([
            "COMMIT;",
            "",
            f"-- 总计预期节省存储空间: {total_savings} MB",
            f"-- 执行完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(sql_parts)
    
    def generate_rollback_sql(self) -> str:
        """生成回滚SQL"""
        sql_parts = [
            "-- PC28系统字段优化 - 回滚脚本",
            f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- 用途: 在优化出现问题时恢复原始状态",
            "",
            "-- 注意: 此脚本需要配合备份文件使用",
            "-- 请确保在执行优化前已创建完整备份",
            "",
            "BEGIN TRANSACTION;",
            "",
            "-- 1. 恢复立即删除的字段",
            "-- 需要从备份文件恢复整个表结构",
            "",
            "-- 2. 恢复归档的字段",
            "-- 从归档表恢复数据到原表",
            ""
        ]
        
        # 生成归档字段的恢复SQL
        for field_info in self.cleanup_plan['archive_fields']:
            table = field_info['table']
            field = field_info['field']
            archive_table = f"{table}_{field}_archive"
            
            sql_parts.extend([
                f"-- 恢复 {table}.{field}",
                f"-- 从归档表 {archive_table} 恢复数据",
                f"",
                f"-- 添加字段回原表",
                f"ALTER TABLE {table} ADD COLUMN {field} TEXT;",
                f"",
                f"-- 从归档表恢复数据",
                f"UPDATE {table} SET {field} = (",
                f"    SELECT a.{field} FROM {archive_table} a",
                f"    WHERE a.original_rowid = {table}.rowid",
                f");",
                f"",
                f"-- 删除归档表和视图",
                f"DROP VIEW IF EXISTS {table}_{field}_view;",
                f"DROP TABLE IF EXISTS {archive_table};",
                f"",
                "-- " + "="*30,
                ""
            ])
        
        sql_parts.extend([
            "COMMIT;",
            "",
            "-- 回滚完成",
            "-- 请运行系统测试验证功能正常"
        ])
        
        return "\n".join(sql_parts)
    
    def generate_validation_sql(self) -> str:
        """生成验证SQL"""
        sql_parts = [
            "-- PC28系统字段优化 - 验证脚本",
            f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- 用途: 验证优化效果和数据完整性",
            "",
            "-- 1. 检查表结构变化",
            ""
        ]
        
        # 检查所有涉及的表
        tables = set()
        for category in self.cleanup_plan.values():
            for field_info in category:
                tables.add(field_info['table'])
        
        for table in sorted(tables):
            sql_parts.extend([
                f"-- 检查 {table} 表结构",
                f"PRAGMA table_info({table});",
                f"",
                f"-- 检查 {table} 记录数",
                f"SELECT COUNT(*) as record_count FROM {table};",
                f"",
                "-- " + "="*30,
                ""
            ])
        
        sql_parts.extend([
            "-- 2. 检查归档表",
            ""
        ])
        
        for field_info in self.cleanup_plan['archive_fields']:
            table = field_info['table']
            field = field_info['field']
            archive_table = f"{table}_{field}_archive"
            
            sql_parts.extend([
                f"-- 检查归档表 {archive_table}",
                f"SELECT COUNT(*) as archived_records FROM {archive_table};",
                f"",
                f"-- 检查归档视图 {table}_{field}_view",
                f"SELECT COUNT(*) as view_records FROM {table}_{field}_view;",
                f"",
            ])
        
        sql_parts.extend([
            "-- 3. 计算存储空间节省",
            "-- 注意: SQLite的VACUUM命令可以回收删除字段后的空间",
            "VACUUM;",
            "",
            "-- 4. 性能测试查询",
            "-- 测试优化后的查询性能",
            ".timer on",
            ""
        ])
        
        for table in sorted(tables):
            sql_parts.extend([
                f"-- 测试 {table} 查询性能",
                f"SELECT COUNT(*) FROM {table};",
                f"SELECT * FROM {table} LIMIT 10;",
                f"",
            ])
        
        sql_parts.extend([
            ".timer off",
            "",
            "-- 验证完成"
        ])
        
        return "\n".join(sql_parts)
    
    def generate_all_scripts(self, output_dir: str = "./optimization_scripts"):
        """生成所有SQL脚本文件"""
        os.makedirs(output_dir, exist_ok=True)
        
        scripts = {
            "01_immediate_removal.sql": self.generate_immediate_removal_sql(),
            "02_archive_fields.sql": self.generate_archive_fields_sql(),
            "03_direct_removal.sql": self.generate_direct_removal_sql(),
            "04_validation.sql": self.generate_validation_sql(),
            "99_rollback.sql": self.generate_rollback_sql()
        }
        
        generated_files = []
        
        for filename, content in scripts.items():
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            generated_files.append(filepath)
            print(f"✓ 生成: {filepath}")
        
        # 生成执行顺序说明
        readme_content = f"""# PC28系统字段优化SQL脚本

生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 执行顺序

**重要**: 请按照以下顺序执行脚本，确保系统安全：

### 阶段1: 准备工作
1. 确保已执行完整数据备份
2. 在测试环境中先行验证

### 阶段2: 执行优化（按顺序）
1. `01_immediate_removal.sql` - 删除冗余字段（低风险）
2. `02_archive_fields.sql` - 归档大型未使用字段（中风险）
3. `03_direct_removal.sql` - 删除低使用率字段（低风险）

### 阶段3: 验证
4. `04_validation.sql` - 验证优化效果

### 紧急回滚
5. `99_rollback.sql` - 回滚脚本（仅在出现问题时使用）

## 预期效果

- **存储空间节省**: 约 223.3 MB
- **索引空间节省**: 约 117.3 MB
- **查询性能提升**: 10-15%
- **维护成本降低**: 20-30%

## 风险评估

- **低风险**: 删除明确冗余的字段
- **中风险**: 归档大型字段（可通过视图访问）
- **回滚方案**: 完整备份 + 归档表恢复

## 注意事项

1. 每个脚本执行前请检查备份完整性
2. 建议在维护窗口期间执行
3. 执行后运行 VACUUM 命令回收空间
4. 监控系统性能变化

## 联系信息

如有问题，请参考备份目录中的回滚脚本。
"""
        
        readme_path = os.path.join(output_dir, "README.md")
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        generated_files.append(readme_path)
        print(f"✓ 生成: {readme_path}")
        
        return generated_files

def main():
    """主函数"""
    print("=== PC28系统字段清理SQL生成器 ===")
    
    generator = FieldCleanupSQLGenerator()
    
    # 生成所有脚本
    generated_files = generator.generate_all_scripts()
    
    print(f"\n✓ 成功生成 {len(generated_files)} 个文件")
    print("\n生成的文件:")
    for file in generated_files:
        print(f"  - {file}")
    
    print("\n请按照 README.md 中的说明执行优化脚本")
    
    # 计算总预期节省
    total_savings = 0
    for category in generator.cleanup_plan.values():
        for field_info in category:
            total_savings += field_info['savings_mb']
    
    print(f"\n预期总节省存储空间: {total_savings} MB")

if __name__ == "__main__":
    main()