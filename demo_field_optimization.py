#!/usr/bin/env python3
"""
PC28字段利用率分析和数据表优化演示脚本
演示完整的字段分析、优化建议生成和迁移脚本创建流程
"""

import json
import os
from datetime import datetime
from field_usage_analysis import FieldUsageAnalyzer
from database_table_optimizer import DatabaseTableOptimizer
from migration_script_generator import MigrationScriptGenerator

def demo_field_optimization():
    """演示完整的字段优化流程"""
    print("=" * 80)
    print("PC28字段利用率分析和数据表优化演示")
    print("=" * 80)
    
    # 1. 初始化分析器
    print("\n1. 初始化字段使用分析器...")
    analyzer = FieldUsageAnalyzer()
    
    # 2. 模拟实时API数据
    print("\n2. 模拟PC28实时API数据...")
    sample_data = {
        "code": "0",
        "msg": "success",
        "data": {
            "preDrawIssue": "20240925001",
            "preDrawTime": "2024-09-25 10:00:00",
            "preDrawCode": "1,2,3",
            "issue": "20240925002",
            "openTime": "2024-09-25 10:05:00",
            "openCode": "4,5,6",
            "drawTime": "2024-09-25 10:05:30",
            "intervalM": 5,
            "result_digits": [4, 5, 6],  # 冗余字段
            "numbers": [4, 5, 6],        # 与result_digits重复
            "curtime": "2024-09-25 10:05:30",  # 未使用字段
            "next": "20240925003",       # 未使用字段
            "nextTime": "2024-09-25 10:10:00"  # 未使用字段
        }
    }
    
    # 3. 执行字段使用分析
    print("\n3. 执行字段使用分析...")
    analysis_result = analyzer.analyze_realtime_api_usage()
    
    print(f"   - 检测到 {len(analysis_result.get('unused_fields', []))} 个未使用字段")
    print(f"   - 字段使用率: {analysis_result.get('usage_rate', 0):.2f}%")
    
    # 模拟冗余字段检测结果（因为实际API分析可能不包含这些字段）
    redundant_fields = ['result_digits', 'numbers']  # 模拟检测到的冗余字段
    analysis_result['redundant_fields'] = redundant_fields
    print(f"   - 模拟检测到 {len(redundant_fields)} 个冗余字段")
    
    # 4. 生成优化建议
    print("\n4. 生成数据表优化建议...")
    optimizer = DatabaseTableOptimizer()
    
    # 模拟表结构
    table_schema = {
        "pc28_results": {
            "columns": [
                {"name": "id", "type": "BIGINT", "primary_key": True},
                {"name": "issue", "type": "VARCHAR(20)", "nullable": False},
                {"name": "openCode", "type": "VARCHAR(50)", "nullable": False},
                {"name": "result_digits", "type": "JSON", "nullable": True},
                {"name": "numbers", "type": "JSON", "nullable": True},
                {"name": "curtime", "type": "TIMESTAMP", "nullable": True},
                {"name": "next", "type": "VARCHAR(20)", "nullable": True},
                {"name": "nextTime", "type": "TIMESTAMP", "nullable": True},
                {"name": "drawTime", "type": "TIMESTAMP", "nullable": False},
                {"name": "created_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"}
            ],
            "indexes": [
                {"name": "idx_issue", "columns": ["issue"]},
                {"name": "idx_draw_time", "columns": ["drawTime"]}
            ]
        }
    }
    
    # 使用现有的表优化分析方法
    optimizations = optimizer.analyze_table_optimization_opportunities()
    
    # 为演示创建一个模拟的优化结果
    from database_table_optimizer import TableOptimization, FieldOptimization
    
    field_opts = [
        FieldOptimization(
            field_name="result_digits",
            optimization_type="remove_redundant",
            reason="与numbers字段功能重复",
            action="删除字段",
            impact_assessment="减少存储空间，简化查询"
        ),
        FieldOptimization(
            field_name="curtime",
            optimization_type="remove_unused",
            reason="字段使用频率极低",
            action="考虑删除或归档",
            impact_assessment="显著减少存储成本"
        )
    ]
    
    optimization = TableOptimization(
        table_name="pc28_results",
        optimization_type="field_optimization",
        description="PC28结果表字段优化",
        impact="HIGH",
        sql_before="-- 原始表结构包含冗余字段",
        sql_after="-- 优化后的表结构",
        migration_script="-- 迁移脚本",
        estimated_savings={
            'storage_mb': 125.5,
            'query_performance': 0.15,
            'index_size_mb': 23.2
        }
    )
    optimization.field_optimizations = field_opts
    
    print(f"   - 生成 {len(optimization.field_optimizations)} 个字段优化建议")
    print(f"   - 预计节省存储空间: {optimization.estimated_savings['storage_mb']:.2f} MB")
    print(f"   - 优化影响等级: {optimization.impact}")
    
    # 5. 生成SQL DDL
    print("\n5. 生成优化后的SQL DDL...")
    sample_optimizations = {"pc28_results": [optimization]}
    ddl_sql = optimizer.generate_optimization_sql_ddl(sample_optimizations)
    print("   - 已生成优化后的表结构DDL")
    
    # 6. 生成迁移脚本
    print("\n6. 生成数据迁移脚本...")
    migration_generator = MigrationScriptGenerator()
    
    migration_plan = migration_generator.generate_migration_plan(sample_optimizations)
    
    print(f"   - 迁移计划: {migration_plan.plan_name}")
    print(f"   - 迁移步骤: {len(migration_plan.steps)} 步")
    print(f"   - 预计耗时: {migration_plan.estimated_duration}")
    print(f"   - 风险评估: {migration_plan.risk_assessment}")
    
    # 7. 生成迁移脚本文件
    print("\n7. 生成迁移脚本文件...")
    scripts = migration_generator.generate_migration_scripts(migration_plan)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 保存迁移计划
    plan_file = f"demo_migration_plan_{timestamp}.json"
    with open(plan_file, 'w', encoding='utf-8') as f:
        json.dump(migration_plan.__dict__, f, ensure_ascii=False, indent=2, default=str)
    
    # 保存迁移脚本
    script_files = {}
    for script_type, script_content in scripts.items():
        script_file = f"demo_{script_type}_migration_{timestamp}.sh"
        with open(script_file, 'w', encoding='utf-8') as f:
            f.write(script_content)
        os.chmod(script_file, 0o755)  # 设置执行权限
        script_files[script_type] = script_file
    
    print(f"   - 迁移计划文件: {plan_file}")
    for script_type, script_file in script_files.items():
        print(f"   - {script_type}脚本: {script_file}")
    
    # 8. 生成优化报告
    print("\n8. 生成优化报告...")
    report = optimizer.generate_optimization_report()
    
    report_file = f"demo_optimization_report_{timestamp}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"   - 优化报告文件: {report_file}")
    
    # 9. 显示详细结果
    print("\n" + "=" * 80)
    print("详细分析结果")
    print("=" * 80)
    
    print("\n冗余字段检测:")
    for field in analysis_result['redundant_fields']:
        print(f"   - {field}: 与其他字段重复")
    
    print("\n未使用字段检测:")
    for field in analysis_result['unused_fields']:
        print(f"   - {field}: 在业务逻辑中未被使用")
    
    print("\n优化建议:")
    for field_opt in optimization.field_optimizations:
        print(f"   - {field_opt.field_name}: {field_opt.optimization_type} - {field_opt.reason}")
    
    print(f"\n存储优化预估:")
    print(f"   - 存储空间节省: {optimization.estimated_savings['storage_mb']:.2f} MB")
    print(f"   - 查询性能提升: {optimization.estimated_savings['query_performance']:.1%}")
    print(f"   - 索引空间节省: {optimization.estimated_savings['index_size_mb']:.2f} MB")
    
    print("\n迁移风险评估:")
    print(f"   - 风险等级: {migration_plan.risk_assessment}")
    print(f"   - 预计停机时间: {migration_plan.estimated_duration}")
    print(f"   - 回滚策略: 已生成回滚脚本")
    
    print("\n" + "=" * 80)
    print("演示完成！所有文件已生成到当前目录")
    print("=" * 80)
    
    return {
        'analysis_result': analysis_result,
        'optimization': optimization,
        'migration_plan': migration_plan,
        'generated_files': {
            'plan': plan_file,
            'scripts': script_files,
            'report': report_file
        }
    }

if __name__ == "__main__":
    demo_result = demo_field_optimization()