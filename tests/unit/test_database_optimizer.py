#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据表优化工具测试用例
测试数据表优化逻辑、SQL DDL生成、迁移脚本生成等功能
"""

import pytest
import json
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Any

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database_table_optimizer import DatabaseTableOptimizer, TableOptimization, FieldOptimization


class TestDatabaseTableOptimizer:
    """数据表优化器测试类"""
    
    @pytest.fixture
    def optimizer(self):
        """创建优化器实例"""
        with patch('database_table_optimizer.FieldUsageAnalyzer'):
            return DatabaseTableOptimizer()
    
    @pytest.fixture
    def sample_table_info(self):
        """示例表信息"""
        return {
            'fields': [
                {'name': 'id', 'type': 'STRING', 'nullable': False, 'usage': 'high'},
                {'name': 'result_digits', 'type': 'REPEATED INTEGER', 'nullable': True, 'usage': 'low'},
                {'name': 'numbers', 'type': 'REPEATED INTEGER', 'nullable': True, 'usage': 'high'},
                {'name': 'curtime', 'type': 'STRING', 'nullable': True, 'usage': 'low'},
                {'name': 'status', 'type': 'STRING', 'nullable': True, 'usage': 'high'}
            ],
            'indexes': ['id'],
            'partitioning': None,
            'estimated_size_gb': 5.0
        }
    
    def test_redundant_field_detection(self, optimizer):
        """测试冗余字段检测"""
        # 测试已知冗余字段
        assert optimizer._is_redundant_field('result_digits')
        assert optimizer._is_redundant_field('ts_utc')
        
        # 测试非冗余字段
        assert not optimizer._is_redundant_field('id')
        assert not optimizer._is_redundant_field('timestamp')
    
    def test_similar_field_detection(self, optimizer):
        """测试相似字段检测"""
        # 测试时间相关字段
        assert optimizer._has_similar_field('timestamp')
        assert optimizer._has_similar_field('ts_utc')
        assert optimizer._has_similar_field('created_at')
        
        # 测试数据源相关字段
        assert optimizer._has_similar_field('source')
        assert optimizer._has_similar_field('data_source')
        
        # 测试独特字段
        assert not optimizer._has_similar_field('unique_field')
    
    def test_type_optimization_detection(self, optimizer):
        """测试数据类型优化检测"""
        # 可优化的字段
        status_field = {'name': 'status', 'type': 'STRING'}
        assert optimizer._can_optimize_type(status_field)
        
        probability_field = {'name': 'probability', 'type': 'FLOAT64'}
        assert optimizer._can_optimize_type(probability_field)
        
        # 不需要优化的字段
        id_field = {'name': 'id', 'type': 'STRING'}
        assert not optimizer._can_optimize_type(id_field)
    
    def test_index_need_detection(self, optimizer):
        """测试索引需求检测"""
        # 需要索引的高查询字段
        assert optimizer._needs_index('draw_id')
        assert optimizer._needs_index('timestamp')
        assert optimizer._needs_index('order_id')
        
        # 不需要索引的字段
        assert not optimizer._needs_index('description')
        assert not optimizer._needs_index('metadata')
    
    def test_field_optimization_analysis(self, optimizer, sample_table_info):
        """测试字段优化分析"""
        optimizations = optimizer._analyze_field_optimizations('test_table', sample_table_info)
        
        # 应该检测到冗余字段
        redundant_opts = [opt for opt in optimizations if opt.optimization_type == 'remove_redundant']
        assert len(redundant_opts) > 0
        
        # 应该检测到未使用字段
        unused_opts = [opt for opt in optimizations if opt.optimization_type == 'remove_unused']
        assert len(unused_opts) > 0
        
        # 验证优化建议结构
        for opt in optimizations:
            assert hasattr(opt, 'field_name')
            assert hasattr(opt, 'optimization_type')
            assert hasattr(opt, 'reason')
            assert hasattr(opt, 'action')
            assert hasattr(opt, 'impact_assessment')
    
    def test_table_structure_analysis(self, optimizer, sample_table_info):
        """测试表结构分析"""
        # 测试大表（需要分区优化）
        large_table_info = sample_table_info.copy()
        large_table_info['estimated_size_gb'] = 15.0
        
        optimizations = optimizer._analyze_table_structure('large_table', large_table_info)
        
        # 应该有分区优化建议
        partition_opts = [opt for opt in optimizations if opt.optimization_type == 'partitioning']
        assert len(partition_opts) > 0
    
    def test_sql_ddl_generation(self, optimizer):
        """测试SQL DDL生成"""
        # 创建示例优化建议
        sample_optimizations = {
            'test_table': [
                TableOptimization(
                    table_name='test_table',
                    optimization_type='remove_field',
                    description='移除冗余字段',
                    impact='减少存储成本',
                    sql_before='-- 原始表结构',
                    sql_after='ALTER TABLE test_table DROP COLUMN redundant_field;',
                    migration_script='# 迁移脚本',
                    estimated_savings={'storage_reduction': '20%'}
                )
            ]
        }
        
        ddl = optimizer.generate_optimization_sql_ddl(sample_optimizations)
        
        # 验证DDL内容
        assert 'test_table' in ddl
        assert 'ALTER TABLE' in ddl
        assert 'DROP COLUMN' in ddl
        assert '减少存储成本' in ddl
    
    def test_migration_script_generation(self, optimizer):
        """测试迁移脚本生成"""
        sample_optimizations = {
            'test_table': [
                TableOptimization(
                    table_name='test_table',
                    optimization_type='remove_field',
                    description='移除冗余字段',
                    impact='减少存储成本',
                    sql_before='-- 原始表结构',
                    sql_after='ALTER TABLE test_table DROP COLUMN redundant_field;',
                    migration_script='bq query --use_legacy_sql=false "ALTER TABLE test_table DROP COLUMN redundant_field"',
                    estimated_savings={'storage_reduction': '20%'}
                )
            ]
        }
        
        scripts = optimizer.generate_migration_scripts(sample_optimizations)
        
        # 验证脚本生成
        assert 'test_table_migration.sh' in scripts
        
        script_content = scripts['test_table_migration.sh']
        assert '#!/bin/bash' in script_content
        assert 'set -e' in script_content
        assert 'bq query' in script_content
        assert '备份' in script_content
    
    def test_optimization_report_generation(self, optimizer):
        """测试优化报告生成"""
        with patch.object(optimizer, 'analyze_table_optimization_opportunities') as mock_analyze:
            # 模拟优化分析结果
            mock_analyze.return_value = {
                'test_table': [
                    TableOptimization(
                        table_name='test_table',
                        optimization_type='remove_field',
                        description='移除冗余字段',
                        impact='减少存储成本50%',
                        sql_before='-- 原始表结构',
                        sql_after='ALTER TABLE test_table DROP COLUMN redundant_field;',
                        migration_script='# 迁移脚本',
                        estimated_savings={'storage_reduction': '20%', 'query_performance': '10%'}
                    )
                ]
            }
            
            report = optimizer.generate_optimization_report()
            
            # 验证报告结构
            assert 'report_metadata' in report
            assert 'optimization_summary' in report
            assert 'table_optimizations' in report
            assert 'sql_ddl_statements' in report
            assert 'migration_scripts' in report
            assert 'implementation_plan' in report
            assert 'risk_assessment' in report
            
            # 验证元数据
            metadata = report['report_metadata']
            assert 'generated_at' in metadata
            assert 'analyzer_version' in metadata
            assert 'total_tables_analyzed' in metadata
            
            # 验证优化摘要
            summary = report['optimization_summary']
            assert 'total_optimizations' in summary
            assert 'high_impact_optimizations' in summary
            assert 'estimated_savings' in summary
    
    def test_savings_calculation(self, optimizer):
        """测试节省计算"""
        sample_optimizations = {
            'table1': [
                TableOptimization(
                    table_name='table1',
                    optimization_type='remove_field',
                    description='优化1',
                    impact='高影响',
                    sql_before='',
                    sql_after='',
                    migration_script='',
                    estimated_savings={'storage_reduction': '20%', 'query_performance': '30%'}
                )
            ],
            'table2': [
                TableOptimization(
                    table_name='table2',
                    optimization_type='optimize_type',
                    description='优化2',
                    impact='中等影响',
                    sql_before='',
                    sql_after='',
                    migration_script='',
                    estimated_savings={'storage_reduction': '15%', 'query_performance': '25%'}
                )
            ]
        }
        
        savings = optimizer._calculate_total_savings(sample_optimizations)
        
        # 验证节省计算结果
        assert 'estimated_storage_reduction' in savings
        assert 'estimated_performance_improvement' in savings
        assert 'estimated_monthly_cost_savings' in savings
        assert 'implementation_effort' in savings
        assert 'risk_level' in savings
        
        # 验证百分比格式
        assert '%' in savings['estimated_storage_reduction']
        assert '%' in savings['estimated_performance_improvement']
        assert '$' in savings['estimated_monthly_cost_savings']
    
    def test_high_impact_optimization_counting(self, optimizer):
        """测试高影响优化统计"""
        sample_optimizations = {
            'table1': [
                TableOptimization(
                    table_name='table1',
                    optimization_type='remove_field',
                    description='高影响优化',
                    impact='减少存储成本60%',  # 高影响
                    sql_before='',
                    sql_after='',
                    migration_script='',
                    estimated_savings={}
                ),
                TableOptimization(
                    table_name='table1',
                    optimization_type='optimize_type',
                    description='低影响优化',
                    impact='减少存储成本5%',   # 低影响
                    sql_before='',
                    sql_after='',
                    migration_script='',
                    estimated_savings={}
                )
            ]
        }
        
        count = optimizer._count_high_impact_optimizations(sample_optimizations)
        assert count == 1  # 只有一个高影响优化
    
    def test_implementation_plan_generation(self, optimizer):
        """测试实施计划生成"""
        sample_optimizations = {'test_table': []}
        
        plan = optimizer._generate_implementation_plan(sample_optimizations)
        
        # 验证计划结构
        assert len(plan) == 3  # 三个阶段
        
        for phase in plan:
            assert 'phase' in phase
            assert 'title' in phase
            assert 'description' in phase
            assert 'duration' in phase
            assert 'risk' in phase
            assert 'actions' in phase
            
            # 验证阶段编号
            assert 1 <= phase['phase'] <= 3
            
            # 验证风险级别
            assert phase['risk'] in ['Low', 'Medium', 'High']
    
    def test_risk_assessment_generation(self, optimizer):
        """测试风险评估生成"""
        sample_optimizations = {'test_table': []}
        
        risk_assessment = optimizer._generate_risk_assessment(sample_optimizations)
        
        # 验证风险评估结构
        assert 'high_risk_operations' in risk_assessment
        assert 'mitigation_strategies' in risk_assessment
        assert 'recommended_testing' in risk_assessment
        assert 'rollback_plan' in risk_assessment
        
        # 验证内容类型
        assert isinstance(risk_assessment['high_risk_operations'], list)
        assert isinstance(risk_assessment['mitigation_strategies'], list)
        assert isinstance(risk_assessment['recommended_testing'], list)
        assert isinstance(risk_assessment['rollback_plan'], dict)
        
        # 验证回滚计划内容
        rollback = risk_assessment['rollback_plan']
        assert 'backup_retention' in rollback
        assert 'rollback_time_estimate' in rollback
        assert 'rollback_complexity' in rollback


class TestTableOptimizationLogic:
    """表优化逻辑测试类"""
    
    def test_redundant_field_logic(self):
        """测试冗余字段逻辑"""
        optimizer = DatabaseTableOptimizer()
        
        # 测试已知冗余字段对
        redundant_pairs = [
            ('result_digits', 'numbers'),
            ('ts_utc', 'timestamp'),
            ('data_source', 'source')
        ]
        
        for redundant_field, _ in redundant_pairs:
            assert optimizer._is_redundant_field(redundant_field), f"{redundant_field} 应该被识别为冗余字段"
    
    def test_field_usage_classification(self):
        """测试字段使用分类"""
        optimizer = DatabaseTableOptimizer()
        
        # 高使用率字段
        high_usage_fields = ['draw_id', 'timestamp', 'order_id', 'status']
        for field in high_usage_fields:
            assert optimizer._needs_index(field), f"{field} 应该需要索引"
        
        # 低使用率字段（不需要索引）
        low_usage_fields = ['description', 'metadata', 'debug_info']
        for field in low_usage_fields:
            assert not optimizer._needs_index(field), f"{field} 不应该需要索引"
    
    def test_type_optimization_logic(self):
        """测试类型优化逻辑"""
        optimizer = DatabaseTableOptimizer()
        
        # 可优化的类型
        optimizable_fields = [
            {'name': 'status', 'type': 'STRING'},      # 可用枚举
            {'name': 'outcome', 'type': 'STRING'},     # 可用枚举
            {'name': 'probability', 'type': 'FLOAT64'}, # 可用FLOAT32
            {'name': 'confidence_score', 'type': 'FLOAT64'} # 可用FLOAT32
        ]
        
        for field in optimizable_fields:
            assert optimizer._can_optimize_type(field), f"{field['name']} 应该可以优化类型"
        
        # 不需要优化的类型
        non_optimizable_fields = [
            {'name': 'id', 'type': 'STRING'},
            {'name': 'description', 'type': 'STRING'},
            {'name': 'amount', 'type': 'FLOAT64'}  # 需要高精度
        ]
        
        for field in non_optimizable_fields:
            assert not optimizer._can_optimize_type(field), f"{field['name']} 不应该需要类型优化"


class TestMigrationScriptGeneration:
    """迁移脚本生成测试类"""
    
    def test_partition_migration_script(self):
        """测试分区迁移脚本生成"""
        optimizer = DatabaseTableOptimizer()
        
        script = optimizer._generate_partition_migration_script('test_table')
        
        # 验证脚本内容
        assert 'CREATE TABLE `test_table_partitioned`' in script
        assert 'PARTITION BY DATE(timestamp)' in script
        assert 'CLUSTER BY draw_id' in script
        assert 'bq query' in script
    
    def test_index_migration_script(self):
        """测试索引迁移脚本生成"""
        optimizer = DatabaseTableOptimizer()
        
        indexes = {'field1', 'field2'}
        script = optimizer._generate_index_migration_script('test_table', indexes)
        
        # 验证脚本内容
        assert 'CREATE INDEX' in script
        assert 'idx_test_table_field1' in script
        assert 'idx_test_table_field2' in script
        assert 'bq query' in script
    
    def test_field_removal_migration_script(self):
        """测试字段删除迁移脚本生成"""
        optimizer = DatabaseTableOptimizer()
        
        script = optimizer._generate_field_removal_migration('test_table', 'redundant_field')
        
        # 验证脚本内容
        assert 'backup' in script.lower()
        assert 'DROP COLUMN redundant_field' in script
        assert 'bq query' in script
    
    def test_field_archive_migration_script(self):
        """测试字段归档迁移脚本生成"""
        optimizer = DatabaseTableOptimizer()
        
        script = optimizer._generate_field_archive_migration('test_table', 'archive_field')
        
        # 验证脚本内容
        assert 'CREATE TABLE `test_table_archive`' in script
        assert 'archive_field' in script
        assert 'DROP COLUMN archive_field' in script
        assert 'bq query' in script


class TestOptimizationIntegration:
    """优化集成测试类"""
    
    def test_end_to_end_optimization_flow(self):
        """测试端到端优化流程"""
        with patch('database_table_optimizer.FieldUsageAnalyzer'):
            optimizer = DatabaseTableOptimizer()
            
            # 模拟完整优化流程
            with patch.object(optimizer, 'analyze_table_optimization_opportunities') as mock_analyze:
                mock_analyze.return_value = {
                    'test_table': [
                        TableOptimization(
                            table_name='test_table',
                            optimization_type='remove_field',
                            description='移除冗余字段',
                            impact='减少存储成本20%',
                            sql_before='-- 原始结构',
                            sql_after='ALTER TABLE test_table DROP COLUMN redundant_field;',
                            migration_script='# 迁移脚本',
                            estimated_savings={'storage_reduction': '20%'}
                        )
                    ]
                }
                
                # 生成完整报告
                report = optimizer.generate_optimization_report()
                
                # 验证报告完整性
                required_sections = [
                    'report_metadata',
                    'optimization_summary', 
                    'table_optimizations',
                    'sql_ddl_statements',
                    'migration_scripts',
                    'implementation_plan',
                    'risk_assessment'
                ]
                
                for section in required_sections:
                    assert section in report, f"报告缺少 {section} 部分"
                
                # 验证SQL DDL生成
                assert isinstance(report['sql_ddl_statements'], str)
                assert len(report['sql_ddl_statements']) > 0
                
                # 验证迁移脚本生成
                assert isinstance(report['migration_scripts'], dict)
                assert len(report['migration_scripts']) > 0
    
    def test_optimization_validation(self):
        """测试优化验证"""
        optimizer = DatabaseTableOptimizer()
        
        # 验证表结构定义
        for table_name, table_info in optimizer.current_tables.items():
            assert 'fields' in table_info
            assert 'indexes' in table_info
            assert 'estimated_size_gb' in table_info
            
            # 验证字段定义
            for field in table_info['fields']:
                assert 'name' in field
                assert 'type' in field
                assert 'nullable' in field
                assert 'usage' in field
                
                # 验证使用率分类
                assert field['usage'] in ['high', 'medium', 'low']


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "--tb=short", "--cov=database_table_optimizer", "--cov-report=html"])