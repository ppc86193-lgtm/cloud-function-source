#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段利用率分析测试用例
测试字段利用逻辑错误、冗余字段检测、未使用字段识别等
"""

import pytest
import json
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Any

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from field_usage_analysis import FieldUsageAnalyzer


class TestFieldUsageAnalyzer:
    """字段使用分析器测试类"""
    
    @pytest.fixture
    def analyzer(self):
        """创建分析器实例"""
        with patch('field_usage_analysis.PC28UpstreamAPI'), \
             patch('field_usage_analysis.RealtimeLotteryService'), \
             patch('field_usage_analysis.HistoryBackfillService'):
            return FieldUsageAnalyzer()
    
    @pytest.fixture
    def sample_realtime_data(self):
        """模拟实时API数据"""
        return {
            "codeid": "10000",
            "message": "success",
            "retdata": {
                "curent": {
                    "kjtime": "2024-01-15 10:30:00",
                    "long_issue": "20240115001",
                    "short_issue": "001",
                    "number": ["1", "2", "3"]
                },
                "next": {
                    "next_issue": "20240115002",
                    "next_time": "2024-01-15 10:33:00",
                    "award_time": "180"
                }
            },
            "curtime": "1705294200"
        }
    
    @pytest.fixture
    def sample_history_data(self):
        """模拟历史API数据"""
        return {
            "codeid": "10000",
            "message": "success",
            "retdata": [
                {
                    "kjtime": "2024-01-15 10:27:00",
                    "long_issue": "20240115000",
                    "number": ["4", "5", "6"]
                },
                {
                    "kjtime": "2024-01-15 10:24:00",
                    "long_issue": "20240114999",
                    "number": ["7", "8", "9"]
                }
            ],
            "curtime": "1705294200"
        }
    
    def test_redundant_field_detection(self, analyzer):
        """测试冗余字段检测"""
        # 测试result_digits与numbers字段重复
        internal_fields = analyzer.internal_fields
        
        # 检查是否存在冗余字段定义
        assert 'result_digits' in internal_fields
        assert 'numbers' in internal_fields
        
        # 验证字段描述表明它们是重复的
        assert '与numbers相同' in internal_fields['result_digits'] or \
               'numbers' in internal_fields['result_digits'].lower()
    
    def test_unused_field_identification(self, analyzer, sample_realtime_data):
        """测试未使用字段识别"""
        with patch.object(analyzer.api_client, 'get_realtime_lottery', 
                         return_value=sample_realtime_data):
            
            analysis = analyzer.analyze_realtime_api_usage()
            
            # 验证未使用字段被正确识别
            unused_fields = analysis.get('unused_fields', [])
            
            # curtime字段应该被标记为未使用
            assert any('curtime' in field for field in unused_fields)
            
            # next相关字段应该被标记为未使用
            next_fields = [field for field in unused_fields if 'next' in field.lower()]
            assert len(next_fields) > 0
    
    def test_field_mapping_consistency(self, analyzer):
        """测试字段映射一致性"""
        # 检查实时API和历史API字段映射的一致性
        realtime_fields = analyzer.realtime_api_fields
        history_fields = analyzer.history_api_fields
        
        # 共同字段应该有一致的描述
        common_fields = ['codeid', 'message', 'retdata']
        
        for field in common_fields:
            if field in realtime_fields and field in history_fields:
                # 基本字段描述应该一致
                assert realtime_fields[field] == history_fields[field]
    
    def test_data_type_conversion_logic(self, analyzer, sample_realtime_data):
        """测试数据类型转换逻辑"""
        with patch.object(analyzer.api_client, 'get_realtime_lottery', 
                         return_value=sample_realtime_data), \
             patch.object(analyzer.api_client, 'parse_lottery_data') as mock_parse:
            
            # 模拟解析后的数据
            mock_parse.return_value = [{
                'draw_id': '20240115001',
                'timestamp': '2024-01-15 10:30:00',
                'numbers': [1, 2, 3],  # 转换为整数
                'result_sum': 6,       # 计算得出
                'result_digits': [1, 2, 3]  # 冗余字段
            }]
            
            analysis = analyzer.analyze_field_mapping_efficiency()
            
            # 验证数据转换逻辑
            mapping_info = analysis.get('field_mapping_summary', {})
            transformations = mapping_info.get('data_transformation', {})
            
            assert 'string_to_int_conversion' in transformations
            assert 'sum_calculation' in transformations
    
    def test_field_usage_rate_calculation(self, analyzer, sample_realtime_data):
        """测试字段使用率计算"""
        with patch.object(analyzer.api_client, 'get_realtime_lottery', 
                         return_value=sample_realtime_data):
            
            analysis = analyzer.analyze_realtime_api_usage()
            
            # 验证使用率计算
            assert 'usage_rate' in analysis
            assert 0 <= analysis['usage_rate'] <= 100
            
            # 验证字段统计
            assert 'total_fields' in analysis
            assert 'used_fields' in analysis
            assert 'unused_fields' in analysis
            
            # 总字段数应该等于已使用+未使用字段数
            total = len(analysis['used_fields']) + len(analysis['unused_fields'])
            assert analysis['total_fields'] == total
    
    def test_efficiency_issue_detection(self, analyzer):
        """测试效率问题检测"""
        with patch.object(analyzer.api_client, 'get_realtime_lottery'), \
             patch.object(analyzer.api_client, 'get_history_lottery'), \
             patch.object(analyzer.api_client, 'parse_lottery_data'):
            
            analysis = analyzer.analyze_field_mapping_efficiency()
            
            # 验证效率问题检测
            efficiency_issues = analysis.get('efficiency_issues', {})
            
            assert 'redundant_fields' in efficiency_issues
            assert 'missing_optimizations' in efficiency_issues
            
            # 应该检测到result_digits冗余
            redundant_fields = efficiency_issues['redundant_fields']
            assert any('result_digits' in issue for issue in redundant_fields)
    
    def test_threading_analysis(self, analyzer):
        """测试多线程分析"""
        analysis = analyzer.analyze_threading_implementation()
        
        # 验证当前实现分析
        current_impl = analysis.get('current_implementation', {})
        assert 'type' in current_impl
        assert 'bottlenecks' in current_impl
        
        # 验证优化机会识别
        opportunities = analysis.get('threading_opportunities', {})
        assert 'realtime_monitoring' in opportunities
        assert 'history_backfill' in opportunities
        assert 'api_calls' in opportunities
    
    def test_optimization_recommendations(self, analyzer):
        """测试优化建议生成"""
        recommendations = analyzer.generate_optimization_recommendations()
        
        # 验证优化建议结构
        assert 'multi_threading_optimizations' in recommendations
        assert 'field_usage_optimizations' in recommendations
        assert 'api_integration_optimizations' in recommendations
        
        # 验证每个类别都有优先级和具体建议
        for category in recommendations.values():
            if isinstance(category, dict):
                assert 'priority' in category
                assert 'recommendations' in category
                
                for rec in category['recommendations']:
                    assert 'title' in rec
                    assert 'description' in rec
                    assert 'implementation' in rec
                    assert 'expected_benefit' in rec
    
    def test_complete_analysis_integration(self, analyzer):
        """测试完整分析集成"""
        with patch.object(analyzer, 'analyze_realtime_api_usage', 
                         return_value={'usage_rate': 75.0}), \
             patch.object(analyzer, 'analyze_history_api_usage', 
                         return_value={'usage_rate': 80.0}), \
             patch.object(analyzer, 'analyze_threading_implementation', 
                         return_value={'current_implementation': {'type': 'single_thread'}}), \
             patch.object(analyzer, 'analyze_field_mapping_efficiency', 
                         return_value={'efficiency_issues': {}}), \
             patch.object(analyzer, 'generate_optimization_recommendations', 
                         return_value={'recommendations': []}):
            
            results = analyzer.run_complete_analysis()
            
            # 验证完整分析结果结构
            assert 'analysis_metadata' in results
            assert 'realtime_api_analysis' in results
            assert 'history_api_analysis' in results
            assert 'threading_analysis' in results
            assert 'field_mapping_analysis' in results
            assert 'optimization_recommendations' in results
            
            # 验证元数据
            metadata = results['analysis_metadata']
            assert 'timestamp' in metadata
            assert 'analyzer_version' in metadata
            assert 'api_endpoints' in metadata


class TestFieldUsageLogicErrors:
    """字段使用逻辑错误测试类"""
    
    def test_field_mapping_errors(self):
        """测试字段映射错误"""
        analyzer = FieldUsageAnalyzer()
        
        # 测试不存在的字段映射
        assert not analyzer._is_field_used_in_code('nonexistent_field')
        
        # 测试已知未使用字段
        assert not analyzer._is_field_used_in_code('curtime')
        assert not analyzer._is_field_used_in_code('retdata.next.next_issue')
        
        # 测试已知使用字段
        assert analyzer._is_field_used_in_code('retdata.curent.kjtime')
        assert analyzer._is_field_used_in_code('retdata.curent.long_issue')
    
    def test_data_structure_inconsistency(self):
        """测试数据结构不一致性"""
        analyzer = FieldUsageAnalyzer()
        
        # 实时数据和历史数据结构不同
        realtime_structure = 'retdata.curent.kjtime'
        history_structure = 'retdata[].kjtime'
        
        # 两种结构都应该被正确处理
        assert analyzer._is_field_used_in_code(realtime_structure)
        assert analyzer._is_field_used_in_code(history_structure)
    
    def test_redundant_field_logic(self):
        """测试冗余字段逻辑"""
        analyzer = FieldUsageAnalyzer()
        
        # result_digits和numbers字段功能重复
        internal_fields = analyzer.internal_fields
        
        # 验证冗余字段存在
        assert 'result_digits' in internal_fields
        assert 'numbers' in internal_fields
        
        # 验证描述表明冗余
        digits_desc = internal_fields['result_digits']
        numbers_desc = internal_fields['numbers']
        
        # result_digits应该被标记为与numbers相同或类似
        assert 'numbers' in digits_desc.lower() or '相同' in digits_desc


class TestDataQualityValidation:
    """数据质量验证测试类"""
    
    def test_field_type_validation(self):
        """测试字段类型验证"""
        # 测试数字字段类型转换
        test_data = {
            'number': ['1', '2', '3'],  # 字符串数组
            'expected': [1, 2, 3]       # 期望的整数数组
        }
        
        # 模拟类型转换逻辑
        converted = [int(x) for x in test_data['number']]
        assert converted == test_data['expected']
    
    def test_timestamp_parsing(self):
        """测试时间戳解析"""
        from datetime import datetime
        
        # 测试时间字符串解析
        time_str = "2024-01-15 10:30:00"
        
        try:
            parsed_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            assert parsed_time.year == 2024
            assert parsed_time.month == 1
            assert parsed_time.day == 15
        except ValueError:
            pytest.fail("时间戳解析失败")
    
    def test_data_completeness(self):
        """测试数据完整性"""
        required_fields = ['draw_id', 'timestamp', 'numbers']
        
        # 完整数据
        complete_data = {
            'draw_id': '20240115001',
            'timestamp': '2024-01-15 10:30:00',
            'numbers': [1, 2, 3]
        }
        
        # 验证所有必需字段存在
        for field in required_fields:
            assert field in complete_data
            assert complete_data[field] is not None
        
        # 不完整数据
        incomplete_data = {
            'draw_id': '20240115001',
            'timestamp': None,  # 缺失时间戳
            'numbers': [1, 2, 3]
        }
        
        # 验证数据不完整
        missing_fields = [field for field in required_fields 
                         if field not in incomplete_data or incomplete_data[field] is None]
        assert len(missing_fields) > 0


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "--tb=short"])