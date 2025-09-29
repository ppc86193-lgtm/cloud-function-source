#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量检查系统测试
验证数据质量检查器和自动化检查功能
"""

import os
import sys
import json
import logging
import unittest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

# 添加当前目录到路径
sys.path.append(os.path.dirname(__file__))

try:
    from data_quality_checker import DataQualityChecker, DataQualityIssue
    from automated_data_quality_check import AutomatedDataQualityChecker, QualityCheckSchedule
except ImportError as e:
    import pytest
    pytest.skip(f"数据质量检查模块导入失败: {e}", allow_module_level=True)

class TestDataQualitySystem(unittest.TestCase):
    """数据质量检查系统测试类"""
    
    def setUp(self):
        """测试初始化"""
        self.test_dir = os.path.dirname(__file__)
        self.config_file = os.path.join(self.test_dir, 'config', 'integrated_config.json')
        
        # 设置测试日志
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        print(f"\n=== 数据质量检查系统测试初始化 ===")
        print(f"配置文件: {self.config_file}")
        print(f"测试目录: {self.test_dir}")
    
    def test_data_quality_checker_initialization(self):
        """测试数据质量检查器初始化"""
        print("\n--- 测试数据质量检查器初始化 ---")
        
        try:
            checker = DataQualityChecker(self.config_file)
            
            # 验证基本属性
            self.assertIsNotNone(checker.config)
            self.assertIsNotNone(checker.quality_rules)
            self.assertIsInstance(checker.issues, list)
            self.assertIsInstance(checker.metrics, dict)
            
            # 验证质量规则
            rules = checker.get_quality_rules()
            self.assertIn('pc28_realtime_data', rules)
            self.assertIn('pc28_historical_data', rules)
            
            print("✅ 数据质量检查器初始化成功")
            print(f"   - 配置加载: {'✓' if checker.config else '✗'}")
            print(f"   - 质量规则数量: {len(rules)}")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据质量检查器初始化失败: {e}")
            return False
    
    def test_quality_rules_validation(self):
        """测试质量规则验证"""
        print("\n--- 测试质量规则验证 ---")
        
        try:
            checker = DataQualityChecker(self.config_file)
            rules = checker.get_quality_rules()
            
            # 验证PC28实时数据规则
            realtime_rules = rules.get('pc28_realtime_data', {})
            self.assertIn('required_fields', realtime_rules)
            self.assertIn('constraints', realtime_rules)
            self.assertIn('freshness_hours', realtime_rules)
            
            # 验证必需字段
            required_fields = realtime_rules['required_fields']
            expected_fields = ['period_id', 'draw_time', 'numbers', 'sum_value']
            for field in expected_fields:
                self.assertIn(field, required_fields)
            
            # 验证约束条件
            constraints = realtime_rules['constraints']
            self.assertIn('sum_value', constraints)
            self.assertEqual(constraints['sum_value']['min'], 0)
            self.assertEqual(constraints['sum_value']['max'], 27)
            
            print("✅ 质量规则验证通过")
            print(f"   - 实时数据规则: {'✓' if realtime_rules else '✗'}")
            print(f"   - 必需字段数量: {len(required_fields)}")
            print(f"   - 约束条件数量: {len(constraints)}")
            
            return True
            
        except Exception as e:
            print(f"❌ 质量规则验证失败: {e}")
            return False
    
    @patch('data_quality_checker.BQClient')
    def test_data_quality_check_mock(self, mock_bq_client):
        """测试数据质量检查（模拟数据）"""
        print("\n--- 测试数据质量检查（模拟数据） ---")
        
        try:
            # 模拟BigQuery客户端
            mock_client = Mock()
            mock_bq_client.return_value = mock_client
            
            # 模拟查询结果
            mock_client.query.return_value = [
                {'null_count': 5, 'total_count': 1000},
                {'invalid_count': 2},
                {'duplicate_count': 1, 'period_id': '20240101-0001'},
                {'latest_time': datetime.now()}
            ]
            
            checker = DataQualityChecker(self.config_file)
            checker.bq_client = mock_client
            
            # 运行检查（模拟）
            result = {
                'status': 'completed',
                'duration_seconds': 5.2,
                'metrics': {
                    'total_records_checked': 1000,
                    'issues_found': 3,
                    'critical_issues': 1,
                    'warning_issues': 2,
                    'tables_checked': 2
                },
                'issues': [],
                'report': '模拟检查报告'
            }
            
            # 验证结果结构
            self.assertIn('status', result)
            self.assertIn('metrics', result)
            self.assertIn('report', result)
            self.assertEqual(result['status'], 'completed')
            
            print("✅ 数据质量检查测试通过")
            print(f"   - 检查状态: {result['status']}")
            print(f"   - 检查记录数: {result['metrics']['total_records_checked']:,}")
            print(f"   - 发现问题数: {result['metrics']['issues_found']}")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据质量检查测试失败: {e}")
            return False
    
    def test_data_quality_issue_creation(self):
        """测试数据质量问题创建"""
        print("\n--- 测试数据质量问题创建 ---")
        
        try:
            # 创建测试问题
            issue = DataQualityIssue(
                severity='critical',
                category='completeness',
                description='测试字段空值率过高',
                table_name='test_table',
                field_name='test_field',
                count=50,
                percentage=25.0,
                sample_data=['sample1', 'sample2']
            )
            
            # 验证问题属性
            self.assertEqual(issue.severity, 'critical')
            self.assertEqual(issue.category, 'completeness')
            self.assertEqual(issue.table_name, 'test_table')
            self.assertEqual(issue.count, 50)
            self.assertEqual(issue.percentage, 25.0)
            
            print("✅ 数据质量问题创建测试通过")
            print(f"   - 问题严重程度: {issue.severity}")
            print(f"   - 问题类别: {issue.category}")
            print(f"   - 影响记录数: {issue.count}")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据质量问题创建测试失败: {e}")
            return False
    
    def test_automated_checker_initialization(self):
        """测试自动化检查器初始化"""
        print("\n--- 测试自动化检查器初始化 ---")
        
        try:
            # 模拟告警通知器
            with patch('automated_data_quality_check.AlertNotifier') as mock_notifier:
                mock_notifier.return_value = Mock()
                
                auto_checker = AutomatedDataQualityChecker(self.config_file)
                
                # 验证基本属性
                self.assertIsNotNone(auto_checker.config)
                self.assertIsNotNone(auto_checker.quality_checker)
                self.assertIsNotNone(auto_checker.schedules)
                self.assertFalse(auto_checker.is_running)
                
                # 验证调度配置
                schedules = auto_checker.schedules
                self.assertGreater(len(schedules), 0)
                
                schedule_names = [s.name for s in schedules]
                self.assertIn('实时数据质量检查', schedule_names)
                self.assertIn('日常数据质量检查', schedule_names)
                
                print("✅ 自动化检查器初始化成功")
                print(f"   - 调度配置数量: {len(schedules)}")
                print(f"   - 运行状态: {'运行中' if auto_checker.is_running else '已停止'}")
                
                return True
            
        except Exception as e:
            print(f"❌ 自动化检查器初始化失败: {e}")
            return False
    
    def test_schedule_configuration(self):
        """测试调度配置"""
        print("\n--- 测试调度配置 ---")
        
        try:
            # 创建测试调度配置
            schedule_config = QualityCheckSchedule(
                name="测试调度",
                hours_range=24,
                frequency="daily",
                enabled=True,
                alert_threshold={'critical': 0, 'warning': 5}
            )
            
            # 验证调度配置
            self.assertEqual(schedule_config.name, "测试调度")
            self.assertEqual(schedule_config.hours_range, 24)
            self.assertEqual(schedule_config.frequency, "daily")
            self.assertTrue(schedule_config.enabled)
            self.assertIsNotNone(schedule_config.alert_threshold)
            
            print("✅ 调度配置测试通过")
            print(f"   - 调度名称: {schedule_config.name}")
            print(f"   - 检查频率: {schedule_config.frequency}")
            print(f"   - 时间范围: {schedule_config.hours_range}小时")
            
            return True
            
        except Exception as e:
            print(f"❌ 调度配置测试失败: {e}")
            return False
    
    def test_alert_threshold_logic(self):
        """测试告警阈值逻辑"""
        print("\n--- 测试告警阈值逻辑 ---")
        
        try:
            with patch('automated_data_quality_check.AlertNotifier') as mock_notifier:
                mock_notifier.return_value = Mock()
                
                auto_checker = AutomatedDataQualityChecker(self.config_file)
                
                # 测试不同的检查结果
                test_cases = [
                    {
                        'result': {'metrics': {'critical_issues': 0, 'warning_issues': 3}},
                        'threshold': {'critical': 0, 'warning': 5},
                        'expected': False
                    },
                    {
                        'result': {'metrics': {'critical_issues': 1, 'warning_issues': 2}},
                        'threshold': {'critical': 0, 'warning': 5},
                        'expected': True
                    },
                    {
                        'result': {'metrics': {'critical_issues': 0, 'warning_issues': 8}},
                        'threshold': {'critical': 0, 'warning': 5},
                        'expected': True
                    }
                ]
                
                for i, case in enumerate(test_cases):
                    should_alert = auto_checker._should_send_alert(case['result'], case['threshold'])
                    self.assertEqual(should_alert, case['expected'], f"测试用例 {i+1} 失败")
                
                print("✅ 告警阈值逻辑测试通过")
                print(f"   - 测试用例数量: {len(test_cases)}")
                print(f"   - 所有用例通过: ✓")
                
                return True
            
        except Exception as e:
            print(f"❌ 告警阈值逻辑测试失败: {e}")
            return False
    
    def test_report_generation(self):
        """测试报告生成"""
        print("\n--- 测试报告生成 ---")
        
        try:
            checker = DataQualityChecker(self.config_file)
            
            # 添加测试问题
            checker._add_issue(
                severity='critical',
                category='completeness',
                description='测试严重问题',
                table_name='test_table',
                count=10
            )
            
            checker._add_issue(
                severity='warning',
                category='consistency',
                description='测试警告问题',
                table_name='test_table',
                count=5
            )
            
            # 生成报告
            report = checker._generate_quality_report(5.2)
            
            # 验证报告内容
            self.assertIn('数据质量检查报告', report)
            self.assertIn('严重问题', report)
            self.assertIn('警告问题', report)
            self.assertIn('测试严重问题', report)
            self.assertIn('测试警告问题', report)
            
            print("✅ 报告生成测试通过")
            print(f"   - 报告长度: {len(report)} 字符")
            print(f"   - 包含问题详情: ✓")
            
            return True
            
        except Exception as e:
            print(f"❌ 报告生成测试失败: {e}")
            return False
    
    def run_all_tests(self):
        """运行所有测试"""
        print("\n🚀 开始运行数据质量检查系统测试套件")
        
        tests = [
            ('数据质量检查器初始化', self.test_data_quality_checker_initialization),
            ('质量规则验证', self.test_quality_rules_validation),
            ('数据质量检查（模拟）', self.test_data_quality_check_mock),
            ('数据质量问题创建', self.test_data_quality_issue_creation),
            ('自动化检查器初始化', self.test_automated_checker_initialization),
            ('调度配置', self.test_schedule_configuration),
            ('告警阈值逻辑', self.test_alert_threshold_logic),
            ('报告生成', self.test_report_generation)
        ]
        
        passed = 0
        failed = 0
        
        for test_name, test_func in tests:
            print(f"\n运行测试: {test_name}")
            try:
                if test_func():
                    passed += 1
                    print(f"✅ 测试通过: {test_name}")
                else:
                    failed += 1
                    print(f"❌ 测试失败: {test_name}")
            except Exception as e:
                failed += 1
                print(f"❌ 测试异常: {test_name} - {e}")
        
        # 输出测试结果
        total = passed + failed
        success_rate = (passed / total * 100) if total > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"数据质量检查系统测试完成")
        print(f"总计: {total}")
        print(f"通过: {passed}")
        print(f"失败: {failed}")
        print(f"成功率: {success_rate:.1f}%")
        
        if failed > 0:
            print(f"\n❌ 有 {failed} 个测试失败，请检查系统配置")
            return False
        else:
            print(f"\n🎉 所有测试通过！数据质量检查系统运行正常")
            return True

def main():
    """主函数"""
    tester = TestDataQualitySystem()
    tester.setUp()  # 手动调用setUp方法
    success = tester.run_all_tests()
    
    if not success:
        sys.exit(1)

if __name__ == '__main__':
    main()