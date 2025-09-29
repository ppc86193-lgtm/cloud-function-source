import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import pytest
import unittest
from pc28_mock_services import *
import pandas as pd
from datetime import datetime

class TestDataProcessing(unittest.TestCase):
    """数据处理测试套件"""
    
    def setUp(self):
        """测试初始化"""
        self.data_service = MockDataService()
        self.etl_service = MockETLService()
        
    def test_data_validation(self):
        """测试数据验证"""
        # 有效数据
        valid_data = {
            'user_id': 'user123',
            'bet_amount': 100.00,
            'bet_time': datetime.now(),
            'period': '20241229001'
        }
        self.assertTrue(self.data_service.validate_data(valid_data))
        
        # 无效数据
        invalid_data = {
            'user_id': '',  # 空用户ID
            'bet_amount': -100.00,  # 负金额
            'bet_time': None,  # 空时间
            'period': 'invalid'  # 无效期次
        }
        self.assertFalse(self.data_service.validate_data(invalid_data))
        
    def test_data_transformation(self):
        """测试数据转换"""
        raw_data = {
            'amount': '100.50',
            'timestamp': '2024-12-29 10:30:00',
            'status': '1'
        }
        
        transformed = self.data_service.transform_data(raw_data)
        
        self.assertIsInstance(transformed['amount'], float)
        self.assertIsInstance(transformed['timestamp'], datetime)
        self.assertIsInstance(transformed['status'], bool)
        
    def test_data_aggregation(self):
        """测试数据聚合"""
        betting_data = [
            {'user_id': 'user1', 'amount': 100, 'period': '001'},
            {'user_id': 'user1', 'amount': 200, 'period': '001'},
            {'user_id': 'user2', 'amount': 150, 'period': '001'}
        ]
        
        aggregated = self.data_service.aggregate_by_user(betting_data)
        
        self.assertEqual(aggregated['user1']['total_amount'], 300)
        self.assertEqual(aggregated['user1']['bet_count'], 2)
        self.assertEqual(aggregated['user2']['total_amount'], 150)
        
    def test_data_cleaning(self):
        """测试数据清洗"""
        dirty_data = [
            {'id': 1, 'amount': 100, 'status': 'valid'},
            {'id': 2, 'amount': None, 'status': 'invalid'},  # 空值
            {'id': 3, 'amount': -50, 'status': 'valid'},     # 异常值
            {'id': 4, 'amount': 200, 'status': 'valid'}
        ]
        
        cleaned = self.data_service.clean_data(dirty_data)
        
        # 应该只保留有效数据
        self.assertEqual(len(cleaned), 2)
        self.assertTrue(all(item['amount'] > 0 for item in cleaned))
        
    def test_data_export(self):
        """测试数据导出"""
        export_data = [
            {'period': '001', 'total_bets': 1000, 'total_amount': 50000},
            {'period': '002', 'total_bets': 1200, 'total_amount': 60000}
        ]
        
        # 导出为CSV
        csv_result = self.data_service.export_to_csv(export_data)
        self.assertTrue(csv_result)
        
        # 导出为JSON
        json_result = self.data_service.export_to_json(export_data)
        self.assertTrue(json_result)
        
    def test_real_time_processing(self):
        """测试实时数据处理"""
        stream_data = {
            'event_type': 'bet_placed',
            'user_id': 'user123',
            'amount': 100,
            'timestamp': datetime.now()
        }
        
        # 实时处理
        processed = self.data_service.process_real_time(stream_data)
        self.assertIsNotNone(processed)
        self.assertIn('processed_at', processed)

if __name__ == '__main__':
    unittest.main()
