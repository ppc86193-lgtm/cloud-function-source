import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import pytest
import unittest
from pc28_mock_services import *
from datetime import datetime, timedelta

class TestLotteryLogic(unittest.TestCase):
    """彩票逻辑测试套件"""
    
    def setUp(self):
        """测试初始化"""
        self.lottery_service = MockLotteryService()
        self.draw_service = MockLotteryService()
        
    def test_lottery_draw_generation(self):
        """测试彩票开奖号码生成"""
        # 测试正常开奖
        result = self.lottery_service.generate_draw_numbers()
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 3)  # PC28应该有3个号码
        
        # 测试号码范围
        for num in result:
            self.assertGreaterEqual(num, 0)
            self.assertLessEqual(num, 27)
            
    def test_lottery_draw_validation(self):
        """测试开奖结果验证"""
        valid_draw = [1, 15, 8]
        invalid_draw = [28, 15, 8]  # 超出范围
        
        self.assertTrue(self.lottery_service.validate_draw(valid_draw))
        self.assertFalse(self.lottery_service.validate_draw(invalid_draw))
        
    def test_lottery_period_management(self):
        """测试彩票期次管理"""
        current_period = self.lottery_service.get_current_period()
        next_period = self.lottery_service.get_next_period()
        
        self.assertIsNotNone(current_period)
        self.assertGreater(next_period, current_period)
        
    def test_lottery_draw_timing(self):
        """测试开奖时间控制"""
        # 测试开奖时间间隔
        last_draw_time = datetime.now() - timedelta(minutes=5)
        can_draw = self.lottery_service.can_draw_now(last_draw_time)
        self.assertTrue(can_draw)
        
        # 测试开奖冷却期
        recent_draw_time = datetime.now() - timedelta(seconds=30)
        cannot_draw = self.lottery_service.can_draw_now(recent_draw_time)
        self.assertFalse(cannot_draw)
        
    def test_lottery_result_calculation(self):
        """测试开奖结果计算"""
        draw_numbers = [1, 15, 8]
        expected_sum = 24  # 1+15+8
        
        result_sum = self.lottery_service.calculate_sum(draw_numbers)
        self.assertEqual(result_sum, expected_sum)
        
        # 测试大小单双
        size_result = self.lottery_service.calculate_size(expected_sum)
        parity_result = self.lottery_service.calculate_parity(expected_sum)
        
        self.assertIn(size_result, ['大', '小'])
        self.assertIn(parity_result, ['单', '双'])

if __name__ == '__main__':
    unittest.main()
