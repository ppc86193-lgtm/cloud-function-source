import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import pytest
import unittest
from pc28_mock_services import *
from decimal import Decimal

class TestPayoutLogic(unittest.TestCase):
    """支付逻辑测试套件"""
    
    def setUp(self):
        """测试初始化"""
        self.payout_service = MockPayoutService()
        self.bet_service = MockBettingService()
        self.balance_service = MockBalanceService()
        
    def test_winning_calculation(self):
        """测试中奖金额计算"""
        bet_amount = Decimal('100.00')
        odds = Decimal('1.98')
        expected_payout = bet_amount * odds
        
        payout = self.payout_service.calculate_payout(bet_amount, odds)
        self.assertEqual(payout, expected_payout)
        
    def test_payout_processing(self):
        """测试派奖处理"""
        winning_bet = {
            'bet_id': 'bet123',
            'user_id': 'user123',
            'amount': Decimal('100.00'),
            'odds': Decimal('1.98'),
            'payout': Decimal('198.00')
        }
        
        result = self.payout_service.process_payout(winning_bet)
        self.assertTrue(result)
        
    def test_balance_update(self):
        """测试余额更新"""
        user_id = 'user123'
        payout_amount = Decimal('198.00')
        
        # 模拟余额更新
        result = self.balance_service.add_balance(user_id, payout_amount)
        self.assertTrue(result)
        
    def test_payout_limits(self):
        """测试派奖限制"""
        # 单笔最大派奖
        max_payout = Decimal('100000.00')
        self.assertTrue(self.payout_service.validate_payout_limit(max_payout))
        
        # 超限派奖
        over_limit = Decimal('1000000.00')
        self.assertFalse(self.payout_service.validate_payout_limit(over_limit))
        
    def test_payout_tax_calculation(self):
        """测试派奖税收计算"""
        # 小额中奖无税
        small_payout = Decimal('1000.00')
        tax = self.payout_service.calculate_tax(small_payout)
        self.assertEqual(tax, Decimal('0.00'))
        
        # 大额中奖需缴税
        large_payout = Decimal('50000.00')
        tax = self.payout_service.calculate_tax(large_payout)
        self.assertGreater(tax, Decimal('0.00'))
        
    def test_payout_record_creation(self):
        """测试派奖记录创建"""
        payout_record = {
            'user_id': 'user123',
            'bet_id': 'bet123',
            'amount': Decimal('198.00'),
            'tax': Decimal('0.00'),
            'net_amount': Decimal('198.00'),
            'status': 'completed'
        }
        
        record_id = self.payout_service.create_record(payout_record)
        self.assertIsNotNone(record_id)

if __name__ == '__main__':
    unittest.main()
