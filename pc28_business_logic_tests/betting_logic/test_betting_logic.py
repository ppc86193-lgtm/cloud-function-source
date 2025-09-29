import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import pytest
import unittest
from pc28_mock_services import *
from decimal import Decimal

class TestBettingLogic(unittest.TestCase):
    """投注逻辑测试套件"""
    
    def setUp(self):
        """测试初始化"""
        self.betting_service = MockBettingService()
        self.user_service = MockUserService()
        self.balance_service = MockBalanceService()
        
    def test_bet_placement_validation(self):
        """测试投注下单验证"""
        # 有效投注
        valid_bet = {
            'user_id': 'user123',
            'period': '20241229001',
            'bet_type': '大小',
            'bet_option': '大',
            'amount': Decimal('100.00')
        }
        
        result = self.betting_service.place_bet(valid_bet)
        self.assertTrue(result)
        
    def test_bet_amount_limits(self):
        """测试投注金额限制"""
        # 测试最小投注金额
        min_bet = Decimal('1.00')
        self.assertTrue(self.betting_service.validate_amount(min_bet))
        
        # 测试最大投注金额
        max_bet = Decimal('10000.00')
        self.assertTrue(self.betting_service.validate_amount(max_bet))
        
        # 测试超限金额
        over_limit = Decimal('50000.00')
        self.assertFalse(self.betting_service.validate_amount(over_limit))
        
    def test_bet_type_validation(self):
        """测试投注类型验证"""
        valid_types = ['大小', '单双', '豹子', '对子', '顺子']
        
        for bet_type in valid_types:
            self.assertTrue(self.betting_service.validate_bet_type(bet_type))
            
        # 无效投注类型
        self.assertFalse(self.betting_service.validate_bet_type('无效类型'))
        
    def test_user_balance_check(self):
        """测试用户余额检查"""
        user_id = 'user123'
        bet_amount = Decimal('100.00')
        
        # 余额充足
        self.assertTrue(self.betting_service.check_balance(user_id, bet_amount))
        
        # 余额不足
        self.assertFalse(self.betting_service.check_balance(user_id, bet_amount))
        
    def test_bet_period_validation(self):
        """测试投注期次验证"""
        current_period = datetime.now().strftime('%Y%m%d001')
        
        # 当前期次可投注
        self.assertTrue(self.betting_service.validate_period(current_period))
        
        # 过期期次不可投注
        expired_period = '20241228001'
        self.assertFalse(self.betting_service.validate_period(expired_period))
        
    def test_bet_odds_calculation(self):
        """测试投注赔率计算"""
        bet_types_odds = {
            '大小': Decimal('1.98'),
            '单双': Decimal('1.98'),
            '豹子': Decimal('180.00'),
            '对子': Decimal('60.00')
        }
        
        for bet_type, expected_odds in bet_types_odds.items():
            odds = self.betting_service.get_odds(bet_type)
            self.assertEqual(odds, expected_odds)

if __name__ == '__main__':
    unittest.main()
