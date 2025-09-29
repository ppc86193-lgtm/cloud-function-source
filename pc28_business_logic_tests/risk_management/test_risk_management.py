import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import pytest
import unittest
from pc28_mock_services import *
from decimal import Decimal
from datetime import datetime, timedelta

class TestRiskManagement(unittest.TestCase):
    """风险管理测试套件"""
    
    def setUp(self):
        """测试初始化"""
        self.risk_service = MockRiskService()
        self.user_service = MockUserService()
        self.betting_service = MockBettingService()
        
    def test_user_betting_limit(self):
        """测试用户投注限制"""
        user_id = 'user123'
        
        # 测试超限情况 - 设置一个很小的限额确保超限
        small_limit = Decimal('500.00')
        result_over_limit = self.risk_service.check_daily_limit(user_id, small_limit)
        self.assertFalse(result_over_limit)
        
        # 测试未超限情况 - 设置一个很大的限额确保未超限
        large_limit = Decimal('10000.00')
        result_within_limit = self.risk_service.check_daily_limit(user_id, large_limit)
        self.assertTrue(result_within_limit)
        
    def test_suspicious_activity_detection(self):
        """测试可疑活动检测"""
        user_id = 'user123'
        
        # 正常投注模式
        normal_pattern = {
            'frequency': 10,  # 每小时10次
            'amount_variance': 0.2,  # 金额变化20%
            'win_rate': 0.45  # 胜率45%
        }
        self.assertFalse(self.risk_service.detect_suspicious_activity(user_id, normal_pattern))
        
        # 异常投注模式
        suspicious_pattern = {
            'frequency': 100,  # 每小时100次
            'amount_variance': 0.01,  # 金额变化1%
            'win_rate': 0.95  # 胜率95%
        }
        self.assertTrue(self.risk_service.detect_suspicious_activity(user_id, suspicious_pattern))
        
    def test_account_security_check(self):
        """测试账户安全检查"""
        user_id = 'user123'
        
        # 安全账户
        safe_account = {
            'login_ip_changes': 1,
            'password_changes': 0,
            'failed_logins': 2
        }
        self.assertTrue(self.risk_service.check_account_security(user_id, safe_account))
        
        # 风险账户
        risky_account = {
            'login_ip_changes': 10,
            'password_changes': 5,
            'failed_logins': 20
        }
        self.assertFalse(self.risk_service.check_account_security(user_id, risky_account))
        
    def test_betting_pattern_analysis(self):
        """测试投注模式分析"""
        betting_history = [
            {'amount': Decimal('100'), 'type': '大小', 'result': 'win'},
            {'amount': Decimal('200'), 'type': '大小', 'result': 'lose'},
            {'amount': Decimal('400'), 'type': '大小', 'result': 'lose'},
            {'amount': Decimal('800'), 'type': '大小', 'result': 'win'}
        ]
        
        # 检测倍投模式
        is_martingale = self.risk_service.detect_martingale_pattern(betting_history)
        self.assertTrue(is_martingale)
        
    def test_platform_risk_limits(self):
        """测试平台风险限制"""
        period = '20241229001'
        
        # 单期总投注限制
        total_bets = Decimal('1000000.00')
        period_limit = Decimal('5000000.00')
        self.assertTrue(self.risk_service.check_period_limit(period, total_bets, period_limit))
        
        # 超过单期限制
        over_limit_bets = Decimal('6000000.00')
        self.assertFalse(self.risk_service.check_period_limit(period, over_limit_bets, period_limit))
        
    def test_auto_risk_response(self):
        """测试自动风险响应"""
        risk_event = {
            'type': 'suspicious_betting',
            'user_id': 'user123',
            'severity': 'high',
            'details': '异常高频投注'
        }
        
        # 自动处理风险事件
        response = self.risk_service.handle_risk_event(risk_event)
        self.assertIn('action', response)
        self.assertIn(response['action'], ['freeze_account', 'limit_betting', 'manual_review'])

if __name__ == '__main__':
    unittest.main()
