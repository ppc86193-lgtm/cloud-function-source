#!/usr/bin/env python3
"""
PC28模拟服务类
为测试提供真实的业务逻辑实现，避免Mock问题
"""

from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import random

class MockLotteryService:
    """模拟彩票服务"""
    
    def generate_draw_numbers(self) -> List[int]:
        """生成开奖号码"""
        return [random.randint(0, 27) for _ in range(3)]
        
    def validate_draw(self, numbers: List[int]) -> bool:
        """验证开奖号码"""
        if len(numbers) != 3:
            return False
        return all(0 <= num <= 27 for num in numbers)
        
    def get_current_period(self) -> str:
        """获取当前期次"""
        now = datetime.now()
        return f"{now.strftime('%Y%m%d')}{now.hour:03d}"
        
    def get_next_period(self) -> str:
        """获取下一期次"""
        current = int(self.get_current_period())
        return str(current + 1)
        
    def can_draw_now(self, last_draw_time: datetime) -> bool:
        """检查是否可以开奖"""
        return datetime.now() - last_draw_time > timedelta(minutes=1)
        
    def calculate_sum(self, numbers: List[int]) -> int:
        """计算号码和值"""
        return sum(numbers)
        
    def calculate_size(self, sum_value: int) -> str:
        """计算大小"""
        return '大' if sum_value >= 14 else '小'
        
    def calculate_parity(self, sum_value: int) -> str:
        """计算单双"""
        return '单' if sum_value % 2 == 1 else '双'

class MockBettingService:
    """模拟投注服务"""
    
    def validate_bet(self, bet_data: Dict) -> bool:
        """验证投注"""
        required_fields = ['user_id', 'period', 'bet_type', 'bet_option', 'amount']
        return all(field in bet_data for field in required_fields)
        
    def place_bet(self, bet_data: Dict) -> bool:
        """下注"""
        return self.validate_bet(bet_data)
        
    def validate_amount(self, amount: Decimal) -> bool:
        """验证投注金额"""
        return Decimal('1.00') <= amount <= Decimal('10000.00')
        
    def validate_bet_type(self, bet_type: str) -> bool:
        """验证投注类型"""
        valid_types = ['大小', '单双', '豹子', '对子', '顺子']
        return bet_type in valid_types
        
    def check_balance(self, user_id: str, amount: Decimal) -> bool:
        """检查余额"""
        # 模拟余额检查，假设用户有足够余额
        return amount <= Decimal('1000.00')
        
    def validate_period(self, period: str) -> bool:
        """验证期次"""
        current_period = datetime.now().strftime('%Y%m%d')
        return period.startswith(current_period)
        
    def get_odds(self, bet_type: str) -> Decimal:
        """获取赔率"""
        odds_map = {
            '大小': Decimal('1.98'),
            '单双': Decimal('1.98'),
            '豹子': Decimal('180.00'),
            '对子': Decimal('60.00'),
            '顺子': Decimal('30.00')
        }
        return odds_map.get(bet_type, Decimal('1.00'))

class MockPayoutService:
    """模拟派奖服务"""
    
    def calculate_payout(self, bet_amount: Decimal, odds: Decimal) -> Decimal:
        """计算派奖金额"""
        return bet_amount * odds
        
    def process_payout(self, winning_bet: Dict) -> bool:
        """处理派奖"""
        return 'bet_id' in winning_bet and 'payout' in winning_bet
        
    def validate_payout_limit(self, amount: Decimal) -> bool:
        """验证派奖限制"""
        return amount <= Decimal('100000.00')
        
    def calculate_tax(self, payout: Decimal) -> Decimal:
        """计算税收"""
        if payout > Decimal('10000.00'):
            return payout * Decimal('0.20')  # 20%税率
        return Decimal('0.00')
        
    def create_record(self, payout_record: Dict) -> str:
        """创建派奖记录"""
        return f"record_{datetime.now().strftime('%Y%m%d%H%M%S')}"

class MockRiskService:
    """模拟风险管理服务"""
    
    def get_daily_bet_amount(self, user_id: str) -> Decimal:
        """获取日投注金额"""
        # 模拟返回随机金额
        return Decimal(str(random.randint(1000, 5000)))
        
    def check_daily_limit(self, user_id: str, limit: Decimal) -> bool:
        """检查日限额"""
        daily_amount = self.get_daily_bet_amount(user_id)
        return daily_amount < limit
        
    def detect_suspicious_activity(self, user_id: str, pattern: Dict) -> bool:
        """检测可疑活动"""
        # 基于模式判断
        if pattern.get('frequency', 0) > 50:  # 每小时超过50次
            return True
        if pattern.get('win_rate', 0) > 0.8:  # 胜率超过80%
            return True
        return False
        
    def check_account_security(self, user_id: str, account_info: Dict) -> bool:
        """检查账户安全"""
        if account_info.get('login_ip_changes', 0) > 5:
            return False
        if account_info.get('failed_logins', 0) > 10:
            return False
        return True
        
    def detect_martingale_pattern(self, betting_history: List[Dict]) -> bool:
        """检测倍投模式"""
        if len(betting_history) < 3:
            return False
            
        # 检查是否存在连续翻倍
        for i in range(1, len(betting_history)):
            current_amount = betting_history[i]['amount']
            prev_amount = betting_history[i-1]['amount']
            if current_amount >= prev_amount * 2:
                return True
        return False
        
    def check_period_limit(self, period: str, total_bets: Decimal, limit: Decimal) -> bool:
        """检查期次限制"""
        return total_bets < limit
        
    def handle_risk_event(self, risk_event: Dict) -> Dict:
        """处理风险事件"""
        severity = risk_event.get('severity', 'low')
        if severity == 'high':
            return {'action': 'freeze_account'}
        elif severity == 'medium':
            return {'action': 'limit_betting'}
        else:
            return {'action': 'manual_review'}

class MockDataService:
    """模拟数据服务"""
    
    def validate_data(self, data: Dict) -> bool:
        """验证数据"""
        if not data.get('user_id'):
            return False
        if data.get('bet_amount', 0) <= 0:
            return False
        return True
        
    def transform_data(self, raw_data: Dict) -> Dict:
        """转换数据"""
        transformed = {}
        for key, value in raw_data.items():
            if key == 'amount':
                transformed[key] = float(value)
            elif key == 'timestamp':
                transformed[key] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            elif key == 'status':
                transformed[key] = bool(int(value))
            else:
                transformed[key] = value
        return transformed
        
    def aggregate_by_user(self, betting_data: List[Dict]) -> Dict:
        """按用户聚合数据"""
        result = {}
        for bet in betting_data:
            user_id = bet['user_id']
            if user_id not in result:
                result[user_id] = {'total_amount': 0, 'bet_count': 0}
            result[user_id]['total_amount'] += bet['amount']
            result[user_id]['bet_count'] += 1
        return result
        
    def clean_data(self, data: List[Dict]) -> List[Dict]:
        """清洗数据"""
        cleaned = []
        for item in data:
            if item.get('amount') and item['amount'] > 0:
                cleaned.append(item)
        return cleaned
        
    def export_to_csv(self, data: List[Dict]) -> bool:
        """导出CSV"""
        return len(data) > 0
        
    def export_to_json(self, data: List[Dict]) -> bool:
        """导出JSON"""
        return len(data) > 0
        
    def process_real_time(self, stream_data: Dict) -> Dict:
        """实时处理"""
        processed = stream_data.copy()
        processed['processed_at'] = datetime.now()
        return processed

class MockBalanceService:
    """模拟余额服务"""
    
    def get_balance(self, user_id: str) -> Decimal:
        """获取余额"""
        return Decimal('1000.00')  # 模拟余额
        
    def add_balance(self, user_id: str, amount: Decimal) -> bool:
        """增加余额"""
        return amount > 0

class MockUserService:
    """模拟用户服务"""
    
    def get_user_info(self, user_id: str) -> Dict:
        """获取用户信息"""
        return {
            'user_id': user_id,
            'status': 'active',
            'level': 'normal'
        }

class MockETLService:
    """模拟ETL服务"""
    
    def extract_data(self, source: str) -> List[Dict]:
        """提取数据"""
        return [{'id': 1, 'data': 'sample'}]
        
    def transform_data(self, data: List[Dict]) -> List[Dict]:
        """转换数据"""
        return data
        
    def load_data(self, data: List[Dict], target: str) -> bool:
        """加载数据"""
        return len(data) > 0
