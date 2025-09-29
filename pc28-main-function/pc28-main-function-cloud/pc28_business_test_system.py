#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28业务测试系统
整合数据逻辑-清洗-分桶-下单的完整业务流程测试
基于现有的优化后系统，实现端到端的业务验证
"""

import os
import sys
import json
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

# 导入核心模块
from enhanced_voting import WeightedVoting, decide
from risk_management import kelly_fraction, stake_units
from adaptive_pi_controller import PIController

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BusinessTestResult:
    """业务测试结果"""
    test_name: str
    status: str  # pass, fail, skip
    message: str
    execution_time: float
    data_details: Optional[Dict[str, Any]] = None
    error_details: Optional[str] = None

@dataclass
class SignalPoolData:
    """信号池数据结构"""
    draw_id: str
    market: str
    pick: str
    p_win: float
    source: str
    vote_ratio: float
    timestamp: datetime

@dataclass
class DecisionCandidate:
    """决策候选数据"""
    id: str
    draw_id: str
    market: str
    pick: str
    p_win: float
    ev: float
    kelly_frac: float
    source: str
    created_at: datetime

@dataclass
class OrderRecord:
    """下单记录"""
    order_id: str
    draw_id: str
    market: str
    pick: str
    p_win: float
    stake_amount: float
    kelly_fraction: float
    expected_value: float
    status: str
    created_at: datetime

class PC28BusinessTestSystem:
    """PC28业务测试系统"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.test_results: List[BusinessTestResult] = []
        self.db_path = "pc28_business_test.db"
        self._init_test_database()
        
        # 初始化核心组件
        self.voting_system = WeightedVoting(self.config)
        self.pi_controller = PIController(self.config.get('controller', {}))
        
        logger.info("PC28业务测试系统初始化完成")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """加载配置"""
        default_config = {
            "voting": {
                "weights_init": {"cloud": 0.4, "map": 0.3, "size": 0.3},
                "weight_floor": 0.1,
                "weight_ceiling": 0.7,
                "weight_eta": 0.02,
                "accept_floor": 0.55,
                "extreme_gate": {
                    "enable": True,
                    "hi": 0.75,
                    "lo": 0.25
                },
                "buckets": ["0.50", "0.67", "1.00"]
            },
            "risk": {
                "kelly_cap": 0.05,
                "unit_size": 100,
                "bankroll": 10000,
                "max_stake_ratio": 0.1
            },
            "controller": {
                "targets": {"cov": 0.50, "acc": 0.80},
                "conservative": {"k_cov": 0.10, "k_acc_up": 0.10, "k_acc_dn": 0.30},
                "balanced": {"k_cov": 0.20, "k_acc_up": 0.15, "k_acc_dn": 0.35},
                "aggressive": {"k_cov": 0.35, "k_acc_up": 0.20, "k_acc_dn": 0.40}
            },
            "markets": ["oe", "size", "pc28"],
            "test_scenarios": {
                "signal_pool_size": 100,
                "decision_threshold": 0.55,
                "max_orders_per_draw": 3
            }
        }
        
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            except Exception as e:
                logger.warning(f"配置文件加载失败，使用默认配置: {e}")
        
        return default_config
    
    def _init_test_database(self):
        """初始化测试数据库"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 信号池表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signal_pool (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    pick TEXT NOT NULL,
                    p_win REAL NOT NULL,
                    source TEXT NOT NULL,
                    vote_ratio REAL DEFAULT 1.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 决策候选表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS decision_candidates (
                    id TEXT PRIMARY KEY,
                    draw_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    pick TEXT NOT NULL,
                    p_win REAL NOT NULL,
                    ev REAL NOT NULL,
                    kelly_frac REAL NOT NULL,
                    source TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 订单记录表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_records (
                    order_id TEXT PRIMARY KEY,
                    draw_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    pick TEXT NOT NULL,
                    p_win REAL NOT NULL,
                    stake_amount REAL NOT NULL,
                    kelly_fraction REAL NOT NULL,
                    expected_value REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 测试结果表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    execution_time REAL,
                    data_details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            logger.info("测试数据库初始化完成")
    
    def generate_mock_signal_pool(self, count: int = 50) -> List[SignalPoolData]:
        """生成模拟信号池数据"""
        import random
        
        signals = []
        markets = self.config["markets"]
        sources = ["cloud", "map", "size"]
        picks = {
            "oe": ["odd", "even"],
            "size": ["big", "small"],
            "pc28": ["0", "1", "2"]
        }
        
        for i in range(count):
            market = random.choice(markets)
            source = random.choice(sources)
            pick = random.choice(picks.get(market, ["option_a", "option_b"]))
            
            # 生成概率，不同源有不同的分布特征
            if source == "cloud":
                p_win = random.uniform(0.45, 0.75)
            elif source == "map":
                p_win = random.uniform(0.40, 0.70)
            else:  # size
                p_win = random.uniform(0.35, 0.65)
            
            signal = SignalPoolData(
                draw_id=f"draw_{i//3 + 1:04d}",
                market=market,
                pick=pick,
                p_win=round(p_win, 4),
                source=source,
                vote_ratio=random.uniform(0.8, 1.0),
                timestamp=datetime.now() - timedelta(minutes=random.randint(0, 60))
            )
            signals.append(signal)
        
        return signals
    
    def test_data_cleaning_logic(self) -> BusinessTestResult:
        """测试数据清洗逻辑"""
        start_time = time.time()
        test_name = "数据清洗逻辑测试"
        
        try:
            # 生成包含异常数据的信号池
            signals = self.generate_mock_signal_pool(30)
            
            # 添加一些异常数据
            signals.append(SignalPoolData(
                draw_id="draw_9999", market="oe", pick="odd",
                p_win=1.5, source="cloud", vote_ratio=1.0,  # 异常概率
                timestamp=datetime.now()
            ))
            signals.append(SignalPoolData(
                draw_id="draw_9998", market="size", pick="big",
                p_win=-0.1, source="map", vote_ratio=1.0,  # 负概率
                timestamp=datetime.now()
            ))
            
            # 数据清洗
            cleaned_signals = []
            for signal in signals:
                # 概率范围检查
                if not (0.0 <= signal.p_win <= 1.0):
                    logger.warning(f"异常概率值被过滤: {signal.p_win}")
                    continue
                
                # 市场类型检查
                if signal.market not in self.config["markets"]:
                    logger.warning(f"未知市场类型被过滤: {signal.market}")
                    continue
                
                # 时间窗口检查（只保留最近1小时的数据）
                if signal.timestamp < datetime.now() - timedelta(hours=1):
                    continue
                
                cleaned_signals.append(signal)
            
            # 存储清洗后的数据
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for signal in cleaned_signals:
                    cursor.execute("""
                        INSERT INTO signal_pool 
                        (draw_id, market, pick, p_win, source, vote_ratio, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        signal.draw_id, signal.market, signal.pick,
                        signal.p_win, signal.source, signal.vote_ratio,
                        signal.timestamp
                    ))
                conn.commit()
            
            return BusinessTestResult(
                test_name=test_name,
                status="pass",
                message=f"数据清洗完成，原始{len(signals)}条，清洗后{len(cleaned_signals)}条",
                execution_time=time.time() - start_time,
                data_details={
                    "original_count": len(signals),
                    "cleaned_count": len(cleaned_signals),
                    "filtered_count": len(signals) - len(cleaned_signals)
                }
            )
            
        except Exception as e:
            return BusinessTestResult(
                test_name=test_name,
                status="fail",
                message=f"数据清洗测试失败: {str(e)}",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def test_signal_bucketing_strategy(self) -> BusinessTestResult:
        """测试信号分桶策略"""
        start_time = time.time()
        test_name = "信号分桶策略测试"
        
        try:
            # 从数据库获取信号池数据
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT draw_id, market, pick, p_win, source, vote_ratio
                    FROM signal_pool
                    ORDER BY created_at DESC
                    LIMIT 50
                """)
                rows = cursor.fetchall()
            
            if not rows:
                return BusinessTestResult(
                    test_name=test_name,
                    status="skip",
                    message="无信号池数据，跳过分桶测试",
                    execution_time=time.time() - start_time
                )
            
            # 按draw_id和market分组
            signal_groups = {}
            for row in rows:
                draw_id, market, pick, p_win, source, vote_ratio = row
                key = f"{draw_id}_{market}"
                if key not in signal_groups:
                    signal_groups[key] = []
                signal_groups[key].append({
                    'draw_id': draw_id,
                    'market': market,
                    'pick': pick,
                    'p_win': p_win,
                    'source': source,
                    'vote_ratio': vote_ratio
                })
            
            # 对每组信号进行投票决策
            decision_candidates = []
            bucket_stats = {"0.50": 0, "0.67": 0, "1.00": 0}
            
            for group_key, signals in signal_groups.items():
                # 按源分组概率
                source_probs = {"cloud": 0.5, "map": 0.5, "size": 0.5}
                for signal in signals:
                    source_probs[signal['source']] = signal['p_win']
                
                # 使用投票决策
                decision = decide(
                    source_probs["cloud"],
                    source_probs["map"], 
                    source_probs["size"],
                    self.config,
                    {}
                )
                
                if decision["accept"]:
                    # 计算期望值和Kelly分数
                    p_win = decision["p_star"]
                    ev = 2.0 * p_win - 1.0
                    kelly_frac = kelly_fraction(p_win, self.config["risk"]["kelly_cap"])
                    
                    candidate = DecisionCandidate(
                        id=f"decision_{group_key}_{int(time.time())}",
                        draw_id=signals[0]['draw_id'],
                        market=signals[0]['market'],
                        pick=signals[0]['pick'],
                        p_win=p_win,
                        ev=ev,
                        kelly_frac=kelly_frac,
                        source="ensemble",
                        created_at=datetime.now()
                    )
                    decision_candidates.append(candidate)
                    bucket_stats[decision["bucket"]] += 1
            
            # 存储决策候选
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for candidate in decision_candidates:
                    cursor.execute("""
                        INSERT OR REPLACE INTO decision_candidates
                        (id, draw_id, market, pick, p_win, ev, kelly_frac, source, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        candidate.id, candidate.draw_id, candidate.market,
                        candidate.pick, candidate.p_win, candidate.ev,
                        candidate.kelly_frac, candidate.source, candidate.created_at
                    ))
                conn.commit()
            
            return BusinessTestResult(
                test_name=test_name,
                status="pass",
                message=f"分桶策略测试完成，生成{len(decision_candidates)}个决策候选",
                execution_time=time.time() - start_time,
                data_details={
                    "signal_groups": len(signal_groups),
                    "decision_candidates": len(decision_candidates),
                    "bucket_distribution": bucket_stats
                }
            )
            
        except Exception as e:
            return BusinessTestResult(
                test_name=test_name,
                status="fail",
                message=f"分桶策略测试失败: {str(e)}",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def test_order_placement_logic(self) -> BusinessTestResult:
        """测试下单逻辑"""
        start_time = time.time()
        test_name = "下单逻辑测试"
        
        try:
            # 从数据库获取决策候选
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, draw_id, market, pick, p_win, ev, kelly_frac, source
                    FROM decision_candidates
                    WHERE ev > 0
                    ORDER BY kelly_frac DESC
                    LIMIT 20
                """)
                rows = cursor.fetchall()
            
            if not rows:
                return BusinessTestResult(
                    test_name=test_name,
                    status="skip",
                    message="无决策候选数据，跳过下单测试",
                    execution_time=time.time() - start_time
                )
            
            orders = []
            total_stake = 0
            bankroll = self.config["risk"]["bankroll"]
            max_stake_ratio = self.config["risk"]["max_stake_ratio"]
            unit_size = self.config["risk"]["unit_size"]
            
            for row in rows:
                candidate_id, draw_id, market, pick, p_win, ev, kelly_frac, source = row
                
                # 计算下注金额
                stake_units_count = stake_units(
                    p_win, 
                    unit_size, 
                    self.config["risk"]["kelly_cap"]
                )
                stake_amount = stake_units_count
                
                # 风险控制检查
                if total_stake + stake_amount > bankroll * max_stake_ratio:
                    logger.info(f"达到最大下注比例限制，跳过订单: {candidate_id}")
                    continue
                
                if stake_amount <= 0:
                    continue
                
                # 创建订单
                order = OrderRecord(
                    order_id=f"order_{draw_id}_{market}_{int(time.time())}",
                    draw_id=draw_id,
                    market=market,
                    pick=pick,
                    p_win=p_win,
                    stake_amount=stake_amount,
                    kelly_fraction=kelly_frac,
                    expected_value=ev,
                    status="pending",
                    created_at=datetime.now()
                )
                orders.append(order)
                total_stake += stake_amount
                
                # 限制每个draw的最大订单数
                draw_orders = [o for o in orders if o.draw_id == draw_id]
                if len(draw_orders) >= self.config["test_scenarios"]["max_orders_per_draw"]:
                    continue
            
            # 存储订单
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for order in orders:
                    cursor.execute("""
                        INSERT INTO order_records
                        (order_id, draw_id, market, pick, p_win, stake_amount, 
                         kelly_fraction, expected_value, status, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        order.order_id, order.draw_id, order.market, order.pick,
                        order.p_win, order.stake_amount, order.kelly_fraction,
                        order.expected_value, order.status, order.created_at
                    ))
                conn.commit()
            
            return BusinessTestResult(
                test_name=test_name,
                status="pass",
                message=f"下单逻辑测试完成，生成{len(orders)}个订单，总金额{total_stake}",
                execution_time=time.time() - start_time,
                data_details={
                    "total_orders": len(orders),
                    "total_stake": total_stake,
                    "average_stake": total_stake / len(orders) if orders else 0,
                    "bankroll_usage": (total_stake / bankroll) * 100
                }
            )
            
        except Exception as e:
            return BusinessTestResult(
                test_name=test_name,
                status="fail",
                message=f"下单逻辑测试失败: {str(e)}",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def test_risk_management_controls(self) -> BusinessTestResult:
        """测试风险管理控制"""
        start_time = time.time()
        test_name = "风险管理控制测试"
        
        try:
            # 获取所有订单进行风险分析
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT order_id, draw_id, market, p_win, stake_amount, 
                           kelly_fraction, expected_value
                    FROM order_records
                    ORDER BY created_at DESC
                """)
                orders = cursor.fetchall()
            
            if not orders:
                return BusinessTestResult(
                    test_name=test_name,
                    status="skip",
                    message="无订单数据，跳过风险管理测试",
                    execution_time=time.time() - start_time
                )
            
            # 风险指标计算
            total_stake = sum(order[4] for order in orders)  # stake_amount
            total_kelly = sum(order[5] for order in orders)  # kelly_fraction
            total_ev = sum(order[6] for order in orders)     # expected_value
            
            bankroll = self.config["risk"]["bankroll"]
            kelly_cap = self.config["risk"]["kelly_cap"]
            max_stake_ratio = self.config["risk"]["max_stake_ratio"]
            
            # 风险检查
            risk_violations = []
            
            # 1. 总下注比例检查
            stake_ratio = total_stake / bankroll
            if stake_ratio > max_stake_ratio:
                risk_violations.append(f"总下注比例超限: {stake_ratio:.2%} > {max_stake_ratio:.2%}")
            
            # 2. Kelly分数检查
            for order in orders:
                if order[5] > kelly_cap:  # kelly_fraction
                    risk_violations.append(f"Kelly分数超限: {order[5]:.4f} > {kelly_cap}")
            
            # 3. 单一draw集中度检查
            draw_stakes = {}
            for order in orders:
                draw_id = order[1]
                stake = order[4]
                draw_stakes[draw_id] = draw_stakes.get(draw_id, 0) + stake
            
            max_draw_stake = max(draw_stakes.values()) if draw_stakes else 0
            max_draw_ratio = max_draw_stake / bankroll
            if max_draw_ratio > 0.05:  # 单一draw不超过5%
                risk_violations.append(f"单一draw集中度过高: {max_draw_ratio:.2%}")
            
            # 4. 期望值检查
            negative_ev_count = sum(1 for order in orders if order[6] < 0)
            if negative_ev_count > 0:
                risk_violations.append(f"存在负期望值订单: {negative_ev_count}个")
            
            status = "fail" if risk_violations else "pass"
            message = "风险管理检查通过" if not risk_violations else f"发现{len(risk_violations)}个风险问题"
            
            return BusinessTestResult(
                test_name=test_name,
                status=status,
                message=message,
                execution_time=time.time() - start_time,
                data_details={
                    "total_orders": len(orders),
                    "total_stake": total_stake,
                    "stake_ratio": stake_ratio,
                    "total_kelly": total_kelly,
                    "total_ev": total_ev,
                    "risk_violations": risk_violations,
                    "draw_concentration": {
                        "max_draw_stake": max_draw_stake,
                        "max_draw_ratio": max_draw_ratio,
                        "draw_count": len(draw_stakes)
                    }
                }
            )
            
        except Exception as e:
            return BusinessTestResult(
                test_name=test_name,
                status="fail",
                message=f"风险管理测试失败: {str(e)}",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def test_end_to_end_workflow(self) -> BusinessTestResult:
        """测试端到端工作流"""
        start_time = time.time()
        test_name = "端到端工作流测试"
        
        try:
            # 清理之前的测试数据
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM signal_pool")
                cursor.execute("DELETE FROM decision_candidates")
                cursor.execute("DELETE FROM order_records")
                conn.commit()
            
            # 1. 数据清洗
            cleaning_result = self.test_data_cleaning_logic()
            if cleaning_result.status != "pass":
                return BusinessTestResult(
                    test_name=test_name,
                    status="fail",
                    message=f"端到端测试失败于数据清洗阶段: {cleaning_result.message}",
                    execution_time=time.time() - start_time,
                    error_details=cleaning_result.error_details
                )
            
            # 2. 信号分桶
            bucketing_result = self.test_signal_bucketing_strategy()
            if bucketing_result.status != "pass":
                return BusinessTestResult(
                    test_name=test_name,
                    status="fail",
                    message=f"端到端测试失败于信号分桶阶段: {bucketing_result.message}",
                    execution_time=time.time() - start_time,
                    error_details=bucketing_result.error_details
                )
            
            # 3. 下单逻辑
            order_result = self.test_order_placement_logic()
            if order_result.status not in ["pass", "skip"]:
                return BusinessTestResult(
                    test_name=test_name,
                    status="fail",
                    message=f"端到端测试失败于下单阶段: {order_result.message}",
                    execution_time=time.time() - start_time,
                    error_details=order_result.error_details
                )
            
            # 4. 风险管理
            risk_result = self.test_risk_management_controls()
            if risk_result.status not in ["pass", "skip"]:
                return BusinessTestResult(
                    test_name=test_name,
                    status="fail",
                    message=f"端到端测试失败于风险管理阶段: {risk_result.message}",
                    execution_time=time.time() - start_time,
                    error_details=risk_result.error_details
                )
            
            # 汇总统计
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*) FROM signal_pool")
                signal_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM decision_candidates")
                candidate_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*), SUM(stake_amount) FROM order_records")
                order_stats = cursor.fetchone()
                order_count, total_stake = order_stats[0], order_stats[1] or 0
            
            return BusinessTestResult(
                test_name=test_name,
                status="pass",
                message=f"端到端工作流测试完成: {signal_count}信号→{candidate_count}候选→{order_count}订单",
                execution_time=time.time() - start_time,
                data_details={
                    "signal_pool_count": signal_count,
                    "decision_candidates_count": candidate_count,
                    "order_count": order_count,
                    "total_stake": total_stake,
                    "conversion_rates": {
                        "signal_to_candidate": (candidate_count / signal_count * 100) if signal_count > 0 else 0,
                        "candidate_to_order": (order_count / candidate_count * 100) if candidate_count > 0 else 0
                    }
                }
            )
            
        except Exception as e:
            return BusinessTestResult(
                test_name=test_name,
                status="fail",
                message=f"端到端工作流测试失败: {str(e)}",
                execution_time=time.time() - start_time,
                error_details=str(e)
            )
    
    def run_all_tests(self) -> Dict[str, Any]:
        """运行所有业务测试"""
        logger.info("开始运行PC28业务测试套件")
        start_time = time.time()
        
        # 定义测试用例
        test_cases = [
            self.test_data_cleaning_logic,
            self.test_signal_bucketing_strategy,
            self.test_order_placement_logic,
            self.test_risk_management_controls,
            self.test_end_to_end_workflow
        ]
        
        # 执行测试
        for test_case in test_cases:
            try:
                result = test_case()
                self.test_results.append(result)
                logger.info(f"✓ {result.test_name}: {result.status} - {result.message}")
            except Exception as e:
                error_result = BusinessTestResult(
                    test_name=test_case.__name__,
                    status="error",
                    message=f"测试执行异常: {str(e)}",
                    execution_time=0,
                    error_details=str(e)
                )
                self.test_results.append(error_result)
                logger.error(f"✗ {test_case.__name__}: 执行异常 - {str(e)}")
        
        # 生成测试报告
        total_time = time.time() - start_time
        passed = len([r for r in self.test_results if r.status == "pass"])
        failed = len([r for r in self.test_results if r.status == "fail"])
        skipped = len([r for r in self.test_results if r.status == "skip"])
        errors = len([r for r in self.test_results if r.status == "error"])
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_tests": len(self.test_results),
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "errors": errors,
            "success_rate": (passed / len(self.test_results) * 100) if self.test_results else 0,
            "total_execution_time": total_time,
            "test_results": [asdict(result) for result in self.test_results]
        }
        
        # 保存测试结果到数据库
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for result in self.test_results:
                cursor.execute("""
                    INSERT INTO test_results 
                    (test_name, status, message, execution_time, data_details)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    result.test_name, result.status, result.message,
                    result.execution_time, json.dumps(result.data_details)
                ))
            conn.commit()
        
        return report
    
    def generate_test_report(self, report: Dict[str, Any]) -> str:
        """生成测试报告"""
        report_lines = [
            "# PC28业务测试系统报告",
            f"**测试时间**: {report['timestamp']}",
            f"**总测试数**: {report['total_tests']}",
            f"**通过**: {report['passed']}",
            f"**失败**: {report['failed']}",
            f"**跳过**: {report['skipped']}",
            f"**错误**: {report['errors']}",
            f"**成功率**: {report['success_rate']:.2f}%",
            f"**总耗时**: {report['total_execution_time']:.2f}秒",
            "",
            "## 详细测试结果"
        ]
        
        for result in self.test_results:
            status_icon = {
                "pass": "✅",
                "fail": "❌", 
                "skip": "⏭️",
                "error": "💥"
            }.get(result.status, "❓")
            
            report_lines.extend([
                f"### {status_icon} {result.test_name}",
                f"**状态**: {result.status}",
                f"**消息**: {result.message}",
                f"**耗时**: {result.execution_time:.2f}秒"
            ])
            
            if result.data_details:
                report_lines.append("**数据详情**:")
                for key, value in result.data_details.items():
                    report_lines.append(f"- {key}: {value}")
            
            if result.error_details:
                report_lines.extend([
                    "**错误详情**:",
                    f"```\n{result.error_details}\n```"
                ])
            
            report_lines.append("")
        
        return "\n".join(report_lines)


def main():
    """主函数"""
    print("PC28业务测试系统启动")
    
    # 初始化测试系统
    test_system = PC28BusinessTestSystem()
    
    # 运行所有测试
    report = test_system.run_all_tests()
    
    # 生成并保存报告
    report_content = test_system.generate_test_report(report)
    report_filename = f"pc28_business_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"\n测试完成！报告已保存到: {report_filename}")
    print(f"成功率: {report['success_rate']:.2f}%")
    print(f"通过: {report['passed']}, 失败: {report['failed']}, 跳过: {report['skipped']}, 错误: {report['errors']}")
    
    return report


if __name__ == "__main__":
    main()