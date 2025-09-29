#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
预测准确率系统
Prediction Accuracy System

功能：
1. 云端财务系统简化计分（不关注盈利，只关注预测效果）
2. 预测准确率统计和分析
3. 数据流转正常性监控
4. 预测模型效果评估
5. 实时预测结果跟踪
"""

import os
import logging
import json
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import statistics

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prediction_accuracy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PredictionConfig:
    """预测配置"""
    database_path: str = "production_data/production.db"
    accuracy_threshold: float = 0.6
    evaluation_periods: int = 100
    score_weight_accuracy: float = 0.7
    score_weight_consistency: float = 0.3

@dataclass
class PredictionResult:
    """预测结果"""
    period: str
    prediction_time: str
    predicted_value: float
    actual_result: Optional[str] = None
    is_correct: Optional[bool] = None
    confidence_score: float = 0.0
    model_version: str = "p_ensemble_today_norm_v5"

@dataclass
class AccuracyMetrics:
    """准确率指标"""
    total_predictions: int
    correct_predictions: int
    accuracy_rate: float
    average_confidence: float
    consistency_score: float
    evaluation_period: str
    model_performance: Dict[str, float]

class PredictionAccuracySystem:
    """预测准确率系统"""
    
    def __init__(self, config: PredictionConfig = None):
        self.config = config or PredictionConfig()
        
        # 初始化系统
        self._init_accuracy_system()
    
    def _init_accuracy_system(self):
        """初始化准确率系统"""
        try:
            logger.info("初始化预测准确率系统...")
            
            # 创建生产数据目录
            os.makedirs("production_data", exist_ok=True)
            
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()
            
            # 创建预测记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prediction_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period TEXT UNIQUE NOT NULL,
                    prediction_time TEXT NOT NULL,
                    predicted_value REAL NOT NULL,
                    predicted_result TEXT,  -- BIG/SMALL, ODD/EVEN等
                    confidence_score REAL DEFAULT 0.0,
                    model_version TEXT DEFAULT 'p_ensemble_today_norm_v5',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建实际结果表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS actual_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period TEXT UNIQUE NOT NULL,
                    draw_time TEXT NOT NULL,
                    numbers TEXT NOT NULL,
                    sum_value INTEGER,
                    big_small TEXT,
                    odd_even TEXT,
                    actual_result TEXT,  -- 实际结果标准化
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建准确率评估表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accuracy_evaluations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    evaluation_period TEXT NOT NULL,
                    total_predictions INTEGER NOT NULL,
                    correct_predictions INTEGER NOT NULL,
                    accuracy_rate REAL NOT NULL,
                    average_confidence REAL NOT NULL,
                    consistency_score REAL NOT NULL,
                    model_performance TEXT,  -- JSON格式
                    evaluation_time TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建财务计分表（简化版）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS simple_scoring (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period TEXT NOT NULL,
                    prediction_correct BOOLEAN,
                    base_score INTEGER DEFAULT 1,  -- 基础分数
                    confidence_bonus REAL DEFAULT 0.0,  -- 置信度奖励
                    total_score REAL,
                    cumulative_score REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建数据流转监控表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS flow_monitoring (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    component TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_update TEXT NOT NULL,
                    data_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    check_time TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
            logger.info("预测准确率系统初始化完成")
            
        except Exception as e:
            logger.error(f"初始化预测准确率系统失败: {e}")
            raise
    
    def record_prediction(self, period: str, predicted_value: float, confidence_score: float = 0.0) -> bool:
        """记录预测结果"""
        try:
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()
            
            # 根据预测值确定预测结果
            predicted_result = "BIG" if predicted_value > 0.5 else "SMALL"
            
            cursor.execute('''
                INSERT OR REPLACE INTO prediction_records 
                (period, prediction_time, predicted_value, predicted_result, confidence_score, model_version)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                period,
                datetime.now().isoformat(),
                predicted_value,
                predicted_result,
                confidence_score,
                "p_ensemble_today_norm_v5"
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"记录预测: 期号{period}, 预测值{predicted_value:.3f}, 结果{predicted_result}")
            return True
            
        except Exception as e:
            logger.error(f"记录预测失败: {e}")
            return False
    
    def record_actual_result(self, period: str, numbers: str, sum_value: int) -> bool:
        """记录实际开奖结果"""
        try:
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()
            
            # 计算实际结果
            big_small = "BIG" if sum_value >= 14 else "SMALL"
            odd_even = "ODD" if sum_value % 2 == 1 else "EVEN"
            actual_result = big_small  # 主要关注大小
            
            cursor.execute('''
                INSERT OR REPLACE INTO actual_results 
                (period, draw_time, numbers, sum_value, big_small, odd_even, actual_result)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                period,
                datetime.now().isoformat(),
                numbers,
                sum_value,
                big_small,
                odd_even,
                actual_result
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"记录实际结果: 期号{period}, 号码{numbers}, 和值{sum_value}, 结果{actual_result}")
            return True
            
        except Exception as e:
            logger.error(f"记录实际结果失败: {e}")
            return False
    
    def calculate_accuracy(self, periods: int = None) -> AccuracyMetrics:
        """计算预测准确率"""
        try:
            if periods is None:
                periods = self.config.evaluation_periods
            
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()
            
            # 获取有预测和实际结果的记录
            cursor.execute('''
                SELECT p.period, p.predicted_result, p.confidence_score, a.actual_result
                FROM prediction_records p
                JOIN actual_results a ON p.period = a.period
                ORDER BY p.prediction_time DESC
                LIMIT ?
            ''', (periods,))
            
            records = cursor.fetchall()
            
            if not records:
                logger.warning("没有找到匹配的预测和实际结果记录")
                return AccuracyMetrics(
                    total_predictions=0,
                    correct_predictions=0,
                    accuracy_rate=0.0,
                    average_confidence=0.0,
                    consistency_score=0.0,
                    evaluation_period=f"最近{periods}期",
                    model_performance={}
                )
            
            # 计算准确率
            total_predictions = len(records)
            correct_predictions = sum(1 for record in records if record[1] == record[3])
            accuracy_rate = correct_predictions / total_predictions if total_predictions > 0 else 0.0
            
            # 计算平均置信度
            confidence_scores = [record[2] for record in records if record[2] > 0]
            average_confidence = statistics.mean(confidence_scores) if confidence_scores else 0.0
            
            # 计算一致性分数（预测结果的稳定性）
            predicted_results = [record[1] for record in records]
            big_count = predicted_results.count("BIG")
            small_count = predicted_results.count("SMALL")
            consistency_score = abs(big_count - small_count) / total_predictions if total_predictions > 0 else 0.0
            consistency_score = 1.0 - consistency_score  # 转换为一致性分数
            
            # 模型性能分析
            model_performance = {
                "准确率": accuracy_rate,
                "置信度": average_confidence,
                "一致性": consistency_score,
                "BIG预测占比": big_count / total_predictions if total_predictions > 0 else 0.0,
                "SMALL预测占比": small_count / total_predictions if total_predictions > 0 else 0.0
            }
            
            # 保存评估结果
            cursor.execute('''
                INSERT INTO accuracy_evaluations 
                (evaluation_period, total_predictions, correct_predictions, accuracy_rate, 
                 average_confidence, consistency_score, model_performance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                f"最近{periods}期",
                total_predictions,
                correct_predictions,
                accuracy_rate,
                average_confidence,
                consistency_score,
                json.dumps(model_performance, ensure_ascii=False)
            ))
            
            conn.commit()
            conn.close()
            
            metrics = AccuracyMetrics(
                total_predictions=total_predictions,
                correct_predictions=correct_predictions,
                accuracy_rate=accuracy_rate,
                average_confidence=average_confidence,
                consistency_score=consistency_score,
                evaluation_period=f"最近{periods}期",
                model_performance=model_performance
            )
            
            logger.info(f"准确率计算完成: {accuracy_rate:.1%} ({correct_predictions}/{total_predictions})")
            return metrics
            
        except Exception as e:
            logger.error(f"计算准确率失败: {e}")
            return AccuracyMetrics(
                total_predictions=0,
                correct_predictions=0,
                accuracy_rate=0.0,
                average_confidence=0.0,
                consistency_score=0.0,
                evaluation_period=f"最近{periods}期",
                model_performance={}
            )
    
    def simple_scoring(self, period: str) -> float:
        """简化财务计分"""
        try:
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()
            
            # 获取预测和实际结果
            cursor.execute('''
                SELECT p.predicted_result, p.confidence_score, a.actual_result
                FROM prediction_records p
                JOIN actual_results a ON p.period = a.period
                WHERE p.period = ?
            ''', (period,))
            
            result = cursor.fetchone()
            
            if not result:
                logger.warning(f"期号{period}没有找到匹配的预测和实际结果")
                return 0.0
            
            predicted_result, confidence_score, actual_result = result
            
            # 基础计分
            is_correct = predicted_result == actual_result
            base_score = 1 if is_correct else 0
            
            # 置信度奖励
            confidence_bonus = confidence_score * 0.5 if is_correct else 0.0
            
            # 总分
            total_score = base_score + confidence_bonus
            
            # 获取累计分数
            cursor.execute('''
                SELECT COALESCE(SUM(total_score), 0) 
                FROM simple_scoring 
                WHERE period < ?
            ''', (period,))
            
            previous_cumulative = cursor.fetchone()[0]
            cumulative_score = previous_cumulative + total_score
            
            # 保存计分结果
            cursor.execute('''
                INSERT OR REPLACE INTO simple_scoring 
                (period, prediction_correct, base_score, confidence_bonus, total_score, cumulative_score)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                period,
                is_correct,
                base_score,
                confidence_bonus,
                total_score,
                cumulative_score
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"期号{period}计分: {'正确' if is_correct else '错误'}, 得分{total_score:.2f}, 累计{cumulative_score:.2f}")
            return total_score
            
        except Exception as e:
            logger.error(f"简化计分失败: {e}")
            return 0.0
    
    def monitor_data_flow(self) -> Dict[str, str]:
        """监控数据流转"""
        try:
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()
            
            flow_status = {}
            
            # 检查预测记录
            cursor.execute('SELECT COUNT(*) FROM prediction_records WHERE date(prediction_time) = date("now")')
            today_predictions = cursor.fetchone()[0]
            
            cursor.execute('SELECT MAX(prediction_time) FROM prediction_records')
            last_prediction_time = cursor.fetchone()[0]
            
            prediction_status = "正常" if today_predictions > 0 else "异常"
            flow_status["预测记录"] = prediction_status
            
            # 检查实际结果
            cursor.execute('SELECT COUNT(*) FROM actual_results WHERE date(draw_time) = date("now")')
            today_results = cursor.fetchone()[0]
            
            cursor.execute('SELECT MAX(draw_time) FROM actual_results')
            last_result_time = cursor.fetchone()[0]
            
            result_status = "正常" if today_results > 0 else "异常"
            flow_status["实际结果"] = result_status
            
            # 检查准确率评估
            cursor.execute('SELECT COUNT(*) FROM accuracy_evaluations WHERE date(evaluation_time) = date("now")')
            today_evaluations = cursor.fetchone()[0]
            
            evaluation_status = "正常" if today_evaluations > 0 else "异常"
            flow_status["准确率评估"] = evaluation_status
            
            # 检查计分系统
            cursor.execute('SELECT COUNT(*) FROM simple_scoring WHERE date(created_at) = date("now")')
            today_scoring = cursor.fetchone()[0]
            
            scoring_status = "正常" if today_scoring > 0 else "异常"
            flow_status["计分系统"] = scoring_status
            
            # 更新监控记录
            for component, status in flow_status.items():
                cursor.execute('''
                    INSERT OR REPLACE INTO flow_monitoring 
                    (component, status, last_update, data_count)
                    VALUES (?, ?, ?, ?)
                ''', (
                    component,
                    status,
                    datetime.now().isoformat(),
                    {
                        "预测记录": today_predictions,
                        "实际结果": today_results,
                        "准确率评估": today_evaluations,
                        "计分系统": today_scoring
                    }.get(component, 0)
                ))
            
            conn.commit()
            conn.close()
            
            logger.info("数据流转监控完成")
            for component, status in flow_status.items():
                logger.info(f"  {component}: {status}")
            
            return flow_status
            
        except Exception as e:
            logger.error(f"数据流转监控失败: {e}")
            return {"错误": str(e)}
    
    def generate_comprehensive_report(self) -> Dict:
        """生成综合报告"""
        try:
            logger.info("生成预测准确率综合报告...")
            
            # 计算准确率指标
            accuracy_metrics = self.calculate_accuracy()
            
            # 监控数据流转
            flow_status = self.monitor_data_flow()
            
            # 获取最新计分情况
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT period, prediction_correct, total_score, cumulative_score
                FROM simple_scoring 
                ORDER BY created_at DESC 
                LIMIT 10
            ''')
            
            recent_scores = cursor.fetchall()
            
            # 获取系统统计
            cursor.execute('SELECT COUNT(*) FROM prediction_records')
            total_predictions = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM actual_results')
            total_results = cursor.fetchone()[0]
            
            cursor.execute('SELECT AVG(total_score) FROM simple_scoring')
            average_score = cursor.fetchone()[0] or 0.0
            
            conn.close()
            
            # 生成报告
            report = {
                "报告生成时间": datetime.now().isoformat(),
                "系统概况": {
                    "总预测数": total_predictions,
                    "总结果数": total_results,
                    "平均得分": f"{average_score:.2f}",
                    "数据匹配率": f"{min(total_predictions, total_results) / max(total_predictions, total_results) * 100:.1f}%" if max(total_predictions, total_results) > 0 else "0%"
                },
                "准确率分析": {
                    "评估期间": accuracy_metrics.evaluation_period,
                    "总预测数": accuracy_metrics.total_predictions,
                    "正确预测": accuracy_metrics.correct_predictions,
                    "准确率": f"{accuracy_metrics.accuracy_rate:.1%}",
                    "平均置信度": f"{accuracy_metrics.average_confidence:.3f}",
                    "一致性分数": f"{accuracy_metrics.consistency_score:.3f}",
                    "模型性能": accuracy_metrics.model_performance
                },
                "数据流转状态": flow_status,
                "最近计分": [
                    {
                        "期号": score[0],
                        "预测正确": "是" if score[1] else "否",
                        "本期得分": f"{score[2]:.2f}",
                        "累计得分": f"{score[3]:.2f}"
                    }
                    for score in recent_scores
                ],
                "系统建议": [
                    f"预测准确率{'达标' if accuracy_metrics.accuracy_rate >= self.config.accuracy_threshold else '需要提升'}",
                    f"数据流转{'正常' if all(status == '正常' for status in flow_status.values()) else '需要关注'}",
                    f"模型一致性{'良好' if accuracy_metrics.consistency_score > 0.7 else '需要优化'}",
                    "继续关注预测效果，不必关注具体盈利情况",
                    "保持数据流转正常，确保预测和结果及时更新"
                ]
            }
            
            return report
            
        except Exception as e:
            logger.error(f"生成综合报告失败: {e}")
            return {"错误": str(e)}

def main():
    """主函数"""
    logger.info("启动预测准确率系统")
    
    # 创建系统配置
    config = PredictionConfig(
        database_path="production_data/production.db",
        accuracy_threshold=0.6,
        evaluation_periods=50,
        score_weight_accuracy=0.7,
        score_weight_consistency=0.3
    )
    
    # 创建预测准确率系统
    accuracy_system = PredictionAccuracySystem(config)
    
    # 模拟一些测试数据
    logger.info("生成测试数据...")
    
    # 模拟预测记录
    test_periods = []
    for i in range(20):
        period = f"TEST_{datetime.now().strftime('%Y%m%d')}_{i:03d}"
        test_periods.append(period)
        
        # 模拟预测值（随机但有一定准确性）
        import random
        predicted_value = random.uniform(0.3, 0.8)
        confidence_score = random.uniform(0.5, 0.9)
        
        accuracy_system.record_prediction(period, predicted_value, confidence_score)
        
        # 模拟实际结果
        numbers = f"{random.randint(0,9)},{random.randint(0,9)},{random.randint(0,9)}"
        sum_value = sum(int(n) for n in numbers.split(','))
        
        accuracy_system.record_actual_result(period, numbers, sum_value)
        
        # 计分
        accuracy_system.simple_scoring(period)
    
    # 生成综合报告
    report = accuracy_system.generate_comprehensive_report()
    
    # 输出报告
    logger.info("预测准确率系统报告:")
    for key, value in report.items():
        if isinstance(value, dict):
            logger.info(f"  {key}:")
            for sub_key, sub_value in value.items():
                logger.info(f"    {sub_key}: {sub_value}")
        elif isinstance(value, list):
            logger.info(f"  {key}:")
            for item in value:
                if isinstance(item, dict):
                    logger.info(f"    - 期号{item.get('期号', '')}: {item.get('预测正确', '')}, 得分{item.get('本期得分', '')}")
                else:
                    logger.info(f"    - {item}")
        else:
            logger.info(f"  {key}: {value}")
    
    # 保存报告
    with open('prediction_accuracy_report.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info("预测准确率系统运行完成")
    logger.info("关注重点：预测准确率和数据流转正常性")
    logger.info("财务计分已简化，不关注具体盈利情况")
    
    return True

if __name__ == "__main__":
    main()