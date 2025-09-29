#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
和值模式检测器
专门检测开奖结果和值的非随机模式，防止AI模拟数据污染
"""

import sqlite3
import statistics
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from collections import Counter, defaultdict
import json
import hashlib
import math

@dataclass
class SumPattern:
    """和值模式"""
    pattern_type: str
    description: str
    confidence: float  # 0-1
    evidence: Dict[str, Any]
    risk_level: str  # low, medium, high, critical

@dataclass
class SumAnalysis:
    """和值分析结果"""
    record_id: str
    sum_value: int
    expected_range: Tuple[int, int]
    deviation_score: float
    patterns: List[SumPattern]
    is_suspicious: bool
    recommendation: str

@dataclass
class HistoricalSumData:
    """历史和值数据"""
    sums: List[int]
    frequencies: Dict[int, int]
    mean: float
    std_dev: float
    distribution: Dict[str, float]

class SumPatternDetector:
    """和值模式检测器"""
    
    def __init__(self, db_path: str = "sum_pattern_analysis.db"):
        self.db_path = db_path
        self.init_database()
        
        # PC28游戏的理论和值范围（通常是0-27）
        self.theoretical_min_sum = 0
        self.theoretical_max_sum = 27
        self.theoretical_mean = 13.5
        
        # 可疑模式阈值
        self.suspicious_thresholds = {
            'deviation_z_score': 3.0,  # Z分数超过3认为异常
            'frequency_anomaly': 0.05,  # 频率异常阈值
            'consecutive_pattern': 5,   # 连续模式长度
            'arithmetic_sequence': 3,   # 等差数列长度
            'geometric_sequence': 3,    # 等比数列长度
            'sum_clustering': 0.8,      # 和值聚集度
            'periodicity_strength': 0.7 # 周期性强度
        }
    
    def init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 创建和值分析表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sum_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id TEXT NOT NULL,
                    sum_value INTEGER NOT NULL,
                    deviation_score REAL NOT NULL,
                    is_suspicious BOOLEAN NOT NULL,
                    patterns TEXT NOT NULL,
                    recommendation TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            # 创建历史和值表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historical_sums (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id TEXT UNIQUE NOT NULL,
                    sum_value INTEGER NOT NULL,
                    numbers TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            # 创建模式检测结果表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_detections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_type TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    risk_level TEXT NOT NULL,
                    evidence TEXT NOT NULL,
                    affected_records TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            conn.commit()
    
    def calculate_sum(self, numbers: List[int]) -> int:
        """计算开奖号码和值"""
        return sum(numbers) % 28 if numbers else 0
    
    def get_historical_sum_data(self, limit: int = 1000) -> HistoricalSumData:
        """获取历史和值数据"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT sum_value FROM historical_sums 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (limit,))
            
            sums = [row[0] for row in cursor.fetchall()]
        
        if not sums:
            # 如果没有历史数据，使用理论分布
            sums = list(range(28)) * 10  # 模拟均匀分布
        
        frequencies = Counter(sums)
        mean_val = statistics.mean(sums)
        std_dev = statistics.stdev(sums) if len(sums) > 1 else 0
        
        # 计算分布
        total = len(sums)
        distribution = {str(k): v/total for k, v in frequencies.items()}
        
        return HistoricalSumData(
            sums=sums,
            frequencies=frequencies,
            mean=mean_val,
            std_dev=std_dev,
            distribution=distribution
        )
    
    def detect_deviation_pattern(self, sum_value: int, historical_data: HistoricalSumData) -> Optional[SumPattern]:
        """检测偏差模式"""
        if historical_data.std_dev == 0:
            return None
        
        # 计算Z分数
        z_score = abs(sum_value - historical_data.mean) / historical_data.std_dev
        
        if z_score > self.suspicious_thresholds['deviation_z_score']:
            return SumPattern(
                pattern_type="extreme_deviation",
                description=f"和值 {sum_value} 严重偏离历史均值 {historical_data.mean:.2f}",
                confidence=min(1.0, z_score / 5.0),
                evidence={
                    "z_score": z_score,
                    "historical_mean": historical_data.mean,
                    "historical_std": historical_data.std_dev
                },
                risk_level="critical" if z_score > 4 else "high"
            )
        return None
    
    def detect_frequency_anomaly(self, sum_value: int, historical_data: HistoricalSumData) -> Optional[SumPattern]:
        """检测频率异常"""
        expected_freq = len(historical_data.sums) / 28  # 理论频率
        actual_freq = historical_data.frequencies.get(sum_value, 0)
        
        # 计算频率偏差
        if expected_freq > 0:
            freq_ratio = actual_freq / expected_freq
            
            # 检查是否过于频繁或过于稀少
            if freq_ratio > 3.0 or freq_ratio < 0.3:
                # 安全计算置信度，避免log(0)错误
                if freq_ratio > 0:
                    confidence = min(1.0, abs(math.log(freq_ratio)) / 2)
                else:
                    confidence = 1.0  # 频率为0时置信度最高
                
                return SumPattern(
                    pattern_type="frequency_anomaly",
                    description=f"和值 {sum_value} 出现频率异常: 实际 {actual_freq}, 期望 {expected_freq:.1f}",
                    confidence=confidence,
                    evidence={
                        "actual_frequency": actual_freq,
                        "expected_frequency": expected_freq,
                        "frequency_ratio": freq_ratio
                    },
                    risk_level="high" if freq_ratio > 5 or freq_ratio < 0.2 else "medium"
                )
        return None
    
    def detect_consecutive_pattern(self, recent_sums: List[int]) -> Optional[SumPattern]:
        """检测连续模式"""
        if len(recent_sums) < self.suspicious_thresholds['consecutive_pattern']:
            return None
        
        # 检查连续相同值
        consecutive_count = 1
        max_consecutive = 1
        current_value = recent_sums[0]
        
        for i in range(1, len(recent_sums)):
            if recent_sums[i] == current_value:
                consecutive_count += 1
                max_consecutive = max(max_consecutive, consecutive_count)
            else:
                consecutive_count = 1
                current_value = recent_sums[i]
        
        if max_consecutive >= self.suspicious_thresholds['consecutive_pattern']:
            return SumPattern(
                pattern_type="consecutive_same_sum",
                description=f"检测到连续 {max_consecutive} 次相同和值 {current_value}",
                confidence=min(1.0, max_consecutive / 10),
                evidence={
                    "consecutive_count": max_consecutive,
                    "repeated_value": current_value,
                    "sequence": recent_sums[-max_consecutive:]
                },
                risk_level="critical"
            )
        return None
    
    def detect_arithmetic_sequence(self, recent_sums: List[int]) -> Optional[SumPattern]:
        """检测等差数列"""
        if len(recent_sums) < self.suspicious_thresholds['arithmetic_sequence']:
            return None
        
        # 检查等差数列
        for start in range(len(recent_sums) - 2):
            for length in range(3, min(8, len(recent_sums) - start + 1)):
                sequence = recent_sums[start:start + length]
                
                # 检查是否为等差数列
                if len(sequence) >= 3:
                    differences = [sequence[i+1] - sequence[i] for i in range(len(sequence)-1)]
                    if len(set(differences)) == 1 and differences[0] != 0:
                        return SumPattern(
                            pattern_type="arithmetic_sequence",
                            description=f"检测到等差数列: {sequence}, 公差: {differences[0]}",
                            confidence=min(1.0, length / 8),
                            evidence={
                                "sequence": sequence,
                                "common_difference": differences[0],
                                "length": length
                            },
                            risk_level="high"
                        )
        return None
    
    def detect_sum_clustering(self, recent_sums: List[int], window_size: int = 20) -> Optional[SumPattern]:
        """检测和值聚集"""
        if len(recent_sums) < window_size:
            return None
        
        # 计算最近窗口内的和值分布
        window_sums = recent_sums[-window_size:]
        unique_sums = set(window_sums)
        
        # 计算聚集度（唯一值比例）
        clustering_ratio = len(unique_sums) / len(window_sums)
        
        if clustering_ratio < self.suspicious_thresholds['sum_clustering']:
            return SumPattern(
                pattern_type="sum_clustering",
                description=f"和值过度聚集: {len(unique_sums)} 个不同值在 {window_size} 次开奖中",
                confidence=1.0 - clustering_ratio,
                evidence={
                    "unique_count": len(unique_sums),
                    "window_size": window_size,
                    "clustering_ratio": clustering_ratio,
                    "clustered_values": list(unique_sums)
                },
                risk_level="high" if clustering_ratio < 0.5 else "medium"
            )
        return None
    
    def detect_periodicity(self, recent_sums: List[int]) -> Optional[SumPattern]:
        """检测周期性模式"""
        if len(recent_sums) < 20:
            return None
        
        # 检查不同周期长度
        for period in range(2, min(10, len(recent_sums) // 3)):
            matches = 0
            total_checks = 0
            
            # 检查周期性重复
            for i in range(period, len(recent_sums)):
                if recent_sums[i] == recent_sums[i - period]:
                    matches += 1
                total_checks += 1
            
            if total_checks > 0:
                periodicity_strength = matches / total_checks
                
                if periodicity_strength > self.suspicious_thresholds['periodicity_strength']:
                    return SumPattern(
                        pattern_type="periodic_pattern",
                        description=f"检测到周期为 {period} 的重复模式，强度: {periodicity_strength:.2f}",
                        confidence=periodicity_strength,
                        evidence={
                            "period": period,
                            "strength": periodicity_strength,
                            "matches": matches,
                            "total_checks": total_checks
                        },
                        risk_level="high" if periodicity_strength > 0.9 else "medium"
                    )
        return None
    
    def analyze_sum_pattern(self, numbers: List[int], record_id: str, 
                           recent_history: Optional[List[int]] = None) -> SumAnalysis:
        """分析和值模式"""
        sum_value = self.calculate_sum(numbers)
        historical_data = self.get_historical_sum_data()
        
        # 如果提供了最近历史，使用它；否则从数据库获取
        if recent_history is None:
            recent_history = historical_data.sums[-50:]  # 最近50期
        
        patterns = []
        
        # 执行各种模式检测
        deviation_pattern = self.detect_deviation_pattern(sum_value, historical_data)
        if deviation_pattern:
            patterns.append(deviation_pattern)
        
        frequency_pattern = self.detect_frequency_anomaly(sum_value, historical_data)
        if frequency_pattern:
            patterns.append(frequency_pattern)
        
        # 将当前和值添加到历史中进行连续模式检测
        extended_history = recent_history + [sum_value]
        
        consecutive_pattern = self.detect_consecutive_pattern(extended_history)
        if consecutive_pattern:
            patterns.append(consecutive_pattern)
        
        arithmetic_pattern = self.detect_arithmetic_sequence(extended_history)
        if arithmetic_pattern:
            patterns.append(arithmetic_pattern)
        
        clustering_pattern = self.detect_sum_clustering(extended_history)
        if clustering_pattern:
            patterns.append(clustering_pattern)
        
        periodicity_pattern = self.detect_periodicity(extended_history)
        if periodicity_pattern:
            patterns.append(periodicity_pattern)
        
        # 计算偏差分数
        deviation_score = 0
        if historical_data.std_dev > 0:
            deviation_score = abs(sum_value - historical_data.mean) / historical_data.std_dev
        
        # 判断是否可疑
        critical_patterns = [p for p in patterns if p.risk_level == "critical"]
        high_risk_patterns = [p for p in patterns if p.risk_level == "high"]
        
        is_suspicious = (
            len(critical_patterns) > 0 or
            len(high_risk_patterns) >= 2 or
            deviation_score > 3.0
        )
        
        # 生成建议
        if is_suspicious:
            if critical_patterns:
                recommendation = f"严重警告：检测到 {len(critical_patterns)} 个严重模式，强烈建议拒绝此数据"
            else:
                recommendation = f"警告：检测到 {len(patterns)} 个可疑模式，建议人工审核"
        else:
            recommendation = "数据通过和值模式检测，可以接受"
        
        return SumAnalysis(
            record_id=record_id,
            sum_value=sum_value,
            expected_range=(int(historical_data.mean - 2*historical_data.std_dev), 
                          int(historical_data.mean + 2*historical_data.std_dev)),
            deviation_score=deviation_score,
            patterns=patterns,
            is_suspicious=is_suspicious,
            recommendation=recommendation
        )
    
    def save_analysis_result(self, analysis: SumAnalysis):
        """保存分析结果"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 保存和值分析结果
            cursor.execute("""
                INSERT INTO sum_analysis 
                (record_id, sum_value, deviation_score, is_suspicious, patterns, recommendation, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.record_id,
                analysis.sum_value,
                analysis.deviation_score,
                analysis.is_suspicious,
                json.dumps([{
                    "type": p.pattern_type,
                    "description": p.description,
                    "confidence": p.confidence,
                    "risk_level": p.risk_level,
                    "evidence": p.evidence
                } for p in analysis.patterns]),
                analysis.recommendation,
                datetime.now().isoformat()
            ))
            
            # 保存模式检测结果
            for pattern in analysis.patterns:
                cursor.execute("""
                    INSERT INTO pattern_detections 
                    (pattern_type, confidence, risk_level, evidence, affected_records, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    pattern.pattern_type,
                    pattern.confidence,
                    pattern.risk_level,
                    json.dumps(pattern.evidence),
                    json.dumps([analysis.record_id]),
                    datetime.now().isoformat()
                ))
            
            conn.commit()
    
    def add_historical_sum(self, draw_id: str, numbers: List[int], timestamp: str, source: str):
        """添加历史和值数据"""
        sum_value = self.calculate_sum(numbers)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO historical_sums 
                (draw_id, sum_value, numbers, timestamp, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                draw_id,
                sum_value,
                json.dumps(numbers),
                timestamp,
                source,
                datetime.now().isoformat()
            ))
            
            conn.commit()
    
    def get_detection_statistics(self) -> Dict[str, Any]:
        """获取检测统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 总体统计
            cursor.execute("SELECT COUNT(*) FROM sum_analysis")
            total_analyses = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM sum_analysis WHERE is_suspicious = 1")
            suspicious_count = cursor.fetchone()[0]
            
            # 按模式类型统计
            cursor.execute("""
                SELECT pattern_type, COUNT(*) as count, AVG(confidence) as avg_confidence
                FROM pattern_detections 
                GROUP BY pattern_type
            """)
            pattern_stats = {}
            for row in cursor.fetchall():
                pattern_stats[row[0]] = {
                    "count": row[1],
                    "avg_confidence": row[2]
                }
            
            # 风险等级统计
            cursor.execute("""
                SELECT risk_level, COUNT(*) as count
                FROM pattern_detections 
                GROUP BY risk_level
            """)
            risk_stats = {row[0]: row[1] for row in cursor.fetchall()}
        
        return {
            "total_analyses": total_analyses,
            "suspicious_count": suspicious_count,
            "suspicious_rate": (suspicious_count / total_analyses) * 100 if total_analyses > 0 else 0,
            "pattern_statistics": pattern_stats,
            "risk_statistics": risk_stats
        }

def main():
    """测试和值模式检测器"""
    detector = SumPatternDetector()
    
    print("=== 和值模式检测测试 ===")
    
    # 添加一些历史数据
    historical_records = [
        ([1, 3, 7, 2, 8], "20241201001"),  # 和值: 21
        ([2, 5, 1, 9, 4], "20241201002"),  # 和值: 21
        ([0, 8, 3, 6, 7], "20241201003"),  # 和值: 24
        ([4, 2, 9, 1, 5], "20241201004"),  # 和值: 21
    ]
    
    for numbers, draw_id in historical_records:
        detector.add_historical_sum(draw_id, numbers, "2024-12-01T10:00:00Z", "test_data")
    
    # 测试数据（包含可疑模式）
    test_cases = [
        {
            "numbers": [1, 3, 7, 2, 8],  # 正常和值: 21
            "record_id": "test_001",
            "description": "正常数据"
        },
        {
            "numbers": [5, 5, 5, 5, 5],  # 异常和值: 25，连续相同
            "record_id": "test_002",
            "description": "连续相同数字"
        },
        {
            "numbers": [0, 0, 0, 0, 1],  # 异常和值: 1，过小
            "record_id": "test_003",
            "description": "和值过小"
        },
        {
            "numbers": [9, 9, 9, 9, 9],  # 异常和值: 17，但模式可疑
            "record_id": "test_004",
            "description": "重复模式"
        }
    ]
    
    print(f"\n开始分析 {len(test_cases)} 个测试用例...")
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n--- 测试用例 {i}: {test_case['description']} ---")
        print(f"开奖号码: {test_case['numbers']}")
        
        # 执行和值模式分析
        analysis = detector.analyze_sum_pattern(
            test_case['numbers'], 
            test_case['record_id']
        )
        
        print(f"和值: {analysis.sum_value}")
        print(f"期望范围: {analysis.expected_range}")
        print(f"偏差分数: {analysis.deviation_score:.2f}")
        print(f"是否可疑: {'是' if analysis.is_suspicious else '否'}")
        print(f"建议: {analysis.recommendation}")
        
        if analysis.patterns:
            print(f"检测到的模式:")
            for pattern in analysis.patterns:
                print(f"  - [{pattern.risk_level.upper()}] {pattern.pattern_type}: {pattern.description}")
                print(f"    置信度: {pattern.confidence:.2f}")
        
        # 保存分析结果
        detector.save_analysis_result(analysis)
    
    # 获取统计信息
    stats = detector.get_detection_statistics()
    print(f"\n=== 检测统计信息 ===")
    print(f"总分析次数: {stats['total_analyses']}")
    print(f"可疑数据: {stats['suspicious_count']} ({stats['suspicious_rate']:.1f}%)")
    
    if stats['pattern_statistics']:
        print(f"\n模式统计:")
        for pattern_type, stat in stats['pattern_statistics'].items():
            print(f"  {pattern_type}: {stat['count']} 次 (平均置信度: {stat['avg_confidence']:.2f})")
    
    if stats['risk_statistics']:
        print(f"\n风险等级统计:")
        for risk_level, count in stats['risk_statistics'].items():
            print(f"  {risk_level}: {count} 次")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()