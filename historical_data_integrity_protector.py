#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
历史数据完整性保护机制
防止伪随机和模拟数据污染数据库
"""

import sqlite3
import hashlib
import json
import time
import statistics
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import Counter, defaultdict
import re

@dataclass
class RandomnessTest:
    """随机性测试结果"""
    test_name: str
    score: float  # 0-100, 100为完全随机
    threshold: float
    passed: bool
    details: Dict[str, Any]

@dataclass
class IntegrityIssue:
    """完整性问题"""
    issue_type: str
    severity: str  # critical, warning, info
    description: str
    affected_records: List[str]
    confidence: float  # 0-1
    recommendation: str

@dataclass
class ProtectionReport:
    """保护报告"""
    timestamp: str
    total_records: int
    blocked_records: int
    randomness_tests: List[RandomnessTest]
    integrity_issues: List[IntegrityIssue]
    overall_score: float
    recommendation: str

class HistoricalDataIntegrityProtector:
    """历史数据完整性保护器"""
    
    def __init__(self, db_path: str = "integrity_protection.db"):
        self.db_path = db_path
        self.init_database()
        
        # 随机性测试阈值
        self.randomness_thresholds = {
            'chi_square': 70.0,
            'frequency': 75.0,
            'runs': 65.0,
            'serial_correlation': 80.0,
            'entropy': 85.0,
            'pattern_detection': 90.0
        }
        
        # 已知的伪随机模式
        self.known_fake_patterns = [
            r'^(\d)\1{4,}$',  # 连续相同数字
            r'^(01|10|12|21|23|32){2,}$',  # 重复模式
            r'^\d*(123|234|345|456|567|678|789|890|901|012)\d*$',  # 连续序列
        ]
    
    def init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 创建保护记录表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS protection_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    data_source TEXT NOT NULL,
                    randomness_score REAL NOT NULL,
                is_blocked BOOLEAN NOT NULL,
                block_reason TEXT,
                created_at TEXT NOT NULL
            )
        """)
        
            # 创建随机性测试结果表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS randomness_tests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_hash TEXT NOT NULL,
                    test_name TEXT NOT NULL,
                    score REAL NOT NULL,
                    threshold REAL NOT NULL,
                    passed BOOLEAN NOT NULL,
                    details TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            
            # 创建完整性问题表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS integrity_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    description TEXT NOT NULL,
                    affected_records TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    recommendation TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            conn.commit()
    
    def calculate_chi_square_test(self, numbers: List[int]) -> RandomnessTest:
        """卡方检验"""
        if not numbers or len(numbers) < 10:
            return RandomnessTest(
                test_name="chi_square",
                score=0.0,
                threshold=self.randomness_thresholds['chi_square'],
                passed=False,
                details={"error": "数据不足"}
            )
        
        # 计算频率分布
        counter = Counter(numbers)
        expected_freq = len(numbers) / 10  # 假设0-9均匀分布
        
        chi_square = 0
        for i in range(10):
            observed = counter.get(i, 0)
            chi_square += (observed - expected_freq) ** 2 / expected_freq
        
        # 转换为0-100分数（越接近理论值分数越高）
        score = max(0, 100 - chi_square * 2)
        
        return RandomnessTest(
            test_name="chi_square",
            score=score,
            threshold=self.randomness_thresholds['chi_square'],
            passed=score >= self.randomness_thresholds['chi_square'],
            details={
                "chi_square_value": chi_square,
                "expected_freq": expected_freq,
                "distribution": dict(counter)
            }
        )
    
    def calculate_frequency_test(self, numbers: List[int]) -> RandomnessTest:
        """频率测试"""
        if not numbers:
            return RandomnessTest(
                test_name="frequency",
                score=0.0,
                threshold=self.randomness_thresholds['frequency'],
                passed=False,
                details={"error": "数据为空"}
            )
        
        counter = Counter(numbers)
        frequencies = list(counter.values())
        
        # 计算频率方差（越小越随机）
        freq_variance = statistics.variance(frequencies) if len(frequencies) > 1 else 0
        max_variance = (len(numbers) / 10) ** 2  # 理论最大方差
        
        score = max(0, 100 - (freq_variance / max_variance) * 100)
        
        return RandomnessTest(
            test_name="frequency",
            score=score,
            threshold=self.randomness_thresholds['frequency'],
            passed=score >= self.randomness_thresholds['frequency'],
            details={
                "frequency_variance": freq_variance,
                "max_variance": max_variance,
                "distribution": dict(counter)
            }
        )
    
    def calculate_runs_test(self, numbers: List[int]) -> RandomnessTest:
        """游程测试"""
        if len(numbers) < 2:
            return RandomnessTest(
                test_name="runs",
                score=0.0,
                threshold=self.randomness_thresholds['runs'],
                passed=False,
                details={"error": "数据不足"}
            )
        
        # 计算游程数量
        runs = 1
        for i in range(1, len(numbers)):
            if numbers[i] != numbers[i-1]:
                runs += 1
        
        # 期望游程数
        n = len(numbers)
        expected_runs = (2 * n - 1) / 3
        
        # 计算偏差
        deviation = abs(runs - expected_runs) / expected_runs
        score = max(0, 100 - deviation * 100)
        
        return RandomnessTest(
            test_name="runs",
            score=score,
            threshold=self.randomness_thresholds['runs'],
            passed=score >= self.randomness_thresholds['runs'],
            details={
                "actual_runs": runs,
                "expected_runs": expected_runs,
                "deviation": deviation
            }
        )
    
    def calculate_serial_correlation(self, numbers: List[int]) -> RandomnessTest:
        """序列相关性测试"""
        if len(numbers) < 3:
            return RandomnessTest(
                test_name="serial_correlation",
                score=0.0,
                threshold=self.randomness_thresholds['serial_correlation'],
                passed=False,
                details={"error": "数据不足"}
            )
        
        # 计算相邻数字的相关系数
        x = numbers[:-1]
        y = numbers[1:]
        
        if len(set(x)) == 1 or len(set(y)) == 1:
            correlation = 1.0  # 完全相关
        else:
            mean_x = statistics.mean(x)
            mean_y = statistics.mean(y)
            
            numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(len(x)))
            denominator_x = sum((x[i] - mean_x) ** 2 for i in range(len(x)))
            denominator_y = sum((y[i] - mean_y) ** 2 for i in range(len(y)))
            
            if denominator_x == 0 or denominator_y == 0:
                correlation = 0.0
            else:
                correlation = abs(numerator / (denominator_x * denominator_y) ** 0.5)
        
        # 相关性越低分数越高
        score = max(0, 100 - correlation * 100)
        
        return RandomnessTest(
            test_name="serial_correlation",
            score=score,
            threshold=self.randomness_thresholds['serial_correlation'],
            passed=score >= self.randomness_thresholds['serial_correlation'],
            details={
                "correlation": correlation,
                "sample_size": len(x)
            }
        )
    
    def calculate_entropy(self, numbers: List[int]) -> RandomnessTest:
        """熵测试"""
        if not numbers:
            return RandomnessTest(
                test_name="entropy",
                score=0.0,
                threshold=self.randomness_thresholds['entropy'],
                passed=False,
                details={"error": "数据为空"}
            )
        
        counter = Counter(numbers)
        total = len(numbers)
        
        # 计算香农熵
        entropy = 0
        for count in counter.values():
            probability = count / total
            if probability > 0:
                entropy -= probability * (probability ** 0.5)  # 简化计算
        
        # 最大熵（均匀分布）
        max_entropy = -(10 * (0.1 * (0.1 ** 0.5)))  # 10个数字均匀分布
        
        # 标准化为0-100分数
        score = (entropy / max_entropy) * 100 if max_entropy != 0 else 0
        
        return RandomnessTest(
            test_name="entropy",
            score=score,
            threshold=self.randomness_thresholds['entropy'],
            passed=score >= self.randomness_thresholds['entropy'],
            details={
                "entropy": entropy,
                "max_entropy": max_entropy,
                "unique_values": len(counter)
            }
        )
    
    def detect_patterns(self, numbers: List[int]) -> RandomnessTest:
        """模式检测"""
        if not numbers:
            return RandomnessTest(
                test_name="pattern_detection",
                score=0.0,
                threshold=self.randomness_thresholds['pattern_detection'],
                passed=False,
                details={"error": "数据为空"}
            )
        
        number_str = ''.join(map(str, numbers))
        detected_patterns = []
        
        # 检查已知伪随机模式
        for pattern in self.known_fake_patterns:
            if re.search(pattern, number_str):
                detected_patterns.append(pattern)
        
        # 检查重复子序列
        for length in range(2, min(6, len(numbers) // 2)):
            for i in range(len(numbers) - length * 2 + 1):
                subseq = numbers[i:i+length]
                for j in range(i + length, len(numbers) - length + 1):
                    if numbers[j:j+length] == subseq:
                        detected_patterns.append(f"重复子序列: {subseq}")
                        break
        
        # 分数基于检测到的模式数量
        pattern_penalty = len(detected_patterns) * 20
        score = max(0, 100 - pattern_penalty)
        
        return RandomnessTest(
            test_name="pattern_detection",
            score=score,
            threshold=self.randomness_thresholds['pattern_detection'],
            passed=score >= self.randomness_thresholds['pattern_detection'],
            details={
                "detected_patterns": detected_patterns,
                "pattern_count": len(detected_patterns)
            }
        )
    
    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, List[RandomnessTest], List[IntegrityIssue]]:
        """验证单条记录"""
        randomness_tests = []
        integrity_issues = []
        
        # 提取开奖号码
        numbers = []
        if 'numbers' in record and isinstance(record['numbers'], list):
            numbers = [int(x) for x in record['numbers'] if str(x).isdigit()]
        elif 'l_number' in record:
            # 解析开奖号码字符串
            number_str = str(record['l_number']).replace(',', '').replace(' ', '')
            numbers = [int(x) for x in number_str if x.isdigit()]
        
        if not numbers:
            integrity_issues.append(IntegrityIssue(
                issue_type="missing_numbers",
                severity="critical",
                description="缺少开奖号码数据",
                affected_records=[str(record.get('draw_id', 'unknown'))],
                confidence=1.0,
                recommendation="拒绝写入，要求提供完整的开奖号码"
            ))
            return False, randomness_tests, integrity_issues
        
        # 执行随机性测试
        randomness_tests.extend([
            self.calculate_chi_square_test(numbers),
            self.calculate_frequency_test(numbers),
            self.calculate_runs_test(numbers),
            self.calculate_serial_correlation(numbers),
            self.calculate_entropy(numbers),
            self.detect_patterns(numbers)
        ])
        
        # 计算总体随机性分数
        passed_tests = sum(1 for test in randomness_tests if test.passed)
        total_tests = len(randomness_tests)
        randomness_score = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
        
        # 检查是否为伪随机数据
        if randomness_score < 60:  # 阈值可调整
            integrity_issues.append(IntegrityIssue(
                issue_type="pseudo_random_detected",
                severity="critical",
                description=f"检测到伪随机数据，随机性分数: {randomness_score:.2f}%",
                affected_records=[str(record.get('draw_id', 'unknown'))],
                confidence=min(1.0, (60 - randomness_score) / 60),
                recommendation="拒绝写入，数据可能为模拟生成"
            ))
            return False, randomness_tests, integrity_issues
        
        # 检查时间戳合理性
        if 'timestamp' in record:
            try:
                record_time = datetime.fromisoformat(str(record['timestamp']).replace('Z', '+00:00'))
                now = datetime.now()
                
                # 检查时间是否过于未来或过于久远
                if record_time > now + timedelta(hours=1):
                    integrity_issues.append(IntegrityIssue(
                        issue_type="future_timestamp",
                        severity="warning",
                        description="时间戳指向未来",
                        affected_records=[str(record.get('draw_id', 'unknown'))],
                        confidence=0.8,
                        recommendation="检查时间同步设置"
                    ))
                elif record_time < now - timedelta(days=365):
                    integrity_issues.append(IntegrityIssue(
                        issue_type="old_timestamp",
                        severity="info",
                        description="时间戳过于久远",
                        affected_records=[str(record.get('draw_id', 'unknown'))],
                        confidence=0.6,
                        recommendation="确认是否为历史数据回填"
                    ))
            except Exception as e:
                integrity_issues.append(IntegrityIssue(
                    issue_type="invalid_timestamp",
                    severity="warning",
                    description=f"时间戳格式错误: {str(e)}",
                    affected_records=[str(record.get('draw_id', 'unknown'))],
                    confidence=0.9,
                    recommendation="修正时间戳格式"
                ))
        
        # 只有严重问题才阻止写入
        critical_issues = [issue for issue in integrity_issues if issue.severity == "critical"]
        return len(critical_issues) == 0, randomness_tests, integrity_issues
    
    def protect_batch_write(self, records: List[Dict[str, Any]], data_source: str = "unknown") -> ProtectionReport:
        """保护批量写入"""
        timestamp = datetime.now().isoformat()
        total_records = len(records)
        blocked_records = 0
        all_randomness_tests = []
        all_integrity_issues = []
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for record in records:
                record_hash = hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()
                
                is_valid, randomness_tests, integrity_issues = self.validate_record(record)
                
                if not is_valid:
                    blocked_records += 1
                    block_reason = "; ".join([issue.description for issue in integrity_issues if issue.severity == "critical"])
                else:
                    block_reason = None
                
                # 计算随机性分数
                randomness_score = statistics.mean([test.score for test in randomness_tests]) if randomness_tests else 0
                
                # 记录保护结果
                cursor.execute("""
                    INSERT INTO protection_records 
                    (timestamp, record_hash, data_source, randomness_score, is_blocked, block_reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (timestamp, record_hash, data_source, randomness_score, not is_valid, block_reason, datetime.now().isoformat()))
                
                # 记录随机性测试结果
                for test in randomness_tests:
                    cursor.execute("""
                        INSERT INTO randomness_tests 
                        (record_hash, test_name, score, threshold, passed, details, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (record_hash, test.test_name, test.score, test.threshold, test.passed, 
                           json.dumps(test.details), datetime.now().isoformat()))
                
                all_randomness_tests.extend(randomness_tests)
                all_integrity_issues.extend(integrity_issues)
            
            # 记录完整性问题
            for issue in all_integrity_issues:
                cursor.execute("""
                    INSERT INTO integrity_issues 
                    (issue_type, severity, description, affected_records, confidence, recommendation, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (issue.issue_type, issue.severity, issue.description, 
                       json.dumps(issue.affected_records), issue.confidence, issue.recommendation, datetime.now().isoformat()))
            
            conn.commit()
        
        # 计算总体分数
        overall_score = ((total_records - blocked_records) / total_records) * 100 if total_records > 0 else 0
        
        # 生成建议
        if blocked_records == 0:
            recommendation = "所有数据通过完整性检查，可以安全写入"
        elif blocked_records == total_records:
            recommendation = "所有数据都被阻止，建议检查数据源的真实性"
        else:
            recommendation = f"部分数据被阻止({blocked_records}/{total_records})，建议审查数据质量"
        
        return ProtectionReport(
            timestamp=timestamp,
            total_records=total_records,
            blocked_records=blocked_records,
            randomness_tests=all_randomness_tests,
            integrity_issues=all_integrity_issues,
            overall_score=overall_score,
            recommendation=recommendation
        )
    
    def get_protection_statistics(self) -> Dict[str, Any]:
        """获取保护统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 总体统计
            cursor.execute("SELECT COUNT(*) FROM protection_records")
            total_records = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM protection_records WHERE is_blocked = 1")
            blocked_records = cursor.fetchone()[0]
            
            # 按数据源统计
            cursor.execute("""
                SELECT data_source, COUNT(*) as total, 
                       SUM(CASE WHEN is_blocked = 1 THEN 1 ELSE 0 END) as blocked
                FROM protection_records 
                GROUP BY data_source
            """)
            source_stats = {row[0]: {"total": row[1], "blocked": row[2]} for row in cursor.fetchall()}
            
            # 随机性测试统计
            cursor.execute("""
                SELECT test_name, AVG(score) as avg_score, 
                       SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END) as passed_count,
                       COUNT(*) as total_count
                FROM randomness_tests 
                GROUP BY test_name
            """)
            test_stats = {}
            for row in cursor.fetchall():
                test_stats[row[0]] = {
                    "avg_score": row[1],
                    "passed_count": row[2],
                    "total_count": row[3],
                    "pass_rate": (row[2] / row[3]) * 100 if row[3] > 0 else 0
                }
        
        return {
            "total_records": total_records,
            "blocked_records": blocked_records,
            "block_rate": (blocked_records / total_records) * 100 if total_records > 0 else 0,
            "source_statistics": source_stats,
            "test_statistics": test_stats
        }

def main():
    """测试历史数据完整性保护机制"""
    protector = HistoricalDataIntegrityProtector()
    
    # 测试数据（包含真实随机和伪随机数据）
    test_records = [
        {
            "draw_id": "20241201001",
            "numbers": [1, 3, 7, 2, 8],  # 相对随机
            "timestamp": "2024-12-01T10:00:00Z",
            "source": "real_api"
        },
        {
            "draw_id": "20241201002", 
            "numbers": [1, 1, 1, 1, 1],  # 明显伪随机
            "timestamp": "2024-12-01T10:05:00Z",
            "source": "suspicious"
        },
        {
            "draw_id": "20241201003",
            "numbers": [1, 2, 3, 4, 5],  # 连续序列
            "timestamp": "2024-12-01T10:10:00Z",
            "source": "test_data"
        },
        {
            "draw_id": "20241201004",
            "numbers": [9, 2, 5, 1, 7],  # 相对随机
            "timestamp": "2024-12-01T10:15:00Z",
            "source": "real_api"
        }
    ]
    
    print("=== 历史数据完整性保护测试 ===")
    
    # 执行保护检查
    report = protector.protect_batch_write(test_records, "test_batch")
    
    print(f"\n保护报告:")
    print(f"时间戳: {report.timestamp}")
    print(f"总记录数: {report.total_records}")
    print(f"被阻止记录数: {report.blocked_records}")
    print(f"总体分数: {report.overall_score:.2f}%")
    print(f"建议: {report.recommendation}")
    
    print(f"\n随机性测试结果:")
    test_summary = defaultdict(list)
    for test in report.randomness_tests:
        test_summary[test.test_name].append(test.score)
    
    for test_name, scores in test_summary.items():
        avg_score = statistics.mean(scores)
        print(f"  {test_name}: 平均分数 {avg_score:.2f}")
    
    print(f"\n完整性问题:")
    issue_summary = defaultdict(int)
    for issue in report.integrity_issues:
        issue_summary[f"{issue.severity}_{issue.issue_type}"] += 1
        if issue.severity == "critical":
            print(f"  [严重] {issue.description} (置信度: {issue.confidence:.2f})")
    
    # 获取统计信息
    stats = protector.get_protection_statistics()
    print(f"\n保护统计信息:")
    print(f"总处理记录: {stats['total_records']}")
    print(f"阻止率: {stats['block_rate']:.2f}%")
    
    print(f"\n按测试类型统计:")
    for test_name, test_stat in stats['test_statistics'].items():
        print(f"  {test_name}: 通过率 {test_stat['pass_rate']:.2f}%, 平均分数 {test_stat['avg_score']:.2f}")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()