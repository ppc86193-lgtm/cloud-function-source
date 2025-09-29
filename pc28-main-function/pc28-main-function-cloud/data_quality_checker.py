#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定期数据质量和完整性检查机制
实现自动化的数据质量监控、完整性验证和质量报告生成
"""

import sqlite3
import json
import logging
import threading
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
import statistics
import hashlib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class QualityMetric:
    """数据质量指标"""
    metric_name: str
    value: float
    threshold: float
    status: str  # 'good', 'warning', 'critical'
    description: str
    timestamp: datetime

@dataclass
class IntegrityCheck:
    """完整性检查结果"""
    check_type: str
    table_name: str
    expected_count: int
    actual_count: int
    missing_records: List[str]
    duplicate_records: List[str]
    status: str
    timestamp: datetime

@dataclass
class QualityReport:
    """质量报告"""
    report_id: str
    timestamp: datetime
    overall_score: float
    quality_metrics: List[QualityMetric]
    integrity_checks: List[IntegrityCheck]
    recommendations: List[str]
    critical_issues: int
    warning_issues: int

@dataclass
class DataProfile:
    """数据概况"""
    table_name: str
    total_records: int
    null_percentage: float
    duplicate_percentage: float
    data_freshness_hours: float
    schema_consistency: bool
    data_types_valid: bool
    timestamp: datetime

class DataQualityChecker:
    """数据质量和完整性检查器"""
    
    def __init__(self, db_path: str = "data_quality.db"):
        self.db_path = db_path
        self.monitoring_active = False
        self.check_interval = 3600  # 1小时检查一次
        self.monitor_thread = None
        
        # 质量阈值配置
        self.quality_thresholds = {
            'null_percentage': {'warning': 5.0, 'critical': 15.0},
            'duplicate_percentage': {'warning': 2.0, 'critical': 10.0},
            'data_freshness_hours': {'warning': 24.0, 'critical': 72.0},
            'completeness_score': {'warning': 90.0, 'critical': 70.0},
            'consistency_score': {'warning': 95.0, 'critical': 85.0}
        }
        
        # 监控的数据库列表
        self.monitored_databases = [
            "lottery_data.db",
            "validation.db",
            "deduplication.db",
            "historical_integrity.db",
            "sum_patterns.db"
        ]
        
        self._init_database()
    
    def _init_database(self):
        """初始化质量检查数据库"""
        with sqlite3.connect(self.db_path) as conn:
            # 质量指标表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    value REAL NOT NULL,
                    threshold REAL NOT NULL,
                    status TEXT NOT NULL,
                    description TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 完整性检查表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS integrity_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_type TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    expected_count INTEGER,
                    actual_count INTEGER,
                    missing_records TEXT,
                    duplicate_records TEXT,
                    status TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 质量报告表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id TEXT UNIQUE NOT NULL,
                    overall_score REAL NOT NULL,
                    critical_issues INTEGER DEFAULT 0,
                    warning_issues INTEGER DEFAULT 0,
                    recommendations TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 数据概况表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    total_records INTEGER,
                    null_percentage REAL,
                    duplicate_percentage REAL,
                    data_freshness_hours REAL,
                    schema_consistency BOOLEAN,
                    data_types_valid BOOLEAN,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
    
    def check_data_completeness(self, db_path: str, table_name: str) -> QualityMetric:
        """检查数据完整性"""
        try:
            if not Path(db_path).exists():
                return QualityMetric(
                    metric_name="completeness",
                    value=0.0,
                    threshold=self.quality_thresholds['completeness_score']['critical'],
                    status="critical",
                    description=f"数据库文件不存在: {db_path}",
                    timestamp=datetime.now()
                )
            
            with sqlite3.connect(db_path) as conn:
                # 获取表结构
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                
                if not columns:
                    return QualityMetric(
                        metric_name="completeness",
                        value=0.0,
                        threshold=self.quality_thresholds['completeness_score']['critical'],
                        status="critical",
                        description=f"表不存在或无列: {table_name}",
                        timestamp=datetime.now()
                    )
                
                # 计算完整性分数
                total_records = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                
                if total_records == 0:
                    completeness_score = 0.0
                else:
                    null_counts = []
                    for column in columns:
                        null_count = conn.execute(
                            f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL"
                        ).fetchone()[0]
                        null_counts.append(null_count)
                    
                    total_nulls = sum(null_counts)
                    total_cells = total_records * len(columns)
                    completeness_score = ((total_cells - total_nulls) / total_cells) * 100
                
                # 确定状态
                if completeness_score >= self.quality_thresholds['completeness_score']['warning']:
                    status = "good"
                elif completeness_score >= self.quality_thresholds['completeness_score']['critical']:
                    status = "warning"
                else:
                    status = "critical"
                
                return QualityMetric(
                    metric_name="completeness",
                    value=completeness_score,
                    threshold=self.quality_thresholds['completeness_score']['warning'],
                    status=status,
                    description=f"数据完整性分数: {completeness_score:.2f}%",
                    timestamp=datetime.now()
                )
        
        except Exception as e:
            logger.error(f"检查数据完整性时出错: {e}")
            return QualityMetric(
                metric_name="completeness",
                value=0.0,
                threshold=self.quality_thresholds['completeness_score']['critical'],
                status="critical",
                description=f"检查失败: {str(e)}",
                timestamp=datetime.now()
            )
    
    def check_data_consistency(self, db_path: str, table_name: str) -> QualityMetric:
        """检查数据一致性"""
        try:
            if not Path(db_path).exists():
                return QualityMetric(
                    metric_name="consistency",
                    value=0.0,
                    threshold=self.quality_thresholds['consistency_score']['critical'],
                    status="critical",
                    description=f"数据库文件不存在: {db_path}",
                    timestamp=datetime.now()
                )
            
            with sqlite3.connect(db_path) as conn:
                # 检查数据类型一致性
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                schema_info = cursor.fetchall()
                
                if not schema_info:
                    return QualityMetric(
                        metric_name="consistency",
                        value=0.0,
                        threshold=self.quality_thresholds['consistency_score']['critical'],
                        status="critical",
                        description=f"表不存在: {table_name}",
                        timestamp=datetime.now()
                    )
                
                consistency_issues = 0
                total_checks = 0
                
                # 检查每列的数据类型一致性
                for column_info in schema_info:
                    column_name = column_info[1]
                    expected_type = column_info[2]
                    
                    # 简单的类型检查
                    if expected_type.upper() in ['INTEGER', 'INT']:
                        invalid_count = conn.execute(f"""
                            SELECT COUNT(*) FROM {table_name} 
                            WHERE {column_name} IS NOT NULL 
                            AND TYPEOF({column_name}) != 'integer'
                        """).fetchone()[0]
                        consistency_issues += invalid_count
                        total_checks += 1
                    
                    elif expected_type.upper() in ['REAL', 'FLOAT', 'DOUBLE']:
                        invalid_count = conn.execute(f"""
                            SELECT COUNT(*) FROM {table_name} 
                            WHERE {column_name} IS NOT NULL 
                            AND TYPEOF({column_name}) NOT IN ('real', 'integer')
                        """).fetchone()[0]
                        consistency_issues += invalid_count
                        total_checks += 1
                
                # 计算一致性分数
                if total_checks == 0:
                    consistency_score = 100.0
                else:
                    consistency_score = max(0, (total_checks - consistency_issues) / total_checks * 100)
                
                # 确定状态
                if consistency_score >= self.quality_thresholds['consistency_score']['warning']:
                    status = "good"
                elif consistency_score >= self.quality_thresholds['consistency_score']['critical']:
                    status = "warning"
                else:
                    status = "critical"
                
                return QualityMetric(
                    metric_name="consistency",
                    value=consistency_score,
                    threshold=self.quality_thresholds['consistency_score']['warning'],
                    status=status,
                    description=f"数据一致性分数: {consistency_score:.2f}%",
                    timestamp=datetime.now()
                )
        
        except Exception as e:
            logger.error(f"检查数据一致性时出错: {e}")
            return QualityMetric(
                metric_name="consistency",
                value=0.0,
                threshold=self.quality_thresholds['consistency_score']['critical'],
                status="critical",
                description=f"检查失败: {str(e)}",
                timestamp=datetime.now()
            )
    
    def check_data_freshness(self, db_path: str, table_name: str, timestamp_column: str = "timestamp") -> QualityMetric:
        """检查数据新鲜度"""
        try:
            if not Path(db_path).exists():
                return QualityMetric(
                    metric_name="freshness",
                    value=999.0,
                    threshold=self.quality_thresholds['data_freshness_hours']['critical'],
                    status="critical",
                    description=f"数据库文件不存在: {db_path}",
                    timestamp=datetime.now()
                )
            
            with sqlite3.connect(db_path) as conn:
                # 获取最新记录的时间戳
                try:
                    latest_timestamp = conn.execute(
                        f"SELECT MAX({timestamp_column}) FROM {table_name}"
                    ).fetchone()[0]
                    
                    if latest_timestamp is None:
                        freshness_hours = 999.0
                    else:
                        # 解析时间戳
                        if isinstance(latest_timestamp, str):
                            latest_time = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
                        else:
                            latest_time = latest_timestamp
                        
                        freshness_hours = (datetime.now() - latest_time).total_seconds() / 3600
                
                except Exception:
                    # 如果时间戳列不存在，尝试其他常见的时间列名
                    for col in ['created_at', 'updated_at', 'date', 'time']:
                        try:
                            latest_timestamp = conn.execute(
                                f"SELECT MAX({col}) FROM {table_name}"
                            ).fetchone()[0]
                            if latest_timestamp:
                                if isinstance(latest_timestamp, str):
                                    latest_time = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
                                else:
                                    latest_time = latest_timestamp
                                freshness_hours = (datetime.now() - latest_time).total_seconds() / 3600
                                break
                        except Exception:
                            continue
                    else:
                        freshness_hours = 999.0
                
                # 确定状态
                if freshness_hours <= self.quality_thresholds['data_freshness_hours']['warning']:
                    status = "good"
                elif freshness_hours <= self.quality_thresholds['data_freshness_hours']['critical']:
                    status = "warning"
                else:
                    status = "critical"
                
                return QualityMetric(
                    metric_name="freshness",
                    value=freshness_hours,
                    threshold=self.quality_thresholds['data_freshness_hours']['warning'],
                    status=status,
                    description=f"数据新鲜度: {freshness_hours:.1f}小时前",
                    timestamp=datetime.now()
                )
        
        except Exception as e:
            logger.error(f"检查数据新鲜度时出错: {e}")
            return QualityMetric(
                metric_name="freshness",
                value=999.0,
                threshold=self.quality_thresholds['data_freshness_hours']['critical'],
                status="critical",
                description=f"检查失败: {str(e)}",
                timestamp=datetime.now()
            )
    
    def check_data_integrity(self, db_path: str, table_name: str) -> IntegrityCheck:
        """检查数据完整性"""
        try:
            if not Path(db_path).exists():
                return IntegrityCheck(
                    check_type="existence",
                    table_name=table_name,
                    expected_count=0,
                    actual_count=0,
                    missing_records=[],
                    duplicate_records=[],
                    status="critical",
                    timestamp=datetime.now()
                )
            
            with sqlite3.connect(db_path) as conn:
                # 检查记录数量
                actual_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                
                # 检查重复记录
                duplicate_records = []
                try:
                    # 尝试找到主键或唯一标识符
                    cursor = conn.execute(f"PRAGMA table_info({table_name})")
                    columns = [row[1] for row in cursor.fetchall()]
                    
                    if 'id' in columns:
                        duplicates = conn.execute(f"""
                            SELECT id, COUNT(*) as count 
                            FROM {table_name} 
                            GROUP BY id 
                            HAVING count > 1
                        """).fetchall()
                        duplicate_records = [str(row[0]) for row in duplicates]
                    
                except Exception:
                    pass
                
                # 确定状态
                if len(duplicate_records) == 0 and actual_count > 0:
                    status = "good"
                elif len(duplicate_records) <= 5:
                    status = "warning"
                else:
                    status = "critical"
                
                return IntegrityCheck(
                    check_type="integrity",
                    table_name=table_name,
                    expected_count=actual_count,
                    actual_count=actual_count,
                    missing_records=[],
                    duplicate_records=duplicate_records,
                    status=status,
                    timestamp=datetime.now()
                )
        
        except Exception as e:
            logger.error(f"检查数据完整性时出错: {e}")
            return IntegrityCheck(
                check_type="integrity",
                table_name=table_name,
                expected_count=0,
                actual_count=0,
                missing_records=[],
                duplicate_records=[],
                status="critical",
                timestamp=datetime.now()
            )
    
    def generate_data_profile(self, db_path: str, table_name: str) -> DataProfile:
        """生成数据概况"""
        try:
            if not Path(db_path).exists():
                return DataProfile(
                    table_name=table_name,
                    total_records=0,
                    null_percentage=100.0,
                    duplicate_percentage=0.0,
                    data_freshness_hours=999.0,
                    schema_consistency=False,
                    data_types_valid=False,
                    timestamp=datetime.now()
                )
            
            with sqlite3.connect(db_path) as conn:
                # 总记录数
                total_records = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                
                if total_records == 0:
                    return DataProfile(
                        table_name=table_name,
                        total_records=0,
                        null_percentage=100.0,
                        duplicate_percentage=0.0,
                        data_freshness_hours=999.0,
                        schema_consistency=True,
                        data_types_valid=True,
                        timestamp=datetime.now()
                    )
                
                # 计算空值百分比
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                
                total_nulls = 0
                for column in columns:
                    null_count = conn.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL"
                    ).fetchone()[0]
                    total_nulls += null_count
                
                total_cells = total_records * len(columns)
                null_percentage = (total_nulls / total_cells) * 100 if total_cells > 0 else 0
                
                # 计算重复百分比
                duplicate_percentage = 0.0
                if 'id' in columns:
                    duplicates = conn.execute(f"""
                        SELECT COUNT(*) FROM (
                            SELECT id FROM {table_name} 
                            GROUP BY id 
                            HAVING COUNT(*) > 1
                        )
                    """).fetchone()[0]
                    duplicate_percentage = (duplicates / total_records) * 100
                
                # 数据新鲜度
                freshness_metric = self.check_data_freshness(db_path, table_name)
                data_freshness_hours = freshness_metric.value
                
                return DataProfile(
                    table_name=table_name,
                    total_records=total_records,
                    null_percentage=null_percentage,
                    duplicate_percentage=duplicate_percentage,
                    data_freshness_hours=data_freshness_hours,
                    schema_consistency=True,
                    data_types_valid=True,
                    timestamp=datetime.now()
                )
        
        except Exception as e:
            logger.error(f"生成数据概况时出错: {e}")
            return DataProfile(
                table_name=table_name,
                total_records=0,
                null_percentage=100.0,
                duplicate_percentage=100.0,
                data_freshness_hours=999.0,
                schema_consistency=False,
                data_types_valid=False,
                timestamp=datetime.now()
            )
    
    def run_quality_check(self) -> QualityReport:
        """运行完整的质量检查"""
        report_id = hashlib.md5(f"{datetime.now().isoformat()}".encode()).hexdigest()[:8]
        quality_metrics = []
        integrity_checks = []
        recommendations = []
        
        # 检查所有监控的数据库
        for db_name in self.monitored_databases:
            if not Path(db_name).exists():
                continue
            
            # 获取数据库中的表
            try:
                with sqlite3.connect(db_name) as conn:
                    tables = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                    
                    for table_row in tables:
                        table_name = table_row[0]
                        
                        # 运行各种质量检查
                        completeness = self.check_data_completeness(db_name, table_name)
                        consistency = self.check_data_consistency(db_name, table_name)
                        freshness = self.check_data_freshness(db_name, table_name)
                        integrity = self.check_data_integrity(db_name, table_name)
                        
                        quality_metrics.extend([completeness, consistency, freshness])
                        integrity_checks.append(integrity)
                        
                        # 生成建议
                        if completeness.status == "critical":
                            recommendations.append(f"表 {table_name} 数据完整性严重不足，需要立即修复")
                        if consistency.status == "critical":
                            recommendations.append(f"表 {table_name} 数据一致性问题严重，需要数据清理")
                        if freshness.status == "critical":
                            recommendations.append(f"表 {table_name} 数据过于陈旧，需要更新数据源")
            
            except Exception as e:
                logger.error(f"检查数据库 {db_name} 时出错: {e}")
        
        # 计算总体分数
        if quality_metrics:
            scores = []
            for metric in quality_metrics:
                if metric.metric_name == "freshness":
                    # 新鲜度分数需要反转（小时数越少越好）
                    if metric.value <= 24:
                        score = 100
                    elif metric.value <= 72:
                        score = 70
                    else:
                        score = 30
                else:
                    score = metric.value
                scores.append(score)
            overall_score = statistics.mean(scores)
        else:
            overall_score = 0.0
        
        # 统计问题数量
        critical_issues = sum(1 for m in quality_metrics if m.status == "critical")
        critical_issues += sum(1 for c in integrity_checks if c.status == "critical")
        
        warning_issues = sum(1 for m in quality_metrics if m.status == "warning")
        warning_issues += sum(1 for c in integrity_checks if c.status == "warning")
        
        # 创建报告
        report = QualityReport(
            report_id=report_id,
            timestamp=datetime.now(),
            overall_score=overall_score,
            quality_metrics=quality_metrics,
            integrity_checks=integrity_checks,
            recommendations=recommendations,
            critical_issues=critical_issues,
            warning_issues=warning_issues
        )
        
        # 保存报告
        self._save_quality_report(report)
        
        return report
    
    def _save_quality_report(self, report: QualityReport):
        """保存质量报告"""
        with sqlite3.connect(self.db_path) as conn:
            # 保存质量指标
            for metric in report.quality_metrics:
                conn.execute("""
                    INSERT INTO quality_metrics 
                    (metric_name, value, threshold, status, description, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    metric.metric_name, metric.value, metric.threshold,
                    metric.status, metric.description, metric.timestamp
                ))
            
            # 保存完整性检查
            for check in report.integrity_checks:
                conn.execute("""
                    INSERT INTO integrity_checks 
                    (check_type, table_name, expected_count, actual_count, 
                     missing_records, duplicate_records, status, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    check.check_type, check.table_name, check.expected_count,
                    check.actual_count, json.dumps(check.missing_records),
                    json.dumps(check.duplicate_records), check.status, check.timestamp
                ))
            
            # 保存报告摘要
            conn.execute("""
                INSERT INTO quality_reports 
                (report_id, overall_score, critical_issues, warning_issues, recommendations)
                VALUES (?, ?, ?, ?, ?)
            """, (
                report.report_id, report.overall_score, report.critical_issues,
                report.warning_issues, json.dumps(report.recommendations)
            ))
            
            conn.commit()
    
    def start_monitoring(self):
        """启动数据质量监控"""
        logger.info("启动数据质量监控服务")
        # 这里可以添加定期监控逻辑
        pass
    
    def stop_monitoring(self):
        """停止数据质量监控"""
        logger.info("停止数据质量监控服务")
        pass
    
    def run_all_checks(self) -> Dict[str, Any]:
        """运行所有数据质量检查"""
        logger.info("开始运行所有数据质量检查")
        
        results = {}
        
        try:
            # 运行完整的质量检查
            quality_report = self.run_quality_check()
            
            # 将QualityReport转换为字典格式
            results['overall_score'] = quality_report.overall_score
            results['status'] = quality_report.status
            results['total_checks'] = len(quality_report.quality_metrics)
            results['passed_checks'] = sum(1 for metric in quality_report.quality_metrics if metric.status == 'good')
            results['failed_checks'] = sum(1 for metric in quality_report.quality_metrics if metric.status == 'critical')
            results['warning_checks'] = sum(1 for metric in quality_report.quality_metrics if metric.status == 'warning')
            results['recommendations'] = quality_report.recommendations
            results['timestamp'] = quality_report.timestamp.isoformat()
            
            # 按检查类型分组结果
            check_types = {}
            for metric in quality_report.quality_metrics:
                check_type = metric.check_type
                if check_type not in check_types:
                    check_types[check_type] = {
                        'passed': 0,
                        'failed': 0,
                        'warnings': 0,
                        'total': 0
                    }
                
                check_types[check_type]['total'] += 1
                if metric.status == 'good':
                    check_types[check_type]['passed'] += 1
                elif metric.status == 'critical':
                    check_types[check_type]['failed'] += 1
                elif metric.status == 'warning':
                    check_types[check_type]['warnings'] += 1
            
            results['check_types'] = check_types
            results['passed'] = quality_report.status in ['good', 'warning']
            
            logger.info(f"数据质量检查完成，总体评分: {quality_report.overall_score:.2f}")
            
        except Exception as e:
            logger.error(f"运行数据质量检查时出错: {e}")
            results = {
                'overall_score': 0.0,
                'status': 'error',
                'error_message': str(e),
                'passed': False,
                'timestamp': datetime.now().isoformat()
            }
        
        return results
    
    def _monitoring_loop(self):
        """监控循环"""
        while self.monitoring_active:
            try:
                logger.info("开始数据质量检查...")
                report = self.run_quality_check()
                
                logger.info(f"质量检查完成 - 总体分数: {report.overall_score:.2f}%")
                logger.info(f"严重问题: {report.critical_issues}, 警告问题: {report.warning_issues}")
                
                # 如果有严重问题，记录详细信息
                if report.critical_issues > 0:
                    logger.warning(f"发现 {report.critical_issues} 个严重质量问题")
                    for rec in report.recommendations:
                        logger.warning(f"建议: {rec}")
                
            except Exception as e:
                logger.error(f"质量检查过程中出错: {e}")
            
            # 等待下次检查
            for _ in range(self.check_interval):
                if not self.monitoring_active:
                    break
                time.sleep(1)
    
    def get_latest_report(self) -> Optional[Dict]:
        """获取最新的质量报告"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 获取最新报告
                report_data = conn.execute("""
                    SELECT report_id, overall_score, critical_issues, 
                           warning_issues, recommendations, timestamp
                    FROM quality_reports 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """).fetchone()
                
                if not report_data:
                    return None
                
                report_id = report_data[0]
                
                # 获取相关的质量指标
                metrics = conn.execute("""
                    SELECT metric_name, value, status, description
                    FROM quality_metrics 
                    WHERE timestamp >= (
                        SELECT timestamp FROM quality_reports 
                        WHERE report_id = ?
                    )
                    ORDER BY timestamp DESC
                """, (report_id,)).fetchall()
                
                # 获取完整性检查
                checks = conn.execute("""
                    SELECT check_type, table_name, status, 
                           expected_count, actual_count
                    FROM integrity_checks 
                    WHERE timestamp >= (
                        SELECT timestamp FROM quality_reports 
                        WHERE report_id = ?
                    )
                    ORDER BY timestamp DESC
                """, (report_id,)).fetchall()
                
                return {
                    "report_id": report_id,
                    "timestamp": report_data[5],
                    "overall_score": report_data[1],
                    "critical_issues": report_data[2],
                    "warning_issues": report_data[3],
                    "recommendations": json.loads(report_data[4]) if report_data[4] else [],
                    "quality_metrics": [
                        {
                            "metric_name": m[0],
                            "value": m[1],
                            "status": m[2],
                            "description": m[3]
                        } for m in metrics
                    ],
                    "integrity_checks": [
                        {
                            "check_type": c[0],
                            "table_name": c[1],
                            "status": c[2],
                            "expected_count": c[3],
                            "actual_count": c[4]
                        } for c in checks
                    ]
                }
        
        except Exception as e:
            logger.error(f"获取最新报告时出错: {e}")
            return None
    
    def cleanup_old_data(self, days: int = 30):
        """清理旧数据"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM quality_metrics WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM integrity_checks WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM quality_reports WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM data_profiles WHERE timestamp < ?", (cutoff_date,))
            conn.commit()
        
        logger.info(f"已清理 {days} 天前的旧数据")

def main():
    """测试数据质量检查器"""
    print("=== 数据质量和完整性检查器测试 ===")
    
    # 创建检查器实例
    checker = DataQualityChecker()
    
    # 运行一次完整的质量检查
    print("\n运行质量检查...")
    report = checker.run_quality_check()
    
    print(f"\n质量报告 ID: {report.report_id}")
    print(f"总体分数: {report.overall_score:.2f}%")
    print(f"严重问题: {report.critical_issues}")
    print(f"警告问题: {report.warning_issues}")
    
    if report.recommendations:
        print("\n建议:")
        for i, rec in enumerate(report.recommendations, 1):
            print(f"{i}. {rec}")
    
    print("\n质量指标详情:")
    for metric in report.quality_metrics:
        print(f"- {metric.metric_name}: {metric.value:.2f} ({metric.status}) - {metric.description}")
    
    print("\n完整性检查详情:")
    for check in report.integrity_checks:
        print(f"- {check.table_name}: {check.status} - 记录数: {check.actual_count}")
        if check.duplicate_records:
            print(f"  重复记录: {len(check.duplicate_records)} 个")
    
    # 获取最新报告
    print("\n获取最新报告...")
    latest_report = checker.get_latest_report()
    if latest_report:
        print(f"最新报告时间: {latest_report['timestamp']}")
        print(f"总体分数: {latest_report['overall_score']:.2f}%")
    
    # 启动短期监控测试
    print("\n启动监控测试 (10秒)...")
    checker.check_interval = 5  # 5秒检查一次
    checker.start_monitoring()
    
    time.sleep(10)
    
    checker.stop_monitoring()
    
    print("\n=== 数据质量检查器测试完成 ===")

if __name__ == "__main__":
    main()