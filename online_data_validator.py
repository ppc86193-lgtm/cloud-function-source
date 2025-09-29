#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
线上数据检查和验证系统
对真实API数据进行完整性、准确性和实时性验证
"""

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
import statistics
from collections import defaultdict, Counter

# 导入相关系统
from real_api_data_system import RealAPIDataSystem, APIConfig, LotteryRecord
from data_deduplication_system import DataDeduplicationSystem

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ValidationRule:
    """验证规则"""
    rule_id: str
    rule_name: str
    rule_type: str  # 'field', 'business', 'consistency', 'timeliness'
    severity: str   # 'critical', 'warning', 'info'
    enabled: bool = True
    description: str = ""

@dataclass
class ValidationIssue:
    """验证问题"""
    issue_id: str
    rule_id: str
    severity: str
    message: str
    record_id: str
    field_name: Optional[str] = None
    expected_value: Optional[Any] = None
    actual_value: Optional[Any] = None
    timestamp: datetime = None

@dataclass
class ValidationReport:
    """验证报告"""
    report_id: str
    validation_time: datetime
    total_records: int
    passed_records: int
    failed_records: int
    critical_issues: int
    warning_issues: int
    info_issues: int
    success_rate: float
    issues: List[ValidationIssue]
    summary: Dict[str, Any]

@dataclass
class DataQualityMetrics:
    """数据质量指标"""
    completeness_score: float  # 完整性得分
    accuracy_score: float      # 准确性得分
    consistency_score: float   # 一致性得分
    timeliness_score: float    # 及时性得分
    overall_score: float       # 总体得分
    trend_analysis: Dict[str, float]  # 趋势分析

class OnlineDataValidator:
    """线上数据验证器"""
    
    def __init__(self, api_config: APIConfig, db_path: str = "validation.db"):
        self.api_config = api_config
        self.db_path = db_path
        self.lock = threading.RLock()
        
        # 初始化数据系统
        self.data_system = RealAPIDataSystem(api_config)
        
        # 验证规则
        self.validation_rules = self._init_validation_rules()
        
        # 统计信息
        self.validation_stats = {
            'total_validations': 0,
            'successful_validations': 0,
            'failed_validations': 0,
            'critical_issues_found': 0,
            'warning_issues_found': 0
        }
        
        self._init_database()
    
    def _init_database(self):
        """初始化验证数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS validation_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        report_id TEXT UNIQUE NOT NULL,
                        validation_time DATETIME NOT NULL,
                        total_records INTEGER NOT NULL,
                        passed_records INTEGER NOT NULL,
                        failed_records INTEGER NOT NULL,
                        critical_issues INTEGER NOT NULL,
                        warning_issues INTEGER NOT NULL,
                        info_issues INTEGER NOT NULL,
                        success_rate REAL NOT NULL,
                        summary TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS validation_issues (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        issue_id TEXT UNIQUE NOT NULL,
                        report_id TEXT NOT NULL,
                        rule_id TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        message TEXT NOT NULL,
                        record_id TEXT NOT NULL,
                        field_name TEXT,
                        expected_value TEXT,
                        actual_value TEXT,
                        timestamp DATETIME NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS quality_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        metric_time DATETIME NOT NULL,
                        completeness_score REAL NOT NULL,
                        accuracy_score REAL NOT NULL,
                        consistency_score REAL NOT NULL,
                        timeliness_score REAL NOT NULL,
                        overall_score REAL NOT NULL,
                        trend_data TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_report_time ON validation_reports(validation_time)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_issue_severity ON validation_issues(severity)
                """)
                
                conn.commit()
                logger.info("验证数据库初始化完成")
        except Exception as e:
            logger.error(f"验证数据库初始化失败: {e}")
            raise
    
    def _init_validation_rules(self) -> List[ValidationRule]:
        """初始化验证规则"""
        return [
            # 字段完整性规则
            ValidationRule(
                rule_id="field_draw_id_required",
                rule_name="开奖ID必填",
                rule_type="field",
                severity="critical",
                description="draw_id字段不能为空"
            ),
            ValidationRule(
                rule_id="field_numbers_required",
                rule_name="开奖号码必填",
                rule_type="field",
                severity="critical",
                description="numbers字段不能为空且必须是有效数组"
            ),
            ValidationRule(
                rule_id="field_timestamp_required",
                rule_name="时间戳必填",
                rule_type="field",
                severity="critical",
                description="timestamp字段不能为空"
            ),
            
            # 业务逻辑规则
            ValidationRule(
                rule_id="business_numbers_count",
                rule_name="开奖号码数量检查",
                rule_type="business",
                severity="critical",
                description="开奖号码必须是3个数字"
            ),
            ValidationRule(
                rule_id="business_numbers_range",
                rule_name="开奖号码范围检查",
                rule_type="business",
                severity="critical",
                description="开奖号码必须在0-27范围内"
            ),
            ValidationRule(
                rule_id="business_sum_calculation",
                rule_name="和值计算检查",
                rule_type="business",
                severity="warning",
                description="和值必须等于三个号码之和"
            ),
            ValidationRule(
                rule_id="business_big_small_logic",
                rule_name="大小判断逻辑",
                rule_type="business",
                severity="warning",
                description="大小判断必须符合PC28规则"
            ),
            ValidationRule(
                rule_id="business_odd_even_logic",
                rule_name="单双判断逻辑",
                rule_type="business",
                severity="warning",
                description="单双判断必须符合PC28规则"
            ),
            
            # 一致性规则
            ValidationRule(
                rule_id="consistency_duplicate_check",
                rule_name="重复记录检查",
                rule_type="consistency",
                severity="warning",
                description="不应存在重复的开奖记录"
            ),
            ValidationRule(
                rule_id="consistency_sequence_check",
                rule_name="期号连续性检查",
                rule_type="consistency",
                severity="info",
                description="期号应该连续递增"
            ),
            
            # 及时性规则
            ValidationRule(
                rule_id="timeliness_data_freshness",
                rule_name="数据新鲜度检查",
                rule_type="timeliness",
                severity="warning",
                description="数据应该及时更新"
            ),
            ValidationRule(
                rule_id="timeliness_api_response",
                rule_name="API响应时间检查",
                rule_type="timeliness",
                severity="info",
                description="API响应时间应在合理范围内"
            )
        ]
    
    def validate_record(self, record: LotteryRecord) -> List[ValidationIssue]:
        """验证单条记录"""
        issues = []
        
        for rule in self.validation_rules:
            if not rule.enabled:
                continue
            
            try:
                issue = self._apply_validation_rule(record, rule)
                if issue:
                    issues.append(issue)
            except Exception as e:
                logger.error(f"应用验证规则失败 {rule.rule_id}: {e}")
        
        return issues
    
    def _apply_validation_rule(self, record: LotteryRecord, rule: ValidationRule) -> Optional[ValidationIssue]:
        """应用单个验证规则"""
        try:
            if rule.rule_id == "field_draw_id_required":
                if not record.draw_id or record.draw_id.strip() == "":
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message="开奖ID不能为空",
                        record_id=record.draw_id or "unknown",
                        field_name="draw_id",
                        expected_value="非空字符串",
                        actual_value=record.draw_id,
                        timestamp=datetime.now()
                    )
            
            elif rule.rule_id == "field_numbers_required":
                if not record.numbers or not isinstance(record.numbers, list) or len(record.numbers) == 0:
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message="开奖号码不能为空",
                        record_id=record.draw_id,
                        field_name="numbers",
                        expected_value="非空数组",
                        actual_value=record.numbers,
                        timestamp=datetime.now()
                    )
            
            elif rule.rule_id == "field_timestamp_required":
                if not record.timestamp:
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message="时间戳不能为空",
                        record_id=record.draw_id,
                        field_name="timestamp",
                        expected_value="有效时间戳",
                        actual_value=record.timestamp,
                        timestamp=datetime.now()
                    )
            
            elif rule.rule_id == "business_numbers_count":
                if len(record.numbers) != 3:
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message=f"开奖号码数量错误，应为3个，实际为{len(record.numbers)}个",
                        record_id=record.draw_id,
                        field_name="numbers",
                        expected_value=3,
                        actual_value=len(record.numbers),
                        timestamp=datetime.now()
                    )
            
            elif rule.rule_id == "business_numbers_range":
                for i, num in enumerate(record.numbers):
                    if not (0 <= num <= 27):
                        return ValidationIssue(
                            issue_id=f"{rule.rule_id}_{record.draw_id}_{i}_{int(time.time())}",
                            rule_id=rule.rule_id,
                            severity=rule.severity,
                            message=f"开奖号码{num}超出范围[0-27]",
                            record_id=record.draw_id,
                            field_name=f"numbers[{i}]",
                            expected_value="0-27",
                            actual_value=num,
                            timestamp=datetime.now()
                        )
            
            elif rule.rule_id == "business_sum_calculation":
                expected_sum = sum(record.numbers)
                if record.sum_value != expected_sum:
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message=f"和值计算错误",
                        record_id=record.draw_id,
                        field_name="sum_value",
                        expected_value=expected_sum,
                        actual_value=record.sum_value,
                        timestamp=datetime.now()
                    )
            
            elif rule.rule_id == "business_big_small_logic":
                expected_big_small = 'big' if record.sum_value >= 14 else 'small'
                if record.big_small != expected_big_small:
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message=f"大小判断错误",
                        record_id=record.draw_id,
                        field_name="big_small",
                        expected_value=expected_big_small,
                        actual_value=record.big_small,
                        timestamp=datetime.now()
                    )
            
            elif rule.rule_id == "business_odd_even_logic":
                expected_odd_even = 'odd' if record.sum_value % 2 == 1 else 'even'
                if record.odd_even != expected_odd_even:
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message=f"单双判断错误",
                        record_id=record.draw_id,
                        field_name="odd_even",
                        expected_value=expected_odd_even,
                        actual_value=record.odd_even,
                        timestamp=datetime.now()
                    )
            
            elif rule.rule_id == "timeliness_data_freshness":
                # 检查数据是否过于陈旧（超过1小时）
                if record.timestamp and (datetime.now() - record.timestamp).total_seconds() > 3600:
                    return ValidationIssue(
                        issue_id=f"{rule.rule_id}_{record.draw_id}_{int(time.time())}",
                        rule_id=rule.rule_id,
                        severity=rule.severity,
                        message=f"数据过于陈旧",
                        record_id=record.draw_id,
                        field_name="timestamp",
                        expected_value="1小时内",
                        actual_value=record.timestamp,
                        timestamp=datetime.now()
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"验证规则执行失败 {rule.rule_id}: {e}")
            return None
    
    def validate_batch_records(self, records: List[LotteryRecord]) -> ValidationReport:
        """批量验证记录"""
        try:
            self.validation_stats['total_validations'] += 1
            
            report_id = f"validation_{int(time.time())}"
            validation_time = datetime.now()
            
            all_issues = []
            passed_records = 0
            failed_records = 0
            
            # 验证每条记录
            for record in records:
                issues = self.validate_record(record)
                
                if issues:
                    failed_records += 1
                    all_issues.extend(issues)
                else:
                    passed_records += 1
            
            # 批量一致性检查
            consistency_issues = self._check_batch_consistency(records)
            all_issues.extend(consistency_issues)
            
            # 统计问题
            critical_issues = sum(1 for issue in all_issues if issue.severity == 'critical')
            warning_issues = sum(1 for issue in all_issues if issue.severity == 'warning')
            info_issues = sum(1 for issue in all_issues if issue.severity == 'info')
            
            # 计算成功率
            success_rate = (passed_records / len(records)) * 100 if records else 0
            
            # 生成摘要
            summary = {
                'validation_rules_applied': len([r for r in self.validation_rules if r.enabled]),
                'issue_distribution': {
                    'critical': critical_issues,
                    'warning': warning_issues,
                    'info': info_issues
                },
                'top_issues': self._get_top_issues(all_issues),
                'recommendations': self._generate_recommendations(all_issues)
            }
            
            # 创建报告
            report = ValidationReport(
                report_id=report_id,
                validation_time=validation_time,
                total_records=len(records),
                passed_records=passed_records,
                failed_records=failed_records,
                critical_issues=critical_issues,
                warning_issues=warning_issues,
                info_issues=info_issues,
                success_rate=success_rate,
                issues=all_issues,
                summary=summary
            )
            
            # 保存报告
            self._save_validation_report(report)
            
            # 更新统计
            if success_rate >= 90:
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            self.validation_stats['critical_issues_found'] += critical_issues
            self.validation_stats['warning_issues_found'] += warning_issues
            
            return report
            
        except Exception as e:
            logger.error(f"批量验证失败: {e}")
            self.validation_stats['failed_validations'] += 1
            raise
    
    def _check_batch_consistency(self, records: List[LotteryRecord]) -> List[ValidationIssue]:
        """检查批量数据一致性"""
        issues = []
        
        try:
            # 检查重复记录
            draw_ids = [record.draw_id for record in records]
            duplicates = [item for item, count in Counter(draw_ids).items() if count > 1]
            
            for duplicate_id in duplicates:
                issues.append(ValidationIssue(
                    issue_id=f"consistency_duplicate_{duplicate_id}_{int(time.time())}",
                    rule_id="consistency_duplicate_check",
                    severity="warning",
                    message=f"发现重复的开奖ID: {duplicate_id}",
                    record_id=duplicate_id,
                    timestamp=datetime.now()
                ))
            
            # 检查期号连续性（如果有期号信息）
            issues_with_numbers = [record for record in records if record.issue.isdigit()]
            if len(issues_with_numbers) > 1:
                issues_with_numbers.sort(key=lambda x: int(x.issue))
                
                for i in range(1, len(issues_with_numbers)):
                    prev_issue = int(issues_with_numbers[i-1].issue)
                    curr_issue = int(issues_with_numbers[i].issue)
                    
                    if curr_issue != prev_issue + 1:
                        issues.append(ValidationIssue(
                            issue_id=f"consistency_sequence_{curr_issue}_{int(time.time())}",
                            rule_id="consistency_sequence_check",
                            severity="info",
                            message=f"期号不连续: {prev_issue} -> {curr_issue}",
                            record_id=issues_with_numbers[i].draw_id,
                            timestamp=datetime.now()
                        ))
            
        except Exception as e:
            logger.error(f"一致性检查失败: {e}")
        
        return issues
    
    def _get_top_issues(self, issues: List[ValidationIssue], limit: int = 5) -> List[Dict]:
        """获取主要问题"""
        try:
            # 按规则ID分组统计
            issue_counts = Counter(issue.rule_id for issue in issues)
            
            top_issues = []
            for rule_id, count in issue_counts.most_common(limit):
                rule = next((r for r in self.validation_rules if r.rule_id == rule_id), None)
                top_issues.append({
                    'rule_id': rule_id,
                    'rule_name': rule.rule_name if rule else rule_id,
                    'count': count,
                    'severity': rule.severity if rule else 'unknown'
                })
            
            return top_issues
        except Exception as e:
            logger.error(f"获取主要问题失败: {e}")
            return []
    
    def _generate_recommendations(self, issues: List[ValidationIssue]) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        try:
            # 统计问题类型
            critical_count = sum(1 for issue in issues if issue.severity == 'critical')
            warning_count = sum(1 for issue in issues if issue.severity == 'warning')
            
            if critical_count > 0:
                recommendations.append(f"发现{critical_count}个严重问题，需要立即修复")
            
            if warning_count > 0:
                recommendations.append(f"发现{warning_count}个警告问题，建议尽快处理")
            
            # 按问题类型给出具体建议
            issue_types = Counter(issue.rule_id for issue in issues)
            
            if 'field_draw_id_required' in issue_types:
                recommendations.append("检查API数据源，确保开奖ID字段完整")
            
            if 'business_numbers_count' in issue_types:
                recommendations.append("验证开奖号码解析逻辑，确保正确提取3个号码")
            
            if 'business_sum_calculation' in issue_types:
                recommendations.append("检查和值计算逻辑，确保与开奖号码一致")
            
            if 'timeliness_data_freshness' in issue_types:
                recommendations.append("增加数据获取频率，确保数据及时性")
            
            if not recommendations:
                recommendations.append("数据质量良好，继续保持")
            
        except Exception as e:
            logger.error(f"生成建议失败: {e}")
            recommendations.append("无法生成建议，请检查系统状态")
        
        return recommendations
    
    def _save_validation_report(self, report: ValidationReport):
        """保存验证报告"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 保存报告
                conn.execute("""
                    INSERT INTO validation_reports 
                    (report_id, validation_time, total_records, passed_records, failed_records,
                     critical_issues, warning_issues, info_issues, success_rate, summary)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    report.report_id,
                    report.validation_time,
                    report.total_records,
                    report.passed_records,
                    report.failed_records,
                    report.critical_issues,
                    report.warning_issues,
                    report.info_issues,
                    report.success_rate,
                    json.dumps(report.summary, ensure_ascii=False)
                ))
                
                # 保存问题详情
                for issue in report.issues:
                    conn.execute("""
                        INSERT INTO validation_issues 
                        (issue_id, report_id, rule_id, severity, message, record_id,
                         field_name, expected_value, actual_value, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        issue.issue_id,
                        report.report_id,
                        issue.rule_id,
                        issue.severity,
                        issue.message,
                        issue.record_id,
                        issue.field_name,
                        str(issue.expected_value) if issue.expected_value is not None else None,
                        str(issue.actual_value) if issue.actual_value is not None else None,
                        issue.timestamp
                    ))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存验证报告失败: {e}")
    
    def calculate_quality_metrics(self, days: int = 7) -> DataQualityMetrics:
        """计算数据质量指标"""
        try:
            cutoff_time = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                # 获取最近的验证报告
                cursor = conn.execute("""
                    SELECT success_rate, critical_issues, warning_issues, total_records
                    FROM validation_reports 
                    WHERE validation_time > ?
                    ORDER BY validation_time DESC
                """, (cutoff_time,))
                
                reports = cursor.fetchall()
                
                if not reports:
                    return DataQualityMetrics(
                        completeness_score=0.0,
                        accuracy_score=0.0,
                        consistency_score=0.0,
                        timeliness_score=0.0,
                        overall_score=0.0,
                        trend_analysis={}
                    )
                
                # 计算各项指标
                success_rates = [report[0] for report in reports]
                completeness_score = statistics.mean(success_rates)
                
                # 准确性得分（基于严重问题比例）
                total_records = sum(report[3] for report in reports)
                total_critical = sum(report[1] for report in reports)
                accuracy_score = max(0, 100 - (total_critical / max(total_records, 1)) * 100)
                
                # 一致性得分（基于警告问题比例）
                total_warnings = sum(report[2] for report in reports)
                consistency_score = max(0, 100 - (total_warnings / max(total_records, 1)) * 100)
                
                # 及时性得分（基于最近数据的新鲜度）
                timeliness_score = self._calculate_timeliness_score()
                
                # 总体得分
                overall_score = (completeness_score + accuracy_score + consistency_score + timeliness_score) / 4
                
                # 趋势分析
                trend_analysis = self._analyze_quality_trends(reports)
                
                metrics = DataQualityMetrics(
                    completeness_score=round(completeness_score, 2),
                    accuracy_score=round(accuracy_score, 2),
                    consistency_score=round(consistency_score, 2),
                    timeliness_score=round(timeliness_score, 2),
                    overall_score=round(overall_score, 2),
                    trend_analysis=trend_analysis
                )
                
                # 保存指标
                self._save_quality_metrics(metrics)
                
                return metrics
                
        except Exception as e:
            logger.error(f"计算质量指标失败: {e}")
            return DataQualityMetrics(
                completeness_score=0.0,
                accuracy_score=0.0,
                consistency_score=0.0,
                timeliness_score=0.0,
                overall_score=0.0,
                trend_analysis={}
            )
    
    def _calculate_timeliness_score(self) -> float:
        """计算及时性得分"""
        try:
            # 检查最新数据的时间戳
            with sqlite3.connect(self.data_system.db_path) as conn:
                cursor = conn.execute("""
                    SELECT MAX(timestamp) FROM lottery_records
                """)
                latest_time = cursor.fetchone()[0]
                
                if latest_time:
                    latest_dt = datetime.fromisoformat(latest_time)
                    time_diff = (datetime.now() - latest_dt).total_seconds()
                    
                    # 根据时间差计算得分
                    if time_diff <= 300:  # 5分钟内
                        return 100.0
                    elif time_diff <= 900:  # 15分钟内
                        return 80.0
                    elif time_diff <= 1800:  # 30分钟内
                        return 60.0
                    elif time_diff <= 3600:  # 1小时内
                        return 40.0
                    else:
                        return 20.0
                else:
                    return 0.0
                    
        except Exception as e:
            logger.error(f"计算及时性得分失败: {e}")
            return 0.0
    
    def _analyze_quality_trends(self, reports: List[Tuple]) -> Dict[str, float]:
        """分析质量趋势"""
        try:
            if len(reports) < 2:
                return {}
            
            # 计算成功率趋势
            recent_success = statistics.mean([r[0] for r in reports[:3]])  # 最近3次
            older_success = statistics.mean([r[0] for r in reports[-3:]])  # 较早3次
            
            success_trend = recent_success - older_success
            
            return {
                'success_rate_trend': round(success_trend, 2),
                'trend_direction': 'improving' if success_trend > 0 else 'declining' if success_trend < 0 else 'stable'
            }
            
        except Exception as e:
            logger.error(f"分析质量趋势失败: {e}")
            return {}
    
    def _save_quality_metrics(self, metrics: DataQualityMetrics):
        """保存质量指标"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO quality_metrics 
                    (metric_time, completeness_score, accuracy_score, consistency_score,
                     timeliness_score, overall_score, trend_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now(),
                    metrics.completeness_score,
                    metrics.accuracy_score,
                    metrics.consistency_score,
                    metrics.timeliness_score,
                    metrics.overall_score,
                    json.dumps(metrics.trend_analysis, ensure_ascii=False)
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存质量指标失败: {e}")
    
    def run_online_validation(self) -> ValidationReport:
        """运行线上数据验证"""
        try:
            logger.info("开始线上数据验证...")
            
            # 获取最新数据
            records = self.data_system.get_current_lottery_data()
            
            if not records:
                logger.warning("未获取到数据，无法进行验证")
                return None
            
            # 执行验证
            report = self.validate_batch_records(records)
            
            logger.info(f"验证完成: 总记录{report.total_records}, 成功率{report.success_rate:.2f}%")
            
            return report
            
        except Exception as e:
            logger.error(f"线上数据验证失败: {e}")
            return None
    
    def get_validation_summary(self, days: int = 7) -> Dict[str, Any]:
        """获取验证摘要"""
        try:
            quality_metrics = self.calculate_quality_metrics(days)
            
            cutoff_time = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                # 获取最近的报告统计
                cursor = conn.execute("""
                    SELECT COUNT(*), AVG(success_rate), SUM(critical_issues), SUM(warning_issues)
                    FROM validation_reports 
                    WHERE validation_time > ?
                """, (cutoff_time,))
                
                stats = cursor.fetchone()
                
                return {
                    'period_days': days,
                    'quality_metrics': asdict(quality_metrics),
                    'validation_stats': {
                        'total_validations': stats[0] or 0,
                        'average_success_rate': round(stats[1] or 0, 2),
                        'total_critical_issues': stats[2] or 0,
                        'total_warning_issues': stats[3] or 0
                    },
                    'system_stats': self.validation_stats.copy()
                }
                
        except Exception as e:
            logger.error(f"获取验证摘要失败: {e}")
            return {}

def main():
    """测试线上数据验证系统"""
    print("=== 线上数据验证系统测试 ===")
    
    # 配置API（使用示例配置）
    api_config = APIConfig(
        appid="test_appid",
        secret_key="test_secret_key_32_characters_long"
    )
    
    # 初始化验证器
    validator = OnlineDataValidator(api_config)
    
    print("\n注意: 使用测试配置，实际使用时请替换为真实的API配置")
    
    # 创建测试数据
    test_records = [
        LotteryRecord(
            draw_id="PC28_20241219_001",
            issue="001",
            numbers=[8, 15, 22],
            sum_value=45,
            big_small="big",
            odd_even="odd",
            dragon_tiger="tiger",
            timestamp=datetime.now()
        ),
        LotteryRecord(
            draw_id="PC28_20241219_002",
            issue="002",
            numbers=[3, 12, 28],  # 28超出范围，应该报错
            sum_value=43,
            big_small="big",
            odd_even="odd",
            dragon_tiger="dragon",
            timestamp=datetime.now()
        ),
        LotteryRecord(
            draw_id="PC28_20241219_003",
            issue="003",
            numbers=[5, 10, 15],
            sum_value=25,  # 错误的和值，应该是30
            big_small="big",
            odd_even="even",
            dragon_tiger="tiger",
            timestamp=datetime.now()
        )
    ]
    
    print("\n1. 测试批量验证:")
    report = validator.validate_batch_records(test_records)
    
    print(f"验证报告ID: {report.report_id}")
    print(f"总记录数: {report.total_records}")
    print(f"通过记录: {report.passed_records}")
    print(f"失败记录: {report.failed_records}")
    print(f"成功率: {report.success_rate:.2f}%")
    print(f"严重问题: {report.critical_issues}")
    print(f"警告问题: {report.warning_issues}")
    
    print("\n2. 问题详情:")
    for issue in report.issues[:5]:  # 显示前5个问题
        print(f"- {issue.severity.upper()}: {issue.message} (记录: {issue.record_id})")
    
    print("\n3. 质量指标:")
    metrics = validator.calculate_quality_metrics()
    print(f"完整性得分: {metrics.completeness_score}")
    print(f"准确性得分: {metrics.accuracy_score}")
    print(f"一致性得分: {metrics.consistency_score}")
    print(f"及时性得分: {metrics.timeliness_score}")
    print(f"总体得分: {metrics.overall_score}")
    
    print("\n4. 验证摘要:")
    summary = validator.get_validation_summary()
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    
    print("\n=== 线上数据验证系统测试完成 ===")

if __name__ == "__main__":
    main()