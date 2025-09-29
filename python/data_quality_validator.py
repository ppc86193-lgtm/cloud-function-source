#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28数据质量验证器
负责数据质量检查、字段完整性验证和数据一致性校验
"""

import json
import re
import logging
import threading
import fcntl
import time
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from api_field_optimization import OptimizedLotteryData, OptimizedPC28DataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ValidationSeverity(Enum):
    """验证问题严重程度"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ValidationIssue:
    """验证问题"""
    field_name: str
    issue_type: str
    severity: ValidationSeverity
    message: str
    expected_value: Any = None
    actual_value: Any = None
    suggestion: str = ""

@dataclass
class ValidationResult:
    """验证结果"""
    draw_id: str
    timestamp: str
    is_valid: bool
    issues: List[ValidationIssue]
    completeness_score: float  # 0-100
    quality_score: float      # 0-100
    overall_score: float      # 0-100

@dataclass
class QualityReport:
    """质量报告"""
    report_timestamp: str
    total_records: int
    valid_records: int
    invalid_records: int
    validation_results: List[ValidationResult]
    field_completeness: Dict[str, float]
    common_issues: Dict[str, int]
    quality_trends: Dict[str, Any]
    recommendations: List[str]

class DataQualityValidator:
    """数据质量验证器"""
    
    def __init__(self):
        self.data_processor = OptimizedPC28DataProcessor()
        
        # 锁机制
        self._validation_lock = threading.RLock()  # 可重入锁，防止死锁
        self._batch_lock = threading.Lock()        # 批量处理锁
        self._report_lock = threading.Lock()       # 报告生成锁
        self._file_lock = threading.Lock()         # 文件操作锁
        self._lock_timeout = 30                    # 锁超时时间（秒）
        
        # 验证规则配置
        self.validation_rules = {
            'draw_id': {
                'required': True,
                'type': str,
                'pattern': r'^\d+$',
                'min_length': 6,
                'max_length': 10
            },
            'numbers': {
                'required': True,
                'type': list,
                'length': 3,
                'element_type': int,
                'element_range': (0, 9)
            },
            'result_sum': {
                'required': True,
                'type': int,
                'range': (0, 27)  # 3个数字，每个0-9，最大和为27
            },
            'timestamp': {
                'required': True,
                'type': str,
                'pattern': r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            },
            'big_small': {
                'required': False,
                'type': str,
                'allowed_values': ['大', '小']
            },
            'odd_even': {
                'required': False,
                'type': str,
                'allowed_values': ['单', '双']
            },
            'dragon_tiger': {
                'required': False,
                'type': str,
                'allowed_values': ['龙', '虎', '和']
            }
        }
        
        # 中国时区
        self.china_tz = timezone(timedelta(hours=8))
        
        # 统计信息
        self.validation_stats = {
            'total_validations': 0,
            'successful_validations': 0,
            'failed_validations': 0,
            'common_issues': {},
            'field_error_counts': {}
        }
    
    def validate_single_record(self, data: OptimizedLotteryData) -> ValidationResult:
        """验证单条记录"""
        # 使用锁保护单条记录验证
        if not self._validation_lock.acquire(timeout=self._lock_timeout):
            logger.error(f"获取验证锁超时，记录ID: {data.draw_id}")
            return ValidationResult(
                draw_id=data.draw_id,
                timestamp=datetime.now(self.china_tz).isoformat(),
                is_valid=False,
                issues=[ValidationIssue(
                    field_name="system",
                    issue_type="lock_timeout",
                    severity=ValidationSeverity.CRITICAL,
                    message="获取验证锁超时，可能存在并发冲突"
                )],
                completeness_score=0.0,
                quality_score=0.0,
                overall_score=0.0
            )
        
        try:
            issues = []
            
            # 转换为字典便于验证
            data_dict = asdict(data)
            
            # 字段完整性验证
            completeness_issues = self._validate_field_completeness(data_dict)
            issues.extend(completeness_issues)
            
            # 数据类型验证
            type_issues = self._validate_data_types(data_dict)
            issues.extend(type_issues)
            
            # 数据范围验证
            range_issues = self._validate_data_ranges(data_dict)
            issues.extend(range_issues)
            
            # 数据格式验证
            format_issues = self._validate_data_formats(data_dict)
            issues.extend(format_issues)
            
            # 业务逻辑验证
            logic_issues = self._validate_business_logic(data_dict)
            issues.extend(logic_issues)
            
            # 计算评分
            completeness_score = self._calculate_completeness_score(data_dict, issues)
            quality_score = self._calculate_quality_score(issues)
            overall_score = (completeness_score + quality_score) / 2
            
            # 更新统计
            self.validation_stats['total_validations'] += 1
            if not issues or all(issue.severity in [ValidationSeverity.INFO, ValidationSeverity.WARNING] for issue in issues):
                self.validation_stats['successful_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            # 统计常见问题
            for issue in issues:
                issue_key = f"{issue.field_name}_{issue.issue_type}"
                self.validation_stats['common_issues'][issue_key] = self.validation_stats['common_issues'].get(issue_key, 0) + 1
                self.validation_stats['field_error_counts'][issue.field_name] = self.validation_stats['field_error_counts'].get(issue.field_name, 0) + 1
            
            return ValidationResult(
                draw_id=data.draw_id,
                timestamp=datetime.now(self.china_tz).isoformat(),
                is_valid=len([i for i in issues if i.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]]) == 0,
                issues=issues,
                completeness_score=completeness_score,
                quality_score=quality_score,
                overall_score=overall_score
            )
        except Exception as e:
            logger.error(f"验证记录 {data.draw_id} 时发生异常: {e}")
            return ValidationResult(
                draw_id=data.draw_id,
                timestamp=datetime.now(self.china_tz).isoformat(),
                is_valid=False,
                issues=[ValidationIssue(
                    field_name="system",
                    issue_type="validation_exception",
                    severity=ValidationSeverity.CRITICAL,
                    message=f"验证过程中发生异常: {str(e)}"
                )],
                completeness_score=0.0,
                quality_score=0.0,
                overall_score=0.0
            )
        finally:
            # 确保锁被释放
            self._validation_lock.release()
    
    def validate_batch_records(self, data_list: List[OptimizedLotteryData]) -> List[ValidationResult]:
        """批量验证记录"""
        # 使用批量处理锁
        if not self._batch_lock.acquire(timeout=self._lock_timeout):
            logger.error("获取批量验证锁超时")
            return [ValidationResult(
                draw_id=getattr(data, 'draw_id', 'unknown'),
                timestamp=datetime.now(self.china_tz).isoformat(),
                is_valid=False,
                issues=[ValidationIssue(
                    field_name="system",
                    issue_type="batch_lock_timeout",
                    severity=ValidationSeverity.CRITICAL,
                    message="获取批量验证锁超时，可能存在并发冲突"
                )],
                completeness_score=0.0,
                quality_score=0.0,
                overall_score=0.0
            ) for data in data_list]
        
        try:
            results = []
            
            for data in data_list:
                try:
                    result = self.validate_single_record(data)
                    results.append(result)
                except Exception as e:
                    logger.error(f"验证记录 {data.draw_id} 时发生异常: {e}")
                    # 创建错误结果
                    error_result = ValidationResult(
                        draw_id=data.draw_id,
                        timestamp=datetime.now(self.china_tz).isoformat(),
                        is_valid=False,
                        issues=[ValidationIssue(
                            field_name="system",
                            issue_type="validation_error",
                            severity=ValidationSeverity.CRITICAL,
                            message=f"验证过程中发生异常: {str(e)}"
                        )],
                        completeness_score=0.0,
                        quality_score=0.0,
                        overall_score=0.0
                    )
                    results.append(error_result)
            
            return results
        finally:
            # 确保批量锁被释放
            self._batch_lock.release()
    
    def _validate_field_completeness(self, data: Dict[str, Any]) -> List[ValidationIssue]:
        """验证字段完整性"""
        issues = []
        
        for field_name, rules in self.validation_rules.items():
            if rules.get('required', False):
                if field_name not in data or data[field_name] is None:
                    issues.append(ValidationIssue(
                        field_name=field_name,
                        issue_type="missing_required_field",
                        severity=ValidationSeverity.ERROR,
                        message=f"缺少必需字段: {field_name}",
                        suggestion=f"请确保字段 {field_name} 存在且不为空"
                    ))
                elif data[field_name] == "" or (isinstance(data[field_name], list) and len(data[field_name]) == 0):
                    issues.append(ValidationIssue(
                        field_name=field_name,
                        issue_type="empty_required_field",
                        severity=ValidationSeverity.ERROR,
                        message=f"必需字段为空: {field_name}",
                        actual_value=data[field_name],
                        suggestion=f"请为字段 {field_name} 提供有效值"
                    ))
        
        return issues
    
    def _validate_data_types(self, data: Dict[str, Any]) -> List[ValidationIssue]:
        """验证数据类型"""
        issues = []
        
        for field_name, rules in self.validation_rules.items():
            if field_name in data and data[field_name] is not None:
                expected_type = rules.get('type')
                actual_value = data[field_name]
                
                if expected_type and not isinstance(actual_value, expected_type):
                    issues.append(ValidationIssue(
                        field_name=field_name,
                        issue_type="invalid_data_type",
                        severity=ValidationSeverity.ERROR,
                        message=f"数据类型错误: {field_name}",
                        expected_value=expected_type.__name__,
                        actual_value=type(actual_value).__name__,
                        suggestion=f"字段 {field_name} 应为 {expected_type.__name__} 类型"
                    ))
                
                # 验证列表元素类型
                if expected_type == list and isinstance(actual_value, list):
                    element_type = rules.get('element_type')
                    if element_type:
                        for i, element in enumerate(actual_value):
                            if not isinstance(element, element_type):
                                issues.append(ValidationIssue(
                                    field_name=f"{field_name}[{i}]",
                                    issue_type="invalid_element_type",
                                    severity=ValidationSeverity.ERROR,
                                    message=f"列表元素类型错误: {field_name}[{i}]",
                                    expected_value=element_type.__name__,
                                    actual_value=type(element).__name__,
                                    suggestion=f"列表 {field_name} 的元素应为 {element_type.__name__} 类型"
                                ))
        
        return issues
    
    def _validate_data_ranges(self, data: Dict[str, Any]) -> List[ValidationIssue]:
        """验证数据范围"""
        issues = []
        
        for field_name, rules in self.validation_rules.items():
            if field_name in data and data[field_name] is not None:
                actual_value = data[field_name]
                
                # 验证数值范围
                if 'range' in rules and isinstance(actual_value, (int, float)):
                    min_val, max_val = rules['range']
                    if not (min_val <= actual_value <= max_val):
                        issues.append(ValidationIssue(
                            field_name=field_name,
                            issue_type="value_out_of_range",
                            severity=ValidationSeverity.ERROR,
                            message=f"数值超出范围: {field_name}",
                            expected_value=f"[{min_val}, {max_val}]",
                            actual_value=actual_value,
                            suggestion=f"字段 {field_name} 的值应在 {min_val} 到 {max_val} 之间"
                        ))
                
                # 验证字符串长度
                if isinstance(actual_value, str):
                    if 'min_length' in rules and len(actual_value) < rules['min_length']:
                        issues.append(ValidationIssue(
                            field_name=field_name,
                            issue_type="string_too_short",
                            severity=ValidationSeverity.WARNING,
                            message=f"字符串长度不足: {field_name}",
                            expected_value=f"最少 {rules['min_length']} 字符",
                            actual_value=f"{len(actual_value)} 字符",
                            suggestion=f"字段 {field_name} 长度应至少为 {rules['min_length']} 字符"
                        ))
                    
                    if 'max_length' in rules and len(actual_value) > rules['max_length']:
                        issues.append(ValidationIssue(
                            field_name=field_name,
                            issue_type="string_too_long",
                            severity=ValidationSeverity.WARNING,
                            message=f"字符串长度超限: {field_name}",
                            expected_value=f"最多 {rules['max_length']} 字符",
                            actual_value=f"{len(actual_value)} 字符",
                            suggestion=f"字段 {field_name} 长度应不超过 {rules['max_length']} 字符"
                        ))
                
                # 验证列表长度
                if isinstance(actual_value, list) and 'length' in rules:
                    expected_length = rules['length']
                    if len(actual_value) != expected_length:
                        issues.append(ValidationIssue(
                            field_name=field_name,
                            issue_type="invalid_list_length",
                            severity=ValidationSeverity.ERROR,
                            message=f"列表长度错误: {field_name}",
                            expected_value=expected_length,
                            actual_value=len(actual_value),
                            suggestion=f"列表 {field_name} 应包含 {expected_length} 个元素"
                        ))
                
                # 验证列表元素范围
                if isinstance(actual_value, list) and 'element_range' in rules:
                    min_val, max_val = rules['element_range']
                    for i, element in enumerate(actual_value):
                        if isinstance(element, (int, float)) and not (min_val <= element <= max_val):
                            issues.append(ValidationIssue(
                                field_name=f"{field_name}[{i}]",
                                issue_type="element_out_of_range",
                                severity=ValidationSeverity.ERROR,
                                message=f"列表元素超出范围: {field_name}[{i}]",
                                expected_value=f"[{min_val}, {max_val}]",
                                actual_value=element,
                                suggestion=f"列表 {field_name} 的元素值应在 {min_val} 到 {max_val} 之间"
                            ))
                
                # 验证允许值
                if 'allowed_values' in rules:
                    allowed_values = rules['allowed_values']
                    if actual_value not in allowed_values:
                        issues.append(ValidationIssue(
                            field_name=field_name,
                            issue_type="invalid_value",
                            severity=ValidationSeverity.ERROR,
                            message=f"值不在允许范围内: {field_name}",
                            expected_value=allowed_values,
                            actual_value=actual_value,
                            suggestion=f"字段 {field_name} 的值应为: {', '.join(map(str, allowed_values))}"
                        ))
        
        return issues
    
    def _validate_data_formats(self, data: Dict[str, Any]) -> List[ValidationIssue]:
        """验证数据格式"""
        issues = []
        
        for field_name, rules in self.validation_rules.items():
            if field_name in data and data[field_name] is not None:
                actual_value = data[field_name]
                
                # 验证正则表达式模式
                if 'pattern' in rules and isinstance(actual_value, str):
                    pattern = rules['pattern']
                    if not re.match(pattern, actual_value):
                        issues.append(ValidationIssue(
                            field_name=field_name,
                            issue_type="invalid_format",
                            severity=ValidationSeverity.ERROR,
                            message=f"格式不正确: {field_name}",
                            expected_value=f"匹配模式: {pattern}",
                            actual_value=actual_value,
                            suggestion=f"字段 {field_name} 的格式应符合正则表达式: {pattern}"
                        ))
        
        return issues
    
    def _validate_business_logic(self, data: Dict[str, Any]) -> List[ValidationIssue]:
        """验证业务逻辑"""
        issues = []
        
        # 验证开奖号码和总和的一致性
        if 'numbers' in data and 'result_sum' in data:
            numbers = data['numbers']
            result_sum = data['result_sum']
            
            if isinstance(numbers, list) and isinstance(result_sum, int):
                if len(numbers) == 3 and all(isinstance(n, int) for n in numbers):
                    calculated_sum = sum(numbers)
                    if calculated_sum != result_sum:
                        issues.append(ValidationIssue(
                            field_name="result_sum",
                            issue_type="sum_mismatch",
                            severity=ValidationSeverity.ERROR,
                            message="和值计算错误",
                            expected_value=calculated_sum,
                            actual_value=result_sum,
                            suggestion=f"数字 {numbers} 的和应为 {calculated_sum}"
                        ))
        
        # 验证大小判断
        if 'result_sum' in data and 'big_small' in data:
            result_sum = data['result_sum']
            big_small = data['big_small']
            
            if isinstance(result_sum, int) and isinstance(big_small, str):
                # 修正大小判断逻辑：13及以下为小，14及以上为大
                expected_big_small = '大' if result_sum >= 14 else '小'
                if big_small != expected_big_small:
                    issues.append(ValidationIssue(
                        field_name="big_small",
                        issue_type="big_small_mismatch",
                        severity=ValidationSeverity.WARNING,
                        message="大小判断可能有误",
                        expected_value=expected_big_small,
                        actual_value=big_small,
                        suggestion=f"和值 {result_sum} 通常判断为 '{expected_big_small}'"
                    ))
        
        # 验证单双判断
        if 'result_sum' in data and 'odd_even' in data:
            result_sum = data['result_sum']
            odd_even = data['odd_even']
            
            if isinstance(result_sum, int) and isinstance(odd_even, str):
                expected_odd_even = '单' if result_sum % 2 == 1 else '双'
                if odd_even != expected_odd_even:
                    issues.append(ValidationIssue(
                        field_name="odd_even",
                        issue_type="odd_even_mismatch",
                        severity=ValidationSeverity.WARNING,
                        message="单双判断可能有误",
                        expected_value=expected_odd_even,
                        actual_value=odd_even,
                        suggestion=f"和值 {result_sum} 通常判断为 '{expected_odd_even}'"
                    ))
        
        # 验证龙虎判断
        if 'numbers' in data and 'dragon_tiger' in data:
            numbers = data['numbers']
            dragon_tiger = data['dragon_tiger']
            
            if isinstance(numbers, list) and len(numbers) >= 3 and isinstance(dragon_tiger, str):
                first_num = numbers[0]
                last_num = numbers[2]
                
                if isinstance(first_num, int) and isinstance(last_num, int):
                    if first_num > last_num:
                        expected_dragon_tiger = '龙'
                    elif first_num < last_num:
                        expected_dragon_tiger = '虎'
                    else:
                        expected_dragon_tiger = '和'
                    
                    if dragon_tiger != expected_dragon_tiger:
                        issues.append(ValidationIssue(
                            field_name="dragon_tiger",
                            issue_type="dragon_tiger_mismatch",
                            severity=ValidationSeverity.WARNING,
                            message="龙虎判断可能有误",
                            expected_value=expected_dragon_tiger,
                            actual_value=dragon_tiger,
                            suggestion=f"首位 {first_num} 与末位 {last_num} 通常判断为 '{expected_dragon_tiger}'"
                        ))
        
        return issues
    
    def _calculate_completeness_score(self, data: Dict[str, Any], issues: List[ValidationIssue]) -> float:
        """计算完整性评分"""
        total_fields = len(self.validation_rules)
        present_fields = len([field for field in self.validation_rules.keys() if field in data and data[field] is not None])
        
        # 基础完整性评分
        base_score = (present_fields / total_fields) * 100
        
        # 扣除缺失字段的分数
        missing_penalty = len([issue for issue in issues if issue.issue_type in ['missing_required_field', 'empty_required_field']]) * 10
        
        return max(0, base_score - missing_penalty)
    
    def _calculate_quality_score(self, issues: List[ValidationIssue]) -> float:
        """计算质量评分"""
        if not issues:
            return 100.0
        
        # 根据问题严重程度扣分
        penalty = 0
        for issue in issues:
            if issue.severity == ValidationSeverity.CRITICAL:
                penalty += 25
            elif issue.severity == ValidationSeverity.ERROR:
                penalty += 15
            elif issue.severity == ValidationSeverity.WARNING:
                penalty += 5
            elif issue.severity == ValidationSeverity.INFO:
                penalty += 1
        
        return max(0, 100 - penalty)
    
    def generate_quality_report(self, validation_results: List[ValidationResult]) -> QualityReport:
        """生成质量报告"""
        # 使用报告生成锁
        if not self._report_lock.acquire(timeout=self._lock_timeout):
            logger.error("获取报告生成锁超时")
            # 返回空报告
            return QualityReport(
                report_timestamp=datetime.now(self.china_tz).isoformat(),
                total_records=0,
                valid_records=0,
                invalid_records=0,
                validation_results=[],
                field_completeness={},
                common_issues={"report_lock_timeout": 1},
                quality_trends={},
                recommendations=["报告生成锁超时，请检查并发访问情况"]
            )
        
        try:
            total_records = len(validation_results)
            valid_records = len([r for r in validation_results if r.is_valid])
            invalid_records = total_records - valid_records
            
            # 计算字段完整性
            field_completeness = self._calculate_field_completeness_stats(validation_results)
            
            # 统计常见问题
            common_issues = {}
            for result in validation_results:
                for issue in result.issues:
                    issue_key = f"{issue.field_name}_{issue.issue_type}"
                    common_issues[issue_key] = common_issues.get(issue_key, 0) + 1
            
            # 质量趋势分析（简化版）
            quality_trends = {
                'average_completeness_score': sum(r.completeness_score for r in validation_results) / total_records if total_records > 0 else 0,
                'average_quality_score': sum(r.quality_score for r in validation_results) / total_records if total_records > 0 else 0,
                'average_overall_score': sum(r.overall_score for r in validation_results) / total_records if total_records > 0 else 0
            }
            
            # 生成建议
            recommendations = self._generate_quality_recommendations(validation_results, common_issues, field_completeness)
            
            return QualityReport(
                report_timestamp=datetime.now(self.china_tz).isoformat(),
                total_records=total_records,
                valid_records=valid_records,
                invalid_records=invalid_records,
                validation_results=validation_results,
                field_completeness=field_completeness,
                common_issues=common_issues,
                quality_trends=quality_trends,
                recommendations=recommendations
            )
        finally:
            # 确保报告锁被释放
            self._report_lock.release()
    
    def _calculate_field_completeness_stats(self, validation_results: List[ValidationResult]) -> Dict[str, float]:
        """计算字段完整性统计"""
        field_stats = {}
        total_records = len(validation_results)
        
        if total_records == 0:
            return field_stats
        
        for field_name in self.validation_rules.keys():
            # 统计该字段的缺失次数
            missing_count = 0
            for result in validation_results:
                field_missing = any(
                    issue.field_name == field_name and 
                    issue.issue_type in ['missing_required_field', 'empty_required_field']
                    for issue in result.issues
                )
                if field_missing:
                    missing_count += 1
            
            completeness_rate = ((total_records - missing_count) / total_records) * 100
            field_stats[field_name] = round(completeness_rate, 2)
        
        return field_stats
    
    def _generate_quality_recommendations(self, validation_results: List[ValidationResult], 
                                        common_issues: Dict[str, int], 
                                        field_completeness: Dict[str, float]) -> List[str]:
        """生成质量改进建议"""
        recommendations = []
        
        # 基于常见问题的建议
        if common_issues:
            top_issues = sorted(common_issues.items(), key=lambda x: x[1], reverse=True)[:5]
            for issue_key, count in top_issues:
                if count > len(validation_results) * 0.1:  # 超过10%的记录有此问题
                    recommendations.append(f"高频问题: {issue_key} 出现 {count} 次，建议重点关注")
        
        # 基于字段完整性的建议
        for field_name, completeness in field_completeness.items():
            if completeness < 90:
                recommendations.append(f"字段 {field_name} 完整性较低 ({completeness:.1f}%)，建议加强数据收集")
        
        # 基于整体质量的建议
        avg_quality = sum(r.overall_score for r in validation_results) / len(validation_results) if validation_results else 0
        if avg_quality < 80:
            recommendations.append(f"整体数据质量偏低 ({avg_quality:.1f}分)，建议全面检查数据源和处理流程")
        elif avg_quality < 90:
            recommendations.append(f"数据质量良好但有改进空间 ({avg_quality:.1f}分)，建议优化数据验证规则")
        else:
            recommendations.append(f"数据质量优秀 ({avg_quality:.1f}分)，建议保持当前标准")
        
        return recommendations
    
    def export_quality_report(self, report: QualityReport, output_file: str = None) -> str:
        """导出质量报告"""
        # 使用文件操作锁
        if not self._file_lock.acquire(timeout=self._lock_timeout):
            logger.error("获取文件操作锁超时")
            raise RuntimeError("获取文件操作锁超时，无法导出报告")
        
        try:
            # 简化验证结果以减少文件大小
            simplified_results = []
            for result in report.validation_results[:10]:  # 只保留前10个详细结果
                simplified_results.append({
                    'draw_id': result.draw_id,
                    'is_valid': result.is_valid,
                    'completeness_score': result.completeness_score,
                    'quality_score': result.quality_score,
                    'overall_score': result.overall_score,
                    'issue_count': len(result.issues),
                    'critical_issues': len([i for i in result.issues if i.severity == ValidationSeverity.CRITICAL]),
                    'error_issues': len([i for i in result.issues if i.severity == ValidationSeverity.ERROR])
                })
            
            report_dict = {
                'report_timestamp': report.report_timestamp,
                'summary': {
                    'total_records': report.total_records,
                    'valid_records': report.valid_records,
                    'invalid_records': report.invalid_records,
                    'validity_rate': f"{(report.valid_records / report.total_records * 100):.2f}%" if report.total_records > 0 else "0%"
                },
                'field_completeness': report.field_completeness,
                'common_issues': dict(sorted(report.common_issues.items(), key=lambda x: x[1], reverse=True)[:10]),
                'quality_trends': report.quality_trends,
                'sample_validation_results': simplified_results,
                'recommendations': report.recommendations,
                'validation_statistics': self.validation_stats
            }
            
            report_json = json.dumps(report_dict, indent=2, ensure_ascii=False)
            
            if output_file:
                # 使用文件锁防止并发写入
                with open(output_file, 'w', encoding='utf-8') as f:
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        f.write(report_json)
                    except IOError:
                        logger.error(f"无法获取文件锁: {output_file}")
                        raise RuntimeError(f"文件 {output_file} 正在被其他进程使用")
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                logger.info(f"质量报告已保存到: {output_file}")
            
            return report_json
        finally:
            # 确保文件操作锁被释放
            self._file_lock.release()

def main():
    """测试数据质量验证器"""
    validator = DataQualityValidator()
    
    # 创建测试数据
    test_data = [
        OptimizedLotteryData(
            draw_id="3339001",
            timestamp="2025-09-25 10:00:00",
            numbers=[1, 2, 3],
            result_sum=6,
            big_small="小",
            odd_even="双",
            dragon_tiger="虎"
        ),
        OptimizedLotteryData(
            draw_id="3339002",
            timestamp="2025-09-25 10:03:00",
            numbers=[9, 8, 7],
            result_sum=24,
            big_small="大",
            odd_even="双",
            dragon_tiger="龙"
        ),
        # 错误数据示例
        OptimizedLotteryData(
            draw_id="invalid",
            timestamp="invalid-time",
            numbers=[1, 2],  # 长度错误
            result_sum=10,   # 和值错误
            big_small="中",  # 无效值
            odd_even="单",   # 与和值不匹配
            dragon_tiger="平" # 无效值
        )
    ]
    
    try:
        print("=== PC28数据质量验证测试 ===")
        
        # 批量验证
        validation_results = validator.validate_batch_records(test_data)
        
        # 显示验证结果
        for result in validation_results:
            print(f"\n期号: {result.draw_id}")
            print(f"有效性: {'✅' if result.is_valid else '❌'}")
            print(f"完整性评分: {result.completeness_score:.1f}")
            print(f"质量评分: {result.quality_score:.1f}")
            print(f"综合评分: {result.overall_score:.1f}")
            
            if result.issues:
                print("问题列表:")
                for issue in result.issues:
                    severity_symbol = {
                        ValidationSeverity.INFO: "ℹ️",
                        ValidationSeverity.WARNING: "⚠️",
                        ValidationSeverity.ERROR: "❌",
                        ValidationSeverity.CRITICAL: "🚨"
                    }
                    symbol = severity_symbol.get(issue.severity, "❓")
                    print(f"  {symbol} {issue.field_name}: {issue.message}")
                    if issue.suggestion:
                        print(f"     建议: {issue.suggestion}")
        
        # 生成质量报告
        quality_report = validator.generate_quality_report(validation_results)
        
        # 导出报告
        report_json = validator.export_quality_report(
            quality_report,
            f"data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        print("\n=== 数据质量报告 ===")
        print(report_json)
        
    except Exception as e:
        logger.error(f"数据质量验证测试失败: {e}")

if __name__ == "__main__":
    main()