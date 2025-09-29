#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataSyncValidator
- 数据同步和验证服务
- 确保上游API数据与BigQuery数据的一致性
- 提供数据质量检查和修复功能
"""
from __future__ import annotations
import os, json, time, datetime, logging, hashlib, re
from typing import Any, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class ValidationResult:
    """验证结果数据类"""
    status: str  # 'ok', 'warning', 'error'
    message: str
    details: Dict[str, Any]
    timestamp: str
    
@dataclass
class SyncResult:
    """同步结果数据类"""
    status: str  # 'success', 'partial', 'failed'
    synced_count: int
    failed_count: int
    skipped_count: int
    errors: List[str]
    timestamp: str

class DataSyncValidator:
    """数据同步和验证服务"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据同步验证器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 验证配置 - 修复验证规则
        self.validation_rules = config.get('validation', {
            'required_fields': ['draw_id', 'timestamp', 'numbers'],
            'number_count': 3,
            'number_range': [0, 27],
            'time_formats': [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S.%fZ'
            ],
            'max_time_diff_minutes': 60,  # 增加时间容忍度
            'draw_id_patterns': [
                r'^\d{6,8}$',  # 6-8位数字
                r'^\d{4}-\d{3}$',  # YYYY-NNN格式
                r'^\d{8}-\d{4}$'  # YYYYMMDD-NNNN格式
            ]
        })
        
        # 同步配置
        self.sync_config = config.get('sync', {
            'batch_size': 100,
            'max_retries': 3,
            'retry_delay': 2,
            'enable_deduplication': True,
            'conflict_resolution': 'upstream_priority'
        })
        
        # 数据质量阈值
        self.quality_thresholds = config.get('quality_thresholds', {
            'completeness_min': 0.90,  # 降低完整性要求
            'freshness_max_minutes': 60,  # 增加新鲜度容忍度
            'consistency_tolerance': 0.05  # 增加一致性容忍度
        })
    
    def _validate_draw_id(self, draw_id: Any) -> Tuple[bool, str]:
        """验证期号格式"""
        if draw_id is None:
            return False, "期号不能为空"
        
        draw_id_str = str(draw_id).strip()
        if not draw_id_str:
            return False, "期号不能为空字符串"
        
        # 检查多种期号格式
        for pattern in self.validation_rules['draw_id_patterns']:
            if re.match(pattern, draw_id_str):
                return True, ""
        
        return False, f"期号格式无效: {draw_id_str}"
    
    def _validate_timestamp(self, timestamp: Any) -> Tuple[bool, str, Optional[datetime.datetime]]:
        """验证时间戳格式"""
        if timestamp is None:
            return False, "时间戳不能为空", None
        
        timestamp_str = str(timestamp).strip()
        if not timestamp_str:
            return False, "时间戳不能为空字符串", None
        
        # 尝试多种时间格式
        for time_format in self.validation_rules['time_formats']:
            try:
                parsed_time = datetime.datetime.strptime(timestamp_str, time_format)
                
                # 检查时间合理性
                now = datetime.datetime.now()
                time_diff = abs((now - parsed_time).total_seconds() / 60)
                max_diff = self.validation_rules['max_time_diff_minutes']
                
                if time_diff > max_diff:
                    return True, f"时间差异较大: {time_diff:.1f}分钟", parsed_time
                
                return True, "", parsed_time
            except ValueError:
                continue
        
        return False, f"时间戳格式无效: {timestamp_str}", None
    
    def _validate_numbers(self, numbers: Any) -> Tuple[bool, str, List[int]]:
        """验证开奖号码"""
        if numbers is None:
            return False, "开奖号码不能为空", []
        
        # 处理不同的号码格式
        parsed_numbers = []
        
        if isinstance(numbers, str):
            # 尝试JSON解析
            try:
                numbers = json.loads(numbers)
            except:
                # 尝试逗号分隔解析
                try:
                    numbers = [int(x.strip()) for x in numbers.split(',')]
                except:
                    return False, "开奖号码格式无效", []
        
        if isinstance(numbers, list):
            try:
                parsed_numbers = [int(x) for x in numbers]
            except (ValueError, TypeError):
                return False, "开奖号码包含非数字值", []
        else:
            return False, "开奖号码必须是列表或字符串", []
        
        # 检查号码数量
        expected_count = self.validation_rules['number_count']
        if len(parsed_numbers) != expected_count:
            return False, f"开奖号码数量错误: 期望{expected_count}个，实际{len(parsed_numbers)}个", parsed_numbers
        
        # 检查号码范围
        min_num, max_num = self.validation_rules['number_range']
        for i, num in enumerate(parsed_numbers):
            if num < min_num or num > max_num:
                return False, f"第{i+1}个号码超出范围[{min_num}, {max_num}]: {num}", parsed_numbers
        
        return True, "", parsed_numbers
    
    def _calculate_derived_values(self, numbers: List[int]) -> Dict[str, Any]:
        """计算衍生值"""
        if len(numbers) != 3:
            return {}
        
        result_sum = sum(numbers)
        
        # 大小判断 (和值 >= 14为大，< 14为小)
        big_small = "大" if result_sum >= 14 else "小"
        
        # 奇偶判断
        odd_even = "奇" if result_sum % 2 == 1 else "偶"
        
        # 龙虎判断 (第一个号码 > 第三个号码为龙，否则为虎)
        dragon_tiger = "龙" if numbers[0] > numbers[2] else "虎"
        
        return {
            'result_sum': result_sum,
            'big_small': big_small,
            'odd_even': odd_even,
            'dragon_tiger': dragon_tiger
        }
    
    def validate_draw_data(self, draw_data: Dict[str, Any], source: str = 'unknown') -> ValidationResult:
        """
        验证单条开奖数据
        
        Args:
            draw_data: 开奖数据
            source: 数据来源
            
        Returns:
            验证结果
        """
        errors = []
        warnings = []
        
        try:
            # 1. 验证期号
            draw_id_valid, draw_id_msg = self._validate_draw_id(draw_data.get('draw_id'))
            if not draw_id_valid:
                errors.append(f"draw_id_invalid_format: {draw_id_msg}")
            
            # 2. 验证时间戳
            timestamp_valid, timestamp_msg, parsed_time = self._validate_timestamp(draw_data.get('timestamp'))
            if not timestamp_valid:
                errors.append(f"timestamp_invalid_format: {timestamp_msg}")
            elif timestamp_msg:  # 有警告信息
                warnings.append(f"timestamp_warning: {timestamp_msg}")
            
            # 3. 验证开奖号码
            numbers_valid, numbers_msg, parsed_numbers = self._validate_numbers(draw_data.get('numbers'))
            if not numbers_valid:
                errors.append(f"numbers_invalid_list_length: {numbers_msg}")
            
            # 4. 如果号码有效，验证衍生值
            if numbers_valid and parsed_numbers:
                derived_values = self._calculate_derived_values(parsed_numbers)
                
                # 验证和值
                if 'result_sum' in draw_data:
                    expected_sum = derived_values['result_sum']
                    actual_sum = draw_data['result_sum']
                    if actual_sum != expected_sum:
                        errors.append(f"result_sum_sum_mismatch: 期望{expected_sum}，实际{actual_sum}")
                
                # 验证大小值
                if 'big_small' in draw_data:
                    expected_big_small = derived_values['big_small']
                    actual_big_small = str(draw_data['big_small']).strip()
                    if actual_big_small not in ['大', '小', 'big', 'small', '1', '0']:
                        errors.append(f"big_small_invalid_value: 无效值 {actual_big_small}")
                    elif actual_big_small != expected_big_small:
                        # 转换比较
                        normalized_actual = self._normalize_big_small(actual_big_small)
                        if normalized_actual != expected_big_small:
                            errors.append(f"big_small_big_small_mismatch: 期望{expected_big_small}，实际{actual_big_small}")
                
                # 验证奇偶值
                if 'odd_even' in draw_data:
                    expected_odd_even = derived_values['odd_even']
                    actual_odd_even = str(draw_data['odd_even']).strip()
                    if actual_odd_even not in ['奇', '偶', 'odd', 'even', '1', '0']:
                        errors.append(f"odd_even_invalid_value: 无效值 {actual_odd_even}")
                    elif actual_odd_even != expected_odd_even:
                        # 转换比较
                        normalized_actual = self._normalize_odd_even(actual_odd_even)
                        if normalized_actual != expected_odd_even:
                            errors.append(f"odd_even_odd_even_mismatch: 期望{expected_odd_even}，实际{actual_odd_even}")
                
                # 验证龙虎值
                if 'dragon_tiger' in draw_data:
                    expected_dragon_tiger = derived_values['dragon_tiger']
                    actual_dragon_tiger = str(draw_data['dragon_tiger']).strip()
                    if actual_dragon_tiger not in ['龙', '虎', 'dragon', 'tiger', '1', '0']:
                        errors.append(f"dragon_tiger_invalid_value: 无效值 {actual_dragon_tiger}")
                    elif actual_dragon_tiger != expected_dragon_tiger:
                        # 转换比较
                        normalized_actual = self._normalize_dragon_tiger(actual_dragon_tiger)
                        if normalized_actual != expected_dragon_tiger:
                            warnings.append(f"dragon_tiger_mismatch: 期望{expected_dragon_tiger}，实际{actual_dragon_tiger}")
            
            # 计算数据完整性评分
            required_fields = ['draw_id', 'timestamp', 'numbers']
            optional_fields = ['result_sum', 'big_small', 'odd_even', 'dragon_tiger']
            
            present_required = sum(1 for field in required_fields if field in draw_data and draw_data[field] is not None)
            present_optional = sum(1 for field in optional_fields if field in draw_data and draw_data[field] is not None)
            
            completeness_score = (present_required + present_optional * 0.5) / (len(required_fields) + len(optional_fields) * 0.5)
            
            # 确定验证状态
            if errors:
                status = 'error'
                message = f"验证失败: {len(errors)}个错误"
            elif warnings:
                status = 'warning'
                message = f"验证通过但有警告: {len(warnings)}个警告"
            else:
                status = 'ok'
                message = "验证通过"
            
            return ValidationResult(
                status=status,
                message=message,
                details={
                    'source': source,
                    'errors': errors,
                    'warnings': warnings,
                    'completeness_score': completeness_score,
                    'data_hash': self._calculate_data_hash(draw_data),
                    'derived_values': derived_values if numbers_valid else {}
                },
                timestamp=datetime.datetime.now().isoformat()
            )
            
        except Exception as e:
            return ValidationResult(
                status='error',
                message=f"验证过程异常: {str(e)}",
                details={'exception': str(e), 'source': source},
                timestamp=datetime.datetime.now().isoformat()
            )
    
    def _normalize_big_small(self, value: str) -> str:
        """标准化大小值"""
        value = str(value).strip().lower()
        if value in ['大', 'big', '1']:
            return '大'
        elif value in ['小', 'small', '0']:
            return '小'
        return value
    
    def _normalize_odd_even(self, value: str) -> str:
        """标准化奇偶值"""
        value = str(value).strip().lower()
        if value in ['奇', 'odd', '1']:
            return '奇'
        elif value in ['偶', 'even', '0']:
            return '偶'
        return value
    
    def _normalize_dragon_tiger(self, value: str) -> str:
        """标准化龙虎值"""
        value = str(value).strip().lower()
        if value in ['龙', 'dragon', '1']:
            return '龙'
        elif value in ['虎', 'tiger', '0']:
            return '虎'
        return value

    def validate_batch_data(self, data_list: List[Dict[str, Any]], source: str = 'unknown') -> Dict[str, Any]:
        """
        批量验证开奖数据
        
        Args:
            data_list: 开奖数据列表
            source: 数据来源
            
        Returns:
            批量验证结果
        """
        results = {
            'total_count': len(data_list),
            'ok_count': 0,
            'warning_count': 0,
            'error_count': 0,
            'details': [],
            'summary': {},
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        error_types = defaultdict(int)
        warning_types = defaultdict(int)
        
        for i, data in enumerate(data_list):
            validation_result = self.validate_draw_data(data, source)
            results['details'].append({
                'index': i,
                'draw_id': data.get('long_issue', 'unknown'),
                'status': validation_result.status,
                'message': validation_result.message,
                'details': validation_result.details
            })
            
            # 统计结果
            if validation_result.status == 'ok':
                results['ok_count'] += 1
            elif validation_result.status == 'warning':
                results['warning_count'] += 1
                for warning in validation_result.details.get('warnings', []):
                    warning_types[warning] += 1
            else:
                results['error_count'] += 1
                for error in validation_result.details.get('errors', []):
                    error_types[error] += 1
        
        # 计算质量指标
        success_rate = results['ok_count'] / results['total_count'] if results['total_count'] > 0 else 0
        completeness_scores = [detail['details'].get('completeness_score', 0) 
                             for detail in results['details']]
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        
        results['summary'] = {
            'success_rate': round(success_rate, 4),
            'avg_completeness': round(avg_completeness, 4),
            'common_errors': dict(error_types),
            'common_warnings': dict(warning_types),
            'quality_assessment': self._assess_data_quality(success_rate, avg_completeness)
        }
        
        return results
    
    def detect_duplicates(self, data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        检测重复数据
        
        Args:
            data_list: 数据列表
            
        Returns:
            重复检测结果
        """
        seen_issues = set()
        seen_hashes = set()
        duplicates = []
        
        for i, data in enumerate(data_list):
            issue = data.get('long_issue')
            data_hash = self._calculate_data_hash(data)
            
            # 检查期号重复
            if issue in seen_issues:
                duplicates.append({
                    'type': 'issue_duplicate',
                    'index': i,
                    'issue': issue,
                    'message': f"期号重复: {issue}"
                })
            else:
                seen_issues.add(issue)
            
            # 检查数据内容重复
            if data_hash in seen_hashes:
                duplicates.append({
                    'type': 'content_duplicate',
                    'index': i,
                    'issue': issue,
                    'hash': data_hash,
                    'message': f"数据内容重复: {issue}"
                })
            else:
                seen_hashes.add(data_hash)
        
        return {
            'total_count': len(data_list),
            'unique_count': len(seen_issues),
            'duplicate_count': len(duplicates),
            'duplicates': duplicates,
            'duplicate_rate': len(duplicates) / len(data_list) if data_list else 0,
            'timestamp': datetime.datetime.now().isoformat()
        }
    
    def compare_data_sources(self, upstream_data: List[Dict[str, Any]], 
                           bigquery_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        比较上游API数据和BigQuery数据的一致性
        
        Args:
            upstream_data: 上游API数据
            bigquery_data: BigQuery数据
            
        Returns:
            比较结果
        """
        # 按期号建立索引
        upstream_index = {str(data.get('long_issue')): data for data in upstream_data}
        bigquery_index = {str(data.get('long_issue')): data for data in bigquery_data}
        
        upstream_issues = set(upstream_index.keys())
        bigquery_issues = set(bigquery_index.keys())
        
        # 找出差异
        only_in_upstream = upstream_issues - bigquery_issues
        only_in_bigquery = bigquery_issues - upstream_issues
        common_issues = upstream_issues & bigquery_issues
        
        # 比较共同期号的数据
        content_differences = []
        for issue in common_issues:
            upstream_item = upstream_index[issue]
            bigquery_item = bigquery_index[issue]
            
            diff = self._compare_draw_data(upstream_item, bigquery_item, issue)
            if diff['has_differences']:
                content_differences.append(diff)
        
        return {
            'upstream_count': len(upstream_data),
            'bigquery_count': len(bigquery_data),
            'common_count': len(common_issues),
            'only_in_upstream': list(only_in_upstream),
            'only_in_bigquery': list(only_in_bigquery),
            'content_differences': content_differences,
            'consistency_rate': (len(common_issues) - len(content_differences)) / len(common_issues) if common_issues else 1.0,
            'timestamp': datetime.datetime.now().isoformat()
        }
    
    def sync_data_to_bigquery(self, data_list: List[Dict[str, Any]], 
                             target_table: str) -> SyncResult:
        """
        将数据同步到BigQuery
        
        Args:
            data_list: 要同步的数据列表
            target_table: 目标表名
            
        Returns:
            同步结果
        """
        # 这里实现具体的BigQuery同步逻辑
        # 目前返回模拟结果
        
        synced_count = 0
        failed_count = 0
        skipped_count = 0
        errors = []
        
        try:
            # 数据预处理和验证
            validated_data = []
            for data in data_list:
                validation_result = self.validate_draw_data(data, 'sync_source')
                if validation_result.status in ['ok', 'warning']:
                    validated_data.append(data)
                else:
                    failed_count += 1
                    errors.append(f"数据验证失败: {data.get('long_issue', 'unknown')} - {validation_result.message}")
            
            # 去重处理
            if self.sync_config['enable_deduplication']:
                dedup_result = self.detect_duplicates(validated_data)
                if dedup_result['duplicate_count'] > 0:
                    self.logger.warning(f"发现 {dedup_result['duplicate_count']} 条重复数据")
                    # 移除重复数据
                    duplicate_indices = {dup['index'] for dup in dedup_result['duplicates']}
                    validated_data = [data for i, data in enumerate(validated_data) 
                                    if i not in duplicate_indices]
                    skipped_count += len(duplicate_indices)
            
            # 模拟同步过程
            batch_size = self.sync_config['batch_size']
            for i in range(0, len(validated_data), batch_size):
                batch = validated_data[i:i + batch_size]
                # 这里应该实现实际的BigQuery插入逻辑
                # 目前假设同步成功
                synced_count += len(batch)
                self.logger.info(f"同步批次 {i//batch_size + 1}: {len(batch)} 条记录")
            
            status = 'success' if failed_count == 0 else 'partial'
            
        except Exception as e:
            status = 'failed'
            errors.append(f"同步过程异常: {str(e)}")
            self.logger.error(f"数据同步失败: {e}")
        
        return SyncResult(
            status=status,
            synced_count=synced_count,
            failed_count=failed_count,
            skipped_count=skipped_count,
            errors=errors,
            timestamp=datetime.datetime.now().isoformat()
        )
    
    def _calculate_data_hash(self, data: Dict[str, Any]) -> str:
        """
        计算数据哈希值
        
        Args:
            data: 数据字典
            
        Returns:
            哈希值
        """
        # 提取关键字段计算哈希
        key_fields = ['long_issue', 'kjtime', 'number']
        hash_data = {k: data.get(k) for k in key_fields if k in data}
        hash_str = json.dumps(hash_data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(hash_str.encode('utf-8')).hexdigest()
    
    def _compare_draw_data(self, data1: Dict[str, Any], data2: Dict[str, Any], 
                          issue: str) -> Dict[str, Any]:
        """
        比较两条开奖数据
        
        Args:
            data1: 数据1
            data2: 数据2
            issue: 期号
            
        Returns:
            比较结果
        """
        differences = []
        
        # 比较关键字段
        compare_fields = ['kjtime', 'number', 'sum_value', 'is_odd', 'is_big']
        for field in compare_fields:
            val1 = data1.get(field)
            val2 = data2.get(field)
            
            if val1 != val2:
                differences.append({
                    'field': field,
                    'upstream_value': val1,
                    'bigquery_value': val2
                })
        
        return {
            'issue': issue,
            'has_differences': len(differences) > 0,
            'differences': differences,
            'difference_count': len(differences)
        }
    
    def _assess_data_quality(self, success_rate: float, completeness: float) -> str:
        """
        评估数据质量
        
        Args:
            success_rate: 成功率
            completeness: 完整性
            
        Returns:
            质量评估结果
        """
        if success_rate >= 0.95 and completeness >= 0.95:
            return 'excellent'
        elif success_rate >= 0.90 and completeness >= 0.90:
            return 'good'
        elif success_rate >= 0.80 and completeness >= 0.80:
            return 'acceptable'
        else:
            return 'poor'