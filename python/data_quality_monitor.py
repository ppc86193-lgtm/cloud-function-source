#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量监控模块
用于监控从上游API获取的数据质量，包括数据完整性、一致性和时效性检查
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class DataQualityMetrics:
    """数据质量指标"""
    timestamp: datetime
    data_source: str
    completeness_score: float  # 完整性评分 (0-1)
    consistency_score: float   # 一致性评分 (0-1)
    timeliness_score: float    # 时效性评分 (0-1)
    accuracy_score: float      # 准确性评分 (0-1)
    overall_score: float       # 总体评分 (0-1)
    issues: List[str]          # 发现的问题列表
    record_count: int          # 记录数量
    missing_fields: List[str]  # 缺失字段
    anomalies: List[str]       # 异常数据

class DataQualityMonitor:
    """数据质量监控器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.quality_history = defaultdict(list)
        
        # 数据质量阈值配置
        self.thresholds = config.get('data_quality', {
            'completeness_threshold': 0.95,
            'consistency_threshold': 0.90,
            'timeliness_threshold': 0.85,
            'accuracy_threshold': 0.90,
            'overall_threshold': 0.85
        })
        
        # 必需字段配置
        self.required_fields = {
            'realtime': ['long_issue', 'kjtime', 'number', 'next_issue', 'next_time'],
            'historical': ['long_issue', 'kjtime', 'number']
        }
    
    def evaluate_realtime_data(self, data: Dict[str, Any]) -> DataQualityMetrics:
        """评估实时数据质量"""
        return self._evaluate_data_quality(data, 'realtime')
    
    def evaluate_historical_data(self, data: List[Dict[str, Any]]) -> DataQualityMetrics:
        """评估历史数据质量"""
        if not data:
            return DataQualityMetrics(
                timestamp=datetime.now(),
                data_source='historical',
                completeness_score=0.0,
                consistency_score=0.0,
                timeliness_score=0.0,
                accuracy_score=0.0,
                overall_score=0.0,
                issues=['数据为空'],
                record_count=0,
                missing_fields=[],
                anomalies=[]
            )
        
        # 对历史数据进行批量评估
        total_completeness = 0
        total_consistency = 0
        total_accuracy = 0
        all_issues = []
        all_missing_fields = set()
        all_anomalies = []
        
        for record in data:
            metrics = self._evaluate_data_quality(record, 'historical')
            total_completeness += metrics.completeness_score
            total_consistency += metrics.consistency_score
            total_accuracy += metrics.accuracy_score
            all_issues.extend(metrics.issues)
            all_missing_fields.update(metrics.missing_fields)
            all_anomalies.extend(metrics.anomalies)
        
        count = len(data)
        avg_completeness = total_completeness / count
        avg_consistency = total_consistency / count
        avg_accuracy = total_accuracy / count
        
        # 时效性基于最新记录的时间
        timeliness_score = self._calculate_timeliness(data[0] if data else {})
        
        overall_score = (avg_completeness + avg_consistency + timeliness_score + avg_accuracy) / 4
        
        return DataQualityMetrics(
            timestamp=datetime.now(),
            data_source='historical',
            completeness_score=avg_completeness,
            consistency_score=avg_consistency,
            timeliness_score=timeliness_score,
            accuracy_score=avg_accuracy,
            overall_score=overall_score,
            issues=list(set(all_issues)),
            record_count=count,
            missing_fields=list(all_missing_fields),
            anomalies=list(set(all_anomalies))
        )
    
    def _evaluate_data_quality(self, data: Dict[str, Any], data_type: str) -> DataQualityMetrics:
        """评估单条数据的质量"""
        issues = []
        missing_fields = []
        anomalies = []
        
        # 1. 完整性检查
        completeness_score = self._calculate_completeness(data, data_type, missing_fields)
        
        # 2. 一致性检查
        consistency_score = self._calculate_consistency(data, issues)
        
        # 3. 时效性检查
        timeliness_score = self._calculate_timeliness(data)
        
        # 4. 准确性检查
        accuracy_score = self._calculate_accuracy(data, anomalies)
        
        # 5. 计算总体评分
        overall_score = (completeness_score + consistency_score + timeliness_score + accuracy_score) / 4
        
        return DataQualityMetrics(
            timestamp=datetime.now(),
            data_source=data_type,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            timeliness_score=timeliness_score,
            accuracy_score=accuracy_score,
            overall_score=overall_score,
            issues=issues,
            record_count=1,
            missing_fields=missing_fields,
            anomalies=anomalies
        )
    
    def _calculate_completeness(self, data: Dict[str, Any], data_type: str, missing_fields: List[str]) -> float:
        """计算数据完整性评分"""
        required_fields = self.required_fields.get(data_type, [])
        if not required_fields:
            return 1.0
        
        present_fields = 0
        for field in required_fields:
            if field in data and data[field] is not None and str(data[field]).strip():
                present_fields += 1
            else:
                missing_fields.append(field)
        
        return present_fields / len(required_fields)
    
    def _calculate_consistency(self, data: Dict[str, Any], issues: List[str]) -> float:
        """计算数据一致性评分"""
        score = 1.0
        
        # 检查期号格式
        if 'long_issue' in data:
            issue = str(data['long_issue'])
            if not issue.isdigit() or len(issue) < 6:
                issues.append(f"期号格式异常: {issue}")
                score -= 0.2
        
        # 检查开奖号码格式
        if 'number' in data and data['number']:
            numbers = data['number']
            if isinstance(numbers, list):
                if len(numbers) != 3:
                    issues.append(f"开奖号码数量异常: {len(numbers)}")
                    score -= 0.3
                else:
                    for num in numbers:
                        if not isinstance(num, int) or num < 0 or num > 27:
                            issues.append(f"开奖号码范围异常: {num}")
                            score -= 0.2
                            break
        
        # 检查时间格式
        if 'kjtime' in data and data['kjtime']:
            try:
                datetime.strptime(str(data['kjtime']), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                issues.append(f"开奖时间格式异常: {data['kjtime']}")
                score -= 0.2
        
        return max(0.0, score)
    
    def _calculate_timeliness(self, data: Dict[str, Any]) -> float:
        """计算数据时效性评分"""
        if 'kjtime' not in data or not data['kjtime']:
            return 0.5  # 缺少时间信息，给中等评分
        
        try:
            kj_time = datetime.strptime(str(data['kjtime']), '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            time_diff = abs((now - kj_time).total_seconds())
            
            # 根据时间差计算评分
            if time_diff <= 300:  # 5分钟内
                return 1.0
            elif time_diff <= 1800:  # 30分钟内
                return 0.8
            elif time_diff <= 3600:  # 1小时内
                return 0.6
            elif time_diff <= 86400:  # 1天内
                return 0.4
            else:
                return 0.2
        except ValueError:
            return 0.3  # 时间格式错误
    
    def _calculate_accuracy(self, data: Dict[str, Any], anomalies: List[str]) -> float:
        """计算数据准确性评分"""
        score = 1.0
        
        # 检查状态码
        if 'codeid' in data:
            code = data['codeid']
            if code != 10000:
                anomalies.append(f"API状态码异常: {code}")
                score -= 0.3
        
        # 检查开奖号码和值
        if 'number' in data and data['number'] and isinstance(data['number'], list):
            numbers = data['number']
            if len(numbers) == 3:
                total = sum(numbers)
                if total < 0 or total > 81:  # 3个0-27的数字和的范围
                    anomalies.append(f"开奖号码和值异常: {total}")
                    score -= 0.2
        
        # 检查下期信息一致性（仅实时数据）
        if 'next_issue' in data and 'long_issue' in data:
            try:
                current_issue = int(data['long_issue'])
                next_issue = int(data['next_issue'])
                if next_issue != current_issue + 1:
                    anomalies.append(f"下期期号不连续: {current_issue} -> {next_issue}")
                    score -= 0.1
            except (ValueError, TypeError):
                pass
        
        return max(0.0, score)
    
    def record_quality_metrics(self, metrics: DataQualityMetrics):
        """记录数据质量指标"""
        self.quality_history[metrics.data_source].append(metrics)
        
        # 保持历史记录在合理范围内
        max_history = 100
        if len(self.quality_history[metrics.data_source]) > max_history:
            self.quality_history[metrics.data_source] = self.quality_history[metrics.data_source][-max_history:]
        
        # 记录日志
        self.logger.info(f"数据质量评估 - {metrics.data_source}: 总分={metrics.overall_score:.3f}, "
                        f"完整性={metrics.completeness_score:.3f}, 一致性={metrics.consistency_score:.3f}, "
                        f"时效性={metrics.timeliness_score:.3f}, 准确性={metrics.accuracy_score:.3f}")
        
        if metrics.issues:
            self.logger.warning(f"数据质量问题 - {metrics.data_source}: {', '.join(metrics.issues)}")
    
    def get_quality_summary(self, hours: int = 24) -> Dict[str, Any]:
        """获取数据质量摘要"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        summary = {}
        
        for data_source, metrics_list in self.quality_history.items():
            recent_metrics = [m for m in metrics_list if m.timestamp >= cutoff_time]
            
            if not recent_metrics:
                continue
            
            # 计算平均分数
            avg_scores = {
                'completeness': sum(m.completeness_score for m in recent_metrics) / len(recent_metrics),
                'consistency': sum(m.consistency_score for m in recent_metrics) / len(recent_metrics),
                'timeliness': sum(m.timeliness_score for m in recent_metrics) / len(recent_metrics),
                'accuracy': sum(m.accuracy_score for m in recent_metrics) / len(recent_metrics),
                'overall': sum(m.overall_score for m in recent_metrics) / len(recent_metrics)
            }
            
            # 统计问题
            all_issues = []
            for m in recent_metrics:
                all_issues.extend(m.issues)
            
            issue_counts = defaultdict(int)
            for issue in all_issues:
                issue_counts[issue] += 1
            
            # 质量状态
            overall_avg = avg_scores['overall']
            if overall_avg >= self.thresholds['overall_threshold']:
                status = 'good'
            elif overall_avg >= 0.7:
                status = 'warning'
            else:
                status = 'critical'
            
            summary[data_source] = {
                'status': status,
                'avg_scores': avg_scores,
                'total_records': len(recent_metrics),
                'total_issues': len(all_issues),
                'top_issues': dict(sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
                'latest_score': recent_metrics[-1].overall_score if recent_metrics else 0
            }
        
        return summary
    
    def check_quality_alerts(self, metrics: DataQualityMetrics) -> List[str]:
        """检查是否需要发送质量告警"""
        alerts = []
        
        # 检查各项指标是否低于阈值
        if metrics.completeness_score < self.thresholds['completeness_threshold']:
            alerts.append(f"数据完整性告警: {metrics.completeness_score:.3f} < {self.thresholds['completeness_threshold']}")
        
        if metrics.consistency_score < self.thresholds['consistency_threshold']:
            alerts.append(f"数据一致性告警: {metrics.consistency_score:.3f} < {self.thresholds['consistency_threshold']}")
        
        if metrics.timeliness_score < self.thresholds['timeliness_threshold']:
            alerts.append(f"数据时效性告警: {metrics.timeliness_score:.3f} < {self.thresholds['timeliness_threshold']}")
        
        if metrics.accuracy_score < self.thresholds['accuracy_threshold']:
            alerts.append(f"数据准确性告警: {metrics.accuracy_score:.3f} < {self.thresholds['accuracy_threshold']}")
        
        if metrics.overall_score < self.thresholds['overall_threshold']:
            alerts.append(f"数据质量总体告警: {metrics.overall_score:.3f} < {self.thresholds['overall_threshold']}")
        
        return alerts