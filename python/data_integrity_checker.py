#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28数据完整性检查器
负责历史数据数量核对、时间校准和数据质量验证
"""

import json
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from pc28_upstream_api import PC28UpstreamAPI
from api_field_optimization import OptimizedPC28DataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DataIntegrityReport:
    """数据完整性报告"""
    check_timestamp: str
    date_range: str
    expected_draws_count: int
    actual_draws_count: int
    missing_draws: List[str]
    duplicate_draws: List[str]
    time_inconsistencies: List[Dict[str, Any]]
    data_quality_issues: List[Dict[str, Any]]
    overall_integrity_score: float
    recommendations: List[str]

class DataIntegrityChecker:
    """数据完整性检查器"""
    
    def __init__(self):
        self.api_client = PC28UpstreamAPI()
        self.data_processor = OptimizedPC28DataProcessor()
        
        # PC28开奖规律：每天约480期（每3分钟一期，24小时）
        self.draws_per_day = 480
        self.draw_interval_minutes = 3
        
        # 中国时区
        self.china_tz = timezone(timedelta(hours=8))
    
    def check_historical_data_integrity(self, start_date: str, end_date: str) -> DataIntegrityReport:
        """检查历史数据完整性"""
        logger.info(f"开始检查历史数据完整性: {start_date} 到 {end_date}")
        
        try:
            # 获取历史数据
            historical_data = self._fetch_historical_data_range(start_date, end_date)
            
            # 计算预期开奖期数
            expected_count = self._calculate_expected_draws(start_date, end_date)
            actual_count = len(historical_data)
            
            # 检查缺失期数
            missing_draws = self._find_missing_draws(historical_data, start_date, end_date)
            
            # 检查重复期数
            duplicate_draws = self._find_duplicate_draws(historical_data)
            
            # 检查时间一致性
            time_inconsistencies = self._check_time_consistency(historical_data)
            
            # 检查数据质量
            data_quality_issues = self._check_data_quality(historical_data)
            
            # 计算完整性评分
            integrity_score = self._calculate_integrity_score(
                expected_count, actual_count, missing_draws, 
                duplicate_draws, time_inconsistencies, data_quality_issues
            )
            
            # 生成建议
            recommendations = self._generate_recommendations(
                expected_count, actual_count, missing_draws, 
                duplicate_draws, time_inconsistencies, data_quality_issues
            )
            
            report = DataIntegrityReport(
                check_timestamp=datetime.now(self.china_tz).isoformat(),
                date_range=f"{start_date} 到 {end_date}",
                expected_draws_count=expected_count,
                actual_draws_count=actual_count,
                missing_draws=missing_draws,
                duplicate_draws=duplicate_draws,
                time_inconsistencies=time_inconsistencies,
                data_quality_issues=data_quality_issues,
                overall_integrity_score=integrity_score,
                recommendations=recommendations
            )
            
            return report
            
        except Exception as e:
            logger.error(f"数据完整性检查失败: {e}")
            raise
    
    def _fetch_historical_data_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取指定日期范围的历史数据"""
        all_data = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"获取 {date_str} 的历史数据")
            
            try:
                # 获取单日数据
                daily_data = self.api_client.get_history_lottery(date=date_str, limit=500)
                if daily_data and daily_data.get('retdata'):
                    processed_data = self.data_processor.process_history_data(daily_data)
                    all_data.extend([{
                        'draw_id': item.draw_id,
                        'timestamp': item.timestamp,
                        'numbers': item.numbers,
                        'result_sum': item.result_sum,
                        'date': date_str
                    } for item in processed_data])
                
                # 避免API频率限制
                time.sleep(1)
                
            except Exception as e:
                logger.warning(f"获取 {date_str} 数据失败: {e}")
            
            current_date += timedelta(days=1)
        
        return all_data
    
    def _calculate_expected_draws(self, start_date: str, end_date: str) -> int:
        """计算预期开奖期数"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        days = (end - start).days + 1
        return days * self.draws_per_day
    
    def _find_missing_draws(self, data: List[Dict[str, Any]], start_date: str, end_date: str) -> List[str]:
        """查找缺失的开奖期数"""
        if not data:
            return []
        
        # 提取所有期号
        existing_draws = set(item['draw_id'] for item in data)
        
        # 生成预期期号范围（简化版本）
        if data:
            min_draw = min(int(item['draw_id']) for item in data)
            max_draw = max(int(item['draw_id']) for item in data)
            
            expected_draws = set(str(i) for i in range(min_draw, max_draw + 1))
            missing = expected_draws - existing_draws
            
            return sorted(list(missing))
        
        return []
    
    def _find_duplicate_draws(self, data: List[Dict[str, Any]]) -> List[str]:
        """查找重复的开奖期数"""
        draw_counts = {}
        for item in data:
            draw_id = item['draw_id']
            draw_counts[draw_id] = draw_counts.get(draw_id, 0) + 1
        
        duplicates = [draw_id for draw_id, count in draw_counts.items() if count > 1]
        return duplicates
    
    def _check_time_consistency(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """检查时间一致性"""
        inconsistencies = []
        
        if len(data) < 2:
            return inconsistencies
        
        # 按期号排序
        sorted_data = sorted(data, key=lambda x: int(x['draw_id']))
        
        for i in range(1, len(sorted_data)):
            current = sorted_data[i]
            previous = sorted_data[i-1]
            
            try:
                # 解析时间
                current_time = datetime.strptime(current['timestamp'], '%Y-%m-%d %H:%M:%S')
                previous_time = datetime.strptime(previous['timestamp'], '%Y-%m-%d %H:%M:%S')
                
                # 计算时间差
                time_diff = (current_time - previous_time).total_seconds() / 60
                
                # 检查是否符合3分钟间隔
                if abs(time_diff - self.draw_interval_minutes) > 1:  # 允许1分钟误差
                    inconsistencies.append({
                        'draw_id': current['draw_id'],
                        'expected_interval': self.draw_interval_minutes,
                        'actual_interval': time_diff,
                        'previous_draw': previous['draw_id'],
                        'issue_type': 'time_interval_anomaly'
                    })
                
            except ValueError as e:
                inconsistencies.append({
                    'draw_id': current['draw_id'],
                    'issue_type': 'time_format_error',
                    'error': str(e)
                })
        
        return inconsistencies
    
    def _check_data_quality(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """检查数据质量"""
        quality_issues = []
        
        for item in data:
            issues = []
            
            # 检查开奖号码
            numbers = item.get('numbers', [])
            if not numbers or len(numbers) != 3:
                issues.append('开奖号码数量不正确')
            else:
                for num in numbers:
                    if not isinstance(num, int) or not (0 <= num <= 9):
                        issues.append(f'开奖号码{num}超出范围[0-9]')
            
            # 检查总和
            if numbers and item.get('result_sum') != sum(numbers):
                issues.append('开奖号码总和计算错误')
            
            # 检查期号格式
            draw_id = item.get('draw_id', '')
            if not draw_id or not draw_id.isdigit():
                issues.append('期号格式错误')
            
            # 检查时间格式
            timestamp = item.get('timestamp', '')
            try:
                datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                issues.append('时间格式错误')
            
            if issues:
                quality_issues.append({
                    'draw_id': draw_id,
                    'issues': issues
                })
        
        return quality_issues
    
    def _calculate_integrity_score(self, expected_count: int, actual_count: int, 
                                 missing_draws: List[str], duplicate_draws: List[str],
                                 time_inconsistencies: List[Dict[str, Any]], 
                                 data_quality_issues: List[Dict[str, Any]]) -> float:
        """计算数据完整性评分（0-100分）"""
        if expected_count == 0:
            return 0.0
        
        # 数据完整性评分（40分）
        completeness_score = min(40, (actual_count / expected_count) * 40)
        
        # 数据准确性评分（30分）
        accuracy_penalty = len(duplicate_draws) + len(data_quality_issues)
        accuracy_score = max(0, 30 - accuracy_penalty)
        
        # 时间一致性评分（30分）
        time_penalty = len(time_inconsistencies)
        time_score = max(0, 30 - time_penalty)
        
        total_score = completeness_score + accuracy_score + time_score
        return round(total_score, 2)
    
    def _generate_recommendations(self, expected_count: int, actual_count: int,
                                missing_draws: List[str], duplicate_draws: List[str],
                                time_inconsistencies: List[Dict[str, Any]],
                                data_quality_issues: List[Dict[str, Any]]) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        # 数据完整性建议
        if actual_count < expected_count * 0.95:
            recommendations.append(f"数据完整性不足，缺失{expected_count - actual_count}期数据，建议进行补充回填")
        
        if missing_draws:
            recommendations.append(f"发现{len(missing_draws)}个缺失期号，建议针对性回填这些期数")
        
        if duplicate_draws:
            recommendations.append(f"发现{len(duplicate_draws)}个重复期号，建议清理重复数据")
        
        # 时间一致性建议
        if time_inconsistencies:
            recommendations.append(f"发现{len(time_inconsistencies)}个时间异常，建议校准时间同步机制")
        
        # 数据质量建议
        if data_quality_issues:
            recommendations.append(f"发现{len(data_quality_issues)}个数据质量问题，建议加强数据验证")
        
        # 通用建议
        if not recommendations:
            recommendations.append("数据完整性良好，建议继续保持当前的数据质量标准")
        
        return recommendations
    
    def generate_integrity_report_json(self, report: DataIntegrityReport, output_file: str = None) -> str:
        """生成JSON格式的完整性报告"""
        report_dict = {
            'check_timestamp': report.check_timestamp,
            'date_range': report.date_range,
            'summary': {
                'expected_draws_count': report.expected_draws_count,
                'actual_draws_count': report.actual_draws_count,
                'data_completeness_rate': f"{(report.actual_draws_count / report.expected_draws_count * 100):.2f}%" if report.expected_draws_count > 0 else "0%",
                'overall_integrity_score': f"{report.overall_integrity_score}/100"
            },
            'issues_found': {
                'missing_draws_count': len(report.missing_draws),
                'duplicate_draws_count': len(report.duplicate_draws),
                'time_inconsistencies_count': len(report.time_inconsistencies),
                'data_quality_issues_count': len(report.data_quality_issues)
            },
            'detailed_issues': {
                'missing_draws': report.missing_draws[:10],  # 只显示前10个
                'duplicate_draws': report.duplicate_draws,
                'time_inconsistencies': report.time_inconsistencies[:5],  # 只显示前5个
                'data_quality_issues': report.data_quality_issues[:5]  # 只显示前5个
            },
            'recommendations': report.recommendations
        }
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report_dict, f, indent=2, ensure_ascii=False)
            logger.info(f"完整性报告已保存到: {output_file}")
        
        return json.dumps(report_dict, indent=2, ensure_ascii=False)

def main():
    """测试数据完整性检查器"""
    checker = DataIntegrityChecker()
    
    try:
        # 检查最近3天的数据完整性
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        
        print(f"=== 检查 {start_date} 到 {end_date} 的数据完整性 ===")
        
        report = checker.check_historical_data_integrity(start_date, end_date)
        
        # 生成报告
        report_json = checker.generate_integrity_report_json(
            report, 
            f"data_integrity_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        print("\n=== 数据完整性报告 ===")
        print(report_json)
        
    except Exception as e:
        logger.error(f"数据完整性检查失败: {e}")

if __name__ == "__main__":
    main()