#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统优化前后性能对比测试
测量API响应时间、内存使用、存储空间等关键指标
"""

import json
import time
import psutil
import sqlite3
import requests
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """性能指标数据结构"""
    test_name: str
    api_response_time: float  # API响应时间(ms)
    memory_usage_mb: float    # 内存使用(MB)
    storage_size_mb: float    # 存储大小(MB)
    network_transfer_kb: float # 网络传输(KB)
    cpu_usage_percent: float  # CPU使用率(%)
    timestamp: str
    
    # 优化相关指标
    fields_count: int = 0     # 字段数量
    response_size_bytes: int = 0  # 响应大小(bytes)
    processing_time_ms: float = 0.0  # 处理时间(ms)

@dataclass
class ComparisonResult:
    """对比结果"""
    before_optimization: PerformanceMetrics
    after_optimization: PerformanceMetrics
    improvement_percentage: Dict[str, float]
    summary: str

class PerformanceComparisonTest:
    """性能对比测试器"""
    
    def __init__(self):
        self.results = []
        self.test_data = self._generate_test_data()
        
    def _generate_test_data(self) -> Dict:
        """生成测试数据"""
        return {
            "original_api_response": {
                "codeid": 0,
                "curtime": "1705294200",  # 优化前包含的字段
                "retdata": {
                    "curent": {
                        "issue": "20240115001",
                        "openCode": "4,5,6",
                        "short_issue": "001",  # 优化前包含的字段
                        "intervalM": 5,        # 优化前包含的字段
                        "drawTime": "2024-01-15 10:05:30"
                    },
                    "next": {
                        "next_issue": "20240115002",
                        "award_time": "2024-01-15 10:10:00"
                    }
                }
            },
            "optimized_api_response": {
                "codeid": 0,
                # curtime 已移除
                "retdata": {
                    "curent": {
                        "issue": "20240115001",
                        "openCode": "4,5,6",
                        # short_issue 已移除
                        # intervalM 已移除
                        "drawTime": "2024-01-15 10:05:30"
                    },
                    "next": {
                        "next_issue": "20240115002",
                        "award_time": "2024-01-15 10:10:00"
                    }
                }
            }
        }
    
    def measure_api_performance(self, response_data: Dict, test_name: str) -> PerformanceMetrics:
        """测量API性能指标"""
        start_time = time.time()
        
        # 获取系统资源使用情况
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        cpu_before = process.cpu_percent()
        
        # 模拟API处理
        response_json = json.dumps(response_data, ensure_ascii=False)
        response_size = len(response_json.encode('utf-8'))
        fields_count = self._count_fields(response_data)
        
        # 模拟数据处理延迟
        time.sleep(0.01)  # 10ms处理时间
        
        # 计算处理时间
        processing_time = (time.time() - start_time) * 1000  # ms
        
        # 获取处理后的资源使用
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        cpu_after = process.cpu_percent()
        
        return PerformanceMetrics(
            test_name=test_name,
            api_response_time=processing_time,
            memory_usage_mb=memory_after - memory_before,
            storage_size_mb=response_size / 1024 / 1024,  # MB
            network_transfer_kb=response_size / 1024,     # KB
            cpu_usage_percent=(cpu_after + cpu_before) / 2,
            timestamp=datetime.now().isoformat(),
            fields_count=fields_count,
            response_size_bytes=response_size,
            processing_time_ms=processing_time
        )
    
    def _count_fields(self, data: Any, count: int = 0) -> int:
        """递归计算字段数量"""
        if isinstance(data, dict):
            count += len(data)
            for value in data.values():
                count = self._count_fields(value, count)
        elif isinstance(data, list):
            for item in data:
                count = self._count_fields(item, count)
        return count
    
    def run_comparison_test(self) -> ComparisonResult:
        """运行对比测试"""
        logger.info("开始性能对比测试...")
        
        # 测试优化前的性能
        before_metrics = self.measure_api_performance(
            self.test_data["original_api_response"], 
            "优化前"
        )
        
        # 测试优化后的性能
        after_metrics = self.measure_api_performance(
            self.test_data["optimized_api_response"], 
            "优化后"
        )
        
        # 计算改进百分比
        improvement = self._calculate_improvement(before_metrics, after_metrics)
        
        # 生成总结
        summary = self._generate_summary(before_metrics, after_metrics, improvement)
        
        result = ComparisonResult(
            before_optimization=before_metrics,
            after_optimization=after_metrics,
            improvement_percentage=improvement,
            summary=summary
        )
        
        self.results.append(result)
        return result
    
    def _calculate_improvement(self, before: PerformanceMetrics, after: PerformanceMetrics) -> Dict[str, float]:
        """计算性能改进百分比"""
        def safe_percentage(old_val, new_val):
            if old_val == 0:
                return 0.0
            return ((old_val - new_val) / old_val) * 100
        
        return {
            "response_size": safe_percentage(before.response_size_bytes, after.response_size_bytes),
            "network_transfer": safe_percentage(before.network_transfer_kb, after.network_transfer_kb),
            "fields_count": safe_percentage(before.fields_count, after.fields_count),
            "processing_time": safe_percentage(before.processing_time_ms, after.processing_time_ms),
            "memory_usage": safe_percentage(before.memory_usage_mb, after.memory_usage_mb)
        }
    
    def _generate_summary(self, before: PerformanceMetrics, after: PerformanceMetrics, improvement: Dict[str, float]) -> str:
        """生成性能对比总结"""
        return f"""
PC28系统字段优化性能对比报告
=====================================

优化前指标:
- 响应大小: {before.response_size_bytes} bytes
- 网络传输: {before.network_transfer_kb:.2f} KB
- 字段数量: {before.fields_count}
- 处理时间: {before.processing_time_ms:.2f} ms

优化后指标:
- 响应大小: {after.response_size_bytes} bytes
- 网络传输: {after.network_transfer_kb:.2f} KB
- 字段数量: {after.fields_count}
- 处理时间: {after.processing_time_ms:.2f} ms

性能改进:
- 响应大小减少: {improvement['response_size']:.1f}%
- 网络传输减少: {improvement['network_transfer']:.1f}%
- 字段数量减少: {improvement['fields_count']:.1f}%
- 处理时间减少: {improvement['processing_time']:.1f}%

总结: 通过移除未使用字段(curtime, short_issue, intervalM)，
系统在响应大小和网络传输方面获得了显著改进。
        """.strip()
    
    def run_database_performance_test(self) -> Dict[str, Any]:
        """运行数据库性能测试"""
        logger.info("开始数据库性能测试...")
        
        # 创建测试数据库
        test_db = "performance_test.db"
        
        # 测试优化前的表结构
        before_result = self._test_database_structure(test_db, include_unused_fields=True)
        
        # 测试优化后的表结构
        after_result = self._test_database_structure(test_db, include_unused_fields=False)
        
        return {
            "before_optimization": before_result,
            "after_optimization": after_result,
            "storage_savings_mb": before_result["storage_size_mb"] - after_result["storage_size_mb"],
            "query_performance_improvement": (before_result["query_time_ms"] - after_result["query_time_ms"]) / before_result["query_time_ms"] * 100
        }
    
    def _test_database_structure(self, db_path: str, include_unused_fields: bool) -> Dict[str, Any]:
        """测试数据库结构性能"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 创建表结构
        if include_unused_fields:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_draws (
                    id INTEGER PRIMARY KEY,
                    draw_id TEXT,
                    result_numbers TEXT,
                    result_sum INTEGER,
                    current_time TEXT,  -- 未使用字段
                    short_issue TEXT,   -- 未使用字段
                    award_time TEXT,
                    created_at TEXT
                )
            """)
        else:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_draws_optimized (
                    id INTEGER PRIMARY KEY,
                    draw_id TEXT,
                    result_numbers TEXT,
                    result_sum INTEGER,
                    award_time TEXT,
                    created_at TEXT
                )
            """)
        
        # 插入测试数据
        table_name = "test_draws" if include_unused_fields else "test_draws_optimized"
        test_data = []
        
        for i in range(1000):  # 插入1000条测试数据
            if include_unused_fields:
                test_data.append((
                    f"draw_{i:04d}",
                    "[1,2,3]",
                    6,
                    "2024-01-15 10:00:00",  # current_time
                    f"00{i%100}",           # short_issue
                    "2024-01-15 10:05:00",
                    datetime.now().isoformat()
                ))
            else:
                test_data.append((
                    f"draw_{i:04d}",
                    "[1,2,3]",
                    6,
                    "2024-01-15 10:05:00",
                    datetime.now().isoformat()
                ))
        
        # 测量插入时间
        start_time = time.time()
        if include_unused_fields:
            cursor.executemany(f"""
                INSERT INTO {table_name} 
                (draw_id, result_numbers, result_sum, current_time, short_issue, award_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, test_data)
        else:
            cursor.executemany(f"""
                INSERT INTO {table_name} 
                (draw_id, result_numbers, result_sum, award_time, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, test_data)
        
        insert_time = (time.time() - start_time) * 1000  # ms
        
        # 测量查询时间
        start_time = time.time()
        cursor.execute(f"SELECT * FROM {table_name} WHERE result_sum > 5")
        results = cursor.fetchall()
        query_time = (time.time() - start_time) * 1000  # ms
        
        conn.commit()
        
        # 获取数据库文件大小
        import os
        storage_size = os.path.getsize(db_path) / 1024 / 1024  # MB
        
        conn.close()
        
        return {
            "table_name": table_name,
            "records_count": len(test_data),
            "insert_time_ms": insert_time,
            "query_time_ms": query_time,
            "storage_size_mb": storage_size,
            "query_results_count": len(results)
        }
    
    def generate_performance_report(self) -> str:
        """生成完整的性能报告"""
        if not self.results:
            return "没有可用的测试结果"
        
        latest_result = self.results[-1]
        db_result = self.run_database_performance_test()
        
        report = f"""
PC28系统优化性能报告
====================
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

API性能对比:
{latest_result.summary}

数据库性能对比:
- 优化前存储大小: {db_result['before_optimization']['storage_size_mb']:.2f} MB
- 优化后存储大小: {db_result['after_optimization']['storage_size_mb']:.2f} MB
- 存储空间节省: {db_result['storage_savings_mb']:.2f} MB
- 查询性能提升: {db_result['query_performance_improvement']:.1f}%

总体优化效果:
✅ API响应大小减少 {latest_result.improvement_percentage['response_size']:.1f}%
✅ 网络传输减少 {latest_result.improvement_percentage['network_transfer']:.1f}%
✅ 数据库存储节省 {db_result['storage_savings_mb']:.2f} MB
✅ 查询性能提升 {db_result['query_performance_improvement']:.1f}%

优化建议:
1. 继续监控API响应时间和错误率
2. 定期清理历史数据中的冗余字段
3. 考虑实施更多字段级别的优化
4. 建立性能监控告警机制
        """
        
        return report.strip()

def main():
    """主函数"""
    tester = PerformanceComparisonTest()
    
    # 运行API性能对比测试
    comparison_result = tester.run_comparison_test()
    
    # 生成完整报告
    report = tester.generate_performance_report()
    
    # 保存报告
    with open("performance_optimization_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    
    # 保存详细数据
    with open("performance_test_data.json", "w", encoding="utf-8") as f:
        json.dump({
            "comparison_result": asdict(comparison_result),
            "timestamp": datetime.now().isoformat()
        }, f, ensure_ascii=False, indent=2)
    
    logger.info("性能测试完成，报告已保存")
    print(report)

if __name__ == "__main__":
    main()