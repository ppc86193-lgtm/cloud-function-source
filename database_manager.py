#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理模块
提供数据库操作、存储功能和数据持久化
"""

import sqlite3
import json
import logging
import os
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from contextlib import contextmanager
from dataclasses import asdict

from models import (
    ComplexityMetrics, PerformanceProfile, OptimizationSuggestion,
    RiskAssessment, OptimizationResult, ComponentAnalysisReport
)

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, db_path: str = "performance_optimizer.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """初始化数据库"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 创建复杂度指标表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS complexity_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        component_id TEXT NOT NULL,
                        cyclomatic_complexity INTEGER,
                        cognitive_complexity INTEGER,
                        nesting_depth INTEGER,
                        lines_of_code INTEGER,
                        function_count INTEGER,
                        class_count INTEGER,
                        duplicate_lines INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(component_id)
                    )
                """)
                
                # 创建性能分析表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS performance_profiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        component_id TEXT NOT NULL,
                        execution_time REAL,
                        memory_peak REAL,
                        memory_average REAL,
                        cpu_usage REAL,
                        io_operations INTEGER,
                        function_calls TEXT,  -- JSON格式存储
                        hotspots TEXT,        -- JSON格式存储
                        bottlenecks TEXT,     -- JSON格式存储
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(component_id)
                    )
                """)
                
                # 创建优化建议表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS optimization_suggestions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        suggestion_id TEXT UNIQUE NOT NULL,
                        component_id TEXT NOT NULL,
                        suggestion_type TEXT NOT NULL,
                        description TEXT NOT NULL,
                        code_location TEXT,
                        original_code TEXT,
                        optimized_code TEXT,
                        impact_score REAL,
                        confidence REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建风险评估表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS risk_assessments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        suggestion_id TEXT NOT NULL,
                        risk_level TEXT NOT NULL,
                        risk_score REAL,
                        risk_factors TEXT,           -- JSON格式存储
                        requires_manual_review BOOLEAN,
                        mitigation_strategies TEXT,  -- JSON格式存储
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (suggestion_id) REFERENCES optimization_suggestions(suggestion_id)
                    )
                """)
                
                # 创建优化结果表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS optimization_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        suggestion_id TEXT NOT NULL,
                        success BOOLEAN NOT NULL,
                        applied_changes TEXT,
                        performance_improvement REAL,
                        backup_location TEXT,
                        error_message TEXT,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (suggestion_id) REFERENCES optimization_suggestions(suggestion_id)
                    )
                """)
                
                # 创建分析报告表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS analysis_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        component_id TEXT NOT NULL,
                        report_type TEXT NOT NULL,
                        report_data TEXT NOT NULL,  -- JSON格式存储完整报告
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建索引以提高查询性能
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_component_id ON complexity_metrics(component_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_performance_component ON performance_profiles(component_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_suggestion_component ON optimization_suggestions(component_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_suggestion_type ON optimization_suggestions(suggestion_type)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_risk_level ON risk_assessments(risk_level)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON optimization_suggestions(created_at)")
                
                conn.commit()
                logger.info("数据库初始化完成")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def save_complexity_metrics(self, metrics: ComplexityMetrics) -> bool:
        """保存复杂度指标"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT OR REPLACE INTO complexity_metrics (
                        component_id, cyclomatic_complexity, cognitive_complexity,
                        nesting_depth, lines_of_code, function_count, class_count,
                        duplicate_lines
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.component_id,
                    metrics.cyclomatic_complexity,
                    metrics.cognitive_complexity,
                    metrics.nesting_depth,
                    metrics.line_count,
                    metrics.function_count,
                    metrics.class_count,
                    metrics.duplicate_lines
                ))
                
                conn.commit()
                logger.info(f"复杂度指标已保存: {metrics.component_id}")
                return True
                
        except Exception as e:
            logger.error(f"保存复杂度指标失败 {metrics.component_id}: {e}")
            return False
    
    def save_performance_profile(self, profile: PerformanceProfile) -> bool:
        """保存性能分析结果"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT OR REPLACE INTO performance_profiles (
                        component_id, execution_time, memory_peak, memory_average,
                        cpu_usage, io_operations, function_calls, hotspots, bottlenecks
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    profile.component_id,
                    profile.execution_time,
                    profile.memory_peak,
                    profile.memory_average,
                    profile.cpu_usage,
                    profile.io_operations,
                    json.dumps(profile.function_calls),
                    json.dumps(profile.hotspots),
                    json.dumps(profile.bottlenecks)
                ))
                
                conn.commit()
                logger.info(f"性能分析结果已保存: {profile.component_id}")
                return True
                
        except Exception as e:
            logger.error(f"保存性能分析结果失败 {profile.component_id}: {e}")
            return False
    
    def save_optimization_suggestion(self, suggestion: OptimizationSuggestion) -> bool:
        """保存优化建议"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT OR REPLACE INTO optimization_suggestions (
                        suggestion_id, component_id, suggestion_type, description,
                        code_location, original_code, optimized_code, impact_score, confidence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    suggestion.suggestion_id,
                    suggestion.component_id,
                    suggestion.type,  # 使用type而不是suggestion_type
                    suggestion.description,
                    str(suggestion.code_location),  # 转换为字符串
                    suggestion.original_code,
                    suggestion.suggested_code,  # 使用suggested_code而不是optimized_code
                    suggestion.estimated_improvement,  # 使用estimated_improvement而不是impact_score
                    1.0  # 默认置信度，因为OptimizationSuggestion没有confidence字段
                ))
                
                conn.commit()
                logger.info(f"优化建议已保存: {suggestion.suggestion_id}")
                return True
                
        except Exception as e:
            logger.error(f"保存优化建议失败 {suggestion.suggestion_id}: {e}")
            return False
    
    def save_risk_assessment(self, assessment: RiskAssessment) -> bool:
        """保存风险评估"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT OR REPLACE INTO risk_assessments (
                        suggestion_id, risk_level, risk_score, risk_factors,
                        requires_manual_review, mitigation_strategies
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    assessment.risk_id,  # 使用risk_id作为suggestion_id
                    assessment.risk_level,
                    assessment.get_risk_score(),  # 使用方法获取风险评分
                    json.dumps(assessment.risk_factors),
                    assessment.requires_manual_review,
                    json.dumps(assessment.mitigation_strategies)
                ))
                
                conn.commit()
                logger.info(f"风险评估已保存: {assessment.suggestion_id}")
                return True
                
        except Exception as e:
            logger.error(f"保存风险评估失败 {assessment.suggestion_id}: {e}")
            return False
    
    def save_optimization_result(self, result: OptimizationResult) -> bool:
        """保存优化结果"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT INTO optimization_results (
                        suggestion_id, success, applied_changes, performance_improvement,
                        backup_location, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    result.suggestion_id,
                    result.success,
                    result.applied_changes,
                    result.performance_improvement,
                    result.backup_location,
                    result.error_message
                ))
                
                conn.commit()
                logger.info(f"优化结果已保存: {result.suggestion_id}")
                return True
                
        except Exception as e:
            logger.error(f"保存优化结果失败 {result.suggestion_id}: {e}")
            return False
    
    def get_complexity_metrics(self, component_id: str) -> Optional[ComplexityMetrics]:
        """获取复杂度指标"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT * FROM complexity_metrics WHERE component_id = ?
                """, (component_id,))
                
                row = cursor.fetchone()
                if row:
                    return ComplexityMetrics(
                        component_id=row['component_id'],
                        cyclomatic_complexity=row['cyclomatic_complexity'],
                        cognitive_complexity=row['cognitive_complexity'],
                        nesting_depth=row['nesting_depth'],
                        line_count=row['lines_of_code'],
                        function_count=row['function_count'],
                        class_count=row['class_count'],
                        duplicate_lines=row['duplicate_lines'],
                        comment_ratio=0.0  # 默认值，因为数据库表中没有此列
                    )
                
        except Exception as e:
            logger.error(f"获取复杂度指标失败 {component_id}: {e}")
        
        return None
    
    def get_performance_profile(self, component_id: str) -> Optional[PerformanceProfile]:
        """获取性能分析结果"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT * FROM performance_profiles WHERE component_id = ?
                """, (component_id,))
                
                row = cursor.fetchone()
                if row:
                    return PerformanceProfile(
                        component_id=row['component_id'],
                        execution_time=row['execution_time'],
                        memory_peak=row['memory_peak'],
                        memory_average=row['memory_average'],
                        cpu_usage=row['cpu_usage'],
                        io_operations=row['io_operations'],
                        function_calls=json.loads(row['function_calls']) if row['function_calls'] else {},
                        hotspots=json.loads(row['hotspots']) if row['hotspots'] else [],
                        bottlenecks=json.loads(row['bottlenecks']) if row['bottlenecks'] else []
                    )
                
        except Exception as e:
            logger.error(f"获取性能分析结果失败 {component_id}: {e}")
        
        return None
    
    def get_optimization_suggestions(self, component_id: Optional[str] = None,
                                   suggestion_type: Optional[str] = None,
                                   limit: Optional[int] = None) -> List[OptimizationSuggestion]:
        """获取优化建议"""
        suggestions = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM optimization_suggestions WHERE 1=1"
                params = []
                
                if component_id:
                    query += " AND component_id = ?"
                    params.append(component_id)
                
                if suggestion_type:
                    query += " AND suggestion_type = ?"
                    params.append(suggestion_type)
                
                query += " ORDER BY created_at DESC"
                
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)
                
                cursor.execute(query, params)
                
                for row in cursor.fetchall():
                    suggestions.append(OptimizationSuggestion(
                        suggestion_id=row['suggestion_id'],
                        component_id=row['component_id'],
                        category=row['suggestion_type'],
                        priority="medium",
                        description=row['description'],
                        code_location=row['code_location'],
                        original_code=row['original_code'],
                        suggested_code=row['optimized_code'],
                        estimated_improvement=row['impact_score'],
                        risk_level="low",
                        auto_applicable=True,
                        reasoning=row['description'],
                        type=row['suggestion_type'],
                        impact="medium",
                        effort="low",
                        code_example=row['original_code']
                    ))
                
        except Exception as e:
            logger.error(f"获取优化建议失败: {e}")
        
        return suggestions
    
    def get_risk_assessments(self, suggestion_ids: Optional[List[str]] = None) -> List[RiskAssessment]:
        """获取风险评估"""
        assessments = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if suggestion_ids:
                    placeholders = ','.join('?' * len(suggestion_ids))
                    query = f"SELECT * FROM risk_assessments WHERE suggestion_id IN ({placeholders})"
                    cursor.execute(query, suggestion_ids)
                else:
                    cursor.execute("SELECT * FROM risk_assessments ORDER BY created_at DESC")
                
                for row in cursor.fetchall():
                    assessments.append(RiskAssessment(
                        suggestion_id=row['suggestion_id'],
                        risk_level=row['risk_level'],
                        risk_score=row['risk_score'],
                        risk_factors=json.loads(row['risk_factors']) if row['risk_factors'] else [],
                        requires_manual_review=bool(row['requires_manual_review']),
                        mitigation_strategies=json.loads(row['mitigation_strategies']) if row['mitigation_strategies'] else []
                    ))
                
        except Exception as e:
            logger.error(f"获取风险评估失败: {e}")
        
        return assessments
    
    def get_high_risk_suggestions(self) -> List[Tuple[OptimizationSuggestion, RiskAssessment]]:
        """获取需要手动确认的高风险建议"""
        high_risk_suggestions = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT s.*, r.risk_level, r.risk_score, r.risk_factors,
                           r.requires_manual_review, r.mitigation_strategies
                    FROM optimization_suggestions s
                    JOIN risk_assessments r ON s.suggestion_id = r.suggestion_id
                    WHERE r.requires_manual_review = 1
                    ORDER BY r.risk_score DESC
                """)
                
                for row in cursor.fetchall():
                    suggestion = OptimizationSuggestion(
                        suggestion_id=row['suggestion_id'],
                        component_id=row['component_id'],
                        category=row['suggestion_type'],
                        priority="high",
                        description=row['description'],
                        code_location=row['code_location'],
                        original_code=row['original_code'],
                        suggested_code=row['optimized_code'],
                        estimated_improvement=row['impact_score'],
                        risk_level=row['risk_level'],
                        auto_applicable=not bool(row['requires_manual_review']),
                        reasoning=row['description'],
                        type=row['suggestion_type'],
                        impact="high",
                        effort="medium",
                        code_example=row['original_code']
                    )
                    
                    assessment = RiskAssessment(
                        suggestion_id=row['suggestion_id'],
                        risk_level=row['risk_level'],
                        risk_score=row['risk_score'],
                        risk_factors=json.loads(row['risk_factors']) if row['risk_factors'] else [],
                        requires_manual_review=bool(row['requires_manual_review']),
                        mitigation_strategies=json.loads(row['mitigation_strategies']) if row['mitigation_strategies'] else []
                    )
                    
                    high_risk_suggestions.append((suggestion, assessment))
                
        except Exception as e:
            logger.error(f"获取高风险建议失败: {e}")
        
        return high_risk_suggestions
    
    def get_optimization_results(self, suggestion_id: Optional[str] = None) -> List[OptimizationResult]:
        """获取优化结果"""
        results = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if suggestion_id:
                    cursor.execute("""
                        SELECT * FROM optimization_results WHERE suggestion_id = ?
                        ORDER BY applied_at DESC
                    """, (suggestion_id,))
                else:
                    cursor.execute("""
                        SELECT * FROM optimization_results ORDER BY applied_at DESC
                    """)
                
                for row in cursor.fetchall():
                    results.append(OptimizationResult(
                        suggestion_id=row['suggestion_id'],
                        success=bool(row['success']),
                        applied_changes=row['applied_changes'],
                        performance_improvement=row['performance_improvement'],
                        backup_location=row['backup_location'],
                        error_message=row['error_message']
                    ))
                
        except Exception as e:
            logger.error(f"获取优化结果失败: {e}")
        
        return results
    
    def save_analysis_report(self, component_id: str, report_type: str, report_data: Dict[str, Any]) -> bool:
        """保存分析报告"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT INTO analysis_reports (
                        component_id, report_type, report_data
                    ) VALUES (?, ?, ?)
                """, (
                    component_id,
                    report_type,
                    json.dumps(report_data, ensure_ascii=False, indent=2)
                ))
                
                conn.commit()
                logger.info(f"分析报告已保存: {component_id} - {report_type}")
                return True
                
        except Exception as e:
            logger.error(f"保存分析报告失败 {component_id}: {e}")
            return False
    
    def get_analysis_reports(self, component_id: Optional[str] = None,
                           report_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取分析报告"""
        reports = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM analysis_reports WHERE 1=1"
                params = []
                
                if component_id:
                    query += " AND component_id = ?"
                    params.append(component_id)
                
                if report_type:
                    query += " AND report_type = ?"
                    params.append(report_type)
                
                query += " ORDER BY created_at DESC"
                
                cursor.execute(query, params)
                
                for row in cursor.fetchall():
                    reports.append({
                        'id': row['id'],
                        'component_id': row['component_id'],
                        'report_type': row['report_type'],
                        'report_data': json.loads(row['report_data']),
                        'created_at': row['created_at']
                    })
                
        except Exception as e:
            logger.error(f"获取分析报告失败: {e}")
        
        return reports
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {}
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 统计建议数量
                cursor.execute("SELECT COUNT(*) as count FROM optimization_suggestions")
                stats['total_suggestions'] = cursor.fetchone()['count']
                
                # 按类型统计建议
                cursor.execute("""
                    SELECT suggestion_type, COUNT(*) as count 
                    FROM optimization_suggestions 
                    GROUP BY suggestion_type
                """)
                stats['suggestions_by_type'] = {row['suggestion_type']: row['count'] for row in cursor.fetchall()}
                
                # 按风险等级统计
                cursor.execute("""
                    SELECT risk_level, COUNT(*) as count 
                    FROM risk_assessments 
                    GROUP BY risk_level
                """)
                stats['suggestions_by_risk'] = {row['risk_level']: row['count'] for row in cursor.fetchall()}
                
                # 统计需要手动确认的建议
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM risk_assessments 
                    WHERE requires_manual_review = 1
                """)
                stats['manual_confirmation_required'] = cursor.fetchone()['count']
                
                # 统计优化结果
                cursor.execute("""
                    SELECT success, COUNT(*) as count 
                    FROM optimization_results 
                    GROUP BY success
                """)
                result_stats = {bool(row['success']): row['count'] for row in cursor.fetchall()}
                stats['optimization_success_rate'] = {
                    'successful': result_stats.get(True, 0),
                    'failed': result_stats.get(False, 0)
                }
                
                # 平均影响分数
                cursor.execute("SELECT AVG(impact_score) as avg_impact FROM optimization_suggestions")
                avg_impact = cursor.fetchone()['avg_impact']
                stats['average_impact_score'] = round(avg_impact, 2) if avg_impact else 0
                
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
        
        return stats
    
    def cleanup_old_data(self, days: int = 30) -> bool:
        """清理旧数据"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 删除旧的分析报告
                cursor.execute("""
                    DELETE FROM analysis_reports 
                    WHERE created_at < datetime('now', '-{} days')
                """.format(days))
                
                deleted_reports = cursor.rowcount
                
                # 删除旧的优化结果
                cursor.execute("""
                    DELETE FROM optimization_results 
                    WHERE applied_at < datetime('now', '-{} days')
                """.format(days))
                
                deleted_results = cursor.rowcount
                
                conn.commit()
                
                logger.info(f"清理完成: 删除了 {deleted_reports} 个报告和 {deleted_results} 个结果")
                return True
                
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")
            return False
    
    def export_data(self, output_file: str) -> bool:
        """导出数据到JSON文件"""
        try:
            export_data = {
                'complexity_metrics': [],
                'performance_profiles': [],
                'optimization_suggestions': [],
                'risk_assessments': [],
                'optimization_results': [],
                'analysis_reports': []
            }
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 导出各个表的数据
                for table_name in export_data.keys():
                    cursor.execute(f"SELECT * FROM {table_name}")
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        row_dict = dict(row)
                        # 处理JSON字段
                        for key, value in row_dict.items():
                            if key in ['function_calls', 'hotspots', 'bottlenecks', 'risk_factors', 'mitigation_strategies', 'report_data']:
                                if value:
                                    try:
                                        row_dict[key] = json.loads(value)
                                    except:
                                        pass
                        
                        export_data[table_name].append(row_dict)
            
            # 写入文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"数据导出完成: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"数据导出失败: {e}")
            return False
    
    def backup_database(self, backup_path: str) -> bool:
        """备份数据库"""
        try:
            import shutil
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"数据库备份完成: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"数据库备份失败: {e}")
            return False