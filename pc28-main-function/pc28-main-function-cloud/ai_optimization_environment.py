#!/usr/bin/env python3
"""
AI优化环境准备系统
为几百张表的AI优化做好完整准备，包括表结构分析、依赖关系映射、优化策略制定
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
from google.cloud import bigquery
import networkx as nx
import pandas as pd
from collections import defaultdict
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/a606/cloud_function_source/logs/ai_optimization.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AIOptimizationEnvironment:
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.local_data_dir = "/Users/a606/cloud_function_source/local_data"
        self.ai_analysis_db_path = "/Users/a606/cloud_function_source/local_data/ai_analysis.db"
        self.optimization_config_path = "/Users/a606/cloud_function_source/ai_optimization_config.json"
        
        # 确保目录存在
        os.makedirs(self.local_data_dir, exist_ok=True)
        os.makedirs("/Users/a606/cloud_function_source/logs", exist_ok=True)
        
        # 初始化BigQuery客户端
        self.bq_client = bigquery.Client(project=self.project_id)
        
        # 初始化分析数据库
        self.init_analysis_database()
        
        # 依赖关系图
        self.dependency_graph = nx.DiGraph()
        
    def init_analysis_database(self):
        """初始化AI分析数据库"""
        try:
            conn = sqlite3.connect(self.ai_analysis_db_path)
            cursor = conn.cursor()
            
            # 创建表结构分析表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS table_analysis (
                    table_name TEXT PRIMARY KEY,
                    table_type TEXT,
                    row_count INTEGER,
                    column_count INTEGER,
                    schema_json TEXT,
                    complexity_score REAL,
                    optimization_priority INTEGER,
                    dependencies TEXT,
                    business_category TEXT,
                    data_freshness TEXT,
                    query_patterns TEXT,
                    performance_metrics TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            
            # 创建字段分析表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS field_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT,
                    field_name TEXT,
                    field_type TEXT,
                    field_mode TEXT,
                    is_nullable BOOLEAN,
                    is_key_field BOOLEAN,
                    cardinality INTEGER,
                    null_percentage REAL,
                    data_quality_score REAL,
                    usage_frequency REAL,
                    optimization_suggestions TEXT,
                    created_at TEXT
                )
            ''')
            
            # 创建依赖关系表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS table_dependencies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_table TEXT,
                    target_table TEXT,
                    dependency_type TEXT,
                    relationship_strength REAL,
                    join_conditions TEXT,
                    created_at TEXT
                )
            ''')
            
            # 创建优化建议表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS optimization_suggestions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT,
                    suggestion_type TEXT,
                    priority INTEGER,
                    description TEXT,
                    expected_impact TEXT,
                    implementation_complexity INTEGER,
                    estimated_savings TEXT,
                    status TEXT,
                    created_at TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("AI分析数据库初始化完成")
            
        except Exception as e:
            logger.error(f"初始化AI分析数据库失败: {e}")
    
    def discover_all_tables(self) -> List[Dict[str, Any]]:
        """发现所有表和视图"""
        logger.info("开始发现所有表和视图")
        
        tables_info = []
        
        try:
            # 获取所有表
            query = f"""
            SELECT 
                table_name,
                table_type,
                COALESCE(row_count, 0) as row_count,
                creation_time,
                last_modified_time
            FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.TABLES`
            ORDER BY table_name
            """
            
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            for row in results:
                table_info = {
                    "table_name": row.table_name,
                    "table_type": row.table_type,
                    "row_count": row.row_count,
                    "creation_time": row.creation_time.isoformat() if row.creation_time else None,
                    "last_modified_time": row.last_modified_time.isoformat() if row.last_modified_time else None
                }
                tables_info.append(table_info)
            
            logger.info(f"发现 {len(tables_info)} 个表和视图")
            return tables_info
            
        except Exception as e:
            logger.error(f"发现表失败: {e}")
            return []
    
    def analyze_table_schema(self, table_name: str) -> Dict[str, Any]:
        """分析表结构"""
        try:
            # 获取表的schema信息
            query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                is_partitioning_column,
                clustering_ordinal_position
            FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            schema_info = []
            key_fields = []
            
            for row in results:
                field_info = {
                    "column_name": row.column_name,
                    "data_type": row.data_type,
                    "is_nullable": row.is_nullable == "YES",
                    "is_partitioning_column": row.is_partitioning_column,
                    "clustering_ordinal_position": row.clustering_ordinal_position
                }
                schema_info.append(field_info)
                
                # 识别关键字段
                if any(keyword in row.column_name.lower() for keyword in ['id', 'key', 'period', 'timestamp', 'time']):
                    key_fields.append(row.column_name)
            
            # 计算复杂度分数
            complexity_score = self.calculate_complexity_score(schema_info)
            
            return {
                "table_name": table_name,
                "column_count": len(schema_info),
                "schema": schema_info,
                "key_fields": key_fields,
                "complexity_score": complexity_score
            }
            
        except Exception as e:
            logger.error(f"分析表 {table_name} 结构失败: {e}")
            return {}
    
    def calculate_complexity_score(self, schema_info: List[Dict[str, Any]]) -> float:
        """计算表复杂度分数"""
        if not schema_info:
            return 0.0
        
        score = 0.0
        
        # 基础分数：字段数量
        score += len(schema_info) * 0.1
        
        # 数据类型复杂度
        complex_types = ['STRUCT', 'ARRAY', 'JSON', 'GEOGRAPHY']
        for field in schema_info:
            if any(ct in field['data_type'] for ct in complex_types):
                score += 0.5
        
        # 可空字段比例
        nullable_count = sum(1 for field in schema_info if field['is_nullable'])
        nullable_ratio = nullable_count / len(schema_info)
        score += nullable_ratio * 0.3
        
        # 分区和聚集字段
        partitioned_fields = sum(1 for field in schema_info if field['is_partitioning_column'])
        clustered_fields = sum(1 for field in schema_info if field['clustering_ordinal_position'])
        score += (partitioned_fields + clustered_fields) * 0.2
        
        return round(score, 2)
    
    def analyze_table_dependencies(self, table_name: str) -> List[Dict[str, Any]]:
        """分析表依赖关系"""
        dependencies = []
        
        try:
            # 如果是视图，分析其定义中的依赖
            query = f"""
            SELECT view_definition
            FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.VIEWS`
            WHERE table_name = '{table_name}'
            """
            
            query_job = self.bq_client.query(query)
            results = list(query_job.result())
            
            if results:
                view_definition = results[0].view_definition
                
                # 使用正则表达式提取表引用
                table_pattern = r'`([^`]+\.[^`]+\.[^`]+)`|FROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
                matches = re.findall(table_pattern, view_definition, re.IGNORECASE)
                
                for match in matches:
                    referenced_table = match[0] or match[1]
                    if referenced_table and referenced_table != f"{self.project_id}.{self.dataset_id}.{table_name}":
                        # 提取表名
                        table_parts = referenced_table.split('.')
                        if len(table_parts) >= 3:
                            dep_table_name = table_parts[-1]
                            dependencies.append({
                                "source_table": table_name,
                                "target_table": dep_table_name,
                                "dependency_type": "view_reference",
                                "relationship_strength": 1.0
                            })
            
            return dependencies
            
        except Exception as e:
            logger.error(f"分析表 {table_name} 依赖关系失败: {e}")
            return []
    
    def categorize_business_function(self, table_name: str, schema_info: List[Dict[str, Any]]) -> str:
        """根据表名和字段推断业务功能分类"""
        table_lower = table_name.lower()
        field_names = [field['column_name'].lower() for field in schema_info]
        
        # 预测相关表
        if any(keyword in table_lower for keyword in ['pred', 'prediction', 'forecast', 'p_']):
            if 'cloud' in table_lower:
                return "cloud_prediction"
            elif 'map' in table_lower:
                return "map_prediction"
            elif 'size' in table_lower:
                return "size_prediction"
            elif 'combo' in table_lower:
                return "combo_prediction"
            elif 'ensemble' in table_lower:
                return "ensemble_prediction"
            else:
                return "general_prediction"
        
        # 数据采集相关
        if any(keyword in table_lower for keyword in ['raw', 'collect', 'crawl', 'fetch']):
            return "data_collection"
        
        # 标签和训练数据
        if any(keyword in table_lower for keyword in ['label', 'train', 'feature']):
            return "training_data"
        
        # 结果和统计
        if any(keyword in table_lower for keyword in ['result', 'stat', 'summary', 'report']):
            return "analytics_reporting"
        
        # 配置和元数据
        if any(keyword in table_lower for keyword in ['config', 'meta', 'setting']):
            return "configuration"
        
        # 实时数据
        if any(keyword in table_lower for keyword in ['real', 'live', 'current', 'today']):
            return "realtime_data"
        
        return "general"
    
    def generate_optimization_suggestions(self, table_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """生成优化建议"""
        suggestions = []
        table_name = table_analysis.get("table_name", "")
        complexity_score = table_analysis.get("complexity_score", 0)
        column_count = table_analysis.get("column_count", 0)
        row_count = table_analysis.get("row_count", 0)
        
        # 基于复杂度的建议
        if complexity_score > 3.0:
            suggestions.append({
                "suggestion_type": "schema_optimization",
                "priority": 1,
                "description": f"表 {table_name} 复杂度较高 ({complexity_score})，建议简化schema结构",
                "expected_impact": "提高查询性能，减少存储成本",
                "implementation_complexity": 3
            })
        
        # 基于字段数量的建议
        if column_count > 50:
            suggestions.append({
                "suggestion_type": "column_reduction",
                "priority": 2,
                "description": f"表 {table_name} 字段过多 ({column_count})，建议移除不必要的字段",
                "expected_impact": "减少存储空间，提高查询速度",
                "implementation_complexity": 2
            })
        
        # 基于行数的建议
        if row_count > 1000000:
            suggestions.append({
                "suggestion_type": "partitioning",
                "priority": 1,
                "description": f"表 {table_name} 数据量大 ({row_count:,} 行)，建议添加分区",
                "expected_impact": "显著提高查询性能",
                "implementation_complexity": 2
            })
        
        # 基于表名的特定建议
        if "today" in table_name.lower():
            suggestions.append({
                "suggestion_type": "data_retention",
                "priority": 2,
                "description": f"表 {table_name} 包含当日数据，建议设置数据保留策略",
                "expected_impact": "控制存储成本",
                "implementation_complexity": 1
            })
        
        if "pred" in table_name.lower() or "prediction" in table_name.lower():
            suggestions.append({
                "suggestion_type": "model_optimization",
                "priority": 1,
                "description": f"预测表 {table_name} 建议优化预测模型和特征工程",
                "expected_impact": "提高预测准确性",
                "implementation_complexity": 4
            })
        
        return suggestions
    
    def run_comprehensive_analysis(self) -> Dict[str, Any]:
        """运行全面分析"""
        logger.info("开始运行全面的AI优化环境分析")
        
        analysis_results = {
            "analysis_time": datetime.now().isoformat(),
            "total_tables": 0,
            "analyzed_tables": 0,
            "failed_analyses": 0,
            "business_categories": defaultdict(int),
            "complexity_distribution": defaultdict(int),
            "optimization_priorities": defaultdict(int),
            "total_suggestions": 0
        }
        
        # 发现所有表
        all_tables = self.discover_all_tables()
        analysis_results["total_tables"] = len(all_tables)
        
        conn = sqlite3.connect(self.ai_analysis_db_path)
        cursor = conn.cursor()
        
        for table_info in all_tables:
            table_name = table_info["table_name"]
            
            try:
                logger.info(f"分析表: {table_name}")
                
                # 分析表结构
                schema_analysis = self.analyze_table_schema(table_name)
                if not schema_analysis:
                    analysis_results["failed_analyses"] += 1
                    continue
                
                # 分析依赖关系
                dependencies = self.analyze_table_dependencies(table_name)
                
                # 业务功能分类
                business_category = self.categorize_business_function(
                    table_name, schema_analysis.get("schema", [])
                )
                
                # 生成优化建议
                table_analysis = {
                    **table_info,
                    **schema_analysis,
                    "business_category": business_category,
                    "dependencies": dependencies
                }
                
                suggestions = self.generate_optimization_suggestions(table_analysis)
                
                # 保存分析结果
                cursor.execute('''
                    INSERT OR REPLACE INTO table_analysis 
                    (table_name, table_type, row_count, column_count, schema_json, 
                     complexity_score, optimization_priority, dependencies, business_category,
                     data_freshness, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    table_name,
                    table_info.get("table_type", ""),
                    table_info.get("row_count", 0),
                    schema_analysis.get("column_count", 0),
                    json.dumps(schema_analysis.get("schema", [])),
                    schema_analysis.get("complexity_score", 0),
                    1 if schema_analysis.get("complexity_score", 0) > 2.0 else 2,
                    json.dumps(dependencies),
                    business_category,
                    table_info.get("last_modified_time", ""),
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                ))
                
                # 保存优化建议
                for suggestion in suggestions:
                    cursor.execute('''
                        INSERT INTO optimization_suggestions 
                        (table_name, suggestion_type, priority, description, expected_impact,
                         implementation_complexity, status, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        table_name,
                        suggestion["suggestion_type"],
                        suggestion["priority"],
                        suggestion["description"],
                        suggestion["expected_impact"],
                        suggestion["implementation_complexity"],
                        "pending",
                        datetime.now().isoformat()
                    ))
                
                # 更新统计信息
                analysis_results["analyzed_tables"] += 1
                analysis_results["business_categories"][business_category] += 1
                
                complexity_score = schema_analysis.get("complexity_score", 0)
                if complexity_score < 1.0:
                    analysis_results["complexity_distribution"]["low"] += 1
                elif complexity_score < 3.0:
                    analysis_results["complexity_distribution"]["medium"] += 1
                else:
                    analysis_results["complexity_distribution"]["high"] += 1
                
                analysis_results["total_suggestions"] += len(suggestions)
                
            except Exception as e:
                logger.error(f"分析表 {table_name} 失败: {e}")
                analysis_results["failed_analyses"] += 1
        
        conn.commit()
        conn.close()
        
        # 保存分析结果
        results_file = os.path.join(self.local_data_dir, "ai_optimization_analysis.json")
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"全面分析完成: 分析了 {analysis_results['analyzed_tables']} 个表")
        return analysis_results
    
    def generate_optimization_roadmap(self) -> Dict[str, Any]:
        """生成优化路线图"""
        logger.info("生成AI优化路线图")
        
        try:
            conn = sqlite3.connect(self.ai_analysis_db_path)
            cursor = conn.cursor()
            
            # 获取高优先级优化建议
            cursor.execute('''
                SELECT table_name, suggestion_type, priority, description, expected_impact,
                       implementation_complexity
                FROM optimization_suggestions 
                WHERE status = 'pending'
                ORDER BY priority ASC, implementation_complexity ASC
            ''')
            
            suggestions = cursor.fetchall()
            
            # 按阶段组织优化建议
            roadmap = {
                "roadmap_created": datetime.now().isoformat(),
                "phases": {
                    "phase_1_quick_wins": [],
                    "phase_2_medium_impact": [],
                    "phase_3_major_overhaul": []
                },
                "total_suggestions": len(suggestions),
                "estimated_timeline": "3-6 months"
            }
            
            for suggestion in suggestions:
                table_name, suggestion_type, priority, description, expected_impact, complexity = suggestion
                
                suggestion_item = {
                    "table_name": table_name,
                    "suggestion_type": suggestion_type,
                    "description": description,
                    "expected_impact": expected_impact,
                    "implementation_complexity": complexity
                }
                
                # 根据优先级和复杂度分配到不同阶段
                if priority == 1 and complexity <= 2:
                    roadmap["phases"]["phase_1_quick_wins"].append(suggestion_item)
                elif priority <= 2 and complexity <= 3:
                    roadmap["phases"]["phase_2_medium_impact"].append(suggestion_item)
                else:
                    roadmap["phases"]["phase_3_major_overhaul"].append(suggestion_item)
            
            conn.close()
            
            # 保存路线图
            roadmap_file = os.path.join(self.local_data_dir, "ai_optimization_roadmap.json")
            with open(roadmap_file, 'w', encoding='utf-8') as f:
                json.dump(roadmap, f, ensure_ascii=False, indent=2)
            
            logger.info("优化路线图生成完成")
            return roadmap
            
        except Exception as e:
            logger.error(f"生成优化路线图失败: {e}")
            return {}

def main():
    """主函数"""
    ai_env = AIOptimizationEnvironment()
    
    print("=" * 60)
    print("AI优化环境准备系统")
    print("=" * 60)
    
    # 运行全面分析
    analysis_results = ai_env.run_comprehensive_analysis()
    
    print(f"总表数: {analysis_results['total_tables']}")
    print(f"成功分析: {analysis_results['analyzed_tables']}")
    print(f"失败分析: {analysis_results['failed_analyses']}")
    print(f"生成建议: {analysis_results['total_suggestions']}")
    
    print("\n业务分类分布:")
    for category, count in analysis_results['business_categories'].items():
        print(f"  {category}: {count}")
    
    print("\n复杂度分布:")
    for level, count in analysis_results['complexity_distribution'].items():
        print(f"  {level}: {count}")
    
    # 生成优化路线图
    roadmap = ai_env.generate_optimization_roadmap()
    
    if roadmap:
        print(f"\n优化路线图:")
        print(f"  第一阶段 (快速优化): {len(roadmap['phases']['phase_1_quick_wins'])} 项")
        print(f"  第二阶段 (中等影响): {len(roadmap['phases']['phase_2_medium_impact'])} 项")
        print(f"  第三阶段 (重大改造): {len(roadmap['phases']['phase_3_major_overhaul'])} 项")
    
    print("=" * 60)
    print("AI优化环境准备完成")
    print("=" * 60)
    
    return analysis_results

if __name__ == "__main__":
    main()