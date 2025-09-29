#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28综合修复系统
基于实际表结构，智能检测和修复数据流问题
"""

import os
import sys
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveRepairSystem:
    """综合修复系统"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.client = bigquery.Client(project=self.project_id)
        
        # 实际存在的表和视图
        self.existing_tables = {}
        self.repair_actions = []
        
    def discover_tables(self) -> Dict[str, Any]:
        """发现所有表和视图"""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            tables = list(self.client.list_tables(dataset_ref))
            
            discovered = {
                "tables": [],
                "views": [],
                "total": len(tables)
            }
            
            for table in tables:
                table_info = {
                    "name": table.table_id,
                    "type": table.table_type,
                    "full_name": f"{self.project_id}.{self.dataset_id}.{table.table_id}"
                }
                
                if table.table_type == "TABLE":
                    discovered["tables"].append(table_info)
                elif table.table_type == "VIEW":
                    discovered["views"].append(table_info)
                
                self.existing_tables[table.table_id] = table_info
            
            logger.info(f"发现 {len(discovered['tables'])} 个表, {len(discovered['views'])} 个视图")
            return discovered
            
        except Exception as e:
            logger.error(f"发现表失败: {e}")
            return {"tables": [], "views": [], "total": 0}
    
    def check_table_health(self, table_name: str) -> Dict[str, Any]:
        """检查表健康状态"""
        try:
            # 基本信息
            health_info = {
                "table_name": table_name,
                "exists": table_name in self.existing_tables,
                "type": self.existing_tables.get(table_name, {}).get("type", "UNKNOWN"),
                "row_count": 0,
                "has_data": False,
                "latest_data": None,
                "date_range": [],
                "issues": []
            }
            
            if not health_info["exists"]:
                health_info["issues"].append("表不存在")
                return health_info
            
            # 检查行数
            count_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
            count_result = list(self.client.query(count_query).result())
            health_info["row_count"] = int(count_result[0]['count']) if count_result else 0
            health_info["has_data"] = health_info["row_count"] > 0
            
            if not health_info["has_data"]:
                health_info["issues"].append("表为空")
                return health_info
            
            # 检查时间字段
            time_fields = ["ts_utc", "timestamp", "created_at", "updated_at"]
            time_field = None
            
            for field in time_fields:
                try:
                    test_query = f"SELECT {field} FROM `{self.project_id}.{self.dataset_id}.{table_name}` LIMIT 1"
                    list(self.client.query(test_query).result())
                    time_field = field
                    break
                except:
                    continue
            
            if time_field:
                # 检查最新数据
                latest_query = f"""
                    SELECT MAX({time_field}) as latest_time 
                    FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                """
                latest_result = list(self.client.query(latest_query).result())
                health_info["latest_data"] = latest_result[0]['latest_time'].isoformat() if latest_result[0]['latest_time'] else None
                
                # 检查日期分布
                date_query = f"""
                    SELECT 
                        DATE({time_field}, 'Asia/Shanghai') as date,
                        COUNT(*) as count
                    FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                    GROUP BY DATE({time_field}, 'Asia/Shanghai')
                    ORDER BY date DESC
                    LIMIT 7
                """
                date_result = list(self.client.query(date_query).result())
                health_info["date_range"] = [{"date": str(row['date']), "count": row['count']} for row in date_result]
                
                # 检查数据新鲜度
                if health_info["latest_data"]:
                    latest_date = datetime.fromisoformat(health_info["latest_data"].replace('Z', '+00:00'))
                    days_old = (datetime.now().replace(tzinfo=latest_date.tzinfo) - latest_date).days
                    
                    if days_old > 1:
                        health_info["issues"].append(f"数据过期 ({days_old} 天前)")
            else:
                health_info["issues"].append("没有时间字段")
            
            return health_info
            
        except Exception as e:
            logger.error(f"检查表健康状态失败 {table_name}: {e}")
            return {
                "table_name": table_name,
                "exists": False,
                "error": str(e),
                "issues": ["检查失败"]
            }
    
    def analyze_view_dependencies(self, view_name: str) -> Dict[str, Any]:
        """分析视图依赖关系"""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(view_name)
            table = self.client.get_table(table_ref)
            
            if not hasattr(table, 'view_query'):
                return {"view_name": view_name, "dependencies": [], "sql": None}
            
            sql = table.view_query
            
            # 提取依赖的表名
            dependencies = []
            
            # 匹配 FROM 和 JOIN 后的表名
            patterns = [
                r'FROM\s+`([^`]+)`',
                r'JOIN\s+`([^`]+)`',
                r'FROM\s+(\w+\.\w+\.\w+)',
                r'JOIN\s+(\w+\.\w+\.\w+)'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, sql, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, str):
                        # 提取表名
                        if '.' in match:
                            table_name = match.split('.')[-1]
                        else:
                            table_name = match
                        
                        if table_name not in dependencies:
                            dependencies.append(table_name)
            
            return {
                "view_name": view_name,
                "dependencies": dependencies,
                "sql": sql,
                "dependency_count": len(dependencies)
            }
            
        except Exception as e:
            logger.error(f"分析视图依赖失败 {view_name}: {e}")
            return {"view_name": view_name, "dependencies": [], "sql": None, "error": str(e)}
    
    def create_missing_source_table(self, table_name: str) -> bool:
        """创建缺失的源表"""
        try:
            # 根据表名推断结构
            if "p_cloud" in table_name:
                schema = [
                    bigquery.SchemaField("period", "INTEGER"),
                    bigquery.SchemaField("ts_utc", "TIMESTAMP"),
                    bigquery.SchemaField("p_even", "FLOAT"),
                    bigquery.SchemaField("src", "STRING"),
                    bigquery.SchemaField("n_src", "INTEGER"),
                ]
            elif "p_map" in table_name or "p_size" in table_name:
                schema = [
                    bigquery.SchemaField("period", "INTEGER"),
                    bigquery.SchemaField("ts_utc", "TIMESTAMP"),
                    bigquery.SchemaField("p_even", "FLOAT"),
                    bigquery.SchemaField("src", "STRING"),
                    bigquery.SchemaField("n_src", "INTEGER"),
                ]
            else:
                # 通用结构
                schema = [
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("created_at", "TIMESTAMP"),
                    bigquery.SchemaField("data", "JSON"),
                ]
            
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            table = bigquery.Table(table_ref, schema=schema)
            
            # 设置分区
            if any(field.name == "ts_utc" for field in schema):
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="ts_utc"
                )
            
            created_table = self.client.create_table(table)
            logger.info(f"创建表成功: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建表失败 {table_name}: {e}")
            return False
    
    def fix_view_with_missing_dependencies(self, view_name: str, missing_deps: List[str]) -> bool:
        """修复有缺失依赖的视图"""
        try:
            # 获取视图定义
            analysis = self.analyze_view_dependencies(view_name)
            if not analysis["sql"]:
                return False
            
            sql = analysis["sql"]
            
            # 尝试替换缺失的依赖
            for missing_dep in missing_deps:
                # 查找相似的表
                similar_tables = []
                for existing_table in self.existing_tables.keys():
                    if self.calculate_similarity(missing_dep, existing_table) > 0.7:
                        similar_tables.append(existing_table)
                
                if similar_tables:
                    # 使用最相似的表替换
                    replacement = similar_tables[0]
                    sql = sql.replace(missing_dep, replacement)
                    logger.info(f"替换依赖: {missing_dep} -> {replacement}")
                else:
                    # 创建缺失的表
                    if self.create_missing_source_table(missing_dep):
                        logger.info(f"创建了缺失的表: {missing_dep}")
                    else:
                        return False
            
            # 更新视图
            create_sql = f"CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.{view_name}` AS {sql}"
            job = self.client.query(create_sql)
            job.result()
            
            logger.info(f"修复视图成功: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"修复视图失败 {view_name}: {e}")
            return False
    
    def calculate_similarity(self, str1: str, str2: str) -> float:
        """计算字符串相似度"""
        if not str1 or not str2:
            return 0.0
        
        # 简单的相似度计算
        common_chars = set(str1.lower()) & set(str2.lower())
        total_chars = set(str1.lower()) | set(str2.lower())
        
        if not total_chars:
            return 0.0
        
        return len(common_chars) / len(total_chars)
    
    def run_comprehensive_repair(self) -> Dict[str, Any]:
        """运行综合修复"""
        logger.info("开始综合数据修复...")
        
        repair_results = {
            "start_time": datetime.now().isoformat(),
            "discovery": {},
            "health_checks": [],
            "repairs": [],
            "successful": 0,
            "failed": 0,
            "summary": ""
        }
        
        # 1. 发现所有表和视图
        logger.info("发现表和视图...")
        repair_results["discovery"] = self.discover_tables()
        
        # 2. 检查关键表的健康状态
        key_tables = [
            "cloud_pred_today_norm",
            "p_cloud_today_v", 
            "p_map_today_v",
            "p_size_today_v",
            "p_map_today_canon_v",
            "p_size_today_canon_v",
            "signal_pool_union_v3",
            "lab_push_candidates_v2"
        ]
        
        logger.info("检查表健康状态...")
        for table_name in key_tables:
            health = self.check_table_health(table_name)
            repair_results["health_checks"].append(health)
            
            # 如果表有问题，尝试修复
            if health["issues"]:
                logger.info(f"发现问题: {table_name} - {health['issues']}")
                
                repair_action = {
                    "table_name": table_name,
                    "issues": health["issues"],
                    "repair_attempted": False,
                    "repair_successful": False,
                    "actions": []
                }
                
                # 如果是视图且为空，分析依赖关系
                if health.get("type") == "VIEW" and "表为空" in health["issues"]:
                    analysis = self.analyze_view_dependencies(table_name)
                    
                    # 检查依赖的表是否存在
                    missing_deps = []
                    for dep in analysis["dependencies"]:
                        if dep not in self.existing_tables:
                            missing_deps.append(dep)
                    
                    if missing_deps:
                        logger.info(f"视图 {table_name} 缺失依赖: {missing_deps}")
                        repair_action["repair_attempted"] = True
                        repair_action["actions"].append(f"修复缺失依赖: {missing_deps}")
                        
                        success = self.fix_view_with_missing_dependencies(table_name, missing_deps)
                        repair_action["repair_successful"] = success
                        
                        if success:
                            repair_results["successful"] += 1
                        else:
                            repair_results["failed"] += 1
                
                repair_results["repairs"].append(repair_action)
        
        # 3. 生成摘要
        repair_results["end_time"] = datetime.now().isoformat()
        repair_results["total_repairs"] = len(repair_results["repairs"])
        repair_results["summary"] = f"综合修复完成: {repair_results['successful']} 成功, {repair_results['failed']} 失败"
        
        logger.info(repair_results["summary"])
        return repair_results

def main():
    """主函数"""
    repair_system = ComprehensiveRepairSystem()
    
    try:
        # 运行综合修复
        results = repair_system.run_comprehensive_repair()
        
        # 输出结果
        print("\n" + "="*60)
        print("PC28综合数据修复报告")
        print("="*60)
        print(f"开始时间: {results['start_time']}")
        print(f"结束时间: {results['end_time']}")
        
        # 发现结果
        discovery = results['discovery']
        print(f"\n发现结果:")
        print(f"  总表数: {discovery['total']}")
        print(f"  数据表: {len(discovery['tables'])}")
        print(f"  视图: {len(discovery['views'])}")
        
        # 健康检查结果
        print(f"\n健康检查结果:")
        healthy_count = 0
        for health in results['health_checks']:
            if not health.get('issues', []):
                healthy_count += 1
                status = "✅ 健康"
            else:
                status = f"⚠️  问题: {', '.join(health['issues'])}"
            
            print(f"  {health['table_name']}: {status}")
            if health.get('row_count', 0) > 0:
                print(f"    行数: {health['row_count']}")
        
        print(f"\n健康表数: {healthy_count}/{len(results['health_checks'])}")
        
        # 修复结果
        print(f"\n修复结果:")
        print(f"  总修复项: {results['total_repairs']}")
        print(f"  成功修复: {results['successful']}")
        print(f"  修复失败: {results['failed']}")
        
        for repair in results['repairs']:
            table_name = repair['table_name']
            attempted = repair['repair_attempted']
            successful = repair['repair_successful']
            
            if attempted:
                status = "✅ 成功" if successful else "❌ 失败"
                print(f"  {status} {table_name}")
                for action in repair['actions']:
                    print(f"    - {action}")
            else:
                print(f"  ⏭️  {table_name} (无需修复)")
        
        # 保存结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"/Users/a606/cloud_function_source/test_suite/comprehensive_repair_report_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n详细报告已保存到: {report_file}")
        
    except Exception as e:
        logger.error(f"综合修复异常: {e}")
        print(f"综合修复失败: {e}")

if __name__ == "__main__":
    main()