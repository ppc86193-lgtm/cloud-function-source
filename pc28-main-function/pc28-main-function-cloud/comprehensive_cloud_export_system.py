#!/usr/bin/env python3
"""
全面云数据导出系统
导出BigQuery中所有表的结构、数据和业务逻辑到本地SQLite数据库
为后续AI优化几百张表做准备
"""

import os
import sqlite3
import logging
import json
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import time
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('comprehensive_cloud_export.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveCloudExportSystem:
    def __init__(self):
        """初始化全面云数据导出系统"""
        self.client = bigquery.Client()
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.local_db_path = "/Users/a606/cloud_function_source/local_data/pc28_complete_mirror.db"
        self.export_log_path = "/Users/a606/cloud_function_source/local_data/export_log.json"
        
        # 创建本地数据目录
        os.makedirs(os.path.dirname(self.local_db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.export_log_path), exist_ok=True)
        
        # 初始化本地数据库
        self.init_local_database()
        
        # 导出统计
        self.export_stats = {
            'start_time': datetime.now().isoformat(),
            'tables_discovered': 0,
            'tables_exported': 0,
            'views_exported': 0,
            'total_rows_exported': 0,
            'errors': [],
            'table_details': {}
        }
    
    def init_local_database(self):
        """初始化本地SQLite数据库"""
        logger.info("初始化本地数据库...")
        
        conn = sqlite3.connect(self.local_db_path)
        cursor = conn.cursor()
        
        # 创建元数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS _export_metadata (
                table_name TEXT PRIMARY KEY,
                table_type TEXT,
                export_timestamp TEXT,
                row_count INTEGER,
                schema_json TEXT,
                source_query TEXT,
                last_updated TEXT
            )
        ''')
        
        # 创建业务规则表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS _business_rules (
                rule_id TEXT PRIMARY KEY,
                rule_name TEXT,
                rule_type TEXT,
                source_table TEXT,
                rule_logic TEXT,
                created_at TEXT
            )
        ''')
        
        # 创建表关系映射
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS _table_relationships (
                parent_table TEXT,
                child_table TEXT,
                relationship_type TEXT,
                join_condition TEXT,
                created_at TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("本地数据库初始化完成")
    
    def discover_all_tables(self):
        """发现所有云端表和视图"""
        logger.info("发现云端所有表和视图...")
        
        # 获取所有表
        tables_query = f"""
        SELECT 
            table_name,
            table_type,
            creation_time
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.TABLES`
        ORDER BY table_name
        """
        
        try:
            tables_df = self.client.query(tables_query).to_dataframe()
            self.export_stats['tables_discovered'] = len(tables_df)
            
            logger.info(f"发现 {len(tables_df)} 个表/视图")
            
            # 按类型分组
            tables_by_type = tables_df.groupby('table_type').size()
            for table_type, count in tables_by_type.items():
                logger.info(f"  {table_type}: {count} 个")
            
            return tables_df
            
        except Exception as e:
            logger.error(f"发现表时出错: {e}")
            return pd.DataFrame()
    
    def get_table_schema(self, table_name):
        """获取表结构"""
        schema_query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        try:
            schema_df = self.client.query(schema_query).to_dataframe()
            return schema_df.to_dict('records')
        except Exception as e:
            logger.error(f"获取表 {table_name} 结构时出错: {e}")
            return []
    
    def get_view_definition(self, view_name):
        """获取视图定义"""
        view_query = f"""
        SELECT view_definition
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.VIEWS`
        WHERE table_name = '{view_name}'
        """
        
        try:
            result = self.client.query(view_query).to_dataframe()
            if not result.empty:
                return result.iloc[0]['view_definition']
            return None
        except Exception as e:
            logger.error(f"获取视图 {view_name} 定义时出错: {e}")
            return None
    
    def create_local_table(self, table_name, schema, table_type):
        """在本地创建表"""
        conn = sqlite3.connect(self.local_db_path)
        cursor = conn.cursor()
        
        try:
            # 删除已存在的表
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # 构建CREATE TABLE语句
            columns = []
            for col in schema:
                col_name = col['column_name']
                data_type = self.map_bigquery_to_sqlite_type(col['data_type'])
                nullable = "" if col['is_nullable'] == 'YES' else " NOT NULL"
                columns.append(f"{col_name} {data_type}{nullable}")
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_sql)
            
            conn.commit()
            logger.info(f"创建本地表: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建本地表 {table_name} 时出错: {e}")
            return False
        finally:
            conn.close()
    
    def map_bigquery_to_sqlite_type(self, bq_type):
        """映射BigQuery数据类型到SQLite"""
        type_mapping = {
            'STRING': 'TEXT',
            'INT64': 'INTEGER',
            'INTEGER': 'INTEGER',
            'FLOAT64': 'REAL',
            'FLOAT': 'REAL',
            'BOOLEAN': 'INTEGER',
            'BOOL': 'INTEGER',
            'TIMESTAMP': 'TEXT',
            'DATETIME': 'TEXT',
            'DATE': 'TEXT',
            'TIME': 'TEXT',
            'BYTES': 'BLOB',
            'NUMERIC': 'REAL',
            'BIGNUMERIC': 'REAL',
            'JSON': 'TEXT',
            'ARRAY': 'TEXT',
            'STRUCT': 'TEXT'
        }
        
        return type_mapping.get(bq_type.upper(), 'TEXT')
    
    def export_table_data(self, table_name: str, table_type: str):
        """导出表数据到本地SQLite"""
        logger.info(f"导出表数据: {table_name} ({table_type})")
        
        try:
            # 获取表数据，处理JSON和复杂类型
            if table_type == 'VIEW':
                # 对于视图，先检查是否可以查询
                test_query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table_name}` LIMIT 1"
                try:
                    self.client.query(test_query).result()
                except Exception as view_error:
                    logger.warning(f"视图 {table_name} 无法查询: {view_error}")
                    return 0
            
            query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
            
            # 分批获取数据以避免内存问题
            job_config = bigquery.QueryJobConfig()
            job_config.maximum_bytes_billed = 10**10  # 10GB limit
            
            query_job = self.client.query(query, job_config=job_config)
            
            # 获取总行数
            count_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
            try:
                count_result = self.client.query(count_query).to_dataframe()
                total_rows = count_result.iloc[0]['count']
                logger.info(f"表 {table_name} 共有 {total_rows} 行数据")
            except:
                total_rows = 0
                logger.warning(f"无法获取表 {table_name} 的行数")
            
            if total_rows == 0:
                return 0
            
            # 分批处理数据
            batch_size = 10000
            offset = 0
            total_inserted = 0
            
            while offset < total_rows:
                batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                
                try:
                    df = self.client.query(batch_query).to_dataframe()
                    
                    if len(df) == 0:
                        break
                    
                    # 处理复杂数据类型
                    df = self._process_complex_data_types(df)
                    
                    # 插入到SQLite
                    df.to_sql(table_name, self.local_conn, if_exists='append', index=False)
                    
                    total_inserted += len(df)
                    offset += batch_size
                    
                    # 显示进度
                    progress = min(100.0, (total_inserted / total_rows) * 100)
                    logger.info(f"已导出 {total_inserted}/{total_rows} 行 ({progress:.1f}%)")
                    
                except Exception as batch_error:
                    logger.error(f"批次导出失败 (offset {offset}): {batch_error}")
                    break
            
            logger.info(f"表 {table_name} 数据导出完成，共 {total_inserted} 行")
            return total_inserted
            
        except Exception as e:
            logger.error(f"导出表 {table_name} 数据时出错: {e}")
            return 0
    
    def _process_complex_data_types(self, df):
        """处理复杂数据类型（JSON、ARRAY等）"""
        for col in df.columns:
            if df[col].dtype == 'object':
                # 检查是否包含复杂类型
                sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                
                if sample_value is not None:
                    if isinstance(sample_value, (dict, list)):
                        # 将复杂类型转换为JSON字符串
                        df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None)
                    elif hasattr(sample_value, '__dict__'):
                        # 处理其他复杂对象
                        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
        
        return df
    
    def save_metadata(self, table_name, table_type, schema, view_definition, row_count):
        """保存表元数据"""
        conn = sqlite3.connect(self.local_db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO _export_metadata 
                (table_name, table_type, export_timestamp, row_count, schema_json, source_query, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                table_name,
                table_type,
                datetime.now().isoformat(),
                row_count,
                json.dumps(schema),
                view_definition or '',
                datetime.now().isoformat()
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"保存表 {table_name} 元数据时出错: {e}")
        finally:
            conn.close()
    
    def extract_business_logic(self, view_definition, view_name):
        """从视图定义中提取业务逻辑"""
        if not view_definition:
            return []
        
        business_rules = []
        
        # 提取常见的业务逻辑模式
        patterns = {
            'aggregation': r'(SUM|COUNT|AVG|MAX|MIN)\s*\(',
            'filtering': r'WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|$)',
            'joining': r'JOIN\s+(\w+)\s+ON\s+(.+?)(?:WHERE|GROUP|ORDER|$)',
            'calculation': r'CASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+(?:ELSE|END)',
            'window_function': r'(ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD)\s*\(',
        }
        
        for rule_type, pattern in patterns.items():
            import re
            matches = re.findall(pattern, view_definition, re.IGNORECASE | re.DOTALL)
            if matches:
                for i, match in enumerate(matches):
                    rule_id = f"{view_name}_{rule_type}_{i+1}"
                    business_rules.append({
                        'rule_id': rule_id,
                        'rule_name': f"{view_name} {rule_type} rule {i+1}",
                        'rule_type': rule_type,
                        'source_table': view_name,
                        'rule_logic': str(match),
                        'created_at': datetime.now().isoformat()
                    })
        
        return business_rules
    
    def save_business_rules(self, business_rules):
        """保存业务规则"""
        if not business_rules:
            return
        
        conn = sqlite3.connect(self.local_db_path)
        cursor = conn.cursor()
        
        try:
            for rule in business_rules:
                cursor.execute('''
                    INSERT OR REPLACE INTO _business_rules 
                    (rule_id, rule_name, rule_type, source_table, rule_logic, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    rule['rule_id'],
                    rule['rule_name'],
                    rule['rule_type'],
                    rule['source_table'],
                    rule['rule_logic'],
                    rule['created_at']
                ))
            
            conn.commit()
            logger.info(f"保存了 {len(business_rules)} 条业务规则")
            
        except Exception as e:
            logger.error(f"保存业务规则时出错: {e}")
        finally:
            conn.close()
    
    def export_single_table(self, table_info):
        """导出单个表"""
        table_name = table_info['table_name']
        table_type = table_info['table_type']
        
        logger.info(f"开始导出: {table_name} ({table_type})")
        
        try:
            # 获取表结构
            schema = self.get_table_schema(table_name)
            if not schema:
                logger.warning(f"无法获取表 {table_name} 的结构，跳过")
                return False
            
            # 创建本地表
            if not self.create_local_table(table_name, schema, table_type):
                return False
            
            # 导出数据
            row_count = 0
            view_definition = None
            
            if table_type == 'BASE TABLE':
                row_count = self.export_table_data(table_name, table_type)
                self.export_stats['tables_exported'] += 1
            elif table_type == 'VIEW':
                view_definition = self.get_view_definition(table_name)
                # 对于视图，也尝试导出数据（如果可能）
                row_count = self.export_table_data(table_name, table_type)
                self.export_stats['views_exported'] += 1
                
                # 提取业务逻辑
                business_rules = self.extract_business_logic(view_definition, table_name)
                self.save_business_rules(business_rules)
            
            # 保存元数据
            self.save_metadata(table_name, table_type, schema, view_definition, row_count)
            
            # 更新统计
            self.export_stats['total_rows_exported'] += row_count
            self.export_stats['table_details'][table_name] = {
                'type': table_type,
                'rows': row_count,
                'schema_columns': len(schema),
                'export_time': datetime.now().isoformat()
            }
            
            logger.info(f"完成导出: {table_name} ({row_count} 行)")
            return True
            
        except Exception as e:
            logger.error(f"导出表 {table_name} 时出错: {e}")
            self.export_stats['errors'].append(f"导出表 {table_name} 失败: {str(e)}")
            return False
    
    def save_export_log(self):
        """保存导出日志"""
        self.export_stats['end_time'] = datetime.now().isoformat()
        self.export_stats['duration_minutes'] = (
            datetime.fromisoformat(self.export_stats['end_time']) - 
            datetime.fromisoformat(self.export_stats['start_time'])
        ).total_seconds() / 60
        
        with open(self.export_log_path, 'w', encoding='utf-8') as f:
            json.dump(self.export_stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"导出日志已保存: {self.export_log_path}")
    
    def run_comprehensive_export(self):
        """运行全面导出流程"""
        logger.info("开始全面云数据导出流程...")
        
        # 1. 发现所有表
        tables_df = self.discover_all_tables()
        if tables_df.empty:
            logger.error("未发现任何表，终止导出")
            return False
        
        # 2. 按优先级排序（先导出基础表，再导出视图）
        base_tables = tables_df[tables_df['table_type'] == 'BASE TABLE'].sort_values('table_name')
        views = tables_df[tables_df['table_type'] == 'VIEW'].sort_values('table_name')
        
        # 3. 导出基础表
        logger.info(f"开始导出 {len(base_tables)} 个基础表...")
        for _, table_info in base_tables.iterrows():
            self.export_single_table(table_info)
        
        # 4. 导出视图
        logger.info(f"开始导出 {len(views)} 个视图...")
        for _, table_info in views.iterrows():
            self.export_single_table(table_info)
        
        # 5. 保存导出日志
        self.save_export_log()
        
        # 6. 生成导出报告
        self.generate_export_report()
        
        logger.info("全面云数据导出流程完成!")
        return True
    
    def generate_export_report(self):
        """生成导出报告"""
        report = f"""
# 云数据导出报告

## 导出概要
- 开始时间: {self.export_stats['start_time']}
- 结束时间: {self.export_stats['end_time']}
- 总耗时: {self.export_stats['duration_minutes']:.2f} 分钟
- 发现表数: {self.export_stats['tables_discovered']}
- 导出基础表: {self.export_stats['tables_exported']}
- 导出视图: {self.export_stats['views_exported']}
- 总导出行数: {self.export_stats['total_rows_exported']:,}

## 本地数据库
- 路径: {self.local_db_path}
- 大小: {os.path.getsize(self.local_db_path) / 1024 / 1024:.2f} MB

## 错误列表
"""
        
        if self.export_stats['errors']:
            for error in self.export_stats['errors']:
                report += f"- {error}\n"
        else:
            report += "无错误\n"
        
        report += "\n## 表详情\n"
        for table_name, details in self.export_stats['table_details'].items():
            report += f"- {table_name}: {details['type']}, {details['rows']:,} 行, {details['schema_columns']} 列\n"
        
        report_path = "/Users/a606/cloud_function_source/local_data/export_report.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"导出报告已生成: {report_path}")

def main():
    """主函数"""
    export_system = ComprehensiveCloudExportSystem()
    
    # 运行全面导出流程
    success = export_system.run_comprehensive_export()
    
    if success:
        logger.info("✅ 全面云数据导出成功完成!")
    else:
        logger.error("❌ 全面云数据导出失败!")
        exit(1)

if __name__ == "__main__":
    main()