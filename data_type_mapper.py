"""
数据类型映射器
用于在 SQLite/BigQuery 和 PostgreSQL 之间转换数据类型
"""

import logging
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, date, time
import json
import decimal

logger = logging.getLogger(__name__)

class DataTypeMapper:
    """数据类型映射和转换器"""
    
    # SQLite 到 PostgreSQL 类型映射
    SQLITE_TO_POSTGRES = {
        'INTEGER': 'INTEGER',
        'REAL': 'DECIMAL',
        'TEXT': 'TEXT',
        'BLOB': 'BYTEA',
        'NUMERIC': 'DECIMAL',
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR',
        'DATE': 'DATE',
        'DATETIME': 'TIMESTAMP WITH TIME ZONE',
        'TIMESTAMP': 'TIMESTAMP WITH TIME ZONE',
        'BOOLEAN': 'BOOLEAN',
        'JSON': 'JSONB'
    }
    
    # BigQuery 到 PostgreSQL 类型映射
    BIGQUERY_TO_POSTGRES = {
        'STRING': 'TEXT',
        'BYTES': 'BYTEA',
        'INTEGER': 'BIGINT',
        'INT64': 'BIGINT',
        'FLOAT': 'DOUBLE PRECISION',
        'FLOAT64': 'DOUBLE PRECISION',
        'NUMERIC': 'DECIMAL',
        'BIGNUMERIC': 'DECIMAL',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'TIMESTAMP': 'TIMESTAMP WITH TIME ZONE',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'DATETIME': 'TIMESTAMP',
        'GEOGRAPHY': 'GEOMETRY',
        'JSON': 'JSONB',
        'ARRAY': 'ARRAY',
        'STRUCT': 'JSONB',
        'RECORD': 'JSONB'
    }
    
    def __init__(self):
        self.conversion_stats = {
            'total_conversions': 0,
            'successful_conversions': 0,
            'failed_conversions': 0,
            'type_conversions': {}
        }
    
    def map_sqlite_type(self, sqlite_type: str) -> str:
        """
        将 SQLite 数据类型映射到 PostgreSQL 类型
        
        Args:
            sqlite_type: SQLite 数据类型
            
        Returns:
            对应的 PostgreSQL 数据类型
        """
        # 处理带参数的类型，如 VARCHAR(50)
        base_type = sqlite_type.split('(')[0].upper()
        
        postgres_type = self.SQLITE_TO_POSTGRES.get(base_type, 'TEXT')
        
        # 保留类型参数
        if '(' in sqlite_type and postgres_type in ['VARCHAR', 'CHAR', 'DECIMAL']:
            params = sqlite_type[sqlite_type.find('('):]
            postgres_type += params
        
        logger.debug(f"Mapped SQLite type {sqlite_type} to PostgreSQL type {postgres_type}")
        return postgres_type
    
    def map_bigquery_type(self, bigquery_type: str) -> str:
        """
        将 BigQuery 数据类型映射到 PostgreSQL 类型
        
        Args:
            bigquery_type: BigQuery 数据类型
            
        Returns:
            对应的 PostgreSQL 数据类型
        """
        base_type = bigquery_type.upper()
        postgres_type = self.BIGQUERY_TO_POSTGRES.get(base_type, 'TEXT')
        
        logger.debug(f"Mapped BigQuery type {bigquery_type} to PostgreSQL type {postgres_type}")
        return postgres_type
    
    def convert_value(self, value: Any, target_type: str) -> Any:
        """
        转换数据值以匹配目标数据类型
        
        Args:
            value: 原始值
            target_type: 目标数据类型
            
        Returns:
            转换后的值
        """
        self.conversion_stats['total_conversions'] += 1
        
        try:
            if value is None:
                return None
            
            target_type = target_type.upper()
            
            # 处理不同的目标类型
            if target_type.startswith('INTEGER') or target_type.startswith('BIGINT'):
                converted = int(float(value)) if value != '' else None
            
            elif target_type.startswith('DECIMAL') or target_type.startswith('NUMERIC'):
                converted = decimal.Decimal(str(value)) if value != '' else None
            
            elif target_type.startswith('DOUBLE PRECISION') or target_type.startswith('REAL'):
                converted = float(value) if value != '' else None
            
            elif target_type.startswith('BOOLEAN'):
                if isinstance(value, bool):
                    converted = value
                elif isinstance(value, str):
                    converted = value.lower() in ('true', '1', 'yes', 'on')
                else:
                    converted = bool(value)
            
            elif target_type.startswith('DATE'):
                if isinstance(value, str):
                    # 尝试解析不同的日期格式
                    for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%m/%d/%Y']:
                        try:
                            converted = datetime.strptime(value, fmt).date()
                            break
                        except ValueError:
                            continue
                    else:
                        converted = value  # 保持原值，让数据库处理
                elif isinstance(value, datetime):
                    converted = value.date()
                else:
                    converted = value
            
            elif target_type.startswith('TIMESTAMP'):
                if isinstance(value, str):
                    # 尝试解析不同的时间戳格式
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S']:
                        try:
                            converted = datetime.strptime(value, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        converted = value  # 保持原值，让数据库处理
                elif isinstance(value, (int, float)):
                    converted = datetime.fromtimestamp(value)
                else:
                    converted = value
            
            elif target_type.startswith('TIME'):
                if isinstance(value, str):
                    try:
                        converted = datetime.strptime(value, '%H:%M:%S').time()
                    except ValueError:
                        converted = value
                else:
                    converted = value
            
            elif target_type.startswith('JSONB') or target_type.startswith('JSON'):
                if isinstance(value, str):
                    try:
                        converted = json.loads(value)
                    except json.JSONDecodeError:
                        converted = value
                elif isinstance(value, (dict, list)):
                    converted = value
                else:
                    converted = str(value)
            
            elif target_type.startswith('ARRAY'):
                if isinstance(value, str):
                    try:
                        # 尝试解析 JSON 数组
                        converted = json.loads(value)
                        if not isinstance(converted, list):
                            converted = [converted]
                    except json.JSONDecodeError:
                        # 尝试解析逗号分隔的值
                        converted = [item.strip() for item in value.split(',')]
                elif isinstance(value, list):
                    converted = value
                else:
                    converted = [value]
            
            else:  # TEXT, VARCHAR, CHAR 等字符串类型
                converted = str(value) if value is not None else None
            
            self.conversion_stats['successful_conversions'] += 1
            
            # 记录类型转换统计
            type_key = f"{type(value).__name__}_to_{target_type}"
            self.conversion_stats['type_conversions'][type_key] = \
                self.conversion_stats['type_conversions'].get(type_key, 0) + 1
            
            return converted
            
        except Exception as e:
            self.conversion_stats['failed_conversions'] += 1
            logger.error(f"Failed to convert value {value} to type {target_type}: {e}")
            return value  # 返回原值，让数据库处理错误
    
    def convert_row(self, row: Dict[str, Any], column_types: Dict[str, str]) -> Dict[str, Any]:
        """
        转换整行数据
        
        Args:
            row: 原始行数据
            column_types: 列名到目标类型的映射
            
        Returns:
            转换后的行数据
        """
        converted_row = {}
        
        for column, value in row.items():
            if column in column_types:
                converted_row[column] = self.convert_value(value, column_types[column])
            else:
                converted_row[column] = value
                logger.warning(f"No type mapping found for column {column}, keeping original value")
        
        return converted_row
    
    def get_postgres_create_table_sql(self, table_name: str, columns: Dict[str, str], 
                                    source_type: str = 'sqlite') -> str:
        """
        生成 PostgreSQL 创建表的 SQL 语句
        
        Args:
            table_name: 表名
            columns: 列名到源数据类型的映射
            source_type: 源数据库类型 ('sqlite' 或 'bigquery')
            
        Returns:
            CREATE TABLE SQL 语句
        """
        sql_parts = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
        
        column_definitions = []
        for column_name, source_type_def in columns.items():
            if source_type.lower() == 'sqlite':
                postgres_type = self.map_sqlite_type(source_type_def)
            else:  # bigquery
                postgres_type = self.map_bigquery_type(source_type_def)
            
            column_definitions.append(f"    {column_name} {postgres_type}")
        
        # 添加标准的元数据列
        column_definitions.extend([
            "    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY",
            "    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
            "    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
            "    sync_source VARCHAR(20) DEFAULT 'unknown'",
            "    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()"
        ])
        
        sql_parts.append(",\n".join(column_definitions))
        sql_parts.append(");")
        
        return "\n".join(sql_parts)
    
    def get_conversion_stats(self) -> Dict[str, Any]:
        """
        获取转换统计信息
        
        Returns:
            转换统计字典
        """
        stats = self.conversion_stats.copy()
        if stats['total_conversions'] > 0:
            stats['success_rate'] = stats['successful_conversions'] / stats['total_conversions']
        else:
            stats['success_rate'] = 0.0
        
        return stats
    
    def reset_stats(self):
        """重置转换统计"""
        self.conversion_stats = {
            'total_conversions': 0,
            'successful_conversions': 0,
            'failed_conversions': 0,
            'type_conversions': {}
        }

# 全局数据类型映射器实例
data_type_mapper = DataTypeMapper()

def convert_sqlite_to_postgres(value: Any, postgres_type: str) -> Any:
    """
    SQLite 到 PostgreSQL 值转换的便捷函数
    
    Args:
        value: SQLite 值
        postgres_type: PostgreSQL 目标类型
        
    Returns:
        转换后的值
    """
    return data_type_mapper.convert_value(value, postgres_type)

def convert_bigquery_to_postgres(value: Any, postgres_type: str) -> Any:
    """
    BigQuery 到 PostgreSQL 值转换的便捷函数
    
    Args:
        value: BigQuery 值
        postgres_type: PostgreSQL 目标类型
        
    Returns:
        转换后的值
    """
    return data_type_mapper.convert_value(value, postgres_type)

if __name__ == "__main__":
    # 测试数据类型映射器
    mapper = DataTypeMapper()
    
    # 测试类型映射
    print("SQLite 类型映射测试:")
    sqlite_types = ['INTEGER', 'REAL', 'TEXT', 'VARCHAR(50)', 'DATETIME']
    for sqlite_type in sqlite_types:
        postgres_type = mapper.map_sqlite_type(sqlite_type)
        print(f"  {sqlite_type} -> {postgres_type}")
    
    print("\nBigQuery 类型映射测试:")
    bigquery_types = ['STRING', 'INTEGER', 'FLOAT64', 'TIMESTAMP', 'BOOLEAN']
    for bigquery_type in bigquery_types:
        postgres_type = mapper.map_bigquery_type(bigquery_type)
        print(f"  {bigquery_type} -> {postgres_type}")
    
    # 测试值转换
    print("\n值转换测试:")
    test_values = [
        ('123', 'INTEGER'),
        ('123.45', 'DECIMAL'),
        ('true', 'BOOLEAN'),
        ('2023-12-01', 'DATE'),
        ('2023-12-01 10:30:00', 'TIMESTAMP'),
        ('[1,2,3]', 'ARRAY'),
        ('{"key": "value"}', 'JSONB')
    ]
    
    for value, target_type in test_values:
        converted = mapper.convert_value(value, target_type)
        print(f"  {value} ({target_type}) -> {converted} ({type(converted).__name__})")
    
    # 显示转换统计
    print(f"\n转换统计: {mapper.get_conversion_stats()}")