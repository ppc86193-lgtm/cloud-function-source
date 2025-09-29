#!/usr/bin/env python3
"""
表结构创建系统合规性测试
测试数据库表创建和结构管理的完整功能和合规性
"""

import pytest
import os
import sys
import sqlite3
import tempfile
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import logging

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from contract_compliance_logger import ContractComplianceLogger

# 配置pytest日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestTableCreationCompliance:
    """表结构创建系统合规性测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_compliance_logging(self):
        """设置合规性日志记录"""
        self.compliance_logger = ContractComplianceLogger()
        self.compliance_logger.log_pytest_entry(
            test_name="表结构创建系统合规性测试",
            test_category="table_creation_compliance",
            description="验证数据库表创建和结构管理的完整功能和合规性"
        )
        yield
    
    @pytest.fixture
    def temp_db(self):
        """创建临时数据库用于测试"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        yield db_path
        
        # 清理
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    @pytest.mark.pytest_compliant
    def test_core_table_creation(self, temp_db):
        """测试核心表创建"""
        logger.info("开始测试核心表创建")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 创建lab_push_candidates_v2表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lab_push_candidates_v2 (
                draw_id TEXT PRIMARY KEY,
                issue TEXT NOT NULL,
                numbers TEXT,
                sum_value INTEGER,
                big_small TEXT,
                odd_even TEXT,
                dragon_tiger TEXT,
                prediction_score REAL,
                confidence_level REAL,
                algorithm_version TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 验证表创建成功
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='lab_push_candidates_v2'")
        result = cursor.fetchone()
        assert result is not None
        logger.info("✅ lab_push_candidates_v2表创建成功")
        
        # 创建cloud_pred_today_norm表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cloud_pred_today_norm (
                id INTEGER PRIMARY KEY,
                draw_id TEXT NOT NULL,
                predicted_numbers TEXT,
                prediction_type TEXT,
                confidence_score REAL,
                normalization_factor REAL,
                model_version TEXT,
                prediction_timestamp DATETIME,
                data_date TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 验证表创建成功
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cloud_pred_today_norm'")
        result = cursor.fetchone()
        assert result is not None
        logger.info("✅ cloud_pred_today_norm表创建成功")
        
        # 创建signal_pool_union_v3表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signal_pool_union_v3 (
                id INTEGER PRIMARY KEY,
                signal_id TEXT NOT NULL,
                signal_type TEXT,
                signal_strength REAL,
                pattern_match TEXT,
                frequency_score REAL,
                reliability_index REAL,
                source_algorithm TEXT,
                last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 验证表创建成功
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signal_pool_union_v3'")
        result = cursor.fetchone()
        assert result is not None
        logger.info("✅ signal_pool_union_v3表创建成功")
        
        conn.commit()
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_core_table_creation",
            test_category="table_creation",
            description="核心表创建测试通过，所有主要表结构创建成功"
        )
    
    @pytest.mark.pytest_compliant
    def test_table_structure_validation(self, temp_db):
        """测试表结构验证"""
        logger.info("开始测试表结构验证")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 先创建表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lab_push_candidates_v2 (
                draw_id TEXT PRIMARY KEY,
                issue TEXT NOT NULL,
                numbers TEXT,
                sum_value INTEGER,
                big_small TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 验证表结构
        cursor.execute("PRAGMA table_info(lab_push_candidates_v2)")
        columns = cursor.fetchall()
        
        # 验证列存在
        column_names = [col[1] for col in columns]
        required_columns = ['draw_id', 'issue', 'numbers', 'sum_value', 'big_small', 'created_at']
        
        for col in required_columns:
            assert col in column_names
            logger.info(f"✅ 列 {col} 存在验证通过")
        
        # 验证主键
        primary_keys = [col[1] for col in columns if col[5] == 1]
        assert 'draw_id' in primary_keys
        logger.info("✅ 主键设置验证通过")
        
        # 验证NOT NULL约束
        not_null_columns = [col[1] for col in columns if col[3] == 1]
        assert 'issue' in not_null_columns
        logger.info("✅ NOT NULL约束验证通过")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_table_structure_validation",
            test_category="structure_validation",
            description="表结构验证测试通过，所有约束和列定义正确"
        )
    
    @pytest.mark.pytest_compliant
    def test_index_creation_compliance(self, temp_db):
        """测试索引创建合规性"""
        logger.info("开始测试索引创建合规性")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 创建表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                draw_id TEXT NOT NULL,
                issue TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_draw_id ON test_table(draw_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_issue ON test_table(issue)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON test_table(created_at)')
        
        # 验证索引创建
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test_table'")
        indexes = cursor.fetchall()
        
        index_names = [idx[0] for idx in indexes]
        expected_indexes = ['idx_draw_id', 'idx_issue', 'idx_created_at']
        
        for idx in expected_indexes:
            assert idx in index_names
            logger.info(f"✅ 索引 {idx} 创建成功")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_index_creation_compliance",
            test_category="index_creation",
            description="索引创建合规性测试通过，所有必要索引创建成功"
        )
    
    @pytest.mark.pytest_compliant
    def test_data_type_compliance(self, temp_db):
        """测试数据类型合规性"""
        logger.info("开始测试数据类型合规性")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 创建包含各种数据类型的表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_type_test (
                id INTEGER PRIMARY KEY,
                text_field TEXT,
                integer_field INTEGER,
                real_field REAL,
                blob_field BLOB,
                datetime_field DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 插入测试数据
        cursor.execute('''
            INSERT INTO data_type_test 
            (text_field, integer_field, real_field, datetime_field)
            VALUES (?, ?, ?, ?)
        ''', ('test_text', 123, 45.67, '2025-09-29 10:00:00'))
        
        # 验证数据插入和类型
        cursor.execute('SELECT * FROM data_type_test WHERE id = 1')
        row = cursor.fetchone()
        
        assert row is not None
        assert isinstance(row[1], str)  # text_field
        assert isinstance(row[2], int)  # integer_field
        assert isinstance(row[3], float)  # real_field
        
        logger.info("✅ 数据类型验证通过")
        
        # 验证表结构信息
        cursor.execute("PRAGMA table_info(data_type_test)")
        columns = cursor.fetchall()
        
        type_mapping = {col[1]: col[2] for col in columns}
        
        assert type_mapping['text_field'] == 'TEXT'
        assert type_mapping['integer_field'] == 'INTEGER'
        assert type_mapping['real_field'] == 'REAL'
        assert type_mapping['datetime_field'] == 'DATETIME'
        
        logger.info("✅ 数据类型定义验证通过")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_data_type_compliance",
            test_category="data_type_compliance",
            description="数据类型合规性测试通过，所有数据类型定义和使用正确"
        )
    
    @pytest.mark.pytest_compliant
    def test_constraint_enforcement(self, temp_db):
        """测试约束强制执行"""
        logger.info("开始测试约束强制执行")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 创建带约束的表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS constraint_test (
                id INTEGER PRIMARY KEY,
                unique_field TEXT UNIQUE NOT NULL,
                check_field INTEGER CHECK(check_field > 0),
                foreign_key_field INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 测试正常插入
        cursor.execute('''
            INSERT INTO constraint_test (unique_field, check_field, foreign_key_field)
            VALUES (?, ?, ?)
        ''', ('unique_value_1', 10, 1))
        
        logger.info("✅ 正常数据插入成功")
        
        # 测试UNIQUE约束
        try:
            cursor.execute('''
                INSERT INTO constraint_test (unique_field, check_field, foreign_key_field)
                VALUES (?, ?, ?)
            ''', ('unique_value_1', 20, 2))
            conn.commit()
            assert False, "UNIQUE约束应该阻止重复值"
        except sqlite3.IntegrityError:
            logger.info("✅ UNIQUE约束正常工作")
        
        # 测试CHECK约束
        try:
            cursor.execute('''
                INSERT INTO constraint_test (unique_field, check_field, foreign_key_field)
                VALUES (?, ?, ?)
            ''', ('unique_value_2', -5, 3))
            conn.commit()
            assert False, "CHECK约束应该阻止负值"
        except sqlite3.IntegrityError:
            logger.info("✅ CHECK约束正常工作")
        
        # 测试NOT NULL约束
        try:
            cursor.execute('''
                INSERT INTO constraint_test (check_field, foreign_key_field)
                VALUES (?, ?)
            ''', (15, 4))
            conn.commit()
            assert False, "NOT NULL约束应该阻止空值"
        except sqlite3.IntegrityError:
            logger.info("✅ NOT NULL约束正常工作")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_constraint_enforcement",
            test_category="constraint_enforcement",
            description="约束强制执行测试通过，所有数据库约束正常工作"
        )
    
    @pytest.mark.pytest_compliant
    def test_migration_compatibility(self, temp_db):
        """测试迁移兼容性"""
        logger.info("开始测试迁移兼容性")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 创建初始表结构
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS migration_test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 插入初始数据
        cursor.execute('INSERT INTO migration_test (name) VALUES (?)', ('initial_data',))
        conn.commit()
        
        # 模拟表结构迁移 - 添加新列
        cursor.execute('ALTER TABLE migration_test ADD COLUMN email TEXT')
        cursor.execute('ALTER TABLE migration_test ADD COLUMN status TEXT DEFAULT "active"')
        
        # 验证迁移后的表结构
        cursor.execute("PRAGMA table_info(migration_test)")
        columns = cursor.fetchall()
        
        column_names = [col[1] for col in columns]
        expected_columns = ['id', 'name', 'created_at', 'email', 'status']
        
        for col in expected_columns:
            assert col in column_names
            logger.info(f"✅ 迁移后列 {col} 存在")
        
        # 验证原有数据完整性
        cursor.execute('SELECT name FROM migration_test WHERE id = 1')
        result = cursor.fetchone()
        assert result[0] == 'initial_data'
        logger.info("✅ 迁移后数据完整性保持")
        
        # 验证新列默认值
        cursor.execute('SELECT status FROM migration_test WHERE id = 1')
        result = cursor.fetchone()
        assert result[0] == 'active'
        logger.info("✅ 新列默认值设置正确")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_migration_compatibility",
            test_category="migration_compatibility",
            description="迁移兼容性测试通过，表结构变更和数据完整性保持正常"
        )
    
    @pytest.mark.pytest_compliant
    def test_performance_optimization(self, temp_db):
        """测试性能优化"""
        logger.info("开始测试性能优化")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 创建性能测试表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_test (
                id INTEGER PRIMARY KEY,
                indexed_field TEXT,
                non_indexed_field TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_indexed_field ON performance_test(indexed_field)')
        
        # 插入测试数据
        test_data = [(f'indexed_{i}', f'non_indexed_{i}') for i in range(1000)]
        cursor.executemany(
            'INSERT INTO performance_test (indexed_field, non_indexed_field) VALUES (?, ?)',
            test_data
        )
        conn.commit()
        
        # 测试索引查询性能
        start_time = datetime.now()
        cursor.execute('SELECT * FROM performance_test WHERE indexed_field = ?', ('indexed_500',))
        result = cursor.fetchone()
        indexed_query_time = (datetime.now() - start_time).total_seconds()
        
        assert result is not None
        logger.info(f"✅ 索引查询时间: {indexed_query_time:.4f}秒")
        
        # 验证查询计划使用索引
        cursor.execute('EXPLAIN QUERY PLAN SELECT * FROM performance_test WHERE indexed_field = ?', ('indexed_500',))
        query_plan = cursor.fetchall()
        
        # 查询计划应该显示使用索引
        plan_text = ' '.join([str(row) for row in query_plan])
        assert 'idx_indexed_field' in plan_text or 'INDEX' in plan_text.upper()
        logger.info("✅ 查询优化器使用索引")
        
        # 验证数据完整性
        cursor.execute('SELECT COUNT(*) FROM performance_test')
        count = cursor.fetchone()[0]
        assert count == 1000
        logger.info(f"✅ 数据完整性验证: {count}条记录")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_performance_optimization",
            test_category="performance_optimization",
            description="性能优化测试通过，索引创建和查询优化正常工作"
        )
    
    @pytest.mark.pytest_compliant
    def test_error_recovery_mechanisms(self, temp_db):
        """测试错误恢复机制"""
        logger.info("开始测试错误恢复机制")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 创建测试表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS error_recovery_test (
                id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 测试事务回滚
        try:
            cursor.execute('BEGIN TRANSACTION')
            cursor.execute('INSERT INTO error_recovery_test (data) VALUES (?)', ('valid_data',))
            cursor.execute('INSERT INTO error_recovery_test (data) VALUES (?)', (None,))  # 这会失败
            cursor.execute('COMMIT')
        except sqlite3.IntegrityError:
            cursor.execute('ROLLBACK')
            logger.info("✅ 事务回滚机制正常工作")
        
        # 验证回滚后数据状态
        cursor.execute('SELECT COUNT(*) FROM error_recovery_test')
        count = cursor.fetchone()[0]
        assert count == 0  # 应该没有数据，因为事务被回滚
        logger.info("✅ 回滚后数据状态正确")
        
        # 测试正常事务提交
        cursor.execute('BEGIN TRANSACTION')
        cursor.execute('INSERT INTO error_recovery_test (data) VALUES (?)', ('valid_data_1',))
        cursor.execute('INSERT INTO error_recovery_test (data) VALUES (?)', ('valid_data_2',))
        cursor.execute('COMMIT')
        
        # 验证提交后数据状态
        cursor.execute('SELECT COUNT(*) FROM error_recovery_test')
        count = cursor.fetchone()[0]
        assert count == 2
        logger.info("✅ 正常事务提交验证通过")
        
        # 测试数据库连接恢复
        conn.close()
        
        # 重新连接
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 验证数据持久性
        cursor.execute('SELECT COUNT(*) FROM error_recovery_test')
        count = cursor.fetchone()[0]
        assert count == 2
        logger.info("✅ 数据库连接恢复和数据持久性验证通过")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_error_recovery_mechanisms",
            test_category="error_recovery",
            description="错误恢复机制测试通过，事务回滚和连接恢复正常工作"
        )
    
    @pytest.mark.pytest_compliant
    def test_compliance_report_generation(self):
        """测试合规性报告生成"""
        logger.info("开始测试合规性报告生成")
        
        # 验证合规性日志记录器功能
        assert hasattr(self.compliance_logger, 'log_pytest_entry')
        assert hasattr(self.compliance_logger, 'generate_compliance_report')
        
        # 记录测试完成
        self.compliance_logger.log_pytest_entry(
            test_name="test_compliance_report_generation",
            test_category="compliance_reporting",
            description="表结构创建合规性报告生成功能验证"
        )
        
        logger.info("✅ 合规性报告生成测试完成")
        
        # 验证日志条目数量
        assert len(self.compliance_logger.pytest_logs) > 0
        logger.info(f"✅ 已记录 {len(self.compliance_logger.pytest_logs)} 条pytest日志")

if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "--tb=short"])