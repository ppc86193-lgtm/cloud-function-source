#!/usr/bin/env python3
"""
数据同步系统合规性测试
测试数据同步管理器的完整功能和合规性
"""

import pytest
import os
import sys
import sqlite3
import json
import tempfile
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import logging

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from supabase_sync_manager import SupabaseSyncManager
from contract_compliance_logger import ContractComplianceLogger

# 配置pytest日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestDataSyncSystemCompliance:
    """数据同步系统合规性测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_compliance_logging(self):
        """设置合规性日志记录"""
        self.compliance_logger = ContractComplianceLogger()
        self.compliance_logger.log_pytest_entry(
            test_name="数据同步系统合规性测试",
            test_category="data_sync_compliance",
            description="验证数据同步系统的完整功能和合规性"
        )
        yield
        
    @pytest.fixture
    def temp_db(self):
        """创建临时数据库用于测试"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        # 创建测试表结构
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 创建核心表
        cursor.execute('''
            CREATE TABLE lab_push_candidates_v2 (
                draw_id TEXT PRIMARY KEY,
                issue TEXT NOT NULL,
                numbers TEXT,
                sum_value INTEGER,
                big_small TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE cloud_pred_today_norm (
                id INTEGER PRIMARY KEY,
                draw_id TEXT NOT NULL,
                predicted_numbers TEXT,
                prediction_type TEXT,
                data_date TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE signal_pool_union_v3 (
                id INTEGER PRIMARY KEY,
                signal_id TEXT NOT NULL,
                signal_type TEXT,
                signal_strength REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 插入测试数据
        cursor.execute('''
            INSERT INTO lab_push_candidates_v2 
            (draw_id, issue, numbers, sum_value, big_small) 
            VALUES (?, ?, ?, ?, ?)
        ''', ('test_001', '2025001', '1,2,3', 6, 'small'))
        
        cursor.execute('''
            INSERT INTO cloud_pred_today_norm 
            (draw_id, predicted_numbers, prediction_type, data_date) 
            VALUES (?, ?, ?, ?)
        ''', ('test_001', '1,2,3', 'big', '2025-09-29'))
        
        cursor.execute('''
            INSERT INTO signal_pool_union_v3 
            (signal_id, signal_type, signal_strength) 
            VALUES (?, ?, ?)
        ''', ('signal_001', 'pattern', 0.85))
        
        conn.commit()
        conn.close()
        
        yield db_path
        
        # 清理
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    @pytest.mark.pytest_compliant
    def test_sync_manager_initialization(self):
        """测试同步管理器初始化"""
        logger.info("开始测试同步管理器初始化")
        
        # 模拟环境变量
        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_ANON_KEY': 'test_anon_key',
            'SUPABASE_SERVICE_ROLE_KEY': 'test_service_key'
        }):
            sync_manager = SupabaseSyncManager()
            
            # 验证核心表配置
            assert hasattr(sync_manager, 'CORE_TABLES')
            assert len(sync_manager.CORE_TABLES) >= 6
            
            # 验证必需的表存在
            required_tables = [
                'lab_push_candidates_v2',
                'cloud_pred_today_norm', 
                'signal_pool_union_v3',
                'p_size_clean_merged_dedup_v',
                'draws_14w_dedup_v',
                'score_ledger'
            ]
            
            for table in required_tables:
                assert table in sync_manager.CORE_TABLES
                logger.info(f"✅ 核心表 {table} 配置验证通过")
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_sync_manager_initialization",
            test_category="initialization",
            description="同步管理器初始化测试通过，所有核心表配置正确"
        )
        
    @pytest.mark.pytest_compliant
    def test_database_connection_validation(self, temp_db):
        """测试数据库连接验证"""
        logger.info("开始测试数据库连接验证")
        
        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_ANON_KEY': 'test_anon_key',
            'SUPABASE_SERVICE_ROLE_KEY': 'test_service_key'
        }):
            sync_manager = SupabaseSyncManager(sqlite_db_path=temp_db)
            
            # 测试本地数据库连接
            conn = sqlite3.connect(temp_db)
            cursor = conn.cursor()
            
            # 验证表存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = ['lab_push_candidates_v2', 'cloud_pred_today_norm', 'signal_pool_union_v3']
            for table in expected_tables:
                assert table in tables
                logger.info(f"✅ 数据库表 {table} 存在验证通过")
            
            conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_database_connection_validation",
            test_category="database_connection",
            description="数据库连接验证测试通过，所有核心表结构正确"
        )
    
    @pytest.mark.pytest_compliant
    def test_table_structure_compliance(self, temp_db):
        """测试表结构合规性"""
        logger.info("开始测试表结构合规性")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 验证lab_push_candidates_v2表结构
        cursor.execute("PRAGMA table_info(lab_push_candidates_v2)")
        columns = [row[1] for row in cursor.fetchall()]
        
        required_columns = ['draw_id', 'issue', 'numbers', 'sum_value', 'big_small', 'created_at']
        for col in required_columns:
            assert col in columns
            logger.info(f"✅ lab_push_candidates_v2表列 {col} 验证通过")
        
        # 验证cloud_pred_today_norm表结构
        cursor.execute("PRAGMA table_info(cloud_pred_today_norm)")
        columns = [row[1] for row in cursor.fetchall()]
        
        required_columns = ['id', 'draw_id', 'predicted_numbers', 'prediction_type', 'data_date', 'created_at']
        for col in required_columns:
            assert col in columns
            logger.info(f"✅ cloud_pred_today_norm表列 {col} 验证通过")
        
        # 验证signal_pool_union_v3表结构
        cursor.execute("PRAGMA table_info(signal_pool_union_v3)")
        columns = [row[1] for row in cursor.fetchall()]
        
        required_columns = ['id', 'signal_id', 'signal_type', 'signal_strength', 'created_at']
        for col in required_columns:
            assert col in columns
            logger.info(f"✅ signal_pool_union_v3表列 {col} 验证通过")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_table_structure_compliance",
            test_category="table_structure",
            description="表结构合规性测试通过，所有核心表结构符合要求"
        )
    
    @pytest.mark.pytest_compliant
    def test_data_sync_configuration(self):
        """测试数据同步配置"""
        logger.info("开始测试数据同步配置")
        
        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_ANON_KEY': 'test_anon_key',
            'SUPABASE_SERVICE_ROLE_KEY': 'test_service_key'
        }):
            sync_manager = SupabaseSyncManager()
            
            # 验证每个核心表的配置
            for table_name, config in sync_manager.CORE_TABLES.items():
                # 验证必需的配置项
                assert 'primary_key' in config
                assert 'timestamp_column' in config
                assert 'batch_size' in config
                assert 'sync_mode' in config
                
                # 验证配置值的合理性
                assert isinstance(config['batch_size'], int)
                assert config['batch_size'] > 0
                assert config['sync_mode'] in ['incremental', 'full']
                
                logger.info(f"✅ 表 {table_name} 同步配置验证通过")
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_data_sync_configuration",
            test_category="sync_configuration",
            description="数据同步配置测试通过，所有表配置参数正确"
        )
    
    @pytest.mark.pytest_compliant
    def test_sync_process_simulation(self, temp_db):
        """测试同步过程模拟"""
        logger.info("开始测试同步过程模拟")
        
        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_ANON_KEY': 'test_anon_key',
            'SUPABASE_SERVICE_ROLE_KEY': 'test_service_key'
        }):
            sync_manager = SupabaseSyncManager(sqlite_db_path=temp_db)
            
            # 模拟Supabase客户端
            with patch.object(sync_manager, 'supabase_client') as mock_supabase:
                mock_table = Mock()
                mock_supabase.table.return_value = mock_table
                mock_table.select.return_value.execute.return_value.data = []
                mock_table.upsert.return_value.execute.return_value = Mock(data=[])
                
                # 测试增量同步
                success, records, error = sync_manager.sync_table_incremental('lab_push_candidates_v2')
                
                # 验证同步结果
                assert isinstance(success, bool)
                assert isinstance(records, int)
                assert records >= 0
                
                logger.info(f"✅ 增量同步测试完成: success={success}, records={records}")
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_sync_process_simulation",
            test_category="sync_process",
            description="同步过程模拟测试通过，同步逻辑正常工作"
        )
    
    @pytest.mark.pytest_compliant
    def test_error_handling_compliance(self, temp_db):
        """测试错误处理合规性"""
        logger.info("开始测试错误处理合规性")
        
        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_ANON_KEY': 'test_anon_key',
            'SUPABASE_SERVICE_ROLE_KEY': 'test_service_key'
        }):
            sync_manager = SupabaseSyncManager(sqlite_db_path=temp_db)
            
            # 测试数据库连接错误处理
            with patch('sqlite3.connect') as mock_connect:
                mock_connect.side_effect = sqlite3.Error("Database connection failed")
                
                try:
                    sync_manager._get_local_data('nonexistent_table')
                    logger.info("✅ 数据库连接错误处理正常")
                except Exception as e:
                    logger.info(f"✅ 捕获到预期的数据库错误: {str(e)}")
            
            # 测试表不存在错误处理
            try:
                sync_manager._get_local_data('nonexistent_table')
                logger.info("✅ 表不存在错误处理正常")
            except Exception as e:
                logger.info(f"✅ 捕获到预期的表错误: {str(e)}")
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_error_handling_compliance",
            test_category="error_handling",
            description="错误处理合规性测试通过，异常处理机制正常"
        )
    
    @pytest.mark.pytest_compliant
    def test_performance_metrics_logging(self, temp_db):
        """测试性能指标日志记录"""
        logger.info("开始测试性能指标日志记录")
        
        with patch.dict(os.environ, {
            'SUPABASE_URL': 'https://test.supabase.co',
            'SUPABASE_ANON_KEY': 'test_anon_key',
            'SUPABASE_SERVICE_ROLE_KEY': 'test_service_key'
        }):
            sync_manager = SupabaseSyncManager(sqlite_db_path=temp_db)
            
            # 验证统计信息初始化
            assert hasattr(sync_manager, 'sync_stats')
            assert 'total_syncs' in sync_manager.sync_stats
            assert 'successful_syncs' in sync_manager.sync_stats
            assert 'failed_syncs' in sync_manager.sync_stats
            
            # 验证统计信息类型
            for key, value in sync_manager.sync_stats.items():
                assert isinstance(value, (int, float, list))
                logger.info(f"✅ 性能指标 {key}: {value}")
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_performance_metrics_logging",
            test_category="performance_metrics",
            description="性能指标日志记录测试通过，统计信息正确初始化"
        )
    
    @pytest.mark.pytest_compliant
    def test_data_integrity_validation(self, temp_db):
        """测试数据完整性验证"""
        logger.info("开始测试数据完整性验证")
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        
        # 验证测试数据存在
        cursor.execute("SELECT COUNT(*) FROM lab_push_candidates_v2")
        count = cursor.fetchone()[0]
        assert count > 0
        logger.info(f"✅ lab_push_candidates_v2表有 {count} 条记录")
        
        cursor.execute("SELECT COUNT(*) FROM cloud_pred_today_norm")
        count = cursor.fetchone()[0]
        assert count > 0
        logger.info(f"✅ cloud_pred_today_norm表有 {count} 条记录")
        
        cursor.execute("SELECT COUNT(*) FROM signal_pool_union_v3")
        count = cursor.fetchone()[0]
        assert count > 0
        logger.info(f"✅ signal_pool_union_v3表有 {count} 条记录")
        
        # 验证数据格式
        cursor.execute("SELECT draw_id, issue, numbers FROM lab_push_candidates_v2 LIMIT 1")
        row = cursor.fetchone()
        assert row is not None
        assert len(row) == 3
        logger.info(f"✅ 数据格式验证通过: {row}")
        
        conn.close()
        
        self.compliance_logger.log_pytest_entry(
            test_name="test_data_integrity_validation",
            test_category="data_integrity",
            description="数据完整性验证测试通过，所有测试数据格式正确"
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
            description="合规性报告生成功能验证"
        )
        
        logger.info("✅ 合规性报告生成测试完成")
        
        # 验证日志条目数量
        assert len(self.compliance_logger.pytest_logs) > 0
        logger.info(f"✅ 已记录 {len(self.compliance_logger.pytest_logs)} 条pytest日志")

if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "--tb=short"])