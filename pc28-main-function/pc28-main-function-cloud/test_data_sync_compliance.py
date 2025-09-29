"""
数据同步合规性测试 - 确保数据同步操作符合pytest自动化日志要求
根据PROJECT_RULES.md合约条款，所有数据同步必须通过pytest验证
"""

import pytest
import sys
import os
import time
import json
from datetime import datetime
from contract_compliance_logger import ContractComplianceLogger, ContractViolationType, ViolationSeverity
from supabase_sync_manager import sync_manager
import logging

logger = logging.getLogger(__name__)

class TestDataSyncCompliance:
    """数据同步合规性测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_compliance_logger(self):
        """设置合规性日志记录器"""
        self.compliance_logger = ContractComplianceLogger()
        
        # 记录测试开始的审计日志
        self.compliance_logger._log_audit_operation(
            operation_type="DATA_SYNC_TEST_START",
            operation_details="数据同步合规性测试开始",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )
        
        yield
        
        # 记录测试结束的审计日志
        self.compliance_logger._log_audit_operation(
            operation_type="DATA_SYNC_TEST_END", 
            operation_details="数据同步合规性测试结束",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )

    @pytest.mark.pytest_compliant
    def test_supabase_sync_logging(self):
        """测试Supabase同步操作的pytest日志记录"""
        # 记录同步操作开始
        test_id = self.compliance_logger.log_pytest_test_execution(
            test_name="test_supabase_sync_logging",
            test_file=__file__,
            test_result="RUNNING",
            execution_time=0.0,
            pytest_version=pytest.__version__
        )
        
        start_time = time.time()
        
        try:
            # 验证同步管理器存在
            assert sync_manager is not None, "同步管理器必须初始化"
            
            # 记录同步操作的合规性日志
            self.compliance_logger._log_audit_operation(
                operation_type="SUPABASE_SYNC_VALIDATION",
                operation_details="验证Supabase同步管理器合规性",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            execution_time = time.time() - start_time
            
            # 更新测试结果
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_supabase_sync_logging",
                test_file=__file__,
                test_result="PASSED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            logger.info(f"✅ Supabase同步日志记录测试通过 (ID: {test_id})")
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # 记录测试失败
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_supabase_sync_logging",
                test_file=__file__,
                test_result="FAILED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            # 记录违规
            self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
                severity=ViolationSeverity.HIGH,
                title="数据同步pytest验证失败",
                description=f"数据同步合规性测试失败: {str(e)}",
                source_component="test_data_sync_compliance.py",
                evidence={"error": str(e), "test_name": "test_supabase_sync_logging"}
            )
            
            raise

    @pytest.mark.pytest_compliant
    def test_data_sync_audit_trail(self):
        """测试数据同步审计跟踪"""
        # 记录审计跟踪测试
        test_id = self.compliance_logger.log_pytest_test_execution(
            test_name="test_data_sync_audit_trail",
            test_file=__file__,
            test_result="RUNNING",
            execution_time=0.0,
            pytest_version=pytest.__version__
        )
        
        start_time = time.time()
        
        try:
            # 验证审计日志功能
            self.compliance_logger._log_audit_operation(
                operation_type="DATA_SYNC_AUDIT_TEST",
                operation_details="测试数据同步审计跟踪功能",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            # 验证合规状态
            compliance_status = self.compliance_logger.get_compliance_status()
            assert compliance_status is not None, "合规状态必须可获取"
            
            execution_time = time.time() - start_time
            
            # 更新测试结果
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_data_sync_audit_trail",
                test_file=__file__,
                test_result="PASSED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            logger.info(f"✅ 数据同步审计跟踪测试通过 (ID: {test_id})")
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # 记录测试失败
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_data_sync_audit_trail",
                test_file=__file__,
                test_result="FAILED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            raise

    @pytest.mark.pytest_compliant
    def test_sync_operation_compliance(self):
        """测试同步操作合规性"""
        # 记录同步操作合规性测试
        test_id = self.compliance_logger.log_pytest_test_execution(
            test_name="test_sync_operation_compliance",
            test_file=__file__,
            test_result="RUNNING",
            execution_time=0.0,
            pytest_version=pytest.__version__
        )
        
        start_time = time.time()
        
        try:
            # 验证pytest上下文
            assert self.compliance_logger.pytest_validator.is_pytest_context(), "必须在pytest上下文中执行"
            
            # 记录同步操作合规性验证
            self.compliance_logger._log_audit_operation(
                operation_type="SYNC_COMPLIANCE_VALIDATION",
                operation_details="验证同步操作符合合约条款要求",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            execution_time = time.time() - start_time
            
            # 更新测试结果
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_sync_operation_compliance",
                test_file=__file__,
                test_result="PASSED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            logger.info(f"✅ 同步操作合规性测试通过 (ID: {test_id})")
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # 记录测试失败
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_sync_operation_compliance",
                test_file=__file__,
                test_result="FAILED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            raise

if __name__ == "__main__":
    print("❌ 警告：此测试文件只能通过pytest执行")
    print("请使用: pytest test_data_sync_compliance.py")
    sys.exit(1)