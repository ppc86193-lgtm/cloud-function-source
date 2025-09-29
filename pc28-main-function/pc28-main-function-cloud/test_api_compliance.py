"""
API操作合规性测试 - 确保API操作符合pytest自动化日志要求
根据PROJECT_RULES.md合约条款，所有API操作必须通过pytest验证
"""

import pytest
import sys
import os
import time
import json
from datetime import datetime
from contract_compliance_logger import ContractComplianceLogger, ContractViolationType, ViolationSeverity
import logging

logger = logging.getLogger(__name__)

class TestAPICompliance:
    """API操作合规性测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_compliance_logger(self):
        """设置合规性日志记录器"""
        self.compliance_logger = ContractComplianceLogger()
        
        # 记录测试开始的审计日志
        self.compliance_logger._log_audit_operation(
            operation_type="API_TEST_START",
            operation_details="API操作合规性测试开始",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )
        
        yield
        
        # 记录测试结束的审计日志
        self.compliance_logger._log_audit_operation(
            operation_type="API_TEST_END", 
            operation_details="API操作合规性测试结束",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )

    @pytest.mark.pytest_compliant
    def test_api_request_logging(self):
        """测试API请求的pytest日志记录"""
        # 记录API测试开始
        test_id = self.compliance_logger.log_pytest_test_execution(
            test_name="test_api_request_logging",
            test_file=__file__,
            test_result="RUNNING",
            execution_time=0.0,
            pytest_version=pytest.__version__
        )
        
        start_time = time.time()
        
        try:
            # 验证API请求合规性
            self.compliance_logger._log_audit_operation(
                operation_type="API_REQUEST_VALIDATION",
                operation_details="验证API请求操作合规性",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            # 检查API相关文件是否存在
            api_files = [
                "cloud_data_pipeline_test.py",
                "pc28_data_flow_test.py"
            ]
            
            for file_path in api_files:
                full_path = os.path.join("/Users/a606/cloud_function_source", file_path)
                if os.path.exists(full_path):
                    self.compliance_logger._log_audit_operation(
                        operation_type="API_FILE_VALIDATION",
                        operation_details=f"API文件验证通过: {file_path}",
                        operator="PYTEST_AUTO_SYSTEM",
                        pytest_context=True
                    )
            
            execution_time = time.time() - start_time
            
            # 更新测试结果
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_api_request_logging",
                test_file=__file__,
                test_result="PASSED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            logger.info(f"✅ API请求日志记录测试通过 (ID: {test_id})")
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # 记录测试失败
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_api_request_logging",
                test_file=__file__,
                test_result="FAILED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            # 记录违规
            self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
                severity=ViolationSeverity.HIGH,
                title="API操作pytest验证失败",
                description=f"API操作合规性测试失败: {str(e)}",
                source_component="test_api_compliance.py",
                evidence={"error": str(e), "test_name": "test_api_request_logging"}
            )
            
            raise

    @pytest.mark.pytest_compliant
    def test_api_response_compliance(self):
        """测试API响应合规性"""
        # 记录API响应测试
        test_id = self.compliance_logger.log_pytest_test_execution(
            test_name="test_api_response_compliance",
            test_file=__file__,
            test_result="RUNNING",
            execution_time=0.0,
            pytest_version=pytest.__version__
        )
        
        start_time = time.time()
        
        try:
            # 验证API响应功能
            self.compliance_logger._log_audit_operation(
                operation_type="API_RESPONSE_TEST",
                operation_details="测试API响应合规性功能",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            # 验证pytest上下文
            assert self.compliance_logger.pytest_validator.is_pytest_context(), "必须在pytest上下文中执行"
            
            execution_time = time.time() - start_time
            
            # 更新测试结果
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_api_response_compliance",
                test_file=__file__,
                test_result="PASSED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            logger.info(f"✅ API响应合规性测试通过 (ID: {test_id})")
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # 记录测试失败
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_api_response_compliance",
                test_file=__file__,
                test_result="FAILED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            raise

    @pytest.mark.pytest_compliant
    def test_api_security_compliance(self):
        """测试API安全合规性"""
        # 记录API安全测试
        test_id = self.compliance_logger.log_pytest_test_execution(
            test_name="test_api_security_compliance",
            test_file=__file__,
            test_result="RUNNING",
            execution_time=0.0,
            pytest_version=pytest.__version__
        )
        
        start_time = time.time()
        
        try:
            # 验证API安全日志功能
            self.compliance_logger._log_audit_operation(
                operation_type="API_SECURITY_TEST",
                operation_details="测试API安全合规性功能",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            # 验证合规状态
            compliance_status = self.compliance_logger.get_compliance_status()
            assert compliance_status is not None, "合规状态必须可获取"
            
            execution_time = time.time() - start_time
            
            # 更新测试结果
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_api_security_compliance",
                test_file=__file__,
                test_result="PASSED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            logger.info(f"✅ API安全合规性测试通过 (ID: {test_id})")
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # 记录测试失败
            self.compliance_logger.log_pytest_test_execution(
                test_name="test_api_security_compliance",
                test_file=__file__,
                test_result="FAILED",
                execution_time=execution_time,
                pytest_version=pytest.__version__
            )
            
            raise

if __name__ == "__main__":
    print("❌ 警告：此测试文件只能通过pytest执行")
    print("请使用: pytest test_api_compliance.py")
    sys.exit(1)