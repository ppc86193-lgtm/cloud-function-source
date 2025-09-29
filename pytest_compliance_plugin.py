"""
pytest合规性插件 - 确保只认可pytest的自动化日志
根据PROJECT_RULES.md合约条款，所有测试必须通过pytest自动化执行
"""

import pytest
import time
import os
import sys
from typing import Dict, Any, Optional
from contract_compliance_logger import ContractComplianceLogger, ContractViolationType, ViolationSeverity
import logging

logger = logging.getLogger(__name__)

class PytestCompliancePlugin:
    """pytest合规性插件 - 强制执行pytest日志记录"""
    
    def __init__(self):
        self.compliance_logger = ContractComplianceLogger()
        self.test_start_times: Dict[str, float] = {}
        self.session_start_time = None
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
        self.skipped_tests = 0
        
    def pytest_configure(self, config):
        """pytest配置阶段 - 初始化合规性日志"""
        try:
            # 验证pytest环境
            if not hasattr(config, 'pluginmanager'):
                raise ValueError("❌ 严重违规：非pytest环境尝试执行测试")
            
            # 记录pytest会话开始
            self.compliance_logger._log_audit_operation(
                operation_type="PYTEST_SESSION_START",
                operation_details=f"pytest会话开始 - 版本: {pytest.__version__}",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            logger.info("✅ pytest合规性插件已激活 - 只认可pytest自动化日志")
            
        except Exception as e:
            logger.error(f"pytest合规性插件配置失败: {e}")
            # 记录配置失败违规
            self._log_configuration_violation(str(e))
    
    def pytest_sessionstart(self, session):
        """pytest会话开始"""
        self.session_start_time = time.time()
        
        # 记录会话开始的合规性日志
        try:
            self.compliance_logger._log_audit_operation(
                operation_type="PYTEST_SESSION_INITIALIZED",
                operation_details=f"pytest测试会话已初始化 - 工作目录: {os.getcwd()}",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
        except Exception as e:
            logger.error(f"记录pytest会话开始失败: {e}")
    
    def pytest_runtest_setup(self, item):
        """测试设置阶段"""
        test_name = item.nodeid
        self.test_start_times[test_name] = time.time()
        
        # 验证测试是否在pytest上下文中执行
        if not self._verify_pytest_context():
            self._log_non_pytest_violation(test_name, "测试设置阶段")
    
    def pytest_runtest_call(self, item):
        """测试执行阶段"""
        test_name = item.nodeid
        
        # 再次验证pytest上下文
        if not self._verify_pytest_context():
            self._log_non_pytest_violation(test_name, "测试执行阶段")
    
    def pytest_runtest_teardown(self, item):
        """测试清理阶段"""
        test_name = item.nodeid
        
        # 验证pytest上下文
        if not self._verify_pytest_context():
            self._log_non_pytest_violation(test_name, "测试清理阶段")
    
    def pytest_runtest_logreport(self, report):
        """测试报告阶段 - 记录测试结果"""
        if report.when == "call":  # 只在测试调用阶段记录
            test_name = report.nodeid
            test_file = report.fspath if hasattr(report, 'fspath') else "unknown"
            
            # 计算执行时间
            start_time = self.test_start_times.get(test_name, time.time())
            execution_time = time.time() - start_time
            
            # 确定测试结果
            if report.passed:
                test_result = "PASSED"
                self.passed_tests += 1
            elif report.failed:
                test_result = "FAILED"
                self.failed_tests += 1
            elif report.skipped:
                test_result = "SKIPPED"
                self.skipped_tests += 1
            else:
                test_result = "UNKNOWN"
            
            self.total_tests += 1
            
            # 记录pytest测试执行 - 唯一认可的日志来源
            try:
                test_id = self.compliance_logger.log_pytest_test_execution(
                    test_name=test_name,
                    test_file=str(test_file),
                    test_result=test_result,
                    execution_time=execution_time,
                    pytest_version=pytest.__version__
                )
                
                logger.info(f"✅ pytest测试已记录: {test_name} - {test_result} (ID: {test_id})")
                
            except Exception as e:
                logger.error(f"记录pytest测试失败: {e}")
                # 记录日志记录失败的违规
                self._log_logging_failure_violation(test_name, str(e))
    
    def pytest_sessionfinish(self, session, exitstatus):
        """pytest会话结束"""
        if self.session_start_time:
            total_time = time.time() - self.session_start_time
        else:
            total_time = 0
        
        # 记录会话结束的合规性日志
        try:
            session_summary = {
                'total_tests': self.total_tests,
                'passed': self.passed_tests,
                'failed': self.failed_tests,
                'skipped': self.skipped_tests,
                'total_time': total_time,
                'exit_status': exitstatus
            }
            
            self.compliance_logger._log_audit_operation(
                operation_type="PYTEST_SESSION_COMPLETE",
                operation_details=f"pytest会话完成 - {session_summary}",
                operator="PYTEST_AUTO_SYSTEM",
                pytest_context=True
            )
            
            logger.info(f"✅ pytest会话完成 - 总计 {self.total_tests} 个测试")
            
        except Exception as e:
            logger.error(f"记录pytest会话结束失败: {e}")
    
    def _verify_pytest_context(self) -> bool:
        """验证当前是否在pytest上下文中"""
        return (
            'pytest' in sys.modules and
            hasattr(sys.modules.get('pytest', None), 'main') and
            any('pytest' in arg for arg in sys.argv)
        )
    
    def _log_non_pytest_violation(self, test_name: str, phase: str):
        """记录非pytest执行的违规"""
        try:
            self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.NON_PYTEST_EXECUTION,
                severity=ViolationSeverity.CRITICAL,
                title=f"非pytest执行违规 - {phase}",
                description=f"测试 {test_name} 在 {phase} 未通过pytest执行，违反合约条款",
                source_component="pytest_compliance_plugin.py",
                evidence={
                    'test_name': test_name,
                    'phase': phase,
                    'pytest_context': self._verify_pytest_context(),
                    'sys_argv': sys.argv,
                    'violation_type': 'non_pytest_execution'
                }
            )
            
        except Exception as e:
            logger.error(f"记录非pytest违规失败: {e}")
    
    def _log_configuration_violation(self, error_message: str):
        """记录配置违规"""
        try:
            self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
                severity=ViolationSeverity.HIGH,
                title="pytest配置违规",
                description=f"pytest合规性插件配置失败: {error_message}",
                source_component="pytest_compliance_plugin.py",
                evidence={
                    'error_message': error_message,
                    'configuration_phase': True,
                    'violation_type': 'configuration_failure'
                }
            )
            
        except Exception as e:
            logger.error(f"记录配置违规失败: {e}")
    
    def _log_logging_failure_violation(self, test_name: str, error_message: str):
        """记录日志记录失败的违规"""
        try:
            self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
                severity=ViolationSeverity.HIGH,
                title="pytest日志记录失败",
                description=f"测试 {test_name} 的pytest日志记录失败: {error_message}",
                source_component="pytest_compliance_plugin.py",
                evidence={
                    'test_name': test_name,
                    'error_message': error_message,
                    'logging_failure': True,
                    'violation_type': 'logging_failure'
                }
            )
            
        except Exception as e:
            logger.error(f"记录日志失败违规失败: {e}")


# pytest插件注册
def pytest_configure(config):
    """注册pytest合规性插件"""
    if not hasattr(config, '_compliance_plugin'):
        config._compliance_plugin = PytestCompliancePlugin()
        config.pluginmanager.register(config._compliance_plugin, "compliance_plugin")


def pytest_sessionstart(session):
    """会话开始钩子"""
    if hasattr(session.config, '_compliance_plugin'):
        session.config._compliance_plugin.pytest_sessionstart(session)


def pytest_runtest_setup(item):
    """测试设置钩子"""
    if hasattr(item.config, '_compliance_plugin'):
        item.config._compliance_plugin.pytest_runtest_setup(item)


def pytest_runtest_call(item):
    """测试执行钩子"""
    if hasattr(item.config, '_compliance_plugin'):
        item.config._compliance_plugin.pytest_runtest_call(item)


def pytest_runtest_teardown(item):
    """测试清理钩子"""
    if hasattr(item.config, '_compliance_plugin'):
        item.config._compliance_plugin.pytest_runtest_teardown(item)


def pytest_runtest_logreport(report):
    """测试报告钩子"""
    if hasattr(report.config, '_compliance_plugin'):
        report.config._compliance_plugin.pytest_runtest_logreport(report)


def pytest_sessionfinish(session, exitstatus):
    """会话结束钩子"""
    if hasattr(session.config, '_compliance_plugin'):
        session.config._compliance_plugin.pytest_sessionfinish(session, exitstatus)