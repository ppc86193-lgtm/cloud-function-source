"""
pytest配置文件 - 强制执行pytest合规性
根据PROJECT_RULES.md合约条款，只认可pytest的自动化日志
"""

import pytest
import sys
import os
from contract_compliance_logger import ContractComplianceLogger, ContractViolationType, ViolationSeverity
from pytest_compliance_plugin import PytestCompliancePlugin
import logging

logger = logging.getLogger(__name__)

# 全局合规性检查器
compliance_logger = ContractComplianceLogger()

def pytest_configure(config):
    """pytest配置 - 强制合规性检查"""
    try:
        # 验证这是真正的pytest执行
        if not _is_genuine_pytest_execution():
            raise ValueError("❌ 严重违规：检测到非pytest执行尝试")
        
        # 记录pytest配置开始
        compliance_logger._log_audit_operation(
            operation_type="PYTEST_CONFIGURE",
            operation_details="pytest配置开始 - 强制执行合规性检查",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )
        
        # 注册合规性插件
        if not hasattr(config, '_compliance_plugin'):
            config._compliance_plugin = PytestCompliancePlugin()
            config.pluginmanager.register(config._compliance_plugin, "compliance_plugin")
        
        logger.info("✅ pytest合规性配置完成 - 只认可pytest自动化日志")
        
    except Exception as e:
        logger.error(f"pytest配置失败: {e}")
        _log_configuration_violation(str(e))
        raise

def pytest_sessionstart(session):
    """pytest会话开始 - 验证合规性"""
    try:
        # 再次验证pytest上下文
        if not _is_genuine_pytest_execution():
            _log_non_pytest_violation("session_start", "会话开始阶段")
            raise ValueError("❌ 严重违规：非pytest会话开始")
        
        # 记录会话开始
        compliance_logger._log_audit_operation(
            operation_type="PYTEST_SESSION_START",
            operation_details=f"pytest会话开始 - 工作目录: {os.getcwd()}",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )
        
        logger.info("✅ pytest会话已开始 - 合规性验证通过")
        
    except Exception as e:
        logger.error(f"pytest会话开始失败: {e}")
        _log_session_violation(str(e))

def pytest_collection_modifyitems(config, items):
    """修改收集的测试项 - 添加合规性标记"""
    try:
        for item in items:
            # 为每个测试项添加合规性标记
            item.add_marker(pytest.mark.pytest_compliant)
            
            # 验证测试文件路径
            if hasattr(item, 'fspath'):
                test_file = str(item.fspath)
                if not _validate_test_file(test_file):
                    _log_invalid_test_file_violation(test_file)
        
        # 记录测试收集完成
        compliance_logger._log_audit_operation(
            operation_type="PYTEST_COLLECTION_COMPLETE",
            operation_details=f"pytest测试收集完成 - 共 {len(items)} 个测试",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )
        
    except Exception as e:
        logger.error(f"pytest测试收集修改失败: {e}")
        _log_collection_violation(str(e))

def pytest_runtest_setup(item):
    """测试设置 - 合规性检查"""
    try:
        # 验证pytest上下文
        if not _is_genuine_pytest_execution():
            _log_non_pytest_violation(item.nodeid, "测试设置阶段")
            raise ValueError(f"❌ 严重违规：测试 {item.nodeid} 非pytest执行")
        
        # 检查测试是否有合规性标记
        if not item.get_closest_marker("pytest_compliant"):
            _log_non_compliant_test_violation(item.nodeid)
        
    except Exception as e:
        logger.error(f"测试设置合规性检查失败: {e}")
        _log_setup_violation(item.nodeid, str(e))

def pytest_sessionfinish(session, exitstatus):
    """pytest会话结束 - 生成合规性报告"""
    try:
        # 记录会话结束
        compliance_logger._log_audit_operation(
            operation_type="PYTEST_SESSION_FINISH",
            operation_details=f"pytest会话结束 - 退出状态: {exitstatus}",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )
        
        # 生成合规性报告
        _generate_compliance_report(session, exitstatus)
        
        logger.info("✅ pytest会话结束 - 合规性报告已生成")
        
    except Exception as e:
        logger.error(f"pytest会话结束处理失败: {e}")

def _is_genuine_pytest_execution() -> bool:
    """验证是否为真正的pytest执行"""
    return (
        'pytest' in sys.modules and
        hasattr(sys.modules.get('pytest', None), 'main') and
        any('pytest' in str(arg).lower() for arg in sys.argv) and
        'PYTEST_CURRENT_TEST' in os.environ or any('test' in str(arg) for arg in sys.argv)
    )

def _validate_test_file(test_file: str) -> bool:
    """验证测试文件有效性"""
    return (
        test_file.endswith('.py') and
        ('test_' in os.path.basename(test_file) or test_file.endswith('_test.py')) and
        os.path.exists(test_file)
    )

def _log_non_pytest_violation(test_name: str, phase: str):
    """记录非pytest执行违规"""
    try:
        compliance_logger.log_contract_violation(
            violation_type=ContractViolationType.NON_PYTEST_EXECUTION,
            severity=ViolationSeverity.CRITICAL,
            title=f"非pytest执行违规 - {phase}",
            description=f"在 {phase} 检测到非pytest执行: {test_name}",
            source_component="conftest.py",
            evidence={
                'test_name': test_name,
                'phase': phase,
                'sys_argv': sys.argv,
                'pytest_in_modules': 'pytest' in sys.modules,
                'violation_type': 'non_pytest_execution'
            }
        )
    except Exception as e:
        logger.error(f"记录非pytest违规失败: {e}")

def _log_configuration_violation(error_message: str):
    """记录配置违规"""
    try:
        compliance_logger.log_contract_violation(
            violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
            severity=ViolationSeverity.HIGH,
            title="pytest配置违规",
            description=f"pytest配置过程中发生错误: {error_message}",
            source_component="conftest.py",
            evidence={
                'error_message': error_message,
                'configuration_phase': True,
                'violation_type': 'configuration_failure'
            }
        )
    except Exception as e:
        logger.error(f"记录配置违规失败: {e}")

def _log_session_violation(error_message: str):
    """记录会话违规"""
    try:
        compliance_logger.log_contract_violation(
            violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
            severity=ViolationSeverity.HIGH,
            title="pytest会话违规",
            description=f"pytest会话过程中发生错误: {error_message}",
            source_component="conftest.py",
            evidence={
                'error_message': error_message,
                'session_phase': True,
                'violation_type': 'session_failure'
            }
        )
    except Exception as e:
        logger.error(f"记录会话违规失败: {e}")

def _log_invalid_test_file_violation(test_file: str):
    """记录无效测试文件违规"""
    try:
        compliance_logger.log_contract_violation(
            violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
            severity=ViolationSeverity.MEDIUM,
            title="无效测试文件",
            description=f"检测到无效的测试文件: {test_file}",
            source_component="conftest.py",
            evidence={
                'test_file': test_file,
                'file_exists': os.path.exists(test_file),
                'violation_type': 'invalid_test_file'
            }
        )
    except Exception as e:
        logger.error(f"记录无效测试文件违规失败: {e}")

def _log_non_compliant_test_violation(test_name: str):
    """记录非合规测试违规"""
    try:
        compliance_logger.log_contract_violation(
            violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
            severity=ViolationSeverity.MEDIUM,
            title="非合规测试",
            description=f"测试 {test_name} 缺少合规性标记",
            source_component="conftest.py",
            evidence={
                'test_name': test_name,
                'missing_marker': 'pytest_compliant',
                'violation_type': 'non_compliant_test'
            }
        )
    except Exception as e:
        logger.error(f"记录非合规测试违规失败: {e}")

def _log_collection_violation(error_message: str):
    """记录收集违规"""
    try:
        compliance_logger.log_contract_violation(
            violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
            severity=ViolationSeverity.MEDIUM,
            title="pytest收集违规",
            description=f"pytest测试收集过程中发生错误: {error_message}",
            source_component="conftest.py",
            evidence={
                'error_message': error_message,
                'collection_phase': True,
                'violation_type': 'collection_failure'
            }
        )
    except Exception as e:
        logger.error(f"记录收集违规失败: {e}")

def _log_setup_violation(test_name: str, error_message: str):
    """记录设置违规"""
    try:
        compliance_logger.log_contract_violation(
            violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
            severity=ViolationSeverity.MEDIUM,
            title="pytest设置违规",
            description=f"测试 {test_name} 设置过程中发生错误: {error_message}",
            source_component="conftest.py",
            evidence={
                'test_name': test_name,
                'error_message': error_message,
                'setup_phase': True,
                'violation_type': 'setup_failure'
            }
        )
    except Exception as e:
        logger.error(f"记录设置违规失败: {e}")

def _generate_compliance_report(session, exitstatus):
    """生成合规性报告"""
    try:
        report_data = {
            'session_id': id(session),
            'exit_status': exitstatus,
            'pytest_version': pytest.__version__,
            'python_version': sys.version,
            'working_directory': os.getcwd(),
            'command_line': ' '.join(sys.argv),
            'compliance_status': 'COMPLIANT' if exitstatus == 0 else 'NON_COMPLIANT'
        }
        
        compliance_logger._log_audit_operation(
            operation_type="PYTEST_COMPLIANCE_REPORT",
            operation_details=f"pytest合规性报告: {report_data}",
            operator="PYTEST_AUTO_SYSTEM",
            pytest_context=True
        )
        
    except Exception as e:
        logger.error(f"生成合规性报告失败: {e}")