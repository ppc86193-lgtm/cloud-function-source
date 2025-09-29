"""
检查并标记所有非pytest生成的日志为违规
根据PROJECT_RULES.md合约条款，只认可pytest的自动化日志
"""

import os
import re
import sqlite3
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from contract_compliance_logger import ContractComplianceLogger, ContractViolationType, ViolationSeverity

logger = logging.getLogger(__name__)

class NonPytestLogChecker:
    """非pytest日志检查器"""
    
    def __init__(self):
        self.compliance_logger = ContractComplianceLogger()
        self.violation_count = 0
        self.checked_files = []
        self.non_pytest_logs = []
        
    def check_all_logs(self) -> Dict[str, Any]:
        """检查所有日志文件，标记非pytest日志为违规"""
        try:
            logger.info("🔍 开始检查所有非pytest日志...")
            
            # 检查Python文件中的日志语句
            self._check_python_files()
            
            # 检查日志文件
            self._check_log_files()
            
            # 检查数据库中的日志记录
            self._check_database_logs()
            
            # 生成检查报告
            report = self._generate_check_report()
            
            logger.info(f"✅ 非pytest日志检查完成 - 发现 {self.violation_count} 个违规")
            
            return report
            
        except Exception as e:
            logger.error(f"检查非pytest日志失败: {e}")
            raise
    
    def _check_python_files(self):
        """检查Python文件中的日志语句"""
        try:
            python_files = self._find_python_files()
            
            for file_path in python_files:
                self._check_file_for_non_pytest_logs(file_path)
                
        except Exception as e:
            logger.error(f"检查Python文件失败: {e}")
    
    def _find_python_files(self) -> List[str]:
        """查找所有Python文件"""
        python_files = []
        
        for root, dirs, files in os.walk('.'):
            # 跳过虚拟环境和缓存目录
            dirs[:] = [d for d in dirs if d not in ['.venv', 'venv', '__pycache__', '.git']]
            
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    python_files.append(file_path)
        
        return python_files
    
    def _check_file_for_non_pytest_logs(self, file_path: str):
        """检查文件中的非pytest日志"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\\n')
            
            self.checked_files.append(file_path)
            
            # 检查可疑的日志模式
            suspicious_patterns = [
                r'logger\\.info\\(',
                r'logger\\.debug\\(',
                r'logger\\.warning\\(',
                r'logger\\.error\\(',
                r'print\\(',
                r'logging\\.info\\(',
                r'logging\\.debug\\(',
                r'logging\\.warning\\(',
                r'logging\\.error\\(',
            ]
            
            for line_num, line in enumerate(lines, 1):
                for pattern in suspicious_patterns:
                    if re.search(pattern, line) and not self._is_pytest_context_log(line, file_path):
                        self._log_non_pytest_violation(file_path, line_num, line.strip())
                        
        except Exception as e:
            logger.error(f"检查文件 {file_path} 失败: {e}")
    
    def _is_pytest_context_log(self, line: str, file_path: str) -> bool:
        """判断是否为pytest上下文中的日志"""
        # 检查是否在测试文件中
        if 'test_' in os.path.basename(file_path) or file_path.endswith('_test.py'):
            return True
        
        # 检查是否包含pytest相关标识
        pytest_indicators = [
            'pytest',
            'PYTEST_AUTO_SYSTEM',
            'pytest_context=True',
            'test_execution',
            'compliance_logger'
        ]
        
        return any(indicator in line for indicator in pytest_indicators)
    
    def _check_log_files(self):
        """检查日志文件"""
        try:
            log_extensions = ['.log', '.txt']
            
            for root, dirs, files in os.walk('.'):
                dirs[:] = [d for d in dirs if d not in ['.venv', 'venv', '__pycache__', '.git']]
                
                for file in files:
                    if any(file.endswith(ext) for ext in log_extensions):
                        file_path = os.path.join(root, file)
                        self._check_log_file_content(file_path)
                        
        except Exception as e:
            logger.error(f"检查日志文件失败: {e}")
    
    def _check_log_file_content(self, file_path: str):
        """检查日志文件内容"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if line and not self._is_pytest_generated_log(line):
                    self._log_non_pytest_log_file_violation(file_path, line_num, line)
                    
        except Exception as e:
            logger.error(f"检查日志文件 {file_path} 失败: {e}")
    
    def _is_pytest_generated_log(self, line: str) -> bool:
        """判断是否为pytest生成的日志"""
        pytest_indicators = [
            'PYTEST_AUTO_SYSTEM',
            'pytest',
            'test_execution',
            'compliance_logger',
            'contract_compliance'
        ]
        
        return any(indicator in line for indicator in pytest_indicators)
    
    def _check_database_logs(self):
        """检查数据库中的日志记录"""
        try:
            db_path = self.compliance_logger.db_path
            
            if not os.path.exists(db_path):
                logger.warning(f"数据库文件不存在: {db_path}")
                return
            
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # 检查审计日志表
                cursor.execute("""
                    SELECT id, operation_type, operation_details, operator, pytest_context
                    FROM audit_logs
                    WHERE pytest_context = FALSE OR pytest_context IS NULL
                """)
                
                non_pytest_audit_logs = cursor.fetchall()
                
                for log_record in non_pytest_audit_logs:
                    self._log_database_non_pytest_violation(log_record)
                
                # 检查违规记录表
                cursor.execute("""
                    SELECT violation_id, title, source_component, pytest_validated
                    FROM contract_violations
                    WHERE pytest_validated = FALSE OR pytest_validated IS NULL
                """)
                
                non_pytest_violations = cursor.fetchall()
                
                for violation_record in non_pytest_violations:
                    self._log_database_violation_non_pytest(violation_record)
                    
        except Exception as e:
            logger.error(f"检查数据库日志失败: {e}")
    
    def _log_non_pytest_violation(self, file_path: str, line_num: int, line_content: str):
        """记录非pytest日志违规"""
        try:
            violation_id = self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.MANUAL_LOG_ENTRY,
                severity=ViolationSeverity.HIGH,
                title=f"非pytest日志违规 - {os.path.basename(file_path)}",
                description=f"在文件 {file_path} 第 {line_num} 行发现非pytest日志: {line_content}",
                source_component="check_non_pytest_logs.py",
                evidence={
                    'file_path': file_path,
                    'line_number': line_num,
                    'line_content': line_content,
                    'violation_type': 'non_pytest_log_statement',
                    'detected_by': 'automated_checker'
                }
            )
            
            self.violation_count += 1
            self.non_pytest_logs.append({
                'violation_id': violation_id,
                'file_path': file_path,
                'line_number': line_num,
                'content': line_content
            })
            
            logger.warning(f"⚠️ 发现非pytest日志违规: {file_path}:{line_num}")
            
        except Exception as e:
            logger.error(f"记录非pytest违规失败: {e}")
    
    def _log_non_pytest_log_file_violation(self, file_path: str, line_num: int, line_content: str):
        """记录日志文件中的非pytest违规"""
        try:
            violation_id = self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.MANUAL_LOG_ENTRY,
                severity=ViolationSeverity.MEDIUM,
                title=f"日志文件非pytest违规 - {os.path.basename(file_path)}",
                description=f"日志文件 {file_path} 第 {line_num} 行包含非pytest生成的日志",
                source_component="check_non_pytest_logs.py",
                evidence={
                    'log_file_path': file_path,
                    'line_number': line_num,
                    'line_content': line_content,
                    'violation_type': 'non_pytest_log_file',
                    'detected_by': 'automated_checker'
                }
            )
            
            self.violation_count += 1
            
            logger.warning(f"⚠️ 发现日志文件非pytest违规: {file_path}:{line_num}")
            
        except Exception as e:
            logger.error(f"记录日志文件违规失败: {e}")
    
    def _log_database_non_pytest_violation(self, log_record: Tuple):
        """记录数据库中的非pytest违规"""
        try:
            log_id, operation_type, operation_details, operator, pytest_context = log_record
            
            violation_id = self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.NON_PYTEST_EXECUTION,
                severity=ViolationSeverity.HIGH,
                title=f"数据库非pytest日志违规 - {operation_type}",
                description=f"数据库审计日志ID {log_id} 未在pytest上下文中生成",
                source_component="check_non_pytest_logs.py",
                evidence={
                    'audit_log_id': log_id,
                    'operation_type': operation_type,
                    'operator': operator,
                    'pytest_context': pytest_context,
                    'violation_type': 'database_non_pytest_log',
                    'detected_by': 'automated_checker'
                }
            )
            
            self.violation_count += 1
            
            logger.warning(f"⚠️ 发现数据库非pytest日志违规: 审计日志ID {log_id}")
            
        except Exception as e:
            logger.error(f"记录数据库违规失败: {e}")
    
    def _log_database_violation_non_pytest(self, violation_record: Tuple):
        """记录数据库中未经pytest验证的违规记录"""
        try:
            violation_id, title, source_component, pytest_validated = violation_record
            
            new_violation_id = self.compliance_logger.log_contract_violation(
                violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
                severity=ViolationSeverity.MEDIUM,
                title=f"未经pytest验证的违规记录 - {title}",
                description=f"违规记录 {violation_id} 未经pytest验证",
                source_component="check_non_pytest_logs.py",
                evidence={
                    'original_violation_id': violation_id,
                    'original_title': title,
                    'original_source': source_component,
                    'pytest_validated': pytest_validated,
                    'violation_type': 'unvalidated_violation_record',
                    'detected_by': 'automated_checker'
                }
            )
            
            self.violation_count += 1
            
            logger.warning(f"⚠️ 发现未经pytest验证的违规记录: {violation_id}")
            
        except Exception as e:
            logger.error(f"记录未验证违规失败: {e}")
    
    def _generate_check_report(self) -> Dict[str, Any]:
        """生成检查报告"""
        try:
            report = {
                'check_timestamp': datetime.now(timezone.utc).isoformat(),
                'total_files_checked': len(self.checked_files),
                'total_violations_found': self.violation_count,
                'non_pytest_logs_count': len(self.non_pytest_logs),
                'checked_files': self.checked_files,
                'non_pytest_logs': self.non_pytest_logs,
                'compliance_status': 'NON_COMPLIANT' if self.violation_count > 0 else 'COMPLIANT',
                'recommendations': [
                    "所有日志必须通过pytest自动化生成",
                    "移除或修改所有手动日志语句",
                    "确保所有测试通过pytest执行",
                    "使用contract_compliance_logger进行合规日志记录"
                ]
            }
            
            # 记录检查报告
            self.compliance_logger._log_audit_operation(
                operation_type="NON_PYTEST_LOG_CHECK_COMPLETE",
                operation_details=f"非pytest日志检查完成 - 发现 {self.violation_count} 个违规",
                operator="AUTOMATED_CHECKER",
                pytest_context=False  # 这是检查工具，不在pytest上下文中
            )
            
            return report
            
        except Exception as e:
            logger.error(f"生成检查报告失败: {e}")
            raise


def main():
    """主函数 - 执行非pytest日志检查"""
    try:
        print("🔍 开始检查所有非pytest日志...")
        
        checker = NonPytestLogChecker()
        report = checker.check_all_logs()
        
        # 输出报告
        print(f"\\n📊 检查报告:")
        print(f"检查文件数: {report['total_files_checked']}")
        print(f"发现违规数: {report['total_violations_found']}")
        print(f"合规状态: {report['compliance_status']}")
        
        if report['total_violations_found'] > 0:
            print(f"\\n⚠️ 发现 {report['total_violations_found']} 个非pytest日志违规")
            print("建议:")
            for recommendation in report['recommendations']:
                print(f"  - {recommendation}")
        else:
            print("\\n✅ 所有日志均符合pytest要求")
        
        # 保存报告到文件
        report_file = 'non_pytest_log_check_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\\n📄 详细报告已保存到: {report_file}")
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)