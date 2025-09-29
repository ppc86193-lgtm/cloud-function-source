#!/usr/bin/env python3
"""
自动化测试日志审计脚本
用于检查每个提交是否符合自动化测试要求
"""

import os
import json
import logging
import re
from datetime import datetime
from pathlib import Path
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('audit_report.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TestLogAuditor:
    """测试日志审计器"""
    
    def __init__(self, log_path='logs/result.log'):
        self.log_path = Path(log_path)
        self.audit_results = {
            'timestamp': datetime.now().isoformat(),
            'log_path': str(log_path),
            'status': 'pending',
            'details': {}
        }
    
    def check_test_log_exists(self):
        """检查测试日志文件是否存在"""
        if self.log_path.exists():
            logger.info(f"✅ 找到日志文件：{self.log_path}")
            self.audit_results['details']['log_exists'] = True
            return True
        else:
            logger.error(f"❌ 未找到日志文件：{self.log_path}")
            self.audit_results['details']['log_exists'] = False
            self.audit_results['status'] = 'failed'
            return False
    
    def check_test_results(self):
        """检查测试结果"""
        if not self.check_test_log_exists():
            return False
        
        try:
            with open(self.log_path, 'r', encoding='utf-8') as log_file:
                content = log_file.read()
                
                # 检查是否有失败的测试
                if 'FAILED' in content or 'failed' in content:
                    logger.warning("⚠️ 测试中存在失败的用例")
                    self.audit_results['details']['has_failures'] = True
                    
                    # 提取失败信息
                    failed_lines = [line for line in content.split('\n') 
                                  if 'FAILED' in line or 'failed' in line]
                    self.audit_results['details']['failed_tests'] = failed_lines[:10]  # 最多记录10个失败
                    
                # 检查是否有通过的测试
                if 'passed' in content or 'PASSED' in content:
                    logger.info("✅ 存在通过的测试用例")
                    self.audit_results['details']['has_passed'] = True
                    
                    # 统计通过的测试数量
                    import re
                    passed_match = re.search(r'(\d+)\s+passed', content)
                    if passed_match:
                        passed_count = int(passed_match.group(1))
                        self.audit_results['details']['passed_count'] = passed_count
                        logger.info(f"📊 通过的测试数量：{passed_count}")
                
                # 检查是否有跳过的测试
                if 'skipped' in content or 'SKIPPED' in content:
                    logger.info("ℹ️ 存在跳过的测试用例")
                    self.audit_results['details']['has_skipped'] = True
                
                # 检查测试覆盖率（如果存在）
                coverage_match = re.search(r'TOTAL.*?(\d+)%', content)
                if coverage_match:
                    coverage = int(coverage_match.group(1))
                    self.audit_results['details']['coverage'] = coverage
                    logger.info(f"📊 测试覆盖率：{coverage}%")
                    
                    if coverage < 60:
                        logger.warning(f"⚠️ 测试覆盖率较低：{coverage}%")
                    elif coverage >= 80:
                        logger.info(f"✅ 测试覆盖率良好：{coverage}%")
                
                # 检查日志完整性
                if '=' * 10 in content or '-' * 10 in content:
                    logger.info("✅ 日志格式完整")
                    self.audit_results['details']['log_format_valid'] = True
                else:
                    logger.warning("⚠️ 日志格式可能不完整")
                    self.audit_results['details']['log_format_valid'] = False
                
                # 判断整体状态
                if self.audit_results['details'].get('has_failures'):
                    self.audit_results['status'] = 'failed'
                    logger.error("❌ 审计失败：存在失败的测试用例")
                    return False
                elif self.audit_results['details'].get('has_passed'):
                    self.audit_results['status'] = 'passed'
                    logger.info("✅ 审计通过：所有测试用例通过")
                    return True
                else:
                    self.audit_results['status'] = 'unknown'
                    logger.warning("⚠️ 无法确定测试状态")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ 读取日志文件时出错：{e}")
            self.audit_results['status'] = 'error'
            self.audit_results['details']['error'] = str(e)
            return False
    
    def check_test_directory(self):
        """检查测试目录结构"""
        tests_dir = Path('tests')
        if not tests_dir.exists():
            logger.warning("⚠️ 未找到tests目录")
            self.audit_results['details']['tests_dir_exists'] = False
            return False
        
        # 检查测试文件
        test_files = list(tests_dir.glob('**/test_*.py'))
        if test_files:
            logger.info(f"✅ 找到 {len(test_files)} 个测试文件")
            self.audit_results['details']['test_files_count'] = len(test_files)
            self.audit_results['details']['test_files'] = [str(f.relative_to('.')) for f in test_files]
            return True
        else:
            logger.warning("⚠️ 未找到测试文件（test_*.py）")
            self.audit_results['details']['test_files_count'] = 0
            return False
    
    def check_pytest_config(self):
        """检查pytest配置"""
        config_files = ['pytest.ini', 'setup.cfg', 'pyproject.toml']
        found_configs = []
        
        for config_file in config_files:
            if Path(config_file).exists():
                found_configs.append(config_file)
                logger.info(f"✅ 找到pytest配置文件：{config_file}")
        
        if found_configs:
            self.audit_results['details']['pytest_configs'] = found_configs
            return True
        else:
            logger.warning("⚠️ 未找到pytest配置文件")
            self.audit_results['details']['pytest_configs'] = []
            return False
    
    def generate_audit_report(self):
        """生成审计报告"""
        report_path = Path('audit_report.json')
        
        try:
            with open(report_path, 'w', encoding='utf-8') as report_file:
                json.dump(self.audit_results, report_file, indent=2, ensure_ascii=False)
            logger.info(f"📝 审计报告已保存到：{report_path}")
            return True
        except Exception as e:
            logger.error(f"❌ 保存审计报告时出错：{e}")
            return False
    
    def run_full_audit(self):
        """运行完整的审计流程"""
        logger.info("="*60)
        logger.info("开始自动化测试日志审计")
        logger.info("="*60)
        
        # 执行各项检查
        self.check_test_directory()
        self.check_pytest_config()
        test_passed = self.check_test_results()
        
        # 生成报告
        self.generate_audit_report()
        
        # 输出总结
        logger.info("="*60)
        logger.info("审计总结")
        logger.info("="*60)
        
        if self.audit_results['status'] == 'passed':
            logger.info("✅ 审计通过：满足所有自动化测试要求")
            logger.info("提交可以继续")
            return True
        elif self.audit_results['status'] == 'failed':
            logger.error("❌ 审计失败：不满足自动化测试要求")
            logger.error("请修复失败的测试后重新提交")
            
            # 输出失败的测试信息
            if 'failed_tests' in self.audit_results['details']:
                logger.error("失败的测试：")
                for failed_test in self.audit_results['details']['failed_tests'][:5]:
                    logger.error(f"  - {failed_test}")
            
            return False
        else:
            logger.warning("⚠️ 审计状态未知，请检查日志文件")
            return False

def main():
    """主函数"""
    # 支持自定义日志路径
    log_path = sys.argv[1] if len(sys.argv) > 1 else 'logs/result.log'
    
    auditor = TestLogAuditor(log_path)
    success = auditor.run_full_audit()
    
    # 返回适当的退出码
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()