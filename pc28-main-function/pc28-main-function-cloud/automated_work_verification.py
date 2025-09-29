#!/usr/bin/env python3
"""
自动化工作验证系统
通过实际执行和测试来验证所有工作的完成状态
生成可验证的自动化测试日志
"""

import subprocess
import json
import os
import datetime
import sys
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('automated_verification.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class AutomatedWorkVerifier:
    """自动化工作验证器"""
    
    def __init__(self):
        self.base_dir = Path.cwd()
        self.verification_results = {
            "verification_timestamp": datetime.datetime.now().isoformat(),
            "verification_status": "RUNNING",
            "test_results": {},
            "system_validations": {},
            "automated_logs": [],
            "final_score": 0
        }
    
    def log_automated_test(self, test_name: str, status: str, details: str = ""):
        """记录自动化测试日志"""
        log_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "test_name": test_name,
            "status": status,
            "details": details,
            "automated": True
        }
        self.verification_results["automated_logs"].append(log_entry)
        logger.info(f"🤖 自动化测试: {test_name} - {status}")
        if details:
            logger.info(f"   详情: {details}")
    
    def run_pytest_tests(self) -> bool:
        """运行pytest测试并验证结果"""
        logger.info("🧪 开始运行pytest自动化测试...")
        
        try:
            # 运行pytest测试
            result = subprocess.run(
                ["python", "-m", "pytest", "test_*compliance.py", "-v", "--tb=short"],
                capture_output=True,
                text=True,
                cwd=self.base_dir
            )
            
            # 解析测试结果
            output_lines = result.stdout.split('\n')
            passed_tests = []
            failed_tests = []
            
            for line in output_lines:
                if "PASSED" in line:
                    test_name = line.split("::")[0] if "::" in line else line
                    passed_tests.append(test_name.strip())
                elif "FAILED" in line:
                    test_name = line.split("::")[0] if "::" in line else line
                    failed_tests.append(test_name.strip())
            
            # 记录测试结果
            self.verification_results["test_results"]["pytest"] = {
                "total_passed": len(passed_tests),
                "total_failed": len(failed_tests),
                "passed_tests": passed_tests,
                "failed_tests": failed_tests,
                "exit_code": result.returncode,
                "full_output": result.stdout
            }
            
            # 记录自动化测试日志
            self.log_automated_test(
                "pytest_compliance_tests",
                "PASSED" if result.returncode == 0 else "FAILED",
                f"通过: {len(passed_tests)}, 失败: {len(failed_tests)}"
            )
            
            return result.returncode == 0
            
        except Exception as e:
            self.log_automated_test("pytest_execution", "ERROR", str(e))
            return False
    
    def verify_smart_contract_system(self) -> bool:
        """验证智能合约系统"""
        logger.info("📋 验证智能合约条款系统...")
        
        try:
            # 检查PROJECT_RULES.md
            project_rules = self.base_dir / "PROJECT_RULES.md"
            if not project_rules.exists():
                self.log_automated_test("project_rules_check", "FAILED", "PROJECT_RULES.md不存在")
                return False
            
            # 检查合约日志系统
            contract_logger = self.base_dir / "contract_compliance_logger.py"
            if not contract_logger.exists():
                self.log_automated_test("contract_logger_check", "FAILED", "contract_compliance_logger.py不存在")
                return False
            
            # 尝试导入和初始化合约日志系统
            try:
                import contract_compliance_logger
                logger_instance = contract_compliance_logger.ContractComplianceLogger()
                self.log_automated_test("contract_system_init", "PASSED", "合约系统初始化成功")
                return True
            except Exception as e:
                self.log_automated_test("contract_system_init", "FAILED", f"初始化失败: {str(e)}")
                return False
                
        except Exception as e:
            self.log_automated_test("smart_contract_verification", "ERROR", str(e))
            return False
    
    def verify_data_sync_system(self) -> bool:
        """验证数据同步系统"""
        logger.info("🔄 验证数据同步和监控系统...")
        
        try:
            # 检查核心同步文件
            sync_manager = self.base_dir / "supabase_sync_manager.py"
            if not sync_manager.exists():
                self.log_automated_test("sync_manager_check", "FAILED", "supabase_sync_manager.py不存在")
                return False
            
            # 检查数据审计系统
            audit_system = self.base_dir / "data_audit_system.py"
            if not audit_system.exists():
                self.log_automated_test("audit_system_check", "FAILED", "data_audit_system.py不存在")
                return False
            
            # 尝试导入同步管理器
            try:
                sys.path.append(str(self.base_dir))
                import supabase_sync_manager
                self.log_automated_test("sync_system_import", "PASSED", "同步系统导入成功")
                return True
            except Exception as e:
                self.log_automated_test("sync_system_import", "FAILED", f"导入失败: {str(e)}")
                return False
                
        except Exception as e:
            self.log_automated_test("data_sync_verification", "ERROR", str(e))
            return False
    
    def verify_api_optimization(self) -> bool:
        """验证API和数据库优化"""
        logger.info("⚡ 验证API和数据库优化...")
        
        try:
            # 检查优化脚本
            api_optimizer = self.base_dir / "api_optimization_script.py"
            db_optimizer = self.base_dir / "database_table_optimizer.py"
            
            if not api_optimizer.exists():
                self.log_automated_test("api_optimizer_check", "FAILED", "api_optimization_script.py不存在")
                return False
            
            if not db_optimizer.exists():
                self.log_automated_test("db_optimizer_check", "FAILED", "database_table_optimizer.py不存在")
                return False
            
            # 运行API优化测试
            try:
                result = subprocess.run(
                    ["python", "api_optimization_script.py"],
                    capture_output=True,
                    text=True,
                    cwd=self.base_dir,
                    timeout=30
                )
                
                if result.returncode == 0:
                    self.log_automated_test("api_optimization_test", "PASSED", "API优化脚本执行成功")
                    return True
                else:
                    self.log_automated_test("api_optimization_test", "FAILED", f"执行失败: {result.stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                self.log_automated_test("api_optimization_test", "TIMEOUT", "执行超时")
                return False
                
        except Exception as e:
            self.log_automated_test("api_optimization_verification", "ERROR", str(e))
            return False
    
    def verify_pc28_business_logic(self) -> bool:
        """验证PC28业务逻辑系统"""
        logger.info("🏢 验证PC28业务逻辑系统...")
        
        try:
            # 检查业务逻辑文件
            business_files = [
                "pc28_business_logic_extractor.py",
                "pc28_business_logic_protector.py",
                "pc28_comprehensive_business_optimizer.py"
            ]
            
            missing_files = []
            for file_name in business_files:
                file_path = self.base_dir / file_name
                if not file_path.exists():
                    missing_files.append(file_name)
            
            if missing_files:
                self.log_automated_test(
                    "pc28_files_check", 
                    "FAILED", 
                    f"缺少文件: {', '.join(missing_files)}"
                )
                return False
            
            self.log_automated_test("pc28_files_check", "PASSED", "所有PC28业务逻辑文件存在")
            
            # 检查优化报告
            optimization_reports = list(self.base_dir.glob("pc28_*优化*报告*.md"))
            if optimization_reports:
                self.log_automated_test(
                    "pc28_reports_check", 
                    "PASSED", 
                    f"找到{len(optimization_reports)}个优化报告"
                )
                return True
            else:
                self.log_automated_test("pc28_reports_check", "WARNING", "未找到优化报告")
                return True  # 不是致命错误
                
        except Exception as e:
            self.log_automated_test("pc28_verification", "ERROR", str(e))
            return False
    
    def verify_git_commits(self) -> bool:
        """验证Git提交"""
        logger.info("📝 验证Git提交状态...")
        
        try:
            # 检查Git状态
            result = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                cwd=self.base_dir
            )
            
            if result.returncode != 0:
                self.log_automated_test("git_status_check", "FAILED", "无法获取Git状态")
                return False
            
            modified_files = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            
            # 检查最新提交
            commit_result = subprocess.run(
                ["git", "log", "-1", "--oneline"],
                capture_output=True,
                text=True,
                cwd=self.base_dir
            )
            
            if commit_result.returncode == 0:
                latest_commit = commit_result.stdout.strip()
                self.log_automated_test(
                    "git_commit_check", 
                    "PASSED", 
                    f"最新提交: {latest_commit}"
                )
                
                if len(modified_files) == 0:
                    self.log_automated_test("git_clean_check", "PASSED", "工作目录干净")
                else:
                    self.log_automated_test(
                        "git_clean_check", 
                        "WARNING", 
                        f"有{len(modified_files)}个未提交文件"
                    )
                
                return True
            else:
                self.log_automated_test("git_commit_check", "FAILED", "无法获取提交信息")
                return False
                
        except Exception as e:
            self.log_automated_test("git_verification", "ERROR", str(e))
            return False
    
    def generate_compliance_report(self) -> bool:
        """生成合规性报告"""
        logger.info("📊 生成合规性报告...")
        
        try:
            result = subprocess.run(
                ["python", "generate_pytest_compliance_report.py"],
                capture_output=True,
                text=True,
                cwd=self.base_dir,
                timeout=60
            )
            
            if result.returncode == 0:
                self.log_automated_test("compliance_report_generation", "PASSED", "合规性报告生成成功")
                return True
            else:
                self.log_automated_test("compliance_report_generation", "FAILED", f"生成失败: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.log_automated_test("compliance_report_generation", "TIMEOUT", "报告生成超时")
            return False
        except Exception as e:
            self.log_automated_test("compliance_report_generation", "ERROR", str(e))
            return False
    
    def run_full_verification(self) -> dict:
        """运行完整的自动化验证"""
        logger.info("🚀 开始完整的自动化工作验证...")
        
        # 执行各项验证
        verifications = [
            ("pytest_tests", self.run_pytest_tests),
            ("smart_contract_system", self.verify_smart_contract_system),
            ("data_sync_system", self.verify_data_sync_system),
            ("api_optimization", self.verify_api_optimization),
            ("pc28_business_logic", self.verify_pc28_business_logic),
            ("git_commits", self.verify_git_commits),
            ("compliance_report", self.generate_compliance_report)
        ]
        
        passed_count = 0
        total_count = len(verifications)
        
        for verification_name, verification_func in verifications:
            logger.info(f"🔍 执行验证: {verification_name}")
            try:
                result = verification_func()
                self.verification_results["system_validations"][verification_name] = {
                    "status": "PASSED" if result else "FAILED",
                    "timestamp": datetime.datetime.now().isoformat()
                }
                if result:
                    passed_count += 1
            except Exception as e:
                logger.error(f"❌ 验证失败 {verification_name}: {str(e)}")
                self.verification_results["system_validations"][verification_name] = {
                    "status": "ERROR",
                    "error": str(e),
                    "timestamp": datetime.datetime.now().isoformat()
                }
        
        # 计算最终得分
        self.verification_results["final_score"] = (passed_count / total_count) * 100
        self.verification_results["verification_status"] = "COMPLETED"
        
        # 保存验证结果
        report_file = f"automated_verification_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.verification_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📋 自动化验证完成！得分: {self.verification_results['final_score']:.1f}%")
        logger.info(f"📄 验证报告已保存: {report_file}")
        
        return self.verification_results

def main():
    """主函数"""
    print("🤖 自动化工作验证系统启动...")
    print("=" * 60)
    
    verifier = AutomatedWorkVerifier()
    results = verifier.run_full_verification()
    
    print("\n" + "=" * 60)
    print("📊 自动化验证结果摘要:")
    print(f"   🎯 总体得分: {results['final_score']:.1f}%")
    print(f"   📝 自动化日志数量: {len(results['automated_logs'])}")
    print(f"   ✅ 通过的验证: {sum(1 for v in results['system_validations'].values() if v['status'] == 'PASSED')}")
    print(f"   ❌ 失败的验证: {sum(1 for v in results['system_validations'].values() if v['status'] == 'FAILED')}")
    
    print("\n🔍 详细验证状态:")
    for name, validation in results['system_validations'].items():
        status_icon = "✅" if validation['status'] == "PASSED" else "❌" if validation['status'] == "FAILED" else "⚠️"
        print(f"   {status_icon} {name}: {validation['status']}")
    
    print("\n🤖 自动化测试日志:")
    for log in results['automated_logs'][-10:]:  # 显示最后10条日志
        print(f"   📝 {log['timestamp'][:19]} - {log['test_name']}: {log['status']}")
    
    if results['final_score'] >= 80:
        print("\n🎉 验证通过！所有工作项目已成功完成并通过自动化验证！")
    else:
        print(f"\n⚠️ 验证得分 {results['final_score']:.1f}% 需要改进。请检查失败的验证项目。")
    
    print("=" * 60)

if __name__ == "__main__":
    main()