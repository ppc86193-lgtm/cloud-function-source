#!/usr/bin/env python3
"""
PC28系统测试运行器
提供多种测试运行模式和报告生成功能
"""

import os
import sys
import argparse
import subprocess
import shutil
from pathlib import Path
from datetime import datetime

class TestRunner:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.reports_dir = self.project_root / "reports"
        self.coverage_dir = self.project_root / "htmlcov"
        
    def setup_directories(self):
        """创建必要的目录"""
        self.reports_dir.mkdir(exist_ok=True)
        self.coverage_dir.mkdir(exist_ok=True)
        
    def run_command(self, cmd, description=""):
        """运行命令并处理输出"""
        print(f"\n{'='*60}")
        print(f"执行: {description or ' '.join(cmd)}")
        print(f"{'='*60}")
        
        try:
            result = subprocess.run(cmd, cwd=self.project_root, check=True, 
                                  capture_output=False, text=True)
            return result.returncode == 0
        except subprocess.CalledProcessError as e:
            print(f"命令执行失败: {e}")
            return False
        except FileNotFoundError:
            print(f"命令未找到: {cmd[0]}")
            return False
    
    def run_unit_tests(self):
        """运行单元测试"""
        cmd = [
            "python", "-m", "pytest",
            "-m", "unit",
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, "运行单元测试")
    
    def run_integration_tests(self):
        """运行集成测试"""
        cmd = [
            "python", "-m", "pytest", 
            "-m", "integration",
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, "运行集成测试")
    
    def run_all_tests(self):
        """运行所有测试"""
        cmd = [
            "python", "-m", "pytest",
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, "运行所有测试")
    
    def run_with_coverage(self):
        """运行测试并生成覆盖率报告"""
        cmd = [
            "python", "-m", "pytest",
            "--cov=.",
            "--cov-report=html",
            "--cov-report=xml",
            "--cov-report=term-missing",
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, "运行测试并生成覆盖率报告")
    
    def run_parallel_tests(self, num_workers="auto"):
        """并行运行测试"""
        cmd = [
            "python", "-m", "pytest",
            "-n", str(num_workers),
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, f"并行运行测试 (workers: {num_workers})")
    
    def run_specific_test(self, test_path):
        """运行特定测试"""
        cmd = [
            "python", "-m", "pytest",
            test_path,
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, f"运行特定测试: {test_path}")
    
    def run_slow_tests(self):
        """运行慢速测试"""
        cmd = [
            "python", "-m", "pytest",
            "-m", "slow",
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, "运行慢速测试")
    
    def run_fast_tests(self):
        """运行快速测试"""
        cmd = [
            "python", "-m", "pytest",
            "-m", "not slow",
            "--tb=short",
            "-v"
        ]
        return self.run_command(cmd, "运行快速测试")
    
    def generate_html_report(self):
        """生成HTML测试报告"""
        cmd = [
            "python", "-m", "pytest",
            "--html=reports/pytest_report.html",
            "--self-contained-html",
            "--tb=short"
        ]
        return self.run_command(cmd, "生成HTML测试报告")
    
    def clean_reports(self):
        """清理旧的报告文件"""
        print("清理旧的报告文件...")
        
        # 清理覆盖率报告
        if self.coverage_dir.exists():
            shutil.rmtree(self.coverage_dir)
            
        # 清理测试报告
        if self.reports_dir.exists():
            shutil.rmtree(self.reports_dir)
            
        # 清理其他文件
        for file in self.project_root.glob("coverage.*"):
            file.unlink(missing_ok=True)
            
        for file in self.project_root.glob(".coverage*"):
            file.unlink(missing_ok=True)
            
        print("清理完成")
    
    def show_coverage_summary(self):
        """显示覆盖率摘要"""
        coverage_file = self.project_root / "coverage.xml"
        if coverage_file.exists():
            print(f"\n覆盖率报告已生成: {coverage_file}")
            
        html_index = self.coverage_dir / "index.html"
        if html_index.exists():
            print(f"HTML覆盖率报告: {html_index}")
    
    def print_summary(self, results):
        """打印测试结果摘要"""
        print(f"\n{'='*60}")
        print("测试结果摘要")
        print(f"{'='*60}")
        
        total_tests = len(results)
        passed_tests = sum(1 for result in results if result[1])
        failed_tests = total_tests - passed_tests
        
        print(f"总测试数: {total_tests}")
        print(f"通过: {passed_tests}")
        print(f"失败: {failed_tests}")
        print(f"成功率: {passed_tests/total_tests*100:.1f}%" if total_tests > 0 else "无测试")
        
        if failed_tests > 0:
            print("\n失败的测试:")
            for name, success in results:
                if not success:
                    print(f"  ❌ {name}")
        
        print(f"\n报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    parser = argparse.ArgumentParser(description="PC28系统测试运行器")
    parser.add_argument("--unit", action="store_true", help="只运行单元测试")
    parser.add_argument("--integration", action="store_true", help="只运行集成测试")
    parser.add_argument("--coverage", action="store_true", help="生成覆盖率报告")
    parser.add_argument("--parallel", type=str, default="auto", help="并行测试 (默认: auto)")
    parser.add_argument("--slow", action="store_true", help="运行慢速测试")
    parser.add_argument("--fast", action="store_true", help="运行快速测试")
    parser.add_argument("--html", action="store_true", help="生成HTML报告")
    parser.add_argument("--clean", action="store_true", help="清理旧报告")
    parser.add_argument("--test", type=str, help="运行特定测试文件或目录")
    parser.add_argument("--all", action="store_true", help="运行所有测试")
    
    args = parser.parse_args()
    
    runner = TestRunner()
    runner.setup_directories()
    
    if args.clean:
        runner.clean_reports()
        return
    
    results = []
    
    try:
        if args.unit:
            results.append(("单元测试", runner.run_unit_tests()))
        elif args.integration:
            results.append(("集成测试", runner.run_integration_tests()))
        elif args.slow:
            results.append(("慢速测试", runner.run_slow_tests()))
        elif args.fast:
            results.append(("快速测试", runner.run_fast_tests()))
        elif args.test:
            results.append((f"特定测试: {args.test}", runner.run_specific_test(args.test)))
        elif args.coverage:
            results.append(("覆盖率测试", runner.run_with_coverage()))
            runner.show_coverage_summary()
        elif args.parallel != "auto" or len(sys.argv) > 1 and "--parallel" in sys.argv:
            results.append((f"并行测试", runner.run_parallel_tests(args.parallel)))
        elif args.html:
            results.append(("HTML报告测试", runner.generate_html_report()))
        elif args.all:
            results.append(("所有测试", runner.run_all_tests()))
        else:
            # 默认运行快速测试
            results.append(("快速测试", runner.run_fast_tests()))
            
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        sys.exit(1)
    
    if results:
        runner.print_summary(results)
        
        # 如果有失败的测试，返回非零退出码
        if any(not success for _, success in results):
            sys.exit(1)

if __name__ == "__main__":
    main()