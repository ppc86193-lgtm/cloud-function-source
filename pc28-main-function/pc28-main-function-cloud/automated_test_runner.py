#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统自动化测试执行器
自动运行pytest并捕获完整的执行日志，确保日志的证据能力
"""

import subprocess
import json
import time
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

class AutomatedTestRunner:
    """自动化测试运行器"""
    
    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir = self.project_root / "test_evidence" / f"run_{self.timestamp}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置日志
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志系统"""
        log_file = self.output_dir / "test_execution.log"
        
        # 创建logger
        self.logger = logging.getLogger('AutomatedTestRunner')
        self.logger.setLevel(logging.INFO)
        
        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 创建格式器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
    def run_pytest_with_full_logging(self, test_paths: List[str] = None, 
                                   markers: List[str] = None,
                                   verbose: bool = True) -> Dict[str, Any]:
        """
        运行pytest并捕获完整日志
        
        Args:
            test_paths: 测试路径列表
            markers: 测试标记列表
            verbose: 是否详细输出
            
        Returns:
            测试执行结果字典
        """
        self.logger.info("="*80)
        self.logger.info(f"开始自动化测试执行 - {self.timestamp}")
        self.logger.info("="*80)
        
        # 构建pytest命令
        cmd = self._build_pytest_command(test_paths, markers, verbose)
        
        self.logger.info(f"执行命令: {' '.join(cmd)}")
        
        # 记录环境信息
        self._log_environment_info()
        
        # 执行测试
        start_time = time.time()
        result = self._execute_pytest(cmd)
        execution_time = time.time() - start_time
        
        # 处理结果
        test_result = self._process_test_results(result, execution_time)
        
        # 保存结果
        self._save_test_evidence(test_result)
        
        self.logger.info("="*80)
        self.logger.info(f"测试执行完成 - 总耗时: {execution_time:.2f}秒")
        self.logger.info("="*80)
        
        return test_result
        
    def _build_pytest_command(self, test_paths: List[str] = None,
                            markers: List[str] = None,
                            verbose: bool = True) -> List[str]:
        """构建pytest命令"""
        cmd = [
            sys.executable, "-m", "pytest",
            "--tb=long",  # 详细的错误回溯
            "--capture=no",  # 不捕获输出，显示所有print
            "--durations=10",  # 显示最慢的10个测试
            "--strict-markers",  # 严格标记模式
            f"--junitxml={self.output_dir}/junit_report.xml",  # JUnit XML报告
            f"--html={self.output_dir}/html_report.html",  # HTML报告
            "--self-contained-html",  # 自包含HTML
            f"--json-report",  # JSON报告
            f"--json-report-file={self.output_dir}/json_report.json"
        ]
        
        if verbose:
            cmd.append("-v")
            
        # 添加测试路径
        if test_paths:
            cmd.extend(test_paths)
        else:
            # 默认测试路径
            if (self.project_root / "tests").exists():
                cmd.append("tests/")
            else:
                cmd.append(".")
                
        # 添加标记过滤
        if markers:
            cmd.extend(["-m", " or ".join(markers)])
            
        return cmd
        
    def _log_environment_info(self):
        """记录环境信息"""
        self.logger.info("环境信息:")
        self.logger.info(f"  Python版本: {sys.version}")
        self.logger.info(f"  工作目录: {os.getcwd()}")
        self.logger.info(f"  项目根目录: {self.project_root}")
        self.logger.info(f"  输出目录: {self.output_dir}")
        
        # 记录已安装的包
        try:
            import pkg_resources
            installed_packages = [f"{d.project_name}=={d.version}" 
                                for d in pkg_resources.working_set]
            self.logger.info(f"  已安装包数量: {len(installed_packages)}")
            
            # 保存包列表到文件
            with open(self.output_dir / "installed_packages.txt", "w") as f:
                f.write("\n".join(sorted(installed_packages)))
                
        except Exception as e:
            self.logger.warning(f"无法获取包信息: {e}")
            
    def _execute_pytest(self, cmd: List[str]) -> subprocess.CompletedProcess:
        """执行pytest命令"""
        self.logger.info("开始执行pytest...")
        
        try:
            # 执行命令并实时捕获输出
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                cwd=self.project_root
            )
            
            # 实时记录输出
            output_lines = []
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    line = output.strip()
                    output_lines.append(line)
                    self.logger.info(f"PYTEST: {line}")
                    
            # 等待进程完成
            return_code = process.poll()
            
            # 创建结果对象
            result = subprocess.CompletedProcess(
                cmd, return_code, 
                stdout="\n".join(output_lines),
                stderr=""
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"执行pytest失败: {e}")
            raise
            
    def _process_test_results(self, result: subprocess.CompletedProcess, 
                            execution_time: float) -> Dict[str, Any]:
        """处理测试结果"""
        self.logger.info("处理测试结果...")
        
        test_result = {
            "execution_info": {
                "timestamp": self.timestamp,
                "execution_time": execution_time,
                "return_code": result.returncode,
                "command": " ".join(result.args),
                "success": result.returncode == 0
            },
            "output": {
                "stdout": result.stdout,
                "stderr": result.stderr or ""
            },
            "files_generated": [],
            "summary": {}
        }
        
        # 检查生成的文件
        for file_path in self.output_dir.glob("*"):
            if file_path.is_file():
                test_result["files_generated"].append({
                    "name": file_path.name,
                    "size": file_path.stat().st_size,
                    "path": str(file_path)
                })
                
        # 尝试解析JSON报告
        json_report_path = self.output_dir / "json_report.json"
        if json_report_path.exists():
            try:
                with open(json_report_path, 'r', encoding='utf-8') as f:
                    json_report = json.load(f)
                    test_result["summary"] = {
                        "total": json_report.get("summary", {}).get("total", 0),
                        "passed": json_report.get("summary", {}).get("passed", 0),
                        "failed": json_report.get("summary", {}).get("failed", 0),
                        "skipped": json_report.get("summary", {}).get("skipped", 0),
                        "error": json_report.get("summary", {}).get("error", 0)
                    }
            except Exception as e:
                self.logger.warning(f"无法解析JSON报告: {e}")
                
        # 记录结果摘要
        self.logger.info(f"测试执行结果:")
        self.logger.info(f"  返回码: {result.returncode}")
        self.logger.info(f"  执行时间: {execution_time:.2f}秒")
        self.logger.info(f"  成功: {'是' if result.returncode == 0 else '否'}")
        
        if test_result["summary"]:
            summary = test_result["summary"]
            self.logger.info(f"  测试统计:")
            self.logger.info(f"    总计: {summary.get('total', 0)}")
            self.logger.info(f"    通过: {summary.get('passed', 0)}")
            self.logger.info(f"    失败: {summary.get('failed', 0)}")
            self.logger.info(f"    跳过: {summary.get('skipped', 0)}")
            self.logger.info(f"    错误: {summary.get('error', 0)}")
            
        return test_result
        
    def _save_test_evidence(self, test_result: Dict[str, Any]):
        """保存测试证据"""
        self.logger.info("保存测试证据...")
        
        # 保存完整的测试结果
        evidence_file = self.output_dir / "test_evidence.json"
        with open(evidence_file, 'w', encoding='utf-8') as f:
            json.dump(test_result, f, ensure_ascii=False, indent=2)
            
        # 创建证据摘要
        summary_file = self.output_dir / "evidence_summary.md"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(f"# 测试执行证据摘要\n\n")
            f.write(f"**执行时间**: {test_result['execution_info']['timestamp']}\n")
            f.write(f"**执行耗时**: {test_result['execution_info']['execution_time']:.2f}秒\n")
            f.write(f"**执行命令**: `{test_result['execution_info']['command']}`\n")
            f.write(f"**返回码**: {test_result['execution_info']['return_code']}\n")
            f.write(f"**执行状态**: {'成功' if test_result['execution_info']['success'] else '失败'}\n\n")
            
            if test_result["summary"]:
                f.write("## 测试统计\n\n")
                summary = test_result["summary"]
                f.write(f"- 总测试数: {summary.get('total', 0)}\n")
                f.write(f"- 通过: {summary.get('passed', 0)}\n")
                f.write(f"- 失败: {summary.get('failed', 0)}\n")
                f.write(f"- 跳过: {summary.get('skipped', 0)}\n")
                f.write(f"- 错误: {summary.get('error', 0)}\n\n")
                
            f.write("## 生成的文件\n\n")
            for file_info in test_result["files_generated"]:
                f.write(f"- {file_info['name']} ({file_info['size']} bytes)\n")
                
        self.logger.info(f"测试证据已保存到: {self.output_dir}")
        
    def run_specific_test_suites(self) -> Dict[str, Any]:
        """运行特定的测试套件"""
        all_results = {}
        
        # 定义测试套件
        test_suites = [
            {
                "name": "unit_tests",
                "markers": ["unit"],
                "description": "单元测试"
            },
            {
                "name": "integration_tests", 
                "markers": ["integration"],
                "description": "集成测试"
            },
            {
                "name": "performance_tests",
                "markers": ["performance"],
                "description": "性能测试"
            },
            {
                "name": "realtime_tests",
                "markers": ["realtime"],
                "description": "实时系统测试"
            },
            {
                "name": "consistency_tests",
                "markers": ["consistency"],
                "description": "数据一致性测试"
            }
        ]
        
        for suite in test_suites:
            self.logger.info(f"执行测试套件: {suite['description']}")
            
            try:
                result = self.run_pytest_with_full_logging(
                    markers=suite["markers"]
                )
                all_results[suite["name"]] = result
                
            except Exception as e:
                self.logger.error(f"测试套件 {suite['name']} 执行失败: {e}")
                all_results[suite["name"]] = {
                    "error": str(e),
                    "success": False
                }
                
        return all_results

def main():
    """主函数"""
    print("PC28系统自动化测试执行器")
    print("="*50)
    
    # 创建测试运行器
    runner = AutomatedTestRunner()
    
    try:
        # 运行所有测试
        print("开始执行完整测试套件...")
        results = runner.run_pytest_with_full_logging()
        
        if results["execution_info"]["success"]:
            print("✅ 所有测试执行成功")
        else:
            print("❌ 测试执行中发现问题")
            
        print(f"📁 测试证据保存在: {runner.output_dir}")
        
        # 显示摘要
        if results.get("summary"):
            summary = results["summary"]
            print(f"\n📊 测试摘要:")
            print(f"   总计: {summary.get('total', 0)}")
            print(f"   通过: {summary.get('passed', 0)}")
            print(f"   失败: {summary.get('failed', 0)}")
            print(f"   跳过: {summary.get('skipped', 0)}")
            
    except Exception as e:
        print(f"❌ 测试执行失败: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())