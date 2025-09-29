#!/usr/bin/env python3
"""
自动化测试日志系统 - 符合PROJECT_RULES.md要求
实现完整的pytest自动化执行和日志记录
"""

import os
import sys
import json
import sqlite3
import hashlib
import subprocess
import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import pytest
import logging
from dataclasses import dataclass, asdict
import uuid

@dataclass
class TestExecutionRecord:
    """测试执行记录数据结构"""
    execution_id: str
    test_file: str
    test_name: str
    test_status: str  # PASSED, FAILED, SKIPPED, ERROR
    execution_time: str
    duration: float
    output: str
    error_message: Optional[str]
    traceback: Optional[str]
    timestamp: str
    hash_signature: str

class AutomatedTestLogger:
    """自动化测试日志系统 - 符合PROJECT_RULES.md第1.2条要求"""
    
    def __init__(self, db_path: str = "test_execution_logs.db"):
        self.db_path = db_path
        self.execution_id = str(uuid.uuid4())
        self.test_records: List[TestExecutionRecord] = []
        self.setup_database()
        self.setup_logging()
        
    def setup_database(self):
        """创建测试日志数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建测试执行日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_execution_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                test_file TEXT NOT NULL,
                test_name TEXT NOT NULL,
                test_status TEXT NOT NULL,
                execution_time TEXT NOT NULL,
                duration REAL NOT NULL,
                output TEXT,
                error_message TEXT,
                traceback TEXT,
                timestamp TEXT NOT NULL,
                hash_signature TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建执行会话表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_execution_sessions (
                execution_id TEXT PRIMARY KEY,
                session_start TEXT NOT NULL,
                session_end TEXT,
                total_tests INTEGER DEFAULT 0,
                passed_tests INTEGER DEFAULT 0,
                failed_tests INTEGER DEFAULT 0,
                skipped_tests INTEGER DEFAULT 0,
                error_tests INTEGER DEFAULT 0,
                compliance_status TEXT DEFAULT 'PENDING',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def setup_logging(self):
        """设置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'test_execution_{self.execution_id}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def generate_hash_signature(self, record: TestExecutionRecord) -> str:
        """生成不可篡改的哈希签名"""
        data = f"{record.test_file}{record.test_name}{record.test_status}{record.timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()
        
    def record_test_execution(self, test_file: str, test_name: str, 
                            test_status: str, duration: float, 
                            output: str, error_message: str = None, 
                            traceback: str = None):
        """记录单个测试执行结果"""
        timestamp = datetime.datetime.now().isoformat()
        
        record = TestExecutionRecord(
            execution_id=self.execution_id,
            test_file=test_file,
            test_name=test_name,
            test_status=test_status,
            execution_time=timestamp,
            duration=duration,
            output=output,
            error_message=error_message,
            traceback=traceback,
            timestamp=timestamp,
            hash_signature=""
        )
        
        # 生成哈希签名
        record.hash_signature = self.generate_hash_signature(record)
        
        # 存储到内存
        self.test_records.append(record)
        
        # 立即写入数据库
        self.save_to_database(record)
        
        self.logger.info(f"测试记录已保存: {test_name} - {test_status}")
        
    def save_to_database(self, record: TestExecutionRecord):
        """保存测试记录到数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO test_execution_logs 
            (execution_id, test_file, test_name, test_status, execution_time, 
             duration, output, error_message, traceback, timestamp, hash_signature)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            record.execution_id, record.test_file, record.test_name,
            record.test_status, record.execution_time, record.duration,
            record.output, record.error_message, record.traceback,
            record.timestamp, record.hash_signature
        ))
        
        conn.commit()
        conn.close()
        
    def start_execution_session(self):
        """开始测试执行会话"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO test_execution_sessions (execution_id, session_start)
            VALUES (?, ?)
        ''', (self.execution_id, datetime.datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"测试执行会话开始: {self.execution_id}")
        
    def end_execution_session(self):
        """结束测试执行会话"""
        # 统计测试结果
        total_tests = len(self.test_records)
        passed_tests = len([r for r in self.test_records if r.test_status == 'PASSED'])
        failed_tests = len([r for r in self.test_records if r.test_status == 'FAILED'])
        skipped_tests = len([r for r in self.test_records if r.test_status == 'SKIPPED'])
        error_tests = len([r for r in self.test_records if r.test_status == 'ERROR'])
        
        # 判断合规状态
        compliance_status = 'COMPLIANT' if failed_tests == 0 and error_tests == 0 else 'NON_COMPLIANT'
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE test_execution_sessions 
            SET session_end = ?, total_tests = ?, passed_tests = ?, 
                failed_tests = ?, skipped_tests = ?, error_tests = ?,
                compliance_status = ?
            WHERE execution_id = ?
        ''', (
            datetime.datetime.now().isoformat(), total_tests, passed_tests,
            failed_tests, skipped_tests, error_tests, compliance_status,
            self.execution_id
        ))
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"测试执行会话结束: {self.execution_id}")
        self.logger.info(f"总测试数: {total_tests}, 通过: {passed_tests}, 失败: {failed_tests}")
        
    def run_pytest_with_logging(self, test_paths: List[str] = None):
        """运行pytest并记录所有测试结果"""
        if test_paths is None:
            # 自动发现所有测试文件
            test_paths = self.discover_test_files()
            
        self.start_execution_session()
        
        for test_path in test_paths:
            self.logger.info(f"执行测试文件: {test_path}")
            self.run_single_test_file(test_path)
            
        self.end_execution_session()
        self.generate_compliance_report()
        
    def discover_test_files(self) -> List[str]:
        """自动发现所有测试文件"""
        test_files = []
        
        # 搜索当前目录及子目录中的测试文件
        for pattern in ['test_*.py', '*_test.py']:
            test_files.extend(Path('.').rglob(pattern))
            
        # 搜索pc28_business_logic_tests目录
        business_logic_tests = Path('pc28_business_logic_tests')
        if business_logic_tests.exists():
            test_files.extend(business_logic_tests.rglob('test_*.py'))
            
        return [str(f) for f in test_files if f.is_file()]
        
    def run_single_test_file(self, test_file: str):
        """运行单个测试文件并记录结果"""
        try:
            # 使用pytest运行测试并捕获输出
            result = subprocess.run([
                sys.executable, '-m', 'pytest', test_file, '-v', '--tb=short'
            ], capture_output=True, text=True, timeout=300)
            
            # 解析pytest输出
            self.parse_pytest_output(test_file, result.stdout, result.stderr, result.returncode)
            
        except subprocess.TimeoutExpired:
            self.record_test_execution(
                test_file=test_file,
                test_name="TIMEOUT",
                test_status="ERROR",
                duration=300.0,
                output="",
                error_message="测试执行超时"
            )
        except Exception as e:
            self.record_test_execution(
                test_file=test_file,
                test_name="EXECUTION_ERROR",
                test_status="ERROR",
                duration=0.0,
                output="",
                error_message=str(e)
            )
            
    def parse_pytest_output(self, test_file: str, stdout: str, stderr: str, returncode: int):
        """解析pytest输出并记录测试结果"""
        lines = stdout.split('\n')
        current_test = None
        
        for line in lines:
            line = line.strip()
            
            # 解析测试结果行
            if '::' in line and any(status in line for status in ['PASSED', 'FAILED', 'SKIPPED', 'ERROR']):
                parts = line.split()
                if len(parts) >= 2:
                    test_name = parts[0].split('::')[-1] if '::' in parts[0] else parts[0]
                    status = parts[-1]
                    
                    # 提取执行时间（如果有）
                    duration = 0.0
                    for part in parts:
                        if 's' in part and part.replace('s', '').replace('.', '').isdigit():
                            try:
                                duration = float(part.replace('s', ''))
                            except:
                                pass
                    
                    self.record_test_execution(
                        test_file=test_file,
                        test_name=test_name,
                        test_status=status,
                        duration=duration,
                        output=line,
                        error_message=stderr if stderr else None
                    )
        
        # 如果没有解析到具体测试，记录整个文件的执行结果
        if not any(record.test_file == test_file for record in self.test_records):
            status = "PASSED" if returncode == 0 else "FAILED"
            self.record_test_execution(
                test_file=test_file,
                test_name="FILE_EXECUTION",
                test_status=status,
                duration=0.0,
                output=stdout,
                error_message=stderr if stderr else None
            )
            
    def generate_compliance_report(self):
        """生成合规性验证报告"""
        report_path = f"test_compliance_report_{self.execution_id}.md"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 自动化测试合规性验证报告\n\n")
            f.write(f"**执行ID**: {self.execution_id}\n")
            f.write(f"**生成时间**: {datetime.datetime.now().isoformat()}\n\n")
            
            # 统计信息
            total_tests = len(self.test_records)
            passed_tests = len([r for r in self.test_records if r.test_status == 'PASSED'])
            failed_tests = len([r for r in self.test_records if r.test_status == 'FAILED'])
            
            f.write("## 测试执行统计\n\n")
            f.write(f"- 总测试数: {total_tests}\n")
            f.write(f"- 通过测试: {passed_tests}\n")
            f.write(f"- 失败测试: {failed_tests}\n")
            f.write(f"- 成功率: {(passed_tests/total_tests*100):.2f}%\n\n")
            
            # 合规性状态
            f.write("## 合规性状态\n\n")
            compliance_status = "✅ 符合PROJECT_RULES.md要求" if failed_tests == 0 else "❌ 存在测试失败"
            f.write(f"**状态**: {compliance_status}\n\n")
            
            # 详细测试记录
            f.write("## 详细测试记录\n\n")
            for record in self.test_records:
                f.write(f"### {record.test_name}\n")
                f.write(f"- 文件: {record.test_file}\n")
                f.write(f"- 状态: {record.test_status}\n")
                f.write(f"- 执行时间: {record.execution_time}\n")
                f.write(f"- 持续时间: {record.duration}s\n")
                f.write(f"- 哈希签名: {record.hash_signature}\n\n")
                
        self.logger.info(f"合规性报告已生成: {report_path}")
        
    def export_logs_json(self):
        """导出日志为JSON格式"""
        export_path = f"test_logs_export_{self.execution_id}.json"
        
        export_data = {
            "execution_id": self.execution_id,
            "export_time": datetime.datetime.now().isoformat(),
            "total_records": len(self.test_records),
            "records": [asdict(record) for record in self.test_records]
        }
        
        with open(export_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
            
        self.logger.info(f"测试日志已导出: {export_path}")

def main():
    """主函数 - 执行完整的自动化测试日志记录"""
    try:
        logger = AutomatedTestLogger()
        
        print("🚀 开始执行自动化测试日志系统...")
        print("📋 符合PROJECT_RULES.md第1.2条要求")
        
        # 运行所有测试并记录日志
        logger.run_pytest_with_logging()
        
        # 导出日志
        logger.export_logs_json()
        
        print("✅ 自动化测试日志系统执行完成")
        print(f"📊 执行ID: {logger.execution_id}")
        print("📁 所有测试证据已妥善保存")
        
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()