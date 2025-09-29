#!/usr/bin/env python3
"""
PC28业务逻辑测试运行器
运行所有业务逻辑测试并生成报告
"""

import unittest
import sys
import os
from datetime import datetime
import json

class PC28TestRunner:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results = {}
        
    def discover_and_run_tests(self):
        """发现并运行所有测试"""
        print(f"开始运行PC28业务逻辑测试套件 - {self.timestamp}")
        
        # 发现测试
        loader = unittest.TestLoader()
        start_dir = 'pc28_business_logic_tests'
        suite = loader.discover(start_dir, pattern='test_*.py')
        
        # 运行测试
        runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
        result = runner.run(suite)
        
        # 收集结果
        self.results = {
            'timestamp': self.timestamp,
            'tests_run': result.testsRun,
            'failures': len(result.failures),
            'errors': len(result.errors),
            'success_rate': (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100 if result.testsRun > 0 else 0,
            'failure_details': [str(failure) for failure in result.failures],
            'error_details': [str(error) for error in result.errors]
        }
        
        return result.wasSuccessful()
        
    def generate_report(self):
        """生成测试报告"""
        report_file = f"pc28_test_report_{self.timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
            
        print(f"\n测试报告已生成: {report_file}")
        print(f"测试总数: {self.results['tests_run']}")
        print(f"失败数: {self.results['failures']}")
        print(f"错误数: {self.results['errors']}")
        print(f"成功率: {self.results['success_rate']:.2f}%")
        
        return report_file

if __name__ == '__main__':
    runner = PC28TestRunner()
    success = runner.discover_and_run_tests()
    runner.generate_report()
    
    if not success:
        print("\n⚠️  部分测试失败，请检查测试报告")
        sys.exit(1)
    else:
        print("\n✅ 所有测试通过！")
