#!/usr/bin/env python3
"""
示例scaffold.py文件 - 用于演示性能优化功能
包含一些可以优化的代码模式
"""

import os
import sys
import json
from typing import List, Dict, Any

# 全局变量（可优化）
GLOBAL_CONFIG = {}
GLOBAL_CACHE = []
GLOBAL_STATS = {}
GLOBAL_SETTINGS = {}
GLOBAL_DATA = []
GLOBAL_TEMP = {}

class DataProcessor:
    """数据处理器类"""
    
    def __init__(self):
        self.data = []
        self.results = []
    
    def process_large_dataset(self, data: List[int]) -> List[int]:
        """处理大数据集 - 包含可优化的循环"""
        # 低效的嵌套循环（可优化）
        results = []
        for i in range(len(data)):
            for j in range(len(data)):
                if i != j:
                    for k in range(len(data)):
                        if k != i and k != j:
                            results.append(data[i] + data[j] + data[k])
        return results
    
    def inefficient_data_structure(self, size: int) -> List[int]:
        """低效的数据结构使用"""
        # 使用list(range())而不是生成器（可优化）
        large_list = list(range(size * 1000))
        return [x * 2 for x in large_list if x % 2 == 0]
    
    def file_operations_without_context(self, filename: str) -> str:
        """不使用with语句的文件操作（可优化）"""
        # 不安全的文件操作（可优化）
        f = open(filename, 'r')
        content = f.read()
        f.close()
        return content
    
    def cpu_intensive_task(self, data: List[int]) -> List[int]:
        """CPU密集型任务（可并行化优化）"""
        results = []
        for item in data:
            # 模拟复杂计算
            result = 0
            for i in range(1000):
                result += item * i * i
            results.append(result)
        return results

def main():
    """主函数"""
    processor = DataProcessor()
    
    # 测试数据
    test_data = [1, 2, 3, 4, 5]
    
    print("开始处理数据...")
    
    # 调用各种可优化的方法
    results1 = processor.process_large_dataset(test_data)
    results2 = processor.inefficient_data_structure(10)
    results3 = processor.cpu_intensive_task(test_data)
    
    print(f"处理完成，结果数量: {len(results1)}, {len(results2)}, {len(results3)}")

if __name__ == "__main__":
    main()