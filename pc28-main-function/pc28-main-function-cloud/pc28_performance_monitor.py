#!/usr/bin/env python3
"""
PC28性能监控脚本
实时监控系统性能指标
"""

import time
import psutil
import json
from datetime import datetime

class PC28PerformanceMonitor:
    def __init__(self):
        self.metrics = []
        
    def collect_metrics(self):
        """收集性能指标"""
        return {
            'timestamp': datetime.now().isoformat(),
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent
        }
        
    def monitor_loop(self, duration=300):
        """监控循环"""
        start_time = time.time()
        while time.time() - start_time < duration:
            metrics = self.collect_metrics()
            self.metrics.append(metrics)
            print(f"CPU: {metrics['cpu_percent']}%, Memory: {metrics['memory_percent']}%")
            time.sleep(10)
            
        # 保存监控结果
        with open(f'pc28_performance_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
            json.dump(self.metrics, f, indent=2)

if __name__ == "__main__":
    monitor = PC28PerformanceMonitor()
    monitor.monitor_loop()
