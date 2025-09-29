#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28监控系统测试脚本
测试监控系统的各个组件和功能
"""

import os
import sys
import json
import time
import asyncio
import logging
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from python.system_monitor import SystemMonitor, SystemMetrics, DataQualityMetrics
    from python.monitoring_dashboard import MonitoringDashboard
    from python.health_alert_system import HealthAlertSystem
except ImportError as e:
    print(f"导入错误: {e}")
    print("请确保监控模块在正确的路径中")
    # 在pytest环境中不要直接退出，而是跳过测试
    import pytest
    pytest.skip(f"监控模块导入失败: {e}", allow_module_level=True)

class TestSystemMonitor(unittest.TestCase):
    """系统监控器测试"""
    
    def setUp(self):
        """测试初始化"""
        self.config = {
            "monitoring": {
                "enabled": True,
                "interval": 60,
                "metrics_retention_days": 30,
                "alert_rules": [
                    {
                        "name": "high_cpu",
                        "metric": "cpu_percent",
                        "threshold": 80,
                        "operator": ">",
                        "severity": "warning"
                    }
                ]
            },
            "alerts": {
                "channels": {
                    "email": {"enabled": False},
                    "slack": {"enabled": False},
                    "telegram": {"enabled": False}
                }
            },
            "logging": {
                "level": "INFO",
                "file": "logs/monitoring/test.log"
            }
        }
        self.monitor = SystemMonitor(self.config)
    
    def test_monitor_initialization(self):
        """测试监控器初始化"""
        self.assertIsNotNone(self.monitor)
        self.assertEqual(self.monitor.config, self.config)
        self.assertFalse(self.monitor.running)
    
    def test_system_metrics_collection(self):
        """测试系统指标收集"""
        metrics = self.monitor.collect_system_metrics()
        
        self.assertIsInstance(metrics, SystemMetrics)
        self.assertIsInstance(metrics.cpu_percent, (int, float))
        self.assertIsInstance(metrics.memory_percent, (int, float))
        self.assertIsInstance(metrics.disk_percent, (int, float))
        self.assertGreaterEqual(metrics.cpu_percent, 0)
        self.assertLessEqual(metrics.cpu_percent, 100)
    
    def test_data_quality_metrics(self):
        """测试数据质量指标"""
        with patch('python.system_monitor.SystemMonitor.check_bigquery_connection') as mock_bq:
            mock_bq.return_value = True
            
            metrics = self.monitor.collect_data_quality_metrics()
            
            self.assertIsInstance(metrics, DataQualityMetrics)
            self.assertIsInstance(metrics.completeness, (int, float))
            self.assertIsInstance(metrics.accuracy, (int, float))
    
    def test_alert_rules(self):
        """测试告警规则"""
        # 创建测试指标
        test_metrics = SystemMetrics(
            timestamp=time.time(),
            cpu_percent=85.0,  # 超过阈值
            memory_percent=60.0,
            disk_percent=45.0,
            network_io={'bytes_sent': 1000, 'bytes_recv': 2000},
            disk_io={'read_bytes': 500, 'write_bytes': 800}
        )
        
        alerts = self.monitor.check_alert_rules(test_metrics)
        
        # 应该触发CPU告警
        self.assertGreater(len(alerts), 0)
        cpu_alert = next((alert for alert in alerts if 'cpu' in alert.message.lower()), None)
        self.assertIsNotNone(cpu_alert)
    
    def test_service_status_check(self):
        """测试服务状态检查"""
        with patch('requests.get') as mock_get:
            # 模拟API响应
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {'status': 'ok'}
            mock_get.return_value = mock_response
            
            status = self.monitor.check_service_status('test_api', 'http://localhost:8080/health')
            
            self.assertTrue(status.is_healthy)
            self.assertEqual(status.response_time, mock_response.elapsed.total_seconds())

class TestMonitoringDashboard(unittest.TestCase):
    """监控仪表板测试"""
    
    def setUp(self):
        """测试初始化"""
        self.config = {
            "monitoring": {
                "dashboard": {
                    "enabled": True,
                    "port": 8080,
                    "host": "localhost"
                }
            }
        }
    
    def test_dashboard_initialization(self):
        """测试仪表板初始化"""
        dashboard = MonitoringDashboard(self.config)
        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard.port, 8080)
        self.assertEqual(dashboard.host, "localhost")
    
    @patch('python.monitoring_dashboard.MonitoringDashboard.start_server')
    def test_dashboard_start(self, mock_start):
        """测试仪表板启动"""
        dashboard = MonitoringDashboard(self.config)
        dashboard.start()
        mock_start.assert_called_once()

class TestHealthAlertSystem(unittest.TestCase):
    """健康告警系统测试"""
    
    def setUp(self):
        """测试初始化"""
        self.config = {
            "alerts": {
                "channels": {
                    "email": {
                        "enabled": True,
                        "smtp_server": "smtp.example.com",
                        "smtp_port": 587,
                        "username": "test@example.com",
                        "password": "password",
                        "recipients": ["admin@example.com"]
                    },
                    "telegram": {
                        "enabled": True,
                        "bot_token": "test_token",
                        "chat_id": "test_chat_id"
                    }
                }
            }
        }
        self.alert_system = HealthAlertSystem(self.config)
    
    def test_alert_system_initialization(self):
        """测试告警系统初始化"""
        self.assertIsNotNone(self.alert_system)
        self.assertEqual(self.alert_system.config, self.config)
    
    @patch('smtplib.SMTP')
    def test_email_alert(self, mock_smtp):
        """测试邮件告警"""
        mock_server = Mock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        alert_data = {
            'severity': 'critical',
            'message': 'Test alert',
            'timestamp': time.time()
        }
        
        result = self.alert_system.send_email_alert(alert_data)
        self.assertTrue(result)
        mock_server.send_message.assert_called_once()
    
    @patch('requests.post')
    def test_telegram_alert(self, mock_post):
        """测试Telegram告警"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'ok': True}
        mock_post.return_value = mock_response
        
        alert_data = {
            'severity': 'warning',
            'message': 'Test telegram alert',
            'timestamp': time.time()
        }
        
        result = self.alert_system.send_telegram_alert(alert_data)
        self.assertTrue(result)
        mock_post.assert_called_once()

class TestConfigurationValidation(unittest.TestCase):
    """配置验证测试"""
    
    def test_config_file_exists(self):
        """测试配置文件存在"""
        config_path = Path('config/ops_config.json')
        self.assertTrue(config_path.exists(), "配置文件不存在")
    
    def test_config_file_valid_json(self):
        """测试配置文件是有效的JSON"""
        config_path = Path('config/ops_config.json')
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.assertIsInstance(config, dict)
        except json.JSONDecodeError as e:
            self.fail(f"配置文件JSON格式错误: {e}")
    
    def test_required_config_sections(self):
        """测试必需的配置节点"""
        config_path = Path('config/ops_config.json')
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        required_sections = ['monitoring', 'alerts', 'logging']
        for section in required_sections:
            self.assertIn(section, config, f"缺少必需的配置节点: {section}")
    
    def test_monitoring_config_structure(self):
        """测试监控配置结构"""
        config_path = Path('config/ops_config.json')
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        monitoring_config = config.get('monitoring', {})
        
        # 检查必需的监控配置
        required_keys = ['enabled', 'interval']
        for key in required_keys:
            self.assertIn(key, monitoring_config, f"监控配置缺少: {key}")

class TestEnvironmentSetup(unittest.TestCase):
    """环境设置测试"""
    
    def test_log_directory_exists(self):
        """测试日志目录存在"""
        log_dir = Path('logs/monitoring')
        self.assertTrue(log_dir.exists() or log_dir.parent.exists(), 
                       "日志目录不存在")
    
    def test_python_dependencies(self):
        """测试Python依赖"""
        required_modules = ['psutil', 'asyncio', 'json', 'logging']
        
        for module in required_modules:
            try:
                __import__(module)
            except ImportError:
                self.fail(f"缺少必需的Python模块: {module}")
    
    def test_file_permissions(self):
        """测试文件权限"""
        script_files = [
            'python/system_monitor.py',
            'python/monitoring_dashboard.py'
        ]
        
        for script_file in script_files:
            script_path = Path(script_file)
            if script_path.exists():
                # 检查文件是否可读
                self.assertTrue(os.access(script_path, os.R_OK), 
                               f"文件不可读: {script_file}")

def run_integration_tests():
    """运行集成测试"""
    print("\n=== 运行集成测试 ===")
    
    # 测试监控系统启动和停止
    print("测试监控系统启动...")
    
    try:
        # 创建测试配置
        test_config = {
            "monitoring": {
                "enabled": True,
                "interval": 5,  # 短间隔用于测试
                "metrics_retention_days": 1
            },
            "alerts": {
                "channels": {
                    "email": {"enabled": False},
                    "slack": {"enabled": False},
                    "telegram": {"enabled": False}
                }
            },
            "logging": {
                "level": "INFO",
                "file": "logs/monitoring/integration_test.log"
            }
        }
        
        # 初始化监控器
        monitor = SystemMonitor(test_config)
        
        # 测试指标收集
        print("测试指标收集...")
        metrics = monitor.collect_system_metrics()
        print(f"CPU使用率: {metrics.cpu_percent}%")
        print(f"内存使用率: {metrics.memory_percent}%")
        print(f"磁盘使用率: {metrics.disk_percent}%")
        
        # 测试告警规则
        print("测试告警规则...")
        alerts = monitor.check_alert_rules(metrics)
        print(f"触发告警数量: {len(alerts)}")
        
        print("集成测试完成")
        return True
        
    except Exception as e:
        print(f"集成测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("PC28监控系统测试")
    print("=" * 50)
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加测试用例
    test_classes = [
        TestConfigurationValidation,
        TestEnvironmentSetup,
        TestSystemMonitor,
        TestMonitoringDashboard,
        TestHealthAlertSystem
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # 运行单元测试
    print("\n运行单元测试...")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 运行集成测试
    integration_success = run_integration_tests()
    
    # 测试总结
    print("\n=== 测试总结 ===")
    print(f"单元测试 - 运行: {result.testsRun}, 失败: {len(result.failures)}, 错误: {len(result.errors)}")
    print(f"集成测试 - {'通过' if integration_success else '失败'}")
    
    # 输出详细的失败信息
    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\n错误的测试:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    # 返回测试结果
    success = (len(result.failures) == 0 and 
               len(result.errors) == 0 and 
               integration_success)
    
    if success:
        print("\n✅ 所有测试通过！监控系统准备就绪。")
        return 0
    else:
        print("\n❌ 部分测试失败，请检查配置和依赖。")
        return 1

if __name__ == '__main__':
    sys.exit(main())