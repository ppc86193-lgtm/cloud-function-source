#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 运维管理系统主入口
整合监控、数据质量检查、并发调优和组件更新管理功能
"""

import os
import sys
import json
import time
import logging
import argparse
import threading
from datetime import datetime
from typing import Dict, Any, List

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

# 导入各个模块
from monitoring_dashboard import MonitoringDashboard
from data_quality_checker import DataQualityChecker
from concurrency_tuner import ConcurrencyTuner
from component_updater import ComponentUpdater
from alert_notification_system import AlertNotificationSystem
from python.api_monitor import APIMonitor

class OpsSystemManager:
    """运维系统管理器"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config', 'ops_config.json')
        self.config = self._load_config()
        
        # 设置日志
        self._setup_logging()
        
        # 初始化各个模块
        self.monitoring_dashboard = None
        self.data_quality_checker = None
        self.concurrency_tuner = None
        self.component_updater = None
        self.alert_system = None
        self.api_monitor = None
        
        # 运行状态
        self.running = False
        self.threads = []
        
        self._initialize_modules()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            # 创建默认配置
            default_config = self._create_default_config()
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, ensure_ascii=False, indent=2)
            return default_config
        except Exception as e:
            print(f"配置文件加载失败: {e}")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """创建默认配置"""
        return {
            "system": {
                "name": "PC28运维管理系统",
                "version": "1.0.0",
                "environment": "production",
                "log_level": "INFO"
            },
            "modules": {
                "monitoring_dashboard": {
                    "enabled": True,
                    "config_file": "config/monitoring_config.json",
                    "auto_start": True
                },
                "data_quality_checker": {
                    "enabled": True,
                    "config_file": "config/data_quality_config.json",
                    "auto_start": True
                },
                "concurrency_tuner": {
                    "enabled": True,
                    "config_file": "config/concurrency_config.json",
                    "auto_start": False
                },
                "component_updater": {
                    "enabled": True,
                    "config_file": "config/component_config.json",
                    "auto_start": True
                },
                "alert_system": {
                    "enabled": True,
                    "config_file": "config/alert_config.json",
                    "auto_start": True
                },
                "api_monitor": {
                    "enabled": True,
                    "config_file": "config/api_monitor_config.json",
                    "auto_start": True
                }
            },
            "scheduling": {
                "data_quality_check_interval": 3600,
                "component_update_check_interval": 21600,
                "performance_monitoring_interval": 300,
                "health_check_interval": 60
            },
            "gcp": {
                "project_id": "pc28-ops-system",
                "region": "us-central1",
                "monitoring_enabled": True,
                "logging_enabled": True
            }
        }
    
    def _setup_logging(self):
        """设置日志"""
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'ops_system_{datetime.now().strftime("%Y%m%d")}.log')
        
        log_level = getattr(logging, self.config.get('system', {}).get('log_level', 'INFO'))
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def _initialize_modules(self):
        """初始化各个模块"""
        modules_config = self.config.get('modules', {})
        
        try:
            # 初始化监控仪表板
            if modules_config.get('monitoring_dashboard', {}).get('enabled', True) or not modules_config:
                config_file = modules_config.get('monitoring_dashboard', {}).get('config_file') if modules_config else None
                if config_file and os.path.exists(os.path.join(os.path.dirname(__file__), config_file)):
                    config_path = os.path.join(os.path.dirname(__file__), config_file)
                    with open(config_path, 'r', encoding='utf-8') as f:
                        dashboard_config = json.load(f)
                else:
                    # 使用默认配置
                    dashboard_config = {
                            "appid": "test_app",
                            "secret_key": "test_secret"
                        },
                        "bigquery": {
                            "project_id": "pc28-project",
                            "dataset_id": "pc28_dataset"
                        }
                    }
                self.monitoring_dashboard = MonitoringDashboard(dashboard_config)
                self.logger.info("监控仪表板初始化成功")
            
            # 初始化数据质量检查器
            if modules_config.get('data_quality_checker', {}).get('enabled', True) or not modules_config:
                self.data_quality_checker = DataQualityChecker()
                self.logger.info("数据质量检查器初始化成功")
            
            # 初始化并发调优器
            if modules_config.get('concurrency_tuner', {}).get('enabled', True) or not modules_config:
                self.concurrency_tuner = ConcurrencyTuner()
                self.logger.info("并发调优器初始化成功")
            
            # 初始化组件更新管理器
            if modules_config.get('component_updater', {}).get('enabled', True) or not modules_config:
                self.component_updater = ComponentUpdater()
                self.logger.info("组件更新管理器初始化成功")
            
            # 初始化告警系统
            if modules_config.get('alert_system', {}).get('enabled', True) or not modules_config:
                self.alert_system = AlertNotificationSystem()
                self.logger.info("告警系统初始化成功")
            
            # 初始化API监控器
            if modules_config.get('api_monitor', {}).get('enabled', True) or not modules_config:
                config_file = modules_config.get('api_monitor', {}).get('config_file') if modules_config else None
                if config_file and os.path.exists(os.path.join(os.path.dirname(__file__), config_file)):
                    config_path = os.path.join(os.path.dirname(__file__), config_file)
                    with open(config_path, 'r', encoding='utf-8') as f:
                        api_config = json.load(f)
                    self.api_monitor = APIMonitor(api_config)
                else:
                    # 使用默认配置
                    default_api_config = {
                            "appid": "test_app",
                            "secret_key": "test_secret"
                        },
                        "bigquery": {
                            "project_id": "pc28-project",
                            "dataset_id": "pc28_dataset"
                        }
                    }
                    self.api_monitor = APIMonitor(default_api_config)
                self.logger.info("API监控器初始化成功")
                
        except Exception as e:
            self.logger.error(f"模块初始化失败: {e}")
            raise
    
    def start_system(self):
        """启动运维系统"""
        if self.running:
            self.logger.warning("系统已经在运行")
            return True
        
        self.running = True
        self.logger.info("启动PC28运维管理系统...")
        
        modules_config = self.config.get('modules', {})
        
        try:
            # 启动各个模块
            if self.monitoring_dashboard and modules_config.get('monitoring_dashboard', {}).get('auto_start', True):
                thread = threading.Thread(target=self._start_monitoring_dashboard, daemon=True)
                thread.start()
                self.threads.append(thread)
            
            if self.data_quality_checker and modules_config.get('data_quality_checker', {}).get('auto_start', True):
                thread = threading.Thread(target=self._start_data_quality_monitoring, daemon=True)
                thread.start()
                self.threads.append(thread)
            
            if self.component_updater and modules_config.get('component_updater', {}).get('auto_start', True):
                self.component_updater.start_auto_check()
            
            if self.concurrency_tuner and modules_config.get('concurrency_tuner', {}).get('auto_start', False):
                self.concurrency_tuner.start_monitoring()
            
            if self.api_monitor and modules_config.get('api_monitor', {}).get('auto_start', True):
                thread = threading.Thread(target=self._start_api_monitoring, daemon=True)
                thread.start()
                self.threads.append(thread)
            
            # 启动健康检查
            thread = threading.Thread(target=self._health_check_loop, daemon=True)
            thread.start()
            self.threads.append(thread)
            
            self.logger.info("PC28运维管理系统启动完成")
            return True
            
        except Exception as e:
            self.logger.error(f"系统启动失败: {e}")
            self.running = False
            return False
    
    def stop_system(self):
        """停止运维系统"""
        if not self.running:
            return
        
        self.running = False
        self.logger.info("停止PC28运维管理系统...")
        
        # 停止各个模块
        if self.component_updater:
            self.component_updater.stop_auto_check()
        
        if self.concurrency_tuner:
            self.concurrency_tuner.stop_monitoring()
        
        # 等待线程结束
        for thread in self.threads:
            thread.join(timeout=5)
        
        self.logger.info("PC28运维管理系统已停止")
    
    def _start_monitoring_dashboard(self):
        """启动监控仪表板"""
        try:
            if self.monitoring_dashboard:
                self.monitoring_dashboard.start_monitoring()
        except Exception as e:
            self.logger.error(f"监控仪表板启动失败: {e}")
    
    def _start_data_quality_monitoring(self):
        """启动数据质量监控"""
        interval = self.config.get('scheduling', {}).get('data_quality_check_interval', 3600)
        
        while self.running:
            try:
                if self.data_quality_checker:
                    self.logger.info("执行数据质量检查...")
                    results = self.data_quality_checker.run_all_checks()
                    
                    # 检查是否有异常需要告警
                    for check_name, result in results.items():
                        if not result.get('passed', True):
                            if self.alert_system:
                                self.alert_system.send_alert(
                                    level='warning',
                                    alert_type='data_quality',
                                    message=f"数据质量检查失败: {check_name}",
                                    details=result
                                )
                
                # 等待下次检查
                for _ in range(interval):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"数据质量监控异常: {e}")
                time.sleep(60)
    
    def _start_api_monitoring(self):
        """启动API监控"""
        interval = self.config.get('scheduling', {}).get('performance_monitoring_interval', 300)
        
        while self.running:
            try:
                if self.api_monitor:
                    # 获取API监控数据
                    stats = self.api_monitor.get_stats()
                    
                    # 检查是否有异常
                    if stats.get('error_rate', 0) > 0.1:  # 错误率超过10%
                        if self.alert_system:
                            self.alert_system.send_alert(
                                level='critical',
                                alert_type='api_error',
                                message=f"API错误率过高: {stats.get('error_rate', 0):.2%}",
                                details=stats
                            )
                
                # 等待下次检查
                for _ in range(interval):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"API监控异常: {e}")
                time.sleep(60)
    
    def _health_check_loop(self):
        """健康检查循环"""
        interval = self.config.get('scheduling', {}).get('health_check_interval', 60)
        
        while self.running:
            try:
                health_status = self.get_system_health()
                
                # 检查系统健康状态
                if health_status.get('overall_health') != 'healthy':
                    if self.alert_system:
                        self.alert_system.send_alert(
                            level='critical',
                            alert_type='system_health',
                            message="系统健康检查发现异常",
                            details=health_status
                        )
                
                # 等待下次检查
                for _ in range(interval):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"健康检查异常: {e}")
                time.sleep(60)
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        status = {
            'system_info': {
                'name': self.config.get('system', {}).get('name', 'PC28运维管理系统'),
                'version': self.config.get('system', {}).get('version', '1.0.0'),
                'running': self.running,
                'uptime': time.time() - getattr(self, 'start_time', time.time())
            },
            'modules': {},
            'timestamp': datetime.now().isoformat()
        }
        
        # 获取各模块状态
        if self.monitoring_dashboard:
            try:
                status['modules']['monitoring_dashboard'] = {
                    'status': 'running',
                    'metrics': self.monitoring_dashboard.get_current_metrics()
                }
            except Exception as e:
                status['modules']['monitoring_dashboard'] = {'status': 'error', 'error': str(e)}
        
        if self.data_quality_checker:
            try:
                status['modules']['data_quality_checker'] = {
                    'status': 'running',
                    'last_check': getattr(self.data_quality_checker, 'last_check_time', None)
                }
            except Exception as e:
                status['modules']['data_quality_checker'] = {'status': 'error', 'error': str(e)}
        
        if self.concurrency_tuner:
            try:
                status['modules']['concurrency_tuner'] = {
                    'status': 'running' if self.concurrency_tuner.monitoring_active else 'stopped',
                    'config': self.concurrency_tuner.get_current_config()
                }
            except Exception as e:
                status['modules']['concurrency_tuner'] = {'status': 'error', 'error': str(e)}
        
        if self.component_updater:
            try:
                status['modules']['component_updater'] = {
                    'status': 'running' if self.component_updater.monitoring_active else 'stopped',
                    'components': len(self.component_updater.components)
                }
            except Exception as e:
                status['modules']['component_updater'] = {'status': 'error', 'error': str(e)}
        
        if self.api_monitor:
            try:
                status['modules']['api_monitor'] = {
                    'status': 'running',
                    'stats': self.api_monitor.get_stats()
                }
            except Exception as e:
                status['modules']['api_monitor'] = {'status': 'error', 'error': str(e)}
        
        return status
    
    def get_system_health(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        health_status = {
            'overall_health': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'modules': {}
        }
        
        issues = []
        
        # 检查各模块健康状态
        if self.monitoring_dashboard:
            try:
                dashboard_status = self.monitoring_dashboard.get_status() if hasattr(self.monitoring_dashboard, 'get_status') else {'running': True}
                health_status['modules']['monitoring_dashboard'] = {
                    'status': 'healthy' if dashboard_status.get('running', False) else 'unhealthy',
                    'details': dashboard_status
                }
                if not dashboard_status.get('running', False):
                    issues.append('监控仪表板未运行')
            except Exception as e:
                health_status['modules']['monitoring_dashboard'] = {
                    'status': 'error',
                    'error': str(e)
                }
                issues.append(f'监控仪表板错误: {e}')
        
        if self.data_quality_checker:
            try:
                dq_status = self.data_quality_checker.get_status() if hasattr(self.data_quality_checker, 'get_status') else {'running': True}
                health_status['modules']['data_quality_checker'] = {
                    'status': 'healthy',
                    'details': dq_status
                }
            except Exception as e:
                health_status['modules']['data_quality_checker'] = {
                    'status': 'error',
                    'error': str(e)
                }
                issues.append(f'数据质量检查器错误: {e}')
        
        if self.concurrency_tuner:
            try:
                ct_status = self.concurrency_tuner.get_status() if hasattr(self.concurrency_tuner, 'get_status') else {'running': True}
                health_status['modules']['concurrency_tuner'] = {
                    'status': 'healthy',
                    'details': ct_status
                }
            except Exception as e:
                health_status['modules']['concurrency_tuner'] = {
                    'status': 'error',
                    'error': str(e)
                }
                issues.append(f'并发调优器错误: {e}')
        
        if self.component_updater:
            try:
                cu_status = self.component_updater.get_component_status() if hasattr(self.component_updater, 'get_component_status') else {'running': True}
                health_status['modules']['component_updater'] = {
                    'status': 'healthy',
                    'details': cu_status
                }
            except Exception as e:
                health_status['modules']['component_updater'] = {
                    'status': 'error',
                    'error': str(e)
                }
                issues.append(f'组件更新器错误: {e}')
        
        if self.api_monitor:
            try:
                api_status = self.api_monitor.get_status() if hasattr(self.api_monitor, 'get_status') else {'running': True}
                health_status['modules']['api_monitor'] = {
                    'status': 'healthy',
                    'details': api_status
                }
            except Exception as e:
                health_status['modules']['api_monitor'] = {
                    'status': 'error',
                    'error': str(e)
                }
                issues.append(f'API监控器错误: {e}')
        
        # 设置整体健康状态
        if issues:
            health_status['overall_health'] = 'degraded' if len(issues) < 3 else 'unhealthy'
            health_status['issues'] = issues
        
        return health_status
    
    def run_health_check(self) -> Dict[str, Any]:
        """运行健康检查"""
        try:
            health_status = self.get_system_health()
            self.logger.info(f"健康检查完成 - 整体状态: {health_status['overall_health']}")
            
            # 如果有问题，记录详细信息
            if 'issues' in health_status:
                for issue in health_status['issues']:
                    self.logger.warning(f"健康检查发现问题: {issue}")
            
            return health_status
            
        except Exception as e:
            self.logger.error(f"健康检查失败: {e}")
            return {
                'overall_health': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def run_end_to_end_test(self) -> Dict[str, Any]:
        """运行端到端测试"""
        self.logger.info("开始端到端测试...")
        
        test_results = {
            'start_time': datetime.now().isoformat(),
            'tests': {},
            'overall_success': True
        }
        
        # 测试数据质量检查
        if self.data_quality_checker:
            try:
                self.logger.info("测试数据质量检查...")
                results = self.data_quality_checker.run_all_checks()
                test_results['tests']['data_quality'] = {
                    'success': True,
                    'results': results
                }
            except Exception as e:
                test_results['tests']['data_quality'] = {
                    'success': False,
                    'error': str(e)
                }
                test_results['overall_success'] = False
        
        # 测试组件更新检查
        if self.component_updater:
            try:
                self.logger.info("测试组件更新检查...")
                updates = self.component_updater.check_updates()
                test_results['tests']['component_updates'] = {
                    'success': True,
                    'updates_available': sum(updates.values())
                }
            except Exception as e:
                test_results['tests']['component_updates'] = {
                    'success': False,
                    'error': str(e)
                }
                test_results['overall_success'] = False
        
        # 测试并发调优
        if self.concurrency_tuner:
            try:
                self.logger.info("测试并发调优...")
                config = self.concurrency_tuner.get_current_config()
                test_results['tests']['concurrency_tuning'] = {
                    'success': True,
                    'current_config': config
                }
            except Exception as e:
                test_results['tests']['concurrency_tuning'] = {
                    'success': False,
                    'error': str(e)
                }
                test_results['overall_success'] = False
        
        # 测试告警系统
        if self.alert_system:
            try:
                self.logger.info("测试告警系统...")
                # 发送测试告警
                success = self.alert_system.send_alert(
                    level='info',
                    alert_type='test',
                    message='端到端测试告警',
                    details={'test': True}
                )
                test_results['tests']['alert_system'] = {
                    'success': success,
                    'message': '测试告警发送成功' if success else '测试告警发送失败'
                }
                if not success:
                    test_results['overall_success'] = False
            except Exception as e:
                test_results['tests']['alert_system'] = {
                    'success': False,
                    'error': str(e)
                }
                test_results['overall_success'] = False
        
        test_results['end_time'] = datetime.now().isoformat()
        
        self.logger.info(f"端到端测试完成，结果: {'成功' if test_results['overall_success'] else '失败'}")
        
        return test_results

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PC28 运维管理系统')
    parser.add_argument('command', choices=[
        'start', 'stop', 'status', 'health', 'test', 'config'
    ], help='命令')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--daemon', action='store_true', help='后台运行')
    
    args = parser.parse_args()
    
    # 初始化系统管理器
    ops_manager = OpsSystemManager(args.config)
    
    try:
        if args.command == 'start':
            ops_manager.start_time = time.time()
            ops_manager.start_system()
            
            if args.daemon:
                print("系统已在后台启动")
            else:
                print("系统已启动，按 Ctrl+C 停止")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print("\n正在停止系统...")
                    ops_manager.stop_system()
        
        elif args.command == 'stop':
            ops_manager.stop_system()
            print("系统已停止")
        
        elif args.command == 'status':
            status = ops_manager.get_system_status()
            print(json.dumps(status, ensure_ascii=False, indent=2))
        
        elif args.command == 'health':
            health = ops_manager.get_system_health()
            print(json.dumps(health, ensure_ascii=False, indent=2))
        
        elif args.command == 'test':
            test_results = ops_manager.run_end_to_end_test()
            print(json.dumps(test_results, ensure_ascii=False, indent=2))
        
        elif args.command == 'config':
            print(json.dumps(ops_manager.config, ensure_ascii=False, indent=2))
    
    except KeyboardInterrupt:
        print("\n系统已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()