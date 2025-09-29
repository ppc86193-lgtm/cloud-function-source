#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统运维管理主入口
统一管理系统监控、数据质量检查、并发调优、组件更新等运维功能
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import argparse
from pathlib import Path

# 导入各个运维模块
try:
    from ops_manager import OpsManager
    from data_quality_checker import DataQualityChecker
    from alert_notification_system import AlertNotificationSystem
    from concurrency_tuner import ConcurrencyTuner
    from component_updater import ComponentUpdater
    from python.monitoring_dashboard import MonitoringDashboard
except ImportError as e:
    print(f"导入运维模块失败: {e}")
    print("请确保所有运维模块文件都在同一目录下")
    sys.exit(1)

class PC28OpsManager:
    """PC28系统运维管理器"""
    
    def __init__(self, config_dir: str = None):
        self.config_dir = config_dir or os.path.join(os.path.dirname(__file__), 'config')
        os.makedirs(self.config_dir, exist_ok=True)
        
        # 设置日志
        self._setup_logging()
        
        # 初始化各个运维模块
        self._initialize_modules()
        
        # 运维状态
        self.monitoring_active = False
        self.monitoring_threads = {}
        
        self.logger.info("PC28运维管理系统初始化完成")
    
    def _setup_logging(self):
        """设置日志系统"""
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'ops_manager_{datetime.now().strftime("%Y%m%d")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def _load_default_config(self) -> Dict[str, Any]:
        """加载默认配置"""
        return {
            'upstream_api': {
                'appid': '45928',
                'secret_key': 'ca9edbfee35c22a0d6c4cf6722506af0'
            },
            'bigquery': {
                'project_id': 'pc28-data-platform',
                'dataset_id': 'pc28_dataset',
                'location': 'asia-east1'
            },
            'api': {
                'base_url': 'https://api.pc28.example.com',
                'timeout': 30,
                'retry_attempts': 3
            },
            'monitoring': {
                'check_interval': 60,
                'alert_threshold': 0.1,
                'history_retention_hours': 168
            },
            'alerts': {
                'email_enabled': False,
                'webhook_enabled': False,
                'slack_enabled': False
            }
        }
    
    def _initialize_modules(self):
        """初始化运维模块"""
        try:
            # 加载默认配置
            default_config = self._load_default_config()
            
            # 系统监控
            self.monitoring_dashboard = MonitoringDashboard(default_config)
            self.logger.info("✅ 监控仪表板初始化成功")
            
            # 数据质量检查
            self.data_quality_checker = DataQualityChecker()
            self.logger.info("✅ 数据质量检查器初始化成功")
            
            # 告警通知系统
            self.alert_system = AlertNotificationSystem()
            self.logger.info("✅ 告警通知系统初始化成功")
            
            # 并发参数调优
            self.concurrency_tuner = ConcurrencyTuner()
            self.logger.info("✅ 并发调优器初始化成功")
            
            # 组件更新管理
            self.component_updater = ComponentUpdater()
            self.logger.info("✅ 组件更新管理器初始化成功")
            
        except Exception as e:
            self.logger.error(f"初始化运维模块失败: {e}")
            raise
    
    def start_monitoring(self, modules: List[str] = None):
        """启动监控服务"""
        if self.monitoring_active:
            self.logger.warning("监控服务已经在运行")
            return
        
        modules = modules or ['dashboard', 'quality', 'concurrency', 'components']
        
        self.monitoring_active = True
        
        # 启动各个监控模块
        if 'dashboard' in modules:
            self._start_dashboard_monitoring()
        
        if 'quality' in modules:
            self._start_quality_monitoring()
        
        if 'concurrency' in modules:
            self._start_concurrency_monitoring()
        
        if 'components' in modules:
            self._start_component_monitoring()
        
        self.logger.info(f"监控服务已启动，模块: {modules}")
    
    def stop_monitoring(self):
        """停止监控服务"""
        self.monitoring_active = False
        
        # 停止各个监控线程
        for name, thread in self.monitoring_threads.items():
            if thread and thread.is_alive():
                self.logger.info(f"停止 {name} 监控")
                thread.join(timeout=5)
        
        # 停止各个模块的监控
        try:
            self.concurrency_tuner.stop_monitoring()
            self.component_updater.stop_auto_check()
        except Exception as e:
            self.logger.warning(f"停止模块监控时出错: {e}")
        
        self.monitoring_threads.clear()
        self.logger.info("监控服务已停止")
    
    def _start_dashboard_monitoring(self):
        """启动仪表板监控"""
        def dashboard_loop():
            while self.monitoring_active:
                try:
                    # 更新系统健康状态
                    health_status = self.monitoring_dashboard.get_system_health()
                    
                    # 检查异常情况并发送告警
                    self._check_system_alerts(health_status)
                    
                    time.sleep(60)  # 每分钟检查一次
                    
                except Exception as e:
                    self.logger.error(f"仪表板监控异常: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=dashboard_loop, daemon=True)
        thread.start()
        self.monitoring_threads['dashboard'] = thread
    
    def _start_quality_monitoring(self):
        """启动数据质量监控"""
        def quality_loop():
            while self.monitoring_active:
                try:
                    # 执行数据质量检查
                    issues = self.data_quality_checker.check_all_tables()
                    
                    # 如果发现问题，发送告警
                    if issues:
                        self._send_quality_alerts(issues)
                    
                    time.sleep(1800)  # 每30分钟检查一次
                    
                except Exception as e:
                    self.logger.error(f"数据质量监控异常: {e}")
                    time.sleep(1800)
        
        thread = threading.Thread(target=quality_loop, daemon=True)
        thread.start()
        self.monitoring_threads['quality'] = thread
    
    def _start_concurrency_monitoring(self):
        """启动并发监控"""
        try:
            self.concurrency_tuner.start_monitoring()
        except Exception as e:
            self.logger.error(f"启动并发监控失败: {e}")
    
    def _start_component_monitoring(self):
        """启动组件监控"""
        try:
            self.component_updater.start_auto_check()
        except Exception as e:
            self.logger.error(f"启动组件监控失败: {e}")
    
    def _check_system_alerts(self, health_status: Dict[str, Any]):
        """检查系统告警"""
        try:
            # 检查API状态
            api_status = health_status.get('api_status', {})
            if not api_status.get('healthy', True):
                self.alert_system.create_alert(
                    alert_type='system',
                    level='high',
                    title='API服务异常',
                    message=f"API服务状态异常: {api_status.get('error', '未知错误')}",
                    source='monitoring_dashboard'
                )
            
            # 检查数据质量
            data_quality = health_status.get('data_quality', {})
            quality_score = data_quality.get('overall_score', 100)
            if quality_score < 80:
                self.alert_system.create_alert(
                    alert_type='data_quality',
                    level='medium',
                    title='数据质量告警',
                    message=f"数据质量评分较低: {quality_score}%",
                    source='monitoring_dashboard'
                )
            
            # 检查系统性能
            performance = health_status.get('performance', {})
            error_rate = performance.get('error_rate', 0)
            if error_rate > 5:
                self.alert_system.create_alert(
                    alert_type='performance',
                    level='high',
                    title='系统错误率过高',
                    message=f"系统错误率: {error_rate}%",
                    source='monitoring_dashboard'
                )
            
        except Exception as e:
            self.logger.error(f"检查系统告警失败: {e}")
    
    def _send_quality_alerts(self, issues: List[Dict[str, Any]]):
        """发送数据质量告警"""
        try:
            for issue in issues:
                severity = issue.get('severity', 'medium')
                level = 'high' if severity == 'critical' else 'medium'
                
                self.alert_system.create_alert(
                    alert_type='data_quality',
                    level=level,
                    title=f"数据质量问题: {issue.get('table', '未知表')}",
                    message=issue.get('description', '数据质量检查发现问题'),
                    source='data_quality_checker',
                    metadata=issue
                )
                
        except Exception as e:
            self.logger.error(f"发送数据质量告警失败: {e}")
    
    def get_system_overview(self) -> Dict[str, Any]:
        """获取系统概览"""
        try:
            overview = {
                'timestamp': datetime.now().isoformat(),
                'monitoring_active': self.monitoring_active,
                'modules': {}
            }
            
            # 监控仪表板状态
            try:
                dashboard_data = self.monitoring_dashboard.get_dashboard_data()
                overview['modules']['monitoring'] = {
                    'status': 'active',
                    'health_score': dashboard_data.get('health_score', 0),
                    'last_update': dashboard_data.get('last_update', '')
                }
            except Exception as e:
                overview['modules']['monitoring'] = {
                    'status': 'error',
                    'error': str(e)
                }
            
            # 数据质量状态
            try:
                quality_summary = self.data_quality_checker.get_quality_summary()
                overview['modules']['data_quality'] = {
                    'status': 'active',
                    'overall_score': quality_summary.get('overall_score', 0),
                    'issues_count': len(quality_summary.get('issues', []))
                }
            except Exception as e:
                overview['modules']['data_quality'] = {
                    'status': 'error',
                    'error': str(e)
                }
            
            # 并发调优状态
            try:
                concurrency_config = self.concurrency_tuner.get_current_config()
                overview['modules']['concurrency'] = {
                    'status': 'active',
                    'max_workers': concurrency_config.get('max_workers', 0),
                    'monitoring_active': self.concurrency_tuner.monitoring_active
                }
            except Exception as e:
                overview['modules']['concurrency'] = {
                    'status': 'error',
                    'error': str(e)
                }
            
            # 组件更新状态
            try:
                component_status = self.component_updater.get_component_status()
                components = component_status.get('components', {})
                updates_available = sum(1 for comp in components.values() 
                                      if comp.get('update_available', False))
                
                overview['modules']['components'] = {
                    'status': 'active',
                    'total_components': len(components),
                    'updates_available': updates_available,
                    'auto_check_active': component_status.get('auto_check_active', False)
                }
            except Exception as e:
                overview['modules']['components'] = {
                    'status': 'error',
                    'error': str(e)
                }
            
            # 告警系统状态
            try:
                alert_stats = self.alert_system.get_alert_statistics()
                overview['modules']['alerts'] = {
                    'status': 'active',
                    'active_alerts': alert_stats.get('active_alerts', 0),
                    'total_alerts_today': alert_stats.get('total_today', 0)
                }
            except Exception as e:
                overview['modules']['alerts'] = {
                    'status': 'error',
                    'error': str(e)
                }
            
            return overview
            
        except Exception as e:
            self.logger.error(f"获取系统概览失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def run_health_check(self) -> Dict[str, Any]:
        """运行系统健康检查"""
        self.logger.info("开始系统健康检查...")
        
        health_report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'checks': {}
        }
        
        issues_count = 0
        
        # 检查监控仪表板
        try:
            dashboard_health = self.monitoring_dashboard.get_system_health()
            health_report['checks']['monitoring'] = {
                'status': 'pass' if dashboard_health.get('healthy', False) else 'fail',
                'details': dashboard_health
            }
            if not dashboard_health.get('healthy', False):
                issues_count += 1
        except Exception as e:
            health_report['checks']['monitoring'] = {
                'status': 'error',
                'error': str(e)
            }
            issues_count += 1
        
        # 检查数据质量
        try:
            quality_issues = self.data_quality_checker.check_all_tables()
            critical_issues = [i for i in quality_issues if i.get('severity') == 'critical']
            
            health_report['checks']['data_quality'] = {
                'status': 'pass' if not critical_issues else 'fail',
                'total_issues': len(quality_issues),
                'critical_issues': len(critical_issues),
                'details': quality_issues[:5]  # 只显示前5个问题
            }
            
            if critical_issues:
                issues_count += len(critical_issues)
                
        except Exception as e:
            health_report['checks']['data_quality'] = {
                'status': 'error',
                'error': str(e)
            }
            issues_count += 1
        
        # 检查并发性能
        try:
            performance_report = self.concurrency_tuner.get_performance_report(1)
            
            if 'error' in performance_report:
                health_report['checks']['concurrency'] = {
                    'status': 'warning',
                    'message': '没有性能数据'
                }
            else:
                avg_response_time = performance_report.get('api_response_time', {}).get('avg', 0)
                avg_success_rate = performance_report.get('api_success_rate', {}).get('avg', 100)
                
                status = 'pass'
                if avg_response_time > 5000 or avg_success_rate < 95:
                    status = 'warning'
                    issues_count += 1
                
                health_report['checks']['concurrency'] = {
                    'status': status,
                    'avg_response_time': avg_response_time,
                    'avg_success_rate': avg_success_rate
                }
                
        except Exception as e:
            health_report['checks']['concurrency'] = {
                'status': 'error',
                'error': str(e)
            }
            issues_count += 1
        
        # 检查组件状态
        try:
            component_status = self.component_updater.get_component_status()
            components = component_status.get('components', {})
            
            critical_updates = sum(1 for comp in components.values() 
                                 if comp.get('update_available', False) and comp.get('critical', False))
            
            health_report['checks']['components'] = {
                'status': 'warning' if critical_updates > 0 else 'pass',
                'total_components': len(components),
                'critical_updates_available': critical_updates
            }
            
            if critical_updates > 0:
                issues_count += critical_updates
                
        except Exception as e:
            health_report['checks']['components'] = {
                'status': 'error',
                'error': str(e)
            }
            issues_count += 1
        
        # 确定整体状态
        if issues_count == 0:
            health_report['overall_status'] = 'healthy'
        elif issues_count <= 2:
            health_report['overall_status'] = 'warning'
        else:
            health_report['overall_status'] = 'critical'
        
        health_report['issues_count'] = issues_count
        
        self.logger.info(f"健康检查完成，状态: {health_report['overall_status']}, 问题数: {issues_count}")
        
        return health_report
    
    def export_ops_report(self, hours: int = 24) -> str:
        """导出运维报告"""
        self.logger.info(f"生成运维报告 (最近{hours}小时)...")
        
        report = {
            'report_info': {
                'generated_at': datetime.now().isoformat(),
                'period_hours': hours,
                'report_type': 'comprehensive_ops_report'
            },
            'system_overview': self.get_system_overview(),
            'health_check': self.run_health_check()
        }
        
        # 添加各模块详细报告
        try:
            # 监控数据
            report['monitoring_data'] = self.monitoring_dashboard.get_dashboard_data()
        except Exception as e:
            report['monitoring_data'] = {'error': str(e)}
        
        try:
            # 数据质量报告
            report['data_quality'] = self.data_quality_checker.get_quality_summary()
        except Exception as e:
            report['data_quality'] = {'error': str(e)}
        
        try:
            # 性能报告
            report['performance'] = self.concurrency_tuner.get_performance_report(hours)
        except Exception as e:
            report['performance'] = {'error': str(e)}
        
        try:
            # 组件状态
            report['components'] = self.component_updater.get_component_status()
        except Exception as e:
            report['components'] = {'error': str(e)}
        
        try:
            # 告警统计
            report['alerts'] = self.alert_system.get_alert_statistics()
        except Exception as e:
            report['alerts'] = {'error': str(e)}
        
        return json.dumps(report, ensure_ascii=False, indent=2)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PC28系统运维管理工具')
    parser.add_argument('command', choices=[
        'start', 'stop', 'status', 'health', 'overview', 'report', 'module'
    ], help='命令')
    parser.add_argument('--config-dir', type=str, help='配置目录路径')
    parser.add_argument('--modules', nargs='+', 
                       choices=['dashboard', 'quality', 'concurrency', 'components'],
                       help='要启动的监控模块')
    parser.add_argument('--hours', type=int, default=24, help='报告时间范围（小时）')
    parser.add_argument('--module-command', type=str, help='模块子命令')
    parser.add_argument('--module-name', type=str, 
                       choices=['monitoring', 'quality', 'concurrency', 'components', 'alerts'],
                       help='模块名称')
    
    args = parser.parse_args()
    
    # 初始化运维管理器
    try:
        ops_manager = PC28OpsManager(args.config_dir)
    except Exception as e:
        print(f"❌ 初始化运维管理器失败: {e}")
        sys.exit(1)
    
    try:
        if args.command == 'start':
            print("启动PC28运维监控服务...")
            ops_manager.start_monitoring(args.modules)
            print("监控服务已启动，按 Ctrl+C 停止")
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n停止监控服务...")
                ops_manager.stop_monitoring()
                
        elif args.command == 'stop':
            ops_manager.stop_monitoring()
            print("✅ 监控服务已停止")
            
        elif args.command == 'status':
            print("\n=== PC28系统运维状态 ===")
            overview = ops_manager.get_system_overview()
            print(json.dumps(overview, ensure_ascii=False, indent=2))
            
        elif args.command == 'health':
            print("\n=== 系统健康检查 ===")
            health_report = ops_manager.run_health_check()
            
            # 格式化输出
            status_emoji = {
                'healthy': '✅',
                'warning': '⚠️',
                'critical': '❌'
            }
            
            overall_status = health_report.get('overall_status', 'unknown')
            print(f"\n整体状态: {status_emoji.get(overall_status, '❓')} {overall_status.upper()}")
            print(f"问题数量: {health_report.get('issues_count', 0)}")
            
            print("\n详细检查结果:")
            for check_name, check_result in health_report.get('checks', {}).items():
                status = check_result.get('status', 'unknown')
                status_symbol = {'pass': '✅', 'warning': '⚠️', 'fail': '❌', 'error': '💥'}.get(status, '❓')
                print(f"  {status_symbol} {check_name}: {status}")
                
                if 'error' in check_result:
                    print(f"    错误: {check_result['error']}")
            
        elif args.command == 'overview':
            print("\n=== 系统概览 ===")
            overview = ops_manager.get_system_overview()
            
            print(f"监控状态: {'🟢 运行中' if overview.get('monitoring_active') else '🔴 已停止'}")
            print(f"生成时间: {overview.get('timestamp', '')}")
            
            print("\n模块状态:")
            for module_name, module_info in overview.get('modules', {}).items():
                status = module_info.get('status', 'unknown')
                status_symbol = {'active': '🟢', 'error': '🔴', 'warning': '🟡'}.get(status, '❓')
                print(f"  {status_symbol} {module_name}: {status}")
                
                if status == 'error' and 'error' in module_info:
                    print(f"    错误: {module_info['error']}")
            
        elif args.command == 'report':
            print(f"\n生成运维报告 (最近{args.hours}小时)...")
            report = ops_manager.export_ops_report(args.hours)
            
            # 保存报告
            report_file = os.path.join(
                os.path.dirname(__file__), 
                'logs', 
                f'ops_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            
            os.makedirs(os.path.dirname(report_file), exist_ok=True)
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            print(f"✅ 运维报告已生成: {report_file}")
            
            # 显示摘要
            report_data = json.loads(report)
            health_status = report_data.get('health_check', {}).get('overall_status', 'unknown')
            issues_count = report_data.get('health_check', {}).get('issues_count', 0)
            
            print(f"\n报告摘要:")
            print(f"  系统状态: {health_status}")
            print(f"  发现问题: {issues_count} 个")
            
        elif args.command == 'module':
            if not args.module_name:
                print("❌ 使用 module 命令需要指定 --module-name")
                return
            
            # 执行模块特定命令
            if args.module_name == 'monitoring':
                dashboard_data = ops_manager.monitoring_dashboard.get_dashboard_data()
                print(json.dumps(dashboard_data, ensure_ascii=False, indent=2))
                
            elif args.module_name == 'quality':
                if args.module_command == 'check':
                    issues = ops_manager.data_quality_checker.check_all_tables()
                    print(f"发现 {len(issues)} 个数据质量问题")
                    for issue in issues[:10]:  # 显示前10个问题
                        print(f"  - {issue.get('table', 'unknown')}: {issue.get('description', '')}")
                else:
                    summary = ops_manager.data_quality_checker.get_quality_summary()
                    print(json.dumps(summary, ensure_ascii=False, indent=2))
                    
            elif args.module_name == 'concurrency':
                config = ops_manager.concurrency_tuner.get_current_config()
                print("当前并发配置:")
                for key, value in config.items():
                    print(f"  {key}: {value}")
                    
            elif args.module_name == 'components':
                if args.module_command == 'check':
                    updates = ops_manager.component_updater.check_updates()
                    print("组件更新检查结果:")
                    for name, has_update in updates.items():
                        status = "有更新" if has_update else "最新"
                        print(f"  {name}: {status}")
                else:
                    status = ops_manager.component_updater.get_component_status()
                    components = status.get('components', {})
                    print(f"组件总数: {len(components)}")
                    updates_available = sum(1 for comp in components.values() 
                                          if comp.get('update_available', False))
                    print(f"可用更新: {updates_available}")
                    
            elif args.module_name == 'alerts':
                stats = ops_manager.alert_system.get_alert_statistics()
                print("告警统计:")
                print(json.dumps(stats, ensure_ascii=False, indent=2))
    
    except KeyboardInterrupt:
        print("\nPC28运维管理工具已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()