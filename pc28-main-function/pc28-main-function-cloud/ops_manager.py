#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 运维管理系统
提供系统监控、数据质量检查、参数调优和组件更新的统一管理界面
"""

import os
import sys
import json
import argparse
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

from python.monitoring_dashboard import MonitoringDashboard
from python.data_quality_monitor import DataQualityMonitor
from python.api_monitor import APIMonitor

class OpsManager:
    """运维管理器"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config', 'integrated_config.json')
        self.config = self._load_config()
        
        # 设置日志
        self._setup_logging()
        
        # 初始化组件
        self.dashboard = MonitoringDashboard(self.config)
        self.data_quality_monitor = DataQualityMonitor(self.config)
        self.api_monitor = APIMonitor(self.config)
        
        # 运维状态文件
        self.state_file = os.path.join(os.path.dirname(__file__), 'ops_state.json')
        self.ops_state = self._load_ops_state()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"配置文件加载失败: {e}")
            return {}
    
    def _setup_logging(self):
        """设置日志"""
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
    
    def _load_ops_state(self) -> Dict[str, Any]:
        """加载运维状态"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.warning(f"运维状态文件加载失败: {e}")
        
        return {
            'last_health_check': None,
            'last_data_quality_check': None,
            'last_parameter_tuning': None,
            'last_component_update': None,
            'monitoring_enabled': True,
            'alert_thresholds': {
                'error_rate': 0.1,
                'response_time': 5000,
                'data_quality_score': 0.8
            },
            'concurrency_params': {
                'max_workers': 10,
                'batch_size': 100,
                'timeout': 30
            }
        }
    
    def _save_ops_state(self):
        """保存运维状态"""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.ops_state, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"运维状态保存失败: {e}")
    
    def run_system_monitoring(self, continuous: bool = False, interval: int = 300) -> Dict[str, Any]:
        """运行系统监控"""
        print("\n=== 系统监控启动 ===")
        
        if continuous:
            print(f"持续监控模式，检查间隔: {interval}秒")
            try:
                while True:
                    health_status = self.dashboard.run_comprehensive_health_check()
                    self._display_health_status(health_status)
                    
                    # 检查告警
                    self._check_and_send_alerts(health_status)
                    
                    # 更新状态
                    self.ops_state['last_health_check'] = datetime.now().isoformat()
                    self._save_ops_state()
                    
                    print(f"\n下次检查时间: {(datetime.now() + timedelta(seconds=interval)).strftime('%H:%M:%S')}")
                    time.sleep(interval)
            except KeyboardInterrupt:
                print("\n监控已停止")
        else:
            health_status = self.dashboard.run_comprehensive_health_check()
            self._display_health_status(health_status)
            
            # 更新状态
            self.ops_state['last_health_check'] = datetime.now().isoformat()
            self._save_ops_state()
            
            return health_status.__dict__
    
    def _display_health_status(self, health_status):
        """显示健康状态"""
        print(f"\n【{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}】系统健康检查")
        print(f"总体状态: {self._get_status_emoji(health_status.overall_status)} {health_status.overall_status.upper()}")
        print(f"运行时间: {health_status.uptime_percentage:.1f}%")
        print(f"错误率: {health_status.error_rate:.1%}")
        
        print("\nAPI状态:")
        for api_type, status in health_status.api_status.items():
            print(f"  {api_type}: {self._get_status_emoji(status)} {status}")
        
        print("\n数据质量:")
        for source, status in health_status.data_quality_status.items():
            print(f"  {source}: {self._get_status_emoji(status)} {status}")
        
        if health_status.active_alerts:
            print("\n⚠️  活跃告警:")
            for alert in health_status.active_alerts:
                print(f"  - {alert}")
        
        print("\n性能指标:")
        metrics = health_status.performance_metrics
        print(f"  平均响应时间: {metrics['avg_api_response_time']:.0f}ms")
        print(f"  数据处理时间: {metrics['avg_data_processing_time']:.0f}ms")
        print(f"  每小时请求数: {metrics['requests_per_hour']:.0f}")
    
    def _get_status_emoji(self, status: str) -> str:
        """获取状态表情符号"""
        status_map = {
            'healthy': '✅',
            'good': '✅',
            'degraded': '⚠️',
            'warning': '⚠️',
            'unhealthy': '❌',
            'critical': '❌',
            'error': '❌'
        }
        return status_map.get(status.lower(), '❓')
    
    def run_data_quality_check(self, hours: int = 24) -> Dict[str, Any]:
        """运行数据质量检查"""
        print(f"\n=== 数据质量检查 (最近{hours}小时) ===")
        
        try:
            # 获取数据质量摘要
            quality_summary = self.data_quality_monitor.get_quality_summary(hours)
            
            print("\n数据质量报告:")
            for source, summary in quality_summary.items():
                status_emoji = self._get_status_emoji(summary['status'])
                print(f"\n{source.upper()}:")
                print(f"  状态: {status_emoji} {summary['status'].upper()}")
                print(f"  质量评分: {summary['latest_score']:.3f}")
                print(f"  检查次数: {summary['check_count']}")
                
                if 'issues' in summary and summary['issues']:
                    print("  发现问题:")
                    for issue in summary['issues'][:5]:  # 显示前5个问题
                        print(f"    - {issue}")
            
            # 更新状态
            self.ops_state['last_data_quality_check'] = datetime.now().isoformat()
            self._save_ops_state()
            
            return quality_summary
            
        except Exception as e:
            self.logger.error(f"数据质量检查失败: {e}")
            print(f"❌ 数据质量检查失败: {e}")
            return {}
    
    def tune_concurrency_parameters(self, auto_adjust: bool = True) -> Dict[str, Any]:
        """调优并发参数"""
        print("\n=== 并发参数调优 ===")
        
        try:
            current_params = self.ops_state['concurrency_params']
            print(f"当前参数: {json.dumps(current_params, ensure_ascii=False, indent=2)}")
            
            if auto_adjust:
                # 获取性能指标
                dashboard_data = self.dashboard.get_dashboard_data(hours=1)
                current_status = dashboard_data['current_status']
                
                # 基于性能指标调整参数
                new_params = self._calculate_optimal_params(current_status, current_params)
                
                if new_params != current_params:
                    print("\n建议的参数调整:")
                    for key, value in new_params.items():
                        if current_params[key] != value:
                            print(f"  {key}: {current_params[key]} → {value}")
                    
                    # 更新参数
                    self.ops_state['concurrency_params'] = new_params
                    self._apply_concurrency_params(new_params)
                    
                    print("\n✅ 参数已更新")
                else:
                    print("\n✅ 当前参数已是最优配置")
            
            # 更新状态
            self.ops_state['last_parameter_tuning'] = datetime.now().isoformat()
            self._save_ops_state()
            
            return self.ops_state['concurrency_params']
            
        except Exception as e:
            self.logger.error(f"参数调优失败: {e}")
            print(f"❌ 参数调优失败: {e}")
            return {}
    
    def _calculate_optimal_params(self, current_status: Dict[str, Any], current_params: Dict[str, Any]) -> Dict[str, Any]:
        """计算最优参数"""
        new_params = current_params.copy()
        
        # 基于错误率调整
        error_rate = current_status.get('error_rate', 0)
        if error_rate > 0.1:  # 错误率过高，减少并发
            new_params['max_workers'] = max(1, int(current_params['max_workers'] * 0.8))
            new_params['batch_size'] = max(10, int(current_params['batch_size'] * 0.8))
        elif error_rate < 0.01:  # 错误率很低，可以增加并发
            new_params['max_workers'] = min(20, int(current_params['max_workers'] * 1.2))
            new_params['batch_size'] = min(200, int(current_params['batch_size'] * 1.2))
        
        # 基于响应时间调整
        avg_response_time = current_status.get('performance_metrics', {}).get('avg_api_response_time', 0)
        if avg_response_time > 3000:  # 响应时间过长
            new_params['timeout'] = min(60, current_params['timeout'] + 10)
        elif avg_response_time < 1000:  # 响应时间很快
            new_params['timeout'] = max(10, current_params['timeout'] - 5)
        
        return new_params
    
    def _apply_concurrency_params(self, params: Dict[str, Any]):
        """应用并发参数"""
        # 更新配置文件
        try:
            config_file = os.path.join(os.path.dirname(__file__), 'config', 'runtime_config.json')
            runtime_config = {
                'concurrency': params,
                'updated_at': datetime.now().isoformat()
            }
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(runtime_config, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"并发参数已更新: {params}")
            
        except Exception as e:
            self.logger.error(f"参数应用失败: {e}")
    
    def check_component_updates(self) -> Dict[str, Any]:
        """检查组件更新"""
        print("\n=== 组件更新检查 ===")
        
        try:
            components = {
                'python_dependencies': self._check_python_dependencies(),
                'config_files': self._check_config_files(),
                'monitoring_scripts': self._check_monitoring_scripts()
            }
            
            update_available = False
            for component, status in components.items():
                print(f"\n{component.replace('_', ' ').title()}:")
                if status['needs_update']:
                    print(f"  ⚠️  需要更新: {status['reason']}")
                    update_available = True
                else:
                    print(f"  ✅ 最新版本")
                
                if 'version' in status:
                    print(f"  当前版本: {status['version']}")
            
            # 更新状态
            self.ops_state['last_component_update'] = datetime.now().isoformat()
            self._save_ops_state()
            
            if update_available:
                print("\n💡 建议运行更新命令: python ops_manager.py update-components")
            
            return components
            
        except Exception as e:
            self.logger.error(f"组件更新检查失败: {e}")
            print(f"❌ 组件更新检查失败: {e}")
            return {}
    
    def _check_python_dependencies(self) -> Dict[str, Any]:
        """检查Python依赖"""
        requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
        if not os.path.exists(requirements_file):
            return {'needs_update': False, 'reason': '无requirements.txt文件'}
        
        # 简化检查，实际应该比较版本
        return {
            'needs_update': False,
            'reason': '依赖检查需要实现',
            'version': 'unknown'
        }
    
    def _check_config_files(self) -> Dict[str, Any]:
        """检查配置文件"""
        config_dir = os.path.join(os.path.dirname(__file__), 'config')
        if not os.path.exists(config_dir):
            return {'needs_update': True, 'reason': '配置目录不存在'}
        
        return {
            'needs_update': False,
            'reason': '配置文件正常'
        }
    
    def _check_monitoring_scripts(self) -> Dict[str, Any]:
        """检查监控脚本"""
        python_dir = os.path.join(os.path.dirname(__file__), 'python')
        required_files = [
            'monitoring_dashboard.py',
            'data_quality_monitor.py',
            'api_monitor.py'
        ]
        
        missing_files = []
        for file in required_files:
            if not os.path.exists(os.path.join(python_dir, file)):
                missing_files.append(file)
        
        if missing_files:
            return {
                'needs_update': True,
                'reason': f'缺少文件: {", ".join(missing_files)}'
            }
        
        return {
            'needs_update': False,
            'reason': '监控脚本完整'
        }
    
    def _check_and_send_alerts(self, health_status):
        """检查并发送告警"""
        thresholds = self.ops_state['alert_thresholds']
        
        # 检查错误率告警
        if health_status.error_rate > thresholds['error_rate']:
            self._send_alert(f"错误率告警: {health_status.error_rate:.1%} > {thresholds['error_rate']:.1%}")
        
        # 检查响应时间告警
        avg_response_time = health_status.performance_metrics.get('avg_api_response_time', 0)
        if avg_response_time > thresholds['response_time']:
            self._send_alert(f"响应时间告警: {avg_response_time:.0f}ms > {thresholds['response_time']}ms")
    
    def _send_alert(self, message: str):
        """发送告警"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        alert_message = f"[{timestamp}] PC28系统告警: {message}"
        
        # 记录到日志
        self.logger.warning(alert_message)
        
        # 输出到控制台
        print(f"🚨 {alert_message}")
        
        # 这里可以添加其他告警方式，如邮件、短信等
    
    def generate_ops_report(self, hours: int = 24) -> str:
        """生成运维报告"""
        print(f"\n=== 生成运维报告 (最近{hours}小时) ===")
        
        try:
            # 获取系统健康报告
            health_report = self.dashboard.generate_health_report(hours)
            
            # 获取数据质量摘要
            quality_summary = self.data_quality_monitor.get_quality_summary(hours)
            
            # 生成综合报告
            report = f"""
=== PC28 系统运维报告 ===
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
报告周期: 最近 {hours} 小时

{health_report}

【运维状态】
- 最后健康检查: {self.ops_state.get('last_health_check', '未执行')}
- 最后数据质量检查: {self.ops_state.get('last_data_quality_check', '未执行')}
- 最后参数调优: {self.ops_state.get('last_parameter_tuning', '未执行')}
- 最后组件更新检查: {self.ops_state.get('last_component_update', '未执行')}

【当前配置】
并发参数: {json.dumps(self.ops_state['concurrency_params'], ensure_ascii=False, indent=2)}
告警阈值: {json.dumps(self.ops_state['alert_thresholds'], ensure_ascii=False, indent=2)}

报告结束
"""
            
            # 保存报告
            report_file = os.path.join(
                os.path.dirname(__file__), 
                'logs', 
                f'ops_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            )
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            print(f"✅ 报告已保存: {report_file}")
            return report
            
        except Exception as e:
            self.logger.error(f"报告生成失败: {e}")
            print(f"❌ 报告生成失败: {e}")
            return ""

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PC28 运维管理系统')
    parser.add_argument('command', choices=[
        'monitor', 'data-quality', 'tune-params', 'check-updates', 
        'report', 'status', 'continuous-monitor'
    ], help='运维命令')
    parser.add_argument('--hours', type=int, default=24, help='时间范围（小时）')
    parser.add_argument('--interval', type=int, default=300, help='持续监控间隔（秒）')
    parser.add_argument('--config', type=str, help='配置文件路径')
    
    args = parser.parse_args()
    
    # 初始化运维管理器
    ops_manager = OpsManager(args.config)
    
    try:
        if args.command == 'monitor':
            ops_manager.run_system_monitoring()
        elif args.command == 'continuous-monitor':
            ops_manager.run_system_monitoring(continuous=True, interval=args.interval)
        elif args.command == 'data-quality':
            ops_manager.run_data_quality_check(args.hours)
        elif args.command == 'tune-params':
            ops_manager.tune_concurrency_parameters()
        elif args.command == 'check-updates':
            ops_manager.check_component_updates()
        elif args.command == 'report':
            report = ops_manager.generate_ops_report(args.hours)
            print(report)
        elif args.command == 'status':
            print("\n=== 运维系统状态 ===")
            print(json.dumps(ops_manager.ops_state, ensure_ascii=False, indent=2, default=str))
    
    except KeyboardInterrupt:
        print("\n运维管理器已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()