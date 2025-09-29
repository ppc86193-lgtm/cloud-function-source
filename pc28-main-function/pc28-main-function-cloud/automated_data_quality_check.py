#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化数据质量检查脚本
定期运行数据质量检查，生成报告并发送告警
"""

import os
import sys
import json
import time
import logging
import schedule
from datetime import datetime, timedelta
from typing import Dict, Any, List
from dataclasses import dataclass

# 添加当前目录到路径
sys.path.append(os.path.dirname(__file__))

from data_quality_checker import DataQualityChecker
from alert_notifier import AlertNotifier

@dataclass
class QualityCheckSchedule:
    """数据质量检查调度配置"""
    name: str
    hours_range: int
    frequency: str  # 'hourly', 'daily', 'weekly'
    enabled: bool = True
    alert_threshold: Dict[str, int] = None

class AutomatedDataQualityChecker:
    """自动化数据质量检查器"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config', 'integrated_config.json')
        self.config = self._load_config()
        
        # 设置日志
        self._setup_logging()
        
        # 初始化组件
        self.quality_checker = DataQualityChecker(self.config_path)
        self.alert_notifier = AlertNotifier(self.config_path)
        
        # 检查调度配置
        self.schedules = self._load_check_schedules()
        
        # 运行状态
        self.is_running = False
        self.last_check_results = {}
        
        self.logger.info("自动化数据质量检查器初始化完成")
    
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
        
        log_file = os.path.join(log_dir, f'automated_quality_check_{datetime.now().strftime("%Y%m%d")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def _load_check_schedules(self) -> List[QualityCheckSchedule]:
        """加载检查调度配置"""
        return [
            QualityCheckSchedule(
                name="实时数据质量检查",
                hours_range=1,
                frequency="hourly",
                enabled=True,
                alert_threshold={'critical': 0, 'warning': 5}
            ),
            QualityCheckSchedule(
                name="日常数据质量检查",
                hours_range=24,
                frequency="daily",
                enabled=True,
                alert_threshold={'critical': 0, 'warning': 10}
            ),
            QualityCheckSchedule(
                name="周度数据质量检查",
                hours_range=168,  # 7 * 24
                frequency="weekly",
                enabled=True,
                alert_threshold={'critical': 0, 'warning': 20}
            )
        ]
    
    def run_scheduled_check(self, schedule_config: QualityCheckSchedule):
        """运行调度检查"""
        try:
            self.logger.info(f"开始执行调度检查: {schedule_config.name}")
            
            # 运行数据质量检查
            result = self.quality_checker.run_comprehensive_check(schedule_config.hours_range)
            
            # 保存检查结果
            self.last_check_results[schedule_config.name] = {
                'timestamp': datetime.now().isoformat(),
                'result': result
            }
            
            # 生成报告文件
            report_file = self._save_check_report(schedule_config.name, result)
            
            # 检查是否需要发送告警
            if self._should_send_alert(result, schedule_config.alert_threshold):
                self._send_quality_alert(schedule_config.name, result, report_file)
            
            self.logger.info(f"调度检查完成: {schedule_config.name}")
            
        except Exception as e:
            self.logger.error(f"调度检查失败 {schedule_config.name}: {e}")
            # 发送系统错误告警
            self._send_system_error_alert(schedule_config.name, str(e))
    
    def _save_check_report(self, check_name: str, result: Dict[str, Any]) -> str:
        """保存检查报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = os.path.join(os.path.dirname(__file__), 'logs', 'quality_reports')
        os.makedirs(report_dir, exist_ok=True)
        
        # 保存文本报告
        report_file = os.path.join(report_dir, f'{check_name.replace(" ", "_")}_{timestamp}.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(result.get('report', ''))
        
        # 保存JSON数据
        json_file = os.path.join(report_dir, f'{check_name.replace(" ", "_")}_{timestamp}.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        return report_file
    
    def _should_send_alert(self, result: Dict[str, Any], threshold: Dict[str, int]) -> bool:
        """判断是否需要发送告警"""
        if not threshold:
            return False
        
        metrics = result.get('metrics', {})
        critical_count = metrics.get('critical_issues', 0)
        warning_count = metrics.get('warning_issues', 0)
        
        # 检查是否超过告警阈值
        if critical_count > threshold.get('critical', 0):
            return True
        
        if warning_count > threshold.get('warning', float('inf')):
            return True
        
        return False
    
    def _send_quality_alert(self, check_name: str, result: Dict[str, Any], report_file: str):
        """发送数据质量告警"""
        try:
            metrics = result.get('metrics', {})
            
            alert_message = f"""
🚨 数据质量告警 - {check_name}

检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
检查状态: {result.get('status', 'unknown')}

问题统计:
- 严重问题: {metrics.get('critical_issues', 0)}
- 警告问题: {metrics.get('warning_issues', 0)}
- 检查记录数: {metrics.get('total_records_checked', 0):,}
- 检查表数: {metrics.get('tables_checked', 0)}

详细报告: {report_file}

请及时处理数据质量问题！
"""
            
            self.alert_notifier.send_alert(
                title=f"数据质量告警 - {check_name}",
                message=alert_message,
                severity='warning' if metrics.get('critical_issues', 0) == 0 else 'critical',
                category='data_quality'
            )
            
            self.logger.info(f"数据质量告警已发送: {check_name}")
            
        except Exception as e:
            self.logger.error(f"发送数据质量告警失败: {e}")
    
    def _send_system_error_alert(self, check_name: str, error_message: str):
        """发送系统错误告警"""
        try:
            alert_message = f"""
❌ 数据质量检查系统错误

检查名称: {check_name}
错误时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
错误信息: {error_message}

请检查系统状态和配置！
"""
            
            self.alert_notifier.send_alert(
                title=f"数据质量检查系统错误 - {check_name}",
                message=alert_message,
                severity='critical',
                category='system_error'
            )
            
        except Exception as e:
            self.logger.error(f"发送系统错误告警失败: {e}")
    
    def setup_schedules(self):
        """设置检查调度"""
        for schedule_config in self.schedules:
            if not schedule_config.enabled:
                continue
            
            if schedule_config.frequency == 'hourly':
                schedule.every().hour.do(self.run_scheduled_check, schedule_config)
            elif schedule_config.frequency == 'daily':
                schedule.every().day.at("02:00").do(self.run_scheduled_check, schedule_config)
            elif schedule_config.frequency == 'weekly':
                schedule.every().monday.at("03:00").do(self.run_scheduled_check, schedule_config)
            
            self.logger.info(f"已设置调度: {schedule_config.name} ({schedule_config.frequency})")
    
    def start_monitoring(self):
        """开始监控"""
        self.logger.info("启动自动化数据质量检查监控")
        self.is_running = True
        
        # 设置调度
        self.setup_schedules()
        
        # 立即运行一次实时检查
        realtime_schedule = next((s for s in self.schedules if s.name == "实时数据质量检查"), None)
        if realtime_schedule:
            self.run_scheduled_check(realtime_schedule)
        
        # 开始调度循环
        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次调度
        except KeyboardInterrupt:
            self.logger.info("收到停止信号")
        finally:
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """停止监控"""
        self.logger.info("停止自动化数据质量检查监控")
        self.is_running = False
        schedule.clear()
    
    def get_status(self) -> Dict[str, Any]:
        """获取运行状态"""
        return {
            'is_running': self.is_running,
            'schedules': [{
                'name': s.name,
                'frequency': s.frequency,
                'enabled': s.enabled,
                'hours_range': s.hours_range
            } for s in self.schedules],
            'last_check_results': {
                name: {
                    'timestamp': result['timestamp'],
                    'status': result['result'].get('status'),
                    'issues_found': result['result'].get('metrics', {}).get('issues_found', 0)
                }
                for name, result in self.last_check_results.items()
            }
        }
    
    def run_manual_check(self, hours: int = 24) -> Dict[str, Any]:
        """手动运行检查"""
        self.logger.info(f"手动运行数据质量检查 (最近{hours}小时)")
        
        result = self.quality_checker.run_comprehensive_check(hours)
        
        # 保存报告
        report_file = self._save_check_report("手动检查", result)
        
        print(result.get('report', ''))
        print(f"\n📄 详细报告已保存: {report_file}")
        
        return result

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='自动化数据质量检查器')
    parser.add_argument('command', choices=[
        'start', 'check', 'status'
    ], help='执行命令')
    parser.add_argument('--hours', type=int, default=24, help='检查时间范围（小时）')
    parser.add_argument('--config', type=str, help='配置文件路径')
    
    args = parser.parse_args()
    
    # 初始化检查器
    checker = AutomatedDataQualityChecker(args.config)
    
    try:
        if args.command == 'start':
            print("🚀 启动自动化数据质量检查监控...")
            print("按 Ctrl+C 停止监控")
            checker.start_monitoring()
            
        elif args.command == 'check':
            print(f"🔍 手动运行数据质量检查 (最近{args.hours}小时)...")
            checker.run_manual_check(args.hours)
            
        elif args.command == 'status':
            status = checker.get_status()
            print("📊 自动化数据质量检查器状态:")
            print(json.dumps(status, ensure_ascii=False, indent=2))
    
    except KeyboardInterrupt:
        print("\n自动化数据质量检查器已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()