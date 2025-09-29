#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28本地化自动修复自愈系统主控制器
整合所有系统组件，提供统一的管理接口
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, asdict
import argparse

# 导入本地系统组件
from local_database import LocalDatabase, get_local_db
from local_sql_engine import LocalSQLEngine
from auto_repair_system import AutoRepairSystem
from cloud_sync_manager import CloudSyncManager
from monitoring_alerting_system import MonitoringAlertingSystem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SystemStatus:
    """系统状态数据类"""
    timestamp: str
    overall_health: str  # 'healthy', 'degraded', 'critical', 'offline'
    components: Dict[str, Dict[str, Any]]
    active_alerts: int
    auto_repair_enabled: bool
    monitoring_enabled: bool
    sync_enabled: bool

class PC28SelfHealingSystem:
    """PC28本地化自动修复自愈系统主控制器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """初始化自愈系统"""
        self.config_path = config_path or "/Users/a606/cloud_function_source/local_system/system_config.json"
        self.config = self._load_config()
        
        # 初始化系统组件
        self.db = get_local_db()
        self.api_collector = LocalAPICollector()
        self.sql_engine = LocalSQLEngine()
        self.repair_system = AutoRepairSystem()
        self.sync_manager = CloudSyncManager()
        self.monitoring_system = MonitoringAlertingSystem()
        
        # 系统状态
        self.is_running = False
        self.main_thread = None
        self.last_health_check = None
        
        # 初始化系统
        self._init_system()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载系统配置"""
        default_config = {
            "system": {
                "name": "PC28自愈系统",
                "version": "1.0.0",
                "auto_start": True,
                "health_check_interval_seconds": 300,  # 5分钟
                "auto_repair_enabled": True,
                "monitoring_enabled": True,
                "sync_enabled": True
            },
            "data_collection": {
                "enabled": True,
                "interval_seconds": 300,  # 5分钟
                "retry_attempts": 3,
                "timeout_seconds": 30
            },
            "auto_repair": {
                "enabled": True,
                "max_repair_attempts": 3,
                "repair_cooldown_minutes": 15,
                "critical_issues_only": False
            },
            "monitoring": {
                "enabled": True,
                "alert_threshold": {
                    "data_age_hours": 2,
                    "error_rate_percent": 5,
                    "memory_usage_percent": 85
                }
            },
            "sync": {
                "enabled": True,
                "auto_sync_interval_minutes": 30,
                "sync_on_repair": True,
                "bidirectional_tables": ["runtime_params", "cloud_pred_today_norm"]
            },
            "logging": {
                "level": "INFO",
                "file_path": "/Users/a606/cloud_function_source/local_system/logs/system.log",
                "max_file_size_mb": 100,
                "backup_count": 5
            }
        }
        
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)
                    # 合并配置
                    default_config.update(user_config)
            else:
                # 创建默认配置文件
                os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
                with open(self.config_path, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=2, ensure_ascii=False)
                logger.info(f"创建默认配置文件: {self.config_path}")
            
            return default_config
            
        except Exception as e:
            logger.error(f"加载配置失败: {e}")
            return default_config
    
    def _init_system(self):
        """初始化系统"""
        try:
            logger.info("初始化PC28自愈系统...")
            
            # 创建日志目录
            log_dir = os.path.dirname(self.config['logging']['file_path'])
            os.makedirs(log_dir, exist_ok=True)
            
            # 初始化数据库
            # self.db.init_database()  # 移除这行，因为LocalDatabase在__init__中已经初始化
            
            # 记录系统启动
            self._log_system_event("system_init", "系统初始化完成")
            
            logger.info("PC28自愈系统初始化完成")
            
        except Exception as e:
            logger.error(f"系统初始化失败: {e}")
            raise
    
    def start_system(self):
        """启动自愈系统"""
        if self.is_running:
            logger.warning("系统已在运行中")
            return
        
        try:
            logger.info("启动PC28自愈系统...")
            
            self.is_running = True
            
            # 启动各个子系统
            if self.config['monitoring']['enabled']:
                self.monitoring_system.start_monitoring()
                logger.info("监控系统已启动")
            
            if self.config['sync']['enabled']:
                self.sync_manager.start_auto_sync(
                    self.config['sync']['auto_sync_interval_minutes']
                )
                logger.info("同步系统已启动")
            
            if self.config['data_collection']['enabled']:
                self.api_collector.start_scheduled_collection(
                    self.config['data_collection']['interval_seconds']
                )
                logger.info("数据采集系统已启动")
            
            # 启动主控制循环
            self.main_thread = threading.Thread(target=self._main_control_loop, daemon=True)
            self.main_thread.start()
            
            # 记录系统启动
            self._log_system_event("system_start", "系统启动成功")
            
            logger.info("PC28自愈系统启动完成")
            
        except Exception as e:
            logger.error(f"系统启动失败: {e}")
            self.is_running = False
            raise
    
    def stop_system(self):
        """停止自愈系统"""
        try:
            logger.info("停止PC28自愈系统...")
            
            self.is_running = False
            
            # 停止各个子系统
            self.monitoring_system.stop_monitoring()
            self.sync_manager.stop_auto_sync()
            self.api_collector.stop_scheduled_collection()
            
            # 等待主线程结束
            if self.main_thread and self.main_thread.is_alive():
                self.main_thread.join(timeout=10)
            
            # 记录系统停止
            self._log_system_event("system_stop", "系统停止")
            
            logger.info("PC28自愈系统已停止")
            
        except Exception as e:
            logger.error(f"系统停止失败: {e}")
    
    def _main_control_loop(self):
        """主控制循环"""
        logger.info("主控制循环启动")
        
        while self.is_running:
            try:
                # 执行健康检查
                health_status = self.perform_health_check()
                
                # 如果启用自动修复，处理发现的问题
                if self.config['auto_repair']['enabled'] and health_status['issues']:
                    self._handle_health_issues(health_status['issues'])
                
                # 更新系统状态
                self._update_system_status(health_status)
                
                # 等待下次检查
                time.sleep(self.config['system']['health_check_interval_seconds'])
                
            except Exception as e:
                logger.error(f"主控制循环异常: {e}")
                time.sleep(60)  # 异常时等待1分钟后重试
        
        logger.info("主控制循环停止")
    
    def perform_health_check(self) -> Dict[str, Any]:
        """执行系统健康检查"""
        try:
            logger.debug("执行系统健康检查...")
            
            health_status = {
                'timestamp': datetime.now().isoformat(),
                'overall_health': 'healthy',
                'components': {},
                'issues': [],
                'recommendations': []
            }
            
            # 检查数据库连接
            try:
                self.db.test_connection()
                health_status['components']['database'] = {
                    'status': 'healthy',
                    'message': '数据库连接正常'
                }
            except Exception as e:
                health_status['components']['database'] = {
                    'status': 'unhealthy',
                    'message': f'数据库连接异常: {e}'
                }
                health_status['issues'].append({
                    'type': 'database_connection',
                    'severity': 'critical',
                    'message': str(e)
                })
            
            # 检查API连接
            try:
                api_healthy = self.api_collector.test_api_connection()
                health_status['components']['api'] = {
                    'status': 'healthy' if api_healthy else 'unhealthy',
                    'message': 'API连接正常' if api_healthy else 'API连接异常'
                }
                if not api_healthy:
                    health_status['issues'].append({
                        'type': 'api_connection',
                        'severity': 'high',
                        'message': 'PC28上游API连接失败'
                    })
            except Exception as e:
                health_status['components']['api'] = {
                    'status': 'unhealthy',
                    'message': f'API检查异常: {e}'
                }
                health_status['issues'].append({
                    'type': 'api_connection',
                    'severity': 'high',
                    'message': str(e)
                })
            
            # 检查数据新鲜度
            data_freshness = self._check_data_freshness()
            health_status['components']['data_freshness'] = data_freshness
            if data_freshness['status'] != 'healthy':
                health_status['issues'].extend(data_freshness.get('issues', []))
            
            # 检查同步状态
            try:
                sync_report = self.sync_manager.get_sync_status_report()
                sync_healthy = sync_report['summary']['sync_health'] == 'healthy'
                health_status['components']['sync'] = {
                    'status': 'healthy' if sync_healthy else 'degraded',
                    'message': f"同步状态: {sync_report['summary']['sync_health']}",
                    'details': sync_report['summary']
                }
                if not sync_healthy:
                    health_status['issues'].append({
                        'type': 'sync_failure',
                        'severity': 'medium',
                        'message': f"有 {sync_report['summary']['failed_tables']} 个表同步失败"
                    })
            except Exception as e:
                health_status['components']['sync'] = {
                    'status': 'unknown',
                    'message': f'同步状态检查异常: {e}'
                }
            
            # 检查监控系统
            try:
                monitoring_dashboard = self.monitoring_system.get_monitoring_dashboard()
                monitoring_healthy = monitoring_dashboard['monitoring_active']
                health_status['components']['monitoring'] = {
                    'status': 'healthy' if monitoring_healthy else 'offline',
                    'message': '监控系统运行正常' if monitoring_healthy else '监控系统未运行',
                    'active_alerts': len(monitoring_dashboard.get('active_alerts', []))
                }
            except Exception as e:
                health_status['components']['monitoring'] = {
                    'status': 'unknown',
                    'message': f'监控系统检查异常: {e}'
                }
            
            # 计算整体健康状态
            health_status['overall_health'] = self._calculate_overall_health(health_status['components'], health_status['issues'])
            
            self.last_health_check = health_status
            
            logger.debug(f"健康检查完成，整体状态: {health_status['overall_health']}")
            
            return health_status
            
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_health': 'unknown',
                'error': str(e)
            }
    
    def _check_data_freshness(self) -> Dict[str, Any]:
        """检查数据新鲜度"""
        try:
            tables = ['cloud_pred_today_norm', 'signal_pool_union_v3', 'lab_push_candidates_v2']
            threshold_hours = self.config['monitoring']['alert_threshold']['data_age_hours']
            
            freshness_status = {
                'status': 'healthy',
                'message': '数据新鲜度正常',
                'details': {},
                'issues': []
            }
            
            for table in tables:
                try:
                    result = self.db.execute_query(f"""
                        SELECT MAX(created_at) as latest_time, COUNT(*) as row_count
                        FROM {table}
                    """)
                    
                    if result and result[0]['latest_time']:
                        latest_time = datetime.fromisoformat(result[0]['latest_time'])
                        hours_old = (datetime.now() - latest_time).total_seconds() / 3600
                        row_count = result[0]['row_count']
                        
                        freshness_status['details'][table] = {
                            'latest_time': result[0]['latest_time'],
                            'hours_old': hours_old,
                            'row_count': row_count,
                            'status': 'fresh' if hours_old <= threshold_hours else 'stale'
                        }
                        
                        if hours_old > threshold_hours:
                            freshness_status['status'] = 'degraded'
                            freshness_status['issues'].append({
                                'type': 'data_freshness',
                                'severity': 'medium',
                                'message': f'表 {table} 数据已过期 {hours_old:.1f} 小时'
                            })
                    else:
                        freshness_status['details'][table] = {
                            'status': 'empty',
                            'row_count': 0
                        }
                        freshness_status['status'] = 'degraded'
                        freshness_status['issues'].append({
                            'type': 'data_missing',
                            'severity': 'high',
                            'message': f'表 {table} 无数据'
                        })
                        
                except Exception as e:
                    freshness_status['details'][table] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    freshness_status['status'] = 'degraded'
            
            return freshness_status
            
        except Exception as e:
            return {
                'status': 'unknown',
                'message': f'数据新鲜度检查失败: {e}'
            }
    
    def _calculate_overall_health(self, components: Dict[str, Dict], issues: List[Dict]) -> str:
        """计算整体健康状态"""
        try:
            # 检查是否有严重问题
            critical_issues = [i for i in issues if i['severity'] == 'critical']
            if critical_issues:
                return 'critical'
            
            # 检查组件状态
            unhealthy_components = [c for c in components.values() if c['status'] == 'unhealthy']
            if unhealthy_components:
                return 'critical'
            
            # 检查高优先级问题
            high_issues = [i for i in issues if i['severity'] == 'high']
            if high_issues:
                return 'degraded'
            
            # 检查降级组件
            degraded_components = [c for c in components.values() if c['status'] in ['degraded', 'offline']]
            if degraded_components:
                return 'degraded'
            
            # 检查中等优先级问题
            medium_issues = [i for i in issues if i['severity'] == 'medium']
            if len(medium_issues) > 2:
                return 'degraded'
            
            return 'healthy'
            
        except Exception as e:
            logger.error(f"计算整体健康状态失败: {e}")
            return 'unknown'
    
    def _handle_health_issues(self, issues: List[Dict]):
        """处理健康问题"""
        try:
            logger.info(f"处理 {len(issues)} 个健康问题...")
            
            for issue in issues:
                try:
                    # 根据问题类型执行相应的修复操作
                    if issue['type'] == 'database_connection':
                        self._repair_database_connection()
                    elif issue['type'] == 'api_connection':
                        self._repair_api_connection()
                    elif issue['type'] == 'data_freshness':
                        self._repair_data_freshness()
                    elif issue['type'] == 'data_missing':
                        self._repair_missing_data()
                    elif issue['type'] == 'sync_failure':
                        self._repair_sync_failure()
                    else:
                        logger.warning(f"未知问题类型: {issue['type']}")
                        
                except Exception as e:
                    logger.error(f"处理问题失败 {issue['type']}: {e}")
            
        except Exception as e:
            logger.error(f"处理健康问题失败: {e}")
    
    def _repair_database_connection(self):
        """修复数据库连接"""
        try:
            logger.info("尝试修复数据库连接...")
            
            # 重新初始化数据库连接
            self.db = get_local_db()
            self.db.init_database()
            
            # 测试连接
            self.db.test_connection()
            
            logger.info("数据库连接修复成功")
            self._log_system_event("repair_success", "数据库连接修复成功")
            
        except Exception as e:
            logger.error(f"数据库连接修复失败: {e}")
            self._log_system_event("repair_failure", f"数据库连接修复失败: {e}")
    
    def _repair_api_connection(self):
        """修复API连接"""
        try:
            logger.info("尝试修复API连接...")
            
            # 重新初始化API客户端
            self.api_collector = LocalAPICollector()
            
            # 测试连接
            if self.api_collector.test_api_connection():
                logger.info("API连接修复成功")
                self._log_system_event("repair_success", "API连接修复成功")
            else:
                logger.warning("API连接仍然异常")
                self._log_system_event("repair_partial", "API连接修复未完全成功")
            
        except Exception as e:
            logger.error(f"API连接修复失败: {e}")
            self._log_system_event("repair_failure", f"API连接修复失败: {e}")
    
    def _repair_data_freshness(self):
        """修复数据新鲜度问题"""
        try:
            logger.info("尝试修复数据新鲜度问题...")
            
            # 触发数据采集
            collection_result = self.api_collector.collect_and_store_data()
            
            if collection_result['success']:
                logger.info("数据采集成功，数据新鲜度问题已修复")
                self._log_system_event("repair_success", "数据新鲜度修复成功")
            else:
                logger.warning("数据采集失败")
                self._log_system_event("repair_failure", f"数据新鲜度修复失败: {collection_result.get('error', '未知错误')}")
            
        except Exception as e:
            logger.error(f"数据新鲜度修复失败: {e}")
            self._log_system_event("repair_failure", f"数据新鲜度修复失败: {e}")
    
    def _repair_missing_data(self):
        """修复缺失数据问题"""
        try:
            logger.info("尝试修复缺失数据问题...")
            
            # 执行完整的数据流水线
            pipeline_result = self.sql_engine.run_full_pipeline()
            
            if pipeline_result['success']:
                logger.info("数据流水线执行成功，缺失数据问题已修复")
                self._log_system_event("repair_success", "缺失数据修复成功")
            else:
                logger.warning("数据流水线执行失败")
                self._log_system_event("repair_failure", f"缺失数据修复失败: {pipeline_result.get('error', '未知错误')}")
            
        except Exception as e:
            logger.error(f"缺失数据修复失败: {e}")
            self._log_system_event("repair_failure", f"缺失数据修复失败: {e}")
    
    def _repair_sync_failure(self):
        """修复同步失败问题"""
        try:
            logger.info("尝试修复同步失败问题...")
            
            # 执行强制同步
            sync_result = self.sync_manager.force_full_sync()
            
            if sync_result['success_count'] > 0:
                logger.info(f"同步修复成功，{sync_result['success_count']} 个表同步成功")
                self._log_system_event("repair_success", f"同步修复成功: {sync_result['success_count']} 个表")
            else:
                logger.warning("同步修复失败")
                self._log_system_event("repair_failure", "同步修复失败")
            
        except Exception as e:
            logger.error(f"同步修复失败: {e}")
            self._log_system_event("repair_failure", f"同步修复失败: {e}")
    
    def _update_system_status(self, health_status: Dict[str, Any]):
        """更新系统状态"""
        try:
            system_status = SystemStatus(
                timestamp=health_status['timestamp'],
                overall_health=health_status['overall_health'],
                components=health_status['components'],
                active_alerts=len(health_status.get('issues', [])),
                auto_repair_enabled=self.config['auto_repair']['enabled'],
                monitoring_enabled=self.config['monitoring']['enabled'],
                sync_enabled=self.config['sync']['enabled']
            )
            
            # 存储系统状态
            self.db.execute_update("""
                INSERT OR REPLACE INTO system_status 
                (component, status, last_check, details, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, ('overall_system', system_status.overall_health, 
                  system_status.timestamp, json.dumps(asdict(system_status)), 
                  datetime.now().isoformat()))
            
        except Exception as e:
            logger.error(f"更新系统状态失败: {e}")
    
    def _log_system_event(self, event_type: str, message: str, metadata: Optional[Dict] = None):
        """记录系统事件"""
        try:
            self.db.execute_update("""
                INSERT INTO repair_logs (timestamp, component, issue_type, repair_action, status, details)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (datetime.now().isoformat(), 'system', event_type, message, 
                  'success' if 'success' in event_type else 'failed', json.dumps(metadata or {})))
            
        except Exception as e:
            logger.error(f"记录系统事件失败: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            if self.last_health_check:
                return {
                    'system_running': self.is_running,
                    'last_health_check': self.last_health_check,
                    'config': self.config,
                    'uptime_seconds': (datetime.now() - datetime.fromisoformat(self.last_health_check['timestamp'])).total_seconds() if self.last_health_check else 0
                }
            else:
                return {
                    'system_running': self.is_running,
                    'message': '系统尚未执行健康检查',
                    'config': self.config
                }
                
        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")
            return {
                'system_running': self.is_running,
                'error': str(e)
            }
    
    def generate_system_report(self) -> Dict[str, Any]:
        """生成系统报告"""
        try:
            # 获取各子系统报告
            monitoring_report = self.monitoring_system.generate_monitoring_report(24)
            sync_report = self.sync_manager.get_sync_status_report()
            
            # 获取修复历史
            repair_history = self.db.execute_query("""
                SELECT * FROM repair_logs
                WHERE timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT 50
            """, ((datetime.now() - timedelta(hours=24)).isoformat(),))
            
            return {
                'generated_at': datetime.now().isoformat(),
                'system_status': self.get_system_status(),
                'monitoring_report': monitoring_report,
                'sync_report': sync_report,
                'repair_history': repair_history,
                'recommendations': self._generate_system_recommendations()
            }
            
        except Exception as e:
            logger.error(f"生成系统报告失败: {e}")
            return {
                'generated_at': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _generate_system_recommendations(self) -> List[str]:
        """生成系统改进建议"""
        recommendations = []
        
        try:
            if self.last_health_check:
                health_status = self.last_health_check
                
                # 基于健康状态生成建议
                if health_status['overall_health'] == 'critical':
                    recommendations.append("系统处于严重状态，建议立即检查所有组件")
                elif health_status['overall_health'] == 'degraded':
                    recommendations.append("系统性能下降，建议检查相关组件并进行优化")
                
                # 基于问题类型生成建议
                issue_types = [issue['type'] for issue in health_status.get('issues', [])]
                
                if 'api_connection' in issue_types:
                    recommendations.append("API连接异常频繁，建议检查网络配置和API密钥")
                
                if 'data_freshness' in issue_types:
                    recommendations.append("数据新鲜度问题，建议增加数据采集频率")
                
                if 'sync_failure' in issue_types:
                    recommendations.append("同步失败较多，建议检查云端连接和权限配置")
            
            if not recommendations:
                recommendations.append("系统运行良好，继续保持当前配置")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"生成系统建议失败: {e}")
            return ["无法生成建议，请检查系统状态"]

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PC28本地化自动修复自愈系统')
    parser.add_argument('--action', choices=['start', 'stop', 'status', 'health', 'report', 'repair'], 
                       default='status', help='执行动作')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--daemon', action='store_true', help='后台运行')
    
    args = parser.parse_args()
    
    # 初始化系统
    system = PC28SelfHealingSystem(args.config)
    
    if args.action == 'start':
        system.start_system()
        
        if args.daemon:
            print("系统已启动（后台运行）")
        else:
            print("系统已启动，按 Ctrl+C 停止...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                system.stop_system()
                print("系统已停止")
    
    elif args.action == 'stop':
        system.stop_system()
        print("系统已停止")
    
    elif args.action == 'status':
        status = system.get_system_status()
        print(f"系统状态: {json.dumps(status, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'health':
        health = system.perform_health_check()
        print(f"健康检查结果: {json.dumps(health, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'report':
        report = system.generate_system_report()
        print(f"系统报告: {json.dumps(report, indent=2, ensure_ascii=False)}")
    
    elif args.action == 'repair':
        print("执行手动修复...")
        health = system.perform_health_check()
        if health.get('issues'):
            system._handle_health_issues(health['issues'])
            print(f"修复完成，处理了 {len(health['issues'])} 个问题")
        else:
            print("未发现需要修复的问题")

if __name__ == "__main__":
    main()