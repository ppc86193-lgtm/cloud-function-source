#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统启动脚本
用于部署和启动完整的PC28回填与实时开奖系统
"""

import os
import sys
import time
import signal
import logging
import argparse
from pathlib import Path
from typing import Optional

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from system_integration_manager import SystemIntegrationManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pc28_system_startup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PC28SystemLauncher:
    """
    PC28系统启动器
    负责系统的启动、监控和优雅关闭
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        初始化系统启动器
        
        Args:
            config_file: 配置文件路径
        """
        self.config = self._load_config(config_file)
        self.system_manager = None
        self.shutdown_requested = False
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("PC28系统启动器初始化完成")
    
    def _load_config(self, config_file: Optional[str]) -> dict:
        """
        加载系统配置
        
        Args:
            config_file: 配置文件路径
            
        Returns:
            配置字典
        """
        default_config = {
            'appid': '45928',
            'secret_key': 'ca9edbfee35c22a0d6c4cf6722506af0',
            'realtime_fetch_interval': 30,
            'health_check_interval': 60,
            'metrics_collection_interval': 300,
            'auto_backfill_enabled': True,
            'auto_backfill_days': 7,
            'alert_thresholds': {
                'error_rate': 5.0,
                'response_time': 5000,
                'memory_usage': 1024,
                'cpu_usage': 80.0
            },
            'notification_channels': ['console', 'log'],
            'data_retention_days': 30,
            'log_level': 'INFO'
        }
        
        if config_file and os.path.exists(config_file):
            try:
                import json
                with open(config_file, 'r', encoding='utf-8') as f:
                    file_config = json.load(f)
                    default_config.update(file_config)
                    logger.info(f"已加载配置文件: {config_file}")
            except Exception as e:
                logger.error(f"加载配置文件失败: {e}，使用默认配置")
        
        return default_config
    
    def _signal_handler(self, signum, frame):
        """
        信号处理器
        
        Args:
            signum: 信号编号
            frame: 当前栈帧
        """
        logger.info(f"收到信号 {signum}，开始优雅关闭系统")
        self.shutdown_requested = True
        
        if self.system_manager:
            self.system_manager.stop_system()
    
    def _check_dependencies(self) -> bool:
        """
        检查系统依赖
        
        Returns:
            依赖检查是否通过
        """
        logger.info("检查系统依赖")
        
        required_modules = [
            'requests',
            'sqlite3',
            'threading',
            'schedule'
        ]
        
        optional_modules = [
            'psutil'  # 用于系统资源监控
        ]
        
        missing_required = []
        missing_optional = []
        
        # 检查必需模块
        for module in required_modules:
            try:
                __import__(module)
                logger.debug(f"✓ {module} 已安装")
            except ImportError:
                missing_required.append(module)
                logger.error(f"✗ {module} 未安装")
        
        # 检查可选模块
        for module in optional_modules:
            try:
                __import__(module)
                logger.debug(f"✓ {module} 已安装")
            except ImportError:
                missing_optional.append(module)
                logger.warning(f"! {module} 未安装（可选）")
        
        if missing_required:
            logger.error(f"缺少必需依赖: {missing_required}")
            logger.error("请安装缺少的依赖后重试")
            return False
        
        if missing_optional:
            logger.warning(f"缺少可选依赖: {missing_optional}")
            logger.warning("系统将以有限功能运行")
        
        logger.info("依赖检查完成")
        return True
    
    def _setup_environment(self):
        """
        设置运行环境
        """
        logger.info("设置运行环境")
        
        # 创建必要的目录
        directories = [
            'logs',
            'data',
            'cache',
            'backups'
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"创建目录: {directory}")
        
        # 设置日志级别
        log_level = self.config.get('log_level', 'INFO')
        logging.getLogger().setLevel(getattr(logging, log_level.upper()))
        
        logger.info("运行环境设置完成")
    
    def _test_api_connectivity(self) -> bool:
        """
        测试API连接性
        
        Returns:
            API连接测试是否通过
        """
        logger.info("测试API连接性")
        
        try:
            from pc28_upstream_api import PC28UpstreamAPI
            
            api = PC28UpstreamAPI(
                appid=self.config['appid'],
                secret_key=self.config['secret_key']
            )
            
            # 测试连接
            if api.test_connection():
                logger.info("✓ API连接测试通过")
                return True
            else:
                logger.error("✗ API连接测试失败")
                return False
                
        except Exception as e:
            logger.error(f"API连接测试异常: {e}")
            return False
    
    def _perform_system_checks(self) -> bool:
        """
        执行系统检查
        
        Returns:
            系统检查是否通过
        """
        logger.info("执行系统检查")
        
        checks = [
            ("依赖检查", self._check_dependencies),
            ("API连接测试", self._test_api_connectivity)
        ]
        
        for check_name, check_func in checks:
            logger.info(f"执行 {check_name}")
            if not check_func():
                logger.error(f"{check_name} 失败")
                return False
            logger.info(f"{check_name} 通过")
        
        logger.info("所有系统检查通过")
        return True
    
    def start_system(self, skip_checks: bool = False) -> bool:
        """
        启动系统
        
        Args:
            skip_checks: 是否跳过系统检查
            
        Returns:
            系统启动是否成功
        """
        logger.info("开始启动PC28系统")
        
        try:
            # 设置运行环境
            self._setup_environment()
            
            # 执行系统检查
            if not skip_checks:
                if not self._perform_system_checks():
                    logger.error("系统检查失败，启动中止")
                    return False
            
            # 创建系统管理器
            logger.info("创建系统管理器")
            self.system_manager = SystemIntegrationManager(self.config)
            
            # 添加自定义通知处理器
            self._setup_notification_handlers()
            
            # 启动系统
            logger.info("启动系统管理器")
            if self.system_manager.start_system():
                logger.info("✓ PC28系统启动成功")
                return True
            else:
                logger.error("✗ PC28系统启动失败")
                return False
                
        except Exception as e:
            logger.error(f"系统启动异常: {e}")
            return False
    
    def _setup_notification_handlers(self):
        """
        设置通知处理器
        """
        def console_notification_handler(notification):
            """控制台通知处理器"""
            timestamp = notification.get('timestamp', '')
            title = notification.get('title', '')
            message = notification.get('message', '')
            print(f"\n[{timestamp}] {title}: {message}")
        
        def file_notification_handler(notification):
            """文件通知处理器"""
            try:
                with open('notifications.log', 'a', encoding='utf-8') as f:
                    import json
                    f.write(json.dumps(notification, ensure_ascii=False) + '\n')
            except Exception as e:
                logger.error(f"写入通知文件失败: {e}")
        
        # 添加通知处理器
        if self.system_manager:
            self.system_manager.add_notification_handler(console_notification_handler)
            self.system_manager.add_notification_handler(file_notification_handler)
    
    def run_system(self):
        """
        运行系统主循环
        """
        logger.info("进入系统主循环")
        
        try:
            while not self.shutdown_requested:
                # 显示系统状态
                self._display_system_status()
                
                # 等待一段时间
                time.sleep(30)
                
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号")
            self.shutdown_requested = True
        
        except Exception as e:
            logger.error(f"系统运行异常: {e}")
            self.shutdown_requested = True
        
        finally:
            logger.info("退出系统主循环")
    
    def _display_system_status(self):
        """
        显示系统状态
        """
        if not self.system_manager:
            return
        
        try:
            status = self.system_manager.get_system_status()
            
            # 清屏（可选）
            # os.system('clear' if os.name == 'posix' else 'cls')
            
            print("\n" + "="*60)
            print(f"PC28系统状态监控 - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60)
            
            # 系统整体状态
            print(f"系统状态: {status['system_status']}")
            print(f"运行时间: {status['uptime_seconds']:.0f}秒 ({status['uptime_seconds']/3600:.1f}小时)")
            print(f"活跃服务: {', '.join(status['active_services'])}")
            
            # 系统指标
            metrics = status['system_metrics']
            print(f"\n系统指标:")
            print(f"  处理数据总量: {metrics['total_data_processed']}")
            print(f"  错误计数: {metrics['error_count']}")
            if metrics['memory_usage_mb'] > 0:
                print(f"  内存使用: {metrics['memory_usage_mb']:.1f}MB")
            if metrics['cpu_usage_percent'] > 0:
                print(f"  CPU使用率: {metrics['cpu_usage_percent']:.1f}%")
            
            # 服务健康状态
            print(f"\n服务健康状态:")
            for service_name, health in status['service_health'].items():
                status_icon = {
                    'healthy': '✓',
                    'degraded': '⚠',
                    'unhealthy': '✗',
                    'error': '✗'
                }.get(health['status'], '?')
                
                print(f"  {status_icon} {service_name}: {health['status']} "
                      f"(成功率: {health['success_rate']:.1f}%, "
                      f"响应时间: {health['response_time_ms']:.1f}ms)")
            
            print("="*60)
            
        except Exception as e:
            logger.error(f"显示系统状态失败: {e}")
    
    def stop_system(self):
        """
        停止系统
        """
        logger.info("停止PC28系统")
        
        if self.system_manager:
            self.system_manager.stop_system()
            self.system_manager = None
        
        logger.info("PC28系统已停止")
    
    def create_backfill_task(self, start_date: str, end_date: str, mode: str = "smart") -> Optional[str]:
        """
        创建回填任务
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            mode: 回填模式 (smart/incremental/full)
            
        Returns:
            任务ID或None
        """
        if not self.system_manager:
            logger.error("系统管理器未初始化")
            return None
        
        return self.system_manager.create_manual_backfill_task(start_date, end_date, mode)

def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description='PC28系统启动器')
    parser.add_argument('--config', '-c', help='配置文件路径')
    parser.add_argument('--skip-checks', action='store_true', help='跳过系统检查')
    parser.add_argument('--daemon', '-d', action='store_true', help='以守护进程模式运行')
    parser.add_argument('--backfill', help='创建回填任务 (格式: start_date,end_date,mode)')
    parser.add_argument('--test-only', action='store_true', help='仅执行测试，不启动系统')
    
    args = parser.parse_args()
    
    # 创建启动器
    launcher = PC28SystemLauncher(args.config)
    
    try:
        if args.test_only:
            # 仅执行测试
            logger.info("执行系统测试")
            if launcher._perform_system_checks():
                logger.info("✓ 所有测试通过")
                return 0
            else:
                logger.error("✗ 测试失败")
                return 1
        
        # 启动系统
        if launcher.start_system(skip_checks=args.skip_checks):
            logger.info("系统启动成功")
            
            # 处理回填任务
            if args.backfill:
                try:
                    parts = args.backfill.split(',')
                    if len(parts) >= 2:
                        start_date = parts[0]
                        end_date = parts[1]
                        mode = parts[2] if len(parts) > 2 else 'smart'
                        
                        task_id = launcher.create_backfill_task(start_date, end_date, mode)
                        if task_id:
                            logger.info(f"回填任务创建成功: {task_id}")
                        else:
                            logger.error("回填任务创建失败")
                    else:
                        logger.error("回填参数格式错误，应为: start_date,end_date[,mode]")
                except Exception as e:
                    logger.error(f"创建回填任务失败: {e}")
            
            # 运行系统
            if not args.daemon:
                launcher.run_system()
            else:
                logger.info("以守护进程模式运行")
                # 在守护进程模式下，保持进程运行
                while not launcher.shutdown_requested:
                    time.sleep(60)
            
        else:
            logger.error("系统启动失败")
            return 1
    
    except Exception as e:
        logger.error(f"系统运行异常: {e}")
        return 1
    
    finally:
        # 确保系统正确关闭
        launcher.stop_system()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())