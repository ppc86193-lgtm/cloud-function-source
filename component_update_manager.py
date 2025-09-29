#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
组件更新管理系统
实现版本管理、自动更新检查和渐进式更新功能
"""

import os
import sys
import json
import time
import logging
import hashlib
import subprocess
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import requests
import shutil

@dataclass
class ComponentVersion:
    """组件版本信息"""
    name: str
    current_version: str
    latest_version: str
    update_available: bool
    last_check: datetime
    changelog: str = ""
    size_mb: float = 0.0
    dependencies: List[str] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        data['last_check'] = self.last_check.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ComponentVersion':
        """从字典创建"""
        data['last_check'] = datetime.fromisoformat(data['last_check'])
        return cls(**data)

@dataclass
class UpdateTask:
    """更新任务"""
    component: str
    from_version: str
    to_version: str
    status: str  # pending, running, completed, failed, rollback
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: str = ""
    backup_path: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        if self.start_time:
            data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        return data

class ComponentUpdateManager:
    """组件更新管理器"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config', 'update_config.json')
        self.config = self._load_config()
        
        # 设置日志
        self._setup_logging()
        
        # 组件信息
        self.components = {}
        self.update_tasks = []
        
        # 备份目录
        self.backup_dir = os.path.join(os.path.dirname(__file__), 'backups')
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # 更新检查线程
        self.check_thread = None
        self.check_active = False
        
        # 初始化组件列表
        self._initialize_components()
        
        self.logger.info("组件更新管理器初始化完成")
    
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
            logging.error(f"配置文件加载失败: {e}")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """创建默认配置"""
        return {
            "components": {
                "pc28_data_collector": {
                    "path": "./pc28_data_collector.py",
                    "version_file": "./version.txt",
                    "update_url": "https://api.github.com/repos/your-repo/pc28-system/releases/latest",
                    "auto_update": False,
                    "backup_enabled": True
                },
                "data_quality_checker": {
                    "path": "./data_quality_checker.py",
                    "version_file": "./version.txt",
                    "update_url": "https://api.github.com/repos/your-repo/pc28-system/releases/latest",
                    "auto_update": False,
                    "backup_enabled": True
                },
                "api_monitor": {
                    "path": "./python/api_monitor.py",
                    "version_file": "./version.txt",
                    "update_url": "https://api.github.com/repos/your-repo/pc28-system/releases/latest",
                    "auto_update": False,
                    "backup_enabled": True
                }
            },
            "update_settings": {
                "check_interval_hours": 24,
                "auto_backup": True,
                "rollback_timeout_minutes": 30,
                "max_backup_count": 5,
                "update_window": {
                    "start_hour": 2,
                    "end_hour": 6
                }
            },
            "notification": {
                "enabled": True,
                "channels": ["log", "file"]
            }
        }
    
    def _setup_logging(self):
        """设置日志"""
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'update_manager_{datetime.now().strftime("%Y%m%d")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def _initialize_components(self):
        """初始化组件列表"""
        components_config = self.config.get('components', {})
        
        for name, config in components_config.items():
            try:
                current_version = self._get_current_version(name, config)
                
                self.components[name] = ComponentVersion(
                    name=name,
                    current_version=current_version,
                    latest_version=current_version,
                    update_available=False,
                    last_check=datetime.now()
                )
                
            except Exception as e:
                self.logger.error(f"初始化组件 {name} 失败: {e}")
    
    def _get_current_version(self, component_name: str, config: Dict[str, Any]) -> str:
        """获取组件当前版本"""
        version_file = config.get('version_file')
        
        if version_file and os.path.exists(version_file):
            try:
                with open(version_file, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except Exception as e:
                self.logger.warning(f"读取版本文件失败 {version_file}: {e}")
        
        # 尝试从文件修改时间生成版本号
        component_path = config.get('path')
        if component_path and os.path.exists(component_path):
            mtime = os.path.getmtime(component_path)
            return datetime.fromtimestamp(mtime).strftime('%Y%m%d_%H%M%S')
        
        return '1.0.0'
    
    def check_updates(self, component_name: str = None) -> Dict[str, Any]:
        """检查更新"""
        results = {}
        
        components_to_check = [component_name] if component_name else list(self.components.keys())
        
        for name in components_to_check:
            if name not in self.components:
                continue
            
            try:
                component = self.components[name]
                config = self.config['components'][name]
                
                # 检查远程版本
                latest_version = self._check_remote_version(name, config)
                
                if latest_version and latest_version != component.current_version:
                    component.latest_version = latest_version
                    component.update_available = True
                    component.last_check = datetime.now()
                    
                    results[name] = {
                        'status': 'update_available',
                        'current': component.current_version,
                        'latest': latest_version
                    }
                else:
                    component.update_available = False
                    component.last_check = datetime.now()
                    
                    results[name] = {
                        'status': 'up_to_date',
                        'current': component.current_version
                    }
                
            except Exception as e:
                self.logger.error(f"检查组件 {name} 更新失败: {e}")
                results[name] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return results
    
    def _check_remote_version(self, component_name: str, config: Dict[str, Any]) -> Optional[str]:
        """检查远程版本"""
        update_url = config.get('update_url')
        if not update_url:
            return None
        
        try:
            # 模拟版本检查（实际应该调用真实的API）
            # 这里返回一个模拟的新版本
            current_time = datetime.now()
            return f"{current_time.year}.{current_time.month}.{current_time.day}"
            
        except Exception as e:
            self.logger.error(f"检查远程版本失败 {component_name}: {e}")
            return None
    
    def update_component(self, component_name: str, force: bool = False) -> bool:
        """更新组件"""
        if component_name not in self.components:
            self.logger.error(f"未知组件: {component_name}")
            return False
        
        component = self.components[component_name]
        config = self.config['components'][component_name]
        
        if not component.update_available and not force:
            self.logger.info(f"组件 {component_name} 无需更新")
            return True
        
        # 创建更新任务
        task = UpdateTask(
            component=component_name,
            from_version=component.current_version,
            to_version=component.latest_version,
            status='pending'
        )
        
        self.update_tasks.append(task)
        
        try:
            task.status = 'running'
            task.start_time = datetime.now()
            
            # 1. 创建备份
            if config.get('backup_enabled', True):
                backup_path = self._create_backup(component_name, config)
                task.backup_path = backup_path
            
            # 2. 下载新版本
            self._download_update(component_name, config)
            
            # 3. 应用更新
            self._apply_update(component_name, config)
            
            # 4. 验证更新
            if self._verify_update(component_name, config):
                task.status = 'completed'
                task.end_time = datetime.now()
                
                # 更新组件信息
                component.current_version = component.latest_version
                component.update_available = False
                
                self.logger.info(f"组件 {component_name} 更新成功: {task.from_version} -> {task.to_version}")
                return True
            else:
                raise Exception("更新验证失败")
        
        except Exception as e:
            task.status = 'failed'
            task.end_time = datetime.now()
            task.error_message = str(e)
            
            self.logger.error(f"组件 {component_name} 更新失败: {e}")
            
            # 尝试回滚
            if task.backup_path:
                self._rollback_update(component_name, task.backup_path)
            
            return False
    
    def _create_backup(self, component_name: str, config: Dict[str, Any]) -> str:
        """创建备份"""
        component_path = config.get('path')
        if not component_path or not os.path.exists(component_path):
            raise Exception(f"组件文件不存在: {component_path}")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"{component_name}_{timestamp}.backup"
        backup_path = os.path.join(self.backup_dir, backup_name)
        
        if os.path.isfile(component_path):
            shutil.copy2(component_path, backup_path)
        else:
            shutil.copytree(component_path, backup_path)
        
        self.logger.info(f"创建备份: {backup_path}")
        
        # 清理旧备份
        self._cleanup_old_backups(component_name)
        
        return backup_path
    
    def _cleanup_old_backups(self, component_name: str):
        """清理旧备份"""
        max_backups = self.config.get('update_settings', {}).get('max_backup_count', 5)
        
        # 获取该组件的所有备份文件
        backup_files = []
        for file in os.listdir(self.backup_dir):
            if file.startswith(f"{component_name}_") and file.endswith('.backup'):
                file_path = os.path.join(self.backup_dir, file)
                backup_files.append((file_path, os.path.getmtime(file_path)))
        
        # 按时间排序，保留最新的几个
        backup_files.sort(key=lambda x: x[1], reverse=True)
        
        for file_path, _ in backup_files[max_backups:]:
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                else:
                    shutil.rmtree(file_path)
                self.logger.info(f"删除旧备份: {file_path}")
            except Exception as e:
                self.logger.warning(f"删除备份失败 {file_path}: {e}")
    
    def _download_update(self, component_name: str, config: Dict[str, Any]):
        """下载更新"""
        # 模拟下载过程
        self.logger.info(f"下载组件 {component_name} 更新...")
        time.sleep(1)  # 模拟下载时间
    
    def _apply_update(self, component_name: str, config: Dict[str, Any]):
        """应用更新"""
        # 模拟应用更新
        self.logger.info(f"应用组件 {component_name} 更新...")
        time.sleep(1)  # 模拟应用时间
    
    def _verify_update(self, component_name: str, config: Dict[str, Any]) -> bool:
        """验证更新"""
        # 模拟验证过程
        self.logger.info(f"验证组件 {component_name} 更新...")
        return True  # 模拟验证成功
    
    def _rollback_update(self, component_name: str, backup_path: str) -> bool:
        """回滚更新"""
        try:
            config = self.config['components'][component_name]
            component_path = config.get('path')
            
            if not component_path:
                return False
            
            # 删除当前文件/目录
            if os.path.exists(component_path):
                if os.path.isfile(component_path):
                    os.remove(component_path)
                else:
                    shutil.rmtree(component_path)
            
            # 恢复备份
            if os.path.isfile(backup_path):
                shutil.copy2(backup_path, component_path)
            else:
                shutil.copytree(backup_path, component_path)
            
            self.logger.info(f"组件 {component_name} 回滚成功")
            return True
            
        except Exception as e:
            self.logger.error(f"组件 {component_name} 回滚失败: {e}")
            return False
    
    def start_auto_check(self):
        """启动自动检查"""
        if self.check_active:
            self.logger.warning("自动检查已经在运行")
            return
        
        self.check_active = True
        self.check_thread = threading.Thread(target=self._auto_check_loop, daemon=True)
        self.check_thread.start()
        
        self.logger.info("自动更新检查已启动")
    
    def stop_auto_check(self):
        """停止自动检查"""
        self.check_active = False
        if self.check_thread:
            self.check_thread.join(timeout=5)
        
        self.logger.info("自动更新检查已停止")
    
    def _auto_check_loop(self):
        """自动检查循环"""
        check_interval = self.config.get('update_settings', {}).get('check_interval_hours', 24) * 3600
        
        while self.check_active:
            try:
                # 检查是否在更新窗口内
                if self._is_in_update_window():
                    self.logger.info("执行自动更新检查")
                    results = self.check_updates()
                    
                    # 处理自动更新
                    for component_name, result in results.items():
                        if result.get('status') == 'update_available':
                            config = self.config['components'].get(component_name, {})
                            if config.get('auto_update', False):
                                self.logger.info(f"自动更新组件: {component_name}")
                                self.update_component(component_name)
                
                time.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"自动检查异常: {e}")
                time.sleep(3600)  # 出错时等待1小时后重试
    
    def _is_in_update_window(self) -> bool:
        """检查是否在更新窗口内"""
        update_window = self.config.get('update_settings', {}).get('update_window', {})
        start_hour = update_window.get('start_hour', 2)
        end_hour = update_window.get('end_hour', 6)
        
        current_hour = datetime.now().hour
        
        if start_hour <= end_hour:
            return start_hour <= current_hour < end_hour
        else:
            return current_hour >= start_hour or current_hour < end_hour
    
    def get_component_status(self) -> Dict[str, Any]:
        """获取组件状态"""
        status = {}
        
        for name, component in self.components.items():
            status[name] = {
                'current_version': component.current_version,
                'latest_version': component.latest_version,
                'update_available': component.update_available,
                'last_check': component.last_check.isoformat()
            }
        
        return status
    
    def get_update_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取更新历史"""
        return [task.to_dict() for task in self.update_tasks[-limit:]]
    
    def export_status_report(self) -> str:
        """导出状态报告"""
        report = {
            'export_time': datetime.now().isoformat(),
            'components': self.get_component_status(),
            'update_history': self.get_update_history(),
            'config': self.config
        }
        
        return json.dumps(report, ensure_ascii=False, indent=2)

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28 组件更新管理工具')
    parser.add_argument('command', choices=[
        'check', 'update', 'status', 'history', 'start-auto', 'stop-auto', 'export'
    ], help='命令')
    parser.add_argument('--component', type=str, help='组件名称')
    parser.add_argument('--force', action='store_true', help='强制更新')
    parser.add_argument('--config', type=str, help='配置文件路径')
    
    args = parser.parse_args()
    
    # 初始化更新管理器
    manager = ComponentUpdateManager(args.config)
    
    try:
        if args.command == 'check':
            print("\n=== 检查组件更新 ===")
            results = manager.check_updates(args.component)
            
            for component, result in results.items():
                status = result['status']
                if status == 'update_available':
                    print(f"🔄 {component}: {result['current']} -> {result['latest']} (有更新)")
                elif status == 'up_to_date':
                    print(f"✅ {component}: {result['current']} (最新)")
                else:
                    print(f"❌ {component}: {result.get('error', '检查失败')}")
        
        elif args.command == 'update':
            if not args.component:
                print("❌ 请指定要更新的组件名称")
                return
            
            print(f"\n=== 更新组件: {args.component} ===")
            success = manager.update_component(args.component, args.force)
            
            if success:
                print(f"✅ 组件 {args.component} 更新成功")
            else:
                print(f"❌ 组件 {args.component} 更新失败")
        
        elif args.command == 'status':
            print("\n=== 组件状态 ===")
            status = manager.get_component_status()
            
            for component, info in status.items():
                update_status = "有更新" if info['update_available'] else "最新"
                print(f"{component}:")
                print(f"  当前版本: {info['current_version']}")
                print(f"  最新版本: {info['latest_version']}")
                print(f"  状态: {update_status}")
                print(f"  最后检查: {info['last_check']}")
                print()
        
        elif args.command == 'history':
            print("\n=== 更新历史 ===")
            history = manager.get_update_history()
            
            if not history:
                print("暂无更新历史")
            else:
                for task in history:
                    status_icon = {
                        'completed': '✅',
                        'failed': '❌',
                        'running': '🔄',
                        'pending': '⏳'
                    }.get(task['status'], '❓')
                    
                    print(f"{status_icon} {task['component']}: {task['from_version']} -> {task['to_version']}")
                    if task.get('start_time'):
                        print(f"   时间: {task['start_time']}")
                    if task.get('error_message'):
                        print(f"   错误: {task['error_message']}")
                    print()
        
        elif args.command == 'start-auto':
            print("启动自动更新检查...")
            manager.start_auto_check()
            print("自动更新检查已启动，按 Ctrl+C 停止")
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n停止自动更新检查...")
                manager.stop_auto_check()
        
        elif args.command == 'stop-auto':
            manager.stop_auto_check()
            print("自动更新检查已停止")
        
        elif args.command == 'export':
            report = manager.export_status_report()
            
            export_file = os.path.join(
                os.path.dirname(__file__), 
                'logs', 
                f'update_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            
            os.makedirs(os.path.dirname(export_file), exist_ok=True)
            with open(export_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            print(f"✅ 状态报告已导出: {export_file}")
    
    except KeyboardInterrupt:
        print("\n组件更新管理工具已停止")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()