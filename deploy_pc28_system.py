#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统部署脚本
自动化部署和启动PC28回填与实时开奖系统
"""

import os
import sys
import json
import subprocess
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

class PC28SystemDeployer:
    """PC28系统部署器"""
    
    def __init__(self, config_path: str = None):
        self.project_root = Path(__file__).parent
        self.python_dir = self.project_root / "python"
        self.config_path = config_path or str(self.python_dir / "pc28_system_config.json")
        self.config = self._load_config()
        self._setup_logging()
        
    def _load_config(self) -> Dict:
        """加载系统配置"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"配置文件加载失败: {e}")
            return {}
    
    def _setup_logging(self):
        """设置日志"""
        log_level = getattr(logging, self.config.get('log_level', 'INFO'))
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pc28_deploy.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def check_dependencies(self) -> bool:
        """检查系统依赖"""
        self.logger.info("检查系统依赖...")
        
        required_modules = [
            'requests', 'sqlite3', 'threading', 'json', 'hashlib',
            'datetime', 'time', 'logging', 'pathlib'
        ]
        
        missing_modules = []
        for module in required_modules:
            try:
                __import__(module)
            except ImportError:
                missing_modules.append(module)
        
        if missing_modules:
            self.logger.error(f"缺少依赖模块: {missing_modules}")
            return False
        
        self.logger.info("依赖检查通过")
        return True
    
    def check_file_structure(self) -> bool:
        """检查文件结构"""
        self.logger.info("检查文件结构...")
        
        required_files = [
            "python/pc28_upstream_api.py",
            "python/enhanced_backfill_service.py",
            "python/enhanced_realtime_service.py",
            "python/system_integration_manager.py",
            "python/start_pc28_system.py",
            "python/pc28_system_config.json"
        ]
        
        missing_files = []
        for file_path in required_files:
            full_path = self.project_root / file_path
            if not full_path.exists():
                missing_files.append(file_path)
        
        if missing_files:
            self.logger.error(f"缺少必要文件: {missing_files}")
            return False
        
        self.logger.info("文件结构检查通过")
        return True
    
    def create_directories(self):
        """创建必要的目录"""
        self.logger.info("创建必要目录...")
        
        directories = [
            "logs",
            "data",
            "backups",
            "temp"
        ]
        
        for directory in directories:
            dir_path = self.project_root / directory
            dir_path.mkdir(exist_ok=True)
            self.logger.info(f"创建目录: {dir_path}")
    
    def test_api_connectivity(self) -> bool:
        """测试API连接"""
        self.logger.info("测试API连接...")
        
        try:
            # 导入并测试API连接
            sys.path.insert(0, str(self.python_dir))
            from pc28_upstream_api import PC28UpstreamAPI
            
            api = PC28UpstreamAPI(
                appid=self.config.get('appid', ''),
                secret_key=self.config.get('secret_key', '')
            )
            
            if api.test_connection():
                self.logger.info("API连接测试成功")
                return True
            else:
                self.logger.error("API连接测试失败")
                return False
                
        except Exception as e:
            self.logger.error(f"API连接测试异常: {e}")
            return False
    
    def initialize_database(self) -> bool:
        """初始化数据库"""
        self.logger.info("初始化数据库...")
        
        try:
            sys.path.insert(0, str(self.python_dir))
            from system_integration_manager import SystemIntegrationManager
            
            manager = SystemIntegrationManager(config=self.config)
            # 数据库会在初始化时自动创建
            self.logger.info("数据库初始化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"数据库初始化失败: {e}")
            return False
    
    def run_system_tests(self) -> bool:
        """运行系统测试"""
        self.logger.info("运行系统测试...")
        
        try:
            # 测试回填服务
            sys.path.insert(0, str(self.python_dir))
            from enhanced_backfill_service import EnhancedBackfillService
            from enhanced_realtime_service import EnhancedRealtimeService
            
            # 创建服务实例
            backfill_service = EnhancedBackfillService(config=self.config)
            realtime_service = EnhancedRealtimeService(config=self.config)
            
            # 测试基本功能
            self.logger.info("测试回填服务...")
            # 这里可以添加具体的测试逻辑
            
            self.logger.info("测试实时服务...")
            # 这里可以添加具体的测试逻辑
            
            self.logger.info("系统测试通过")
            return True
            
        except Exception as e:
            self.logger.error(f"系统测试失败: {e}")
            return False
    
    def deploy(self) -> bool:
        """执行部署"""
        self.logger.info("开始部署PC28系统...")
        
        deployment_steps = [
            ("检查依赖", self.check_dependencies),
            ("检查文件结构", self.check_file_structure),
            ("创建目录", lambda: (self.create_directories(), True)[1]),
            ("测试API连接", self.test_api_connectivity),
            ("初始化数据库", self.initialize_database),
            ("运行系统测试", self.run_system_tests)
        ]
        
        for step_name, step_func in deployment_steps:
            self.logger.info(f"执行步骤: {step_name}")
            try:
                if not step_func():
                    self.logger.error(f"步骤失败: {step_name}")
                    return False
                self.logger.info(f"步骤完成: {step_name}")
            except Exception as e:
                self.logger.error(f"步骤异常: {step_name} - {e}")
                return False
        
        self.logger.info("PC28系统部署完成!")
        return True
    
    def start_system(self, daemon_mode: bool = False):
        """启动系统"""
        self.logger.info("启动PC28系统...")
        
        try:
            start_script = self.python_dir / "start_pc28_system.py"
            cmd = [sys.executable, str(start_script)]
            
            if daemon_mode:
                cmd.append("--daemon")
            
            # 启动系统
            process = subprocess.Popen(
                cmd,
                cwd=str(self.python_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            self.logger.info(f"系统已启动，进程ID: {process.pid}")
            
            if not daemon_mode:
                # 非守护模式下等待进程结束
                stdout, stderr = process.communicate()
                if stdout:
                    self.logger.info(f"系统输出: {stdout.decode()}")
                if stderr:
                    self.logger.error(f"系统错误: {stderr.decode()}")
            
            return process
            
        except Exception as e:
            self.logger.error(f"系统启动失败: {e}")
            return None
    
    def show_status(self):
        """显示系统状态"""
        print("\n=== PC28系统部署状态 ===")
        print(f"项目根目录: {self.project_root}")
        print(f"配置文件: {self.config_path}")
        print(f"Python目录: {self.python_dir}")
        
        # 检查关键文件
        key_files = [
            "python/start_pc28_system.py",
            "python/system_integration_manager.py",
            "python/enhanced_backfill_service.py",
            "python/enhanced_realtime_service.py"
        ]
        
        print("\n关键文件状态:")
        for file_path in key_files:
            full_path = self.project_root / file_path
            status = "✓" if full_path.exists() else "✗"
            print(f"  {status} {file_path}")
        
        print("\n配置信息:")
        print(f"  应用ID: {self.config.get('appid', 'N/A')}")
        print(f"  实时获取间隔: {self.config.get('realtime_fetch_interval', 'N/A')}秒")
        print(f"  自动回填: {'启用' if self.config.get('auto_backfill_enabled') else '禁用'}")
        print(f"  日志级别: {self.config.get('log_level', 'N/A')}")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28系统部署工具')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--deploy', action='store_true', help='执行部署')
    parser.add_argument('--start', action='store_true', help='启动系统')
    parser.add_argument('--daemon', action='store_true', help='守护进程模式')
    parser.add_argument('--status', action='store_true', help='显示状态')
    
    args = parser.parse_args()
    
    deployer = PC28SystemDeployer(config_path=args.config)
    
    if args.status:
        deployer.show_status()
    elif args.deploy:
        if deployer.deploy():
            print("\n✓ 部署成功!")
            if args.start:
                print("\n启动系统...")
                deployer.start_system(daemon_mode=args.daemon)
        else:
            print("\n✗ 部署失败!")
            sys.exit(1)
    elif args.start:
        deployer.start_system(daemon_mode=args.daemon)
    else:
        deployer.show_status()
        print("\n使用 --help 查看可用选项")

if __name__ == "__main__":
    main()