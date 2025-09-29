#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置加载器模块

提供统一的配置加载功能，支持多种配置文件格式
"""

import os
import json
import yaml
from typing import Dict, Any, Optional

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径，如果为None则使用默认配置
        
    Returns:
        配置字典
    """
    # 默认配置
    default_config = {
        'appid': '45928',
        'secret_key': 'ca9edbfee35c22a0d6c4cf6722506af0',
        'realtime_fetch_interval': 30,
        'health_check_interval': 60,
        'metrics_collection_interval': 300,
        'auto_backfill_enabled': True,
        'max_concurrent_tasks': 3,
        'api_rate_limit': 10,
        'retry_attempts': 3,
        'retry_delay': 1,
        'database': {
            'path': 'pc28_system.db',
            'backup_enabled': True,
            'backup_interval': 3600
        },
        'logging': {
            'level': 'INFO',
            'file': 'pc28_system.log',
            'max_size': '10MB',
            'backup_count': 5
        },
        'monitoring': {
            'enabled': True,
            'check_interval': 60,
            'alert_threshold': 0.8
        },
        'upstream_api': {
            'base_url': 'https://api.api68.com',
            'timeout': 30,
            'max_retries': 3
        }
    }
    
    # 如果没有指定配置文件路径，返回默认配置
    if not config_path:
        return default_config
    
    # 尝试加载配置文件
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}，使用默认配置")
        return default_config
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            if config_path.endswith('.json'):
                loaded_config = json.load(f)
            elif config_path.endswith(('.yaml', '.yml')):
                loaded_config = yaml.safe_load(f)
            else:
                # 尝试JSON格式
                loaded_config = json.load(f)
        
        # 合并配置（加载的配置覆盖默认配置）
        merged_config = default_config.copy()
        merged_config.update(loaded_config)
        
        return merged_config
        
    except Exception as e:
        print(f"配置文件加载失败: {e}，使用默认配置")
        return default_config

def load_integrated_config(config_path: str = 'config/integrated_config.json') -> Dict[str, Any]:
    """
    加载集成配置，包含上游API配置，支持环境变量替换
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置字典
    """
    import re
    
    # 默认集成配置
    default_integrated_config = {
        'upstream_api': {
            'base_url': 'https://api.api68.com',
            'appid': '45928',
            'secret_key': 'ca9edbfee35c22a0d6c4cf6722506af0',
            'timeout': 30,
            'max_retries': 3,
            'rate_limit': 10
        },
        'database': {
            'type': 'sqlite',
            'path': 'pc28_system.db'
        },
        'cache': {
            'enabled': True,
            'ttl': 300,
            'max_size': 1000
        },
        'monitoring': {
            'enabled': True,
            'metrics_interval': 60,
            'health_check_interval': 30
        }
    }
    
    if not os.path.exists(config_path):
        print(f"集成配置文件不存在: {config_path}，使用默认配置")
        return default_integrated_config
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_content = f.read()
        
        # 环境变量替换
        def replace_env_vars(match):
            env_var = match.group(1)
            default_value = match.group(2) if match.group(2) else ''
            return os.getenv(env_var, default_value)
        
        # 替换 ${ENV_VAR} 或 ${ENV_VAR:default_value} 格式的环境变量
        config_content = re.sub(r'\$\{([^}:]+)(?::([^}]*))?\}', replace_env_vars, config_content)
        
        loaded_config = json.loads(config_content)
        
        # 合并配置
        merged_config = default_integrated_config.copy()
        merged_config.update(loaded_config)
        
        return merged_config
        
    except Exception as e:
        print(f"集成配置文件加载失败: {e}，使用默认配置")
        return default_integrated_config

def save_config(config: Dict[str, Any], config_path: str) -> bool:
    """
    保存配置到文件
    
    Args:
        config: 配置字典
        config_path: 配置文件路径
        
    Returns:
        是否保存成功
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        with open(config_path, 'w', encoding='utf-8') as f:
            if config_path.endswith('.json'):
                json.dump(config, f, ensure_ascii=False, indent=2)
            elif config_path.endswith(('.yaml', '.yml')):
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
            else:
                # 默认使用JSON格式
                json.dump(config, f, ensure_ascii=False, indent=2)
        
        return True
        
    except Exception as e:
        print(f"配置文件保存失败: {e}")
        return False

def validate_config(config: Dict[str, Any]) -> bool:
    """
    验证配置的有效性
    
    Args:
        config: 配置字典
        
    Returns:
        配置是否有效
    """
    required_keys = ['appid', 'secret_key']
    
    for key in required_keys:
        if key not in config:
            print(f"配置缺少必要项: {key}")
            return False
        
        if not config[key]:
            print(f"配置项为空: {key}")
            return False
    
    return True

if __name__ == "__main__":
    # 测试配置加载
    config = load_config()
    print("默认配置:")
    print(json.dumps(config, ensure_ascii=False, indent=2))
    
    # 验证配置
    if validate_config(config):
        print("\n配置验证通过")
    else:
        print("\n配置验证失败")