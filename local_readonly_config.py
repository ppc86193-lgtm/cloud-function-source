#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地只读模式配置系统
Local Read-Only Mode Configuration System

功能：
1. 配置本地系统为只读模式
2. 禁用所有数据采集功能
3. 启用实验模式
4. 数据源重定向到本地同步数据
"""

import os
import json
import logging
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ReadOnlyConfig:
    """只读模式配置"""
    mode: str = "readonly"  # readonly, experiment, production
    data_collection_enabled: bool = False
    local_db_path: str = "local_experiment.db"
    cloud_sync_enabled: bool = True
    experiment_mode: bool = True
    api_endpoints_disabled: bool = True
    write_operations_blocked: bool = True
    
class LocalReadOnlyManager:
    """本地只读模式管理器"""
    
    def __init__(self):
        self.config = ReadOnlyConfig()
        self.config_file = "readonly_config.json"
        self._load_config()
        
    def _load_config(self):
        """加载配置"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.config = ReadOnlyConfig(**data)
                logger.info("只读模式配置加载完成")
        except Exception as e:
            logger.warning(f"加载配置失败，使用默认配置: {e}")
    
    def _save_config(self):
        """保存配置"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(self.config), f, ensure_ascii=False, indent=2)
            logger.info("只读模式配置保存完成")
        except Exception as e:
            logger.error(f"保存配置失败: {e}")
    
    def enable_readonly_mode(self):
        """启用只读模式"""
        logger.info("启用本地只读模式...")
        
        self.config.mode = "readonly"
        self.config.data_collection_enabled = False
        self.config.api_endpoints_disabled = True
        self.config.write_operations_blocked = True
        self.config.experiment_mode = True
        self.config.cloud_sync_enabled = True
        
        self._save_config()
        self._create_readonly_database_views()
        self._disable_data_collection_services()
        
        logger.info("本地只读模式启用完成")
    
    def _create_readonly_database_views(self):
        """创建只读数据库视图"""
        try:
            if not os.path.exists(self.config.local_db_path):
                logger.warning("本地数据库不存在，请先运行数据同步")
                return
            
            conn = sqlite3.connect(self.config.local_db_path)
            cursor = conn.cursor()
            
            # 创建只读视图
            cursor.execute('''
                CREATE VIEW IF NOT EXISTS readonly_ensemble_view AS
                SELECT 
                    period,
                    ts_utc,
                    ts_cst,
                    p_cloud,
                    conf_cloud,
                    p_map,
                    conf_map,
                    p_size,
                    conf_size,
                    p_combo,
                    conf_combo,
                    p_hit,
                    conf_hit,
                    p_star_ens,
                    vote_ratio,
                    cooling_status,
                    'READONLY' as data_source,
                    sync_timestamp
                FROM p_ensemble_today_norm_v5
                ORDER BY period DESC
            ''')
            
            # 创建实验数据视图
            cursor.execute('''
                CREATE VIEW IF NOT EXISTS experiment_data_view AS
                SELECT 
                    period,
                    p_star_ens as prediction,
                    vote_ratio as confidence,
                    cooling_status,
                    CASE 
                        WHEN p_star_ens >= 0.7 THEN 'HIGH'
                        WHEN p_star_ens >= 0.5 THEN 'MEDIUM'
                        ELSE 'LOW'
                    END as risk_level,
                    sync_timestamp as data_timestamp
                FROM p_ensemble_today_norm_v5
                WHERE p_star_ens IS NOT NULL
                ORDER BY period DESC
            ''')
            
            # 创建配置表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_config (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    description TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 插入只读模式配置
            cursor.execute('''
                INSERT OR REPLACE INTO system_config (key, value, description)
                VALUES 
                ('mode', 'readonly', '系统运行模式'),
                ('data_collection', 'disabled', '数据采集状态'),
                ('experiment_mode', 'enabled', '实验模式状态'),
                ('last_config_update', ?, '最后配置更新时间')
            ''', (datetime.now().isoformat(),))
            
            conn.commit()
            conn.close()
            
            logger.info("只读数据库视图创建完成")
            
        except Exception as e:
            logger.error(f"创建只读数据库视图失败: {e}")
    
    def _disable_data_collection_services(self):
        """禁用数据采集服务"""
        try:
            # 创建禁用标记文件
            disabled_services = [
                "real_api_data_system.py",
                "data_collection_service.py",
                "api_data_collector.py"
            ]
            
            for service in disabled_services:
                disable_file = f"{service}.disabled"
                with open(disable_file, 'w') as f:
                    f.write(f"Service disabled in readonly mode at {datetime.now().isoformat()}")
                
                logger.info(f"已禁用服务: {service}")
            
            # 创建只读模式标记
            with open('.readonly_mode', 'w') as f:
                f.write(json.dumps({
                    "enabled": True,
                    "timestamp": datetime.now().isoformat(),
                    "mode": "experiment"
                }, indent=2))
            
            logger.info("数据采集服务禁用完成")
            
        except Exception as e:
            logger.error(f"禁用数据采集服务失败: {e}")
    
    def verify_readonly_setup(self) -> Dict:
        """验证只读模式设置"""
        verification_results = {
            "只读模式状态": "未知",
            "数据采集状态": "未知",
            "本地数据库": "未知",
            "同步数据": "未知",
            "实验环境": "未知",
            "配置文件": "未知"
        }
        
        try:
            # 检查配置文件
            if os.path.exists(self.config_file):
                verification_results["配置文件"] = "存在"
                verification_results["只读模式状态"] = "已启用" if self.config.mode == "readonly" else "未启用"
                verification_results["数据采集状态"] = "已禁用" if not self.config.data_collection_enabled else "未禁用"
            else:
                verification_results["配置文件"] = "不存在"
            
            # 检查本地数据库
            if os.path.exists(self.config.local_db_path):
                verification_results["本地数据库"] = "存在"
                
                # 检查同步数据
                conn = sqlite3.connect(self.config.local_db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM p_ensemble_today_norm_v5")
                record_count = cursor.fetchone()[0]
                conn.close()
                
                verification_results["同步数据"] = f"正常 ({record_count}条记录)"
            else:
                verification_results["本地数据库"] = "不存在"
                verification_results["同步数据"] = "无数据"
            
            # 检查只读模式标记
            if os.path.exists('.readonly_mode'):
                verification_results["实验环境"] = "已配置"
            else:
                verification_results["实验环境"] = "未配置"
            
        except Exception as e:
            logger.error(f"验证只读模式设置失败: {e}")
            verification_results["错误信息"] = str(e)
        
        return verification_results
    
    def get_experiment_data_sample(self, limit: int = 10) -> List[Dict]:
        """获取实验数据样本"""
        try:
            if not os.path.exists(self.config.local_db_path):
                return []
            
            conn = sqlite3.connect(self.config.local_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM experiment_data_view 
                ORDER BY period DESC 
                LIMIT ?
            ''', (limit,))
            
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            conn.close()
            
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"获取实验数据样本失败: {e}")
            return []
    
    def generate_readonly_report(self) -> Dict:
        """生成只读模式报告"""
        verification = self.verify_readonly_setup()
        sample_data = self.get_experiment_data_sample(5)
        
        return {
            "系统状态": "只读模式" if self.config.mode == "readonly" else "其他模式",
            "配置信息": asdict(self.config),
            "验证结果": verification,
            "实验数据样本": sample_data,
            "报告生成时间": datetime.now().isoformat(),
            "建议": [
                "本地系统已配置为只读模式，不会进行数据采集",
                "所有实验数据来源于云端同步",
                "可以安全进行本地实验和测试",
                "定期运行数据同步以获取最新数据"
            ]
        }

def main():
    """主函数"""
    logger.info("启动本地只读模式配置系统")
    
    # 创建只读模式管理器
    readonly_manager = LocalReadOnlyManager()
    
    # 启用只读模式
    readonly_manager.enable_readonly_mode()
    
    # 生成报告
    report = readonly_manager.generate_readonly_report()
    
    logger.info("只读模式配置报告:")
    for key, value in report.items():
        if isinstance(value, (dict, list)):
            logger.info(f"  {key}:")
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    logger.info(f"    {sub_key}: {sub_value}")
            else:
                for item in value:
                    logger.info(f"    - {item}")
        else:
            logger.info(f"  {key}: {value}")
    
    logger.info("本地只读模式配置完成")
    return True

if __name__ == "__main__":
    main()