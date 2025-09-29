#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动数据拉取脚本
定期从云端拉取数据到本地
"""

import json
import time
from cloud_data_repair_system import CloudDataRepairSystem

def main():
    """主函数"""
    repair_system = CloudDataRepairSystem()
    
    # 加载配置
    with open("local_data/pull_config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    
    if not config.get("enabled", False):
        print("数据拉取已禁用")
        return
    
    # 执行数据同步
    for table_name in config["tables_to_sync"]:
        print(f"同步表: {table_name}")
        result = repair_system.sync_cloud_data_to_local(
            table_name=table_name,
            limit=config["max_records_per_sync"]
        )
        print(f"同步结果: {result['status']}, 记录数: {result['records_synced']}")
        
        if result["status"] == "error":
            print(f"同步错误: {result['error']}")
    
    # 检查数据完整性
    if config.get("auto_repair", False):
        print("检查数据完整性...")
        integrity_report = repair_system.check_bigquery_data_integrity()
        print(f"发现 {len(integrity_report['issues'])} 个问题")

if __name__ == "__main__":
    main()
