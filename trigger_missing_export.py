#!/usr/bin/env python3
"""
触发缺失视图导出
专门用于导出p_ensemble_today_norm_v5视图
"""

import json
import os
import subprocess
from datetime import datetime
from typing import Dict, List, Any
import logging
from google.cloud import bigquery

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/a606/cloud_function_source/logs/missing_export_trigger.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MissingExportTrigger:
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.target_view = "p_ensemble_today_norm_v5"
        self.local_data_dir = "/Users/a606/cloud_function_source/local_data"
        self.export_log_path = os.path.join(self.local_data_dir, "export_log.json")
        
        # 确保目录存在
        os.makedirs(self.local_data_dir, exist_ok=True)
        os.makedirs("/Users/a606/cloud_function_source/logs", exist_ok=True)
        
    def export_view_to_local(self, view_name: str) -> bool:
        """导出指定视图到本地"""
        try:
            logger.info(f"开始导出视图: {view_name}")
            
            # 初始化BigQuery客户端
            client = bigquery.Client(project=self.project_id)
            
            # 构建查询
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{view_name}`
            LIMIT 10000
            """
            
            logger.info(f"执行查询: {query}")
            
            # 执行查询
            query_job = client.query(query)
            results = query_job.result()
            
            # 转换为列表
            rows = []
            schema_info = []
            
            # 获取schema信息
            for field in results.schema:
                schema_info.append({
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode
                })
            
            # 获取数据行
            for row in results:
                row_dict = {}
                for field in results.schema:
                    value = row[field.name]
                    # 处理特殊数据类型
                    if value is not None:
                        if hasattr(value, 'isoformat'):  # datetime对象
                            row_dict[field.name] = value.isoformat()
                        else:
                            row_dict[field.name] = value
                    else:
                        row_dict[field.name] = None
                rows.append(row_dict)
            
            # 保存到本地文件
            output_file = os.path.join(self.local_data_dir, f"{view_name}.json")
            export_data = {
                "table_name": view_name,
                "export_time": datetime.now().isoformat(),
                "row_count": len(rows),
                "schema": schema_info,
                "data": rows
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"视图 {view_name} 导出成功，共 {len(rows)} 行数据")
            logger.info(f"数据保存到: {output_file}")
            
            # 更新导出日志
            self.update_export_log(view_name, len(rows), True, None)
            
            return True
            
        except Exception as e:
            error_msg = f"导出视图 {view_name} 失败: {str(e)}"
            logger.error(error_msg)
            self.update_export_log(view_name, 0, False, error_msg)
            return False
    
    def update_export_log(self, view_name: str, row_count: int, success: bool, error_msg: str = None):
        """更新导出日志"""
        try:
            # 加载现有日志
            export_log = {}
            if os.path.exists(self.export_log_path):
                with open(self.export_log_path, 'r', encoding='utf-8') as f:
                    export_log = json.load(f)
            
            # 更新日志条目
            if "exports" not in export_log:
                export_log["exports"] = {}
            
            export_log["exports"][view_name] = {
                "type": "view",
                "export_time": datetime.now().isoformat(),
                "row_count": row_count,
                "success": success,
                "error": error_msg,
                "schema_columns": 0  # 将在后续更新
            }
            
            # 保存日志
            with open(self.export_log_path, 'w', encoding='utf-8') as f:
                json.dump(export_log, f, ensure_ascii=False, indent=2)
                
            logger.info(f"导出日志已更新: {view_name}")
            
        except Exception as e:
            logger.error(f"更新导出日志失败: {e}")
    
    def run(self):
        """执行缺失视图导出"""
        logger.info("开始触发缺失视图导出")
        logger.info(f"目标视图: {self.target_view}")
        
        success = self.export_view_to_local(self.target_view)
        
        if success:
            logger.info(f"✅ 视图 {self.target_view} 导出成功")
        else:
            logger.error(f"❌ 视图 {self.target_view} 导出失败")
        
        return success

if __name__ == "__main__":
    trigger = MissingExportTrigger()
    success = trigger.run()
    
    if success:
        print(f"✅ 五桶模型视图 {trigger.target_view} 导出成功")
    else:
        print(f"❌ 五桶模型视图 {trigger.target_view} 导出失败")