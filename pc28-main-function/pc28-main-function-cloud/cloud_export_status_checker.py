#!/usr/bin/env python3
"""
云数据导出状态检查器
检查云数据导出进度，确保新创建的五桶模型视图被包含在导出中
"""

import json
import os
import subprocess
from datetime import datetime
from typing import Dict, List, Any
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/a606/cloud_function_source/logs/cloud_export_checker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CloudExportStatusChecker:
    def __init__(self):
        self.export_log_path = "/Users/a606/cloud_function_source/local_data/export_log.json"
        self.target_views = [
            "p_ensemble_today_norm_v5",  # 新创建的五桶模型视图
            "p_ensemble_today_norm_v4",
            "p_ensemble_today_norm_v3",
            "p_ensemble_today_norm_v2",
            "p_ensemble_today_norm_v"
        ]
        
    def load_export_log(self) -> Dict[str, Any]:
        """加载导出日志"""
        try:
            if os.path.exists(self.export_log_path):
                with open(self.export_log_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.warning(f"导出日志文件不存在: {self.export_log_path}")
                return {}
        except Exception as e:
            logger.error(f"加载导出日志失败: {e}")
            return {}
    
    def check_bigquery_views(self) -> List[str]:
        """检查BigQuery中的视图列表"""
        try:
            cmd = ["bq", "ls", "wprojectl:pc28_lab"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            views = []
            for line in result.stdout.split('\n'):
                if 'VIEW' in line:
                    parts = line.strip().split()
                    if parts:
                        view_name = parts[0]
                        views.append(view_name)
            
            return views
        except subprocess.CalledProcessError as e:
            logger.error(f"检查BigQuery视图失败: {e}")
            return []
    
    def check_view_export_status(self) -> Dict[str, Any]:
        """检查目标视图的导出状态"""
        export_log = self.load_export_log()
        bigquery_views = self.check_bigquery_views()
        
        status_report = {
            "timestamp": datetime.now().isoformat(),
            "export_log_exists": bool(export_log),
            "last_export_time": export_log.get("end_time", "未知"),
            "total_tables_exported": export_log.get("tables_exported", 0),
            "total_views_exported": export_log.get("views_exported", 0),
            "target_views_status": {},
            "missing_from_export": [],
            "export_errors": export_log.get("errors", [])
        }
        
        table_details = export_log.get("table_details", {})
        
        for view_name in self.target_views:
            # 检查是否在BigQuery中存在
            exists_in_bigquery = view_name in bigquery_views
            
            # 检查是否在导出日志中
            exported = view_name in table_details
            
            # 检查导出是否有错误
            export_error = None
            for error in export_log.get("errors", []):
                if view_name in error:
                    export_error = error
                    break
            
            status_report["target_views_status"][view_name] = {
                "exists_in_bigquery": exists_in_bigquery,
                "exported": exported,
                "export_error": export_error,
                "export_details": table_details.get(view_name, {})
            }
            
            if exists_in_bigquery and not exported:
                status_report["missing_from_export"].append(view_name)
        
        return status_report
    
    def trigger_missing_view_export(self, missing_views: List[str]) -> bool:
        """触发缺失视图的导出"""
        if not missing_views:
            logger.info("没有缺失的视图需要导出")
            return True
        
        logger.info(f"开始导出缺失的视图: {missing_views}")
        
        try:
            # 这里可以调用现有的导出系统来导出特定视图
            # 暂时记录需要导出的视图
            missing_export_log = {
                "timestamp": datetime.now().isoformat(),
                "missing_views": missing_views,
                "action": "需要手动触发导出或等待下次自动导出"
            }
            
            with open("/Users/a606/cloud_function_source/logs/missing_views_export.json", "w", encoding="utf-8") as f:
                json.dump(missing_export_log, f, indent=2, ensure_ascii=False)
            
            logger.info(f"缺失视图导出请求已记录到日志文件")
            return True
            
        except Exception as e:
            logger.error(f"触发缺失视图导出失败: {e}")
            return False
    
    def generate_status_report(self) -> str:
        """生成状态报告"""
        status = self.check_view_export_status()
        
        report_lines = [
            "=" * 60,
            "云数据导出状态检查报告",
            "=" * 60,
            f"检查时间: {status['timestamp']}",
            f"最后导出时间: {status['last_export_time']}",
            f"已导出表数量: {status['total_tables_exported']}",
            f"已导出视图数量: {status['total_views_exported']}",
            "",
            "目标视图状态:",
            "-" * 40
        ]
        
        for view_name, view_status in status["target_views_status"].items():
            report_lines.append(f"视图: {view_name}")
            report_lines.append(f"  - BigQuery中存在: {'是' if view_status['exists_in_bigquery'] else '否'}")
            report_lines.append(f"  - 已导出: {'是' if view_status['exported'] else '否'}")
            
            if view_status['export_error']:
                report_lines.append(f"  - 导出错误: {view_status['export_error'][:100]}...")
            
            if view_status['export_details']:
                details = view_status['export_details']
                report_lines.append(f"  - 导出行数: {details.get('rows', 0)}")
                report_lines.append(f"  - 导出时间: {details.get('export_time', '未知')}")
            
            report_lines.append("")
        
        if status["missing_from_export"]:
            report_lines.extend([
                "缺失导出的视图:",
                "-" * 20
            ])
            for view in status["missing_from_export"]:
                report_lines.append(f"  - {view}")
            report_lines.append("")
        
        if status["export_errors"]:
            report_lines.extend([
                f"导出错误总数: {len(status['export_errors'])}",
                "主要错误类型:",
                "-" * 20
            ])
            
            # 统计错误类型
            error_types = {}
            for error in status["export_errors"]:
                if "Failed to parse input string" in error:
                    error_types["时间戳解析错误"] = error_types.get("时间戳解析错误", 0) + 1
                elif "Not found: Table" in error:
                    error_types["表不存在错误"] = error_types.get("表不存在错误", 0) + 1
                elif "Unrecognized name" in error:
                    error_types["字段名错误"] = error_types.get("字段名错误", 0) + 1
                else:
                    error_types["其他错误"] = error_types.get("其他错误", 0) + 1
            
            for error_type, count in error_types.items():
                report_lines.append(f"  - {error_type}: {count}个")
        
        report_lines.extend([
            "",
            "=" * 60,
            "检查完成"
        ])
        
        return "\n".join(report_lines)
    
    def run_check(self) -> Dict[str, Any]:
        """运行完整检查"""
        logger.info("开始云数据导出状态检查...")
        
        # 生成状态报告
        report = self.generate_status_report()
        logger.info(f"\n{report}")
        
        # 保存报告到文件
        report_path = "/Users/a606/cloud_function_source/logs/cloud_export_status_report.txt"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report)
        
        # 获取状态数据
        status = self.check_view_export_status()
        
        # 处理缺失的导出
        if status["missing_from_export"]:
            self.trigger_missing_view_export(status["missing_from_export"])
        
        logger.info(f"状态报告已保存到: {report_path}")
        return status

def main():
    """主函数"""
    # 确保日志目录存在
    os.makedirs("/Users/a606/cloud_function_source/logs", exist_ok=True)
    
    checker = CloudExportStatusChecker()
    status = checker.run_check()
    
    # 返回关键信息
    print(f"\n关键信息:")
    print(f"- 五桶模型视图(p_ensemble_today_norm_v5)在BigQuery中存在: {status['target_views_status'].get('p_ensemble_today_norm_v5', {}).get('exists_in_bigquery', False)}")
    print(f"- 五桶模型视图已导出: {status['target_views_status'].get('p_ensemble_today_norm_v5', {}).get('exported', False)}")
    print(f"- 缺失导出的视图数量: {len(status['missing_from_export'])}")
    print(f"- 导出错误总数: {len(status['export_errors'])}")

if __name__ == "__main__":
    main()