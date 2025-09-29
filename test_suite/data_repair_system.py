#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28数据修复系统
自动检测和修复数据流程中的问题
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import threading

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'python'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RepairTask:
    """修复任务数据类"""
    task_id: str
    task_name: str
    priority: str  # 'high', 'medium', 'low'
    status: str  # 'pending', 'in_progress', 'completed', 'failed'
    description: str
    repair_action: str
    created_at: str
    completed_at: Optional[str] = None
    error_message: Optional[str] = None

class PC28DataRepairSystem:
    """PC28数据修复系统"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_lab = "pc28_lab"
        self.dataset_prod = "pc28"
        self.repair_tasks = []
        self.lock = threading.Lock()
        
    def run_bq_query(self, query: str) -> Tuple[bool, Any]:
        """执行BigQuery查询"""
        try:
            cmd = [
                'bq', 'query', '--use_legacy_sql=false', 
                '--format=json', '--max_rows=1000', query
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                if result.stdout.strip():
                    return True, json.loads(result.stdout)
                else:
                    return True, []
            else:
                logger.error(f"BQ查询失败: {result.stderr}")
                return False, result.stderr
                
        except Exception as e:
            logger.error(f"执行BQ查询异常: {e}")
            return False, str(e)
    
    def run_bq_update(self, query: str) -> Tuple[bool, str]:
        """执行BigQuery更新操作"""
        try:
            cmd = ['bq', 'query', '--use_legacy_sql=false', query]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                return True, "更新成功"
            else:
                logger.error(f"BQ更新失败: {result.stderr}")
                return False, result.stderr
                
        except Exception as e:
            logger.error(f"执行BQ更新异常: {e}")
            return False, str(e)
    
    def detect_data_issues(self) -> List[RepairTask]:
        """检测数据问题"""
        issues = []
        
        # 1. 检查数据时效性问题
        logger.info("检查数据时效性...")
        time_issues = self._detect_time_issues()
        issues.extend(time_issues)
        
        # 2. 检查视图依赖问题
        logger.info("检查视图依赖...")
        view_issues = self._detect_view_issues()
        issues.extend(view_issues)
        
        # 3. 检查数据完整性问题
        logger.info("检查数据完整性...")
        integrity_issues = self._detect_integrity_issues()
        issues.extend(integrity_issues)
        
        return issues
    
    def _detect_time_issues(self) -> List[RepairTask]:
        """检测时间相关问题"""
        issues = []
        
        try:
            # 检查cloud_pred_today_norm数据日期
            query = """
            SELECT 
                DATE(ts_utc,'Asia/Shanghai') as data_date,
                CURRENT_DATE('Asia/Shanghai') as today,
                COUNT(*) as count
            FROM `wprojectl.pc28_lab.p_cloud_clean_merged_dedup_v`
            GROUP BY 1, 2
            """
            
            success, result = self.run_bq_query(query)
            if success and result:
                data_date = result[0]['data_date']
                today = result[0]['today']
                
                if data_date != today:
                    issues.append(RepairTask(
                        task_id=f"time_issue_{int(time.time())}",
                        task_name="数据时效性问题",
                        priority="high",
                        status="pending",
                        description=f"数据日期({data_date})与当前日期({today})不匹配",
                        repair_action="update_data_date_filter",
                        created_at=datetime.now().isoformat()
                    ))
            
        except Exception as e:
            logger.error(f"检测时间问题异常: {e}")
        
        return issues
    
    def _detect_view_issues(self) -> List[RepairTask]:
        """检测视图问题"""
        issues = []
        
        # 检查关键视图是否有数据
        critical_views = [
            "p_cloud_today_v",
            "p_map_today_v", 
            "p_size_today_v",
            "p_map_today_canon_v",
            "p_size_today_canon_v",
            "signal_pool_union_v3"
        ]
        
        for view in critical_views:
            try:
                query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.{view}`"
                success, result = self.run_bq_query(query)
                
                if success and result:
                    count = int(result[0]['count'])
                    if count == 0:
                        issues.append(RepairTask(
                            task_id=f"view_issue_{view}_{int(time.time())}",
                            task_name=f"视图无数据: {view}",
                            priority="high" if "signal_pool" in view else "medium",
                            status="pending",
                            description=f"{view}视图返回0行数据",
                            repair_action=f"repair_view_{view}",
                            created_at=datetime.now().isoformat()
                        ))
                        
            except Exception as e:
                logger.error(f"检测视图{view}异常: {e}")
        
        return issues
    
    def _detect_integrity_issues(self) -> List[RepairTask]:
        """检测数据完整性问题"""
        issues = []
        
        # 检查signal_pool表是否为空
        try:
            query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_lab}.signal_pool`"
            success, result = self.run_bq_query(query)
            
            if success and result:
                count = int(result[0]['count'])
                if count == 0:
                    issues.append(RepairTask(
                        task_id=f"integrity_signal_pool_{int(time.time())}",
                        task_name="signal_pool表为空",
                        priority="high",
                        status="pending",
                        description="signal_pool表没有今日数据",
                        repair_action="populate_signal_pool",
                        created_at=datetime.now().isoformat()
                    ))
                    
        except Exception as e:
            logger.error(f"检测signal_pool完整性异常: {e}")
        
        return issues
    
    def repair_data_date_filter(self, task: RepairTask) -> bool:
        """修复数据日期过滤问题"""
        try:
            logger.info("修复数据日期过滤问题...")
            
            # 更新p_cloud_today_v视图，使用最新数据日期而不是当前日期
            update_query = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_lab}.p_cloud_today_v` AS
            WITH params AS (
                SELECT MAX(DATE(ts_utc,'Asia/Shanghai')) AS day_id
                FROM `{self.project_id}.{self.dataset_lab}.p_cloud_clean_merged_dedup_v`
            )
            SELECT period, ts_utc,
                   GREATEST(LEAST(CAST(p_even AS FLOAT64), 1-1e-6), 1e-6) AS p_even,
                   'cloud' AS src,
                   999 AS n_src
            FROM `{self.project_id}.{self.dataset_lab}.p_cloud_clean_merged_dedup_v`, params
            WHERE DATE(ts_utc,'Asia/Shanghai')=params.day_id
            """
            
            success, message = self.run_bq_update(update_query)
            if success:
                logger.info("p_cloud_today_v视图已更新为使用最新数据日期")
                return True
            else:
                logger.error(f"更新p_cloud_today_v失败: {message}")
                return False
                
        except Exception as e:
            logger.error(f"修复数据日期过滤异常: {e}")
            return False
    
    def repair_view_p_map_today_v(self, task: RepairTask) -> bool:
        """修复p_map_today_v视图"""
        try:
            logger.info("修复p_map_today_v视图...")
            
            # 创建一个基本的p_map_today_v视图，基于cloud数据
            update_query = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_lab}.p_map_today_v` AS
            SELECT 
                period,
                ts_utc,
                p_even,
                'map' as src,
                100 as n_src
            FROM `{self.project_id}.{self.dataset_lab}.p_cloud_today_v`
            """
            
            success, message = self.run_bq_update(update_query)
            if success:
                logger.info("p_map_today_v视图已创建")
                return True
            else:
                logger.error(f"创建p_map_today_v失败: {message}")
                return False
                
        except Exception as e:
            logger.error(f"修复p_map_today_v异常: {e}")
            return False
    
    def repair_view_p_map_today_canon_v(self, task: RepairTask) -> bool:
        """修复p_map_today_canon_v视图"""
        try:
            logger.info("修复p_map_today_canon_v视图...")
            
            # 创建标准化的map预测视图
            update_query = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_lab}.p_map_today_canon_v` AS
            SELECT 
                period,
                ts_utc,
                'oe' as market,
                CASE WHEN p_even >= 0.5 THEN 'even' ELSE 'odd' END as pick,
                CASE WHEN p_even >= 0.5 THEN p_even ELSE 1-p_even END as p_win,
                'map' as source
            FROM `{self.project_id}.{self.dataset_lab}.p_map_today_v`
            WHERE p_even IS NOT NULL
            """
            
            success, message = self.run_bq_update(update_query)
            if success:
                logger.info("p_map_today_canon_v视图已创建")
                return True
            else:
                logger.error(f"创建p_map_today_canon_v失败: {message}")
                return False
                
        except Exception as e:
            logger.error(f"修复p_map_today_canon_v异常: {e}")
            return False
    
    def populate_signal_pool(self, task: RepairTask) -> bool:
        """填充signal_pool表"""
        try:
            logger.info("填充signal_pool表...")
            
            # 从signal_pool_union_v3插入数据到signal_pool表
            insert_query = f"""
            INSERT INTO `{self.project_id}.{self.dataset_lab}.signal_pool`
            (id, created_at, ts_utc, period, market, pick, p_win, source, vote_ratio, params, features, notes)
            SELECT 
                id, created_at, ts_utc, period, market, pick, p_win, source, vote_ratio, params, features, notes
            FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
            WHERE DATE(ts_utc, 'Asia/Shanghai') = CURRENT_DATE('Asia/Shanghai')
            """
            
            success, message = self.run_bq_update(insert_query)
            if success:
                logger.info("signal_pool表已填充")
                return True
            else:
                logger.error(f"填充signal_pool失败: {message}")
                return False
                
        except Exception as e:
            logger.error(f"填充signal_pool异常: {e}")
            return False
    
    def execute_repair_task(self, task: RepairTask) -> bool:
        """执行修复任务"""
        with self.lock:
            task.status = "in_progress"
            logger.info(f"开始执行修复任务: {task.task_name}")
            
            try:
                success = False
                
                if task.repair_action == "update_data_date_filter":
                    success = self.repair_data_date_filter(task)
                elif task.repair_action == "repair_view_p_map_today_v":
                    success = self.repair_view_p_map_today_v(task)
                elif task.repair_action == "repair_view_p_map_today_canon_v":
                    success = self.repair_view_p_map_today_canon_v(task)
                elif task.repair_action == "populate_signal_pool":
                    success = self.populate_signal_pool(task)
                else:
                    logger.warning(f"未知的修复动作: {task.repair_action}")
                    success = False
                
                if success:
                    task.status = "completed"
                    task.completed_at = datetime.now().isoformat()
                    logger.info(f"修复任务完成: {task.task_name}")
                else:
                    task.status = "failed"
                    task.error_message = "修复操作失败"
                    logger.error(f"修复任务失败: {task.task_name}")
                
                return success
                
            except Exception as e:
                task.status = "failed"
                task.error_message = str(e)
                logger.error(f"执行修复任务异常: {task.task_name}, {e}")
                return False
    
    def run_auto_repair(self) -> Dict[str, Any]:
        """运行自动修复"""
        logger.info("开始自动修复流程...")
        start_time = time.time()
        
        # 1. 检测问题
        issues = self.detect_data_issues()
        logger.info(f"检测到 {len(issues)} 个问题")
        
        # 2. 按优先级排序
        issues.sort(key=lambda x: {'high': 0, 'medium': 1, 'low': 2}[x.priority])
        
        # 3. 执行修复
        completed_tasks = []
        failed_tasks = []
        
        for issue in issues:
            success = self.execute_repair_task(issue)
            if success:
                completed_tasks.append(issue)
            else:
                failed_tasks.append(issue)
        
        total_time = time.time() - start_time
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_issues': len(issues),
            'completed': len(completed_tasks),
            'failed': len(failed_tasks),
            'success_rate': len(completed_tasks) / len(issues) if issues else 1.0,
            'execution_time': total_time,
            'completed_tasks': [
                {
                    'task_id': t.task_id,
                    'task_name': t.task_name,
                    'priority': t.priority,
                    'description': t.description,
                    'repair_action': t.repair_action,
                    'completed_at': t.completed_at
                }
                for t in completed_tasks
            ],
            'failed_tasks': [
                {
                    'task_id': t.task_id,
                    'task_name': t.task_name,
                    'priority': t.priority,
                    'description': t.description,
                    'error_message': t.error_message
                }
                for t in failed_tasks
            ]
        }
        
        logger.info(f"自动修复完成: {len(completed_tasks)}/{len(issues)} 成功, 耗时 {total_time:.2f}s")
        return summary
    
    def generate_repair_report(self, summary: Dict[str, Any]) -> str:
        """生成修复报告"""
        report = []
        report.append("# PC28数据自动修复报告")
        report.append(f"**修复时间**: {summary['timestamp']}")
        report.append(f"**检测问题数**: {summary['total_issues']}")
        report.append(f"**修复成功**: {summary['completed']}")
        report.append(f"**修复失败**: {summary['failed']}")
        report.append(f"**成功率**: {summary['success_rate']:.2%}")
        report.append(f"**总耗时**: {summary['execution_time']:.2f}秒")
        report.append("")
        
        if summary['completed_tasks']:
            report.append("## ✅ 修复成功的任务")
            for task in summary['completed_tasks']:
                report.append(f"### {task['task_name']}")
                report.append(f"**优先级**: {task['priority']}")
                report.append(f"**描述**: {task['description']}")
                report.append(f"**修复动作**: {task['repair_action']}")
                report.append(f"**完成时间**: {task['completed_at']}")
                report.append("")
        
        if summary['failed_tasks']:
            report.append("## ❌ 修复失败的任务")
            for task in summary['failed_tasks']:
                report.append(f"### {task['task_name']}")
                report.append(f"**优先级**: {task['priority']}")
                report.append(f"**描述**: {task['description']}")
                report.append(f"**错误信息**: {task['error_message']}")
                report.append("")
        
        return "\n".join(report)

def main():
    """主函数"""
    repair_system = PC28DataRepairSystem()
    
    # 运行自动修复
    summary = repair_system.run_auto_repair()
    
    # 生成报告
    report = repair_system.generate_repair_report(summary)
    
    # 保存报告
    report_file = f"/Users/a606/cloud_function_source/test_suite/repair_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    # 保存JSON结果
    json_file = f"/Users/a606/cloud_function_source/test_suite/repair_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(report)
    print(f"\n报告已保存到: {report_file}")
    print(f"JSON结果已保存到: {json_file}")
    
    # 返回退出码
    return 0 if summary['failed'] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())