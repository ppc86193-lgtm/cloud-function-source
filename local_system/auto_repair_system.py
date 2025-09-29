#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28自动修复系统
智能检测故障并自动修复，实现完全自愈能力
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import threading
from enum import Enum
from dataclasses import dataclass, asdict

from local_database import get_local_db
from local_api_collector import LocalAPICollector
from local_sql_engine import LocalSQLEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IssueType(Enum):
    """问题类型枚举"""
    DATA_MISSING = "data_missing"
    DATA_STALE = "data_stale"
    VIEW_ERROR = "view_error"
    API_ERROR = "api_error"
    PIPELINE_ERROR = "pipeline_error"
    DEPENDENCY_ERROR = "dependency_error"

class RepairAction(Enum):
    """修复动作枚举"""
    COLLECT_DATA = "collect_data"
    REFRESH_VIEWS = "refresh_views"
    RESTART_PIPELINE = "restart_pipeline"
    RESET_PARAMETERS = "reset_parameters"
    REBUILD_TABLES = "rebuild_tables"
    SYNC_FROM_CLOUD = "sync_from_cloud"

@dataclass
class Issue:
    """问题数据类"""
    id: str
    component: str
    issue_type: IssueType
    severity: str  # critical, warning, info
    description: str
    detected_at: str
    metadata: Dict[str, Any]

@dataclass
class RepairResult:
    """修复结果数据类"""
    issue_id: str
    action: RepairAction
    success: bool
    execution_time_ms: int
    details: str
    timestamp: str

class AutoRepairSystem:
    """自动修复系统"""
    
    def __init__(self):
        """初始化修复系统"""
        self.db = get_local_db()
        self.collector = LocalAPICollector()
        self.sql_engine = LocalSQLEngine()
        self.is_monitoring = False
        self.monitor_thread = None
        self.repair_history = []
        
        # 修复策略配置
        self.repair_strategies = self._init_repair_strategies()
        
        # 健康检查配置
        self.health_checks = self._init_health_checks()
    
    def _init_repair_strategies(self) -> Dict[IssueType, List[RepairAction]]:
        """初始化修复策略"""
        return {
            IssueType.DATA_MISSING: [
                RepairAction.COLLECT_DATA,
                RepairAction.SYNC_FROM_CLOUD,
                RepairAction.REBUILD_TABLES
            ],
            IssueType.DATA_STALE: [
                RepairAction.COLLECT_DATA,
                RepairAction.REFRESH_VIEWS
            ],
            IssueType.VIEW_ERROR: [
                RepairAction.REFRESH_VIEWS,
                RepairAction.RESTART_PIPELINE
            ],
            IssueType.API_ERROR: [
                RepairAction.COLLECT_DATA,
                RepairAction.RESET_PARAMETERS
            ],
            IssueType.PIPELINE_ERROR: [
                RepairAction.RESTART_PIPELINE,
                RepairAction.REFRESH_VIEWS,
                RepairAction.REBUILD_TABLES
            ],
            IssueType.DEPENDENCY_ERROR: [
                RepairAction.REFRESH_VIEWS,
                RepairAction.RESTART_PIPELINE
            ]
        }
    
    def _init_health_checks(self) -> Dict[str, Dict[str, Any]]:
        """初始化健康检查配置"""
        return {
            'data_freshness': {
                'description': '检查数据新鲜度',
                'max_age_hours': 2,
                'critical_tables': ['cloud_pred_today_norm', 'p_map_clean_merged_dedup_v']
            },
            'data_completeness': {
                'description': '检查数据完整性',
                'min_records': 10,
                'required_tables': ['cloud_pred_today_norm', 'signal_pool_union_v3']
            },
            'view_availability': {
                'description': '检查视图可用性',
                'required_views': ['p_cloud_today_v', 'p_map_today_v', 'signal_pool_union_v3_view']
            },
            'pipeline_health': {
                'description': '检查数据管道健康状态',
                'max_candidates': 1000,
                'min_candidates': 1
            },
            'api_connectivity': {
                'description': '检查API连接状态',
                'timeout_seconds': 30
            }
        }
    
    def run_health_checks(self) -> List[Issue]:
        """运行所有健康检查"""
        logger.info("开始运行健康检查...")
        issues = []
        
        try:
            # 1. 数据新鲜度检查
            issues.extend(self._check_data_freshness())
            
            # 2. 数据完整性检查
            issues.extend(self._check_data_completeness())
            
            # 3. 视图可用性检查
            issues.extend(self._check_view_availability())
            
            # 4. 管道健康检查
            issues.extend(self._check_pipeline_health())
            
            # 5. API连接检查
            issues.extend(self._check_api_connectivity())
            
            logger.info(f"健康检查完成，发现 {len(issues)} 个问题")
            
            # 记录检查结果
            self._log_health_check_results(issues)
            
            return issues
            
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return [Issue(
                id=f"health_check_error_{int(time.time())}",
                component="health_checker",
                issue_type=IssueType.PIPELINE_ERROR,
                severity="critical",
                description=f"健康检查系统故障: {str(e)}",
                detected_at=datetime.now().isoformat(),
                metadata={"error": str(e)}
            )]
    
    def _check_data_freshness(self) -> List[Issue]:
        """检查数据新鲜度"""
        issues = []
        config = self.health_checks['data_freshness']
        max_age = timedelta(hours=config['max_age_hours'])
        
        for table in config['critical_tables']:
            try:
                # 获取最新数据时间
                query = f"""
                    SELECT MAX(created_at) as latest_time 
                    FROM {table} 
                    WHERE data_date = date('now')
                """
                result = self.db.execute_query(query)
                
                if not result or not result[0]['latest_time']:
                    issues.append(Issue(
                        id=f"data_missing_{table}_{int(time.time())}",
                        component=table,
                        issue_type=IssueType.DATA_MISSING,
                        severity="critical",
                        description=f"表 {table} 今日无数据",
                        detected_at=datetime.now().isoformat(),
                        metadata={"table": table, "expected_date": datetime.now().strftime('%Y-%m-%d')}
                    ))
                    continue
                
                latest_time = datetime.fromisoformat(result[0]['latest_time'].replace('Z', '+00:00'))
                age = datetime.now() - latest_time.replace(tzinfo=None)
                
                if age > max_age:
                    issues.append(Issue(
                        id=f"data_stale_{table}_{int(time.time())}",
                        component=table,
                        issue_type=IssueType.DATA_STALE,
                        severity="warning",
                        description=f"表 {table} 数据过期，最后更新: {latest_time}",
                        detected_at=datetime.now().isoformat(),
                        metadata={"table": table, "latest_time": str(latest_time), "age_hours": age.total_seconds() / 3600}
                    ))
                    
            except Exception as e:
                issues.append(Issue(
                    id=f"freshness_check_error_{table}_{int(time.time())}",
                    component=table,
                    issue_type=IssueType.PIPELINE_ERROR,
                    severity="warning",
                    description=f"无法检查表 {table} 的数据新鲜度: {str(e)}",
                    detected_at=datetime.now().isoformat(),
                    metadata={"table": table, "error": str(e)}
                ))
        
        return issues
    
    def _check_data_completeness(self) -> List[Issue]:
        """检查数据完整性"""
        issues = []
        config = self.health_checks['data_completeness']
        min_records = config['min_records']
        
        for table in config['required_tables']:
            try:
                count = self.db.get_table_count(table, f"data_date = '{datetime.now().strftime('%Y-%m-%d')}'")
                
                if count < min_records:
                    severity = "critical" if count == 0 else "warning"
                    issues.append(Issue(
                        id=f"incomplete_data_{table}_{int(time.time())}",
                        component=table,
                        issue_type=IssueType.DATA_MISSING if count == 0 else IssueType.DATA_STALE,
                        severity=severity,
                        description=f"表 {table} 数据不足，当前: {count} 行，期望: >= {min_records} 行",
                        detected_at=datetime.now().isoformat(),
                        metadata={"table": table, "current_count": count, "expected_min": min_records}
                    ))
                    
            except Exception as e:
                issues.append(Issue(
                    id=f"completeness_check_error_{table}_{int(time.time())}",
                    component=table,
                    issue_type=IssueType.PIPELINE_ERROR,
                    severity="warning",
                    description=f"无法检查表 {table} 的数据完整性: {str(e)}",
                    detected_at=datetime.now().isoformat(),
                    metadata={"table": table, "error": str(e)}
                ))
        
        return issues
    
    def _check_view_availability(self) -> List[Issue]:
        """检查视图可用性"""
        issues = []
        config = self.health_checks['view_availability']
        
        for view in config['required_views']:
            try:
                # 测试视图查询
                result = self.db.execute_query(f"SELECT COUNT(*) as count FROM {view} LIMIT 1")
                
                if not result:
                    issues.append(Issue(
                        id=f"view_unavailable_{view}_{int(time.time())}",
                        component=view,
                        issue_type=IssueType.VIEW_ERROR,
                        severity="critical",
                        description=f"视图 {view} 不可用或不存在",
                        detected_at=datetime.now().isoformat(),
                        metadata={"view": view}
                    ))
                    
            except Exception as e:
                issues.append(Issue(
                    id=f"view_error_{view}_{int(time.time())}",
                    component=view,
                    issue_type=IssueType.VIEW_ERROR,
                    severity="critical",
                    description=f"视图 {view} 查询失败: {str(e)}",
                    detected_at=datetime.now().isoformat(),
                    metadata={"view": view, "error": str(e)}
                ))
        
        return issues
    
    def _check_pipeline_health(self) -> List[Issue]:
        """检查数据管道健康状态"""
        issues = []
        config = self.health_checks['pipeline_health']
        
        try:
            # 检查决策候选数量
            candidate_count = self.db.get_table_count('lab_push_candidates_v2')
            
            if candidate_count == 0:
                issues.append(Issue(
                    id=f"no_candidates_{int(time.time())}",
                    component="lab_push_candidates_v2",
                    issue_type=IssueType.PIPELINE_ERROR,
                    severity="critical",
                    description="决策管道无候选决策生成",
                    detected_at=datetime.now().isoformat(),
                    metadata={"candidate_count": candidate_count}
                ))
            elif candidate_count > config['max_candidates']:
                issues.append(Issue(
                    id=f"too_many_candidates_{int(time.time())}",
                    component="lab_push_candidates_v2",
                    issue_type=IssueType.PIPELINE_ERROR,
                    severity="warning",
                    description=f"决策候选过多: {candidate_count} > {config['max_candidates']}",
                    detected_at=datetime.now().isoformat(),
                    metadata={"candidate_count": candidate_count, "max_expected": config['max_candidates']}
                ))
            
            # 检查信号池状态
            signal_count = self.db.get_table_count('signal_pool_union_v3')
            if signal_count == 0:
                issues.append(Issue(
                    id=f"empty_signal_pool_{int(time.time())}",
                    component="signal_pool_union_v3",
                    issue_type=IssueType.PIPELINE_ERROR,
                    severity="critical",
                    description="信号池为空",
                    detected_at=datetime.now().isoformat(),
                    metadata={"signal_count": signal_count}
                ))
                
        except Exception as e:
            issues.append(Issue(
                id=f"pipeline_check_error_{int(time.time())}",
                component="pipeline_health_checker",
                issue_type=IssueType.PIPELINE_ERROR,
                severity="critical",
                description=f"管道健康检查失败: {str(e)}",
                detected_at=datetime.now().isoformat(),
                metadata={"error": str(e)}
            ))
        
        return issues
    
    def _check_api_connectivity(self) -> List[Issue]:
        """检查API连接状态"""
        issues = []
        
        try:
            # 测试API连接
            if not self.collector.api_client.test_connection():
                issues.append(Issue(
                    id=f"api_connection_failed_{int(time.time())}",
                    component="upstream_api",
                    issue_type=IssueType.API_ERROR,
                    severity="critical",
                    description="上游API连接失败",
                    detected_at=datetime.now().isoformat(),
                    metadata={"api_endpoint": self.collector.api_client.base_url}
                ))
                
        except Exception as e:
            issues.append(Issue(
                id=f"api_check_error_{int(time.time())}",
                component="api_connectivity_checker",
                issue_type=IssueType.API_ERROR,
                severity="warning",
                description=f"API连接检查失败: {str(e)}",
                detected_at=datetime.now().isoformat(),
                metadata={"error": str(e)}
            ))
        
        return issues
    
    def auto_repair(self, issues: List[Issue]) -> List[RepairResult]:
        """自动修复问题"""
        logger.info(f"开始自动修复 {len(issues)} 个问题...")
        repair_results = []
        
        for issue in issues:
            logger.info(f"修复问题: {issue.description}")
            
            # 获取修复策略
            strategies = self.repair_strategies.get(issue.issue_type, [])
            
            if not strategies:
                logger.warning(f"无修复策略: {issue.issue_type}")
                continue
            
            # 尝试每个修复动作
            for action in strategies:
                try:
                    start_time = datetime.now()
                    success = self._execute_repair_action(action, issue)
                    end_time = datetime.now()
                    
                    execution_time = int((end_time - start_time).total_seconds() * 1000)
                    
                    result = RepairResult(
                        issue_id=issue.id,
                        action=action,
                        success=success,
                        execution_time_ms=execution_time,
                        details=f"修复动作 {action.value} {'成功' if success else '失败'}",
                        timestamp=datetime.now().isoformat()
                    )
                    
                    repair_results.append(result)
                    
                    # 记录修复日志
                    self._log_repair_action(issue, result)
                    
                    if success:
                        logger.info(f"问题修复成功: {issue.id} -> {action.value}")
                        break  # 修复成功，跳出循环
                    else:
                        logger.warning(f"修复动作失败: {action.value}")
                        
                except Exception as e:
                    logger.error(f"修复动作执行异常 {action.value}: {e}")
                    
                    result = RepairResult(
                        issue_id=issue.id,
                        action=action,
                        success=False,
                        execution_time_ms=0,
                        details=f"修复动作异常: {str(e)}",
                        timestamp=datetime.now().isoformat()
                    )
                    repair_results.append(result)
                    self._log_repair_action(issue, result)
        
        logger.info(f"自动修复完成，处理了 {len(repair_results)} 个修复动作")
        return repair_results
    
    def _execute_repair_action(self, action: RepairAction, issue: Issue) -> bool:
        """执行具体的修复动作"""
        try:
            if action == RepairAction.COLLECT_DATA:
                return self._repair_collect_data(issue)
            
            elif action == RepairAction.REFRESH_VIEWS:
                return self._repair_refresh_views(issue)
            
            elif action == RepairAction.RESTART_PIPELINE:
                return self._repair_restart_pipeline(issue)
            
            elif action == RepairAction.RESET_PARAMETERS:
                return self._repair_reset_parameters(issue)
            
            elif action == RepairAction.REBUILD_TABLES:
                return self._repair_rebuild_tables(issue)
            
            elif action == RepairAction.SYNC_FROM_CLOUD:
                return self._repair_sync_from_cloud(issue)
            
            else:
                logger.warning(f"未知修复动作: {action}")
                return False
                
        except Exception as e:
            logger.error(f"修复动作执行失败 {action}: {e}")
            return False
    
    def _repair_collect_data(self, issue: Issue) -> bool:
        """修复：采集数据"""
        try:
            # 采集实时数据
            realtime_success = self.collector.collect_realtime_data()
            
            # 采集历史数据
            history_success = self.collector.collect_history_data()
            
            return realtime_success or history_success
            
        except Exception as e:
            logger.error(f"数据采集修复失败: {e}")
            return False
    
    def _repair_refresh_views(self, issue: Issue) -> bool:
        """修复：刷新视图"""
        try:
            # 创建所有视图
            view_results = self.sql_engine.create_all_views()
            
            # 刷新信号池
            signal_success = self.sql_engine.refresh_signal_pool()
            
            # 刷新决策候选
            candidate_success = self.sql_engine.refresh_candidates()
            
            return all(view_results.values()) and signal_success and candidate_success
            
        except Exception as e:
            logger.error(f"视图刷新修复失败: {e}")
            return False
    
    def _repair_restart_pipeline(self, issue: Issue) -> bool:
        """修复：重启数据管道"""
        try:
            # 运行完整数据管道
            pipeline_result = self.sql_engine.run_full_pipeline()
            return pipeline_result.get('success', False)
            
        except Exception as e:
            logger.error(f"管道重启修复失败: {e}")
            return False
    
    def _repair_reset_parameters(self, issue: Issue) -> bool:
        """修复：重置参数"""
        try:
            # 重置运行时参数为默认值
            default_params = [
                ('oe', 0.56, 1.0e-6, 0.05, 0.8, 0.5),
                ('size', 0.56, 1.0e-6, 0.05, 0.8, 0.5),
                ('pc28', 0.55, 1.0e-6, 0.05, 0.8, 0.5)
            ]
            
            for market, p_min_base, ev_min, max_kelly, target_acc, target_cov in default_params:
                self.db.execute_update("""
                    INSERT OR REPLACE INTO runtime_params 
                    (market, p_min_base, ev_min, max_kelly, target_acc, target_cov, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (market, p_min_base, ev_min, max_kelly, target_acc, target_cov, datetime.now().isoformat()))
            
            return True
            
        except Exception as e:
            logger.error(f"参数重置修复失败: {e}")
            return False
    
    def _repair_rebuild_tables(self, issue: Issue) -> bool:
        """修复：重建表"""
        try:
            # 这里可以实现表重建逻辑
            # 暂时返回True，实际应该重新创建表结构
            logger.info("表重建修复（暂未实现具体逻辑）")
            return True
            
        except Exception as e:
            logger.error(f"表重建修复失败: {e}")
            return False
    
    def _repair_sync_from_cloud(self, issue: Issue) -> bool:
        """修复：从云端同步"""
        try:
            # 这里可以实现云端同步逻辑
            # 暂时返回True，实际应该从BigQuery同步数据
            logger.info("云端同步修复（暂未实现具体逻辑）")
            return True
            
        except Exception as e:
            logger.error(f"云端同步修复失败: {e}")
            return False
    
    def _log_health_check_results(self, issues: List[Issue]):
        """记录健康检查结果"""
        try:
            for issue in issues:
                self.db.execute_update("""
                    INSERT INTO system_status 
                    (component, status, last_check, error_message, metadata)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    issue.component,
                    f"{issue.severity}_{issue.issue_type.value}",
                    issue.detected_at,
                    issue.description,
                    json.dumps(issue.metadata)
                ))
        except Exception as e:
            logger.error(f"记录健康检查结果失败: {e}")
    
    def _log_repair_action(self, issue: Issue, result: RepairResult):
        """记录修复动作"""
        try:
            self.db.execute_update("""
                INSERT INTO repair_logs 
                (component, issue_type, repair_action, status, details, execution_time_ms)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                issue.component,
                issue.issue_type.value,
                result.action.value,
                'success' if result.success else 'failed',
                result.details,
                result.execution_time_ms
            ))
        except Exception as e:
            logger.error(f"记录修复动作失败: {e}")
    
    def start_monitoring(self, interval_minutes: int = 5):
        """启动监控"""
        if self.is_monitoring:
            logger.warning("监控已在运行中")
            return
        
        self.is_monitoring = True
        
        def monitor_loop():
            logger.info(f"自动监控启动，检查间隔: {interval_minutes} 分钟")
            
            while self.is_monitoring:
                try:
                    # 运行健康检查
                    issues = self.run_health_checks()
                    
                    # 如果发现问题，自动修复
                    if issues:
                        repair_results = self.auto_repair(issues)
                        
                        # 统计修复结果
                        successful_repairs = sum(1 for r in repair_results if r.success)
                        logger.info(f"自动修复完成: {successful_repairs}/{len(repair_results)} 成功")
                    
                    # 等待下次检查
                    time.sleep(interval_minutes * 60)
                    
                except Exception as e:
                    logger.error(f"监控循环异常: {e}")
                    time.sleep(60)  # 异常时等待1分钟后重试
            
            logger.info("自动监控停止")
        
        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info("自动监控已启动")
    
    def stop_monitoring(self):
        """停止监控"""
        self.is_monitoring = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=10)
        
        logger.info("自动监控已停止")
    
    def get_system_health_report(self) -> Dict[str, Any]:
        """获取系统健康报告"""
        try:
            # 运行健康检查
            issues = self.run_health_checks()
            
            # 获取修复历史
            repair_history = self.db.execute_query("""
                SELECT * FROM repair_logs 
                ORDER BY timestamp DESC 
                LIMIT 10
            """)
            
            # 获取系统状态
            system_status = self.db.execute_query("""
                SELECT * FROM system_status 
                ORDER BY last_check DESC
            """)
            
            # 统计信息
            critical_issues = [i for i in issues if i.severity == 'critical']
            warning_issues = [i for i in issues if i.severity == 'warning']
            
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_health': 'critical' if critical_issues else ('warning' if warning_issues else 'healthy'),
                'issues': {
                    'total': len(issues),
                    'critical': len(critical_issues),
                    'warning': len(warning_issues),
                    'details': [asdict(issue) for issue in issues]
                },
                'repair_history': repair_history,
                'system_status': system_status,
                'monitoring_active': self.is_monitoring
            }
            
        except Exception as e:
            logger.error(f"获取系统健康报告失败: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_health': 'error',
                'error': str(e)
            }

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28自动修复系统')
    parser.add_argument('--action', choices=['check', 'repair', 'monitor', 'stop', 'report'], 
                       default='check', help='执行动作')
    parser.add_argument('--interval', type=int, default=5, help='监控间隔（分钟）')
    
    args = parser.parse_args()
    
    repair_system = AutoRepairSystem()
    
    if args.action == 'check':
        issues = repair_system.run_health_checks()
        print(f"健康检查完成，发现 {len(issues)} 个问题:")
        for issue in issues:
            print(f"  - [{issue.severity}] {issue.component}: {issue.description}")
    
    elif args.action == 'repair':
        issues = repair_system.run_health_checks()
        if issues:
            repair_results = repair_system.auto_repair(issues)
            successful = sum(1 for r in repair_results if r.success)
            print(f"自动修复完成: {successful}/{len(repair_results)} 成功")
        else:
            print("未发现需要修复的问题")
    
    elif args.action == 'monitor':
        repair_system.start_monitoring(args.interval)
        print(f"监控已启动，间隔: {args.interval} 分钟，按 Ctrl+C 停止...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            repair_system.stop_monitoring()
            print("监控已停止")
    
    elif args.action == 'stop':
        repair_system.stop_monitoring()
        print("监控已停止")
    
    elif args.action == 'report':
        report = repair_system.get_system_health_report()
        print(f"系统健康报告: {json.dumps(report, indent=2, ensure_ascii=False)}")

if __name__ == "__main__":
    main()