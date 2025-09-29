#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 数据同步推送执行脚本
执行完整的数据同步和推送流程
"""

import os
import sys
import logging
import subprocess
import json
from datetime import datetime
from typing import Dict, Any, List

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'sync_execution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SyncExecutionManager:
    """同步执行管理器"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.execution_id = f"sync_{self.start_time.strftime('%Y%m%d_%H%M%S')}"
        self.results = {
            'execution_id': self.execution_id,
            'start_time': self.start_time.isoformat(),
            'steps': [],
            'status': 'running',
            'errors': []
        }
        
    def log_step(self, step_name: str, status: str, details: Dict[str, Any] = None):
        """记录执行步骤"""
        step = {
            'step': step_name,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        self.results['steps'].append(step)
        logger.info(f"Step: {step_name} - Status: {status}")
        
    def setup_environment(self) -> bool:
        """设置环境变量"""
        try:
            # 设置 Supabase 环境变量
            os.environ['SUPABASE_URL'] = 'https://spzssrffipekpjyghcru.supabase.co'
            os.environ['SUPABASE_ANON_KEY'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNwenNzcmZmaXBla3BqeWdoY3J1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTg3ODcwMzIsImV4cCI6MjA3NDM2MzAzMn0.2VzWdXcAxkK0sN6W7FfyAACTAuTTJl6ycL9snbKRyGQ'
            os.environ['SUPABASE_SERVICE_ROLE_KEY'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNwenNzcmZmaXBla3BqeWdoY3J1Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1ODc4NzAzMiwiZXhwIjoyMDc0MzYzMDMyfQ.LRgqLyTU2cdht82xGi-gO44n64xM0h1uDjvUBP3tgr4'
            
            self.log_step("setup_environment", "success", {"message": "Environment variables configured"})
            return True
        except Exception as e:
            self.log_step("setup_environment", "failed", {"error": str(e)})
            return False
    
    def create_sample_data(self) -> bool:
        """创建示例数据用于同步测试"""
        try:
            import sqlite3
            
            # 连接到 lottery_data.db
            conn = sqlite3.connect('lottery_data.db')
            cursor = conn.cursor()
            
            # 创建示例数据
            sample_data = [
                ('20250929001', '12,15,23', '2025-09-29 08:00:00', 'test_source', 1),
                ('20250929002', '05,18,27', '2025-09-29 08:05:00', 'test_source', 1),
                ('20250929003', '09,14,21', '2025-09-29 08:10:00', 'test_source', 1),
            ]
            
            cursor.executemany('''
                INSERT OR REPLACE INTO lottery_records 
                (period, numbers, draw_time, source, verified)
                VALUES (?, ?, ?, ?, ?)
            ''', sample_data)
            
            conn.commit()
            conn.close()
            
            self.log_step("create_sample_data", "success", {
                "message": "Sample data created",
                "records_count": len(sample_data)
            })
            return True
        except Exception as e:
            self.log_step("create_sample_data", "failed", {"error": str(e)})
            return False
    
    def execute_data_sync(self) -> bool:
        """执行数据同步"""
        try:
            # 导入同步管理器
            from supabase_sync_manager import SupabaseSyncManager
            
            # 创建同步管理器实例
            sync_manager = SupabaseSyncManager()
            
            # 执行同步
            results = sync_manager.sync_all_tables('incremental')
            
            self.log_step("execute_data_sync", "success", {
                "sync_results": results,
                "successful_tables": results.get('successful_tables', []),
                "failed_tables": results.get('failed_tables', []),
                "total_records_synced": results.get('total_records_synced', 0),
                "duration_seconds": results.get('duration_seconds', 0)
            })
            
            return results.get('status') == 'completed'
        except Exception as e:
            self.log_step("execute_data_sync", "failed", {"error": str(e)})
            return False
    
    def validate_sync_results(self) -> bool:
        """验证同步结果"""
        try:
            from supabase_sync_manager import SupabaseSyncManager
            
            sync_manager = SupabaseSyncManager()
            
            # 获取同步统计
            stats = sync_manager.get_sync_stats()
            
            # 验证核心表的同步完整性
            validation_results = {}
            for table_name in sync_manager.CORE_TABLES.keys():
                try:
                    integrity_check = sync_manager.validate_sync_integrity(table_name)
                    validation_results[table_name] = integrity_check
                except Exception as e:
                    validation_results[table_name] = {"status": "error", "error": str(e)}
            
            self.log_step("validate_sync_results", "success", {
                "sync_stats": stats,
                "validation_results": validation_results
            })
            return True
        except Exception as e:
            self.log_step("validate_sync_results", "failed", {"error": str(e)})
            return False
    
    def check_git_status(self) -> Dict[str, Any]:
        """检查 Git 状态"""
        try:
            # 检查 Git 状态
            result = subprocess.run(['git', 'status', '--porcelain'], 
                                  capture_output=True, text=True, check=True)
            
            changes = result.stdout.strip().split('\n') if result.stdout.strip() else []
            
            return {
                "has_changes": len(changes) > 0,
                "changes": changes,
                "change_count": len(changes)
            }
        except subprocess.CalledProcessError as e:
            return {"error": str(e), "has_changes": False}
    
    def commit_and_push_changes(self) -> bool:
        """提交并推送更改"""
        try:
            # 检查 Git 状态
            git_status = self.check_git_status()
            
            if not git_status.get("has_changes", False):
                self.log_step("commit_and_push_changes", "skipped", {
                    "message": "No changes to commit",
                    "git_status": git_status
                })
                return True
            
            # 添加所有更改
            subprocess.run(['git', 'add', '.'], check=True)
            
            # 提交更改
            commit_message = f"Auto-sync: Data synchronization execution {self.execution_id}"
            subprocess.run(['git', 'commit', '-m', commit_message], check=True)
            
            # 推送到远程仓库
            subprocess.run(['git', 'push', 'origin', 'main'], check=True)
            
            self.log_step("commit_and_push_changes", "success", {
                "commit_message": commit_message,
                "changes_committed": git_status.get("change_count", 0)
            })
            return True
        except subprocess.CalledProcessError as e:
            self.log_step("commit_and_push_changes", "failed", {"error": str(e)})
            return False
    
    def generate_execution_report(self) -> str:
        """生成执行报告"""
        try:
            self.results['end_time'] = datetime.now().isoformat()
            self.results['duration_seconds'] = (datetime.now() - self.start_time).total_seconds()
            
            # 统计执行结果
            successful_steps = len([s for s in self.results['steps'] if s['status'] == 'success'])
            failed_steps = len([s for s in self.results['steps'] if s['status'] == 'failed'])
            
            self.results['summary'] = {
                'total_steps': len(self.results['steps']),
                'successful_steps': successful_steps,
                'failed_steps': failed_steps,
                'success_rate': f"{(successful_steps / len(self.results['steps']) * 100):.1f}%" if self.results['steps'] else "0%"
            }
            
            # 确定整体状态
            if failed_steps == 0:
                self.results['status'] = 'completed_successfully'
            elif successful_steps > 0:
                self.results['status'] = 'completed_with_errors'
            else:
                self.results['status'] = 'failed'
            
            # 保存报告
            report_filename = f"sync_execution_report_{self.execution_id}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Execution report saved to: {report_filename}")
            return report_filename
        except Exception as e:
            logger.error(f"Failed to generate execution report: {e}")
            return ""
    
    def execute_full_sync_pipeline(self) -> bool:
        """执行完整的同步推送流程"""
        logger.info(f"Starting sync execution pipeline: {self.execution_id}")
        
        try:
            # 1. 设置环境
            if not self.setup_environment():
                return False
            
            # 2. 创建示例数据（如果需要）
            self.create_sample_data()
            
            # 3. 执行数据同步
            if not self.execute_data_sync():
                return False
            
            # 4. 验证同步结果
            self.validate_sync_results()
            
            # 5. Git 提交和推送
            self.commit_and_push_changes()
            
            # 6. 生成执行报告
            report_file = self.generate_execution_report()
            
            logger.info(f"Sync execution pipeline completed: {self.results['status']}")
            logger.info(f"Report saved to: {report_file}")
            
            return self.results['status'] in ['completed_successfully', 'completed_with_errors']
            
        except Exception as e:
            logger.error(f"Sync execution pipeline failed: {e}")
            self.results['status'] = 'failed'
            self.results['errors'].append(str(e))
            return False

def main():
    """主函数"""
    print("🚀 PC28 数据同步推送执行器")
    print("=" * 50)
    
    # 创建执行管理器
    executor = SyncExecutionManager()
    
    # 执行完整流程
    success = executor.execute_full_sync_pipeline()
    
    # 输出结果摘要
    print("\n📊 执行结果摘要:")
    print(f"执行ID: {executor.execution_id}")
    print(f"状态: {executor.results['status']}")
    print(f"总步骤: {executor.results.get('summary', {}).get('total_steps', 0)}")
    print(f"成功步骤: {executor.results.get('summary', {}).get('successful_steps', 0)}")
    print(f"失败步骤: {executor.results.get('summary', {}).get('failed_steps', 0)}")
    print(f"成功率: {executor.results.get('summary', {}).get('success_rate', '0%')}")
    
    if success:
        print("\n✅ 同步推送执行成功!")
    else:
        print("\n❌ 同步推送执行失败!")
        sys.exit(1)

if __name__ == "__main__":
    main()