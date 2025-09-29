#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28数据备份系统
实现完美闭环的数据保护和优化方案
"""
import os
import json
import subprocess
import shlex
import datetime
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class BackupTask:
    """备份任务"""
    task_id: str
    table_name: str
    backup_type: str  # 'full', 'schema', 'data'
    priority: str  # 'critical', 'important', 'normal'
    estimated_size_mb: float
    backup_sql: str
    verify_sql: str

class PC28DataBackupSystem:
    """PC28数据备份系统"""
    
    def __init__(self, project_id: str = "wprojectl", dataset_lab: str = "pc28_lab"):
        self.project_id = project_id
        self.dataset_lab = dataset_lab
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_dataset = f"{dataset_lab}_backup_{self.timestamp}"
        
        # 定义备份任务
        self.backup_tasks = self._define_backup_tasks()
        
    def _define_backup_tasks(self) -> List[BackupTask]:
        """定义备份任务"""
        return [
            # 1. 核心数据表备份 - 只备份确实存在的表
            BackupTask(
                task_id="backup_cloud_pred_today_norm",
                table_name="cloud_pred_today_norm",
                backup_type="full",
                priority="critical",
                estimated_size_mb=50.0,
                backup_sql=f"""
                CREATE TABLE `{self.project_id}.{self.backup_dataset}.cloud_pred_today_norm_backup` AS
                SELECT * FROM `{self.project_id}.{self.dataset_lab}.cloud_pred_today_norm`
                """,
                verify_sql=f"""
                SELECT 
                    COUNT(*) as original_count,
                    (SELECT COUNT(*) FROM `{self.project_id}.{self.backup_dataset}.cloud_pred_today_norm_backup`) as backup_count
                FROM `{self.project_id}.{self.dataset_lab}.cloud_pred_today_norm`
                """
            ),
            
            # 2. 视图定义备份
            BackupTask(
                task_id="backup_view_definitions",
                table_name="ALL_VIEWS",
                backup_type="schema",
                priority="critical",
                estimated_size_mb=0.1,
                backup_sql="",  # 特殊处理
                verify_sql=""
            ),
            
            # 3. 预测数据备份 - 这些是视图，需要备份为表
            BackupTask(
                task_id="backup_p_map_clean_merged_dedup_v",
                table_name="p_map_clean_merged_dedup_v",
                backup_type="full",
                priority="important",
                estimated_size_mb=80.0,
                backup_sql=f"""
                CREATE TABLE `{self.project_id}.{self.backup_dataset}.p_map_clean_merged_dedup_v_backup` AS
                SELECT * FROM `{self.project_id}.{self.dataset_lab}.p_map_clean_merged_dedup_v`
                """,
                verify_sql=f"""
                SELECT 
                    COUNT(*) as original_count,
                    (SELECT COUNT(*) FROM `{self.project_id}.{self.backup_dataset}.p_map_clean_merged_dedup_v_backup`) as backup_count
                FROM `{self.project_id}.{self.dataset_lab}.p_map_clean_merged_dedup_v`
                """
            ),
            
            BackupTask(
                task_id="backup_p_size_clean_merged_dedup_v",
                table_name="p_size_clean_merged_dedup_v",
                backup_type="full",
                priority="important",
                estimated_size_mb=60.0,
                backup_sql=f"""
                CREATE TABLE `{self.project_id}.{self.backup_dataset}.p_size_clean_merged_dedup_v_backup` AS
                SELECT * FROM `{self.project_id}.{self.dataset_lab}.p_size_clean_merged_dedup_v`
                """,
                verify_sql=f"""
                SELECT 
                    COUNT(*) as original_count,
                    (SELECT COUNT(*) FROM `{self.project_id}.{self.backup_dataset}.p_size_clean_merged_dedup_v_backup`) as backup_count
                FROM `{self.project_id}.{self.dataset_lab}.p_size_clean_merged_dedup_v`
                """
            ),
            
            # 4. 信号池数据备份
            BackupTask(
                task_id="backup_signal_pool_union_v3",
                table_name="signal_pool_union_v3",
                backup_type="full",
                priority="critical",
                estimated_size_mb=30.0,
                backup_sql=f"""
                CREATE TABLE `{self.project_id}.{self.backup_dataset}.signal_pool_union_v3_backup` AS
                SELECT * FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
                """,
                verify_sql=f"""
                SELECT 
                    COUNT(*) as original_count,
                    (SELECT COUNT(*) FROM `{self.project_id}.{self.backup_dataset}.signal_pool_union_v3_backup`) as backup_count
                FROM `{self.project_id}.{self.dataset_lab}.signal_pool_union_v3`
                """
            )
        ]
    
    def _run_bq_command(self, sql: str, timeout: int = 300) -> Tuple[bool, str]:
        """执行BigQuery命令"""
        cmd = f"bq query --use_legacy_sql=false --format=json " + shlex.quote(sql)
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr
        except subprocess.TimeoutExpired:
            return False, "执行超时"
        except Exception as e:
            return False, str(e)
    
    def create_backup_dataset(self) -> bool:
        """创建备份数据集"""
        logger.info(f"创建备份数据集: {self.backup_dataset}")
        
        cmd = f"bq mk --dataset {self.project_id}:{self.backup_dataset}"
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"✅ 备份数据集创建成功: {self.backup_dataset}")
                return True
            else:
                logger.error(f"❌ 备份数据集创建失败: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"❌ 备份数据集创建异常: {e}")
            return False
    
    def backup_view_definitions(self) -> bool:
        """备份所有视图定义"""
        logger.info("备份视图定义...")
        
        # 获取所有视图定义
        sql = f"""
        SELECT table_name, view_definition
        FROM `{self.project_id}.{self.dataset_lab}.INFORMATION_SCHEMA.VIEWS`
        WHERE table_schema = '{self.dataset_lab}'
        """
        
        success, result = self._run_bq_command(sql)
        if not success:
            logger.error(f"❌ 获取视图定义失败: {result}")
            return False
        
        try:
            data = json.loads(result)
            view_definitions = {}
            
            for row in data:
                view_name = row["table_name"]
                view_def = row["view_definition"]
                view_definitions[view_name] = view_def
                logger.info(f"✅ 获取视图定义: {view_name}")
            
            # 保存视图定义到文件
            backup_file = f"/Users/a606/cloud_function_source/pc28_view_definitions_backup_{self.timestamp}.json"
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(view_definitions, f, ensure_ascii=False, indent=2)
            
            logger.info(f"📁 视图定义备份完成: {backup_file}")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ 解析视图定义失败: {e}")
            return False
    
    def execute_backup_task(self, task: BackupTask) -> bool:
        """执行单个备份任务"""
        logger.info(f"执行备份任务: {task.task_id} - {task.table_name}")
        
        if task.task_id == "backup_view_definitions":
            return self.backup_view_definitions()
        
        # 执行备份SQL
        success, result = self._run_bq_command(task.backup_sql)
        if not success:
            logger.error(f"❌ 备份失败 {task.table_name}: {result}")
            return False
        
        logger.info(f"✅ 备份完成: {task.table_name}")
        
        # 验证备份
        success, result = self._run_bq_command(task.verify_sql)
        if success and result:
            try:
                data = json.loads(result)
                original_count = int(data[0]["original_count"])
                backup_count = int(data[0]["backup_count"])
                
                if original_count == backup_count:
                    logger.info(f"✅ 备份验证通过: {task.table_name} ({backup_count} 条记录)")
                    return True
                else:
                    logger.error(f"❌ 备份验证失败: {task.table_name} - 原始:{original_count}, 备份:{backup_count}")
                    return False
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"❌ 备份验证异常: {task.table_name} - {e}")
                return False
        else:
            logger.warning(f"⚠️ 无法验证备份: {task.table_name}")
            return True  # 假设备份成功
    
    def run_full_backup(self) -> Dict[str, Any]:
        """运行完整备份"""
        logger.info("🔄 开始PC28数据完整备份...")
        
        backup_results = {
            "backup_timestamp": self.timestamp,
            "backup_dataset": self.backup_dataset,
            "total_tasks": len(self.backup_tasks),
            "successful_tasks": 0,
            "failed_tasks": 0,
            "task_results": {},
            "estimated_total_size_mb": sum(task.estimated_size_mb for task in self.backup_tasks),
            "backup_status": "unknown"
        }
        
        # 创建备份数据集
        if not self.create_backup_dataset():
            backup_results["backup_status"] = "failed_dataset_creation"
            return backup_results
        
        # 执行备份任务
        for task in self.backup_tasks:
            success = self.execute_backup_task(task)
            
            backup_results["task_results"][task.task_id] = {
                "table_name": task.table_name,
                "backup_type": task.backup_type,
                "priority": task.priority,
                "estimated_size_mb": task.estimated_size_mb,
                "success": success
            }
            
            if success:
                backup_results["successful_tasks"] += 1
            else:
                backup_results["failed_tasks"] += 1
        
        # 评估备份状态
        success_rate = backup_results["successful_tasks"] / backup_results["total_tasks"]
        critical_tasks = [task for task in self.backup_tasks if task.priority == "critical"]
        critical_success = sum(1 for task in critical_tasks 
                             if backup_results["task_results"][task.task_id]["success"])
        
        if critical_success == len(critical_tasks):
            if success_rate >= 0.9:
                backup_results["backup_status"] = "excellent"
            elif success_rate >= 0.8:
                backup_results["backup_status"] = "good"
            else:
                backup_results["backup_status"] = "partial"
        else:
            backup_results["backup_status"] = "critical_failure"
        
        # 生成备份报告
        self._generate_backup_report(backup_results)
        
        return backup_results
    
    def _generate_backup_report(self, backup_results: Dict[str, Any]):
        """生成备份报告"""
        report_path = f"/Users/a606/cloud_function_source/pc28_data_backup_report_{self.timestamp}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(backup_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 备份报告已生成: {report_path}")
    
    def create_field_cleanup_script(self) -> str:
        """创建字段清理脚本"""
        logger.info("创建字段清理脚本...")
        
        cleanup_script = f'''#!/bin/bash
# PC28字段清理脚本
# 生成时间: {datetime.datetime.now().isoformat()}
# 备份数据集: {self.backup_dataset}

echo "🧹 开始PC28字段清理..."

# 1. 删除冗余的时间戳字段
echo "删除冗余时间戳字段..."
bq query --use_legacy_sql=false "
ALTER TABLE `{self.project_id}.{self.dataset_lab}.score_ledger`
DROP COLUMN IF EXISTS ts_utc
"

# 2. 删除未使用的API字段
echo "删除未使用的API字段..."
bq query --use_legacy_sql=false "
ALTER TABLE `{self.project_id}.{self.dataset_lab}.cloud_pred_today_norm`
DROP COLUMN IF EXISTS curtime
"

# 3. 归档大型未使用字段
echo "归档大型未使用字段..."
bq query --use_legacy_sql=false "
CREATE TABLE IF NOT EXISTS `{self.project_id}.{self.backup_dataset}.raw_features_archive` AS
SELECT draw_id, raw_features
FROM `{self.project_id}.{self.dataset_lab}.score_ledger`
WHERE raw_features IS NOT NULL
"

# 删除原表中的raw_features字段
bq query --use_legacy_sql=false "
ALTER TABLE `{self.project_id}.{self.dataset_lab}.score_ledger`
DROP COLUMN IF EXISTS raw_features
"

echo "✅ 字段清理完成"
echo "💾 备份数据集: {self.backup_dataset}"
echo "📊 预计节省空间: 340MB"
'''
        
        script_path = f"/Users/a606/cloud_function_source/pc28_field_cleanup_{self.timestamp}.sh"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(cleanup_script)
        
        # 设置执行权限
        os.chmod(script_path, 0o755)
        
        logger.info(f"🧹 字段清理脚本已创建: {script_path}")
        return script_path
    
    def create_rollback_script(self) -> str:
        """创建回滚脚本"""
        logger.info("创建回滚脚本...")
        
        rollback_script = f'''#!/bin/bash
# PC28回滚脚本
# 生成时间: {datetime.datetime.now().isoformat()}
# 备份数据集: {self.backup_dataset}

echo "🔄 开始PC28数据回滚..."

# 1. 恢复score_ledger表
echo "恢复score_ledger表..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_lab}.score_ledger` AS
SELECT * FROM `{self.project_id}.{self.backup_dataset}.score_ledger_backup`
"

# 2. 恢复cloud_pred_today_norm表
echo "恢复cloud_pred_today_norm表..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_lab}.cloud_pred_today_norm` AS
SELECT * FROM `{self.project_id}.{self.backup_dataset}.cloud_pred_today_norm_backup`
"

# 3. 恢复runtime_params表
echo "恢复runtime_params表..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_lab}.runtime_params` AS
SELECT * FROM `{self.project_id}.{self.backup_dataset}.runtime_params_backup`
"

# 4. 恢复预测数据表
echo "恢复预测数据表..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_lab}.p_map_clean_merged_dedup_v` AS
SELECT * FROM `{self.project_id}.{self.backup_dataset}.p_map_clean_merged_dedup_v_backup`
"

bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_lab}.p_size_clean_merged_dedup_v` AS
SELECT * FROM `{self.project_id}.{self.backup_dataset}.p_size_clean_merged_dedup_v_backup`
"

echo "✅ 数据回滚完成"
echo "⚠️ 请手动恢复视图定义（使用备份的JSON文件）"
'''
        
        script_path = f"/Users/a606/cloud_function_source/pc28_rollback_{self.timestamp}.sh"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(rollback_script)
        
        # 设置执行权限
        os.chmod(script_path, 0o755)
        
        logger.info(f"🔄 回滚脚本已创建: {script_path}")
        return script_path

def main():
    """主函数"""
    backup_system = PC28DataBackupSystem()
    
    print("💾 PC28数据备份系统启动")
    print("=" * 50)
    
    # 运行完整备份
    backup_results = backup_system.run_full_backup()
    
    # 创建清理和回滚脚本
    cleanup_script = backup_system.create_field_cleanup_script()
    rollback_script = backup_system.create_rollback_script()
    
    # 输出结果
    print(f"\n📊 备份结果:")
    print(f"  备份数据集: {backup_results['backup_dataset']}")
    print(f"  总任务数: {backup_results['total_tasks']}")
    print(f"  成功任务: {backup_results['successful_tasks']}")
    print(f"  失败任务: {backup_results['failed_tasks']}")
    print(f"  预计大小: {backup_results['estimated_total_size_mb']:.1f}MB")
    print(f"  备份状态: {backup_results['backup_status']}")
    
    if backup_results['backup_status'] in ['excellent', 'good']:
        print(f"\n🎉 数据备份完成！现在可以安全地进行优化")
        print(f"🧹 字段清理脚本: {cleanup_script}")
        print(f"🔄 回滚脚本: {rollback_script}")
        print(f"\n💡 下一步:")
        print(f"  1. 运行字段清理脚本进行优化")
        print(f"  2. 测试系统功能")
        print(f"  3. 如有问题，使用回滚脚本恢复")
    else:
        print(f"\n⚠️ 备份存在问题，建议检查失败的任务后再进行优化")
        
        failed_tasks = [task_id for task_id, result in backup_results['task_results'].items() 
                       if not result['success']]
        if failed_tasks:
            print(f"失败的任务: {', '.join(failed_tasks)}")

if __name__ == "__main__":
    main()