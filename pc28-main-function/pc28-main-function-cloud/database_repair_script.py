#!/usr/bin/env python3
"""
PC28数据库修复脚本
基于诊断结果修复数据库问题
"""

import os
import json
import logging
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/database_repair.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseRepair:
    def __init__(self):
        self.client = bigquery.Client(project='wprojectl')
        self.dataset_id = 'pc28_lab'
        self.main_table = 'draws_14w_clean'
        self.repair_results = {}
        
    def remove_duplicate_records(self):
        """清理重复数据，保留最新的记录"""
        logger.info("开始清理重复数据...")
        
        try:
            # 创建临时表，去除重复数据
            temp_table = f"{self.main_table}_temp_dedup"
            
            # 删除重复数据的SQL，保留每个issue的最新记录
            dedup_query = f"""
            CREATE OR REPLACE TABLE `wprojectl.{self.dataset_id}.{temp_table}` AS
            SELECT * EXCEPT(row_num)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY issue 
                        ORDER BY timestamp DESC, 
                                CASE WHEN a IS NOT NULL THEN 1 ELSE 2 END,
                                CASE WHEN b IS NOT NULL THEN 1 ELSE 2 END,
                                CASE WHEN c IS NOT NULL THEN 1 ELSE 2 END
                    ) as row_num
                FROM `wprojectl.{self.dataset_id}.{self.main_table}`
            )
            WHERE row_num = 1
            """
            
            # 执行去重查询
            job = self.client.query(dedup_query)
            job.result()  # 等待完成
            
            # 获取去重前后的行数
            original_count_query = f"SELECT COUNT(*) as count FROM `wprojectl.{self.dataset_id}.{self.main_table}`"
            original_count = self.client.query(original_count_query).to_dataframe().iloc[0]['count']
            
            temp_count_query = f"SELECT COUNT(*) as count FROM `wprojectl.{self.dataset_id}.{temp_table}`"
            temp_count = self.client.query(temp_count_query).to_dataframe().iloc[0]['count']
            
            # 备份原表
            backup_table = f"{self.main_table}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_query = f"""
            CREATE TABLE `wprojectl.{self.dataset_id}.{backup_table}` AS
            SELECT * FROM `wprojectl.{self.dataset_id}.{self.main_table}`
            """
            self.client.query(backup_query).result()
            
            # 替换原表
            replace_query = f"""
            CREATE OR REPLACE TABLE `wprojectl.{self.dataset_id}.{self.main_table}` AS
            SELECT * FROM `wprojectl.{self.dataset_id}.{temp_table}`
            """
            self.client.query(replace_query).result()
            
            # 删除临时表
            self.client.delete_table(f"wprojectl.{self.dataset_id}.{temp_table}")
            
            removed_count = original_count - temp_count
            
            self.repair_results['duplicate_removal'] = {
                'status': 'success',
                'original_count': int(original_count),
                'final_count': int(temp_count),
                'removed_count': int(removed_count),
                'backup_table': backup_table
            }
            
            logger.info(f"重复数据清理完成: 原始{original_count}行 -> 清理后{temp_count}行 (删除{removed_count}行)")
            logger.info(f"原始数据已备份到: {backup_table}")
            
            return True
            
        except Exception as e:
            logger.error(f"清理重复数据失败: {e}")
            self.repair_results['duplicate_removal'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def fix_null_values(self):
        """修复空值问题"""
        logger.info("开始修复空值...")
        
        try:
            # 更新空值字段
            update_query = f"""
            UPDATE `wprojectl.{self.dataset_id}.{self.main_table}`
            SET 
                sum = CASE WHEN sum IS NULL AND a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL 
                          THEN a + b + c ELSE sum END,
                tail = CASE WHEN tail IS NULL AND sum IS NOT NULL 
                           THEN MOD(sum, 10) ELSE tail END,
                hour = CASE WHEN hour IS NULL AND timestamp IS NOT NULL 
                           THEN EXTRACT(HOUR FROM timestamp) ELSE hour END,
                session = CASE WHEN session IS NULL AND timestamp IS NOT NULL 
                              THEN CASE 
                                  WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 9 AND 11 THEN 'morning'
                                  WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 14 AND 16 THEN 'afternoon'  
                                  WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 20 AND 22 THEN 'evening'
                                  ELSE 'other'
                              END ELSE session END,
                source = CASE WHEN source IS NULL THEN 'api_auto_fetch' ELSE source END,
                size = CASE WHEN size IS NULL AND sum IS NOT NULL 
                           THEN CASE WHEN sum <= 13 THEN 'small' ELSE 'big' END ELSE size END,
                odd_even = CASE WHEN odd_even IS NULL AND sum IS NOT NULL 
                               THEN CASE WHEN MOD(sum, 2) = 0 THEN 'even' ELSE 'odd' END ELSE odd_even END,
                size_calculated = CASE WHEN size_calculated IS NULL AND sum IS NOT NULL 
                                      THEN CASE WHEN sum <= 13 THEN 'small' ELSE 'big' END ELSE size_calculated END,
                odd_even_calculated = CASE WHEN odd_even_calculated IS NULL AND sum IS NOT NULL 
                                          THEN CASE WHEN MOD(sum, 2) = 0 THEN 'even' ELSE 'odd' END ELSE odd_even_calculated END
            WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL
            """
            
            job = self.client.query(update_query)
            result = job.result()
            
            # 检查更新后的空值情况
            check_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(sum) as valid_sum,
                COUNT(tail) as valid_tail,
                COUNT(hour) as valid_hour,
                COUNT(session) as valid_session,
                COUNT(source) as valid_source,
                COUNT(size) as valid_size,
                COUNT(odd_even) as valid_odd_even
            FROM `wprojectl.{self.dataset_id}.{self.main_table}`
            WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL
            """
            
            check_result = self.client.query(check_query).to_dataframe().iloc[0]
            
            self.repair_results['null_value_fix'] = {
                'status': 'success',
                'updated_rows': result.num_dml_affected_rows if hasattr(result, 'num_dml_affected_rows') else 'unknown',
                'completeness_after_fix': {
                    'sum': int(check_result['valid_sum']),
                    'tail': int(check_result['valid_tail']),
                    'hour': int(check_result['valid_hour']),
                    'session': int(check_result['valid_session']),
                    'source': int(check_result['valid_source']),
                    'size': int(check_result['valid_size']),
                    'odd_even': int(check_result['valid_odd_even'])
                }
            }
            
            logger.info("空值修复完成")
            return True
            
        except Exception as e:
            logger.error(f"修复空值失败: {e}")
            self.repair_results['null_value_fix'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def optimize_table_structure(self):
        """优化表结构"""
        logger.info("开始优化表结构...")
        
        try:
            # 重新组织表以提高查询性能
            optimize_query = f"""
            CREATE OR REPLACE TABLE `wprojectl.{self.dataset_id}.{self.main_table}`
            PARTITION BY DATE(timestamp)
            CLUSTER BY issue, timestamp
            AS
            SELECT * FROM `wprojectl.{self.dataset_id}.{self.main_table}`
            ORDER BY timestamp DESC, issue DESC
            """
            
            job = self.client.query(optimize_query)
            job.result()
            
            self.repair_results['table_optimization'] = {
                'status': 'success',
                'partitioning': 'DATE(timestamp)',
                'clustering': 'issue, timestamp'
            }
            
            logger.info("表结构优化完成")
            return True
            
        except Exception as e:
            logger.error(f"表结构优化失败: {e}")
            self.repair_results['table_optimization'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def create_monitoring_view(self):
        """创建监控视图"""
        logger.info("创建数据监控视图...")
        
        try:
            # 创建数据质量监控视图
            monitoring_view_query = f"""
            CREATE OR REPLACE VIEW `wprojectl.{self.dataset_id}.data_quality_monitor` AS
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as total_records,
                COUNT(DISTINCT issue) as unique_issues,
                COUNT(CASE WHEN a IS NULL OR b IS NULL OR c IS NULL THEN 1 END) as incomplete_records,
                AVG(sum) as avg_sum,
                MIN(timestamp) as earliest_time,
                MAX(timestamp) as latest_time,
                COUNT(CASE WHEN source = 'api_auto_fetch' THEN 1 END) as api_records,
                ROUND(COUNT(CASE WHEN a IS NULL OR b IS NULL OR c IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as incomplete_percentage
            FROM `wprojectl.{self.dataset_id}.{self.main_table}`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
            """
            
            self.client.query(monitoring_view_query).result()
            
            self.repair_results['monitoring_view'] = {
                'status': 'success',
                'view_name': 'data_quality_monitor'
            }
            
            logger.info("监控视图创建完成")
            return True
            
        except Exception as e:
            logger.error(f"创建监控视图失败: {e}")
            self.repair_results['monitoring_view'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_full_repair(self):
        """运行完整的数据库修复"""
        logger.info("开始数据库修复...")
        
        repair_steps = [
            ('清理重复数据', self.remove_duplicate_records),
            ('修复空值', self.fix_null_values),
            ('优化表结构', self.optimize_table_structure),
            ('创建监控视图', self.create_monitoring_view)
        ]
        
        success_count = 0
        for step_name, step_func in repair_steps:
            logger.info(f"执行步骤: {step_name}")
            if step_func():
                success_count += 1
                logger.info(f"✅ {step_name} 完成")
            else:
                logger.error(f"❌ {step_name} 失败")
        
        self.repair_results['summary'] = {
            'timestamp': datetime.now().isoformat(),
            'total_steps': len(repair_steps),
            'successful_steps': success_count,
            'overall_success': success_count == len(repair_steps)
        }
        
        return self.repair_results
    
    def save_repair_report(self):
        """保存修复报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f'logs/database_repair_report_{timestamp}.json'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.repair_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"修复报告已保存: {report_file}")
        return report_file

def main():
    """主函数"""
    try:
        # 确保日志目录存在
        os.makedirs('logs', exist_ok=True)
        
        # 运行修复
        repair = DatabaseRepair()
        results = repair.run_full_repair()
        
        # 保存报告
        report_file = repair.save_repair_report()
        
        # 打印摘要
        summary = results.get('summary', {})
        print("\n" + "="*50)
        print("数据库修复摘要")
        print("="*50)
        print(f"总步骤数: {summary.get('total_steps', 0)}")
        print(f"成功步骤: {summary.get('successful_steps', 0)}")
        print(f"整体状态: {'✅ 成功' if summary.get('overall_success') else '❌ 部分失败'}")
        
        # 显示各步骤结果
        for key, value in results.items():
            if key != 'summary' and isinstance(value, dict):
                status = value.get('status', 'unknown')
                print(f"{key}: {'✅' if status == 'success' else '❌'} {status}")
        
        print(f"\n详细报告: {report_file}")
        print("="*50)
        
        return 0 if summary.get('overall_success') else 1
        
    except Exception as e:
        logger.error(f"修复过程出错: {e}")
        return 1

if __name__ == "__main__":
    exit(main())