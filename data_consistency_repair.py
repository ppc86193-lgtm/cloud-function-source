#!/usr/bin/env python3
"""
数据一致性修复系统
修复云上数据的大小写不一致问题，特别是单双（odd_even）和大小（size）字段
"""

import logging
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_consistency_repair.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataConsistencyRepair:
    def __init__(self):
        """初始化数据一致性修复系统"""
        self.client = bigquery.Client()
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.table_id = "draws_14w_clean"
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
    def check_data_consistency(self):
        """检查数据一致性问题"""
        logger.info("开始检查数据一致性...")
        
        # 检查odd_even字段的不一致性
        query_odd_even = f"""
        SELECT 
            issue,
            timestamp,
            a, b, c, sum, tail,
            odd_even,
            odd_even_calculated,
            CASE 
                WHEN MOD(a + b + c, 2) = 0 THEN 'even'
                ELSE 'odd'
            END as correct_odd_even
        FROM `{self.full_table_id}`
        WHERE odd_even != odd_even_calculated 
           OR odd_even = 'unknown'
           OR odd_even_calculated IS NULL
        ORDER BY timestamp DESC
        LIMIT 100
        """
        
        # 检查size字段的不一致性
        query_size = f"""
        SELECT 
            issue,
            timestamp,
            a, b, c, sum, tail,
            size,
            size_calculated,
            CASE 
                WHEN (a + b + c) >= 14 THEN 'large'
                ELSE 'small'
            END as correct_size
        FROM `{self.full_table_id}`
        WHERE size != size_calculated 
           OR size = 'unknown'
           OR size_calculated IS NULL
        ORDER BY timestamp DESC
        LIMIT 100
        """
        
        try:
            # 执行查询
            odd_even_issues = self.client.query(query_odd_even).to_dataframe()
            size_issues = self.client.query(query_size).to_dataframe()
            
            logger.info(f"发现 {len(odd_even_issues)} 条odd_even不一致记录")
            logger.info(f"发现 {len(size_issues)} 条size不一致记录")
            
            return odd_even_issues, size_issues
            
        except Exception as e:
            logger.error(f"检查数据一致性时出错: {e}")
            return None, None
    
    def repair_odd_even_consistency(self):
        """修复odd_even字段的一致性"""
        logger.info("开始修复odd_even字段一致性...")
        
        # 更新odd_even字段，使其与计算值一致
        update_query = f"""
        UPDATE `{self.full_table_id}`
        SET 
            odd_even = CASE 
                WHEN MOD(a + b + c, 2) = 0 THEN 'even'
                ELSE 'odd'
            END,
            odd_even_calculated = CASE 
                WHEN MOD(a + b + c, 2) = 0 THEN 'even'
                ELSE 'odd'
            END
        WHERE odd_even = 'unknown' 
           OR odd_even != odd_even_calculated
           OR odd_even_calculated IS NULL
        """
        
        try:
            job = self.client.query(update_query)
            job.result()  # 等待查询完成
            logger.info(f"odd_even字段修复完成，影响行数: {job.num_dml_affected_rows}")
            return True
        except Exception as e:
            logger.error(f"修复odd_even字段时出错: {e}")
            return False
    
    def repair_size_consistency(self):
        """修复size字段的一致性"""
        logger.info("开始修复size字段一致性...")
        
        # 更新size字段，使其与计算值一致
        update_query = f"""
        UPDATE `{self.full_table_id}`
        SET 
            size = CASE 
                WHEN (a + b + c) >= 14 THEN 'large'
                ELSE 'small'
            END,
            size_calculated = CASE 
                WHEN (a + b + c) >= 14 THEN 'large'
                ELSE 'small'
            END
        WHERE size = 'unknown' 
           OR size != size_calculated
           OR size_calculated IS NULL
        """
        
        try:
            job = self.client.query(update_query)
            job.result()  # 等待查询完成
            logger.info(f"size字段修复完成，影响行数: {job.num_dml_affected_rows}")
            return True
        except Exception as e:
            logger.error(f"修复size字段时出错: {e}")
            return False
    
    def verify_repairs(self):
        """验证修复结果"""
        logger.info("验证修复结果...")
        
        # 检查是否还有不一致的数据
        verification_query = f"""
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN odd_even = 'unknown' THEN 1 ELSE 0 END) as unknown_odd_even,
            SUM(CASE WHEN size = 'unknown' THEN 1 ELSE 0 END) as unknown_size,
            SUM(CASE WHEN odd_even != odd_even_calculated THEN 1 ELSE 0 END) as inconsistent_odd_even,
            SUM(CASE WHEN size != size_calculated THEN 1 ELSE 0 END) as inconsistent_size
        FROM `{self.full_table_id}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        """
        
        try:
            result = self.client.query(verification_query).to_dataframe()
            logger.info("修复验证结果:")
            logger.info(f"总记录数: {result.iloc[0]['total_records']}")
            logger.info(f"unknown odd_even: {result.iloc[0]['unknown_odd_even']}")
            logger.info(f"unknown size: {result.iloc[0]['unknown_size']}")
            logger.info(f"不一致的odd_even: {result.iloc[0]['inconsistent_odd_even']}")
            logger.info(f"不一致的size: {result.iloc[0]['inconsistent_size']}")
            
            return result
        except Exception as e:
            logger.error(f"验证修复结果时出错: {e}")
            return None
    
    def create_backup_table(self):
        """创建备份表"""
        backup_table_id = f"{self.table_id}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_full_table_id = f"{self.project_id}.{self.dataset_id}.{backup_table_id}"
        
        logger.info(f"创建备份表: {backup_table_id}")
        
        backup_query = f"""
        CREATE TABLE `{backup_full_table_id}` AS
        SELECT * FROM `{self.full_table_id}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        """
        
        try:
            job = self.client.query(backup_query)
            job.result()
            logger.info(f"备份表创建成功: {backup_table_id}")
            return backup_table_id
        except Exception as e:
            logger.error(f"创建备份表时出错: {e}")
            return None
    
    def run_full_repair(self):
        """运行完整的修复流程"""
        logger.info("开始完整的数据一致性修复流程...")
        
        # 1. 创建备份
        backup_table = self.create_backup_table()
        if not backup_table:
            logger.error("创建备份失败，终止修复流程")
            return False
        
        # 2. 检查数据一致性
        odd_even_issues, size_issues = self.check_data_consistency()
        if odd_even_issues is None or size_issues is None:
            logger.error("检查数据一致性失败，终止修复流程")
            return False
        
        # 3. 修复odd_even字段
        if not self.repair_odd_even_consistency():
            logger.error("修复odd_even字段失败")
            return False
        
        # 4. 修复size字段
        if not self.repair_size_consistency():
            logger.error("修复size字段失败")
            return False
        
        # 5. 验证修复结果
        verification_result = self.verify_repairs()
        if verification_result is None:
            logger.error("验证修复结果失败")
            return False
        
        logger.info("数据一致性修复流程完成！")
        return True

def main():
    """主函数"""
    repair_system = DataConsistencyRepair()
    
    # 运行完整修复流程
    success = repair_system.run_full_repair()
    
    if success:
        logger.info("数据一致性修复成功完成！")
    else:
        logger.error("数据一致性修复失败！")
        exit(1)

if __name__ == "__main__":
    main()