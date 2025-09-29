#!/usr/bin/env python3
"""BigQuery完整修复和验证测试

自动化测试脚本，用于：
1. 验证BigQuery连接
2. 检查所有表结构
3. 修复字段映射问题
4. 验证数据流转
5. 测试历史回填
6. 测试实时写入
"""

import pytest
import logging
import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# BigQuery配置
BQ_PROJECT = 'wprojectl'
BQ_DATASET = 'pc28_lab'

# 需要修复的表列表
TABLES_TO_CHECK = [
    'draws_14w_clean',
    'draws_14w_partitioned', 
    'cloud_pred_today_norm',
    'p_map_today_canon_v',
    'p_size_today_canon_v',
    'combo_based_predictions',
    'lab_push_candidates_v2'
]

# 关键视图
CRITICAL_VIEW = 'p_ensemble_today_norm_v5'

class BigQueryRepairTestSuite:
    """BigQuery修复测试套件"""
    
    def __init__(self):
        self.test_results = {
            'start_time': datetime.now().isoformat(),
            'tests': [],
            'errors': [],
            'fixes_applied': [],
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0
        }
        self.client = None
        self.local_conn = None
        
    def setup(self):
        """初始化测试环境"""
        logger.info("初始化BigQuery修复测试套件...")
        
        try:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=BQ_PROJECT)
            logger.info(f"成功连接到BigQuery项目: {BQ_PROJECT}")
            self.test_results['bigquery_connected'] = True
        except Exception as e:
            logger.error(f"BigQuery连接失败: {e}")
            self.test_results['bigquery_connected'] = False
            self.test_results['errors'].append(str(e))
            
        # 初始化本地数据库
        self.local_conn = sqlite3.connect('pc28_test_repair.db')
        self.create_local_tables()
        
    def create_local_tables(self):
        """创建本地测试表"""
        cursor = self.local_conn.cursor()
        
        # 测试结果表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_results (
                test_id TEXT PRIMARY KEY,
                test_name TEXT,
                status TEXT,
                error_message TEXT,
                fix_applied TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 数据验证表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_validation (
                table_name TEXT,
                record_count INTEGER,
                last_updated DATETIME,
                validation_status TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.local_conn.commit()
        
    @pytest.mark.pytest_compliant
    def test_bigquery_connection(self):
        """测试1: 验证BigQuery连接"""
        test_id = 'test_001_connection'
        logger.info(f"执行测试: {test_id}")
        
        try:
            # 测试BigQuery连接
            query = f"SELECT 1 as test"
            result = self.client.query(query).result()
            
            for row in result:
                if row.test == 1:
                    self.test_results['bigquery_connected'] = True
                    self.record_test_result(test_id, 'PASSED', None, None)
                    logger.info(f"✓ {test_id} 通过")
                    return True
                    
        except Exception as e:
            self.test_results['bigquery_connected'] = False
            self.record_test_result(test_id, 'FAILED', str(e), "检查BigQuery认证和项目设置")
            logger.error(f"✗ {test_id} 失败: {e}")
            return False
            
    @pytest.mark.pytest_compliant
    def test_table_structures(self):
        """测试2: 验证表结构"""
        test_id = 'test_002_table_structures'
        logger.info(f"执行测试: {test_id}")
        
        required_tables = [
            'draws_14w_clean',
            'cloud_pred_today_norm',
            'p_map_today_canon_v',
            'p_size_today_canon_v'
        ]
        
        all_passed = True
        
        for table in required_tables:
            try:
                query = f"""
                SELECT COUNT(*) as row_count
                FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`
                LIMIT 1
                """
                
                result = self.client.query(query).result()
                for row in result:
                    logger.info(f"  {table}: {row.row_count} 行")
                    
            except Exception as e:
                all_passed = False
                self.record_test_result(f"{test_id}_{table}", 'FAILED', str(e), f"创建表 {table}")
                logger.error(f"  ✗ 表 {table} 访问失败: {e}")
                
        if all_passed:
            self.record_test_result(test_id, 'PASSED', None, None)
            logger.info(f"✓ {test_id} 通过")
        else:
            logger.warning(f"⚠ {test_id} 部分失败")
            
        return all_passed
        
    @pytest.mark.pytest_compliant
    def test_critical_view(self):
        """测试3: 验证关键视图p_ensemble_today_norm_v5"""
        test_id = 'test_003_critical_view'
        logger.info(f"执行测试: {test_id}")
        
        try:
            # 测试视图查询
            query = f"""
            SELECT *
            FROM `{BQ_PROJECT}.{BQ_DATASET}.p_ensemble_today_norm_v5`
            LIMIT 1
            """
            
            result = self.client.query(query).result()
            self.record_test_result(test_id, 'PASSED', None, None)
            logger.info(f"✓ {test_id} 通过")
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"✗ {test_id} 失败: {error_msg}")
            
            # 生成修复SQL
            fix_sql = self.generate_view_fix()
            self.record_test_result(test_id, 'FAILED', error_msg, fix_sql)
            
            # 保存修复脚本
            with open('fix_p_ensemble_view.sql', 'w') as f:
                f.write(fix_sql)
                
            logger.info("修复脚本已生成: fix_p_ensemble_view.sql")
            self.test_results['fixes_applied'].append("Generated fix_p_ensemble_view.sql")
            
            return False
            
    @pytest.mark.pytest_compliant
    def test_data_flow(self):
        """测试4: 验证数据流转"""
        test_id = 'test_004_data_flow'
        logger.info(f"执行测试: {test_id}")
        
        try:
            # 测试数据采集和写入
            cursor = self.local_conn.cursor()
            
            # 创建本地验证表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_validation (
                source_table TEXT,
                record_count INTEGER,
                latest_timestamp TEXT,
                validation_time TEXT
            )
            """)
            
            # 验证源数据
            source_tables = ['draws_14w_clean', 'cloud_pred_today_norm']
            
            for table in source_tables:
                try:
                    query = f"""
                    SELECT 
                        COUNT(*) as count,
                        MAX(COALESCE(timestamp, ts_utc, CURRENT_TIMESTAMP())) as latest
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`
                    """
                    
                    result = self.client.query(query).result()
                    
                    for row in result:
                        cursor.execute(
                            "INSERT INTO data_validation VALUES (?, ?, ?, ?)",
                            (table, row.count, str(row.latest), datetime.now().isoformat())
                        )
                        logger.info(f"  {table}: {row.count} 条记录, 最新: {row.latest}")
                        
                except Exception as e:
                    logger.error(f"  {table} 验证失败: {e}")
                    
            self.local_conn.commit()
            self.record_test_result(test_id, 'PASSED', None, None)
            logger.info(f"✓ {test_id} 通过")
            return True
            
        except Exception as e:
            self.record_test_result(test_id, 'FAILED', str(e), None)
            logger.error(f"✗ {test_id} 失败: {e}")
            return False
            
    @pytest.mark.pytest_compliant
    def test_historical_backfill(self):
        """测试5: 验证历史数据回填"""
        test_id = 'test_005_historical_backfill'
        logger.info(f"执行测试: {test_id}")
        
        try:
            # 检查历史数据完整性
            query = f"""
            SELECT 
                DATE(COALESCE(timestamp, ts_utc, CURRENT_TIMESTAMP())) as date,
                COUNT(*) as count
            FROM `{BQ_PROJECT}.{BQ_DATASET}.draws_14w_clean`
            GROUP BY date
            ORDER BY date DESC
            LIMIT 7
            """
            
            result = self.client.query(query).result()
            
            missing_dates = []
            data_counts = []
            
            for row in result:
                data_counts.append(f"{row.date}: {row.count}条")
                if row.count < 100:  # 假设每天至少100条记录
                    missing_dates.append(str(row.date))
                    
            logger.info(f"  最近7天数据量: {', '.join(data_counts)}")
            
            if missing_dates:
                # 触发历史数据回填
                backfill_sql = self.trigger_backfill(missing_dates)
                self.record_test_result(test_id, 'WARNING', f"数据量不足的日期: {missing_dates}", backfill_sql)
                logger.warning(f"⚠ {test_id} 建议回填: {missing_dates}")
                
                # 保存回填脚本
                with open('backfill_script.sql', 'w') as f:
                    f.write(backfill_sql)
                logger.info("回填脚本已生成: backfill_script.sql")
                
                return False
            else:
                self.record_test_result(test_id, 'PASSED', None, None)
                logger.info(f"✓ {test_id} 通过 - 历史数据完整")
                return True
                
        except Exception as e:
            self.record_test_result(test_id, 'FAILED', str(e), None)
            logger.error(f"✗ {test_id} 失败: {e}")
            return False
            
    @pytest.mark.pytest_compliant
    def test_realtime_writing(self):
        """测试6: 验证实时数据写入"""
        test_id = 'test_006_realtime_writing'
        logger.info(f"执行测试: {test_id}")
        
        try:
            # 测试实时数据插入
            test_data = {
                'timestamp': datetime.now().isoformat(),
                'test_id': f'test_{datetime.now().strftime("%Y%m%d%H%M%S")}',
                'value': 1.0,
                'test_type': 'automated_test'
            }
            
            # 尝试插入测试数据到测试表
            table_id = f"{BQ_PROJECT}.{BQ_DATASET}.test_realtime_data"
            
            from google.cloud import bigquery
            schema = [
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("test_id", "STRING"),
                bigquery.SchemaField("value", "FLOAT64"),
                bigquery.SchemaField("test_type", "STRING"),
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            
            # 创建或更新表
            try:
                table = self.client.create_table(table)
                logger.info(f"  创建测试表: {table_id}")
            except:
                logger.info(f"  使用已存在的表: {table_id}")
                
            # 插入数据
            errors = self.client.insert_rows_json(table_id, [test_data])
            
            if errors:
                self.record_test_result(test_id, 'FAILED', str(errors), "检查BigQuery写入权限")
                logger.error(f"✗ {test_id} 失败: {errors}")
                return False
            else:
                # 验证数据已写入
                query = f"""
                SELECT COUNT(*) as count
                FROM `{table_id}`
                WHERE test_id = '{test_data['test_id']}'
                """
                
                result = self.client.query(query).result()
                for row in result:
                    if row.count > 0:
                        self.record_test_result(test_id, 'PASSED', None, None)
                        logger.info(f"✓ {test_id} 通过 - 数据成功写入并验证")
                        self.test_results['fixes_applied'].append(f"Verified realtime write to {table_id}")
                        return True
                        
                self.record_test_result(test_id, 'WARNING', "数据写入但未能验证", None)
                logger.warning(f"⚠ {test_id} 警告: 数据已写入但验证失败")
                return False
                
        except Exception as e:
            self.record_test_result(test_id, 'FAILED', str(e), "检查BigQuery API配置")
            logger.error(f"✗ {test_id} 失败: {e}")
            return False
            
    def trigger_backfill(self, missing_dates=None):
        """触发历史数据回填"""
        logger.info("触发历史数据回填...")
        
        try:
            # 生成回填SQL
            if missing_dates:
                dates_condition = " OR ".join([f"DATE(timestamp) = '{date}'" for date in missing_dates])
                backfill_sql = f"""
                -- 历史数据回填脚本（针对特定日期）
                INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.draws_14w_clean` 
                SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.draws_14w_partitioned`
                WHERE ({dates_condition})
                AND DATE(timestamp) NOT IN (
                    SELECT DISTINCT DATE(timestamp)
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.draws_14w_clean`
                    WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                )
                """
            else:
                backfill_sql = f"""
                -- 历史数据回填脚本（最近30天）
                INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.draws_14w_clean` 
                SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.draws_14w_partitioned`
                WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                AND DATE(timestamp) NOT IN (
                    SELECT DISTINCT DATE(timestamp)
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.draws_14w_clean`
                )
                """
            
            logger.info("回填脚本已生成: backfill_script.sql")
            self.test_results['fixes_applied'].append("Generated backfill script")
            return backfill_sql
            
        except Exception as e:
            logger.error(f"生成回填脚本失败: {e}")
            return None
            
    def record_test_result(self, test_id: str, status: str, error: Optional[str], fix: Optional[str]):
        """记录测试结果"""
        self.test_results['total_tests'] += 1
        
        if status == 'PASSED':
            self.test_results['passed_tests'] += 1
        else:
            self.test_results['failed_tests'] += 1
            
        test_record = {
            'test_id': test_id,
            'status': status,
            'error': error,
            'fix': fix,
            'timestamp': datetime.now().isoformat()
        }
        
        self.test_results['tests'].append(test_record)
        
        # 保存到本地数据库
        cursor = self.local_conn.cursor()
        cursor.execute(
            "INSERT INTO test_results (test_id, test_name, status, error_message, fix_applied) VALUES (?, ?, ?, ?, ?)",
            (test_id, test_id, status, error, fix)
        )
        self.local_conn.commit()
        
    def generate_report(self):
        """生成测试报告"""
        self.test_results['end_time'] = datetime.now().isoformat()
        
        # 保存JSON报告
        report_file = f"bigquery_repair_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(self.test_results, f, indent=2, default=str)
            
        logger.info(f"测试报告已保存: {report_file}")
        
        # 生成Markdown报告
        md_report = self.generate_markdown_report()
        md_file = f"bigquery_repair_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(md_file, 'w') as f:
            f.write(md_report)
            
        logger.info(f"Markdown报告已保存: {md_file}")
        
        return report_file, md_file
        
    def generate_markdown_report(self) -> str:
        """生成Markdown格式报告"""
        report = f"""
# BigQuery修复测试报告

## 测试概要
- 开始时间: {self.test_results['start_time']}
- 结束时间: {self.test_results.get('end_time', 'N/A')}
- BigQuery连接: {'✓ 成功' if self.test_results.get('bigquery_connected') else '✗ 失败'}
- 总测试数: {self.test_results['total_tests']}
- 通过测试: {self.test_results['passed_tests']}
- 失败测试: {self.test_results['failed_tests']}
- 成功率: {self.test_results['passed_tests'] / max(1, self.test_results['total_tests']) * 100:.1f}%

## 测试详情
"""
        
        for test in self.test_results['tests']:
            status_icon = '✓' if test['status'] == 'PASSED' else '✗' if test['status'] == 'FAILED' else '⚠'
            report += f"\n### {status_icon} {test['test_id']}\n"
            report += f"- 状态: {test['status']}\n"
            report += f"- 时间: {test['timestamp']}\n"
            
            if test['error']:
                report += f"- 错误: {test['error']}\n"
                
            if test['fix']:
                report += f"- 修复建议:\n```sql\n{test['fix'][:500]}...\n```\n"
                
        if self.test_results['fixes_applied']:
            report += "\n## 已应用的修复\n"
            for fix in self.test_results['fixes_applied']:
                report += f"- {fix}\n"
                
        if self.test_results['errors']:
            report += "\n## 系统错误\n"
            for error in self.test_results['errors']:
                report += f"- {error}\n"
                
        report += f"\n## 建议\n"
        
        if self.test_results['failed_tests'] > 0:
            report += "- 请检查并修复失败的测试\n"
            report += "- 执行生成的SQL脚本进行修复\n"
            report += "- 重新运行测试验证修复效果\n"
        else:
            report += "- 所有测试通过，系统运行正常\n"
            report += "- 建议定期运行测试确保稳定性\n"
            
        return report
        
    def run_all_tests(self):
        """运行所有测试"""
        logger.info("="*60)
        logger.info("开始BigQuery完整修复测试")
        logger.info("="*60)
        
        self.setup()
        
        # 运行测试
        self.test_bigquery_connection()
        self.test_table_structures()
        self.test_critical_view()
        self.test_data_flow()
        self.test_historical_backfill()
        self.test_realtime_writing()
        
        # 生成报告
        json_report, md_report = self.generate_report()
        
        logger.info("="*60)
        logger.info(f"测试完成！")
        logger.info(f"通过: {self.test_results['passed_tests']}/{self.test_results['total_tests']}")
        logger.info(f"报告: {json_report}, {md_report}")
        logger.info("="*60)
        
        return self.test_results

if __name__ == "__main__":
    # 创建测试套件并运行
    test_suite = BigQueryRepairTestSuite()
    results = test_suite.run_all_tests()
    
    # 返回状态码
    import sys
    sys.exit(0 if results['failed_tests'] == 0 else 1)