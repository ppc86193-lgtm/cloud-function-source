#!/usr/bin/env python3
"""
PC28数据库诊断和修复脚本
检查BigQuery数据库状态，识别问题并生成修复建议
"""

import os
import json
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/database_diagnostic.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseDiagnostic:
    def __init__(self):
        self.client = bigquery.Client(project='wprojectl')
        self.dataset_id = 'pc28_lab'
        self.main_table = 'draws_14w_clean'
        self.diagnostic_results = {}
        
    def check_table_exists(self, table_name):
        """检查表是否存在"""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
    
    def get_table_info(self, table_name):
        """获取表的基本信息"""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_name)
            table = self.client.get_table(table_ref)
            
            return {
                'exists': True,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created.isoformat() if table.created else None,
                'modified': table.modified.isoformat() if table.modified else None,
                'schema_fields': len(table.schema)
            }
        except Exception as e:
            logger.error(f"获取表信息失败 {table_name}: {e}")
            return {'exists': False, 'error': str(e)}
    
    def check_data_quality(self, table_name):
        """检查数据质量"""
        try:
            # 检查空值情况
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(issue) as valid_issues,
                COUNT(a) as valid_a,
                COUNT(b) as valid_b,
                COUNT(c) as valid_c,
                COUNT(timestamp) as valid_timestamps
            FROM `wprojectl.{self.dataset_id}.{table_name}`
            """
            
            result = self.client.query(query).to_dataframe()
            
            if len(result) > 0:
                row = result.iloc[0]
                return {
                    'total_rows': int(row['total_rows']),
                    'data_completeness': {
                        'issue': int(row['valid_issues']),
                        'a': int(row['valid_a']),
                        'b': int(row['valid_b']),
                        'c': int(row['valid_c']),
                        'timestamp': int(row['valid_timestamps'])
                    },
                    'null_percentage': {
                        'issue': (int(row['total_rows']) - int(row['valid_issues'])) / int(row['total_rows']) * 100,
                        'a': (int(row['total_rows']) - int(row['valid_a'])) / int(row['total_rows']) * 100,
                        'b': (int(row['total_rows']) - int(row['valid_b'])) / int(row['total_rows']) * 100,
                        'c': (int(row['total_rows']) - int(row['valid_c'])) / int(row['total_rows']) * 100,
                        'timestamp': (int(row['total_rows']) - int(row['valid_timestamps'])) / int(row['total_rows']) * 100
                    }
                }
        except Exception as e:
            logger.error(f"数据质量检查失败 {table_name}: {e}")
            return {'error': str(e)}
    
    def check_recent_data(self, table_name, hours=24):
        """检查最近的数据更新"""
        try:
            # 检查最近24小时的数据
            query = f"""
            SELECT 
                COUNT(*) as recent_count,
                MAX(timestamp) as latest_timestamp,
                MIN(timestamp) as earliest_timestamp
            FROM `wprojectl.{self.dataset_id}.{table_name}`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            """
            
            result = self.client.query(query).to_dataframe()
            
            if len(result) > 0:
                row = result.iloc[0]
                return {
                    'recent_count': int(row['recent_count']),
                    'latest_timestamp': str(row['latest_timestamp']),
                    'earliest_timestamp': str(row['earliest_timestamp']),
                    'data_freshness_hours': hours
                }
        except Exception as e:
            logger.error(f"最近数据检查失败 {table_name}: {e}")
            return {'error': str(e)}
    
    def check_duplicate_data(self, table_name):
        """检查重复数据"""
        try:
            query = f"""
            SELECT 
                issue,
                COUNT(*) as duplicate_count
            FROM `wprojectl.{self.dataset_id}.{table_name}`
            GROUP BY issue
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            LIMIT 10
            """
            
            result = self.client.query(query).to_dataframe()
            
            return {
                'duplicate_issues_count': len(result),
                'top_duplicates': result.to_dict('records') if len(result) > 0 else []
            }
        except Exception as e:
            logger.error(f"重复数据检查失败 {table_name}: {e}")
            return {'error': str(e)}
    
    def run_full_diagnostic(self):
        """运行完整的数据库诊断"""
        logger.info("开始数据库诊断...")
        
        # 检查主表
        logger.info(f"检查主表: {self.main_table}")
        self.diagnostic_results['main_table'] = {
            'table_name': self.main_table,
            'info': self.get_table_info(self.main_table),
            'data_quality': self.check_data_quality(self.main_table),
            'recent_data': self.check_recent_data(self.main_table),
            'duplicates': self.check_duplicate_data(self.main_table)
        }
        
        # 检查其他重要表
        other_tables = ['draws_raw', 'draws_today_v2']
        self.diagnostic_results['other_tables'] = {}
        
        for table in other_tables:
            if self.check_table_exists(table):
                logger.info(f"检查表: {table}")
                self.diagnostic_results['other_tables'][table] = {
                    'info': self.get_table_info(table),
                    'recent_data': self.check_recent_data(table)
                }
            else:
                logger.warning(f"表不存在: {table}")
                self.diagnostic_results['other_tables'][table] = {'exists': False}
        
        # 生成诊断摘要
        self.generate_diagnostic_summary()
        
        return self.diagnostic_results
    
    def generate_diagnostic_summary(self):
        """生成诊断摘要"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'issues_found': [],
            'recommendations': []
        }
        
        # 检查主表状态
        main_table_info = self.diagnostic_results['main_table']['info']
        if not main_table_info.get('exists', False):
            summary['overall_status'] = 'critical'
            summary['issues_found'].append('主表不存在')
            summary['recommendations'].append('重新创建主表')
        
        # 检查数据质量
        data_quality = self.diagnostic_results['main_table'].get('data_quality', {})
        if 'null_percentage' in data_quality:
            for field, null_pct in data_quality['null_percentage'].items():
                if null_pct > 50:  # 超过50%的空值
                    summary['issues_found'].append(f'{field}字段空值过多: {null_pct:.1f}%')
                    summary['recommendations'].append(f'修复{field}字段的空值问题')
        
        # 检查重复数据
        duplicates = self.diagnostic_results['main_table'].get('duplicates', {})
        if duplicates.get('duplicate_issues_count', 0) > 0:
            summary['issues_found'].append(f'发现{duplicates["duplicate_issues_count"]}个重复期号')
            summary['recommendations'].append('清理重复数据')
        
        # 检查数据新鲜度
        recent_data = self.diagnostic_results['main_table'].get('recent_data', {})
        if recent_data.get('recent_count', 0) == 0:
            summary['issues_found'].append('最近24小时无新数据')
            summary['recommendations'].append('检查数据采集服务')
        
        if len(summary['issues_found']) > 0:
            summary['overall_status'] = 'needs_attention'
        
        self.diagnostic_results['summary'] = summary
    
    def save_diagnostic_report(self):
        """保存诊断报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f'logs/database_diagnostic_report_{timestamp}.json'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.diagnostic_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"诊断报告已保存: {report_file}")
        return report_file

def main():
    """主函数"""
    try:
        # 确保日志目录存在
        os.makedirs('logs', exist_ok=True)
        
        # 运行诊断
        diagnostic = DatabaseDiagnostic()
        results = diagnostic.run_full_diagnostic()
        
        # 保存报告
        report_file = diagnostic.save_diagnostic_report()
        
        # 打印摘要
        summary = results.get('summary', {})
        print("\n" + "="*50)
        print("数据库诊断摘要")
        print("="*50)
        print(f"整体状态: {summary.get('overall_status', 'unknown')}")
        print(f"发现问题数: {len(summary.get('issues_found', []))}")
        
        if summary.get('issues_found'):
            print("\n发现的问题:")
            for issue in summary['issues_found']:
                print(f"  - {issue}")
        
        if summary.get('recommendations'):
            print("\n修复建议:")
            for rec in summary['recommendations']:
                print(f"  - {rec}")
        
        print(f"\n详细报告: {report_file}")
        print("="*50)
        
        return 0 if summary.get('overall_status') == 'healthy' else 1
        
    except Exception as e:
        logger.error(f"诊断过程出错: {e}")
        return 1

if __name__ == "__main__":
    exit(main())