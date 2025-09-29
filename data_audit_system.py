"""
数据完整性检查和审计系统
用于定期验证 PC28 系统和 Supabase 之间的数据一致性
"""

import os
import logging
import sqlite3
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import json
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

from supabase_config import get_supabase_client
from supabase_sync_manager import sync_manager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataAuditSystem:
    """数据审计系统"""
    
    def __init__(self, sqlite_db_path: Optional[str] = None):
        self.sqlite_db_path = sqlite_db_path or os.getenv('SQLITE_DB_PATH', 'pc28_data.db')
        self.supabase_client = get_supabase_client(use_service_role=True)
        
        # 审计配置
        self.audit_tables = [
            'lab_push_candidates_v2',
            'cloud_pred_today_norm', 
            'signal_pool_union_v3',
            'p_size_clean_merged_dedup_v',
            'draws_14w_dedup_v',
            'score_ledger'
        ]
        
        self.audit_history = []
        self.max_history_records = 50
        
        # 审计阈值
        self.integrity_threshold = 0.95  # 完整性得分阈值
        self.consistency_threshold = 0.90  # 一致性得分阈值
        
    def get_sqlite_connection(self) -> sqlite3.Connection:
        """获取 SQLite 数据库连接"""
        try:
            conn = sqlite3.connect(self.sqlite_db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise
    
    def count_records(self, table_name: str, source: str = 'both') -> Dict[str, int]:
        """
        统计表记录数
        
        Args:
            table_name: 表名
            source: 数据源 ('sqlite', 'supabase', 'both')
            
        Returns:
            记录数统计
        """
        counts = {}
        
        try:
            if source in ['sqlite', 'both']:
                with self.get_sqlite_connection() as conn:
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                    counts['sqlite'] = cursor.fetchone()[0]
            
            if source in ['supabase', 'both']:
                response = self.supabase_client.table(table_name).select('id', count='exact').execute()
                counts['supabase'] = response.count or 0
                
        except Exception as e:
            logger.error(f"Failed to count records for {table_name}: {e}")
            if source == 'sqlite' or source == 'both':
                counts['sqlite'] = -1
            if source == 'supabase' or source == 'both':
                counts['supabase'] = -1
        
        return counts
    
    def check_data_integrity(self, table_name: str) -> Dict[str, Any]:
        """
        检查数据完整性
        
        Args:
            table_name: 表名
            
        Returns:
            完整性检查结果
        """
        start_time = datetime.now()
        
        try:
            # 统计记录数
            counts = self.count_records(table_name)
            
            if counts.get('sqlite', -1) == -1 or counts.get('supabase', -1) == -1:
                return {
                    'table_name': table_name,
                    'status': 'error',
                    'error_message': 'Failed to retrieve record counts',
                    'timestamp': start_time.isoformat()
                }
            
            sqlite_count = counts['sqlite']
            supabase_count = counts['supabase']
            
            # 计算完整性得分
            if sqlite_count == 0 and supabase_count == 0:
                integrity_score = 1.0
                missing_records = 0
            elif sqlite_count == 0:
                integrity_score = 0.0
                missing_records = supabase_count
            else:
                missing_records = abs(sqlite_count - supabase_count)
                integrity_score = 1.0 - (missing_records / sqlite_count)
            
            # 检查数据质量
            quality_issues = self._check_data_quality(table_name)
            
            result = {
                'table_name': table_name,
                'sqlite_count': sqlite_count,
                'supabase_count': supabase_count,
                'missing_records': missing_records,
                'integrity_score': round(integrity_score, 4),
                'quality_issues': quality_issues,
                'status': 'passed' if integrity_score >= self.integrity_threshold else 'failed',
                'timestamp': start_time.isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds()
            }
            
            logger.info(f"Integrity check for {table_name}: {result['status']} "
                       f"(score: {integrity_score:.3f}, missing: {missing_records})")
            
            return result
            
        except Exception as e:
            error_msg = f"Integrity check failed for {table_name}: {str(e)}"
            logger.error(error_msg)
            
            return {
                'table_name': table_name,
                'status': 'error',
                'error_message': error_msg,
                'timestamp': start_time.isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds()
            }
    
    def _check_data_quality(self, table_name: str) -> List[Dict[str, Any]]:
        """
        检查数据质量问题
        
        Args:
            table_name: 表名
            
        Returns:
            质量问题列表
        """
        issues = []
        
        try:
            # 检查 Supabase 中的数据质量
            response = self.supabase_client.table(table_name).select('*').limit(1000).execute()
            
            if not response.data:
                return issues
            
            data = response.data
            total_records = len(data)
            
            # 检查空值
            null_counts = {}
            for record in data:
                for field, value in record.items():
                    if value is None or value == '':
                        null_counts[field] = null_counts.get(field, 0) + 1
            
            # 报告高空值率的字段
            for field, null_count in null_counts.items():
                null_rate = null_count / total_records
                if null_rate > 0.1:  # 超过10%的空值率
                    issues.append({
                        'type': 'high_null_rate',
                        'field': field,
                        'null_count': null_count,
                        'null_rate': round(null_rate, 3),
                        'severity': 'warning' if null_rate < 0.3 else 'error'
                    })
            
            # 检查重复记录（基于主键字段）
            if table_name in sync_manager.CORE_TABLES:
                primary_key = sync_manager.CORE_TABLES[table_name].get('primary_key')
                if primary_key:
                    key_values = [record.get(primary_key) for record in data if record.get(primary_key)]
                    unique_keys = set(key_values)
                    
                    if len(key_values) != len(unique_keys):
                        duplicate_count = len(key_values) - len(unique_keys)
                        issues.append({
                            'type': 'duplicate_keys',
                            'field': primary_key,
                            'duplicate_count': duplicate_count,
                            'severity': 'error'
                        })
            
        except Exception as e:
            logger.error(f"Data quality check failed for {table_name}: {e}")
            issues.append({
                'type': 'quality_check_error',
                'error_message': str(e),
                'severity': 'error'
            })
        
        return issues
    
    def check_data_consistency(self, table_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        检查数据一致性（比较 SQLite 和 Supabase 中的实际数据）
        
        Args:
            table_name: 表名
            sample_size: 采样大小
            
        Returns:
            一致性检查结果
        """
        start_time = datetime.now()
        
        try:
            # 从 SQLite 获取样本数据
            with self.get_sqlite_connection() as conn:
                query = f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {sample_size}"
                sqlite_df = pd.read_sql_query(query, conn)
            
            if sqlite_df.empty:
                return {
                    'table_name': table_name,
                    'status': 'skipped',
                    'message': 'No data in SQLite table',
                    'timestamp': start_time.isoformat()
                }
            
            # 获取主键字段
            primary_key = sync_manager.CORE_TABLES.get(table_name, {}).get('primary_key', 'id')
            
            # 从 Supabase 获取对应数据
            key_values = sqlite_df[primary_key].tolist()
            response = self.supabase_client.table(table_name).select('*').in_(primary_key, key_values).execute()
            
            if not response.data:
                return {
                    'table_name': table_name,
                    'status': 'failed',
                    'consistency_score': 0.0,
                    'message': 'No matching data found in Supabase',
                    'timestamp': start_time.isoformat()
                }
            
            supabase_df = pd.DataFrame(response.data)
            
            # 比较数据一致性
            consistency_results = self._compare_dataframes(sqlite_df, supabase_df, primary_key)
            
            result = {
                'table_name': table_name,
                'sample_size': len(sqlite_df),
                'matched_records': consistency_results['matched_records'],
                'consistency_score': consistency_results['consistency_score'],
                'field_differences': consistency_results['field_differences'],
                'status': 'passed' if consistency_results['consistency_score'] >= self.consistency_threshold else 'failed',
                'timestamp': start_time.isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds()
            }
            
            logger.info(f"Consistency check for {table_name}: {result['status']} "
                       f"(score: {consistency_results['consistency_score']:.3f})")
            
            return result
            
        except Exception as e:
            error_msg = f"Consistency check failed for {table_name}: {str(e)}"
            logger.error(error_msg)
            
            return {
                'table_name': table_name,
                'status': 'error',
                'error_message': error_msg,
                'timestamp': start_time.isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds()
            }
    
    def _compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                           primary_key: str) -> Dict[str, Any]:
        """
        比较两个 DataFrame 的一致性
        
        Args:
            df1: 第一个 DataFrame (SQLite)
            df2: 第二个 DataFrame (Supabase)
            primary_key: 主键字段
            
        Returns:
            比较结果
        """
        # 按主键合并数据
        merged = df1.set_index(primary_key).join(
            df2.set_index(primary_key), 
            how='inner', 
            rsuffix='_supabase'
        )
        
        matched_records = len(merged)
        total_records = len(df1)
        
        if matched_records == 0:
            return {
                'matched_records': 0,
                'consistency_score': 0.0,
                'field_differences': {}
            }
        
        # 比较字段值
        field_differences = {}
        consistent_fields = 0
        total_fields = 0
        
        for column in df1.columns:
            if column == primary_key:
                continue
                
            supabase_column = f"{column}_supabase"
            if supabase_column not in merged.columns:
                continue
            
            total_fields += 1
            
            # 比较字段值（处理类型差异）
            sqlite_values = merged[column].astype(str).fillna('')
            supabase_values = merged[supabase_column].astype(str).fillna('')
            
            differences = (sqlite_values != supabase_values).sum()
            consistency_rate = 1.0 - (differences / matched_records)
            
            if consistency_rate < 1.0:
                field_differences[column] = {
                    'differences': int(differences),
                    'consistency_rate': round(consistency_rate, 4),
                    'severity': 'warning' if consistency_rate >= 0.9 else 'error'
                }
            else:
                consistent_fields += 1
        
        # 计算总体一致性得分
        if total_fields > 0:
            field_consistency_score = consistent_fields / total_fields
        else:
            field_consistency_score = 1.0
        
        record_consistency_score = matched_records / total_records
        overall_consistency_score = (field_consistency_score + record_consistency_score) / 2
        
        return {
            'matched_records': matched_records,
            'consistency_score': round(overall_consistency_score, 4),
            'field_differences': field_differences
        }
    
    def run_full_audit(self) -> Dict[str, Any]:
        """
        运行完整的数据审计
        
        Returns:
            审计结果摘要
        """
        start_time = datetime.now()
        
        audit_result = {
            'audit_id': hashlib.md5(start_time.isoformat().encode()).hexdigest()[:8],
            'start_time': start_time.isoformat(),
            'audit_type': 'full_audit',
            'table_results': {},
            'summary': {
                'total_tables': len(self.audit_tables),
                'passed_integrity': 0,
                'failed_integrity': 0,
                'passed_consistency': 0,
                'failed_consistency': 0,
                'total_issues': 0
            }
        }
        
        logger.info(f"Starting full audit for {len(self.audit_tables)} tables")
        
        # 使用线程池并行执行审计
        with ThreadPoolExecutor(max_workers=4) as executor:
            # 提交完整性检查任务
            integrity_futures = {
                executor.submit(self.check_data_integrity, table): table 
                for table in self.audit_tables
            }
            
            # 提交一致性检查任务
            consistency_futures = {
                executor.submit(self.check_data_consistency, table): table 
                for table in self.audit_tables
            }
            
            # 收集完整性检查结果
            for future in as_completed(integrity_futures):
                table_name = integrity_futures[future]
                try:
                    result = future.result()
                    audit_result['table_results'][table_name] = {'integrity': result}
                    
                    if result['status'] == 'passed':
                        audit_result['summary']['passed_integrity'] += 1
                    else:
                        audit_result['summary']['failed_integrity'] += 1
                    
                    # 统计质量问题
                    if 'quality_issues' in result:
                        audit_result['summary']['total_issues'] += len(result['quality_issues'])
                        
                except Exception as e:
                    logger.error(f"Integrity check failed for {table_name}: {e}")
                    audit_result['table_results'][table_name] = {
                        'integrity': {
                            'status': 'error',
                            'error_message': str(e)
                        }
                    }
                    audit_result['summary']['failed_integrity'] += 1
            
            # 收集一致性检查结果
            for future in as_completed(consistency_futures):
                table_name = consistency_futures[future]
                try:
                    result = future.result()
                    if table_name not in audit_result['table_results']:
                        audit_result['table_results'][table_name] = {}
                    
                    audit_result['table_results'][table_name]['consistency'] = result
                    
                    if result['status'] == 'passed':
                        audit_result['summary']['passed_consistency'] += 1
                    else:
                        audit_result['summary']['failed_consistency'] += 1
                        
                except Exception as e:
                    logger.error(f"Consistency check failed for {table_name}: {e}")
                    if table_name not in audit_result['table_results']:
                        audit_result['table_results'][table_name] = {}
                    
                    audit_result['table_results'][table_name]['consistency'] = {
                        'status': 'error',
                        'error_message': str(e)
                    }
                    audit_result['summary']['failed_consistency'] += 1
        
        # 完成审计
        audit_result['end_time'] = datetime.now().isoformat()
        audit_result['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        
        # 计算总体状态
        total_checks = audit_result['summary']['total_tables'] * 2  # 完整性 + 一致性
        passed_checks = (audit_result['summary']['passed_integrity'] + 
                        audit_result['summary']['passed_consistency'])
        
        audit_result['summary']['overall_pass_rate'] = passed_checks / total_checks if total_checks > 0 else 0
        audit_result['summary']['status'] = 'passed' if audit_result['summary']['overall_pass_rate'] >= 0.8 else 'failed'
        
        # 保存审计历史
        self._save_audit_result(audit_result)
        
        logger.info(f"Full audit completed: {audit_result['summary']['status']} "
                   f"(pass rate: {audit_result['summary']['overall_pass_rate']:.2%})")
        
        return audit_result
    
    def _save_audit_result(self, audit_result: Dict[str, Any]):
        """
        保存审计结果到 Supabase
        
        Args:
            audit_result: 审计结果
        """
        try:
            # 保存到审计日志表
            audit_log = {
                'audit_type': 'full_audit',
                'details': audit_result,
                'status': audit_result['summary']['status'],
                'created_by': 'audit_system'
            }
            
            self.supabase_client.table('audit_logs').insert(audit_log).execute()
            
            # 保存到本地历史
            self.audit_history.append(audit_result)
            
            # 保持历史记录数量限制
            if len(self.audit_history) > self.max_history_records:
                self.audit_history = self.audit_history[-self.max_history_records:]
            
            logger.info(f"Audit result saved: {audit_result['audit_id']}")
            
        except Exception as e:
            logger.error(f"Failed to save audit result: {e}")
    
    def get_audit_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        获取审计摘要报告
        
        Args:
            days: 查询天数
            
        Returns:
            审计摘要
        """
        try:
            # 从 Supabase 获取最近的审计记录
            since_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            response = self.supabase_client.table('audit_logs') \
                .select('*') \
                .eq('audit_type', 'full_audit') \
                .gte('created_at', since_date) \
                .order('created_at', desc=True) \
                .execute()
            
            if not response.data:
                return {
                    'period_days': days,
                    'total_audits': 0,
                    'message': 'No audit records found'
                }
            
            audits = response.data
            
            # 计算统计信息
            total_audits = len(audits)
            passed_audits = sum(1 for audit in audits if audit.get('status') == 'passed')
            
            # 计算平均通过率
            pass_rates = []
            for audit in audits:
                details = audit.get('details', {})
                summary = details.get('summary', {})
                if 'overall_pass_rate' in summary:
                    pass_rates.append(summary['overall_pass_rate'])
            
            avg_pass_rate = statistics.mean(pass_rates) if pass_rates else 0
            
            # 最近审计状态
            latest_audit = audits[0] if audits else None
            
            summary = {
                'period_days': days,
                'total_audits': total_audits,
                'passed_audits': passed_audits,
                'failed_audits': total_audits - passed_audits,
                'success_rate': passed_audits / total_audits if total_audits > 0 else 0,
                'average_pass_rate': avg_pass_rate,
                'latest_audit': {
                    'timestamp': latest_audit['created_at'] if latest_audit else None,
                    'status': latest_audit['status'] if latest_audit else None
                },
                'generated_at': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate audit summary: {e}")
            return {
                'period_days': days,
                'error_message': str(e),
                'generated_at': datetime.now().isoformat()
            }
    
    def generate_audit_report(self, audit_result: Dict[str, Any]) -> str:
        """
        生成审计报告（Markdown 格式）
        
        Args:
            audit_result: 审计结果
            
        Returns:
            Markdown 格式的报告
        """
        report_lines = [
            f"# 数据审计报告",
            f"",
            f"**审计ID**: {audit_result.get('audit_id', 'N/A')}",
            f"**审计时间**: {audit_result.get('start_time', 'N/A')}",
            f"**审计类型**: {audit_result.get('audit_type', 'N/A')}",
            f"**总体状态**: {audit_result.get('summary', {}).get('status', 'N/A')}",
            f"**持续时间**: {audit_result.get('duration_seconds', 0):.2f} 秒",
            f"",
            f"## 审计摘要",
            f"",
            f"- **审计表数量**: {audit_result.get('summary', {}).get('total_tables', 0)}",
            f"- **完整性检查通过**: {audit_result.get('summary', {}).get('passed_integrity', 0)}",
            f"- **完整性检查失败**: {audit_result.get('summary', {}).get('failed_integrity', 0)}",
            f"- **一致性检查通过**: {audit_result.get('summary', {}).get('passed_consistency', 0)}",
            f"- **一致性检查失败**: {audit_result.get('summary', {}).get('failed_consistency', 0)}",
            f"- **总体通过率**: {audit_result.get('summary', {}).get('overall_pass_rate', 0):.2%}",
            f"- **发现问题总数**: {audit_result.get('summary', {}).get('total_issues', 0)}",
            f"",
            f"## 详细结果",
            f""
        ]
        
        # 添加每个表的详细结果
        table_results = audit_result.get('table_results', {})
        for table_name, results in table_results.items():
            report_lines.extend([
                f"### {table_name}",
                f""
            ])
            
            # 完整性检查结果
            integrity = results.get('integrity', {})
            if integrity:
                status_icon = "✅" if integrity.get('status') == 'passed' else "❌"
                report_lines.extend([
                    f"**完整性检查**: {status_icon} {integrity.get('status', 'N/A')}",
                    f"- SQLite 记录数: {integrity.get('sqlite_count', 'N/A')}",
                    f"- Supabase 记录数: {integrity.get('supabase_count', 'N/A')}",
                    f"- 缺失记录数: {integrity.get('missing_records', 'N/A')}",
                    f"- 完整性得分: {integrity.get('integrity_score', 'N/A')}",
                    f""
                ])
                
                # 质量问题
                quality_issues = integrity.get('quality_issues', [])
                if quality_issues:
                    report_lines.append("**质量问题**:")
                    for issue in quality_issues:
                        severity_icon = "⚠️" if issue.get('severity') == 'warning' else "🚨"
                        report_lines.append(f"- {severity_icon} {issue.get('type', 'N/A')}: {issue}")
                    report_lines.append("")
            
            # 一致性检查结果
            consistency = results.get('consistency', {})
            if consistency:
                status_icon = "✅" if consistency.get('status') == 'passed' else "❌"
                report_lines.extend([
                    f"**一致性检查**: {status_icon} {consistency.get('status', 'N/A')}",
                    f"- 样本大小: {consistency.get('sample_size', 'N/A')}",
                    f"- 匹配记录数: {consistency.get('matched_records', 'N/A')}",
                    f"- 一致性得分: {consistency.get('consistency_score', 'N/A')}",
                    f""
                ])
                
                # 字段差异
                field_differences = consistency.get('field_differences', {})
                if field_differences:
                    report_lines.append("**字段差异**:")
                    for field, diff in field_differences.items():
                        severity_icon = "⚠️" if diff.get('severity') == 'warning' else "🚨"
                        report_lines.append(f"- {severity_icon} {field}: {diff.get('differences')} 差异 "
                                          f"(一致性: {diff.get('consistency_rate', 0):.2%})")
                    report_lines.append("")
            
            report_lines.append("---")
            report_lines.append("")
        
        return "\n".join(report_lines)

# 全局审计系统实例
audit_system = DataAuditSystem()

def run_data_audit() -> Dict[str, Any]:
    """
    运行数据审计的便捷函数
    
    Returns:
        审计结果
    """
    return audit_system.run_full_audit()

def check_table_integrity(table_name: str) -> Dict[str, Any]:
    """
    检查单个表完整性的便捷函数
    
    Args:
        table_name: 表名
        
    Returns:
        完整性检查结果
    """
    return audit_system.check_data_integrity(table_name)

if __name__ == "__main__":
    # 测试审计系统
    print("Testing Data Audit System...")
    
    try:
        system = DataAuditSystem()
        
        # 运行完整审计
        print("🔍 Running full audit...")
        audit_result = system.run_full_audit()
        
        print(f"Audit completed:")
        print(f"  Status: {audit_result['summary']['status']}")
        print(f"  Pass rate: {audit_result['summary']['overall_pass_rate']:.2%}")
        print(f"  Duration: {audit_result['duration_seconds']:.2f}s")
        
        # 生成报告
        report = system.generate_audit_report(audit_result)
        print(f"\nGenerated audit report ({len(report)} characters)")
        
        # 获取审计摘要
        summary = system.get_audit_summary(7)
        print(f"\nAudit summary: {summary}")
        
    except Exception as e:
        print(f"❌ Audit system test failed: {e}")