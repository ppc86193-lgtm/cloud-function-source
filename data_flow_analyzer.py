#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统数据流转分析器
在字段优化前，必须先分析数据流转和依赖关系
"""

import os
import re
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple
from pathlib import Path

class DataFlowAnalyzer:
    """数据流转分析器"""
    
    def __init__(self):
        self.project_root = Path(".")
        self.analysis_results = {
            'field_dependencies': {},
            'data_flow_paths': {},
            'code_references': {},
            'api_usage': {},
            'risk_assessment': {}
        }
        
        # 需要分析的字段
        self.target_fields = {
            'score_ledger': ['result_digits', 'source'],
            'draws_14w_dedup_v': ['ts_utc', 'legacy_format', 'data_source'],
            'p_size_clean_merged_dedup_v': ['model_version', 'raw_features', 'processing_time']
        }
    
    def analyze_code_references(self) -> Dict:
        """分析代码中的字段引用"""
        print("=== 分析代码中的字段引用 ===")
        
        code_references = {}
        
        # 搜索Python文件
        python_files = list(self.project_root.glob("**/*.py"))
        python_files.extend(list(self.project_root.glob("**/*.sql")))
        
        for table, fields in self.target_fields.items():
            code_references[table] = {}
            
            for field in fields:
                code_references[table][field] = {
                    'references': [],
                    'usage_patterns': [],
                    'last_used': None
                }
                
                # 搜索字段引用
                for file_path in python_files:
                    if 'venv' in str(file_path) or '__pycache__' in str(file_path):
                        continue
                        
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            
                        # 搜索字段名
                        patterns = [
                            rf"['\"]?{field}['\"]?",  # 直接引用
                            rf"{table}\.{field}",     # 表.字段
                            rf"SELECT.*{field}",      # SQL查询
                            rf"INSERT.*{field}",      # SQL插入
                            rf"UPDATE.*{field}",      # SQL更新
                        ]
                        
                        for pattern in patterns:
                            matches = re.finditer(pattern, content, re.IGNORECASE)
                            for match in matches:
                                line_num = content[:match.start()].count('\n') + 1
                                context = self._get_line_context(content, match.start())
                                
                                code_references[table][field]['references'].append({
                                    'file': str(file_path),
                                    'line': line_num,
                                    'context': context,
                                    'pattern': pattern
                                })
                                
                    except Exception as e:
                        print(f"  警告: 无法读取文件 {file_path}: {e}")
                
                ref_count = len(code_references[table][field]['references'])
                print(f"  {table}.{field}: 发现 {ref_count} 个引用")
        
        return code_references
    
    def analyze_database_usage(self) -> Dict:
        """分析数据库中的字段实际使用情况"""
        print("\n=== 分析数据库字段使用情况 ===")
        
        db_usage = {}
        
        # 查找所有数据库文件
        db_files = list(self.project_root.glob("**/*.db"))
        
        for db_file in db_files:
            if 'venv' in str(db_file):
                continue
                
            print(f"  分析数据库: {db_file}")
            
            try:
                conn = sqlite3.connect(str(db_file))
                cursor = conn.cursor()
                
                # 获取所有表
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [row[0] for row in cursor.fetchall()]
                
                for table in tables:
                    if table in self.target_fields:
                        db_usage[table] = {}
                        
                        # 获取表结构
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [row[1] for row in cursor.fetchall()]
                        
                        for field in self.target_fields[table]:
                            if field in columns:
                                # 分析字段使用情况
                                usage_stats = self._analyze_field_usage(cursor, table, field)
                                db_usage[table][field] = usage_stats
                                
                                print(f"    {table}.{field}: {usage_stats['summary']}")
                
                conn.close()
                
            except Exception as e:
                print(f"  警告: 无法分析数据库 {db_file}: {e}")
        
        return db_usage
    
    def analyze_api_data_flow(self) -> Dict:
        """分析API数据流转"""
        print("\n=== 分析API数据流转 ===")
        
        api_flow = {
            'upstream_api': {},
            'internal_processing': {},
            'downstream_usage': {}
        }
        
        # 分析上游API字段使用
        try:
            from field_usage_analysis import FieldUsageAnalyzer
            analyzer = FieldUsageAnalyzer()
            
            # 实时API分析
            realtime_analysis = analyzer.analyze_realtime_api_usage()
            api_flow['upstream_api']['realtime'] = realtime_analysis
            
            # 历史API分析
            history_analysis = analyzer.analyze_history_api_usage()
            api_flow['upstream_api']['history'] = history_analysis
            
            print(f"  实时API字段使用率: {realtime_analysis.get('usage_rate', 0):.1f}%")
            print(f"  历史API字段使用率: {history_analysis.get('usage_rate', 0):.1f}%")
            
        except Exception as e:
            print(f"  警告: 无法分析API流转: {e}")
        
        return api_flow
    
    def analyze_data_dependencies(self) -> Dict:
        """分析数据依赖关系"""
        print("\n=== 分析数据依赖关系 ===")
        
        dependencies = {}
        
        for table, fields in self.target_fields.items():
            dependencies[table] = {}
            
            for field in fields:
                deps = {
                    'upstream_sources': [],    # 上游数据源
                    'downstream_targets': [],  # 下游使用目标
                    'transformation_logic': [], # 转换逻辑
                    'backup_references': []     # 备份引用
                }
                
                # 分析可能的依赖关系
                if field == 'result_digits':
                    deps['upstream_sources'] = ['API响应中的numbers字段']
                    deps['downstream_targets'] = ['可能用于结果验证']
                    deps['transformation_logic'] = ['numbers数组的副本']
                
                elif field == 'ts_utc':
                    deps['upstream_sources'] = ['timestamp字段的UTC转换']
                    deps['downstream_targets'] = ['时区相关查询']
                    deps['transformation_logic'] = ['timestamp -> UTC转换']
                
                elif field == 'raw_features':
                    deps['upstream_sources'] = ['机器学习特征提取']
                    deps['downstream_targets'] = ['模型调试和分析']
                    deps['transformation_logic'] = ['特征工程输出']
                
                elif field == 'legacy_format':
                    deps['upstream_sources'] = ['历史数据格式']
                    deps['downstream_targets'] = ['兼容性处理']
                    deps['transformation_logic'] = ['格式转换逻辑']
                
                dependencies[table][field] = deps
                print(f"  {table}.{field}: {len(deps['upstream_sources'])} 个上游源")
        
        return dependencies
    
    def assess_removal_risk(self) -> Dict:
        """评估字段删除风险"""
        print("\n=== 评估字段删除风险 ===")
        
        risk_assessment = {}
        
        for table, fields in self.target_fields.items():
            risk_assessment[table] = {}
            
            for field in fields:
                risk_factors = {
                    'code_references': 0,
                    'data_usage': 0,
                    'api_dependency': 0,
                    'business_logic': 0
                }
                
                # 基于代码引用评估风险
                if table in self.analysis_results.get('code_references', {}):
                    ref_count = len(self.analysis_results['code_references'][table].get(field, {}).get('references', []))
                    risk_factors['code_references'] = min(ref_count * 10, 100)  # 最高100分
                
                # 基于数据使用评估风险
                if table in self.analysis_results.get('database_usage', {}):
                    usage_stats = self.analysis_results['database_usage'][table].get(field, {})
                    usage_rate = usage_stats.get('usage_percentage', 0)
                    risk_factors['data_usage'] = usage_rate
                
                # 计算综合风险分数
                total_risk = sum(risk_factors.values()) / len(risk_factors)
                
                risk_level = 'low'
                if total_risk > 70:
                    risk_level = 'high'
                elif total_risk > 30:
                    risk_level = 'medium'
                
                risk_assessment[table][field] = {
                    'risk_factors': risk_factors,
                    'total_risk_score': total_risk,
                    'risk_level': risk_level,
                    'recommendation': self._get_risk_recommendation(field, total_risk, risk_factors)
                }
                
                print(f"  {table}.{field}: 风险等级 {risk_level} (分数: {total_risk:.1f})")
        
        return risk_assessment
    
    def generate_safe_cleanup_plan(self) -> Dict:
        """生成安全的清理计划"""
        print("\n=== 生成安全清理计划 ===")
        
        cleanup_plan = {
            'immediate_safe': [],      # 可以立即安全删除
            'archive_first': [],       # 需要先归档
            'monitor_period': [],      # 需要监控期
            'keep_indefinitely': []    # 建议保留
        }
        
        for table, fields in self.target_fields.items():
            for field in fields:
                risk_info = self.analysis_results.get('risk_assessment', {}).get(table, {}).get(field, {})
                risk_level = risk_info.get('risk_level', 'high')
                risk_score = risk_info.get('total_risk_score', 100)
                
                field_info = {
                    'table': table,
                    'field': field,
                    'risk_level': risk_level,
                    'risk_score': risk_score,
                    'recommendation': risk_info.get('recommendation', '需要进一步分析')
                }
                
                if risk_level == 'low' and risk_score < 10:
                    cleanup_plan['immediate_safe'].append(field_info)
                elif risk_level == 'low' and risk_score < 30:
                    cleanup_plan['archive_first'].append(field_info)
                elif risk_level == 'medium':
                    cleanup_plan['monitor_period'].append(field_info)
                else:
                    cleanup_plan['keep_indefinitely'].append(field_info)
        
        # 打印计划摘要
        for category, items in cleanup_plan.items():
            print(f"  {category}: {len(items)} 个字段")
        
        return cleanup_plan
    
    def _analyze_field_usage(self, cursor, table: str, field: str) -> Dict:
        """分析单个字段的使用情况"""
        try:
            # 总记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_records = cursor.fetchone()[0]
            
            # 非空记录数
            cursor.execute(f"SELECT COUNT({field}) FROM {table}")
            non_null_records = cursor.fetchone()[0]
            
            # 唯一值数量
            cursor.execute(f"SELECT COUNT(DISTINCT {field}) FROM {table}")
            unique_values = cursor.fetchone()[0]
            
            # 最近更新时间（如果有时间戳字段）
            last_updated = None
            try:
                cursor.execute(f"SELECT MAX(timestamp) FROM {table} WHERE {field} IS NOT NULL")
                result = cursor.fetchone()
                if result and result[0]:
                    last_updated = result[0]
            except:
                pass
            
            usage_percentage = (non_null_records / total_records * 100) if total_records > 0 else 0
            
            return {
                'total_records': total_records,
                'non_null_records': non_null_records,
                'unique_values': unique_values,
                'usage_percentage': usage_percentage,
                'last_updated': last_updated,
                'summary': f"{usage_percentage:.1f}% 使用率 ({non_null_records}/{total_records})"
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'summary': '分析失败'
            }
    
    def _get_line_context(self, content: str, position: int, context_lines: int = 2) -> str:
        """获取代码行上下文"""
        lines = content.split('\n')
        line_num = content[:position].count('\n')
        
        start = max(0, line_num - context_lines)
        end = min(len(lines), line_num + context_lines + 1)
        
        context_lines = lines[start:end]
        return '\n'.join(f"{start + i + 1}: {line}" for i, line in enumerate(context_lines))
    
    def _get_risk_recommendation(self, field: str, risk_score: float, risk_factors: Dict) -> str:
        """获取风险建议"""
        if risk_score < 10:
            return "可以安全删除"
        elif risk_score < 30:
            return "建议先归档，监控1个月后删除"
        elif risk_score < 70:
            return "需要详细分析依赖关系，建议保留"
        else:
            return "高风险，不建议删除"
    
    def run_complete_analysis(self) -> Dict:
        """运行完整的数据流转分析"""
        print("=== PC28系统数据流转完整分析 ===")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. 分析代码引用
        self.analysis_results['code_references'] = self.analyze_code_references()
        
        # 2. 分析数据库使用
        self.analysis_results['database_usage'] = self.analyze_database_usage()
        
        # 3. 分析API流转
        self.analysis_results['api_usage'] = self.analyze_api_data_flow()
        
        # 4. 分析数据依赖
        self.analysis_results['data_dependencies'] = self.analyze_data_dependencies()
        
        # 5. 评估删除风险
        self.analysis_results['risk_assessment'] = self.assess_removal_risk()
        
        # 6. 生成安全清理计划
        self.analysis_results['cleanup_plan'] = self.generate_safe_cleanup_plan()
        
        print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return self.analysis_results
    
    def save_analysis_report(self, output_file: str = "data_flow_analysis_report.json"):
        """保存分析报告"""
        report = {
            'analysis_metadata': {
                'generated_at': datetime.now().isoformat(),
                'analyzer_version': '1.0',
                'project_root': str(self.project_root.absolute())
            },
            'analysis_results': self.analysis_results
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 分析报告已保存: {output_file}")
        return output_file

def main():
    """主函数"""
    analyzer = DataFlowAnalyzer()
    
    # 运行完整分析
    results = analyzer.run_complete_analysis()
    
    # 保存报告
    report_file = analyzer.save_analysis_report()
    
    # 打印摘要
    print("\n=== 分析摘要 ===")
    cleanup_plan = results.get('cleanup_plan', {})
    
    print(f"立即安全删除: {len(cleanup_plan.get('immediate_safe', []))} 个字段")
    print(f"需要先归档: {len(cleanup_plan.get('archive_first', []))} 个字段")
    print(f"需要监控期: {len(cleanup_plan.get('monitor_period', []))} 个字段")
    print(f"建议保留: {len(cleanup_plan.get('keep_indefinitely', []))} 个字段")
    
    print(f"\n详细报告: {report_file}")
    print("请根据分析结果调整字段优化计划")

if __name__ == "__main__":
    main()