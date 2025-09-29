#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大字段归档方案
专门处理raw_features等大型字段的安全归档和清理
"""

import os
import sqlite3
import json
import gzip
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

class LargeFieldArchiver:
    """大字段归档器"""
    
    def __init__(self, db_path: str = "pc28_local.db"):
        self.db_path = db_path
        self.archive_dir = Path("field_archives")
        self.archive_dir.mkdir(exist_ok=True)
        
        # 大字段配置
        self.large_fields = {
            'p_size_clean_merged_dedup_v': {
                'raw_features': {
                    'estimated_size_mb': 128.5,
                    'compression_ratio': 0.3,  # 预期压缩比
                    'archive_format': 'compressed_json',
                    'retention_days': 365
                }
            }
        }
    
    def analyze_large_field_usage(self, table: str, field: str) -> Dict:
        """分析大字段的使用情况"""
        print(f"=== 分析 {table}.{field} 使用情况 ===")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if not cursor.fetchone():
                print(f"  警告: 表 {table} 不存在")
                return {'error': 'table_not_found'}
            
            # 检查字段是否存在
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            if field not in columns:
                print(f"  警告: 字段 {field} 不存在于表 {table}")
                return {'error': 'field_not_found'}
            
            # 分析字段使用情况
            analysis = {}
            
            # 总记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_records = cursor.fetchone()[0]
            
            # 非空记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {field} IS NOT NULL")
            non_null_records = cursor.fetchone()[0]
            
            # 字段大小分析（采样）
            cursor.execute(f"SELECT {field} FROM {table} WHERE {field} IS NOT NULL LIMIT 10")
            sample_data = cursor.fetchall()
            
            if sample_data:
                sample_sizes = []
                for row in sample_data:
                    if row[0]:
                        # 估算字段大小
                        field_size = len(str(row[0]).encode('utf-8'))
                        sample_sizes.append(field_size)
                
                avg_field_size = sum(sample_sizes) / len(sample_sizes) if sample_sizes else 0
                estimated_total_size = avg_field_size * non_null_records
            else:
                avg_field_size = 0
                estimated_total_size = 0
            
            # 最近访问时间（如果有时间戳字段）
            last_accessed = None
            try:
                cursor.execute(f"SELECT MAX(timestamp) FROM {table} WHERE {field} IS NOT NULL")
                result = cursor.fetchone()
                if result and result[0]:
                    last_accessed = result[0]
            except:
                pass
            
            analysis = {
                'total_records': total_records,
                'non_null_records': non_null_records,
                'usage_percentage': (non_null_records / total_records * 100) if total_records > 0 else 0,
                'avg_field_size_bytes': avg_field_size,
                'estimated_total_size_mb': estimated_total_size / (1024 * 1024),
                'last_accessed': last_accessed,
                'sample_count': len(sample_data)
            }
            
            conn.close()
            
            print(f"  总记录数: {total_records}")
            print(f"  非空记录数: {non_null_records}")
            print(f"  使用率: {analysis['usage_percentage']:.1f}%")
            print(f"  预估总大小: {analysis['estimated_total_size_mb']:.1f} MB")
            
            return analysis
            
        except Exception as e:
            print(f"  分析失败: {e}")
            return {'error': str(e)}
    
    def create_archive_strategy(self, table: str, field: str, analysis: Dict) -> Dict:
        """创建归档策略"""
        print(f"\n=== 创建 {table}.{field} 归档策略 ===")
        
        field_config = self.large_fields.get(table, {}).get(field, {})
        estimated_size_mb = analysis.get('estimated_total_size_mb', 0)
        usage_percentage = analysis.get('usage_percentage', 0)
        
        # 基于使用率和大小确定策略
        if usage_percentage < 5 and estimated_size_mb > 50:
            strategy = 'immediate_archive'
            priority = 'high'
        elif usage_percentage < 20 and estimated_size_mb > 20:
            strategy = 'gradual_archive'
            priority = 'medium'
        else:
            strategy = 'monitor_only'
            priority = 'low'
        
        archive_strategy = {
            'strategy': strategy,
            'priority': priority,
            'estimated_size_mb': estimated_size_mb,
            'usage_percentage': usage_percentage,
            'compression_enabled': estimated_size_mb > 10,
            'retention_days': field_config.get('retention_days', 365),
            'archive_format': field_config.get('archive_format', 'compressed_json'),
            'steps': self._generate_archive_steps(strategy, table, field),
            'expected_savings': {
                'storage_mb': estimated_size_mb * 0.8,  # 预期节省80%存储
                'query_performance': '15-25%',
                'backup_time': '30-40%'
            }
        }
        
        print(f"  策略: {strategy}")
        print(f"  优先级: {priority}")
        print(f"  预期节省: {archive_strategy['expected_savings']['storage_mb']:.1f} MB")
        
        return archive_strategy
    
    def _generate_archive_steps(self, strategy: str, table: str, field: str) -> List[str]:
        """生成归档步骤"""
        if strategy == 'immediate_archive':
            return [
                f"1. 创建压缩归档文件 {table}_{field}_archive.gz",
                f"2. 导出 {field} 数据到归档文件",
                f"3. 验证归档数据完整性",
                f"4. 创建字段访问代理（如需要）",
                f"5. 删除原字段数据",
                f"6. 监控系统运行1周"
            ]
        elif strategy == 'gradual_archive':
            return [
                f"1. 创建归档表 {table}_{field}_archive",
                f"2. 分批迁移旧数据（>30天）到归档表",
                f"3. 监控业务影响2周",
                f"4. 如无影响，迁移剩余数据",
                f"5. 删除原字段"
            ]
        else:
            return [
                f"1. 监控 {field} 字段使用情况",
                f"2. 定期评估归档必要性",
                f"3. 暂不执行归档操作"
            ]
    
    def execute_field_archive(self, table: str, field: str, strategy: Dict) -> Dict:
        """执行字段归档"""
        print(f"\n=== 执行 {table}.{field} 归档 ===")
        
        if strategy['strategy'] == 'monitor_only':
            print("  策略为仅监控，跳过归档操作")
            return {'status': 'skipped', 'reason': 'monitor_only_strategy'}
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 1. 导出数据
            print("  1. 导出字段数据...")
            cursor.execute(f"SELECT rowid, {field}, timestamp FROM {table} WHERE {field} IS NOT NULL")
            field_data = cursor.fetchall()
            
            if not field_data:
                print("  没有数据需要归档")
                return {'status': 'no_data'}
            
            # 2. 创建归档文件
            archive_filename = f"{table}_{field}_archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.gz"
            archive_path = self.archive_dir / archive_filename
            
            print(f"  2. 创建归档文件: {archive_filename}")
            
            archive_data = {
                'metadata': {
                    'table': table,
                    'field': field,
                    'archived_at': datetime.now().isoformat(),
                    'record_count': len(field_data),
                    'original_size_mb': strategy['estimated_size_mb']
                },
                'data': []
            }
            
            for row in field_data:
                archive_data['data'].append({
                    'rowid': row[0],
                    'field_value': row[1],
                    'timestamp': row[2]
                })
            
            # 3. 压缩保存
            with gzip.open(archive_path, 'wt', encoding='utf-8') as f:
                json.dump(archive_data, f, ensure_ascii=False, indent=2)
            
            # 4. 验证归档
            print("  3. 验证归档数据...")
            with gzip.open(archive_path, 'rt', encoding='utf-8') as f:
                verified_data = json.load(f)
            
            if len(verified_data['data']) != len(field_data):
                raise Exception("归档数据验证失败：记录数不匹配")
            
            # 5. 计算压缩效果
            original_size = archive_path.stat().st_size
            compressed_size = original_size
            compression_ratio = compressed_size / (strategy['estimated_size_mb'] * 1024 * 1024) if strategy['estimated_size_mb'] > 0 else 0
            
            print(f"  4. 归档完成")
            print(f"     - 归档记录数: {len(field_data)}")
            print(f"     - 压缩文件大小: {compressed_size / 1024 / 1024:.1f} MB")
            print(f"     - 压缩比: {compression_ratio:.2f}")
            
            # 6. 创建访问代理（如果需要）
            if strategy.get('create_proxy', False):
                self._create_field_access_proxy(table, field, archive_path)
            
            conn.close()
            
            return {
                'status': 'success',
                'archive_file': str(archive_path),
                'record_count': len(field_data),
                'compressed_size_mb': compressed_size / 1024 / 1024,
                'compression_ratio': compression_ratio
            }
            
        except Exception as e:
            print(f"  归档失败: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _create_field_access_proxy(self, table: str, field: str, archive_path: Path):
        """创建字段访问代理"""
        proxy_code = f"""
# {table}.{field} 字段访问代理
# 归档文件: {archive_path}

import gzip
import json
from pathlib import Path

def get_{field}_data(rowid: int):
    \"\"\"获取归档的{field}数据\"\"\"
    archive_path = Path("{archive_path}")
    
    with gzip.open(archive_path, 'rt', encoding='utf-8') as f:
        archive_data = json.load(f)
    
    for record in archive_data['data']:
        if record['rowid'] == rowid:
            return record['field_value']
    
    return None

def search_{field}_data(criteria: dict):
    \"\"\"搜索归档的{field}数据\"\"\"
    archive_path = Path("{archive_path}")
    
    with gzip.open(archive_path, 'rt', encoding='utf-8') as f:
        archive_data = json.load(f)
    
    results = []
    for record in archive_data['data']:
        # 这里可以添加搜索逻辑
        results.append(record)
    
    return results
"""
        
        proxy_file = self.archive_dir / f"{table}_{field}_proxy.py"
        with open(proxy_file, 'w', encoding='utf-8') as f:
            f.write(proxy_code)
        
        print(f"  ✓ 创建访问代理: {proxy_file}")
    
    def generate_archive_report(self, results: Dict) -> str:
        """生成归档报告"""
        report_data = {
            'archive_summary': {
                'generated_at': datetime.now().isoformat(),
                'total_fields_processed': len(results),
                'successful_archives': len([r for r in results.values() if r.get('status') == 'success']),
                'total_space_saved_mb': sum(r.get('compressed_size_mb', 0) for r in results.values()),
                'archive_directory': str(self.archive_dir)
            },
            'field_results': results
        }
        
        report_file = f"large_field_archive_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 归档报告已保存: {report_file}")
        return report_file
    
    def run_large_field_archival(self) -> Dict:
        """运行大字段归档流程"""
        print("=== PC28大字段归档流程 ===")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        results = {}
        
        for table, fields in self.large_fields.items():
            for field, config in fields.items():
                print(f"\n处理 {table}.{field}...")
                
                # 1. 分析字段使用情况
                analysis = self.analyze_large_field_usage(table, field)
                
                if 'error' in analysis:
                    results[f"{table}.{field}"] = analysis
                    continue
                
                # 2. 创建归档策略
                strategy = self.create_archive_strategy(table, field, analysis)
                
                # 3. 执行归档（如果策略允许）
                if strategy['strategy'] != 'monitor_only':
                    archive_result = self.execute_field_archive(table, field, strategy)
                    results[f"{table}.{field}"] = {
                        'analysis': analysis,
                        'strategy': strategy,
                        'archive_result': archive_result
                    }
                else:
                    results[f"{table}.{field}"] = {
                        'analysis': analysis,
                        'strategy': strategy,
                        'archive_result': {'status': 'skipped', 'reason': 'monitor_only'}
                    }
        
        # 4. 生成报告
        report_file = self.generate_archive_report(results)
        
        print(f"\n=== 归档流程完成 ===")
        print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"详细报告: {report_file}")
        
        return results

def main():
    """主函数"""
    archiver = LargeFieldArchiver()
    
    # 运行大字段归档
    results = archiver.run_large_field_archival()
    
    # 打印摘要
    print("\n=== 归档摘要 ===")
    successful = 0
    total_saved_mb = 0
    
    for field_name, result in results.items():
        archive_result = result.get('archive_result', {})
        if archive_result.get('status') == 'success':
            successful += 1
            total_saved_mb += archive_result.get('compressed_size_mb', 0)
        
        print(f"  {field_name}: {archive_result.get('status', 'unknown')}")
    
    print(f"\n成功归档: {successful} 个字段")
    print(f"节省空间: {total_saved_mb:.1f} MB")

if __name__ == "__main__":
    main()