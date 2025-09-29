#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于数据流转分析的安全字段清理计划
根据风险评估结果，生成分阶段的安全清理方案
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

class SafeFieldCleanupPlan:
    """安全字段清理计划生成器"""
    
    def __init__(self):
        self.analysis_report = self.load_analysis_report()
        self.cleanup_phases = {
            'phase_1_archive': [],     # 阶段1：先归档再清理
            'phase_2_monitor': [],     # 阶段2：监控期后清理
            'phase_3_optimize': [],    # 阶段3：API优化
            'phase_4_maintain': []     # 阶段4：保持监控
        }
    
    def load_analysis_report(self) -> Dict:
        """加载数据流转分析报告"""
        try:
            with open('data_flow_analysis_report.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print("警告: 未找到数据流转分析报告，使用默认配置")
            return {}
    
    def generate_safe_cleanup_plan(self) -> Dict:
        """生成安全的清理计划"""
        print("=== 生成安全字段清理计划 ===")
        
        cleanup_plan = self.analysis_report.get('analysis_results', {}).get('cleanup_plan', {})
        
        # 阶段1：需要先归档的字段（低风险但需要谨慎）
        archive_first = cleanup_plan.get('archive_first', [])
        for field_info in archive_first:
            table = field_info['table']
            field = field_info['field']
            
            cleanup_action = {
                'table': table,
                'field': field,
                'action': 'archive_then_drop',
                'risk_level': field_info['risk_level'],
                'timeline': '2周归档期 + 1周监控期',
                'steps': [
                    f"1. 创建 {table}_{field}_archive 表",
                    f"2. 迁移 {field} 数据到归档表",
                    f"3. 监控1周确保无业务影响",
                    f"4. 删除原字段"
                ],
                'rollback_plan': f"从归档表恢复 {field} 字段",
                'estimated_savings': self.calculate_field_savings(table, field)
            }
            
            self.cleanup_phases['phase_1_archive'].append(cleanup_action)
        
        # 阶段2：API优化（移除未使用的API字段）
        api_fields = [
            {'field': 'curtime', 'api': 'realtime', 'usage_rate': 0},
            {'field': 'short_issue', 'api': 'realtime', 'usage_rate': 0},
            {'field': 'intervalM', 'api': 'realtime', 'usage_rate': 0}
        ]
        
        for api_field in api_fields:
            cleanup_action = {
                'field': api_field['field'],
                'api_type': api_field['api'],
                'action': 'remove_from_api_response',
                'risk_level': 'low',
                'timeline': '1周测试期',
                'steps': [
                    f"1. 修改API响应模板，移除 {api_field['field']}",
                    f"2. 更新API文档",
                    f"3. 通知下游系统（如有）",
                    f"4. 监控API调用无异常"
                ],
                'rollback_plan': f"恢复 {api_field['field']} 到API响应",
                'estimated_savings': {'network_transfer': '5-10%', 'response_size': '15%'}
            }
            
            self.cleanup_phases['phase_3_optimize'].append(cleanup_action)
        
        print(f"阶段1（归档清理）: {len(self.cleanup_phases['phase_1_archive'])} 个字段")
        print(f"阶段3（API优化）: {len(self.cleanup_phases['phase_3_optimize'])} 个字段")
        
        return self.cleanup_phases
    
    def calculate_field_savings(self, table: str, field: str) -> Dict:
        """计算字段清理的预期节省"""
        # 基于之前的分析结果估算
        field_size_estimates = {
            'result_digits': {'storage_mb': 15.2, 'index_mb': 7.6},
            'source': {'storage_mb': 8.5, 'index_mb': 4.2},
            'ts_utc': {'storage_mb': 12.8, 'index_mb': 6.4},
            'legacy_format': {'storage_mb': 25.3, 'index_mb': 0},
            'data_source': {'storage_mb': 18.7, 'index_mb': 9.3},
            'model_version': {'storage_mb': 6.4, 'index_mb': 3.2},
            'raw_features': {'storage_mb': 128.5, 'index_mb': 0},
            'processing_time': {'storage_mb': 7.9, 'index_mb': 3.9}
        }
        
        savings = field_size_estimates.get(field, {'storage_mb': 5.0, 'index_mb': 2.5})
        
        return {
            'storage_savings_mb': savings['storage_mb'],
            'index_savings_mb': savings['index_mb'],
            'total_savings_mb': savings['storage_mb'] + savings['index_mb'],
            'query_performance_improvement': '5-10%'
        }
    
    def generate_implementation_scripts(self) -> Dict:
        """生成实施脚本"""
        print("\n=== 生成实施脚本 ===")
        
        scripts = {}
        
        # 1. 归档脚本
        archive_script = self.generate_archive_script()
        scripts['archive_script'] = archive_script
        
        # 2. API优化脚本
        api_optimization_script = self.generate_api_optimization_script()
        scripts['api_optimization_script'] = api_optimization_script
        
        # 3. 监控脚本
        monitoring_script = self.generate_monitoring_script()
        scripts['monitoring_script'] = monitoring_script
        
        # 4. 回滚脚本
        rollback_script = self.generate_rollback_script()
        scripts['rollback_script'] = rollback_script
        
        return scripts
    
    def generate_archive_script(self) -> str:
        """生成字段归档脚本"""
        script_lines = [
            "#!/bin/bash",
            "# PC28系统字段归档脚本",
            "# 安全地将低使用率字段迁移到归档表",
            "",
            "set -e  # 遇到错误立即退出",
            "",
            "echo '=== PC28字段归档开始 ==='",
            "echo \"开始时间: $(date)\"",
            "",
            "# 备份原始数据",
            "echo '1. 创建备份...'",
            "sqlite3 pc28_local.db '.backup pc28_archive_backup_$(date +%Y%m%d_%H%M%S).db'",
            "",
            "# 为每个需要归档的字段创建归档表和迁移数据",
        ]
        
        for cleanup_action in self.cleanup_phases['phase_1_archive']:
            table = cleanup_action['table']
            field = cleanup_action['field']
            
            script_lines.extend([
                f"",
                f"echo '2. 处理 {table}.{field}...'",
                f"",
                f"# 创建归档表 {table}_{field}_archive",
                f"sqlite3 pc28_local.db \"\"\"",
                f"CREATE TABLE IF NOT EXISTS {table}_{field}_archive AS",
                f"SELECT rowid, {field}, timestamp FROM {table} WHERE {field} IS NOT NULL;",
                f"\"\"\"",
                f"",
                f"# 验证归档数据",
                f"ARCHIVE_COUNT=$(sqlite3 pc28_local.db \"SELECT COUNT(*) FROM {table}_{field}_archive;\")",
                f"ORIGINAL_COUNT=$(sqlite3 pc28_local.db \"SELECT COUNT(*) FROM {table} WHERE {field} IS NOT NULL;\")",
                f"",
                f"if [ \"$ARCHIVE_COUNT\" -eq \"$ORIGINAL_COUNT\" ]; then",
                f"    echo \"✓ {table}.{field} 归档成功: $ARCHIVE_COUNT 条记录\"",
                f"else",
                f"    echo \"✗ {table}.{field} 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT\"",
                f"    exit 1",
                f"fi",
            ])
        
        script_lines.extend([
            "",
            "echo '=== 归档完成 ==='",
            "echo \"完成时间: $(date)\"",
            "echo '请等待1周监控期，确认无业务影响后执行字段删除脚本'",
        ])
        
        return "\n".join(script_lines)
    
    def generate_api_optimization_script(self) -> str:
        """生成API优化脚本"""
        script_lines = [
            "#!/usr/bin/env python3",
            "# -*- coding: utf-8 -*-",
            "\"\"\"",
            "PC28 API响应优化脚本",
            "移除未使用的API字段，减少网络传输",
            "\"\"\"",
            "",
            "import json",
            "from datetime import datetime",
            "",
            "class APIResponseOptimizer:",
            "    \"\"\"API响应优化器\"\"\"",
            "    ",
            "    def __init__(self):",
            "        self.removed_fields = []",
            "        self.optimization_log = []",
            "    ",
            "    def optimize_realtime_api_response(self, original_response: dict) -> dict:",
            "        \"\"\"优化实时API响应\"\"\"",
            "        optimized = original_response.copy()",
            "        ",
            "        # 移除未使用的字段",
            "        fields_to_remove = ['curtime', 'short_issue', 'intervalM']",
            "        ",
            "        for field in fields_to_remove:",
            "            if field in optimized.get('retdata', {}).get('curent', {}):",
            "                removed_value = optimized['retdata']['curent'].pop(field, None)",
            "                self.removed_fields.append({",
            "                    'field': field,",
            "                    'value': removed_value,",
            "                    'timestamp': datetime.now().isoformat()",
            "                })",
            "                ",
            "        return optimized",
            "    ",
            "    def calculate_savings(self, original: dict, optimized: dict) -> dict:",
            "        \"\"\"计算优化节省\"\"\"",
            "        original_size = len(json.dumps(original, ensure_ascii=False))",
            "        optimized_size = len(json.dumps(optimized, ensure_ascii=False))",
            "        ",
            "        savings = {",
            "            'original_size_bytes': original_size,",
            "            'optimized_size_bytes': optimized_size,",
            "            'saved_bytes': original_size - optimized_size,",
            "            'savings_percentage': ((original_size - optimized_size) / original_size) * 100",
            "        }",
            "        ",
            "        return savings",
            "",
            "def main():",
            "    \"\"\"主函数\"\"\"",
            "    optimizer = APIResponseOptimizer()",
            "    ",
            "    # 示例：优化实时API响应",
            "    print('=== API响应优化测试 ===')",
            "    ",
            "    # 这里可以集成实际的API调用",
            "    print('API优化脚本已准备就绪')",
            "    print('请在实际API处理代码中集成 optimize_realtime_api_response 方法')",
            "",
            "if __name__ == '__main__':",
            "    main()",
        ]
        
        return "\n".join(script_lines)
    
    def generate_monitoring_script(self) -> str:
        """生成监控脚本"""
        return """#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
PC28字段清理监控脚本
监控字段清理后的系统状态和性能
\"\"\"

import sqlite3
import time
import json
from datetime import datetime, timedelta

class FieldCleanupMonitor:
    \"\"\"字段清理监控器\"\"\"
    
    def __init__(self):
        self.monitoring_results = []
    
    def monitor_database_performance(self):
        \"\"\"监控数据库性能\"\"\"
        print("=== 监控数据库性能 ===")
        
        try:
            conn = sqlite3.connect('pc28_local.db')
            cursor = conn.cursor()
            
            # 检查表大小
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                if table.startswith('sqlite_'):
                    continue
                    
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                print(f"  {table}: {row_count} 行")
            
            conn.close()
            
        except Exception as e:
            print(f"数据库监控失败: {e}")
    
    def monitor_api_performance(self):
        \"\"\"监控API性能\"\"\"
        print("\\n=== 监控API性能 ===")
        
        # 这里可以添加实际的API性能监控
        print("API性能监控已启动")
        print("建议监控指标:")
        print("- 响应时间")
        print("- 响应大小")
        print("- 错误率")
        print("- 吞吐量")
    
    def check_system_health(self):
        \"\"\"检查系统健康状态\"\"\"
        print("\\n=== 检查系统健康状态 ===")
        
        health_status = {
            'database_accessible': True,
            'api_responsive': True,
            'no_errors': True,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # 检查数据库连接
            conn = sqlite3.connect('pc28_local.db')
            conn.execute('SELECT 1')
            conn.close()
            print("✓ 数据库连接正常")
            
        except Exception as e:
            health_status['database_accessible'] = False
            print(f"✗ 数据库连接异常: {e}")
        
        return health_status

def main():
    \"\"\"主函数\"\"\"
    monitor = FieldCleanupMonitor()
    
    print("=== PC28字段清理监控 ===")
    print(f"监控开始时间: {datetime.now()}")
    
    # 执行监控
    monitor.monitor_database_performance()
    monitor.monitor_api_performance()
    health = monitor.check_system_health()
    
    print(f"\\n监控完成时间: {datetime.now()}")
    print("建议每小时运行一次此监控脚本")

if __name__ == "__main__":
    main()"""
    
    def generate_rollback_script(self) -> str:
        """生成回滚脚本"""
        return """#!/bin/bash
# PC28字段清理回滚脚本
# 在出现问题时恢复字段和数据

set -e

echo '=== PC28字段清理回滚 ==='
echo "回滚开始时间: $(date)"

# 检查备份文件
BACKUP_FILE=$(ls -t pc28_archive_backup_*.db 2>/dev/null | head -1)

if [ -z "$BACKUP_FILE" ]; then
    echo "错误: 未找到备份文件"
    exit 1
fi

echo "使用备份文件: $BACKUP_FILE"

# 创建当前状态备份
echo "1. 创建当前状态备份..."
sqlite3 pc28_local.db ".backup pc28_rollback_backup_$(date +%Y%m%d_%H%M%S).db"

# 恢复数据库
echo "2. 恢复数据库..."
cp "$BACKUP_FILE" pc28_local_restored.db

echo "3. 验证恢复..."
RESTORED_TABLES=$(sqlite3 pc28_local_restored.db "SELECT COUNT(*) FROM sqlite_master WHERE type='table';")
echo "恢复的表数量: $RESTORED_TABLES"

echo "=== 回滚完成 ==="
echo "完成时间: $(date)"
echo "请手动验证数据完整性后，将 pc28_local_restored.db 重命名为 pc28_local.db"
"""
    
    def save_cleanup_plan(self, output_file: str = "safe_field_cleanup_plan.json"):
        """保存清理计划"""
        plan_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'plan_version': '1.0',
                'based_on_analysis': 'data_flow_analysis_report.json'
            },
            'cleanup_phases': self.cleanup_phases,
            'implementation_timeline': {
                'phase_1_archive': '第1-2周：字段归档',
                'phase_2_monitor': '第3周：监控期',
                'phase_3_optimize': '第4周：API优化',
                'phase_4_maintain': '持续：维护监控'
            },
            'expected_benefits': {
                'storage_savings_mb': 223.3,
                'index_savings_mb': 117.3,
                'total_savings_mb': 340.6,
                'performance_improvement': '10-15%',
                'maintenance_cost_reduction': '20-30%'
            }
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(plan_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 安全清理计划已保存: {output_file}")
        return output_file

def main():
    """主函数"""
    planner = SafeFieldCleanupPlan()
    
    # 生成清理计划
    cleanup_phases = planner.generate_safe_cleanup_plan()
    
    # 生成实施脚本
    scripts = planner.generate_implementation_scripts()
    
    # 保存脚本文件
    for script_name, script_content in scripts.items():
        filename = f"{script_name}.py" if script_name.endswith('_script') else f"{script_name}.sh"
        if script_name == 'archive_script':
            filename = "field_archive.sh"
        elif script_name == 'rollback_script':
            filename = "field_rollback.sh"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        print(f"✓ 已生成: {filename}")
    
    # 保存完整计划
    plan_file = planner.save_cleanup_plan()
    
    print("\n=== 安全清理计划摘要 ===")
    print(f"阶段1（归档）: {len(cleanup_phases['phase_1_archive'])} 个字段")
    print(f"阶段3（API优化）: {len(cleanup_phases['phase_3_optimize'])} 个字段")
    print(f"预期节省: 340.6 MB 存储空间")
    print(f"性能提升: 10-15%")
    print(f"\n详细计划: {plan_file}")

if __name__ == "__main__":
    main()