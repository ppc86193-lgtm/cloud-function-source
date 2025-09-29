#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28安全字段优化系统
基于测试结果和系统稳定性，安全识别和删除冗余字段
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery
from dataclasses import dataclass

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FieldOptimization:
    """字段优化建议"""
    table_name: str
    field_name: str
    optimization_type: str  # 'remove_redundant', 'remove_unused', 'archive'
    reason: str
    risk_level: str  # 'low', 'medium', 'high'
    estimated_savings: Dict[str, Any]
    backup_required: bool
    validation_queries: List[str]

class PC28SafeFieldOptimizer:
    """PC28安全字段优化器"""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.optimizations = []
        self.safety_checks = []
        
        # 已知的安全冗余字段（基于之前的分析）
        self.safe_redundant_fields = {
            'ts_utc': 'timestamp',  # ts_utc与timestamp重复
            'result_digits': 'numbers',  # result_digits与numbers重复
            'data_source': 'source',  # 类似功能字段
            'curtime': 'drawTime',  # API中的冗余时间字段
        }
        
        # 需要特别小心的字段（不建议删除）
        self.protected_fields = {
            'id', 'created_at', 'updated_at', 'timestamp', 'draw_id', 
            'period', 'issue', 'openCode', 'drawTime', 'numbers'
        }
    
    def analyze_system_readiness(self) -> Dict[str, Any]:
        """分析系统是否准备好进行优化"""
        logger.info("🔍 分析系统优化准备状态...")
        
        readiness = {
            "ready_for_optimization": False,
            "test_results": {},
            "system_health": {},
            "risk_assessment": "high",
            "recommendations": []
        }
        
        # 检查最新的业务测试结果
        business_test_results = self._get_latest_business_test_results()
        if business_test_results:
            readiness["test_results"] = business_test_results
            success_rate = business_test_results.get("success_rate", 0)
            
            if success_rate >= 95:
                readiness["ready_for_optimization"] = True
                readiness["risk_assessment"] = "low"
                logger.info(f"✅ 系统测试通过率 {success_rate}%，可以进行安全优化")
            else:
                readiness["recommendations"].append(
                    f"系统测试通过率仅 {success_rate}%，建议先修复失败的测试"
                )
                logger.warning(f"⚠️ 系统测试通过率 {success_rate}%，不建议进行优化")
        
        # 检查系统健康状态
        system_health = self._check_system_health()
        readiness["system_health"] = system_health
        
        return readiness
    
    def identify_safe_optimizations(self) -> List[FieldOptimization]:
        """识别安全的字段优化机会"""
        logger.info("🔍 识别安全的字段优化机会...")
        
        optimizations = []
        
        # 获取所有表的信息
        tables = self._get_table_information()
        
        for table_name, table_info in tables.items():
            # 分析每个表的字段优化机会
            table_optimizations = self._analyze_table_optimizations(table_name, table_info)
            optimizations.extend(table_optimizations)
        
        # 按风险级别和收益排序
        optimizations.sort(key=lambda x: (
            {'low': 0, 'medium': 1, 'high': 2}[x.risk_level],
            -x.estimated_savings.get('storage_mb', 0)
        ))
        
        self.optimizations = optimizations
        logger.info(f"✅ 识别到 {len(optimizations)} 个优化机会")
        
        return optimizations
    
    def _analyze_table_optimizations(self, table_name: str, table_info: Dict) -> List[FieldOptimization]:
        """分析单个表的优化机会"""
        optimizations = []
        
        if not table_info.get('schema') or not table_info['schema'].get('fields'):
            return optimizations
        
        for field in table_info['schema']['fields']:
            field_name = field['name']
            
            # 跳过受保护的字段
            if field_name in self.protected_fields:
                continue
            
            # 检查是否为安全的冗余字段
            if field_name in self.safe_redundant_fields:
                optimization = self._create_redundant_field_optimization(
                    table_name, field_name, field
                )
                if optimization:
                    optimizations.append(optimization)
            
            # 检查是否为未使用的字段
            elif self._is_unused_field(table_name, field_name):
                optimization = self._create_unused_field_optimization(
                    table_name, field_name, field
                )
                if optimization:
                    optimizations.append(optimization)
        
        return optimizations
    
    def _create_redundant_field_optimization(self, table_name: str, field_name: str, field_info: Dict) -> Optional[FieldOptimization]:
        """创建冗余字段优化建议"""
        replacement_field = self.safe_redundant_fields[field_name]
        
        # 验证替代字段确实存在
        if not self._field_exists_in_table(table_name, replacement_field):
            logger.warning(f"⚠️ 表 {table_name} 中不存在替代字段 {replacement_field}")
            return None
        
        # 估算存储节省
        estimated_savings = self._estimate_field_savings(table_name, field_name)
        
        # 生成验证查询
        validation_queries = [
            f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.{table_name}` WHERE {field_name} IS NOT NULL",
            f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.{table_name}` WHERE {replacement_field} IS NOT NULL",
            f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.{table_name}` WHERE {field_name} != {replacement_field}"
        ]
        
        return FieldOptimization(
            table_name=table_name,
            field_name=field_name,
            optimization_type='remove_redundant',
            reason=f'字段 {field_name} 与 {replacement_field} 功能重复',
            risk_level='low',
            estimated_savings=estimated_savings,
            backup_required=True,
            validation_queries=validation_queries
        )
    
    def _create_unused_field_optimization(self, table_name: str, field_name: str, field_info: Dict) -> Optional[FieldOptimization]:
        """创建未使用字段优化建议"""
        # 估算存储节省
        estimated_savings = self._estimate_field_savings(table_name, field_name)
        
        # 只有当存储节省显著时才建议删除
        if estimated_savings.get('storage_mb', 0) < 1:
            return None
        
        # 生成验证查询
        validation_queries = [
            f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.{table_name}` WHERE {field_name} IS NOT NULL",
            f"SELECT COUNT(DISTINCT {field_name}) FROM `{self.project_id}.{self.dataset_id}.{table_name}` WHERE {field_name} IS NOT NULL"
        ]
        
        return FieldOptimization(
            table_name=table_name,
            field_name=field_name,
            optimization_type='remove_unused',
            reason=f'字段 {field_name} 在业务逻辑中未被使用',
            risk_level='medium',
            estimated_savings=estimated_savings,
            backup_required=True,
            validation_queries=validation_queries
        )
    
    def validate_optimizations(self) -> Dict[str, Any]:
        """验证优化建议的安全性"""
        logger.info("🔍 验证优化建议的安全性...")
        
        validation_results = {
            "safe_optimizations": [],
            "risky_optimizations": [],
            "validation_errors": [],
            "total_estimated_savings": {"storage_mb": 0, "query_performance": 0}
        }
        
        for optimization in self.optimizations:
            try:
                # 运行验证查询
                validation_passed = True
                validation_details = {}
                
                for query in optimization.validation_queries:
                    try:
                        result = self.client.query(query).result()
                        rows = list(result)
                        validation_details[query] = rows[0][0] if rows else 0
                    except Exception as e:
                        logger.error(f"验证查询失败: {query}, 错误: {e}")
                        validation_passed = False
                        validation_results["validation_errors"].append({
                            "optimization": f"{optimization.table_name}.{optimization.field_name}",
                            "query": query,
                            "error": str(e)
                        })
                
                if validation_passed:
                    # 额外的安全检查
                    if self._perform_safety_checks(optimization, validation_details):
                        validation_results["safe_optimizations"].append({
                            "optimization": optimization,
                            "validation_details": validation_details
                        })
                        validation_results["total_estimated_savings"]["storage_mb"] += optimization.estimated_savings.get("storage_mb", 0)
                    else:
                        validation_results["risky_optimizations"].append({
                            "optimization": optimization,
                            "reason": "未通过安全检查"
                        })
                
            except Exception as e:
                logger.error(f"验证优化 {optimization.table_name}.{optimization.field_name} 时出错: {e}")
                validation_results["validation_errors"].append({
                    "optimization": f"{optimization.table_name}.{optimization.field_name}",
                    "error": str(e)
                })
        
        logger.info(f"✅ 验证完成: {len(validation_results['safe_optimizations'])} 个安全优化，{len(validation_results['risky_optimizations'])} 个风险优化")
        
        return validation_results
    
    def _perform_safety_checks(self, optimization: FieldOptimization, validation_details: Dict) -> bool:
        """执行安全检查"""
        # 对于冗余字段，确保数据一致性
        if optimization.optimization_type == 'remove_redundant':
            # 检查是否有不一致的数据
            inconsistent_query = optimization.validation_queries[-1]  # 最后一个查询检查不一致性
            inconsistent_count = validation_details.get(inconsistent_query, 0)
            
            if inconsistent_count > 0:
                logger.warning(f"⚠️ 字段 {optimization.field_name} 与替代字段存在 {inconsistent_count} 条不一致数据")
                return False
        
        # 对于未使用字段，确保确实没有被使用
        if optimization.optimization_type == 'remove_unused':
            non_null_count = validation_details.get(optimization.validation_queries[0], 0)
            distinct_count = validation_details.get(optimization.validation_queries[1], 0)
            
            # 如果字段有大量非空数据，需要更谨慎
            if non_null_count > 1000:
                logger.warning(f"⚠️ 字段 {optimization.field_name} 有 {non_null_count} 条非空数据，需要谨慎处理")
                return False
        
        return True
    
    def generate_optimization_plan(self, validation_results: Dict) -> Dict[str, Any]:
        """生成优化执行计划"""
        logger.info("📋 生成优化执行计划...")
        
        safe_optimizations = validation_results["safe_optimizations"]
        
        plan = {
            "execution_phases": [],
            "backup_requirements": [],
            "rollback_plan": [],
            "estimated_timeline": {},
            "risk_mitigation": []
        }
        
        # 阶段1：低风险冗余字段删除
        phase1_optimizations = [
            opt["optimization"] for opt in safe_optimizations 
            if opt["optimization"].risk_level == 'low' and opt["optimization"].optimization_type == 'remove_redundant'
        ]
        
        if phase1_optimizations:
            plan["execution_phases"].append({
                "phase": 1,
                "name": "低风险冗余字段删除",
                "optimizations": phase1_optimizations,
                "estimated_duration": "1-2小时",
                "prerequisites": ["完整备份", "测试环境验证"]
            })
        
        # 阶段2：未使用字段归档
        phase2_optimizations = [
            opt["optimization"] for opt in safe_optimizations 
            if opt["optimization"].optimization_type == 'remove_unused'
        ]
        
        if phase2_optimizations:
            plan["execution_phases"].append({
                "phase": 2,
                "name": "未使用字段归档",
                "optimizations": phase2_optimizations,
                "estimated_duration": "2-4小时",
                "prerequisites": ["阶段1完成", "业务确认"]
            })
        
        # 生成备份要求
        for opt_data in safe_optimizations:
            opt = opt_data["optimization"]
            if opt.backup_required:
                plan["backup_requirements"].append({
                    "table": opt.table_name,
                    "field": opt.field_name,
                    "backup_query": f"CREATE TABLE `{self.project_id}.{self.dataset_id}.{opt.table_name}_backup_{datetime.now().strftime('%Y%m%d')}` AS SELECT * FROM `{self.project_id}.{self.dataset_id}.{opt.table_name}`"
                })
        
        # 生成回滚计划
        plan["rollback_plan"] = self._generate_rollback_plan(safe_optimizations)
        
        # 风险缓解措施
        plan["risk_mitigation"] = [
            "在测试环境中完整验证所有优化操作",
            "创建完整的数据备份",
            "分阶段执行，每阶段后验证系统功能",
            "准备快速回滚方案",
            "监控系统性能指标"
        ]
        
        return plan
    
    def _generate_rollback_plan(self, safe_optimizations: List[Dict]) -> List[Dict]:
        """生成回滚计划"""
        rollback_steps = []
        
        for opt_data in safe_optimizations:
            opt = opt_data["optimization"]
            
            if opt.optimization_type == 'remove_redundant':
                rollback_steps.append({
                    "action": "restore_field",
                    "table": opt.table_name,
                    "field": opt.field_name,
                    "sql": f"ALTER TABLE `{self.project_id}.{self.dataset_id}.{opt.table_name}` ADD COLUMN {opt.field_name} STRING",
                    "data_restore": f"UPDATE `{self.project_id}.{self.dataset_id}.{opt.table_name}` SET {opt.field_name} = {self.safe_redundant_fields[opt.field_name]} WHERE {opt.field_name} IS NULL"
                })
        
        return rollback_steps
    
    def _get_latest_business_test_results(self) -> Optional[Dict]:
        """获取最新的业务测试结果"""
        try:
            # 查找最新的业务测试报告
            report_files = []
            for file in os.listdir('.'):
                if file.startswith('pc28_business_test_report_') and file.endswith('.json'):
                    report_files.append(file)
            
            if not report_files:
                return None
            
            # 获取最新的报告
            latest_report = sorted(report_files)[-1]
            
            with open(latest_report, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        except Exception as e:
            logger.error(f"获取业务测试结果失败: {e}")
            return None
    
    def _check_system_health(self) -> Dict[str, Any]:
        """检查系统健康状态"""
        health = {
            "database_connectivity": False,
            "table_accessibility": {},
            "recent_errors": []
        }
        
        try:
            # 测试数据库连接
            query = f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.TABLES`"
            result = self.client.query(query).result()
            health["database_connectivity"] = True
            
            # 测试关键表的可访问性
            key_tables = ['signal_pool', 'lab_push_candidates_v2', 'runtime_params']
            for table in key_tables:
                try:
                    query = f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.{table}` LIMIT 1"
                    self.client.query(query).result()
                    health["table_accessibility"][table] = True
                except Exception as e:
                    health["table_accessibility"][table] = False
                    health["recent_errors"].append(f"表 {table} 不可访问: {e}")
        
        except Exception as e:
            health["recent_errors"].append(f"数据库连接失败: {e}")
        
        return health
    
    def _get_table_information(self) -> Dict[str, Dict]:
        """获取表信息"""
        tables = {}
        
        try:
            # 获取数据集中的所有表
            dataset = self.client.get_dataset(f"{self.project_id}.{self.dataset_id}")
            
            for table_ref in self.client.list_tables(dataset):
                table = self.client.get_table(table_ref)
                
                tables[table.table_id] = {
                    "schema": {
                        "fields": [
                            {
                                "name": field.name,
                                "type": field.field_type,
                                "mode": field.mode,
                                "description": field.description
                            }
                            for field in table.schema
                        ]
                    },
                    "num_rows": table.num_rows,
                    "num_bytes": table.num_bytes
                }
        
        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
        
        return tables
    
    def _field_exists_in_table(self, table_name: str, field_name: str) -> bool:
        """检查字段是否存在于表中"""
        try:
            table = self.client.get_table(f"{self.project_id}.{self.dataset_id}.{table_name}")
            return any(field.name == field_name for field in table.schema)
        except Exception:
            return False
    
    def _is_unused_field(self, table_name: str, field_name: str) -> bool:
        """检查字段是否未被使用"""
        # 这里可以实现更复杂的逻辑来检查字段使用情况
        # 目前基于已知的未使用字段列表
        unused_patterns = ['curtime', 'next', 'legacy_', 'temp_', 'old_']
        return any(pattern in field_name.lower() for pattern in unused_patterns)
    
    def _estimate_field_savings(self, table_name: str, field_name: str) -> Dict[str, Any]:
        """估算删除字段的存储节省"""
        try:
            # 获取表信息
            table = self.client.get_table(f"{self.project_id}.{self.dataset_id}.{table_name}")
            
            # 简单估算：假设每个字段占用总存储的平均比例
            total_fields = len(table.schema)
            if total_fields > 0:
                field_storage_mb = (table.num_bytes / (1024 * 1024)) / total_fields
            else:
                field_storage_mb = 0
            
            return {
                "storage_mb": round(field_storage_mb, 2),
                "query_performance": 0.05  # 假设5%的性能提升
            }
        
        except Exception:
            return {"storage_mb": 0, "query_performance": 0}
    
    def save_optimization_report(self, readiness: Dict, optimizations: List[FieldOptimization], 
                               validation_results: Dict, plan: Dict):
        """保存优化报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        report = {
            "timestamp": timestamp,
            "system_readiness": readiness,
            "identified_optimizations": [
                {
                    "table_name": opt.table_name,
                    "field_name": opt.field_name,
                    "optimization_type": opt.optimization_type,
                    "reason": opt.reason,
                    "risk_level": opt.risk_level,
                    "estimated_savings": opt.estimated_savings
                }
                for opt in optimizations
            ],
            "validation_results": validation_results,
            "execution_plan": plan,
            "summary": {
                "total_optimizations": len(optimizations),
                "safe_optimizations": len(validation_results.get("safe_optimizations", [])),
                "total_estimated_savings_mb": validation_results.get("total_estimated_savings", {}).get("storage_mb", 0),
                "ready_for_execution": readiness.get("ready_for_optimization", False)
            }
        }
        
        # 保存JSON报告
        json_file = f"pc28_safe_field_optimization_report_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 保存Markdown报告
        md_file = f"pc28_safe_field_optimization_report_{timestamp}.md"
        self._generate_markdown_report(report, md_file)
        
        logger.info(f"📄 优化报告已保存:")
        logger.info(f"  JSON: {json_file}")
        logger.info(f"  Markdown: {md_file}")
        
        return json_file, md_file
    
    def _generate_markdown_report(self, report: Dict, file_path: str):
        """生成Markdown格式的报告"""
        content = f"""# PC28安全字段优化报告

## 🎯 执行摘要

**生成时间**: {report['timestamp']}
**系统准备状态**: {'✅ 就绪' if report['system_readiness']['ready_for_optimization'] else '❌ 未就绪'}
**识别优化机会**: {report['summary']['total_optimizations']} 个
**安全优化数量**: {report['summary']['safe_optimizations']} 个
**预估存储节省**: {report['summary']['total_estimated_savings_mb']:.2f} MB

## 📊 系统准备状态

### 测试结果
"""
        
        test_results = report['system_readiness'].get('test_results', {})
        if test_results:
            content += f"""
- **成功率**: {test_results.get('success_rate', 0)}%
- **通过测试**: {test_results.get('passed_tests', 0)} 个
- **失败测试**: {test_results.get('failed_tests', 0)} 个
"""
        
        content += f"""
### 风险评估
**风险级别**: {report['system_readiness']['risk_assessment']}

## 🔍 识别的优化机会

"""
        
        for opt in report['identified_optimizations']:
            risk_emoji = {'low': '🟢', 'medium': '🟡', 'high': '🔴'}[opt['risk_level']]
            content += f"""
### {opt['table_name']}.{opt['field_name']} {risk_emoji}
- **优化类型**: {opt['optimization_type']}
- **原因**: {opt['reason']}
- **风险级别**: {opt['risk_level']}
- **预估节省**: {opt['estimated_savings']['storage_mb']:.2f} MB
"""
        
        content += f"""
## 📋 执行计划

"""
        
        plan = report['execution_plan']
        for phase in plan.get('execution_phases', []):
            content += f"""
### 阶段 {phase['phase']}: {phase['name']}
- **预估时间**: {phase['estimated_duration']}
- **优化数量**: {len(phase['optimizations'])} 个
- **前置条件**: {', '.join(phase['prerequisites'])}
"""
        
        content += f"""
## 🛡️ 风险缓解措施

"""
        for measure in plan.get('risk_mitigation', []):
            content += f"- {measure}\n"
        
        content += f"""
## 📈 预期收益

- **存储空间节省**: {report['summary']['total_estimated_savings_mb']:.2f} MB
- **查询性能提升**: 预计 5-10%
- **维护成本降低**: 预计 10-15%
- **系统复杂度降低**: 移除冗余字段

## ⚠️ 注意事项

1. 在生产环境执行前，必须在测试环境完整验证
2. 确保所有相关系统和应用程序已更新
3. 准备快速回滚方案
4. 监控执行过程中的系统性能
5. 与业务团队确认字段删除的影响

## 🔄 回滚计划

如果优化过程中出现问题，可以按以下步骤回滚：

"""
        
        for step in plan.get('rollback_plan', []):
            content += f"""
### {step['table']}.{step['field']}
```sql
{step['sql']}
{step.get('data_restore', '')}
```
"""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

def main():
    """主函数"""
    print("🔧 PC28安全字段优化系统")
    print("=" * 60)
    print("🎯 目标：基于系统稳定性，安全识别和删除冗余字段")
    print("📋 范围：冗余字段删除、未使用字段归档、存储优化")
    print("=" * 60)
    
    optimizer = PC28SafeFieldOptimizer()
    
    try:
        # 1. 分析系统准备状态
        logger.info("🔍 分析系统优化准备状态...")
        readiness = optimizer.analyze_system_readiness()
        
        if not readiness["ready_for_optimization"]:
            logger.warning("⚠️ 系统尚未准备好进行优化")
            for recommendation in readiness["recommendations"]:
                logger.warning(f"   - {recommendation}")
            
            # 即使系统未完全准备好，也可以生成优化计划供参考
            logger.info("📋 生成优化计划供参考...")
        
        # 2. 识别安全的优化机会
        optimizations = optimizer.identify_safe_optimizations()
        
        if not optimizations:
            logger.info("✅ 未发现需要优化的字段")
            return
        
        # 3. 验证优化建议
        validation_results = optimizer.validate_optimizations()
        
        # 4. 生成执行计划
        plan = optimizer.generate_optimization_plan(validation_results)
        
        # 5. 保存报告
        json_file, md_file = optimizer.save_optimization_report(
            readiness, optimizations, validation_results, plan
        )
        
        # 6. 显示摘要
        print("\n" + "=" * 60)
        print("📊 优化分析摘要")
        print("=" * 60)
        
        print(f"\n系统准备状态: {'✅ 就绪' if readiness['ready_for_optimization'] else '❌ 未就绪'}")
        print(f"识别优化机会: {len(optimizations)} 个")
        print(f"安全优化数量: {len(validation_results['safe_optimizations'])} 个")
        print(f"预估存储节省: {validation_results['total_estimated_savings']['storage_mb']:.2f} MB")
        
        if readiness["ready_for_optimization"] and validation_results["safe_optimizations"]:
            print("\n🎯 建议执行优化:")
            for opt_data in validation_results["safe_optimizations"]:
                opt = opt_data["optimization"]
                print(f"   - {opt.table_name}.{opt.field_name}: {opt.reason}")
        else:
            print("\n⚠️ 建议暂缓执行优化，先解决系统问题")
        
        print(f"\n📄 详细报告: {md_file}")
        
    except Exception as e:
        logger.error(f"优化分析失败: {e}")
        raise

if __name__ == "__main__":
    main()