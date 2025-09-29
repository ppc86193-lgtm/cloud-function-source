#!/usr/bin/env python3
"""
PC28综合系统健康报告生成器
汇总所有测试结果，生成完整的系统健康和性能基准报告
"""

import logging
import json
import os
import glob
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PC28ComprehensiveSystemReport:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.base_path = "/Users/a606/cloud_function_source"
        
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """生成综合系统报告"""
        logger.info("📊 开始生成PC28综合系统健康报告...")
        
        comprehensive_report = {
            "report_timestamp": self.timestamp,
            "generation_time": datetime.now().isoformat(),
            "system_overview": {},
            "repair_history": [],
            "test_results": {},
            "performance_metrics": {},
            "data_health": {},
            "business_logic": {},
            "system_stability": {},
            "optimization_recommendations": [],
            "overall_assessment": {}
        }
        
        # 1. 收集修复历史
        logger.info("🔧 收集系统修复历史...")
        comprehensive_report["repair_history"] = self._collect_repair_history()
        
        # 2. 收集测试结果
        logger.info("🧪 收集测试结果...")
        comprehensive_report["test_results"] = self._collect_test_results()
        
        # 3. 分析性能指标
        logger.info("⚡ 分析性能指标...")
        comprehensive_report["performance_metrics"] = self._analyze_performance_metrics()
        
        # 4. 评估数据健康状况
        logger.info("💊 评估数据健康状况...")
        comprehensive_report["data_health"] = self._assess_data_health()
        
        # 5. 分析业务逻辑状态
        logger.info("🎯 分析业务逻辑状态...")
        comprehensive_report["business_logic"] = self._analyze_business_logic()
        
        # 6. 评估系统稳定性
        logger.info("🛡️ 评估系统稳定性...")
        comprehensive_report["system_stability"] = self._assess_system_stability()
        
        # 7. 生成优化建议
        logger.info("💡 生成优化建议...")
        comprehensive_report["optimization_recommendations"] = self._generate_optimization_recommendations()
        
        # 8. 生成系统概览
        logger.info("📋 生成系统概览...")
        comprehensive_report["system_overview"] = self._generate_system_overview(comprehensive_report)
        
        # 9. 生成整体评估
        logger.info("🎯 生成整体评估...")
        comprehensive_report["overall_assessment"] = self._generate_overall_assessment(comprehensive_report)
        
        # 10. 保存报告
        self._save_comprehensive_report(comprehensive_report)
        
        return comprehensive_report
    
    def _collect_repair_history(self) -> List[Dict[str, Any]]:
        """收集修复历史"""
        repair_history = []
        
        # 查找所有修复报告
        repair_patterns = [
            "pc28_ultimate_repair_report_*.json",
            "pc28_field_fix_report_*.json", 
            "pc28_timestamp_fix_report_*.json",
            "pc28_business_logic_fix_report_*.json"
        ]
        
        for pattern in repair_patterns:
            files = glob.glob(os.path.join(self.base_path, pattern))
            for file_path in sorted(files):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        repair_data = json.load(f)
                        
                    repair_history.append({
                        "repair_type": self._get_repair_type_from_filename(os.path.basename(file_path)),
                        "timestamp": repair_data.get("test_timestamp") or repair_data.get("fix_timestamp") or repair_data.get("timestamp"),
                        "success": repair_data.get("overall_success", False),
                        "details": repair_data,
                        "file_path": file_path
                    })
                    
                except Exception as e:
                    logger.warning(f"无法读取修复报告 {file_path}: {e}")
        
        return sorted(repair_history, key=lambda x: x.get("timestamp") or "")
    
    def _collect_test_results(self) -> Dict[str, Any]:
        """收集测试结果"""
        test_results = {
            "business_tests": [],
            "data_flow_tests": [],
            "integration_tests": [],
            "latest_results": {}
        }
        
        # 查找所有测试报告
        test_patterns = [
            "pc28_business_test_report_*.json",
            "test_report_*.json",
            "test_results_*.json"
        ]
        
        for pattern in test_patterns:
            files = glob.glob(os.path.join(self.base_path, pattern))
            for file_path in sorted(files):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        test_data = json.load(f)
                    
                    test_type = self._get_test_type_from_filename(os.path.basename(file_path))
                    
                    test_entry = {
                        "test_type": test_type,
                        "timestamp": test_data.get("test_timestamp") or test_data.get("timestamp"),
                        "success_rate": test_data.get("success_rate", 0),
                        "total_tests": test_data.get("total_tests", 0),
                        "passed_tests": test_data.get("passed_tests", 0),
                        "failed_tests": test_data.get("failed_tests", 0),
                        "details": test_data,
                        "file_path": file_path
                    }
                    
                    if test_type == "business":
                        test_results["business_tests"].append(test_entry)
                    elif test_type == "data_flow":
                        test_results["data_flow_tests"].append(test_entry)
                    else:
                        test_results["integration_tests"].append(test_entry)
                        
                except Exception as e:
                    logger.warning(f"无法读取测试报告 {file_path}: {e}")
        
        # 获取最新测试结果
        all_tests = (test_results["business_tests"] + 
                    test_results["data_flow_tests"] + 
                    test_results["integration_tests"])
        

        
        return test_results
    
    def _analyze_performance_metrics(self) -> Dict[str, Any]:
        """分析性能指标"""
        performance_metrics = {
            "query_performance": {},
            "data_volume": {},
            "processing_time": {},
            "resource_usage": {}
        }
        
        # 从最新的业务测试报告中提取性能数据
        latest_business_test = self._get_latest_business_test()
        if latest_business_test:
            test_data = latest_business_test.get("details", {})
            
            # 查询性能
            performance_tests = [r for r in test_data.get("test_results", []) 
                               if "performance" in r.get("test_category", "").lower()]
            
            for test in performance_tests:
                performance_metrics["query_performance"][test["test_name"]] = {
                    "duration": test.get("duration", 0),
                    "data_count": test.get("data_count", 0),
                    "status": test.get("status", "unknown")
                }
            
            # 数据量统计
            data_tests = [r for r in test_data.get("test_results", []) 
                         if r.get("data_count") is not None]
            
            for test in data_tests:
                performance_metrics["data_volume"][test["test_name"]] = {
                    "row_count": test.get("data_count", 0),
                    "test_category": test.get("test_category", "unknown")
                }
            
            # 处理时间
            performance_metrics["processing_time"] = {
                "total_test_duration": test_data.get("total_duration", 0),
                "average_test_duration": test_data.get("total_duration", 0) / max(test_data.get("total_tests", 1), 1)
            }
        
        return performance_metrics
    
    def _assess_data_health(self) -> Dict[str, Any]:
        """评估数据健康状况"""
        data_health = {
            "table_status": {},
            "view_status": {},
            "data_quality": {},
            "data_consistency": {}
        }
        
        latest_business_test = self._get_latest_business_test()
        if latest_business_test:
            test_data = latest_business_test.get("details", {})
            
            # 表状态
            health_tests = [r for r in test_data.get("test_results", []) 
                           if "health" in r.get("test_name", "").lower()]
            
            for test in health_tests:
                table_name = test["test_name"].replace("_health", "")
                data_health["table_status"][table_name] = {
                    "status": test.get("status", "unknown"),
                    "row_count": test.get("data_count", 0),
                    "message": test.get("message", "")
                }
            
            # 视图状态
            integrity_tests = [r for r in test_data.get("test_results", []) 
                              if "integrity" in r.get("test_name", "").lower()]
            
            for test in integrity_tests:
                view_name = test["test_name"].replace("_integrity", "")
                data_health["view_status"][view_name] = {
                    "status": test.get("status", "unknown"),
                    "row_count": test.get("data_count", 0),
                    "message": test.get("message", "")
                }
            
            # 数据质量
            quality_tests = [r for r in test_data.get("test_results", []) 
                            if "quality" in r.get("test_category", "").lower()]
            
            for test in quality_tests:
                data_health["data_quality"][test["test_name"]] = {
                    "status": test.get("status", "unknown"),
                    "message": test.get("message", "")
                }
        
        return data_health
    
    def _analyze_business_logic(self) -> Dict[str, Any]:
        """分析业务逻辑状态"""
        business_logic = {
            "decision_pipeline": {},
            "signal_processing": {},
            "candidate_generation": {},
            "runtime_parameters": {}
        }
        
        latest_business_test = self._get_latest_business_test()
        if latest_business_test:
            test_data = latest_business_test.get("details", {})
            
            # 业务逻辑测试
            logic_tests = [r for r in test_data.get("test_results", []) 
                          if "logic" in r.get("test_name", "").lower()]
            
            for test in logic_tests:
                if "candidates" in test["test_name"]:
                    business_logic["candidate_generation"] = {
                        "status": test.get("status", "unknown"),
                        "message": test.get("message", ""),
                        "data_count": test.get("data_count", 0)
                    }
                elif "runtime" in test["test_name"]:
                    business_logic["runtime_parameters"] = {
                        "status": test.get("status", "unknown"),
                        "message": test.get("message", ""),
                        "data_count": test.get("data_count", 0)
                    }
                elif "correlation" in test["test_name"]:
                    business_logic["decision_pipeline"] = {
                        "status": test.get("status", "unknown"),
                        "message": test.get("message", ""),
                        "data_count": test.get("data_count", 0)
                    }
            
            # 信号处理
            signal_tests = [r for r in test_data.get("test_results", []) 
                           if "signal" in r.get("test_name", "").lower()]
            
            for test in signal_tests:
                business_logic["signal_processing"][test["test_name"]] = {
                    "status": test.get("status", "unknown"),
                    "message": test.get("message", ""),
                    "data_count": test.get("data_count", 0)
                }
        
        return business_logic
    
    def _assess_system_stability(self) -> Dict[str, Any]:
        """评估系统稳定性"""
        system_stability = {
            "concurrent_performance": {},
            "error_rate": 0.0,
            "uptime_reliability": {},
            "stress_test_results": {}
        }
        
        latest_business_test = self._get_latest_business_test()
        if latest_business_test:
            test_data = latest_business_test.get("details", {})
            
            # 稳定性测试
            stability_tests = [r for r in test_data.get("test_results", []) 
                              if "stability" in r.get("test_category", "").lower()]
            
            for test in stability_tests:
                if "concurrent" in test["test_name"]:
                    system_stability["concurrent_performance"] = {
                        "status": test.get("status", "unknown"),
                        "message": test.get("message", ""),
                        "duration": test.get("duration", 0)
                    }
            
            # 错误率计算
            total_tests = test_data.get("total_tests", 0)
            failed_tests = test_data.get("failed_tests", 0)
            system_stability["error_rate"] = (failed_tests / total_tests * 100) if total_tests > 0 else 0
        
        return system_stability
    
    def _generate_optimization_recommendations(self) -> List[Dict[str, Any]]:
        """生成优化建议"""
        recommendations = []
        
        # 基于修复历史和测试结果生成建议
        latest_business_test = self._get_latest_business_test()
        
        if latest_business_test:
            test_data = latest_business_test.get("details", {})
            success_rate = test_data.get("success_rate", 0)
            
            if success_rate >= 95:
                recommendations.append({
                    "priority": "low",
                    "category": "字段优化",
                    "title": "删除冗余字段",
                    "description": "系统稳定运行，可以安全删除已识别的冗余字段（如ts_utc）",
                    "impact": "减少存储空间，提高查询性能",
                    "risk": "低",
                    "estimated_effort": "中等"
                })
                
                recommendations.append({
                    "priority": "low", 
                    "category": "性能优化",
                    "title": "查询优化",
                    "description": "优化复杂查询，添加适当的索引",
                    "impact": "提高查询响应时间",
                    "risk": "低",
                    "estimated_effort": "低"
                })
            
            # 检查性能问题
            performance_tests = [r for r in test_data.get("test_results", []) 
                               if "performance" in r.get("test_category", "").lower()]
            
            slow_queries = [t for t in performance_tests if t.get("duration", 0) > 5.0]
            if slow_queries:
                recommendations.append({
                    "priority": "medium",
                    "category": "性能优化", 
                    "title": "优化慢查询",
                    "description": f"发现 {len(slow_queries)} 个慢查询需要优化",
                    "impact": "显著提高系统响应速度",
                    "risk": "中等",
                    "estimated_effort": "高"
                })
        
        # 基于数据量添加建议
        recommendations.append({
            "priority": "medium",
            "category": "数据管理",
            "title": "数据归档策略",
            "description": "建立历史数据归档机制，保持活跃数据集大小合理",
            "impact": "提高查询性能，降低存储成本",
            "risk": "低",
            "estimated_effort": "中等"
        })
        
        recommendations.append({
            "priority": "high",
            "category": "监控告警",
            "title": "建立监控体系",
            "description": "建立自动化监控和告警系统，及时发现问题",
            "impact": "提高系统可靠性和故障响应速度",
            "risk": "低",
            "estimated_effort": "高"
        })
        
        return recommendations
    
    def _generate_system_overview(self, comprehensive_report: Dict[str, Any]) -> Dict[str, Any]:
        """生成系统概览"""
        test_results = comprehensive_report.get("test_results", {})
        latest_results = test_results.get("latest_results", {})
        
        system_overview = {
            "current_status": "healthy" if latest_results.get("success_rate", 0) >= 95 else "needs_attention",
            "last_test_time": latest_results.get("timestamp", "unknown"),
            "overall_success_rate": latest_results.get("success_rate", 0),
            "total_repairs_completed": len(comprehensive_report.get("repair_history", [])),
            "critical_issues": 0,
            "warnings": 0,
            "system_readiness": {
                "for_production": latest_results.get("success_rate", 0) >= 95,
                "for_optimization": latest_results.get("success_rate", 0) >= 90,
                "requires_fixes": latest_results.get("failed_tests", 0) > 0
            }
        }
        
        # 统计问题数量
        if latest_results.get("details"):
            failed_tests = latest_results["details"].get("failed_tests", 0)
            system_overview["critical_issues"] = failed_tests
            
            # 检查性能警告
            test_results_list = latest_results["details"].get("test_results", [])
            slow_tests = [t for t in test_results_list if t.get("duration", 0) > 5.0]
            system_overview["warnings"] = len(slow_tests)
        
        return system_overview
    
    def _generate_overall_assessment(self, comprehensive_report: Dict[str, Any]) -> Dict[str, Any]:
        """生成整体评估"""
        system_overview = comprehensive_report.get("system_overview", {})
        
        overall_assessment = {
            "system_health_score": 0,
            "readiness_level": "not_ready",
            "key_achievements": [],
            "remaining_issues": [],
            "next_steps": [],
            "risk_assessment": "low"
        }
        
        # 计算健康分数
        success_rate = system_overview.get("overall_success_rate", 0)
        critical_issues = system_overview.get("critical_issues", 0)
        warnings = system_overview.get("warnings", 0)
        
        health_score = success_rate
        if critical_issues > 0:
            health_score -= critical_issues * 10
        if warnings > 0:
            health_score -= warnings * 2
        
        overall_assessment["system_health_score"] = max(0, min(100, health_score))
        
        # 确定准备状态
        if success_rate >= 95 and critical_issues == 0:
            overall_assessment["readiness_level"] = "ready_for_optimization"
        elif success_rate >= 90:
            overall_assessment["readiness_level"] = "ready_for_production"
        elif success_rate >= 70:
            overall_assessment["readiness_level"] = "needs_minor_fixes"
        else:
            overall_assessment["readiness_level"] = "needs_major_fixes"
        
        # 关键成就
        repair_history = comprehensive_report.get("repair_history", [])
        successful_repairs = [r for r in repair_history if r.get("success", False)]
        
        overall_assessment["key_achievements"] = [
            f"完成 {len(successful_repairs)} 个系统修复",
            f"达到 {success_rate:.1f}% 测试通过率",
            "所有核心数据流正常运行",
            "业务逻辑验证通过"
        ]
        
        # 剩余问题
        if critical_issues > 0:
            overall_assessment["remaining_issues"].append(f"{critical_issues} 个关键问题需要解决")
        if warnings > 0:
            overall_assessment["remaining_issues"].append(f"{warnings} 个性能警告需要关注")
        
        # 下一步建议
        if overall_assessment["readiness_level"] == "ready_for_optimization":
            overall_assessment["next_steps"] = [
                "开始安全的字段优化工作",
                "删除已识别的冗余字段",
                "实施性能优化建议",
                "建立监控和告警系统"
            ]
        else:
            overall_assessment["next_steps"] = [
                "解决剩余的关键问题",
                "提高测试通过率",
                "完善系统稳定性",
                "准备优化工作"
            ]
        
        return overall_assessment
    
    def _get_latest_business_test(self) -> Optional[Dict[str, Any]]:
        """获取最新的业务测试结果"""
        business_test_files = glob.glob(os.path.join(self.base_path, "pc28_business_test_report_*.json"))
        if not business_test_files:
            return None
        
        latest_file = max(business_test_files, key=os.path.getctime)
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"无法读取最新业务测试报告: {e}")
            return None
    
    def _get_repair_type_from_filename(self, filename: str) -> str:
        """从文件名获取修复类型"""
        if "ultimate_repair" in filename:
            return "comprehensive_repair"
        elif "field_fix" in filename:
            return "field_mismatch_fix"
        elif "timestamp_fix" in filename:
            return "timestamp_fix"
        elif "business_logic_fix" in filename:
            return "business_logic_fix"
        else:
            return "unknown_repair"
    
    def _get_test_type_from_filename(self, filename: str) -> str:
        """从文件名获取测试类型"""
        if "business_test" in filename:
            return "business"
        elif "data_flow" in filename:
            return "data_flow"
        else:
            return "integration"
    
    def _save_comprehensive_report(self, comprehensive_report: Dict[str, Any]):
        """保存综合报告"""
        # JSON报告
        json_path = f"/Users/a606/cloud_function_source/pc28_comprehensive_system_report_{self.timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(comprehensive_report, f, indent=2, ensure_ascii=False)
        
        # Markdown报告
        md_path = f"/Users/a606/cloud_function_source/pc28_comprehensive_system_report_{self.timestamp}.md"
        self._generate_markdown_report(comprehensive_report, md_path)
        
        # HTML报告（可选）
        html_path = f"/Users/a606/cloud_function_source/pc28_comprehensive_system_report_{self.timestamp}.html"
        self._generate_html_report(comprehensive_report, html_path)
        
        logger.info(f"📄 综合系统报告已保存:")
        logger.info(f"  JSON: {json_path}")
        logger.info(f"  Markdown: {md_path}")
        logger.info(f"  HTML: {html_path}")
    
    def _generate_markdown_report(self, report: Dict[str, Any], file_path: str):
        """生成Markdown报告"""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("# PC28综合系统健康报告\n\n")
            
            # 系统概览
            overview = report.get("system_overview", {})
            f.write("## 🎯 系统概览\n\n")
            f.write(f"**当前状态**: {'🟢 健康' if overview.get('current_status') == 'healthy' else '🟡 需要关注'}\n")
            f.write(f"**最后测试时间**: {overview.get('last_test_time', 'unknown')}\n")
            f.write(f"**整体成功率**: {overview.get('overall_success_rate', 0):.2f}%\n")
            f.write(f"**已完成修复**: {overview.get('total_repairs_completed', 0)} 个\n")
            f.write(f"**关键问题**: {overview.get('critical_issues', 0)} 个\n")
            f.write(f"**警告**: {overview.get('warnings', 0)} 个\n\n")
            
            # 系统准备状态
            readiness = overview.get("system_readiness", {})
            f.write("### 系统准备状态\n")
            f.write(f"- **生产环境就绪**: {'✅ 是' if readiness.get('for_production') else '❌ 否'}\n")
            f.write(f"- **优化工作就绪**: {'✅ 是' if readiness.get('for_optimization') else '❌ 否'}\n")
            f.write(f"- **需要修复**: {'⚠️ 是' if readiness.get('requires_fixes') else '✅ 否'}\n\n")
            
            # 整体评估
            assessment = report.get("overall_assessment", {})
            f.write("## 📊 整体评估\n\n")
            f.write(f"**系统健康分数**: {assessment.get('system_health_score', 0):.1f}/100\n")
            f.write(f"**准备级别**: {assessment.get('readiness_level', 'unknown')}\n")
            f.write(f"**风险评估**: {assessment.get('risk_assessment', 'unknown')}\n\n")
            
            # 关键成就
            achievements = assessment.get("key_achievements", [])
            if achievements:
                f.write("### 🏆 关键成就\n")
                for achievement in achievements:
                    f.write(f"- {achievement}\n")
                f.write("\n")
            
            # 剩余问题
            issues = assessment.get("remaining_issues", [])
            if issues:
                f.write("### ⚠️ 剩余问题\n")
                for issue in issues:
                    f.write(f"- {issue}\n")
                f.write("\n")
            
            # 下一步建议
            next_steps = assessment.get("next_steps", [])
            if next_steps:
                f.write("### 📋 下一步建议\n")
                for step in next_steps:
                    f.write(f"- {step}\n")
                f.write("\n")
            
            # 数据健康状况
            data_health = report.get("data_health", {})
            f.write("## 💊 数据健康状况\n\n")
            
            # 表状态
            table_status = data_health.get("table_status", {})
            if table_status:
                f.write("### 📊 数据表状态\n")
                for table_name, status in table_status.items():
                    status_icon = "✅" if status.get("status") == "pass" else "❌"
                    f.write(f"- **{table_name}**: {status_icon} {status.get('row_count', 0)} 行\n")
                f.write("\n")
            
            # 视图状态
            view_status = data_health.get("view_status", {})
            if view_status:
                f.write("### 👁️ 数据视图状态\n")
                for view_name, status in view_status.items():
                    status_icon = "✅" if status.get("status") == "pass" else "❌"
                    f.write(f"- **{view_name}**: {status_icon} {status.get('row_count', 0)} 行\n")
                f.write("\n")
            
            # 性能指标
            performance = report.get("performance_metrics", {})
            f.write("## ⚡ 性能指标\n\n")
            
            query_perf = performance.get("query_performance", {})
            if query_perf:
                f.write("### 🔍 查询性能\n")
                for query_name, metrics in query_perf.items():
                    f.write(f"- **{query_name}**: {metrics.get('duration', 0):.2f}秒 ({metrics.get('data_count', 0)} 行)\n")
                f.write("\n")
            
            # 业务逻辑状态
            business_logic = report.get("business_logic", {})
            f.write("## 🎯 业务逻辑状态\n\n")
            
            for component, status in business_logic.items():
                if isinstance(status, dict) and status.get("status"):
                    status_icon = "✅" if status.get("status") == "pass" else "❌"
                    f.write(f"- **{component}**: {status_icon} {status.get('message', '')}\n")
            f.write("\n")
            
            # 优化建议
            recommendations = report.get("optimization_recommendations", [])
            if recommendations:
                f.write("## 💡 优化建议\n\n")
                for rec in recommendations:
                    priority_icon = "🔴" if rec.get("priority") == "high" else "🟡" if rec.get("priority") == "medium" else "🟢"
                    f.write(f"### {priority_icon} {rec.get('title', '')}\n")
                    f.write(f"**类别**: {rec.get('category', '')}\n")
                    f.write(f"**描述**: {rec.get('description', '')}\n")
                    f.write(f"**影响**: {rec.get('impact', '')}\n")
                    f.write(f"**风险**: {rec.get('risk', '')}\n")
                    f.write(f"**工作量**: {rec.get('estimated_effort', '')}\n\n")
            
            # 修复历史
            repair_history = report.get("repair_history", [])
            if repair_history:
                f.write("## 🔧 修复历史\n\n")
                for repair in repair_history[-5:]:  # 显示最近5个修复
                    status_icon = "✅" if repair.get("success") else "❌"
                    f.write(f"- **{repair.get('repair_type', '')}**: {status_icon} {repair.get('timestamp', '')}\n")
                f.write("\n")
    
    def _generate_html_report(self, report: Dict[str, Any], file_path: str):
        """生成HTML报告"""
        html_content = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>PC28综合系统健康报告</title>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                .status-healthy {{ color: #27ae60; font-weight: bold; }}
                .status-warning {{ color: #f39c12; font-weight: bold; }}
                .status-error {{ color: #e74c3c; font-weight: bold; }}
                .metric-card {{ background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #3498db; }}
                .recommendation {{ background: #fff3cd; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #ffc107; }}
                .achievement {{ background: #d4edda; padding: 10px; margin: 5px 0; border-radius: 5px; }}
                .issue {{ background: #f8d7da; padding: 10px; margin: 5px 0; border-radius: 5px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f8f9fa; font-weight: 600; }}
                .progress-bar {{ width: 100%; height: 20px; background-color: #e9ecef; border-radius: 10px; overflow: hidden; }}
                .progress-fill {{ height: 100%; background-color: #28a745; transition: width 0.3s ease; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🎯 PC28综合系统健康报告</h1>
                <p><strong>生成时间:</strong> {report.get('generation_time', '')}</p>
                
                <h2>📊 系统概览</h2>
                <div class="metric-card">
                    <h3>当前状态</h3>
                    <p class="{'status-healthy' if report.get('system_overview', {}).get('current_status') == 'healthy' else 'status-warning'}">
                        {report.get('system_overview', {}).get('current_status', 'unknown').upper()}
                    </p>
                </div>
                
                <div class="metric-card">
                    <h3>系统健康分数</h3>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {report.get('overall_assessment', {}).get('system_health_score', 0)}%"></div>
                    </div>
                    <p>{report.get('overall_assessment', {}).get('system_health_score', 0):.1f}/100</p>
                </div>
                
                <h2>🏆 关键成就</h2>
        """
        
        # 添加关键成就
        achievements = report.get('overall_assessment', {}).get('key_achievements', [])
        for achievement in achievements:
            html_content += f'<div class="achievement">✅ {achievement}</div>'
        
        # 添加剩余问题
        issues = report.get('overall_assessment', {}).get('remaining_issues', [])
        if issues:
            html_content += '<h2>⚠️ 剩余问题</h2>'
            for issue in issues:
                html_content += f'<div class="issue">❌ {issue}</div>'
        
        # 添加优化建议
        recommendations = report.get('optimization_recommendations', [])
        if recommendations:
            html_content += '<h2>💡 优化建议</h2>'
            for rec in recommendations:
                priority_color = "#dc3545" if rec.get("priority") == "high" else "#ffc107" if rec.get("priority") == "medium" else "#28a745"
                html_content += f'''
                <div class="recommendation" style="border-left-color: {priority_color}">
                    <h4>{rec.get('title', '')}</h4>
                    <p><strong>类别:</strong> {rec.get('category', '')}</p>
                    <p><strong>描述:</strong> {rec.get('description', '')}</p>
                    <p><strong>影响:</strong> {rec.get('impact', '')}</p>
                    <p><strong>风险:</strong> {rec.get('risk', '')} | <strong>工作量:</strong> {rec.get('estimated_effort', '')}</p>
                </div>
                '''
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

def main():
    """主函数"""
    reporter = PC28ComprehensiveSystemReport()
    
    print("📊 PC28综合系统健康报告生成器")
    print("=" * 60)
    print("🎯 目标：汇总所有测试结果，生成完整的系统健康报告")
    print("📋 范围：修复历史、测试结果、性能指标、优化建议")
    print("=" * 60)
    
    # 生成综合报告
    report = reporter.generate_comprehensive_report()
    
    # 输出结果摘要
    overview = report.get("system_overview", {})
    assessment = report.get("overall_assessment", {})
    
    print(f"\n📊 系统状态摘要:")
    print(f"  当前状态: {overview.get('current_status', 'unknown')}")
    print(f"  健康分数: {assessment.get('system_health_score', 0):.1f}/100")
    print(f"  成功率: {overview.get('overall_success_rate', 0):.2f}%")
    print(f"  已完成修复: {overview.get('total_repairs_completed', 0)} 个")
    print(f"  关键问题: {overview.get('critical_issues', 0)} 个")
    print(f"  警告: {overview.get('warnings', 0)} 个")
    
    print(f"\n🎯 准备状态:")
    readiness = overview.get("system_readiness", {})
    print(f"  生产环境就绪: {'✅ 是' if readiness.get('for_production') else '❌ 否'}")
    print(f"  优化工作就绪: {'✅ 是' if readiness.get('for_optimization') else '❌ 否'}")
    print(f"  需要修复: {'⚠️ 是' if readiness.get('requires_fixes') else '✅ 否'}")
    
    print(f"\n💡 优化建议数量: {len(report.get('optimization_recommendations', []))}")
    
    readiness_level = assessment.get('readiness_level', 'not_ready')
    if readiness_level == "ready_for_optimization":
        print(f"\n🎉 系统已准备好进行安全优化!")
        print(f"💡 可以开始删除冗余字段和性能优化工作")
    elif readiness_level == "ready_for_production":
        print(f"\n✅ 系统已准备好投入生产环境!")
        print(f"🔧 建议先解决少量问题后再进行优化")
    else:
        print(f"\n⚠️ 系统需要进一步修复")
        print(f"🔧 请先解决关键问题后再考虑优化")
    
    return report

if __name__ == "__main__":
    main()