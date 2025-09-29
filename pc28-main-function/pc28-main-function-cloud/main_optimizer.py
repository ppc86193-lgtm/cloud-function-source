#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主控制器模块
协调各个模块的工作，提供统一的接口和交互式界面
"""

import os
import sys
import logging
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from pathlib import Path

# 导入各个模块
from models import (
    ComplexityMetrics, PerformanceProfile, OptimizationSuggestion,
    RiskAssessment, OptimizationResult, ComponentAnalysisReport
)
from code_analyzer import CodeMetricsCollector
from performance_profiler import PerformanceProfiler
from optimization_engine import OptimizationEngine, RiskLevel
from database_manager import DatabaseManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('performance_optimizer.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class AdvancedPerformanceOptimizer:
    """高级性能优化器主控制器"""
    
    def __init__(self, db_path: str = "performance_optimizer.db"):
        """初始化优化器"""
        self.db_manager = DatabaseManager(db_path)
        self.code_analyzer = CodeMetricsCollector()
        self.performance_profiler = PerformanceProfiler()
        self.optimization_engine = OptimizationEngine()
        
        logger.info("高级性能优化器初始化完成")
    
    def analyze_component(self, file_path: str) -> ComponentAnalysisReport:
        """分析单个组件"""
        logger.info(f"开始分析组件: {file_path}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        component_id = os.path.basename(file_path)
        
        try:
            # 1. 代码复杂度分析
            logger.info("执行代码复杂度分析...")
            # 收集代码复杂度指标
            file_metrics = self.code_analyzer.collect_file_metrics(file_path)
            complexity_metrics = file_metrics.get('complexity_metrics')
            if complexity_metrics:
                complexity_metrics.component_id = component_id
            
            # 2. 性能分析
            logger.info("执行性能分析...")
            performance_profile = self.performance_profiler.analyze_file_performance(file_path)
            
            # 3. 生成优化建议
            logger.info("生成优化建议...")
            suggestions = self.optimization_engine.generate_suggestions(file_path)
            
            # 4. 风险评估
            logger.info("执行风险评估...")
            risk_assessments = self.optimization_engine.assess_risks(suggestions)
            
            # 5. 保存分析结果
            self._save_analysis_results(
                component_id, complexity_metrics, performance_profile,
                suggestions, risk_assessments
            )
            
            # 6. 生成分析报告
            report = ComponentAnalysisReport(
                component_id=component_id,
                complexity_metrics=complexity_metrics,
                performance_profile=performance_profile,
                optimization_suggestions=suggestions,
                risk_assessments=risk_assessments,
                analysis_timestamp=datetime.now()
            )
            
            logger.info(f"组件分析完成: {file_path}")
            return report
            
        except Exception as e:
            logger.error(f"组件分析失败 {file_path}: {e}")
            raise
    
    def analyze_project(self, project_path: str, file_patterns: List[str] = None) -> List[ComponentAnalysisReport]:
        """分析整个项目"""
        if file_patterns is None:
            file_patterns = ['*.py']
        
        logger.info(f"开始分析项目: {project_path}")
        
        reports = []
        project_path = Path(project_path)
        
        # 查找所有匹配的文件
        files_to_analyze = []
        for pattern in file_patterns:
            files_to_analyze.extend(project_path.rglob(pattern))
        
        logger.info(f"找到 {len(files_to_analyze)} 个文件需要分析")
        
        for file_path in files_to_analyze:
            try:
                if file_path.is_file():
                    report = self.analyze_component(str(file_path))
                    reports.append(report)
            except Exception as e:
                logger.error(f"分析文件失败 {file_path}: {e}")
                continue
        
        logger.info(f"项目分析完成，共分析了 {len(reports)} 个文件")
        return reports
    
    def _save_analysis_results(self, component_id: str, complexity_metrics: ComplexityMetrics,
                             performance_profile: PerformanceProfile,
                             suggestions: List[OptimizationSuggestion],
                             risk_assessments: List[RiskAssessment]):
        """保存分析结果到数据库"""
        try:
            # 保存复杂度指标
            self.db_manager.save_complexity_metrics(complexity_metrics)
            
            # 保存性能分析结果
            self.db_manager.save_performance_profile(performance_profile)
            
            # 保存优化建议和风险评估
            for suggestion, risk in zip(suggestions, risk_assessments):
                self.db_manager.save_optimization_suggestion(suggestion)
                self.db_manager.save_risk_assessment(risk)
            
            logger.info(f"分析结果已保存: {component_id}")
            
        except Exception as e:
            logger.error(f"保存分析结果失败 {component_id}: {e}")
    
    def get_high_risk_suggestions(self) -> List[Tuple[OptimizationSuggestion, RiskAssessment]]:
        """获取需要手动确认的高风险建议"""
        return self.db_manager.get_high_risk_suggestions()
    
    def apply_optimization(self, suggestion_id: str, confirmed: bool = False) -> OptimizationResult:
        """应用优化建议"""
        logger.info(f"开始应用优化建议: {suggestion_id}")
        
        # 获取建议和风险评估
        suggestions = self.db_manager.get_optimization_suggestions()
        suggestion = next((s for s in suggestions if s.suggestion_id == suggestion_id), None)
        
        if not suggestion:
            raise ValueError(f"未找到优化建议: {suggestion_id}")
        
        risk_assessments = self.db_manager.get_risk_assessments([suggestion_id])
        risk_assessment = risk_assessments[0] if risk_assessments else None
        
        # 检查是否需要手动确认
        if risk_assessment and risk_assessment.requires_manual_review and not confirmed:
            logger.warning(f"优化建议 {suggestion_id} 需要手动确认")
            return OptimizationResult(
                suggestion_id=suggestion_id,
                success=False,
                applied_changes="",
                performance_improvement=0.0,
                backup_location="",
                error_message="需要手动确认的高风险操作"
            )
        
        try:
            # 应用优化
            result = self.optimization_engine.apply_optimization(suggestion)
            
            # 保存结果
            self.db_manager.save_optimization_result(result)
            
            logger.info(f"优化应用完成: {suggestion_id}")
            return result
            
        except Exception as e:
            logger.error(f"应用优化失败 {suggestion_id}: {e}")
            result = OptimizationResult(
                suggestion_id=suggestion_id,
                success=False,
                applied_changes="",
                performance_improvement=0.0,
                backup_location="",
                error_message=str(e)
            )
            self.db_manager.save_optimization_result(result)
            return result
    
    def generate_performance_report(self, component_id: Optional[str] = None) -> Dict[str, Any]:
        """生成性能报告"""
        logger.info("生成性能报告...")
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'statistics': self.db_manager.get_statistics(),
            'components': []
        }
        
        # 获取组件信息
        suggestions = self.db_manager.get_optimization_suggestions(component_id)
        
        component_ids = set(s.component_id for s in suggestions)
        
        for comp_id in component_ids:
            complexity = self.db_manager.get_complexity_metrics(comp_id)
            performance = self.db_manager.get_performance_profile(comp_id)
            comp_suggestions = [s for s in suggestions if s.component_id == comp_id]
            
            component_report = {
                'component_id': comp_id,
                'complexity_metrics': complexity.__dict__ if complexity else None,
                'performance_profile': performance.__dict__ if performance else None,
                'optimization_suggestions': len(comp_suggestions),
                'high_risk_suggestions': len([s for s in comp_suggestions 
                                            if any(r.requires_manual_review 
                                                 for r in self.db_manager.get_risk_assessments([s.suggestion_id]))])
            }
            
            report['components'].append(component_report)
        
        # 保存报告
        if component_id:
            self.db_manager.save_analysis_report(component_id, 'performance_report', report)
        else:
            self.db_manager.save_analysis_report('project', 'performance_report', report)
        
        return report
    
    def interactive_optimization_session(self):
        """交互式优化会话"""
        print("\n" + "="*60)
        print("🚀 高级性能优化器 - 交互式会话")
        print("="*60)
        
        while True:
            print("\n📋 可用操作:")
            print("1. 分析单个文件")
            print("2. 分析整个项目")
            print("3. 查看优化建议")
            print("4. 应用优化")
            print("5. 性能监控")
            print("6. 生成报告")
            print("7. 查看统计信息")
            print("8. 处理高风险建议")
            print("0. 退出")
            
            try:
                choice = input("\n请选择操作 (0-8): ").strip()
                
                if choice == '0':
                    print("\n👋 感谢使用高级性能优化器！")
                    break
                elif choice == '1':
                    self._handle_analyze_file()
                elif choice == '2':
                    self._handle_analyze_project()
                elif choice == '3':
                    self._handle_view_suggestions()
                elif choice == '4':
                    self._handle_apply_optimization()
                elif choice == '5':
                    self._handle_performance_monitoring()
                elif choice == '6':
                    self._handle_generate_report()
                elif choice == '7':
                    self._handle_view_statistics()
                elif choice == '8':
                    self._handle_high_risk_suggestions()
                else:
                    print("❌ 无效选择，请重试")
                    
            except KeyboardInterrupt:
                print("\n\n👋 用户中断，退出程序")
                break
            except Exception as e:
                print(f"❌ 操作失败: {e}")
                logger.error(f"交互式会话错误: {e}")
    
    def _handle_analyze_file(self):
        """处理文件分析"""
        file_path = input("请输入文件路径: ").strip()
        if not file_path:
            print("❌ 文件路径不能为空")
            return
        
        try:
            print(f"\n🔍 正在分析文件: {file_path}")
            report = self.analyze_component(file_path)
            
            print(f"\n✅ 分析完成！")
            print(f"📊 复杂度分数: {report.complexity_metrics.complexity_score():.2f}")
            print(f"⚡ 性能效率: {report.performance_profile.efficiency_score():.2f}")
            print(f"💡 优化建议数量: {len(report.optimization_suggestions)}")
            print(f"⚠️  高风险建议: {len([r for r in report.risk_assessments if r.requires_manual_review])}")
            
        except Exception as e:
            print(f"❌ 分析失败: {e}")
    
    def _handle_analyze_project(self):
        """处理项目分析"""
        project_path = input("请输入项目路径: ").strip()
        if not project_path:
            print("❌ 项目路径不能为空")
            return
        
        try:
            print(f"\n🔍 正在分析项目: {project_path}")
            reports = self.analyze_project(project_path)
            
            print(f"\n✅ 项目分析完成！")
            print(f"📁 分析文件数量: {len(reports)}")
            
            total_suggestions = sum(len(r.optimization_suggestions) for r in reports)
            total_high_risk = sum(len([ra for ra in r.risk_assessments if ra.requires_manual_review]) for r in reports)
            
            print(f"💡 总优化建议: {total_suggestions}")
            print(f"⚠️  高风险建议: {total_high_risk}")
            
        except Exception as e:
            print(f"❌ 项目分析失败: {e}")
    
    def _handle_view_suggestions(self):
        """处理查看建议"""
        component_id = input("请输入组件ID (留空查看所有): ").strip() or None
        
        try:
            suggestions = self.db_manager.get_optimization_suggestions(component_id, limit=20)
            
            if not suggestions:
                print("📭 没有找到优化建议")
                return
            
            print(f"\n💡 找到 {len(suggestions)} 个优化建议:")
            print("-" * 80)
            
            for i, suggestion in enumerate(suggestions, 1):
                risk_assessments = self.db_manager.get_risk_assessments([suggestion.suggestion_id])
                risk = risk_assessments[0] if risk_assessments else None
                
                risk_indicator = "🔴" if risk and risk.requires_manual_review else "🟢"
                
                print(f"{i}. {risk_indicator} [{suggestion.suggestion_type}] {suggestion.description}")
                print(f"   组件: {suggestion.component_id}")
                print(f"   影响分数: {suggestion.impact_score:.2f} | 置信度: {suggestion.confidence:.2f}")
                if risk:
                    print(f"   风险等级: {risk.risk_level} | 风险分数: {risk.risk_score:.2f}")
                print()
                
        except Exception as e:
            print(f"❌ 查看建议失败: {e}")
    
    def _handle_apply_optimization(self):
        """处理应用优化"""
        suggestion_id = input("请输入建议ID: ").strip()
        if not suggestion_id:
            print("❌ 建议ID不能为空")
            return
        
        try:
            # 检查是否为高风险建议
            risk_assessments = self.db_manager.get_risk_assessments([suggestion_id])
            risk = risk_assessments[0] if risk_assessments else None
            
            confirmed = True
            if risk and risk.requires_manual_review:
                print(f"\n⚠️  这是一个高风险优化建议！")
                print(f"风险等级: {risk.risk_level}")
                print(f"风险分数: {risk.risk_score:.2f}")
                print(f"风险因素: {', '.join(risk.risk_factors)}")
                
                confirm = input("\n确认要应用此优化吗？(yes/no): ").strip().lower()
                confirmed = confirm in ['yes', 'y', '是']
                
                if not confirmed:
                    print("❌ 用户取消操作")
                    return
            
            print(f"\n🔧 正在应用优化建议: {suggestion_id}")
            result = self.apply_optimization(suggestion_id, confirmed)
            
            if result.success:
                print(f"✅ 优化应用成功！")
                if result.performance_improvement > 0:
                    print(f"📈 性能提升: {result.performance_improvement:.2f}%")
                if result.backup_location:
                    print(f"💾 备份位置: {result.backup_location}")
            else:
                print(f"❌ 优化应用失败: {result.error_message}")
                
        except Exception as e:
            print(f"❌ 应用优化失败: {e}")
    
    def _handle_performance_monitoring(self):
        """处理性能监控"""
        print("\n📊 性能监控功能")
        print("1. 实时监控")
        print("2. 查看历史数据")
        
        choice = input("请选择 (1-2): ").strip()
        
        if choice == '1':
            duration = input("监控时长(秒，默认10): ").strip()
            try:
                duration = int(duration) if duration else 10
                print(f"\n🔍 开始 {duration} 秒性能监控...")
                
                with self.performance_profiler.monitor_performance() as monitor:
                    import time
                    time.sleep(duration)
                
                print("✅ 监控完成")
                
            except ValueError:
                print("❌ 无效的时长")
        
        elif choice == '2':
            component_id = input("请输入组件ID: ").strip()
            if component_id:
                profile = self.db_manager.get_performance_profile(component_id)
                if profile:
                    print(f"\n📊 {component_id} 性能数据:")
                    print(f"执行时间: {profile.execution_time:.4f}s")
                    print(f"内存峰值: {profile.memory_peak:.2f}MB")
                    print(f"CPU使用率: {profile.cpu_usage:.2f}%")
                    print(f"I/O操作: {profile.io_operations}")
                else:
                    print("❌ 未找到性能数据")
    
    def _handle_generate_report(self):
        """处理生成报告"""
        component_id = input("请输入组件ID (留空生成项目报告): ").strip() or None
        
        try:
            print("\n📋 正在生成报告...")
            report = self.generate_performance_report(component_id)
            
            # 保存报告到文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"performance_report_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"✅ 报告生成完成: {filename}")
            print(f"📊 统计信息:")
            stats = report['statistics']
            print(f"   总建议数: {stats.get('total_suggestions', 0)}")
            print(f"   需手动确认: {stats.get('manual_confirmation_required', 0)}")
            print(f"   平均影响分数: {stats.get('average_impact_score', 0)}")
            
        except Exception as e:
            print(f"❌ 生成报告失败: {e}")
    
    def _handle_view_statistics(self):
        """处理查看统计信息"""
        try:
            stats = self.db_manager.get_statistics()
            
            print("\n📊 系统统计信息:")
            print("-" * 40)
            print(f"总优化建议数: {stats.get('total_suggestions', 0)}")
            print(f"需手动确认: {stats.get('manual_confirmation_required', 0)}")
            print(f"平均影响分数: {stats.get('average_impact_score', 0)}")
            
            print("\n📈 按类型统计:")
            for suggestion_type, count in stats.get('suggestions_by_type', {}).items():
                print(f"  {suggestion_type}: {count}")
            
            print("\n⚠️  按风险等级统计:")
            for risk_level, count in stats.get('suggestions_by_risk', {}).items():
                print(f"  {risk_level}: {count}")
            
            print("\n✅ 优化成功率:")
            success_stats = stats.get('optimization_success_rate', {})
            total = success_stats.get('successful', 0) + success_stats.get('failed', 0)
            if total > 0:
                success_rate = success_stats.get('successful', 0) / total * 100
                print(f"  成功: {success_stats.get('successful', 0)} ({success_rate:.1f}%)")
                print(f"  失败: {success_stats.get('failed', 0)}")
            else:
                print("  暂无优化记录")
                
        except Exception as e:
            print(f"❌ 获取统计信息失败: {e}")
    
    def _handle_high_risk_suggestions(self):
        """处理高风险建议"""
        try:
            high_risk_suggestions = self.get_high_risk_suggestions()
            
            if not high_risk_suggestions:
                print("✅ 没有需要手动确认的高风险建议")
                return
            
            print(f"\n⚠️  找到 {len(high_risk_suggestions)} 个高风险建议需要手动确认:")
            print("=" * 80)
            
            for i, (suggestion, risk) in enumerate(high_risk_suggestions, 1):
                print(f"\n{i}. 🔴 [{suggestion.suggestion_type}] {suggestion.description}")
                print(f"   组件: {suggestion.component_id}")
                print(f"   位置: {suggestion.code_location}")
                print(f"   风险等级: {risk.risk_level} | 风险分数: {risk.risk_score:.2f}")
                print(f"   风险因素: {', '.join(risk.risk_factors)}")
                print(f"   影响分数: {suggestion.impact_score:.2f} | 置信度: {suggestion.confidence:.2f}")
                
                if suggestion.original_code:
                    print(f"   原始代码: {suggestion.original_code[:100]}...")
                if suggestion.optimized_code:
                    print(f"   优化代码: {suggestion.optimized_code[:100]}...")
                
                print(f"   缓解策略: {', '.join(risk.mitigation_strategies)}")
            
            print("\n💡 提示: 使用 '应用优化' 功能来处理这些高风险建议")
            
        except Exception as e:
            print(f"❌ 获取高风险建议失败: {e}")

def main():
    """主函数"""
    try:
        optimizer = AdvancedPerformanceOptimizer()
        
        if len(sys.argv) > 1:
            # 命令行模式
            command = sys.argv[1]
            
            if command == 'analyze' and len(sys.argv) > 2:
                file_path = sys.argv[2]
                report = optimizer.analyze_component(file_path)
                print(f"分析完成: {file_path}")
                print(f"优化建议数量: {len(report.optimization_suggestions)}")
                
            elif command == 'project' and len(sys.argv) > 2:
                project_path = sys.argv[2]
                reports = optimizer.analyze_project(project_path)
                print(f"项目分析完成，共分析 {len(reports)} 个文件")
                
            elif command == 'report':
                component_id = sys.argv[2] if len(sys.argv) > 2 else None
                report = optimizer.generate_performance_report(component_id)
                print("性能报告生成完成")
                
            else:
                print("用法:")
                print("  python main_optimizer.py analyze <file_path>")
                print("  python main_optimizer.py project <project_path>")
                print("  python main_optimizer.py report [component_id]")
                print("  python main_optimizer.py  # 交互式模式")
        else:
            # 交互式模式
            optimizer.interactive_optimization_session()
            
    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        print(f"❌ 程序执行失败: {e}")

if __name__ == "__main__":
    main()