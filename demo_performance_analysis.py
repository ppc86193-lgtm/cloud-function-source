#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
演示性能分析和高风险任务确认流程
"""

import sys
import os
from main_optimizer import AdvancedPerformanceOptimizer
from optimization_engine import RiskLevel

def main():
    """演示性能分析流程"""
    print("\n" + "="*60)
    print("🚀 性能分析演示 - component_updater.py")
    print("="*60)
    
    # 初始化优化器
    optimizer = AdvancedPerformanceOptimizer()
    
    # 目标文件
    target_file = "component_updater.py"
    
    if not os.path.exists(target_file):
        print(f"❌ 错误: 文件 {target_file} 不存在")
        return
    
    print(f"\n📁 分析目标文件: {target_file}")
    print("-" * 40)
    
    try:
        # 1. 执行性能分析
        print("\n🔍 步骤1: 执行性能分析...")
        report = optimizer.analyze_component(target_file)
        
        if report:
            print(f"✅ 分析完成!")
            print(f"📊 复杂度指标:")
            print(f"   - 圈复杂度: {report.complexity_metrics.cyclomatic_complexity}")
            print(f"   - 认知复杂度: {report.complexity_metrics.cognitive_complexity}")
            print(f"   - 嵌套深度: {report.complexity_metrics.nesting_depth}")
            print(f"   - 函数数量: {report.complexity_metrics.function_count}")
            print(f"   - 类数量: {report.complexity_metrics.class_count}")
            print(f"   - 代码行数: {report.complexity_metrics.line_count}")
            
            if report.performance_profile:
                print(f"\n⚡ 性能指标:")
                print(f"   - 执行时间: {report.performance_profile.execution_time:.4f}s")
                print(f"   - 内存峰值: {report.performance_profile.memory_peak:.2f}MB")
                print(f"   - CPU使用率: {report.performance_profile.cpu_usage:.2f}%")
            
            # 2. 展示优化建议
            print(f"\n💡 步骤2: 优化建议分析...")
            if report.optimization_suggestions:
                print(f"✅ 发现 {len(report.optimization_suggestions)} 个优化机会:")
                
                high_risk_suggestions = []
                for i, suggestion in enumerate(report.optimization_suggestions, 1):
                    risk_level = "未知"
                    if i <= len(report.risk_assessments):
                        risk_assessment = report.risk_assessments[i-1]
                        risk_level = risk_assessment.risk_level
                        if risk_level in ['high', 'critical']:
                            high_risk_suggestions.append((suggestion, risk_assessment))
                    
                    print(f"\n   {i}. {suggestion.type}")
                    print(f"      描述: {suggestion.description}")
                    print(f"      位置: {suggestion.code_location}")
                    print(f"      预期改进: {suggestion.estimated_improvement:.2f}")
                    print(f"      风险级别: {risk_level}")
                
                # 3. 演示高风险任务确认流程
                if high_risk_suggestions:
                    print(f"\n⚠️  步骤3: 高风险任务手动确认流程")
                    print("-" * 40)
                    print(f"🔴 发现 {len(high_risk_suggestions)} 个高风险优化建议")
                    
                    for i, (suggestion, risk) in enumerate(high_risk_suggestions, 1):
                        print(f"\n🚨 高风险建议 #{i}:")
                        print(f"   类型: {suggestion.type}")
                        print(f"   描述: {suggestion.description}")
                        print(f"   风险分数: {risk.get_risk_score():.2f}")
                        print(f"   风险因素: {', '.join(risk.risk_factors)}")
                        
                        # 模拟手动确认
                        print(f"\n❓ 是否应用此高风险优化? (y/n): ", end="")
                        
                        # 自动回答演示
                        response = "n"  # 演示中选择不应用
                        print(response)
                        
                        if response.lower() == 'y':
                            print(f"✅ 用户确认应用高风险优化")
                            print(f"🔧 正在应用优化...")
                            print(f"✅ 优化已应用")
                        else:
                            print(f"❌ 用户拒绝应用高风险优化")
                            print(f"📝 建议已记录但未应用")
                else:
                    print(f"\n✅ 步骤3: 无高风险优化建议，可安全应用所有建议")
                
                # 4. 生成性能优化报告
                print(f"\n📋 步骤4: 生成性能优化报告...")
                
                # 生成项目报告
                project_report = optimizer.generate_performance_report()
                
                print(f"✅ 报告生成完成!")
                print(f"\n📊 项目整体统计:")
                print(f"   - 分析文件数: {len(project_report.get('components', []))}")
                print(f"   - 总优化建议: {project_report.get('total_suggestions', 0)}")
                print(f"   - 高风险建议: {project_report.get('high_risk_suggestions', 0)}")
                
                # 5. 展示优化前后对比
                print(f"\n📈 步骤5: 优化前后对比")
                print("-" * 40)
                print(f"优化前:")
                print(f"   - 复杂度分数: {report.complexity_metrics.cyclomatic_complexity + report.complexity_metrics.cognitive_complexity}")
                print(f"   - 潜在问题: {len(report.optimization_suggestions)}")
                print(f"   - 风险评估: {len([r for r in report.risk_assessments if r.risk_level in ['high', 'critical']])} 个高风险项")
                
                print(f"\n优化后 (模拟):")
                print(f"   - 预期复杂度降低: 15-25%")
                print(f"   - 预期性能提升: 10-20%")
                print(f"   - 代码可维护性: 显著提升")
                
            else:
                print(f"✅ 未发现需要优化的问题")
        else:
            print(f"❌ 分析失败")
            
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print(f"\n" + "="*60)
    print("🎉 性能分析演示完成!")
    print("="*60)

if __name__ == "__main__":
    main()