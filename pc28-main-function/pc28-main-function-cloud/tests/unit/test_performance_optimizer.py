#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能优化器测试脚本
演示高风险任务的手动确认流程
"""

from advanced_performance_optimizer import AdvancedPerformanceOptimizer
import os

def main():
    print("=== Python性能优化系统演示 ===")
    print("正在分析 component_updater.py...")
    
    # 创建优化器实例
    optimizer = AdvancedPerformanceOptimizer()
    
    # 分析组件
    file_path = 'component_updater.py'
    if not os.path.exists(file_path):
        print(f"错误: 文件 {file_path} 不存在")
        return
    
    try:
        # 1. 执行复杂度分析
        print("\n1. 执行复杂度分析...")
        complexity, performance = optimizer.analyze_component(file_path)
        
        print(f"\n复杂度分析结果:")
        print(f"  圈复杂度: {complexity.cyclomatic_complexity}")
        print(f"  认知复杂度: {complexity.cognitive_complexity}")
        print(f"  最大嵌套深度: {complexity.nesting_depth}")
        print(f"  函数数量: {complexity.function_count}")
        print(f"  类数量: {complexity.class_count}")
        print(f"  代码行数: {complexity.line_count}")
        print(f"  注释比例: {complexity.comment_ratio:.2%}")
        print(f"  重复代码行: {complexity.duplicate_lines}")
        
        # 2. 生成优化建议
        print("\n2. 生成优化建议...")
        suggestions = optimizer.generate_optimization_suggestions(file_path)
        
        print(f"\n发现 {len(suggestions)} 个优化建议:")
        
        high_risk_count = 0
        auto_applicable_count = 0
        
        for i, suggestion in enumerate(suggestions):
            print(f"\n{i+1}. {suggestion.category} - {suggestion.priority}优先级")
            print(f"   描述: {suggestion.description}")
            print(f"   风险级别: {suggestion.risk_level}")
            print(f"   预期改进: {suggestion.estimated_improvement:.1%}")
            
            if suggestion.auto_applicable:
                print(f"   状态: 可自动应用")
                auto_applicable_count += 1
            else:
                print(f"   状态: 需要手动确认 (高风险任务)")
                high_risk_count += 1
            
            if suggestion.reasoning:
                print(f"   理由: {suggestion.reasoning}")
        
        # 3. 风险评估总结
        print(f"\n3. 风险评估总结:")
        print(f"   总建议数: {len(suggestions)}")
        print(f"   可自动应用: {auto_applicable_count}")
        print(f"   需手动确认: {high_risk_count}")
        
        # 4. 演示手动确认流程
        if high_risk_count > 0:
            print(f"\n4. 高风险任务手动确认演示:")
            print(f"   检测到 {high_risk_count} 个高风险优化任务")
            print(f"   这些任务需要人工审查和确认才能应用")
            print(f"   系统已自动标记为'需要手动确认'状态")
            print(f"   建议在应用前进行代码备份")
        else:
            print(f"\n4. 未发现高风险任务，所有优化都可以安全自动应用")
        
        print(f"\n=== 性能优化分析完成 ===")
        
    except Exception as e:
        print(f"分析过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()