#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化测试高级性能优化器
"""

import subprocess
import sys
import os

def run_optimizer_test():
    """运行优化器测试"""
    print("🚀 开始测试高级性能优化器...")
    
    # 确保在正确的目录
    os.chdir('/Users/a606/Documents/9999/deploy_package')
    
    try:
        # 导入主优化器模块
        from main_optimizer import AdvancedPerformanceOptimizer
        
        # 创建优化器实例
        optimizer = AdvancedPerformanceOptimizer()
        
        print("\n📋 开始分析 component_updater.py...")
        
        # 分析单个文件
        file_path = "component_updater.py"
        if not os.path.exists(file_path):
            print(f"❌ 文件 {file_path} 不存在")
            return
        
        # 执行分析
        result = optimizer.analyze_component(file_path)
        
        print(f"\n✅ 分析完成！")
        print(f"📊 发现 {len(result.get('suggestions', []))} 个优化建议")
        
        # 显示优化建议
        suggestions = result.get('suggestions', [])
        high_risk_count = 0
        
        for i, suggestion in enumerate(suggestions, 1):
            print(f"\n🔍 建议 {i}:")
            print(f"   类型: {suggestion.get('optimization_type', 'N/A')}")
            print(f"   描述: {suggestion.get('description', 'N/A')}")
            print(f"   预期收益: {suggestion.get('expected_benefit', 'N/A')}")
            
            # 检查风险评估
            risk_assessment = suggestion.get('risk_assessment')
            if risk_assessment:
                risk_level = risk_assessment.get('risk_level', 'unknown')
                print(f"   风险级别: {risk_level}")
                
                if risk_level == 'high':
                    high_risk_count += 1
                    print(f"   ⚠️  高风险任务 - 需要手动确认")
                    print(f"   影响分析: {risk_assessment.get('impact_analysis', 'N/A')}")
                    print(f"   缓解策略: {risk_assessment.get('mitigation_strategies', 'N/A')}")
        
        print(f"\n📈 统计信息:")
        print(f"   总建议数: {len(suggestions)}")
        print(f"   高风险建议数: {high_risk_count}")
        
        if high_risk_count > 0:
            print(f"\n⚠️  发现 {high_risk_count} 个高风险优化建议")
            print("   这些建议需要手动确认才能应用")
            print("   建议使用选项8 '处理高风险建议' 来逐一审核")
        
        print("\n🎉 测试完成！")
        
    except Exception as e:
        print(f"❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_optimizer_test()