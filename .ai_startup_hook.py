#!/usr/bin/env python3
"""
AI启动钩子 - 强制执行项目标准
所有AI必须在开始工作前执行此脚本
"""

import os
import sys
import json
import subprocess
from pathlib import Path

def on_session_start():
    """会话开始时自动执行"""
    memory = AIMemorySystem()
    
    print("\n" + "="*50)
    print("AI助手规则自动加载系统 v1.0")
    print("="*50)
    
    # 显示关键规则
    critical_rules = memory.get_critical_rules()
    if critical_rules:
        print("\n📋 关键规则提醒:")
        for rule in critical_rules[:5]:
            print(f"  🔴 {rule.rule_id}: {rule.content[:80]}...")
    
    # 显示记忆中的重要提醒
    if 'important_reminders' in memory.memory:
        print("\n💡 重要提醒:")
        for reminder in memory.memory['important_reminders']:
            print(f"  • {reminder}")
    
    # 显示最近的违规记录
    if 'common_violations' in memory.memory and memory.memory['common_violations']:
        recent_violations = memory.memory['common_violations'][-3:]
        if recent_violations:
            print("\n⚠️  最近违规记录:")
            for violation in recent_violations:
                print(f"  - {violation['rule_id']}: {violation['description']}")
    
    # 记录会话开始
    memory.memory['last_session'] = datetime.now().isoformat()
    memory.memory['session_count'] = memory.memory.get('session_count', 0) + 1
    memory.save_memory()
    
    print("\n✅ 规则已自动加载，共{}条规则".format(len(memory.rules)))
    print("📖 规则文件: {}".format(memory.RULES_FILE))
    print("💾 记忆文件: {}".format(memory.MEMORY_FILE))
    print("="*50 + "\n")
    
    return memory

def validate_action(action: str, context: dict = None):
    """验证操作是否符合规则"""
    memory = AIMemorySystem()
    if context is None:
        context = {}
    
    compliant = memory.check_rule_compliance(action, context)
    
    if not compliant:
        print("\n⚠️  警告: 操作可能违反项目规则")
        print(f"操作: {action}")
        print(f"上下文: {context}")
        
        # 提供修正建议
        if 'log' in action.lower():
            print("\n建议: 使用logging模块而不是手写日志")
            print("示例: logger.info('Your message here')")
        
        if 'test' in action.lower():
            print("\n建议: 使用pytest执行测试")
            print("示例: pytest test_file.py -v --cov")
    
    return compliant

def get_rule_by_id(rule_id: str) -> dict:
    """根据ID获取规则详情"""
    memory = AIMemorySystem()
    rule = memory.rules.get(rule_id)
    
    if rule:
        return {
            'id': rule.rule_id,
            'category': rule.category,
            'content': rule.content,
            'severity': rule.severity
        }
    return None

def quick_rule_check() -> str:
    """快速规则检查，返回摘要"""
    memory = AIMemorySystem()
    return memory.get_rule_summary()

if __name__ == "__main__":
    # 测试启动钩子
    memory_system = on_session_start()
    
    # 测试规则检查
    print("\n测试规则合规性检查:")
    validate_action("手动添加日志", {"file_path": "test.py"})
    validate_action("运行pytest测试", {"file_path": "test_module.py"})
    
    # 显示规则摘要
    print(quick_rule_check())