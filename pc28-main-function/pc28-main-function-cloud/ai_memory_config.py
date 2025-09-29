#!/usr/bin/env python3
"""AI助手记忆和规则遵守配置系统

该系统确保AI助手在每次会话时都能:
1. 自动加载并解析PROJECT_RULES.md
2. 验证是否遵守所有规则
3. 提供规则快速查询接口
4. 记录违规行为并自动修正
"""

import os
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import re

# 配置自动化日志系统（遵守规则：禁止手写日志）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ai_memory_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ProjectRule:
    """项目规则数据结构"""
    category: str
    rule_id: str
    content: str
    severity: str  # critical, high, medium, low
    auto_check: bool = True
    validation_method: Optional[str] = None

@dataclass
class RuleViolation:
    """规则违反记录"""
    timestamp: datetime
    rule_id: str
    description: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    auto_fixed: bool = False
    fix_description: Optional[str] = None

class AIMemorySystem:
    """AI助手记忆系统 - 确保规则遵守"""
    
    RULES_FILE = "/Users/a606/cloud_function_source/PROJECT_RULES.md"
    MEMORY_FILE = "/Users/a606/cloud_function_source/.ai_memory.json"
    RULES_CACHE_FILE = "/Users/a606/cloud_function_source/.rules_cache.json"
    
    def __init__(self):
        self.rules: Dict[str, ProjectRule] = {}
        self.violations: List[RuleViolation] = []
        self.memory: Dict[str, Any] = {}
        self.rules_hash: Optional[str] = None
        self._load_rules()
        self._load_memory()
        
    def _load_rules(self) -> None:
        """自动加载并解析PROJECT_RULES.md"""
        try:
            with open(self.RULES_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # 计算文件哈希，检测规则是否更新
            new_hash = hashlib.sha256(content.encode()).hexdigest()
            
            # 如果规则文件没有变化且缓存存在，使用缓存
            if os.path.exists(self.RULES_CACHE_FILE):
                with open(self.RULES_CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    if cache.get('hash') == new_hash:
                        self.rules = {k: ProjectRule(**v) for k, v in cache['rules'].items()}
                        self.rules_hash = new_hash
                        logger.info(f"从缓存加载了{len(self.rules)}条规则")
                        return
            
            # 解析规则
            self._parse_rules(content)
            self.rules_hash = new_hash
            
            # 保存缓存
            self._save_rules_cache()
            
            logger.info(f"成功加载{len(self.rules)}条项目规则")
            
        except Exception as e:
            logger.error(f"加载规则失败: {e}")
            raise
    
    def _parse_rules(self, content: str) -> None:
        """解析规则内容"""
        # 关键规则提取
        critical_rules = [
            {
                'id': 'no_manual_logs',
                'pattern': r'禁止手写日志.*所有日志必须通过自动化系统生成',
                'category': '日志与证据要求',
                'severity': 'critical'
            },
            {
                'id': 'test_coverage_min',
                'pattern': r'代码测试覆盖率不得低于80%',
                'category': '代码质量要求',
                'severity': 'high'
            },
            {
                'id': 'auto_testing',
                'pattern': r'所有测试必须通过pytest等自动化工具执行',
                'category': '测试执行要求',
                'severity': 'critical'
            },
            {
                'id': 'git_required',
                'pattern': r'必须使用Git进行版本控制',
                'category': '开发流程要求',
                'severity': 'critical'
            },
            {
                'id': 'code_review',
                'pattern': r'所有代码变更必须经过审查',
                'category': '代码审查',
                'severity': 'high'
            },
            {
                'id': 'service_guarantee',
                'pattern': r'服务提供方无条件保证所有条款的严格执行',
                'category': '智能合约条款',
                'severity': 'critical'
            }
        ]
        
        for rule_def in critical_rules:
            match = re.search(rule_def['pattern'], content)
            if match:
                rule = ProjectRule(
                    category=rule_def['category'],
                    rule_id=rule_def['id'],
                    content=match.group(0),
                    severity=rule_def['severity']
                )
                self.rules[rule_def['id']] = rule
    
    def _save_rules_cache(self) -> None:
        """保存规则缓存"""
        try:
            cache = {
                'hash': self.rules_hash,
                'timestamp': datetime.now().isoformat(),
                'rules': {k: {
                    'category': v.category,
                    'rule_id': v.rule_id,
                    'content': v.content,
                    'severity': v.severity,
                    'auto_check': v.auto_check,
                    'validation_method': v.validation_method
                } for k, v in self.rules.items()}
            }
            with open(self.RULES_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存规则缓存失败: {e}")
    
    def _load_memory(self) -> None:
        """加载AI记忆"""
        if os.path.exists(self.MEMORY_FILE):
            try:
                with open(self.MEMORY_FILE, 'r', encoding='utf-8') as f:
                    self.memory = json.load(f)
                logger.info(f"加载了{len(self.memory)}条记忆")
            except Exception as e:
                logger.error(f"加载记忆失败: {e}")
                self.memory = {}
        else:
            self.memory = {
                'session_count': 0,
                'last_session': None,
                'common_violations': [],
                'important_reminders': [
                    "必须遵守PROJECT_RULES.md中的所有规则",
                    "禁止手写日志，所有日志必须通过自动化系统生成",
                    "所有测试必须通过pytest执行",
                    "代码覆盖率不得低于80%"
                ]
            }
    
    def save_memory(self) -> None:
        """保存AI记忆"""
        self.memory['last_updated'] = datetime.now().isoformat()
        self.memory['session_count'] = self.memory.get('session_count', 0) + 1
        
        try:
            with open(self.MEMORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.memory, f, ensure_ascii=False, indent=2)
            logger.info("记忆已保存")
        except Exception as e:
            logger.error(f"保存记忆失败: {e}")
    
    def check_rule_compliance(self, action: str, context: Dict[str, Any]) -> bool:
        """检查操作是否符合规则"""
        compliant = True
        
        # 检查是否违反禁止手写日志规则
        if 'log' in action.lower() and 'manual' in str(context).lower():
            self.record_violation(
                rule_id='no_manual_logs',
                description='检测到可能的手写日志操作',
                file_path=context.get('file_path')
            )
            compliant = False
        
        # 检查测试是否通过pytest执行
        if 'test' in action.lower() and 'pytest' not in str(context).lower():
            self.record_violation(
                rule_id='auto_testing',
                description='测试未通过pytest执行',
                file_path=context.get('file_path')
            )
            compliant = False
        
        return compliant
    
    def record_violation(self, rule_id: str, description: str, 
                        file_path: Optional[str] = None,
                        line_number: Optional[int] = None) -> None:
        """记录规则违反"""
        violation = RuleViolation(
            timestamp=datetime.now(),
            rule_id=rule_id,
            description=description,
            file_path=file_path,
            line_number=line_number
        )
        self.violations.append(violation)
        
        # 自动记录到日志
        logger.warning(f"规则违反: {rule_id} - {description}")
        
        # 更新记忆中的常见违规
        if 'common_violations' not in self.memory:
            self.memory['common_violations'] = []
        
        self.memory['common_violations'].append({
            'timestamp': violation.timestamp.isoformat(),
            'rule_id': rule_id,
            'description': description
        })
        
        # 只保留最近100条违规记录
        self.memory['common_violations'] = self.memory['common_violations'][-100:]
    
    def get_rule_summary(self) -> str:
        """获取规则摘要"""
        summary = "\n=== 项目规则摘要 ===\n"
        
        # 按类别分组
        categories = {}
        for rule in self.rules.values():
            if rule.category not in categories:
                categories[rule.category] = []
            categories[rule.category].append(rule)
        
        for category, rules in categories.items():
            summary += f"\n【{category}】\n"
            for rule in rules:
                severity_marker = "🔴" if rule.severity == 'critical' else "🟡" if rule.severity == 'high' else "🟢"
                summary += f"  {severity_marker} {rule.rule_id}: {rule.content[:50]}...\n"
        
        # 添加最近违规
        if self.violations:
            summary += "\n=== 最近违规记录 ===\n"
            for v in self.violations[-5:]:
                summary += f"  - {v.timestamp.strftime('%Y-%m-%d %H:%M')}: {v.rule_id} - {v.description}\n"
        
        return summary
    
    def get_critical_rules(self) -> List[ProjectRule]:
        """获取关键规则列表"""
        return [rule for rule in self.rules.values() if rule.severity == 'critical']
    
    def validate_file(self, file_path: str) -> List[str]:
        """验证文件是否符合规则"""
        issues = []
        
        if not os.path.exists(file_path):
            return [f"文件不存在: {file_path}"]
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
        
        # 检查手写日志
        for i, line in enumerate(lines, 1):
            if 'print(' in line and not line.strip().startswith('#'):
                issues.append(f"第{i}行: 可能存在手写日志（使用了print）")
            
            if 'logger.' not in line and ('log' in line.lower() or 'Log' in line):
                if not line.strip().startswith('#') and not line.strip().startswith('//'):
                    issues.append(f"第{i}行: 可能存在非标准日志")
        
        return issues

def create_ai_startup_hook():
    """创建AI启动钩子，确保每次会话都加载规则"""
    return AIMemorySystem()

if __name__ == "__main__":
    # 测试系统
    memory = AIMemorySystem()
    print(memory.get_rule_summary())
    print(f"\n✅ AI记忆系统已初始化")
    print(f"📋 已加载{len(memory.rules)}条规则")
    print(f"📂 规则文件: {memory.RULES_FILE}")