#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优化引擎模块
提供优化建议生成、风险评估、自动优化应用等功能
"""

import ast
import re
import os
import logging
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import defaultdict
from dataclasses import asdict
from enum import Enum

from models import OptimizationSuggestion, RiskAssessment, OptimizationResult

def create_optimization_suggestion(
    suggestion_id: str,
    component_id: str,
    category: str,
    priority: str,
    description: str,
    code_location: Tuple[int, int],
    original_code: str,
    suggested_code: str,
    estimated_improvement: float,
    risk_level: str = "safe",
    auto_applicable: bool = True,
    reasoning: str = "",
    suggestion_type: str = "performance",
    impact: str = "medium",
    effort: str = "low"
) -> OptimizationSuggestion:
    """创建优化建议的辅助函数"""
    return OptimizationSuggestion(
        suggestion_id=suggestion_id,
        component_id=component_id,
        category=category,
        priority=priority,
        description=description,
        code_location=code_location,
        original_code=original_code,
        suggested_code=suggested_code,
        estimated_improvement=estimated_improvement,
        risk_level=risk_level,
        auto_applicable=auto_applicable,
        reasoning=reasoning,
        type=suggestion_type,
        impact=impact,
        effort=effort,
        code_example=suggested_code
    )

logger = logging.getLogger(__name__)

class OptimizationType(Enum):
    """优化类型枚举"""
    PERFORMANCE = "performance"
    MEMORY = "memory"
    ALGORITHM = "algorithm"
    CODE_QUALITY = "code_quality"
    SECURITY = "security"
    MAINTAINABILITY = "maintainability"

class RiskLevel(Enum):
    """风险等级枚举"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class DataStructureAnalyzer:
    """数据结构分析器"""
    
    def analyze(self, file_path: str) -> List[OptimizationSuggestion]:
        """分析数据结构使用"""
        suggestions = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                # 检查低效的数据结构使用
                if isinstance(node, ast.Call):
                    suggestions.extend(self._analyze_function_calls(node, file_path))
                elif isinstance(node, ast.ListComp):
                    suggestions.extend(self._analyze_list_comprehensions(node, file_path))
                elif isinstance(node, ast.For):
                    suggestions.extend(self._analyze_loops(node, file_path))
        
        except Exception as e:
            logger.error(f"数据结构分析失败 {file_path}: {e}")
        
        return suggestions
    
    def _analyze_function_calls(self, node: ast.Call, file_path: str) -> List[OptimizationSuggestion]:
        """分析函数调用"""
        suggestions = []
        
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            
            # 检查 list(range()) 模式
            if func_name == 'list' and len(node.args) == 1:
                arg = node.args[0]
                if isinstance(arg, ast.Call) and isinstance(arg.func, ast.Name) and arg.func.id == 'range':
                    suggestions.append(OptimizationSuggestion(
                        suggestion_id=f"perf_{len(suggestions)+1}",
                        component_id=file_path,
                        category="performance",
                        priority="high",
                        description=f"使用 {ast.unparse(arg)} 替代 {ast.unparse(node)}",
                        code_location=(node.lineno, node.lineno),
                        original_code=ast.unparse(node),
                        suggested_code=ast.unparse(arg),
                        estimated_improvement=7.0,
                        risk_level="safe",
                        auto_applicable=True,
                        reasoning="使用更高效的数据结构可以提升性能",
                        type="performance",
                        impact="medium",
                        effort="low",
                        code_example=ast.unparse(arg)
                    ))
            
            # 检查字典的 keys() 遍历
            elif func_name in ['keys', 'values', 'items']:
                suggestions.append(OptimizationSuggestion(
                    suggestion_id=f"perf_{len(suggestions)+1}",
                    component_id=file_path,
                    category="performance",
                    priority="medium",
                    description="直接遍历字典而不是调用 keys()",
                    code_location=(node.lineno, node.lineno),
                    original_code=ast.unparse(node),
                    suggested_code="# 直接遍历字典",
                    estimated_improvement=3.0,
                    risk_level="safe",
                    auto_applicable=True,
                    reasoning="直接遍历字典比使用.keys()更高效",
                    type="performance",
                    impact="low",
                    effort="low",
                    code_example="for key in dict_name:"
                ))
        
        return suggestions
    
    def _analyze_list_comprehensions(self, node: ast.ListComp, file_path: str) -> List[OptimizationSuggestion]:
        """分析列表推导式"""
        suggestions = []
        
        # 检查是否可以使用生成器表达式
        if len(node.generators) == 1:
            suggestions.append(create_optimization_suggestion(
                suggestion_id=f"mem_{len(suggestions)+1}",
                component_id=file_path,
                category="memory",
                priority="medium",
                description="考虑使用生成器表达式替代列表推导式以节省内存",
                code_location=(node.lineno, node.lineno),
                original_code=ast.unparse(node),
                suggested_code=f"({ast.unparse(node.elt)} for {ast.unparse(node.generators[0])})",
                estimated_improvement=5.0,
                reasoning="生成器表达式比列表推导式更节省内存"
            ))
        
        return suggestions
    
    def _analyze_loops(self, node: ast.For, file_path: str) -> List[OptimizationSuggestion]:
        """分析循环结构"""
        suggestions = []
        
        # 检查 range(len()) 模式
        if isinstance(node.iter, ast.Call):
            if (isinstance(node.iter.func, ast.Name) and 
                node.iter.func.id == 'range' and 
                len(node.iter.args) == 1):
                
                arg = node.iter.args[0]
                if (isinstance(arg, ast.Call) and 
                    isinstance(arg.func, ast.Name) and 
                    arg.func.id == 'len'):
                    
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"perf_{len(suggestions)+1}",
                        component_id=file_path,
                        category="performance",
                        priority="high",
                        description="使用 enumerate() 替代 range(len())",
                        code_location=(node.lineno, node.lineno),
                        original_code=f"for {ast.unparse(node.target)} in {ast.unparse(node.iter)}:",
                        suggested_code=f"for {ast.unparse(node.target)}, item in enumerate({ast.unparse(arg.args[0])}):",
                        estimated_improvement=6.0,
                        reasoning="enumerate()比range(len())更高效且更Pythonic"
                    ))
        
        return suggestions

class AlgorithmAnalyzer:
    """算法分析器"""
    
    def analyze(self, file_path: str) -> List[OptimizationSuggestion]:
        """分析算法效率"""
        suggestions = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # 分析嵌套循环
            suggestions.extend(self._analyze_nested_loops(tree, file_path))
            
            # 分析排序操作
            suggestions.extend(self._analyze_sorting_operations(tree, file_path))
            
            # 分析搜索操作
            suggestions.extend(self._analyze_search_operations(tree, file_path))
        
        except Exception as e:
            logger.error(f"算法分析失败 {file_path}: {e}")
        
        return suggestions
    
    def _analyze_nested_loops(self, tree: ast.AST, file_path: str) -> List[OptimizationSuggestion]:
        """分析嵌套循环"""
        suggestions = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.For):
                nested_depth = self._count_nested_loops(node)
                if nested_depth > 2:
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"algo_{len(suggestions)+1}",
                        component_id=file_path,
                        category="algorithm",
                        priority="high",
                        description=f"检测到 {nested_depth} 层嵌套循环，考虑算法优化",
                        code_location=(node.lineno, node.lineno),
                        original_code=ast.unparse(node)[:100] + "...",
                        suggested_code="# 考虑使用更高效的算法，如哈希表或预计算",
                        estimated_improvement=9.0,
                        reasoning="嵌套循环可能导致性能问题，建议优化算法复杂度"
                    ))
        
        return suggestions
    
    def _count_nested_loops(self, node: ast.For) -> int:
        """计算嵌套循环深度"""
        max_depth = 1
        
        for child in ast.walk(node):
            if isinstance(child, ast.For) and child != node:
                depth = 1 + self._count_nested_loops(child)
                max_depth = max(max_depth, depth)
        
        return max_depth
    
    def _analyze_sorting_operations(self, tree: ast.AST, file_path: str) -> List[OptimizationSuggestion]:
        """分析排序操作"""
        suggestions = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if (isinstance(node.func, ast.Attribute) and 
                    node.func.attr == 'sort'):
                    
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"algo_{len(suggestions)+1}",
                        component_id=file_path,
                        category="algorithm",
                        priority="medium",
                        description="检查排序操作是否必要，考虑使用 sorted() 或其他数据结构",
                        code_location=(node.lineno, node.lineno),
                        original_code=ast.unparse(node),
                        suggested_code="# 考虑使用 heapq 或 bisect 模块",
                        estimated_improvement=5.0,
                        reasoning="使用专门的排序模块可能更高效"
                    ))
        
        return suggestions
    
    def _analyze_search_operations(self, tree: ast.AST, file_path: str) -> List[OptimizationSuggestion]:
        """分析搜索操作"""
        suggestions = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                # 检查 'in' 操作符用于列表搜索
                for op in node.ops:
                    if isinstance(op, ast.In):
                        suggestions.append(create_optimization_suggestion(
                            suggestion_id=f"perf_{len(suggestions)+1}",
                            component_id=file_path,
                            category="performance",
                            priority="medium",
                            description="列表搜索效率较低，考虑使用集合或字典",
                            code_location=(node.lineno, node.lineno),
                            original_code=ast.unparse(node),
                            suggested_code="# 使用 set 或 dict 进行 O(1) 查找",
                            estimated_improvement=6.0,
                            reasoning="集合和字典的查找时间复杂度为O(1)，比列表的O(n)更高效"
                        ))
        
        return suggestions

class MemoryAnalyzer:
    """内存使用分析器"""
    
    def analyze(self, file_path: str) -> List[OptimizationSuggestion]:
        """分析内存使用"""
        suggestions = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                
                # 检查全局变量
                if stripped.startswith('global '):
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"mem_{len(suggestions)+1}",
                        component_id=file_path,
                        category="memory",
                        priority="medium",
                        description="全局变量可能导致内存泄漏，考虑使用局部变量",
                        code_location=(i, i),
                        original_code=stripped,
                        suggested_code="# 使用函数参数或类属性",
                        estimated_improvement=4.0,
                        reasoning="全局变量会增加内存占用和耦合度"
                    ))
                
                # 检查大数据结构创建
                if re.search(r'\[.*\]\s*\*\s*\d+', stripped):
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"mem_{len(suggestions)+1}",
                        component_id=file_path,
                        category="memory",
                        priority="high",
                        description="大列表创建可能消耗大量内存，考虑使用生成器",
                        code_location=(i, i),
                        original_code=stripped,
                        suggested_code="# 使用生成器或 numpy 数组",
                        estimated_improvement=7.0,
                        reasoning="大列表会占用大量内存，生成器可以节省内存"
                    ))
                
                # 检查字符串拼接
                if '+=' in stripped and ('str' in stripped or '"' in stripped or "'" in stripped):
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"mem_{len(suggestions)+1}",
                        component_id=file_path,
                        category="memory",
                        priority="medium",
                        description="字符串拼接效率低，使用 join() 方法",
                        code_location=(i, i),
                        original_code=stripped,
                        suggested_code="# 使用 ''.join(string_list)",
                        estimated_improvement=5.0,
                        reasoning="字符串拼接会创建多个临时对象，join()更高效"
                    ))
        
        except Exception as e:
            logger.error(f"内存分析失败 {file_path}: {e}")
        
        return suggestions

class SecurityAnalyzer:
    """安全分析器"""
    
    def analyze(self, file_path: str) -> List[OptimizationSuggestion]:
        """分析安全问题"""
        suggestions = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                
                # 检查 eval 和 exec 使用
                if 'eval(' in stripped or 'exec(' in stripped:
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"sec_{len(suggestions)+1}",
                        component_id=file_path,
                        category="security",
                        priority="critical",
                        description="避免使用 eval() 和 exec()，存在代码注入风险",
                        code_location=(i, i),
                        original_code=stripped,
                        suggested_code="# 使用 ast.literal_eval() 或其他安全方法",
                        estimated_improvement=9.0,
                        reasoning="eval()和exec()存在严重的安全风险"
                    ))
                
                # 检查硬编码密码
                if re.search(r'password\s*=\s*["\'][^"\']++["\']', stripped, re.IGNORECASE):
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"sec_{len(suggestions)+1}",
                        component_id=file_path,
                        category="security",
                        priority="high",
                        description="避免硬编码密码，使用环境变量或配置文件",
                        code_location=(i, i),
                        original_code=stripped,
                        suggested_code="# 使用 os.environ.get('PASSWORD')",
                        estimated_improvement=8.0,
                        reasoning="硬编码密码存在安全风险"
                    ))
                
                # 检查 shell=True
                if 'shell=True' in stripped:
                    suggestions.append(create_optimization_suggestion(
                        suggestion_id=f"sec_{len(suggestions)+1}",
                        component_id=file_path,
                        category="security",
                        priority="high",
                        description="避免使用 shell=True，存在命令注入风险",
                        code_location=(i, i),
                        original_code=stripped,
                        suggested_code="# 使用列表形式的命令参数",
                        estimated_improvement=7.0,
                        reasoning="shell=True存在命令注入风险"
                    ))
        
        except Exception as e:
            logger.error(f"安全分析失败 {file_path}: {e}")
        
        return suggestions

class CodeQualityAnalyzer:
    """代码质量分析器"""
    
    def analyze(self, file_path: str) -> List[OptimizationSuggestion]:
        """分析代码质量"""
        suggestions = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            # 检查函数长度
            suggestions.extend(self._analyze_function_length(content, file_path))
            
            # 检查重复代码
            suggestions.extend(self._analyze_duplicate_code(lines, file_path))
            
            # 检查注释覆盖率
            suggestions.extend(self._analyze_comment_coverage(lines, file_path))
        
        except Exception as e:
            logger.error(f"代码质量分析失败 {file_path}: {e}")
        
        return suggestions
    
    def _analyze_function_length(self, content: str, file_path: str) -> List[OptimizationSuggestion]:
        """分析函数长度"""
        suggestions = []
        
        try:
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # 计算函数行数
                    if hasattr(node, 'end_lineno') and node.end_lineno:
                        func_lines = node.end_lineno - node.lineno + 1
                        
                        if func_lines > 50:
                            suggestions.append(create_optimization_suggestion(
                                suggestion_id=f"quality_{len(suggestions)+1}",
                                component_id=file_path,
                                category="code_quality",
                                priority="medium",
                                description=f"函数 {node.name} 过长 ({func_lines} 行)，建议拆分",
                                code_location=(node.lineno, node.lineno),
                                original_code=f"def {node.name}(...):",
                                suggested_code="# 拆分为多个小函数",
                                estimated_improvement=6.0,
                                reasoning="长函数难以维护和测试"
                            ))
        
        except Exception as e:
            logger.error(f"函数长度分析失败: {e}")
        
        return suggestions
    
    def _analyze_duplicate_code(self, lines: List[str], file_path: str) -> List[OptimizationSuggestion]:
        """分析重复代码"""
        suggestions = []
        
        # 简单的重复行检测
        line_counts = defaultdict(list)
        
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped and not stripped.startswith('#') and len(stripped) > 10:
                line_counts[stripped].append(i)
        
        for line_content, line_numbers in line_counts.items():
            if len(line_numbers) > 2:
                suggestions.append(create_optimization_suggestion(
                    suggestion_id=f"quality_{len(suggestions)+1}",
                    component_id=file_path,
                    category="code_quality",
                    priority="medium",
                    description=f"检测到重复代码，出现在第 {', '.join(map(str, line_numbers))} 行",
                    code_location=(line_numbers[0], line_numbers[0]),
                    original_code=line_content[:50] + "...",
                    suggested_code="# 提取为函数或常量",
                    estimated_improvement=4.0,
                    reasoning="重复代码增加维护成本"
                ))
        
        return suggestions
    
    def _analyze_comment_coverage(self, lines: List[str], file_path: str) -> List[OptimizationSuggestion]:
        """分析注释覆盖率"""
        suggestions = []
        
        total_lines = len([line for line in lines if line.strip()])
        comment_lines = len([line for line in lines if line.strip().startswith('#')])
        
        if total_lines > 0:
            comment_ratio = comment_lines / total_lines
            
            if comment_ratio < 0.1:  # 注释覆盖率低于10%
                suggestions.append(create_optimization_suggestion(
                    suggestion_id=f"quality_{len(suggestions)+1}",
                    component_id=file_path,
                    category="code_quality",
                    priority="low",
                    description=f"注释覆盖率较低 ({comment_ratio:.1%})，建议增加注释",
                    code_location=(1, total_lines),
                    original_code="# 缺少注释",
                    suggested_code="# 添加函数和类的文档字符串",
                    estimated_improvement=3.0,
                    reasoning="充分的注释有助于代码维护"
                ))
        
        return suggestions

class RiskAssessor:
    """风险评估器"""
    
    def assess_optimization_risk(self, suggestion: OptimizationSuggestion) -> RiskAssessment:
        """评估优化建议的风险"""
        risk_factors = []
        risk_score = 0.0
        
        # 基于优化类型的风险评估
        type_risks = {
            "security": 9.0,
            "algorithm": 7.0,
            "performance": 5.0,
            "memory": 4.0,
            "code_quality": 2.0,
            "maintainability": 1.0
        }
        
        base_risk = type_risks.get(suggestion.type, 3.0)
        risk_score += base_risk
        
        # 基于影响分数的风险评估
        if suggestion.estimated_improvement > 8.0:
            risk_score += 3.0
            risk_factors.append("高影响分数")
        elif suggestion.estimated_improvement > 5.0:
            risk_score += 1.0
            risk_factors.append("中等影响分数")
        
        # 基于置信度的风险评估 - 使用默认值
        confidence = 0.8  # 默认置信度
        if confidence < 0.5:
            risk_score += 2.0
            risk_factors.append("低置信度")
        elif confidence < 0.7:
            risk_score += 1.0
            risk_factors.append("中等置信度")
        
        # 基于代码位置的风险评估
        code_location_str = str(suggestion.code_location)
        if '核心' in code_location_str or '关键' in code_location_str:
            risk_score += 2.0
            risk_factors.append("核心代码区域")
        
        # 确定风险等级
        if risk_score >= 8.0:
            risk_level = RiskLevel.CRITICAL
        elif risk_score >= 6.0:
            risk_level = RiskLevel.HIGH
        elif risk_score >= 4.0:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW
        
        # 确定是否需要手动确认
        requires_manual_confirmation = risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]
        
        return RiskAssessment(
            risk_id=f"risk_{suggestion.suggestion_id}",
            component_id=suggestion.component_id,
            risk_level=risk_level.value,
            risk_factors=risk_factors,
            impact_analysis=f"优化建议 {suggestion.suggestion_id} 的风险分析",
            mitigation_strategies=self._generate_mitigation_strategies(risk_factors),
            requires_manual_review=requires_manual_confirmation,
            backup_required=risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL],
            rollback_plan=f"如需回滚，请恢复 {suggestion.component_id} 的原始代码",
            confidence_score=risk_score / 10.0
        )
    
    def _generate_mitigation_strategies(self, risk_factors: List[str]) -> List[str]:
        """生成风险缓解策略"""
        strategies = []
        
        if "高影响分数" in risk_factors:
            strategies.append("在测试环境中充分验证")
            strategies.append("创建详细的回滚计划")
        
        if "低置信度" in risk_factors:
            strategies.append("进行人工代码审查")
            strategies.append("增加单元测试覆盖")
        
        if "核心代码区域" in risk_factors:
            strategies.append("分阶段实施优化")
            strategies.append("监控性能指标")
        
        if not strategies:
            strategies.append("标准测试流程")
        
        return strategies

class OptimizationEngine:
    """优化引擎主类"""
    
    def __init__(self):
        self.analyzers = {
            'data_structure': DataStructureAnalyzer(),
            'algorithm': AlgorithmAnalyzer(),
            'memory': MemoryAnalyzer(),
            'security': SecurityAnalyzer(),
            'code_quality': CodeQualityAnalyzer()
        }
        self.risk_assessor = RiskAssessor()
    
    def generate_suggestions(self, file_path: str) -> List[OptimizationSuggestion]:
        """生成优化建议"""
        all_suggestions = []
        
        logger.info(f"开始分析文件: {file_path}")
        
        for analyzer_name, analyzer in self.analyzers.items():
            try:
                suggestions = analyzer.analyze(file_path)
                all_suggestions.extend(suggestions)
                logger.info(f"{analyzer_name} 分析器发现 {len(suggestions)} 个建议")
            except Exception as e:
                logger.error(f"{analyzer_name} 分析器执行失败: {e}")
        
        # 为每个建议生成唯一ID
        for i, suggestion in enumerate(all_suggestions):
            suggestion.suggestion_id = f"{os.path.basename(file_path)}_{i+1}"
        
        logger.info(f"总共生成 {len(all_suggestions)} 个优化建议")
        return all_suggestions
    
    def assess_risks(self, suggestions: List[OptimizationSuggestion]) -> List[RiskAssessment]:
        """评估优化建议的风险"""
        risk_assessments = []
        
        for suggestion in suggestions:
            try:
                assessment = self.risk_assessor.assess_optimization_risk(suggestion)
                risk_assessments.append(assessment)
            except Exception as e:
                logger.error(f"风险评估失败 {suggestion.suggestion_id}: {e}")
        
        return risk_assessments
    
    def filter_suggestions_by_risk(self, suggestions: List[OptimizationSuggestion], 
                                 risk_assessments: List[RiskAssessment],
                                 max_risk_level: str = "medium") -> List[OptimizationSuggestion]:
        """根据风险等级过滤建议"""
        risk_order = {"low": 1, "medium": 2, "high": 3, "critical": 4}
        max_risk_value = risk_order.get(max_risk_level, 2)
        
        filtered_suggestions = []
        risk_dict = {assessment.risk_id.replace('risk_', ''): assessment for assessment in risk_assessments}
        
        for suggestion in suggestions:
            assessment = risk_dict.get(suggestion.suggestion_id)
            if assessment:
                risk_value = risk_order.get(assessment.risk_level, 4)
                if risk_value <= max_risk_value:
                    filtered_suggestions.append(suggestion)
        
        return filtered_suggestions
    
    def get_high_risk_suggestions(self, suggestions: List[OptimizationSuggestion],
                                risk_assessments: List[RiskAssessment]) -> List[Tuple[OptimizationSuggestion, RiskAssessment]]:
        """获取高风险建议（需要手动确认）"""
        high_risk_suggestions = []
        risk_dict = {assessment.risk_id.replace('risk_', ''): assessment for assessment in risk_assessments}
        
        for suggestion in suggestions:
            assessment = risk_dict.get(suggestion.suggestion_id)
            if assessment and assessment.requires_manual_review:
                high_risk_suggestions.append((suggestion, assessment))
        
        return high_risk_suggestions
    
    def apply_optimization(self, suggestion: OptimizationSuggestion, 
                         backup_path: Optional[str] = None) -> OptimizationResult:
        """应用优化建议"""
        try:
            # 这里实现具体的优化应用逻辑
            # 由于涉及代码修改，这里只是示例框架
            
            result = OptimizationResult(
                suggestion_id=suggestion.suggestion_id,
                success=True,
                applied_changes="示例优化应用",
                performance_improvement=suggestion.impact_score,
                backup_location=backup_path,
                error_message=None
            )
            
            logger.info(f"优化应用成功: {suggestion.suggestion_id}")
            return result
            
        except Exception as e:
            logger.error(f"优化应用失败 {suggestion.suggestion_id}: {e}")
            return OptimizationResult(
                suggestion_id=suggestion.suggestion_id,
                success=False,
                applied_changes=None,
                performance_improvement=0.0,
                backup_location=backup_path,
                error_message=str(e)
            )
    
    def generate_optimization_report(self, suggestions: List[OptimizationSuggestion],
                                   risk_assessments: List[RiskAssessment]) -> Dict[str, Any]:
        """生成优化报告"""
        risk_dict = {assessment.risk_id.replace('risk_', ''): assessment for assessment in risk_assessments}
        
        report = {
            'total_suggestions': len(suggestions),
            'risk_distribution': defaultdict(int),
            'type_distribution': defaultdict(int),
            'high_impact_suggestions': [],
            'manual_confirmation_required': [],
            'summary': {}
        }
        
        total_impact = 0.0
        high_risk_count = 0
        
        for suggestion in suggestions:
            # 统计类型分布
            report['type_distribution'][suggestion.type] += 1
            
            # 统计风险分布
            assessment = risk_dict.get(suggestion.suggestion_id)
            if assessment:
                report['risk_distribution'][assessment.risk_level] += 1
                
                if assessment.requires_manual_review:
                    report['manual_confirmation_required'].append({
                        'suggestion': asdict(suggestion),
                        'risk_assessment': asdict(assessment)
                    })
                    high_risk_count += 1
            
            # 统计高影响建议
            if suggestion.estimated_improvement > 7.0:
                report['high_impact_suggestions'].append(asdict(suggestion))
            
            total_impact += suggestion.estimated_improvement
        
        # 生成摘要
        report['summary'] = {
            'average_impact_score': total_impact / len(suggestions) if suggestions else 0,
            'high_risk_percentage': (high_risk_count / len(suggestions) * 100) if suggestions else 0,
            'most_common_type': max(report['type_distribution'].items(), key=lambda x: x[1])[0] if report['type_distribution'] else None,
            'most_common_risk': max(report['risk_distribution'].items(), key=lambda x: x[1])[0] if report['risk_distribution'] else None
        }
        
        return report