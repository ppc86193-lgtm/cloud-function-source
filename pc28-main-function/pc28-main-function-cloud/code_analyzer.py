#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代码分析模块
提供代码复杂度分析、AST解析、静态分析等功能
"""

import ast
import os
import re
import logging
from typing import Dict, List, Tuple, Set, Optional, Any
from collections import defaultdict, Counter
from pathlib import Path

from models import ComplexityMetrics, MemoryUsageMetrics, CPUUsageMetrics, IOMetrics

logger = logging.getLogger(__name__)

class ASTAnalyzer:
    """AST抽象语法树分析器"""
    
    def __init__(self):
        self.node_counts = defaultdict(int)
        self.function_info = []
        self.class_info = []
        self.import_info = []
        self.complexity_info = []
    
    def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """分析Python文件的AST结构"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content, filename=file_path)
            
            # 重置分析结果
            self.node_counts.clear()
            self.function_info.clear()
            self.class_info.clear()
            self.import_info.clear()
            self.complexity_info.clear()
            
            # 遍历AST节点
            self._visit_node(tree)
            
            return {
                'node_counts': dict(self.node_counts),
                'functions': self.function_info,
                'classes': self.class_info,
                'imports': self.import_info,
                'complexity': self.complexity_info
            }
            
        except Exception as e:
            logger.error(f"分析文件 {file_path} 时出错: {e}")
            return {}
    
    def _visit_node(self, node: ast.AST, depth: int = 0):
        """递归访问AST节点"""
        node_type = type(node).__name__
        self.node_counts[node_type] += 1
        
        # 分析函数定义
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self._analyze_function(node, depth)
        
        # 分析类定义
        elif isinstance(node, ast.ClassDef):
            self._analyze_class(node, depth)
        
        # 分析导入语句
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            self._analyze_import(node)
        
        # 分析控制流语句
        elif isinstance(node, (ast.If, ast.For, ast.While, ast.Try)):
            self._analyze_control_flow(node, depth)
        
        # 递归访问子节点
        for child in ast.iter_child_nodes(node):
            self._visit_node(child, depth + 1)
    
    def _analyze_function(self, node: ast.FunctionDef, depth: int):
        """分析函数定义"""
        func_info = {
            'name': node.name,
            'line_start': node.lineno,
            'line_end': getattr(node, 'end_lineno', node.lineno),
            'args_count': len(node.args.args),
            'decorators_count': len(node.decorator_list),
            'is_async': isinstance(node, ast.AsyncFunctionDef),
            'depth': depth,
            'complexity': self._calculate_function_complexity(node)
        }
        
        # 分析函数参数
        func_info['has_defaults'] = len(node.args.defaults) > 0
        func_info['has_varargs'] = node.args.vararg is not None
        func_info['has_kwargs'] = node.args.kwarg is not None
        
        self.function_info.append(func_info)
    
    def _analyze_class(self, node: ast.ClassDef, depth: int):
        """分析类定义"""
        class_info = {
            'name': node.name,
            'line_start': node.lineno,
            'line_end': getattr(node, 'end_lineno', node.lineno),
            'bases_count': len(node.bases),
            'decorators_count': len(node.decorator_list),
            'depth': depth,
            'methods': [],
            'attributes': []
        }
        
        # 分析类方法和属性
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                class_info['methods'].append(item.name)
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        class_info['attributes'].append(target.id)
        
        self.class_info.append(class_info)
    
    def _analyze_import(self, node: ast.AST):
        """分析导入语句"""
        import_info = {
            'line': node.lineno,
            'type': type(node).__name__
        }
        
        if isinstance(node, ast.Import):
            import_info['modules'] = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            import_info['module'] = node.module
            import_info['names'] = [alias.name for alias in node.names]
            import_info['level'] = node.level
        
        self.import_info.append(import_info)
    
    def _analyze_control_flow(self, node: ast.AST, depth: int):
        """分析控制流语句"""
        complexity_info = {
            'type': type(node).__name__,
            'line': node.lineno,
            'depth': depth
        }
        
        if isinstance(node, ast.If):
            complexity_info['has_else'] = len(node.orelse) > 0
            complexity_info['elif_count'] = sum(1 for n in node.orelse if isinstance(n, ast.If))
        
        elif isinstance(node, (ast.For, ast.While)):
            complexity_info['has_else'] = len(node.orelse) > 0
        
        elif isinstance(node, ast.Try):
            complexity_info['except_count'] = len(node.handlers)
            complexity_info['has_finally'] = len(node.finalbody) > 0
            complexity_info['has_else'] = len(node.orelse) > 0
        
        self.complexity_info.append(complexity_info)
    
    def _calculate_function_complexity(self, node: ast.FunctionDef) -> int:
        """计算函数的圈复杂度"""
        complexity = 1  # 基础复杂度
        
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.While, ast.For, ast.AsyncFor)):
                complexity += 1
            elif isinstance(child, ast.ExceptHandler):
                complexity += 1
            elif isinstance(child, (ast.And, ast.Or)):
                complexity += 1
            elif isinstance(child, ast.BoolOp):
                complexity += len(child.values) - 1
        
        return complexity

class ComplexityAnalyzer:
    """代码复杂度分析器"""
    
    def __init__(self):
        self.ast_analyzer = ASTAnalyzer()
    
    def analyze_file_complexity(self, file_path: str) -> ComplexityMetrics:
        """分析文件的复杂度指标"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            # 基础指标
            line_count = len(lines)
            comment_lines = self._count_comment_lines(lines)
            comment_ratio = comment_lines / max(line_count, 1)
            
            # AST分析
            ast_result = self.ast_analyzer.analyze_file(file_path)
            function_count = len(ast_result.get('functions', []))
            class_count = len(ast_result.get('classes', []))
            
            # 复杂度计算
            cyclomatic_complexity = self._calculate_cyclomatic_complexity(ast_result)
            cognitive_complexity = self._calculate_cognitive_complexity(ast_result)
            nesting_depth = self._calculate_max_nesting_depth(file_path)
            duplicate_lines = self._count_duplicate_lines(lines)
            
            return ComplexityMetrics(
                component_id=file_path,
                cyclomatic_complexity=cyclomatic_complexity,
                cognitive_complexity=cognitive_complexity,
                nesting_depth=nesting_depth,
                function_count=function_count,
                class_count=class_count,
                line_count=line_count,
                comment_ratio=comment_ratio,
                duplicate_lines=duplicate_lines
            )
            
        except Exception as e:
            logger.error(f"分析文件复杂度时出错 {file_path}: {e}")
            return ComplexityMetrics(
                component_id=file_path,
                cyclomatic_complexity=0,
                cognitive_complexity=0,
                nesting_depth=0,
                function_count=0,
                class_count=0,
                line_count=0,
                comment_ratio=0.0,
                duplicate_lines=0
            )
    
    def _count_comment_lines(self, lines: List[str]) -> int:
        """统计注释行数"""
        comment_count = 0
        in_multiline_comment = False
        
        for line in lines:
            stripped = line.strip()
            
            # 多行注释
            if '"""' in stripped or "'''" in stripped:
                quote_count = stripped.count('"""') + stripped.count("'''")
                if quote_count % 2 == 1:
                    in_multiline_comment = not in_multiline_comment
                if in_multiline_comment or quote_count > 0:
                    comment_count += 1
            elif in_multiline_comment:
                comment_count += 1
            # 单行注释
            elif stripped.startswith('#'):
                comment_count += 1
        
        return comment_count
    
    def _calculate_cyclomatic_complexity(self, ast_result: Dict[str, Any]) -> int:
        """计算圈复杂度"""
        total_complexity = 1  # 基础复杂度
        
        # 从函数复杂度累加
        for func in ast_result.get('functions', []):
            total_complexity += func.get('complexity', 1)
        
        # 从控制流语句累加
        for control in ast_result.get('complexity', []):
            if control['type'] in ['If', 'For', 'While', 'Try']:
                total_complexity += 1
        
        return total_complexity
    
    def _calculate_cognitive_complexity(self, ast_result: Dict[str, Any]) -> int:
        """计算认知复杂度"""
        cognitive_complexity = 0
        
        # 基于嵌套深度和控制结构计算
        for control in ast_result.get('complexity', []):
            depth = control.get('depth', 0)
            control_type = control['type']
            
            # 基础复杂度
            base_complexity = 1
            
            # 嵌套惩罚
            nesting_penalty = depth
            
            # 特定结构的额外复杂度
            if control_type == 'If':
                if control.get('elif_count', 0) > 0:
                    base_complexity += control['elif_count']
            elif control_type == 'Try':
                base_complexity += control.get('except_count', 0)
            
            cognitive_complexity += base_complexity + nesting_penalty
        
        return cognitive_complexity
    
    def _calculate_max_nesting_depth(self, file_path: str) -> int:
        """计算最大嵌套深度"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            max_depth = 0
            current_depth = 0
            
            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith('#'):
                    continue
                
                # 计算缩进级别
                indent_level = (len(line) - len(line.lstrip())) // 4
                
                # 检查是否是控制结构
                if any(stripped.startswith(keyword) for keyword in 
                       ['if ', 'elif ', 'else:', 'for ', 'while ', 'try:', 'except', 'finally:', 'with ', 'def ', 'class ']):
                    current_depth = indent_level + 1
                    max_depth = max(max_depth, current_depth)
                else:
                    current_depth = indent_level
            
            return max_depth
            
        except Exception as e:
            logger.error(f"计算嵌套深度时出错 {file_path}: {e}")
            return 0
    
    def _count_duplicate_lines(self, lines: List[str]) -> int:
        """统计重复代码行数"""
        # 过滤空行和注释行
        code_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                # 标准化代码行（移除多余空格）
                normalized = ' '.join(stripped.split())
                code_lines.append(normalized)
        
        # 统计重复行
        line_counts = Counter(code_lines)
        duplicate_count = sum(count - 1 for count in line_counts.values() if count > 1)
        
        return duplicate_count

class StaticAnalyzer:
    """静态代码分析器"""
    
    def __init__(self):
        self.complexity_analyzer = ComplexityAnalyzer()
    
    def analyze_code_quality(self, file_path: str) -> Dict[str, Any]:
        """分析代码质量"""
        quality_issues = {
            'style_issues': [],
            'potential_bugs': [],
            'security_issues': [],
            'performance_issues': [],
            'maintainability_issues': []
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            # 检查各种代码质量问题
            self._check_style_issues(lines, quality_issues['style_issues'])
            self._check_potential_bugs(content, quality_issues['potential_bugs'])
            self._check_security_issues(content, quality_issues['security_issues'])
            self._check_performance_issues(content, quality_issues['performance_issues'])
            self._check_maintainability_issues(lines, quality_issues['maintainability_issues'])
            
        except Exception as e:
            logger.error(f"分析代码质量时出错 {file_path}: {e}")
        
        return quality_issues
    
    def _check_style_issues(self, lines: List[str], issues: List[Dict]):
        """检查代码风格问题"""
        for i, line in enumerate(lines, 1):
            # 行长度检查
            if len(line) > 120:
                issues.append({
                    'type': 'line_too_long',
                    'line': i,
                    'message': f'行长度超过120字符 ({len(line)}字符)',
                    'severity': 'warning'
                })
            
            # 尾随空格检查
            if line.endswith(' ') or line.endswith('\t'):
                issues.append({
                    'type': 'trailing_whitespace',
                    'line': i,
                    'message': '行末有多余空格',
                    'severity': 'info'
                })
            
            # 制表符检查
            if '\t' in line:
                issues.append({
                    'type': 'tab_character',
                    'line': i,
                    'message': '使用制表符而非空格缩进',
                    'severity': 'warning'
                })
    
    def _check_potential_bugs(self, content: str, issues: List[Dict]):
        """检查潜在的bug"""
        # 检查可能的变量名拼写错误
        if re.search(r'\b(lenght|widht|heigth)\b', content):
            issues.append({
                'type': 'possible_typo',
                'message': '可能存在变量名拼写错误',
                'severity': 'warning'
            })
        
        # 检查可能的逻辑错误
        if re.search(r'if.*=.*:', content):
            issues.append({
                'type': 'assignment_in_condition',
                'message': '条件语句中可能存在赋值操作',
                'severity': 'error'
            })
    
    def _check_security_issues(self, content: str, issues: List[Dict]):
        """检查安全问题"""
        # 检查硬编码密码
        if re.search(r'password\s*=\s*["\'][^"\']++["\']', content, re.IGNORECASE):
            issues.append({
                'type': 'hardcoded_password',
                'message': '发现硬编码密码',
                'severity': 'critical'
            })
        
        # 检查SQL注入风险
        if re.search(r'execute\s*\(.*%.*\)', content):
            issues.append({
                'type': 'sql_injection_risk',
                'message': '可能存在SQL注入风险',
                'severity': 'high'
            })
        
        # 检查eval使用
        if 'eval(' in content:
            issues.append({
                'type': 'eval_usage',
                'message': '使用eval()函数存在安全风险',
                'severity': 'high'
            })
    
    def _check_performance_issues(self, content: str, issues: List[Dict]):
        """检查性能问题"""
        # 检查低效的字符串拼接
        if re.search(r'\+\s*=\s*["\']', content):
            issues.append({
                'type': 'inefficient_string_concat',
                'message': '使用+=进行字符串拼接效率较低',
                'severity': 'warning'
            })
        
        # 检查全局变量过多
        global_vars = re.findall(r'^\s*global\s+', content, re.MULTILINE)
        if len(global_vars) > 5:
            issues.append({
                'type': 'too_many_globals',
                'message': f'全局变量过多 ({len(global_vars)}个)',
                'severity': 'warning'
            })
    
    def _check_maintainability_issues(self, lines: List[str], issues: List[Dict]):
        """检查可维护性问题"""
        # 检查函数长度
        in_function = False
        function_start = 0
        function_name = ''
        
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            
            if stripped.startswith('def ') or stripped.startswith('async def '):
                if in_function:
                    # 检查上一个函数的长度
                    func_length = i - function_start
                    if func_length > 50:
                        issues.append({
                            'type': 'function_too_long',
                            'line': function_start,
                            'message': f'函数 {function_name} 过长 ({func_length}行)',
                            'severity': 'warning'
                        })
                
                in_function = True
                function_start = i
                function_name = stripped.split('(')[0].replace('def ', '').replace('async def ', '')
            
            elif stripped.startswith('class '):
                in_function = False
        
        # 检查最后一个函数
        if in_function:
            func_length = len(lines) - function_start + 1
            if func_length > 50:
                issues.append({
                    'type': 'function_too_long',
                    'line': function_start,
                    'message': f'函数 {function_name} 过长 ({func_length}行)',
                    'severity': 'warning'
                })

class CodeMetricsCollector:
    """代码指标收集器"""
    
    def __init__(self):
        self.complexity_analyzer = ComplexityAnalyzer()
        self.static_analyzer = StaticAnalyzer()
    
    def collect_file_metrics(self, file_path: str) -> Dict[str, Any]:
        """收集文件的所有指标"""
        metrics = {
            'file_path': file_path,
            'file_size': 0,
            'complexity_metrics': None,
            'quality_issues': None,
            'ast_analysis': None
        }
        
        try:
            # 文件大小
            metrics['file_size'] = os.path.getsize(file_path)
            
            # 复杂度指标
            metrics['complexity_metrics'] = self.complexity_analyzer.analyze_file_complexity(file_path)
            
            # 代码质量问题
            metrics['quality_issues'] = self.static_analyzer.analyze_code_quality(file_path)
            
            # AST分析
            ast_analyzer = ASTAnalyzer()
            metrics['ast_analysis'] = ast_analyzer.analyze_file(file_path)
            
        except Exception as e:
            logger.error(f"收集文件指标时出错 {file_path}: {e}")
        
        return metrics
    
    def collect_project_metrics(self, project_path: str) -> Dict[str, Any]:
        """收集整个项目的指标"""
        project_metrics = {
            'project_path': project_path,
            'total_files': 0,
            'total_lines': 0,
            'total_functions': 0,
            'total_classes': 0,
            'average_complexity': 0.0,
            'file_metrics': []
        }
        
        python_files = list(Path(project_path).rglob('*.py'))
        project_metrics['total_files'] = len(python_files)
        
        total_complexity = 0
        
        for file_path in python_files:
            try:
                file_metrics = self.collect_file_metrics(str(file_path))
                project_metrics['file_metrics'].append(file_metrics)
                
                if file_metrics['complexity_metrics']:
                    complexity = file_metrics['complexity_metrics']
                    project_metrics['total_lines'] += complexity.line_count
                    project_metrics['total_functions'] += complexity.function_count
                    project_metrics['total_classes'] += complexity.class_count
                    total_complexity += complexity.get_complexity_score()
                
            except Exception as e:
                logger.error(f"处理文件时出错 {file_path}: {e}")
        
        # 计算平均复杂度
        if project_metrics['total_files'] > 0:
            project_metrics['average_complexity'] = total_complexity / project_metrics['total_files']
        
        return project_metrics