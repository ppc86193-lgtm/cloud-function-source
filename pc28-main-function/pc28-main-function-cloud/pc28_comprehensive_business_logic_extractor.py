#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统全面业务逻辑提取器
重新进行完整的业务逻辑提取，确保覆盖所有代码和数据库逻辑
"""

import os
import ast
import json
import logging
import re
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Set
from collections import defaultdict
import sqlite3
import subprocess

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'pc28_comprehensive_extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveBusinessLogicExtractor:
    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)
        self.business_logic = {
            'code_logic': [],
            'database_logic': [],
            'config_logic': [],
            'api_endpoints': [],
            'business_rules': [],
            'data_flows': [],
            'calculations': [],
            'validations': [],
            'workflows': []
        }
        self.statistics = {
            'total_files_scanned': 0,
            'python_files': 0,
            'config_files': 0,
            'sql_files': 0,
            'functions_extracted': 0,
            'classes_extracted': 0,
            'api_endpoints': 0,
            'business_rules': 0
        }
        
        # 业务逻辑关键词
        self.business_keywords = {
            'lottery': ['lottery', '彩票', 'pc28', 'draw', '开奖', 'period', '期号'],
            'betting': ['bet', '投注', 'wager', 'stake', '下注', 'odds', '赔率'],
            'payout': ['payout', '派奖', 'prize', '奖金', 'win', '中奖', 'bonus'],
            'risk': ['risk', '风险', 'limit', '限制', 'control', '控制', 'monitor'],
            'user': ['user', '用户', 'account', '账户', 'profile', '档案'],
            'financial': ['balance', '余额', 'deposit', '充值', 'withdraw', '提现', 'transaction'],
            'game': ['game', '游戏', 'round', '轮次', 'result', '结果'],
            'admin': ['admin', '管理', 'manage', '管理员', 'operator', '操作员']
        }

    def extract_all_business_logic(self):
        """提取所有业务逻辑"""
        logger.info("开始全面业务逻辑提取...")
        
        # 1. 深度代码扫描
        self._scan_python_files()
        
        # 2. 配置文件分析
        self._analyze_config_files()
        
        # 3. SQL文件分析
        self._analyze_sql_files()
        
        # 4. 数据库逻辑分析
        self._analyze_database_logic()
        
        # 5. 生成完整报告
        report = self._generate_comprehensive_report()
        
        return report

    def _scan_python_files(self):
        """深度扫描Python文件"""
        logger.info("开始扫描Python文件...")
        
        python_files = list(self.root_dir.rglob("*.py"))
        self.statistics['python_files'] = len(python_files)
        
        for py_file in python_files:
            try:
                self._analyze_python_file(py_file)
                self.statistics['total_files_scanned'] += 1
            except Exception as e:
                logger.warning(f"分析Python文件失败 {py_file}: {e}")

    def _analyze_python_file(self, file_path: Path):
        """分析单个Python文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # AST解析
            tree = ast.parse(content)
            
            # 提取函数和类
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    self._extract_function_logic(node, file_path, content)
                elif isinstance(node, ast.ClassDef):
                    self._extract_class_logic(node, file_path, content)
                elif isinstance(node, ast.Assign):
                    self._extract_assignment_logic(node, file_path, content)
            
            # 提取API端点
            self._extract_api_endpoints(content, file_path)
            
            # 提取业务规则
            self._extract_business_rules(content, file_path)
            
        except Exception as e:
            logger.warning(f"解析文件失败 {file_path}: {e}")

    def _extract_function_logic(self, node: ast.FunctionDef, file_path: Path, content: str):
        """提取函数业务逻辑"""
        func_name = node.name
        
        # 获取函数源码
        try:
            lines = content.split('\n')
            start_line = node.lineno - 1
            end_line = node.end_lineno if hasattr(node, 'end_lineno') else start_line + 10
            func_code = '\n'.join(lines[start_line:end_line])
        except:
            func_code = f"def {func_name}(...)"
        
        # 分析业务逻辑类型
        business_type = self._classify_business_logic(func_name, func_code)
        
        logic_item = {
            'type': 'function',
            'name': func_name,
            'file': str(file_path.relative_to(self.root_dir)),
            'line': node.lineno,
            'business_category': business_type,
            'code': func_code[:500],  # 限制长度
            'docstring': ast.get_docstring(node) or '',
            'parameters': [arg.arg for arg in node.args.args],
            'complexity': self._calculate_complexity(node),
            'extracted_at': datetime.now().isoformat()
        }
        
        self.business_logic['code_logic'].append(logic_item)
        self.statistics['functions_extracted'] += 1

    def _extract_class_logic(self, node: ast.ClassDef, file_path: Path, content: str):
        """提取类业务逻辑"""
        class_name = node.name
        
        # 获取类的方法
        methods = [n.name for n in node.body if isinstance(n, ast.FunctionDef)]
        
        business_type = self._classify_business_logic(class_name, str(methods))
        
        logic_item = {
            'type': 'class',
            'name': class_name,
            'file': str(file_path.relative_to(self.root_dir)),
            'line': node.lineno,
            'business_category': business_type,
            'methods': methods,
            'docstring': ast.get_docstring(node) or '',
            'extracted_at': datetime.now().isoformat()
        }
        
        self.business_logic['code_logic'].append(logic_item)
        self.statistics['classes_extracted'] += 1

    def _extract_assignment_logic(self, node: ast.Assign, file_path: Path, content: str):
        """提取赋值语句中的业务逻辑"""
        try:
            # 获取变量名
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                var_name = node.targets[0].id
                
                # 检查是否包含业务逻辑
                if any(keyword in var_name.lower() for keywords in self.business_keywords.values() for keyword in keywords):
                    lines = content.split('\n')
                    line_content = lines[node.lineno - 1] if node.lineno <= len(lines) else ''
                    
                    logic_item = {
                        'type': 'variable',
                        'name': var_name,
                        'file': str(file_path.relative_to(self.root_dir)),
                        'line': node.lineno,
                        'business_category': self._classify_business_logic(var_name, line_content),
                        'code': line_content.strip(),
                        'extracted_at': datetime.now().isoformat()
                    }
                    
                    self.business_logic['code_logic'].append(logic_item)
        except:
            pass

    def _extract_api_endpoints(self, content: str, file_path: Path):
        """提取API端点"""
        # Flask路由
        flask_patterns = [
            r'@app\.route\([\'"]([^\'"]+)[\'"]',
            r'@bp\.route\([\'"]([^\'"]+)[\'"]',
            r'@.*\.route\([\'"]([^\'"]+)[\'"]'
        ]
        
        # FastAPI路由
        fastapi_patterns = [
            r'@app\.(get|post|put|delete|patch)\([\'"]([^\'"]+)[\'"]',
            r'@router\.(get|post|put|delete|patch)\([\'"]([^\'"]+)[\'"]'
        ]
        
        all_patterns = flask_patterns + [p.replace('(get|post|put|delete|patch)', 'get|post|put|delete|patch') for p in fastapi_patterns]
        
        for pattern in all_patterns:
            matches = re.finditer(pattern, content, re.MULTILINE)
            for match in matches:
                endpoint = match.group(1) if len(match.groups()) == 1 else match.group(2)
                
                api_item = {
                    'type': 'api_endpoint',
                    'endpoint': endpoint,
                    'file': str(file_path.relative_to(self.root_dir)),
                    'business_category': self._classify_business_logic(endpoint, ''),
                    'extracted_at': datetime.now().isoformat()
                }
                
                self.business_logic['api_endpoints'].append(api_item)
                self.statistics['api_endpoints'] += 1

    def _extract_business_rules(self, content: str, file_path: Path):
        """提取业务规则"""
        # 查找条件语句和业务规则
        rule_patterns = [
            r'if.*(?:limit|max|min|check|validate|verify).*:',
            r'assert.*(?:limit|max|min|check|validate|verify)',
            r'raise.*(?:limit|max|min|check|validate|verify)',
            r'def.*(?:check|validate|verify|calculate).*\(',
        ]
        
        lines = content.split('\n')
        for i, line in enumerate(lines):
            for pattern in rule_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    rule_item = {
                        'type': 'business_rule',
                        'rule': line.strip(),
                        'file': str(file_path.relative_to(self.root_dir)),
                        'line': i + 1,
                        'business_category': self._classify_business_logic(line, ''),
                        'extracted_at': datetime.now().isoformat()
                    }
                    
                    self.business_logic['business_rules'].append(rule_item)
                    self.statistics['business_rules'] += 1

    def _analyze_config_files(self):
        """分析配置文件"""
        logger.info("分析配置文件...")
        
        config_patterns = ['*.yaml', '*.yml', '*.json', '*.ini', '*.conf', '*.cfg']
        
        for pattern in config_patterns:
            config_files = list(self.root_dir.rglob(pattern))
            for config_file in config_files:
                try:
                    self._analyze_config_file(config_file)
                    self.statistics['config_files'] += 1
                except Exception as e:
                    logger.warning(f"分析配置文件失败 {config_file}: {e}")

    def _analyze_config_file(self, file_path: Path):
        """分析单个配置文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 尝试解析YAML/JSON
            config_data = None
            if file_path.suffix.lower() in ['.yaml', '.yml']:
                config_data = yaml.safe_load(content)
            elif file_path.suffix.lower() == '.json':
                config_data = json.loads(content)
            
            if config_data:
                self._extract_config_business_logic(config_data, file_path)
            
            # 提取配置中的业务规则
            self._extract_config_rules(content, file_path)
            
        except Exception as e:
            logger.warning(f"解析配置文件失败 {file_path}: {e}")

    def _extract_config_business_logic(self, config_data: Dict, file_path: Path):
        """从配置数据中提取业务逻辑"""
        def extract_recursive(data, path=""):
            if isinstance(data, dict):
                for key, value in data.items():
                    current_path = f"{path}.{key}" if path else key
                    
                    # 检查是否包含业务逻辑
                    if any(keyword in key.lower() for keywords in self.business_keywords.values() for keyword in keywords):
                        logic_item = {
                            'type': 'config_setting',
                            'key': current_path,
                            'value': str(value)[:200],  # 限制长度
                            'file': str(file_path.relative_to(self.root_dir)),
                            'business_category': self._classify_business_logic(key, str(value)),
                            'extracted_at': datetime.now().isoformat()
                        }
                        self.business_logic['config_logic'].append(logic_item)
                    
                    extract_recursive(value, current_path)
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    extract_recursive(item, f"{path}[{i}]")
        
        extract_recursive(config_data)

    def _extract_config_rules(self, content: str, file_path: Path):
        """从配置文件内容中提取业务规则"""
        lines = content.split('\n')
        for i, line in enumerate(lines):
            # 查找包含业务关键词的配置行
            if any(keyword in line.lower() for keywords in self.business_keywords.values() for keyword in keywords):
                rule_item = {
                    'type': 'config_rule',
                    'rule': line.strip(),
                    'file': str(file_path.relative_to(self.root_dir)),
                    'line': i + 1,
                    'business_category': self._classify_business_logic(line, ''),
                    'extracted_at': datetime.now().isoformat()
                }
                self.business_logic['config_logic'].append(rule_item)

    def _analyze_sql_files(self):
        """分析SQL文件"""
        logger.info("分析SQL文件...")
        
        sql_files = list(self.root_dir.rglob("*.sql"))
        self.statistics['sql_files'] = len(sql_files)
        
        for sql_file in sql_files:
            try:
                self._analyze_sql_file(sql_file)
            except Exception as e:
                logger.warning(f"分析SQL文件失败 {sql_file}: {e}")

    def _analyze_sql_file(self, file_path: Path):
        """分析单个SQL文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 提取SQL中的业务逻辑
            self._extract_sql_business_logic(content, file_path)
            
        except Exception as e:
            logger.warning(f"解析SQL文件失败 {file_path}: {e}")

    def _extract_sql_business_logic(self, content: str, file_path: Path):
        """从SQL内容中提取业务逻辑"""
        # SQL业务逻辑模式
        sql_patterns = [
            r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:VIEW|FUNCTION|PROCEDURE)\s+(\w+)',
            r'SELECT.*FROM\s+(\w+)',
            r'INSERT\s+INTO\s+(\w+)',
            r'UPDATE\s+(\w+)\s+SET',
            r'DELETE\s+FROM\s+(\w+)',
            r'WHERE.*(?:limit|max|min|check|validate)',
            r'CASE\s+WHEN.*THEN.*END',
        ]
        
        lines = content.split('\n')
        for i, line in enumerate(lines):
            for pattern in sql_patterns:
                matches = re.finditer(pattern, line, re.IGNORECASE)
                for match in matches:
                    logic_item = {
                        'type': 'sql_logic',
                        'statement': line.strip()[:200],
                        'file': str(file_path.relative_to(self.root_dir)),
                        'line': i + 1,
                        'business_category': self._classify_business_logic(line, ''),
                        'extracted_at': datetime.now().isoformat()
                    }
                    self.business_logic['database_logic'].append(logic_item)

    def _analyze_database_logic(self):
        """分析数据库逻辑（如果有数据库连接）"""
        logger.info("分析数据库逻辑...")
        
        # 这里可以添加实际的数据库连接和分析逻辑
        # 由于没有具体的数据库连接信息，我们跳过这一步
        logger.info("数据库逻辑分析需要具体的连接信息，跳过此步骤")

    def _classify_business_logic(self, name: str, content: str) -> str:
        """分类业务逻辑"""
        text = f"{name} {content}".lower()
        
        for category, keywords in self.business_keywords.items():
            if any(keyword in text for keyword in keywords):
                return category
        
        return 'general'

    def _calculate_complexity(self, node: ast.AST) -> int:
        """计算代码复杂度"""
        complexity = 1
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.For, ast.While, ast.Try, ast.With)):
                complexity += 1
        return complexity

    def _generate_comprehensive_report(self) -> Dict[str, Any]:
        """生成完整报告"""
        logger.info("生成完整业务逻辑报告...")
        
        # 统计信息
        total_logic_items = sum(len(items) for items in self.business_logic.values())
        
        # 按类别统计
        category_stats = defaultdict(int)
        for items in self.business_logic.values():
            for item in items:
                category_stats[item.get('business_category', 'unknown')] += 1
        
        # 生成报告
        report = {
            'extraction_info': {
                'timestamp': datetime.now().isoformat(),
                'root_directory': str(self.root_dir),
                'extractor_version': '2.0.0',
                'extraction_method': 'comprehensive_ast_analysis'
            },
            'statistics': {
                **self.statistics,
                'total_logic_items': total_logic_items,
                'category_distribution': dict(category_stats)
            },
            'business_logic': self.business_logic,
            'summary': {
                'code_logic_count': len(self.business_logic['code_logic']),
                'database_logic_count': len(self.business_logic['database_logic']),
                'config_logic_count': len(self.business_logic['config_logic']),
                'api_endpoints_count': len(self.business_logic['api_endpoints']),
                'business_rules_count': len(self.business_logic['business_rules']),
                'most_common_category': max(category_stats.items(), key=lambda x: x[1])[0] if category_stats else 'none'
            }
        }
        
        return report

def main():
    """主函数"""
    root_dir = "/Users/a606/cloud_function_source"
    
    logger.info(f"开始PC28系统全面业务逻辑提取，根目录: {root_dir}")
    
    # 创建提取器
    extractor = ComprehensiveBusinessLogicExtractor(root_dir)
    
    # 执行提取
    report = extractor.extract_all_business_logic()
    
    # 保存报告
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"pc28_comprehensive_business_logic_report_{timestamp}.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"业务逻辑提取完成！")
    logger.info(f"总计提取 {report['statistics']['total_logic_items']} 个业务逻辑项")
    logger.info(f"扫描文件: {report['statistics']['total_files_scanned']} 个")
    logger.info(f"Python文件: {report['statistics']['python_files']} 个")
    logger.info(f"配置文件: {report['statistics']['config_files']} 个")
    logger.info(f"SQL文件: {report['statistics']['sql_files']} 个")
    logger.info(f"函数: {report['statistics']['functions_extracted']} 个")
    logger.info(f"类: {report['statistics']['classes_extracted']} 个")
    logger.info(f"API端点: {report['statistics']['api_endpoints']} 个")
    logger.info(f"业务规则: {report['statistics']['business_rules']} 个")
    logger.info(f"报告已保存到: {report_file}")
    
    return report_file

if __name__ == "__main__":
    main()