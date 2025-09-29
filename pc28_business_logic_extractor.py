#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28业务逻辑提取器
全面提取系统中的所有业务逻辑，包括代码逻辑和数据库中的业务逻辑
为统一优化做准备
"""

import os
import json
import logging
import ast
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set
from pathlib import Path
from google.cloud import bigquery
import pandas as pd

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BusinessLogicExtractor:
    """业务逻辑提取器"""
    
    def __init__(self, base_path: str = "."):
        self.base_path = Path(base_path)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 业务逻辑分类
        self.business_logic = {
            "lottery_logic": [],           # 彩票逻辑
            "betting_logic": [],           # 投注逻辑
            "payout_logic": [],            # 赔付逻辑
            "risk_management": [],         # 风险管理
            "data_processing": [],         # 数据处理
            "validation_rules": [],        # 验证规则
            "calculation_formulas": [],    # 计算公式
            "business_rules": [],          # 业务规则
            "workflow_logic": [],          # 工作流逻辑
            "integration_logic": []        # 集成逻辑
        }
        
        # 数据库业务逻辑
        self.database_logic = {
            "table_relationships": [],     # 表关系
            "business_constraints": [],    # 业务约束
            "calculated_fields": [],       # 计算字段
            "triggers_procedures": [],     # 触发器和存储过程
            "views_logic": [],            # 视图逻辑
            "indexes_optimization": []     # 索引优化
        }
        
        # 初始化BigQuery客户端
        try:
            self.bq_client = bigquery.Client()
            logger.info("✅ BigQuery客户端初始化成功")
        except Exception as e:
            logger.warning(f"⚠️ BigQuery客户端初始化失败: {e}")
            self.bq_client = None
    
    def extract_all_business_logic(self) -> Dict[str, Any]:
        """提取所有业务逻辑"""
        logger.info("🔍 开始提取PC28系统的所有业务逻辑...")
        
        # 1. 提取代码中的业务逻辑
        self._extract_code_business_logic()
        
        # 2. 提取数据库中的业务逻辑
        self._extract_database_business_logic()
        
        # 3. 分析业务逻辑关系
        relationships = self._analyze_business_relationships()
        
        # 4. 识别优化机会
        optimization_opportunities = self._identify_optimization_opportunities()
        
        # 5. 生成业务逻辑报告
        extraction_report = {
            "extraction_metadata": {
                "timestamp": self.timestamp,
                "base_path": str(self.base_path),
                "extraction_scope": "comprehensive"
            },
            "code_business_logic": self.business_logic,
            "database_business_logic": self.database_logic,
            "business_relationships": relationships,
            "optimization_opportunities": optimization_opportunities,
            "unified_optimization_plan": self._create_unified_optimization_plan()
        }
        
        return extraction_report
    
    def _extract_code_business_logic(self):
        """提取代码中的业务逻辑"""
        logger.info("📝 提取代码业务逻辑...")
        
        # 业务逻辑关键词映射
        business_keywords = {
            "lottery_logic": [
                "lottery", "draw", "winning", "number", "result", "prize",
                "jackpot", "combination", "random", "generate"
            ],
            "betting_logic": [
                "bet", "wager", "stake", "odds", "prediction", "choice",
                "selection", "ticket", "entry", "submit"
            ],
            "payout_logic": [
                "payout", "payment", "reward", "prize", "winning", "amount",
                "calculate", "distribution", "settlement", "commission"
            ],
            "risk_management": [
                "risk", "limit", "threshold", "validation", "check", "verify",
                "security", "fraud", "detection", "monitoring"
            ],
            "data_processing": [
                "process", "transform", "aggregate", "filter", "sort",
                "group", "merge", "join", "update", "sync"
            ],
            "validation_rules": [
                "validate", "verify", "check", "rule", "constraint", "format",
                "pattern", "required", "optional", "condition"
            ],
            "calculation_formulas": [
                "calculate", "compute", "formula", "algorithm", "math",
                "sum", "average", "percentage", "ratio", "rate"
            ],
            "business_rules": [
                "rule", "policy", "condition", "requirement", "specification",
                "criteria", "standard", "guideline", "regulation"
            ],
            "workflow_logic": [
                "workflow", "process", "step", "stage", "phase", "sequence",
                "order", "flow", "pipeline", "chain"
            ],
            "integration_logic": [
                "integration", "api", "service", "client", "connection",
                "interface", "endpoint", "request", "response"
            ]
        }
        
        # 扫描Python文件
        python_files = list(self.base_path.rglob("*.py"))
        
        # 排除目录
        exclude_dirs = {
            "venv", "env", ".venv", ".env", "node_modules", 
            "__pycache__", ".git", "site-packages", "dist-packages"
        }
        
        for file_path in python_files:
            # 检查是否在排除目录中
            should_exclude = False
            for part in file_path.parts:
                if part in exclude_dirs or part.startswith('.'):
                    should_exclude = True
                    break
            
            if should_exclude:
                continue
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # 解析AST
                try:
                    tree = ast.parse(content)
                    self._analyze_ast_for_business_logic(tree, file_path, content, business_keywords)
                except SyntaxError:
                    # 如果AST解析失败，使用正则表达式分析
                    self._analyze_content_for_business_logic(content, file_path, business_keywords)
                
            except Exception as e:
                logger.warning(f"分析文件失败 {file_path}: {e}")
    
    def _analyze_ast_for_business_logic(self, tree: ast.AST, file_path: Path, content: str, keywords: Dict[str, List[str]]):
        """通过AST分析业务逻辑"""
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_info = {
                    "type": "function",
                    "name": node.name,
                    "file_path": str(file_path),
                    "line_number": node.lineno,
                    "docstring": ast.get_docstring(node),
                    "parameters": [arg.arg for arg in node.args.args],
                    "business_indicators": []
                }
                
                # 分析函数名和文档字符串
                func_text = (node.name + " " + (func_info["docstring"] or "")).lower()
                
                # 获取函数体代码
                func_lines = content.split('\n')[node.lineno-1:node.end_lineno if hasattr(node, 'end_lineno') else node.lineno+10]
                func_body = '\n'.join(func_lines).lower()
                
                # 分类业务逻辑
                for category, category_keywords in keywords.items():
                    matches = []
                    for keyword in category_keywords:
                        if keyword in func_text or keyword in func_body:
                            matches.append(keyword)
                    
                    if matches:
                        func_info["business_indicators"] = matches
                        func_info["category"] = category
                        func_info["confidence"] = len(matches) / len(category_keywords)
                        self.business_logic[category].append(func_info)
                        break
            
            elif isinstance(node, ast.ClassDef):
                class_info = {
                    "type": "class",
                    "name": node.name,
                    "file_path": str(file_path),
                    "line_number": node.lineno,
                    "docstring": ast.get_docstring(node),
                    "methods": [],
                    "business_indicators": []
                }
                
                # 分析类名和文档字符串
                class_text = (node.name + " " + (class_info["docstring"] or "")).lower()
                
                # 分类业务逻辑
                for category, category_keywords in keywords.items():
                    matches = []
                    for keyword in category_keywords:
                        if keyword in class_text:
                            matches.append(keyword)
                    
                    if matches:
                        class_info["business_indicators"] = matches
                        class_info["category"] = category
                        class_info["confidence"] = len(matches) / len(category_keywords)
                        
                        # 分析类中的方法
                        for item in node.body:
                            if isinstance(item, ast.FunctionDef):
                                method_info = {
                                    "name": item.name,
                                    "line_number": item.lineno,
                                    "docstring": ast.get_docstring(item)
                                }
                                class_info["methods"].append(method_info)
                        
                        self.business_logic[category].append(class_info)
                        break
    
    def _analyze_content_for_business_logic(self, content: str, file_path: Path, keywords: Dict[str, List[str]]):
        """通过内容分析业务逻辑（备用方法）"""
        
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            
            # 跳过注释和空行
            if not line_lower or line_lower.startswith('#'):
                continue
            
            # 检查是否包含业务逻辑关键词
            for category, category_keywords in keywords.items():
                matches = []
                for keyword in category_keywords:
                    if keyword in line_lower:
                        matches.append(keyword)
                
                if matches:
                    logic_info = {
                        "type": "code_line",
                        "content": line.strip(),
                        "file_path": str(file_path),
                        "line_number": i + 1,
                        "business_indicators": matches,
                        "category": category,
                        "confidence": len(matches) / len(category_keywords)
                    }
                    
                    self.business_logic[category].append(logic_info)
    
    def _extract_database_business_logic(self):
        """提取数据库中的业务逻辑"""
        logger.info("🗄️ 提取数据库业务逻辑...")
        
        if not self.bq_client:
            logger.warning("⚠️ 无法连接BigQuery，跳过数据库业务逻辑提取")
            return
        
        try:
            # 获取所有数据集和表
            datasets = list(self.bq_client.list_datasets())
            
            for dataset in datasets:
                dataset_id = dataset.dataset_id
                logger.info(f"📊 分析数据集: {dataset_id}")
                
                # 获取数据集中的所有表
                tables = list(self.bq_client.list_tables(dataset_id))
                
                for table in tables:
                    table_id = table.table_id
                    full_table_id = f"{dataset_id}.{table_id}"
                    
                    try:
                        # 获取表结构
                        table_ref = self.bq_client.get_table(full_table_id)
                        
                        # 分析表的业务逻辑
                        self._analyze_table_business_logic(table_ref, full_table_id)
                        
                    except Exception as e:
                        logger.warning(f"分析表失败 {full_table_id}: {e}")
        
        except Exception as e:
            logger.error(f"提取数据库业务逻辑失败: {e}")
    
    def _analyze_table_business_logic(self, table_ref, full_table_id: str):
        """分析单个表的业务逻辑"""
        
        table_info = {
            "table_id": full_table_id,
            "table_name": table_ref.table_id,
            "description": table_ref.description,
            "num_rows": table_ref.num_rows,
            "num_bytes": table_ref.num_bytes,
            "created": table_ref.created.isoformat() if table_ref.created else None,
            "modified": table_ref.modified.isoformat() if table_ref.modified else None,
            "fields": [],
            "business_indicators": []
        }
        
        # 分析字段
        for field in table_ref.schema:
            field_info = {
                "name": field.name,
                "field_type": field.field_type,
                "mode": field.mode,
                "description": field.description,
                "business_meaning": self._infer_business_meaning(field.name, field.description)
            }
            table_info["fields"].append(field_info)
        
        # 根据表名和字段推断业务逻辑类型
        table_name_lower = table_ref.table_id.lower()
        
        # 业务逻辑分类
        if any(keyword in table_name_lower for keyword in ["lottery", "draw", "winning", "result"]):
            table_info["business_category"] = "lottery_logic"
            self.database_logic["table_relationships"].append(table_info)
        elif any(keyword in table_name_lower for keyword in ["bet", "wager", "stake", "ticket"]):
            table_info["business_category"] = "betting_logic"
            self.database_logic["table_relationships"].append(table_info)
        elif any(keyword in table_name_lower for keyword in ["payout", "payment", "reward", "prize"]):
            table_info["business_category"] = "payout_logic"
            self.database_logic["table_relationships"].append(table_info)
        elif any(keyword in table_name_lower for keyword in ["user", "customer", "account", "profile"]):
            table_info["business_category"] = "user_management"
            self.database_logic["table_relationships"].append(table_info)
        elif any(keyword in table_name_lower for keyword in ["log", "audit", "history", "track"]):
            table_info["business_category"] = "audit_tracking"
            self.database_logic["table_relationships"].append(table_info)
        else:
            table_info["business_category"] = "general"
            self.database_logic["table_relationships"].append(table_info)
        
        # 分析计算字段
        calculated_fields = []
        for field in table_info["fields"]:
            if any(keyword in field["name"].lower() for keyword in ["calc", "computed", "derived", "total", "sum", "avg"]):
                calculated_fields.append({
                    "table_id": full_table_id,
                    "field_name": field["name"],
                    "field_type": field["field_type"],
                    "business_meaning": field["business_meaning"]
                })
        
        if calculated_fields:
            self.database_logic["calculated_fields"].extend(calculated_fields)
    
    def _infer_business_meaning(self, field_name: str, field_description: str) -> str:
        """推断字段的业务含义"""
        
        field_name_lower = field_name.lower()
        description_lower = (field_description or "").lower()
        
        # 业务含义映射
        business_meanings = {
            "user_id": "用户标识",
            "customer_id": "客户标识", 
            "bet_id": "投注标识",
            "lottery_id": "彩票标识",
            "amount": "金额",
            "balance": "余额",
            "timestamp": "时间戳",
            "status": "状态",
            "result": "结果",
            "winning": "中奖",
            "payout": "赔付",
            "odds": "赔率",
            "commission": "佣金",
            "risk": "风险",
            "limit": "限制"
        }
        
        # 匹配业务含义
        for keyword, meaning in business_meanings.items():
            if keyword in field_name_lower or keyword in description_lower:
                return meaning
        
        return "未知业务含义"
    
    def _analyze_business_relationships(self) -> Dict[str, Any]:
        """分析业务逻辑关系"""
        logger.info("🔗 分析业务逻辑关系...")
        
        relationships = {
            "code_to_database": [],
            "cross_category": [],
            "dependency_chains": [],
            "integration_points": []
        }
        
        # 分析代码与数据库的关系
        for category, code_logic in self.business_logic.items():
            for logic_item in code_logic:
                # 检查代码中是否引用了数据库表
                for db_table in self.database_logic["table_relationships"]:
                    table_name = db_table["table_name"].lower()
                    
                    # 检查函数名、类名或内容中是否包含表名
                    logic_content = ""
                    if logic_item["type"] == "function":
                        logic_content = logic_item["name"].lower()
                    elif logic_item["type"] == "class":
                        logic_content = logic_item["name"].lower()
                    elif logic_item["type"] == "code_line":
                        logic_content = logic_item["content"].lower()
                    
                    if table_name in logic_content or any(part in logic_content for part in table_name.split("_")):
                        relationships["code_to_database"].append({
                            "code_logic": logic_item,
                            "database_table": db_table,
                            "relationship_type": "references"
                        })
        
        return relationships
    
    def _identify_optimization_opportunities(self) -> Dict[str, Any]:
        """识别优化机会"""
        logger.info("🎯 识别优化机会...")
        
        opportunities = {
            "redundant_logic": [],
            "performance_bottlenecks": [],
            "data_optimization": [],
            "code_consolidation": [],
            "unified_improvements": []
        }
        
        # 识别冗余逻辑
        logic_signatures = {}
        for category, logic_items in self.business_logic.items():
            for item in logic_items:
                # 创建逻辑签名
                signature = self._create_logic_signature(item)
                
                if signature in logic_signatures:
                    opportunities["redundant_logic"].append({
                        "signature": signature,
                        "original": logic_signatures[signature],
                        "duplicate": item,
                        "optimization_potential": "high"
                    })
                else:
                    logic_signatures[signature] = item
        
        # 识别数据优化机会
        table_analysis = {}
        for table in self.database_logic["table_relationships"]:
            table_size = table.get("num_bytes", 0)
            table_rows = table.get("num_rows", 0)
            
            if table_size > 1000000000:  # 1GB以上的表
                opportunities["data_optimization"].append({
                    "table_id": table["table_id"],
                    "size_bytes": table_size,
                    "num_rows": table_rows,
                    "optimization_type": "large_table_optimization",
                    "priority": "high"
                })
        
        # 识别性能瓶颈
        for category, logic_items in self.business_logic.items():
            if len(logic_items) > 20:  # 逻辑过多的类别
                opportunities["performance_bottlenecks"].append({
                    "category": category,
                    "logic_count": len(logic_items),
                    "optimization_type": "logic_consolidation",
                    "priority": "medium"
                })
        
        return opportunities
    
    def _create_logic_signature(self, logic_item: Dict[str, Any]) -> str:
        """创建逻辑签名用于识别重复"""
        
        if logic_item["type"] == "function":
            return f"func_{logic_item['name']}_{len(logic_item.get('parameters', []))}"
        elif logic_item["type"] == "class":
            return f"class_{logic_item['name']}_{len(logic_item.get('methods', []))}"
        elif logic_item["type"] == "code_line":
            # 简化代码行，移除变量名
            simplified = re.sub(r'\b\w+\s*=', 'var=', logic_item["content"])
            return f"line_{hash(simplified) % 10000}"
        
        return "unknown"
    
    def _create_unified_optimization_plan(self) -> Dict[str, Any]:
        """创建统一优化计划"""
        logger.info("📋 创建统一优化计划...")
        
        plan = {
            "optimization_phases": [
                {
                    "phase": 1,
                    "name": "代码逻辑优化",
                    "description": "优化重复和冗余的业务逻辑代码",
                    "priority": "high",
                    "estimated_impact": "medium"
                },
                {
                    "phase": 2,
                    "name": "数据库结构优化",
                    "description": "优化表结构、索引和查询性能",
                    "priority": "high", 
                    "estimated_impact": "high"
                },
                {
                    "phase": 3,
                    "name": "业务流程整合",
                    "description": "整合相关业务逻辑，减少重复处理",
                    "priority": "medium",
                    "estimated_impact": "medium"
                },
                {
                    "phase": 4,
                    "name": "性能监控优化",
                    "description": "建立性能监控和自动优化机制",
                    "priority": "medium",
                    "estimated_impact": "low"
                }
            ],
            "optimization_targets": {
                "code_reduction": "减少20%的冗余代码",
                "performance_improvement": "提升30%的查询性能",
                "storage_optimization": "减少15%的存储空间",
                "maintenance_efficiency": "提升50%的维护效率"
            },
            "risk_mitigation": [
                "在优化前建立完整的测试基线",
                "分阶段执行优化，每阶段验证系统稳定性",
                "保留原始逻辑备份，支持快速回滚",
                "建立监控机制，实时跟踪优化效果"
            ]
        }
        
        return plan
    
    def save_business_logic_report(self, report: Dict[str, Any]):
        """保存业务逻辑报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存JSON报告
        json_file = f"pc28_business_logic_extraction_report_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 保存Markdown报告
        md_file = f"pc28_business_logic_extraction_report_{timestamp}.md"
        self._generate_markdown_report(report, md_file)
        
        logger.info(f"📄 业务逻辑提取报告已保存:")
        logger.info(f"  JSON: {json_file}")
        logger.info(f"  Markdown: {md_file}")
        
        return json_file, md_file
    
    def _generate_markdown_report(self, report: Dict[str, Any], file_path: str):
        """生成Markdown格式的报告"""
        
        metadata = report["extraction_metadata"]
        code_logic = report["code_business_logic"]
        db_logic = report["database_business_logic"]
        relationships = report["business_relationships"]
        opportunities = report["optimization_opportunities"]
        plan = report["unified_optimization_plan"]
        
        content = f"""# PC28业务逻辑提取报告

## 📊 提取概览

**提取时间**: {metadata['timestamp']}
**基础路径**: {metadata['base_path']}
**提取范围**: {metadata['extraction_scope']}

## 💼 代码业务逻辑

"""
        
        total_code_logic = sum(len(items) for items in code_logic.values())
        content += f"**总计**: {total_code_logic} 个业务逻辑项\n\n"
        
        for category, items in code_logic.items():
            if items:
                category_name = category.replace("_", " ").title()
                content += f"""
### {category_name}
**数量**: {len(items)}

**主要项目**:
"""
                for item in items[:5]:  # 只显示前5个
                    if item["type"] == "function":
                        content += f"- 函数: `{item['name']}` ({Path(item['file_path']).name}:{item['line_number']})\n"
                    elif item["type"] == "class":
                        content += f"- 类: `{item['name']}` ({Path(item['file_path']).name}:{item['line_number']})\n"
                    elif item["type"] == "code_line":
                        content += f"- 代码: `{item['content'][:50]}...` ({Path(item['file_path']).name}:{item['line_number']})\n"
                
                if len(items) > 5:
                    content += f"- ... 还有 {len(items) - 5} 个项目\n"
        
        content += f"""
## 🗄️ 数据库业务逻辑

**表关系**: {len(db_logic['table_relationships'])} 个表
**计算字段**: {len(db_logic['calculated_fields'])} 个字段

### 主要业务表
"""
        
        # 按业务类别分组显示表
        table_categories = {}
        for table in db_logic["table_relationships"]:
            category = table.get("business_category", "general")
            if category not in table_categories:
                table_categories[category] = []
            table_categories[category].append(table)
        
        for category, tables in table_categories.items():
            category_name = category.replace("_", " ").title()
            content += f"""
#### {category_name}
"""
            for table in tables[:5]:  # 只显示前5个
                size_mb = table.get("num_bytes", 0) / (1024 * 1024) if table.get("num_bytes") else 0
                content += f"- `{table['table_name']}`: {table.get('num_rows', 0):,} 行, {size_mb:.1f} MB\n"
            
            if len(tables) > 5:
                content += f"- ... 还有 {len(tables) - 5} 个表\n"
        
        content += f"""
## 🔗 业务逻辑关系

**代码-数据库关系**: {len(relationships['code_to_database'])} 个关联
**跨类别关系**: {len(relationships['cross_category'])} 个关联

## 🎯 优化机会

### 冗余逻辑
**发现**: {len(opportunities['redundant_logic'])} 个冗余项
"""
        
        for redundant in opportunities["redundant_logic"][:3]:
            content += f"- 签名: `{redundant['signature']}` (优化潜力: {redundant['optimization_potential']})\n"
        
        content += f"""
### 数据优化
**大表优化**: {len(opportunities['data_optimization'])} 个表需要优化
"""
        
        for data_opt in opportunities["data_optimization"][:3]:
            size_gb = data_opt["size_bytes"] / (1024 * 1024 * 1024)
            content += f"- `{data_opt['table_id']}`: {size_gb:.2f} GB, {data_opt['num_rows']:,} 行\n"
        
        content += f"""
### 性能瓶颈
**逻辑集中**: {len(opportunities['performance_bottlenecks'])} 个类别需要优化
"""
        
        for bottleneck in opportunities["performance_bottlenecks"]:
            content += f"- {bottleneck['category']}: {bottleneck['logic_count']} 个逻辑项\n"
        
        content += f"""
## 📋 统一优化计划

### 优化阶段
"""
        
        for phase in plan["optimization_phases"]:
            content += f"""
#### 阶段 {phase['phase']}: {phase['name']}
- **描述**: {phase['description']}
- **优先级**: {phase['priority']}
- **预期影响**: {phase['estimated_impact']}
"""
        
        content += f"""
### 优化目标
"""
        
        for target, description in plan["optimization_targets"].items():
            content += f"- **{target.replace('_', ' ').title()}**: {description}\n"
        
        content += f"""
### 风险缓解
"""
        
        for risk in plan["risk_mitigation"]:
            content += f"- {risk}\n"
        
        content += f"""
## 🚀 下一步行动

1. **运行基线测试** - 建立优化前的性能和功能基准
2. **执行阶段1优化** - 开始代码逻辑优化
3. **验证优化效果** - 确保系统稳定性和性能提升
4. **继续后续阶段** - 按计划执行数据库和流程优化
5. **建立监控机制** - 持续跟踪优化效果

---

**报告生成时间**: {datetime.now().isoformat()}
**版本**: 1.0
"""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

def main():
    """主函数"""
    print("💼 PC28业务逻辑提取器")
    print("=" * 60)
    print("🎯 目标：全面提取系统中的所有业务逻辑")
    print("📋 范围：代码逻辑 + 数据库逻辑 + 业务关系")
    print("🔧 用途：为统一优化提供完整的业务逻辑基础")
    print("=" * 60)
    
    extractor = BusinessLogicExtractor()
    
    try:
        # 提取所有业务逻辑
        extraction_report = extractor.extract_all_business_logic()
        
        # 保存报告
        json_file, md_file = extractor.save_business_logic_report(extraction_report)
        
        # 显示摘要
        print("\n" + "=" * 60)
        print("📊 业务逻辑提取摘要")
        print("=" * 60)
        
        code_logic = extraction_report["code_business_logic"]
        db_logic = extraction_report["database_business_logic"]
        opportunities = extraction_report["optimization_opportunities"]
        
        total_code_logic = sum(len(items) for items in code_logic.values())
        total_tables = len(db_logic["table_relationships"])
        total_opportunities = sum(len(items) for items in opportunities.values())
        
        print(f"\n💼 代码业务逻辑: {total_code_logic} 个")
        print(f"🗄️ 数据库表: {total_tables} 个")
        print(f"🎯 优化机会: {total_opportunities} 个")
        
        print(f"\n📊 代码逻辑分布:")
        for category, items in code_logic.items():
            if items:
                category_name = category.replace("_", " ").title()
                print(f"   {category_name}: {len(items)} 个")
        
        print(f"\n🗄️ 数据库逻辑:")
        print(f"   表关系: {len(db_logic['table_relationships'])} 个")
        print(f"   计算字段: {len(db_logic['calculated_fields'])} 个")
        
        print(f"\n🎯 主要优化机会:")
        print(f"   冗余逻辑: {len(opportunities['redundant_logic'])} 个")
        print(f"   数据优化: {len(opportunities['data_optimization'])} 个")
        print(f"   性能瓶颈: {len(opportunities['performance_bottlenecks'])} 个")
        
        print(f"\n📄 详细报告: {md_file}")
        print("\n🎉 业务逻辑提取完成！系统已准备好进行统一优化。")
        
    except Exception as e:
        logger.error(f"业务逻辑提取失败: {e}")
        raise

if __name__ == "__main__":
    main()