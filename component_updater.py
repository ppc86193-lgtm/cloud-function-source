#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统组件定期更新和优化机制
自动检测、更新和优化系统各个组件
"""

import sqlite3
import json
import logging
import threading
import time
import os
import hashlib
import subprocess
import shutil
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any, Callable, Tuple
from pathlib import Path
import importlib
import sys
import ast
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ComponentInfo:
    """组件信息"""
    component_id: str
    name: str
    version: str
    file_path: str
    dependencies: List[str]
    last_modified: datetime
    file_hash: str
    status: str  # 'active', 'inactive', 'deprecated', 'error'
    performance_score: float
    memory_usage: float
    cpu_usage: float
    error_count: int
    last_check: datetime

@dataclass
class UpdateRule:
    """更新规则"""
    rule_id: str
    rule_name: str
    component_pattern: str  # 正则表达式匹配组件
    check_interval: int  # 检查间隔（秒）
    update_condition: Dict[str, Any]  # 更新条件
    optimization_strategy: str  # 优化策略
    auto_apply: bool
    priority: int
    enabled: bool
    description: str

@dataclass
class OptimizationTask:
    """优化任务"""
    task_id: str
    component_id: str
    task_type: str  # 'update', 'optimize', 'refactor', 'cleanup'
    description: str
    estimated_impact: float
    risk_level: str  # 'low', 'medium', 'high'
    created_at: datetime
    scheduled_at: Optional[datetime]
    completed_at: Optional[datetime]
    status: str  # 'pending', 'running', 'completed', 'failed', 'cancelled'
    result: Optional[str]
    backup_path: Optional[str]

@dataclass
class PerformanceMetric:
    """性能指标"""
    component_id: str
    timestamp: datetime
    execution_time: float
    memory_usage: float
    cpu_usage: float
    error_rate: float
    throughput: float
    response_time: float
    success_rate: float

class ComponentUpdater:
    """系统组件定期更新和优化机制"""
    
    def __init__(self, db_path: str = "component_updater.db", project_root: str = "."):
        self.db_path = db_path
        self.project_root = Path(project_root).resolve()
        self.update_active = False
        self.check_interval = 3600  # 1小时检查一次
        self.updater_thread = None
        
        # 组件信息缓存
        self.components = {}
        
        # 更新规则
        self.update_rules = {}
        
        # 优化任务队列
        self.optimization_queue = []
        
        # 性能历史
        self.performance_history = {}
        
        # 备份目录
        self.backup_dir = self.project_root / "backups"
        self.backup_dir.mkdir(exist_ok=True)
        
        # 支持的文件类型
        self.supported_extensions = {".py", ".js", ".ts", ".json", ".yaml", ".yml", ".sql"}
        
        # 优化策略
        self.optimization_strategies = {
            'performance': self._optimize_performance,
            'memory': self._optimize_memory,
            'code_quality': self._optimize_code_quality,
            'security': self._optimize_security,
            'maintainability': self._optimize_maintainability
        }
        
        self._init_database()
        self._setup_default_rules()
        self._scan_components()
    
    def _init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            # 组件信息表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS components (
                    component_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    dependencies TEXT DEFAULT '[]',
                    last_modified DATETIME NOT NULL,
                    file_hash TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    performance_score REAL DEFAULT 0.0,
                    memory_usage REAL DEFAULT 0.0,
                    cpu_usage REAL DEFAULT 0.0,
                    error_count INTEGER DEFAULT 0,
                    last_check DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 更新规则表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS update_rules (
                    rule_id TEXT PRIMARY KEY,
                    rule_name TEXT NOT NULL,
                    component_pattern TEXT NOT NULL,
                    check_interval INTEGER NOT NULL,
                    update_condition TEXT NOT NULL,
                    optimization_strategy TEXT NOT NULL,
                    auto_apply BOOLEAN DEFAULT 0,
                    priority INTEGER DEFAULT 5,
                    enabled BOOLEAN DEFAULT 1,
                    description TEXT
                )
            """)
            
            # 优化任务表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS optimization_tasks (
                    task_id TEXT PRIMARY KEY,
                    component_id TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    description TEXT NOT NULL,
                    estimated_impact REAL DEFAULT 0.0,
                    risk_level TEXT DEFAULT 'medium',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    scheduled_at DATETIME,
                    completed_at DATETIME,
                    status TEXT DEFAULT 'pending',
                    result TEXT,
                    backup_path TEXT
                )
            """)
            
            # 性能指标表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    component_id TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    execution_time REAL DEFAULT 0.0,
                    memory_usage REAL DEFAULT 0.0,
                    cpu_usage REAL DEFAULT 0.0,
                    error_rate REAL DEFAULT 0.0,
                    throughput REAL DEFAULT 0.0,
                    response_time REAL DEFAULT 0.0,
                    success_rate REAL DEFAULT 1.0
                )
            """)
            
            conn.commit()
    
    def _setup_default_rules(self):
        """设置默认更新规则"""
        default_rules = [
            UpdateRule(
                rule_id="python_performance",
                rule_name="Python性能优化",
                component_pattern=r".*\.py$",
                check_interval=3600,
                update_condition={
                    "performance_threshold": 0.7,
                    "error_rate_threshold": 0.05,
                    "memory_usage_threshold": 100.0
                },
                optimization_strategy="performance",
                auto_apply=False,
                priority=1,
                enabled=True,
                description="检测Python文件的性能问题并提供优化建议"
            ),
            UpdateRule(
                rule_id="memory_optimization",
                rule_name="内存使用优化",
                component_pattern=r".*\.(py|js|ts)$",
                check_interval=7200,
                update_condition={
                    "memory_usage_threshold": 200.0,
                    "memory_growth_rate": 0.1
                },
                optimization_strategy="memory",
                auto_apply=False,
                priority=2,
                enabled=True,
                description="优化内存使用过高的组件"
            ),
            UpdateRule(
                rule_id="code_quality",
                rule_name="代码质量检查",
                component_pattern=r".*\.(py|js|ts)$",
                check_interval=86400,
                update_condition={
                    "complexity_threshold": 10,
                    "duplication_threshold": 0.3
                },
                optimization_strategy="code_quality",
                auto_apply=False,
                priority=3,
                enabled=True,
                description="检查代码质量并提供改进建议"
            ),
            UpdateRule(
                rule_id="security_check",
                rule_name="安全检查",
                component_pattern=r".*\.(py|js|ts|sql)$",
                check_interval=43200,
                update_condition={
                    "security_score_threshold": 0.8
                },
                optimization_strategy="security",
                auto_apply=False,
                priority=0,
                enabled=True,
                description="检查安全漏洞和风险"
            ),
            UpdateRule(
                rule_id="dependency_update",
                rule_name="依赖更新检查",
                component_pattern=r".*\.(py|json|yaml|yml)$",
                check_interval=604800,  # 一周
                update_condition={
                    "outdated_threshold": 30  # 30天
                },
                optimization_strategy="maintainability",
                auto_apply=False,
                priority=4,
                enabled=True,
                description="检查过时的依赖并建议更新"
            )
        ]
        
        for rule in default_rules:
            self.add_update_rule(rule)
    
    def add_update_rule(self, rule: UpdateRule):
        """添加更新规则"""
        self.update_rules[rule.rule_id] = rule
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO update_rules 
                (rule_id, rule_name, component_pattern, check_interval, update_condition,
                 optimization_strategy, auto_apply, priority, enabled, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.rule_id, rule.rule_name, rule.component_pattern, rule.check_interval,
                json.dumps(rule.update_condition), rule.optimization_strategy,
                rule.auto_apply, rule.priority, rule.enabled, rule.description
            ))
            conn.commit()
    
    def _scan_components(self):
        """扫描项目组件"""
        logger.info("开始扫描项目组件...")
        
        for file_path in self.project_root.rglob("*"):
            if file_path.is_file() and file_path.suffix in self.supported_extensions:
                # 跳过备份目录和隐藏文件
                if any(part.startswith('.') for part in file_path.parts):
                    continue
                if 'backup' in str(file_path).lower():
                    continue
                
                component_info = self._analyze_component(file_path)
                if component_info:
                    self.components[component_info.component_id] = component_info
                    self._save_component(component_info)
        
        logger.info(f"扫描完成，发现 {len(self.components)} 个组件")
    
    def _analyze_component(self, file_path: Path) -> Optional[ComponentInfo]:
        """分析组件"""
        try:
            # 计算文件哈希
            with open(file_path, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
            
            # 获取文件信息
            stat = file_path.stat()
            last_modified = datetime.fromtimestamp(stat.st_mtime)
            
            # 分析依赖
            dependencies = self._extract_dependencies(file_path)
            
            # 生成组件ID
            relative_path = file_path.relative_to(self.project_root)
            component_id = str(relative_path).replace(os.sep, '_').replace('.', '_')
            
            # 检测版本
            version = self._extract_version(file_path)
            
            # 初始性能指标
            performance_score = self._calculate_initial_performance_score(file_path)
            
            return ComponentInfo(
                component_id=component_id,
                name=file_path.name,
                version=version,
                file_path=str(file_path),
                dependencies=dependencies,
                last_modified=last_modified,
                file_hash=file_hash,
                status='active',
                performance_score=performance_score,
                memory_usage=0.0,
                cpu_usage=0.0,
                error_count=0,
                last_check=datetime.now()
            )
        
        except Exception as e:
            logger.error(f"分析组件 {file_path} 时出错: {e}")
            return None
    
    def _extract_dependencies(self, file_path: Path) -> List[str]:
        """提取依赖"""
        dependencies = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if file_path.suffix == '.py':
                # Python导入分析
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            dependencies.append(alias.name)
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            dependencies.append(node.module)
            
            elif file_path.suffix in {'.js', '.ts'}:
                # JavaScript/TypeScript导入分析
                import_pattern = r'(?:import|require)\s*\(?[\'"]([^\'"]*)[\'"]'
                matches = re.findall(import_pattern, content)
                dependencies.extend(matches)
            
            elif file_path.suffix == '.json':
                # JSON依赖分析（如package.json）
                try:
                    data = json.loads(content)
                    if 'dependencies' in data:
                        dependencies.extend(data['dependencies'].keys())
                    if 'devDependencies' in data:
                        dependencies.extend(data['devDependencies'].keys())
                except json.JSONDecodeError:
                    pass
        
        except Exception as e:
            logger.debug(f"提取依赖时出错 {file_path}: {e}")
        
        return list(set(dependencies))  # 去重
    
    def _extract_version(self, file_path: Path) -> str:
        """提取版本信息"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 查找版本模式
            version_patterns = [
                r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                r'version\s*=\s*[\'"]([^\'"]*)[\'"]',
                r'VERSION\s*=\s*[\'"]([^\'"]*)[\'"]',
                r'"version"\s*:\s*"([^"]*)"'
            ]
            
            for pattern in version_patterns:
                match = re.search(pattern, content)
                if match:
                    return match.group(1)
        
        except Exception:
            pass
        
        return "1.0.0"  # 默认版本
    
    def _calculate_initial_performance_score(self, file_path: Path) -> float:
        """计算初始性能分数"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 基于文件大小和复杂度的简单评分
            file_size = len(content)
            line_count = content.count('\n')
            
            # 基础分数
            score = 1.0
            
            # 文件大小惩罚
            if file_size > 10000:  # 10KB
                score -= 0.1
            if file_size > 50000:  # 50KB
                score -= 0.2
            
            # 行数惩罚
            if line_count > 500:
                score -= 0.1
            if line_count > 1000:
                score -= 0.2
            
            # Python特定分析
            if file_path.suffix == '.py':
                # 检查复杂度指标
                if 'for ' in content:
                    score -= content.count('for ') * 0.01
                if 'while ' in content:
                    score -= content.count('while ') * 0.02
                if 'if ' in content:
                    score -= content.count('if ') * 0.005
            
            return max(score, 0.1)  # 最低0.1分
        
        except Exception:
            return 0.5  # 默认分数
    
    def _save_component(self, component: ComponentInfo):
        """保存组件信息"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO components 
                (component_id, name, version, file_path, dependencies, last_modified,
                 file_hash, status, performance_score, memory_usage, cpu_usage,
                 error_count, last_check)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                component.component_id, component.name, component.version, component.file_path,
                json.dumps(component.dependencies), component.last_modified, component.file_hash,
                component.status, component.performance_score, component.memory_usage,
                component.cpu_usage, component.error_count, component.last_check
            ))
            conn.commit()
    
    def check_for_updates(self) -> List[OptimizationTask]:
        """检查更新"""
        tasks = []
        
        for rule in self.update_rules.values():
            if not rule.enabled:
                continue
            
            # 匹配组件
            pattern = re.compile(rule.component_pattern)
            matching_components = [
                comp for comp in self.components.values()
                if pattern.match(comp.file_path)
            ]
            
            for component in matching_components:
                # 检查是否需要更新
                if self._should_update_component(component, rule):
                    task = self._create_optimization_task(component, rule)
                    if task:
                        tasks.append(task)
        
        return tasks
    
    def _should_update_component(self, component: ComponentInfo, rule: UpdateRule) -> bool:
        """判断是否需要更新组件"""
        condition = rule.update_condition
        
        # 检查性能阈值
        if 'performance_threshold' in condition:
            if component.performance_score < condition['performance_threshold']:
                return True
        
        # 检查错误率
        if 'error_rate_threshold' in condition:
            # 简化的错误率计算
            error_rate = component.error_count / max(1, component.error_count + 100)
            if error_rate > condition['error_rate_threshold']:
                return True
        
        # 检查内存使用
        if 'memory_usage_threshold' in condition:
            if component.memory_usage > condition['memory_usage_threshold']:
                return True
        
        # 检查文件修改时间
        if 'outdated_threshold' in condition:
            days_old = (datetime.now() - component.last_modified).days
            if days_old > condition['outdated_threshold']:
                return True
        
        return False
    
    def _create_optimization_task(self, component: ComponentInfo, rule: UpdateRule) -> Optional[OptimizationTask]:
        """创建优化任务"""
        task_id = f"{rule.rule_id}_{component.component_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 估算影响
        estimated_impact = self._estimate_optimization_impact(component, rule)
        
        # 评估风险
        risk_level = self._assess_risk_level(component, rule)
        
        task = OptimizationTask(
            task_id=task_id,
            component_id=component.component_id,
            task_type=rule.optimization_strategy,
            description=f"对组件 {component.name} 应用 {rule.rule_name}",
            estimated_impact=estimated_impact,
            risk_level=risk_level,
            created_at=datetime.now(),
            scheduled_at=None,
            completed_at=None,
            status='pending',
            result=None,
            backup_path=None
        )
        
        self._save_optimization_task(task)
        return task
    
    def _estimate_optimization_impact(self, component: ComponentInfo, rule: UpdateRule) -> float:
        """估算优化影响"""
        # 基于组件当前状态和规则类型估算影响
        base_impact = 0.1
        
        if rule.optimization_strategy == 'performance':
            base_impact = 1.0 - component.performance_score
        elif rule.optimization_strategy == 'memory':
            base_impact = min(component.memory_usage / 100.0, 0.5)
        elif rule.optimization_strategy == 'security':
            base_impact = 0.8  # 安全优化通常影响较大
        elif rule.optimization_strategy == 'code_quality':
            base_impact = 0.3
        elif rule.optimization_strategy == 'maintainability':
            base_impact = 0.2
        
        return min(base_impact, 1.0)
    
    def _assess_risk_level(self, component: ComponentInfo, rule: UpdateRule) -> str:
        """评估风险级别"""
        # 基于组件重要性和优化类型评估风险
        risk_factors = 0
        
        # 核心组件风险更高
        if 'main' in component.name.lower() or 'core' in component.name.lower():
            risk_factors += 2
        
        # 依赖多的组件风险更高
        if len(component.dependencies) > 10:
            risk_factors += 1
        
        # 某些优化类型风险更高
        if rule.optimization_strategy in ['security', 'performance']:
            risk_factors += 1
        
        if risk_factors >= 3:
            return 'high'
        elif risk_factors >= 1:
            return 'medium'
        else:
            return 'low'
    
    def _save_optimization_task(self, task: OptimizationTask):
        """保存优化任务"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO optimization_tasks 
                (task_id, component_id, task_type, description, estimated_impact,
                 risk_level, created_at, scheduled_at, completed_at, status, result, backup_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task.task_id, task.component_id, task.task_type, task.description,
                task.estimated_impact, task.risk_level, task.created_at,
                task.scheduled_at, task.completed_at, task.status, task.result, task.backup_path
            ))
            conn.commit()
    
    def execute_optimization_task(self, task_id: str) -> bool:
        """执行优化任务"""
        try:
            # 获取任务信息
            with sqlite3.connect(self.db_path) as conn:
                task_data = conn.execute(
                    "SELECT * FROM optimization_tasks WHERE task_id = ?",
                    (task_id,)
                ).fetchone()
                
                if not task_data:
                    logger.error(f"未找到优化任务: {task_id}")
                    return False
            
            # 构建任务对象
            task = OptimizationTask(
                task_id=task_data[0],
                component_id=task_data[1],
                task_type=task_data[2],
                description=task_data[3],
                estimated_impact=task_data[4],
                risk_level=task_data[5],
                created_at=datetime.fromisoformat(task_data[6]),
                scheduled_at=datetime.fromisoformat(task_data[7]) if task_data[7] else None,
                completed_at=datetime.fromisoformat(task_data[8]) if task_data[8] else None,
                status=task_data[9],
                result=task_data[10],
                backup_path=task_data[11]
            )
            
            # 获取组件信息
            component = self.components.get(task.component_id)
            if not component:
                logger.error(f"未找到组件: {task.component_id}")
                return False
            
            # 更新任务状态
            task.status = 'running'
            self._save_optimization_task(task)
            
            logger.info(f"开始执行优化任务: {task.description}")
            
            # 创建备份
            backup_path = self._create_backup(component)
            task.backup_path = backup_path
            
            # 执行优化策略
            strategy_func = self.optimization_strategies.get(task.task_type)
            if not strategy_func:
                raise ValueError(f"未知的优化策略: {task.task_type}")
            
            result = strategy_func(component, task)
            
            # 更新任务状态
            task.status = 'completed'
            task.completed_at = datetime.now()
            task.result = result
            self._save_optimization_task(task)
            
            logger.info(f"优化任务完成: {task.description}")
            logger.info(f"结果: {result}")
            
            return True
        
        except Exception as e:
            logger.error(f"执行优化任务时出错: {e}")
            
            # 更新任务状态为失败
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "UPDATE optimization_tasks SET status = 'failed', result = ? WHERE task_id = ?",
                        (str(e), task_id)
                    )
                    conn.commit()
            except Exception:
                pass
            
            return False
    
    def _create_backup(self, component: ComponentInfo) -> str:
        """创建备份"""
        source_path = Path(component.file_path)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"{component.component_id}_{timestamp}{source_path.suffix}"
        backup_path = self.backup_dir / backup_filename
        
        shutil.copy2(source_path, backup_path)
        logger.info(f"已创建备份: {backup_path}")
        
        return str(backup_path)
    
    def _optimize_performance(self, component: ComponentInfo, task: OptimizationTask) -> str:
        """性能优化策略"""
        suggestions = []
        
        try:
            with open(component.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if component.file_path.endswith('.py'):
                # Python性能优化建议
                if 'for i in range(len(' in content:
                    suggestions.append("建议使用enumerate()替代range(len())")
                
                if '+=' in content and 'str' in content:
                    suggestions.append("建议使用join()替代字符串连接")
                
                if 'import *' in content:
                    suggestions.append("避免使用import *，明确导入需要的模块")
                
                if content.count('def ') > 20:
                    suggestions.append("考虑将大文件拆分为多个模块")
            
            # 通用优化建议
            lines = content.split('\n')
            if len(lines) > 500:
                suggestions.append("文件过大，建议拆分为更小的模块")
            
            if len([line for line in lines if len(line) > 120]) > 10:
                suggestions.append("存在过长的代码行，建议重构")
        
        except Exception as e:
            suggestions.append(f"分析时出错: {e}")
        
        return "; ".join(suggestions) if suggestions else "未发现明显的性能问题"
    
    def _optimize_memory(self, component: ComponentInfo, task: OptimizationTask) -> str:
        """内存优化策略"""
        suggestions = []
        
        try:
            with open(component.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if component.file_path.endswith('.py'):
                # Python内存优化建议
                if 'global ' in content:
                    suggestions.append("减少全局变量的使用")
                
                if 'list(' in content and 'range(' in content:
                    suggestions.append("考虑使用生成器替代列表")
                
                if 'cache' in content.lower():
                    suggestions.append("检查缓存策略，避免内存泄漏")
                
                if content.count('[]') > 10:
                    suggestions.append("考虑使用更高效的数据结构")
        
        except Exception as e:
            suggestions.append(f"分析时出错: {e}")
        
        return "; ".join(suggestions) if suggestions else "未发现明显的内存问题"
    
    def _optimize_code_quality(self, component: ComponentInfo, task: OptimizationTask) -> str:
        """代码质量优化策略"""
        suggestions = []
        
        try:
            with open(component.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            # 检查代码质量指标
            if len([line for line in lines if line.strip().startswith('#')]) / len(lines) < 0.1:
                suggestions.append("增加代码注释")
            
            if 'TODO' in content or 'FIXME' in content:
                suggestions.append("处理待办事项和修复标记")
            
            if content.count('try:') != content.count('except'):
                suggestions.append("检查异常处理的完整性")
            
            # 检查重复代码
            line_counts = {}
            for line in lines:
                stripped = line.strip()
                if len(stripped) > 10:  # 忽略短行
                    line_counts[stripped] = line_counts.get(stripped, 0) + 1
            
            duplicates = [line for line, count in line_counts.items() if count > 2]
            if duplicates:
                suggestions.append(f"发现 {len(duplicates)} 行重复代码，建议重构")
        
        except Exception as e:
            suggestions.append(f"分析时出错: {e}")
        
        return "; ".join(suggestions) if suggestions else "代码质量良好"
    
    def _optimize_security(self, component: ComponentInfo, task: OptimizationTask) -> str:
        """安全优化策略"""
        suggestions = []
        
        try:
            with open(component.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 安全检查
            security_patterns = [
                (r'password\s*=\s*[\'"][^\'"]*[\'"]', "避免硬编码密码"),
                (r'api_key\s*=\s*[\'"][^\'"]*[\'"]', "避免硬编码API密钥"),
                (r'eval\s*\(', "避免使用eval()函数"),
                (r'exec\s*\(', "避免使用exec()函数"),
                (r'shell=True', "谨慎使用shell=True参数"),
                (r'input\s*\(', "验证用户输入"),
                (r'pickle\.loads?\s*\(', "谨慎使用pickle反序列化")
            ]
            
            for pattern, message in security_patterns:
                if re.search(pattern, content, re.IGNORECASE):
                    suggestions.append(message)
            
            # SQL注入检查
            if 'sql' in content.lower() and '%s' in content:
                suggestions.append("使用参数化查询防止SQL注入")
        
        except Exception as e:
            suggestions.append(f"分析时出错: {e}")
        
        return "; ".join(suggestions) if suggestions else "未发现明显的安全问题"
    
    def _optimize_maintainability(self, component: ComponentInfo, task: OptimizationTask) -> str:
        """可维护性优化策略"""
        suggestions = []
        
        try:
            with open(component.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            # 可维护性检查
            if len(lines) > 1000:
                suggestions.append("文件过大，建议拆分")
            
            if component.file_path.endswith('.py'):
                # 检查函数长度
                in_function = False
                function_lines = 0
                max_function_lines = 0
                
                for line in lines:
                    if line.strip().startswith('def '):
                        if in_function and function_lines > max_function_lines:
                            max_function_lines = function_lines
                        in_function = True
                        function_lines = 0
                    elif in_function:
                        function_lines += 1
                
                if max_function_lines > 50:
                    suggestions.append("存在过长的函数，建议拆分")
            
            # 检查依赖复杂度
            if len(component.dependencies) > 20:
                suggestions.append("依赖过多，考虑重构")
            
            # 检查文档
            if '"""' not in content and "'''" not in content:
                suggestions.append("添加文档字符串")
        
        except Exception as e:
            suggestions.append(f"分析时出错: {e}")
        
        return "; ".join(suggestions) if suggestions else "可维护性良好"
    
    def start_auto_check(self):
        """启动自动检查"""
        if self.update_active:
            logger.warning("自动检查已在运行中")
            return
        
        self.update_active = True
        logger.info("启动组件自动检查服务")
        
        # 启动后台线程进行定期检查
        self.updater_thread = threading.Thread(target=self._auto_check_loop, daemon=True)
        self.updater_thread.start()
        
        logger.info("组件自动检查服务已启动")
    
    def stop_auto_check(self):
        """停止自动检查"""
        if not self.update_active:
            return
        
        self.update_active = False
        logger.info("停止组件自动检查服务")
        
        if self.updater_thread:
            self.updater_thread.join(timeout=5)
        
        logger.info("组件自动检查服务已停止")
    
    def _auto_check_loop(self):
        """自动检查循环"""
        while self.update_active:
            try:
                logger.info("执行组件自动检查...")
                
                # 扫描组件变化
                self._scan_components()
                
                # 检查更新
                updates = self.check_for_updates()
                if updates:
                    logger.info(f"发现 {len(updates)} 个组件需要优化")
                
                # 等待下次检查
                for _ in range(self.check_interval):
                    if not self.update_active:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"自动检查循环异常: {e}")
                time.sleep(60)  # 异常时等待1分钟后重试
    
    def start_auto_update(self):
        """启动自动更新"""
        if self.update_active:
            logger.warning("组件自动更新已经在运行中")
            return
        
        self.update_active = True
        self.updater_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.updater_thread.start()
        logger.info("组件自动更新已启动")
    
    def stop_auto_update(self):
        """停止自动更新"""
        self.update_active = False
        if self.updater_thread:
            self.updater_thread.join(timeout=5)
        logger.info("组件自动更新已停止")
    
    def _update_loop(self):
        """更新循环"""
        while self.update_active:
            try:
                logger.info("开始检查组件更新...")
                
                # 重新扫描组件
                self._scan_components()
                
                # 检查更新
                tasks = self.check_for_updates()
                
                logger.info(f"发现 {len(tasks)} 个优化任务")
                
                # 执行自动应用的任务
                for task in tasks:
                    if task.risk_level == 'low':  # 只自动执行低风险任务
                        logger.info(f"自动执行低风险任务: {task.description}")
                        self.execute_optimization_task(task.task_id)
                    else:
                        logger.info(f"高风险任务需要手动确认: {task.description}")
            
            except Exception as e:
                logger.error(f"更新循环中出错: {e}")
            
            # 等待下次检查
            for _ in range(self.check_interval):
                if not self.update_active:
                    break
                time.sleep(1)
    
    def get_component_status(self) -> Dict[str, Any]:
        """获取组件状态"""
        total_components = len(self.components)
        active_components = len([c for c in self.components.values() if c.status == 'active'])
        
        # 获取待处理任务
        with sqlite3.connect(self.db_path) as conn:
            pending_tasks = conn.execute(
                "SELECT COUNT(*) FROM optimization_tasks WHERE status = 'pending'"
            ).fetchone()[0]
            
            completed_tasks = conn.execute(
                "SELECT COUNT(*) FROM optimization_tasks WHERE status = 'completed'"
            ).fetchone()[0]
        
        # 计算平均性能分数
        avg_performance = sum(c.performance_score for c in self.components.values()) / max(total_components, 1)
        
        return {
            "total_components": total_components,
            "active_components": active_components,
            "pending_tasks": pending_tasks,
            "completed_tasks": completed_tasks,
            "average_performance_score": avg_performance,
            "update_active": self.update_active,
            "last_scan": datetime.now().isoformat()
        }
    
    def get_optimization_report(self, days: int = 7) -> Dict[str, Any]:
        """获取优化报告"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            # 获取最近的优化任务
            tasks = conn.execute("""
                SELECT task_type, status, estimated_impact, risk_level, created_at
                FROM optimization_tasks 
                WHERE created_at > ?
                ORDER BY created_at DESC
            """, (cutoff_date,)).fetchall()
            
            # 统计信息
            task_stats = {}
            total_impact = 0
            risk_distribution = {'low': 0, 'medium': 0, 'high': 0}
            status_distribution = {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0}
            
            for task in tasks:
                task_type, status, impact, risk, created = task
                
                task_stats[task_type] = task_stats.get(task_type, 0) + 1
                total_impact += impact
                risk_distribution[risk] += 1
                status_distribution[status] += 1
        
        return {
            "period_days": days,
            "total_tasks": len(tasks),
            "task_types": task_stats,
            "total_estimated_impact": total_impact,
            "risk_distribution": risk_distribution,
            "status_distribution": status_distribution,
            "generated_at": datetime.now().isoformat()
        }
    
    def cleanup_old_data(self, days: int = 30):
        """清理旧数据"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            # 清理旧的性能指标
            conn.execute("DELETE FROM performance_metrics WHERE timestamp < ?", (cutoff_date,))
            
            # 清理已完成的旧任务
            conn.execute(
                "DELETE FROM optimization_tasks WHERE completed_at < ? AND status = 'completed'",
                (cutoff_date,)
            )
            
            conn.commit()
        
        # 清理旧备份
        if self.backup_dir.exists():
            for backup_file in self.backup_dir.glob("*"):
                if backup_file.is_file():
                    file_age = datetime.now() - datetime.fromtimestamp(backup_file.stat().st_mtime)
                    if file_age.days > days:
                        backup_file.unlink()
                        logger.info(f"已删除旧备份: {backup_file}")
        
        logger.info(f"已清理 {days} 天前的旧数据")

def main():
    """测试系统组件定期更新和优化机制"""
    print("=== 系统组件定期更新和优化机制测试 ===")
    
    # 创建更新器实例
    updater = ComponentUpdater()
    
    # 获取组件状态
    print("\n组件状态:")
    status = updater.get_component_status()
    for key, value in status.items():
        print(f"  {key}: {value}")
    
    # 检查更新
    print("\n检查组件更新...")
    tasks = updater.check_for_updates()
    
    print(f"发现 {len(tasks)} 个优化任务:")
    for task in tasks[:5]:  # 显示前5个任务
        print(f"  - {task.description}")
        print(f"    类型: {task.task_type}, 风险: {task.risk_level}, 影响: {task.estimated_impact:.2%}")
    
    # 执行一个低风险任务（如果有的话）
    low_risk_tasks = [t for t in tasks if t.risk_level == 'low']
    if low_risk_tasks:
        print(f"\n执行低风险任务: {low_risk_tasks[0].description}")
        success = updater.execute_optimization_task(low_risk_tasks[0].task_id)
        print(f"执行结果: {'成功' if success else '失败'}")
    
    # 获取优化报告
    print("\n优化报告:")
    report = updater.get_optimization_report()
    for key, value in report.items():
        if isinstance(value, dict):
            print(f"  {key}:")
            for sub_key, sub_value in value.items():
                print(f"    {sub_key}: {sub_value}")
        else:
            print(f"  {key}: {value}")
    
    # 启动短期自动更新测试
    print("\n启动自动更新测试 (30秒)...")
    updater.check_interval = 15  # 15秒检查一次
    updater.start_auto_update()
    
    time.sleep(30)
    
    updater.stop_auto_update()
    
    # 清理测试数据
    print("\n清理旧数据...")
    updater.cleanup_old_data(days=0)  # 清理所有测试数据
    
    print("\n=== 系统组件定期更新和优化机制测试完成 ===")

if __name__ == "__main__":
    main()