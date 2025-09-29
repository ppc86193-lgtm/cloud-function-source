#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28优化基线创建器
基于提取的业务逻辑创建优化基线和测试保障
确保优化过程的安全性和可追溯性
"""

import os
import json
import logging
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import shutil

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizationBaselineCreator:
    """优化基线创建器"""
    
    def __init__(self, base_path: str = "."):
        self.base_path = Path(base_path)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 基线数据
        self.baseline_data = {
            "business_logic_baseline": {},
            "test_baseline": {},
            "performance_baseline": {},
            "data_baseline": {},
            "optimization_checkpoints": []
        }
        
        # 备份目录
        self.backup_dir = self.base_path / "optimization_baseline_backups" / self.timestamp
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
    def create_optimization_baseline(self) -> Dict[str, Any]:
        """创建优化基线"""
        logger.info("🎯 开始创建PC28系统优化基线...")
        
        # 1. 加载业务逻辑提取报告
        business_logic_data = self._load_business_logic_data()
        
        # 2. 创建业务逻辑基线
        self._create_business_logic_baseline(business_logic_data)
        
        # 3. 创建测试基线
        self._create_test_baseline()
        
        # 4. 创建性能基线
        self._create_performance_baseline()
        
        # 5. 创建数据基线
        self._create_data_baseline()
        
        # 6. 设置优化检查点
        self._setup_optimization_checkpoints()
        
        # 7. 创建备份
        self._create_system_backup()
        
        # 8. 生成基线报告
        baseline_report = {
            "baseline_metadata": {
                "timestamp": self.timestamp,
                "base_path": str(self.base_path),
                "backup_location": str(self.backup_dir)
            },
            "baseline_data": self.baseline_data,
            "optimization_readiness": self._assess_optimization_readiness(),
            "safety_measures": self._get_safety_measures()
        }
        
        return baseline_report
    
    def _load_business_logic_data(self) -> Dict[str, Any]:
        """加载业务逻辑提取数据"""
        logger.info("📊 加载业务逻辑提取数据...")
        
        # 查找最新的业务逻辑提取报告
        json_files = list(self.base_path.glob("pc28_business_logic_extraction_report_*.json"))
        
        if not json_files:
            logger.warning("⚠️ 未找到业务逻辑提取报告，将创建空基线")
            return {}
        
        # 选择最新的报告
        latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
        logger.info(f"📄 使用报告: {latest_file.name}")
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载业务逻辑数据失败: {e}")
            return {}
    
    def _create_business_logic_baseline(self, business_logic_data: Dict[str, Any]):
        """创建业务逻辑基线"""
        logger.info("💼 创建业务逻辑基线...")
        
        if not business_logic_data:
            logger.warning("⚠️ 业务逻辑数据为空，跳过基线创建")
            return
        
        code_logic = business_logic_data.get("code_business_logic", {})
        db_logic = business_logic_data.get("database_business_logic", {})
        
        # 统计业务逻辑基线
        self.baseline_data["business_logic_baseline"] = {
            "code_logic_counts": {
                category: len(items) for category, items in code_logic.items()
            },
            "total_code_logic": sum(len(items) for items in code_logic.values()),
            "database_tables": len(db_logic.get("table_relationships", [])),
            "calculated_fields": len(db_logic.get("calculated_fields", [])),
            "critical_logic_items": self._identify_critical_logic(code_logic),
            "optimization_targets": business_logic_data.get("optimization_opportunities", {})
        }
        
        logger.info(f"✅ 业务逻辑基线创建完成: {self.baseline_data['business_logic_baseline']['total_code_logic']} 个代码逻辑项")
    
    def _identify_critical_logic(self, code_logic: Dict[str, List[Dict]]) -> List[Dict[str, Any]]:
        """识别关键业务逻辑"""
        critical_items = []
        
        # 关键业务逻辑类别
        critical_categories = ["lottery_logic", "betting_logic", "payout_logic", "risk_management"]
        
        for category in critical_categories:
            items = code_logic.get(category, [])
            for item in items:
                if item.get("confidence", 0) > 0.7:  # 高置信度的逻辑
                    critical_items.append({
                        "category": category,
                        "name": item.get("name", "unknown"),
                        "file_path": item.get("file_path", ""),
                        "line_number": item.get("line_number", 0),
                        "confidence": item.get("confidence", 0),
                        "type": item.get("type", "unknown")
                    })
        
        return critical_items[:50]  # 限制为前50个最关键的
    
    def _create_test_baseline(self):
        """创建测试基线"""
        logger.info("🧪 创建测试基线...")
        
        # 查找测试报告
        test_reports = []
        
        # 查找逻辑测试提取报告
        logic_test_files = list(self.base_path.glob("pc28_logic_test_extraction_report_*.json"))
        if logic_test_files:
            latest_logic_test = max(logic_test_files, key=lambda f: f.stat().st_mtime)
            try:
                with open(latest_logic_test, 'r', encoding='utf-8') as f:
                    logic_test_data = json.load(f)
                    test_reports.append({
                        "type": "logic_tests",
                        "file": latest_logic_test.name,
                        "data": logic_test_data
                    })
            except Exception as e:
                logger.warning(f"加载逻辑测试报告失败: {e}")
        
        # 查找其他测试结果
        test_result_files = list(self.base_path.glob("test_results_*.json"))
        test_result_files.extend(list(self.base_path.glob("test_suite/test_results_*.json")))
        
        for test_file in test_result_files:
            try:
                with open(test_file, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
                    test_reports.append({
                        "type": "test_results",
                        "file": test_file.name,
                        "data": test_data
                    })
            except Exception as e:
                logger.warning(f"加载测试结果失败 {test_file}: {e}")
        
        # 统计测试基线
        total_tests = 0
        passed_tests = 0
        test_categories = {}
        
        for report in test_reports:
            if report["type"] == "logic_tests":
                data = report["data"]
                logic_tests = data.get("logic_test_classification", {})
                for category, tests in logic_tests.items():
                    test_categories[category] = len(tests)
                    total_tests += len(tests)
            elif report["type"] == "test_results":
                data = report["data"]
                if isinstance(data, dict) and "tests" in data:
                    tests = data["tests"]
                    for test in tests:
                        total_tests += 1
                        if test.get("status") == "passed":
                            passed_tests += 1
        
        self.baseline_data["test_baseline"] = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "test_success_rate": (passed_tests / total_tests * 100) if total_tests > 0 else 0,
            "test_categories": test_categories,
            "test_reports": [{"type": r["type"], "file": r["file"]} for r in test_reports]
        }
        
        logger.info(f"✅ 测试基线创建完成: {total_tests} 个测试，成功率 {self.baseline_data['test_baseline']['test_success_rate']:.1f}%")
    
    def _create_performance_baseline(self):
        """创建性能基线"""
        logger.info("⚡ 创建性能基线...")
        
        # 运行性能测试获取基线数据
        performance_data = {}
        
        try:
            # 运行简单的性能测试
            result = subprocess.run([
                "python", "-c", 
                "import time; start=time.time(); import main; print(f'Import time: {time.time()-start:.3f}s')"
            ], capture_output=True, text=True, timeout=30, cwd=self.base_path)
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if "Import time:" in output:
                    import_time = float(output.split(":")[1].replace("s", "").strip())
                    performance_data["main_import_time"] = import_time
            
        except Exception as e:
            logger.warning(f"性能基线测试失败: {e}")
        
        # 检查系统资源使用情况
        try:
            import psutil
            performance_data.update({
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_usage_percent": psutil.disk_usage('.').percent
            })
        except ImportError:
            logger.warning("psutil未安装，跳过系统资源监控")
        
        self.baseline_data["performance_baseline"] = {
            "timestamp": datetime.now().isoformat(),
            "metrics": performance_data,
            "benchmark_status": "baseline_established" if performance_data else "no_data"
        }
        
        logger.info(f"✅ 性能基线创建完成: {len(performance_data)} 个指标")
    
    def _create_data_baseline(self):
        """创建数据基线"""
        logger.info("🗄️ 创建数据基线...")
        
        # 统计文件和目录信息
        file_stats = {
            "python_files": len(list(self.base_path.rglob("*.py"))),
            "json_files": len(list(self.base_path.rglob("*.json"))),
            "sql_files": len(list(self.base_path.rglob("*.sql"))),
            "md_files": len(list(self.base_path.rglob("*.md"))),
            "total_files": len(list(self.base_path.rglob("*"))),
        }
        
        # 计算代码行数
        total_lines = 0
        python_lines = 0
        
        for py_file in self.base_path.rglob("*.py"):
            # 跳过虚拟环境和缓存目录
            if any(part in str(py_file) for part in ["venv", "__pycache__", ".git"]):
                continue
            
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    lines = len(f.readlines())
                    python_lines += lines
                    total_lines += lines
            except Exception:
                continue
        
        self.baseline_data["data_baseline"] = {
            "file_statistics": file_stats,
            "code_statistics": {
                "total_lines": total_lines,
                "python_lines": python_lines
            },
            "directory_structure": self._get_directory_structure()
        }
        
        logger.info(f"✅ 数据基线创建完成: {file_stats['python_files']} 个Python文件，{python_lines} 行代码")
    
    def _get_directory_structure(self) -> Dict[str, int]:
        """获取目录结构统计"""
        structure = {}
        
        for item in self.base_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                file_count = len(list(item.rglob("*")))
                structure[item.name] = file_count
        
        return structure
    
    def _setup_optimization_checkpoints(self):
        """设置优化检查点"""
        logger.info("🎯 设置优化检查点...")
        
        checkpoints = [
            {
                "checkpoint_id": "pre_optimization",
                "name": "优化前检查点",
                "description": "确保所有测试通过，系统稳定",
                "criteria": [
                    "所有关键业务测试必须通过",
                    "系统性能基线已建立",
                    "完整备份已创建",
                    "回滚机制已准备"
                ],
                "status": "pending"
            },
            {
                "checkpoint_id": "phase1_validation",
                "name": "阶段1验证检查点",
                "description": "代码逻辑优化后的验证",
                "criteria": [
                    "冗余代码清理完成",
                    "所有测试仍然通过",
                    "性能没有退化",
                    "业务逻辑完整性保持"
                ],
                "status": "pending"
            },
            {
                "checkpoint_id": "phase2_validation",
                "name": "阶段2验证检查点",
                "description": "数据库优化后的验证",
                "criteria": [
                    "数据库结构优化完成",
                    "查询性能提升验证",
                    "数据完整性检查通过",
                    "业务功能正常运行"
                ],
                "status": "pending"
            },
            {
                "checkpoint_id": "final_validation",
                "name": "最终验证检查点",
                "description": "全面优化完成后的最终验证",
                "criteria": [
                    "所有优化目标达成",
                    "系统性能显著提升",
                    "业务逻辑完全正常",
                    "监控系统正常运行"
                ],
                "status": "pending"
            }
        ]
        
        self.baseline_data["optimization_checkpoints"] = checkpoints
        
        logger.info(f"✅ 优化检查点设置完成: {len(checkpoints)} 个检查点")
    
    def _create_system_backup(self):
        """创建系统备份"""
        logger.info("💾 创建系统备份...")
        
        # 备份关键文件
        critical_files = [
            "main.py",
            "models.py", 
            "requirements.txt",
            "app.yaml"
        ]
        
        # 备份关键目录
        critical_dirs = [
            "sql",
            "config",
            "test_suite"
        ]
        
        backup_summary = {
            "files_backed_up": [],
            "dirs_backed_up": [],
            "backup_size": 0
        }
        
        # 备份文件
        for file_name in critical_files:
            file_path = self.base_path / file_name
            if file_path.exists():
                backup_path = self.backup_dir / file_name
                try:
                    shutil.copy2(file_path, backup_path)
                    backup_summary["files_backed_up"].append(file_name)
                    backup_summary["backup_size"] += file_path.stat().st_size
                except Exception as e:
                    logger.warning(f"备份文件失败 {file_name}: {e}")
        
        # 备份目录
        for dir_name in critical_dirs:
            dir_path = self.base_path / dir_name
            if dir_path.exists() and dir_path.is_dir():
                backup_path = self.backup_dir / dir_name
                try:
                    shutil.copytree(dir_path, backup_path, dirs_exist_ok=True)
                    backup_summary["dirs_backed_up"].append(dir_name)
                    # 计算目录大小
                    for file_path in backup_path.rglob("*"):
                        if file_path.is_file():
                            backup_summary["backup_size"] += file_path.stat().st_size
                except Exception as e:
                    logger.warning(f"备份目录失败 {dir_name}: {e}")
        
        # 保存备份清单
        backup_manifest = {
            "backup_timestamp": self.timestamp,
            "backup_location": str(self.backup_dir),
            "backup_summary": backup_summary
        }
        
        manifest_path = self.backup_dir / "backup_manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(backup_manifest, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ 系统备份完成: {len(backup_summary['files_backed_up'])} 个文件，{len(backup_summary['dirs_backed_up'])} 个目录")
    
    def _assess_optimization_readiness(self) -> Dict[str, Any]:
        """评估优化准备情况"""
        logger.info("📋 评估优化准备情况...")
        
        readiness_score = 0
        max_score = 100
        
        readiness_factors = []
        
        # 业务逻辑基线 (30分)
        if self.baseline_data["business_logic_baseline"]:
            logic_score = min(30, self.baseline_data["business_logic_baseline"]["total_code_logic"] / 100 * 30)
            readiness_score += logic_score
            readiness_factors.append({
                "factor": "业务逻辑基线",
                "score": logic_score,
                "max_score": 30,
                "status": "完成" if logic_score > 20 else "部分完成"
            })
        
        # 测试基线 (25分)
        test_baseline = self.baseline_data["test_baseline"]
        if test_baseline["total_tests"] > 0:
            test_score = min(25, test_baseline["test_success_rate"] / 100 * 25)
            readiness_score += test_score
            readiness_factors.append({
                "factor": "测试基线",
                "score": test_score,
                "max_score": 25,
                "status": "完成" if test_score > 20 else "需要改进"
            })
        
        # 性能基线 (20分)
        perf_baseline = self.baseline_data["performance_baseline"]
        if perf_baseline["benchmark_status"] == "baseline_established":
            perf_score = 20
            readiness_score += perf_score
            readiness_factors.append({
                "factor": "性能基线",
                "score": perf_score,
                "max_score": 20,
                "status": "完成"
            })
        
        # 数据基线 (15分)
        data_baseline = self.baseline_data["data_baseline"]
        if data_baseline["code_statistics"]["python_lines"] > 0:
            data_score = 15
            readiness_score += data_score
            readiness_factors.append({
                "factor": "数据基线",
                "score": data_score,
                "max_score": 15,
                "status": "完成"
            })
        
        # 检查点设置 (10分)
        if self.baseline_data["optimization_checkpoints"]:
            checkpoint_score = 10
            readiness_score += checkpoint_score
            readiness_factors.append({
                "factor": "检查点设置",
                "score": checkpoint_score,
                "max_score": 10,
                "status": "完成"
            })
        
        # 确定准备状态
        if readiness_score >= 80:
            readiness_status = "ready"
        elif readiness_score >= 60:
            readiness_status = "mostly_ready"
        elif readiness_score >= 40:
            readiness_status = "partially_ready"
        else:
            readiness_status = "not_ready"
        
        return {
            "readiness_score": readiness_score,
            "max_score": max_score,
            "readiness_percentage": (readiness_score / max_score) * 100,
            "readiness_status": readiness_status,
            "readiness_factors": readiness_factors,
            "recommendations": self._get_readiness_recommendations(readiness_status, readiness_factors)
        }
    
    def _get_readiness_recommendations(self, status: str, factors: List[Dict]) -> List[str]:
        """获取准备情况建议"""
        recommendations = []
        
        if status == "ready":
            recommendations.append("✅ 系统已准备好进行优化")
            recommendations.append("建议按计划执行阶段1代码逻辑优化")
        elif status == "mostly_ready":
            recommendations.append("⚠️ 系统基本准备就绪，建议完善以下方面后开始优化")
            for factor in factors:
                if factor["score"] < factor["max_score"] * 0.8:
                    recommendations.append(f"- 改进{factor['factor']}（当前得分: {factor['score']:.1f}/{factor['max_score']}）")
        else:
            recommendations.append("❌ 系统尚未准备好进行优化")
            recommendations.append("必须完成以下准备工作:")
            for factor in factors:
                if factor["score"] < factor["max_score"] * 0.5:
                    recommendations.append(f"- 完善{factor['factor']}（当前得分: {factor['score']:.1f}/{factor['max_score']}）")
        
        return recommendations
    
    def _get_safety_measures(self) -> List[Dict[str, str]]:
        """获取安全措施"""
        return [
            {
                "measure": "完整备份",
                "description": "所有关键文件和目录已备份",
                "location": str(self.backup_dir)
            },
            {
                "measure": "测试保障",
                "description": "建立了完整的测试基线，确保优化后功能正常",
                "coverage": f"{self.baseline_data['test_baseline']['total_tests']} 个测试"
            },
            {
                "measure": "性能监控",
                "description": "建立了性能基线，可监控优化效果",
                "metrics": f"{len(self.baseline_data['performance_baseline']['metrics'])} 个指标"
            },
            {
                "measure": "检查点验证",
                "description": "设置了多个验证检查点，确保每个阶段的安全性",
                "checkpoints": f"{len(self.baseline_data['optimization_checkpoints'])} 个检查点"
            },
            {
                "measure": "回滚机制",
                "description": "可以快速回滚到优化前的状态",
                "method": "基于备份的完整回滚"
            }
        ]
    
    def save_baseline_report(self, report: Dict[str, Any]):
        """保存基线报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存JSON报告
        json_file = f"pc28_optimization_baseline_report_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 保存Markdown报告
        md_file = f"pc28_optimization_baseline_report_{timestamp}.md"
        self._generate_markdown_report(report, md_file)
        
        logger.info(f"📄 优化基线报告已保存:")
        logger.info(f"  JSON: {json_file}")
        logger.info(f"  Markdown: {md_file}")
        
        return json_file, md_file
    
    def _generate_markdown_report(self, report: Dict[str, Any], file_path: str):
        """生成Markdown格式的报告"""
        
        metadata = report["baseline_metadata"]
        baseline_data = report["baseline_data"]
        readiness = report["optimization_readiness"]
        safety_measures = report["safety_measures"]
        
        content = f"""# PC28优化基线报告

## 📊 基线概览

**创建时间**: {metadata['timestamp']}
**基础路径**: {metadata['base_path']}
**备份位置**: {metadata['backup_location']}

## 💼 业务逻辑基线

"""
        
        logic_baseline = baseline_data["business_logic_baseline"]
        if logic_baseline:
            content += f"""
**代码逻辑总数**: {logic_baseline['total_code_logic']} 个
**数据库表数**: {logic_baseline['database_tables']} 个
**计算字段数**: {logic_baseline['calculated_fields']} 个

### 代码逻辑分布
"""
            for category, count in logic_baseline["code_logic_counts"].items():
                category_name = category.replace("_", " ").title()
                content += f"- **{category_name}**: {count} 个\n"
            
            content += f"""
### 关键业务逻辑
**识别数量**: {len(logic_baseline['critical_logic_items'])} 个

"""
            for item in logic_baseline["critical_logic_items"][:10]:  # 显示前10个
                content += f"- `{item['name']}` ({item['category']}, 置信度: {item['confidence']:.2f})\n"
        
        content += f"""
## 🧪 测试基线

"""
        
        test_baseline = baseline_data["test_baseline"]
        content += f"""
**测试总数**: {test_baseline['total_tests']} 个
**通过测试**: {test_baseline['passed_tests']} 个
**成功率**: {test_baseline['test_success_rate']:.1f}%

### 测试分类
"""
        
        for category, count in test_baseline["test_categories"].items():
            category_name = category.replace("_", " ").title()
            content += f"- **{category_name}**: {count} 个\n"
        
        content += f"""
## ⚡ 性能基线

"""
        
        perf_baseline = baseline_data["performance_baseline"]
        content += f"""
**基线状态**: {perf_baseline['benchmark_status']}
**测试时间**: {perf_baseline['timestamp']}

### 性能指标
"""
        
        for metric, value in perf_baseline["metrics"].items():
            content += f"- **{metric}**: {value}\n"
        
        content += f"""
## 🗄️ 数据基线

"""
        
        data_baseline = baseline_data["data_baseline"]
        file_stats = data_baseline["file_statistics"]
        code_stats = data_baseline["code_statistics"]
        
        content += f"""
### 文件统计
- **Python文件**: {file_stats['python_files']} 个
- **JSON文件**: {file_stats['json_files']} 个
- **SQL文件**: {file_stats['sql_files']} 个
- **Markdown文件**: {file_stats['md_files']} 个
- **总文件数**: {file_stats['total_files']} 个

### 代码统计
- **总代码行数**: {code_stats['total_lines']:,} 行
- **Python代码行数**: {code_stats['python_lines']:,} 行

### 目录结构
"""
        
        for dir_name, file_count in data_baseline["directory_structure"].items():
            content += f"- **{dir_name}**: {file_count} 个文件\n"
        
        content += f"""
## 🎯 优化检查点

"""
        
        for checkpoint in baseline_data["optimization_checkpoints"]:
            status_emoji = {"pending": "⏳", "completed": "✅", "failed": "❌"}.get(checkpoint["status"], "❓")
            content += f"""
### {checkpoint['name']} {status_emoji}
**ID**: {checkpoint['checkpoint_id']}
**描述**: {checkpoint['description']}
**状态**: {checkpoint['status']}

**验证标准**:
"""
            for criterion in checkpoint["criteria"]:
                content += f"- {criterion}\n"
        
        content += f"""
## 📋 优化准备情况

**准备得分**: {readiness['readiness_score']:.1f}/{readiness['max_score']} ({readiness['readiness_percentage']:.1f}%)
**准备状态**: {readiness['readiness_status']}

### 准备因素评估
"""
        
        for factor in readiness["readiness_factors"]:
            status_emoji = {"完成": "✅", "部分完成": "⚠️", "需要改进": "❌"}.get(factor["status"], "❓")
            content += f"""
#### {factor['factor']} {status_emoji}
- **得分**: {factor['score']:.1f}/{factor['max_score']}
- **状态**: {factor['status']}
"""
        
        content += f"""
### 建议
"""
        
        for recommendation in readiness["recommendations"]:
            content += f"{recommendation}\n"
        
        content += f"""
## 🛡️ 安全措施

"""
        
        for measure in safety_measures:
            content += f"""
### {measure['measure']}
**描述**: {measure['description']}
"""
            if 'location' in measure:
                content += f"**位置**: {measure['location']}\n"
            if 'coverage' in measure:
                content += f"**覆盖**: {measure['coverage']}\n"
            if 'metrics' in measure:
                content += f"**指标**: {measure['metrics']}\n"
            if 'checkpoints' in measure:
                content += f"**检查点**: {measure['checkpoints']}\n"
            if 'method' in measure:
                content += f"**方法**: {measure['method']}\n"
        
        content += f"""
## 🚀 下一步行动

1. **验证准备情况** - 确认所有基线数据完整且准确
2. **运行基线测试** - 执行完整的测试套件，确保100%通过
3. **开始阶段1优化** - 处理429个冗余逻辑项
4. **持续监控** - 在优化过程中持续监控系统状态
5. **检查点验证** - 在每个阶段完成后进行检查点验证

---

**报告生成时间**: {datetime.now().isoformat()}
**版本**: 1.0
"""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

def main():
    """主函数"""
    print("🎯 PC28优化基线创建器")
    print("=" * 60)
    print("📋 目标：为系统优化创建完整的基线和安全保障")
    print("🛡️ 范围：业务逻辑、测试、性能、数据基线 + 安全措施")
    print("=" * 60)
    
    creator = OptimizationBaselineCreator()
    
    try:
        # 创建优化基线
        baseline_report = creator.create_optimization_baseline()
        
        # 保存报告
        json_file, md_file = creator.save_baseline_report(baseline_report)
        
        # 显示摘要
        print("\n" + "=" * 60)
        print("📊 优化基线创建摘要")
        print("=" * 60)
        
        baseline_data = baseline_report["baseline_data"]
        readiness = baseline_report["optimization_readiness"]
        
        logic_baseline = baseline_data["business_logic_baseline"]
        test_baseline = baseline_data["test_baseline"]
        
        print(f"\n💼 业务逻辑基线:")
        if logic_baseline:
            print(f"   代码逻辑: {logic_baseline['total_code_logic']} 个")
            print(f"   数据库表: {logic_baseline['database_tables']} 个")
            print(f"   关键逻辑: {len(logic_baseline['critical_logic_items'])} 个")
        else:
            print("   未建立业务逻辑基线")
        
        print(f"\n🧪 测试基线:")
        print(f"   总测试数: {test_baseline['total_tests']} 个")
        print(f"   成功率: {test_baseline['test_success_rate']:.1f}%")
        
        print(f"\n📋 优化准备情况:")
        print(f"   准备得分: {readiness['readiness_score']:.1f}/{readiness['max_score']} ({readiness['readiness_percentage']:.1f}%)")
        print(f"   准备状态: {readiness['readiness_status']}")
        
        print(f"\n🛡️ 安全措施:")
        safety_measures = baseline_report["safety_measures"]
        for measure in safety_measures:
            print(f"   ✓ {measure['measure']}")
        
        print(f"\n📄 详细报告: {md_file}")
        
        if readiness['readiness_status'] == 'ready':
            print("\n🎉 优化基线创建完成！系统已准备好进行安全优化。")
        else:
            print(f"\n⚠️ 优化基线创建完成，但系统准备情况为: {readiness['readiness_status']}")
            print("建议完善准备工作后再开始优化。")
        
    except Exception as e:
        logger.error(f"优化基线创建失败: {e}")
        raise

if __name__ == "__main__":
    main()