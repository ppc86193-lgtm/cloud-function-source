#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理本地API采集相关引用的脚本
删除所有对已删除文件的引用，避免误导
"""

import os
import re
import logging
from pathlib import Path
from typing import List, Dict, Set

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIReferenceCleanup:
    """API引用清理器"""
    
    def __init__(self, project_root: str = "/Users/a606/cloud_function_source"):
        self.project_root = Path(project_root)
        self.deleted_modules = {
            'real_api_data_system',
            'local_api_collector', 
            'pc28_upstream_api'
        }
        self.files_to_fix = []
        self.backup_dir = self.project_root / "CHANGESETS" / "BACKUPS" / "api_cleanup"
        
    def scan_references(self) -> Dict[str, List[str]]:
        """扫描所有引用已删除模块的文件"""
        references = {}
        
        # 扫描Python文件
        for py_file in self.project_root.rglob("*.py"):
            if py_file.is_file():
                try:
                    with open(py_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    found_refs = []
                    for module in self.deleted_modules:
                        # 检查import语句
                        import_patterns = [
                            rf'from\s+{module}\s+import',
                            rf'import\s+{module}',
                            rf'from\s+.*{module}.*\s+import',
                            rf'import\s+.*{module}.*'
                        ]
                        
                        for pattern in import_patterns:
                            if re.search(pattern, content):
                                found_refs.append(f"import {module}")
                                break
                    
                    if found_refs:
                        references[str(py_file)] = found_refs
                        
                except Exception as e:
                    logger.warning(f"无法读取文件 {py_file}: {e}")
        
        return references
    
    def create_backup(self, file_path: str):
        """创建文件备份"""
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            source = Path(file_path)
            backup_name = f"{source.name}.backup"
            backup_path = self.backup_dir / backup_name
            
            with open(source, 'r', encoding='utf-8') as src:
                content = src.read()
            
            with open(backup_path, 'w', encoding='utf-8') as dst:
                dst.write(content)
                
            logger.info(f"备份文件: {backup_path}")
            
        except Exception as e:
            logger.error(f"备份文件失败 {file_path}: {e}")
    
    def clean_file(self, file_path: str) -> bool:
        """清理单个文件中的API引用"""
        try:
            self.create_backup(file_path)
            
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            cleaned_lines = []
            removed_imports = []
            
            for line in lines:
                should_remove = False
                
                # 检查是否包含已删除模块的引用
                for module in self.deleted_modules:
                    import_patterns = [
                        rf'from\s+{module}\s+import',
                        rf'import\s+{module}',
                        rf'from\s+.*{module}.*\s+import',
                        rf'import\s+.*{module}.*'
                    ]
                    
                    for pattern in import_patterns:
                        if re.search(pattern, line):
                            should_remove = True
                            removed_imports.append(line.strip())
                            break
                    
                    if should_remove:
                        break
                
                if not should_remove:
                    cleaned_lines.append(line)
            
            # 写回清理后的内容
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(cleaned_lines)
            
            if removed_imports:
                logger.info(f"清理文件 {file_path}:")
                for imp in removed_imports:
                    logger.info(f"  移除: {imp}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"清理文件失败 {file_path}: {e}")
            return False
    
    def run_cleanup(self):
        """执行清理操作"""
        logger.info("开始扫描API引用...")
        
        references = self.scan_references()
        
        if not references:
            logger.info("未发现需要清理的API引用")
            return
        
        logger.info(f"发现 {len(references)} 个文件包含API引用:")
        for file_path, refs in references.items():
            logger.info(f"  {file_path}: {refs}")
        
        logger.info("\n开始清理...")
        
        cleaned_count = 0
        for file_path in references.keys():
            if self.clean_file(file_path):
                cleaned_count += 1
        
        logger.info(f"\n清理完成! 共处理 {cleaned_count} 个文件")
        logger.info(f"备份文件保存在: {self.backup_dir}")

def main():
    """主函数"""
    print("=== API引用清理工具 ===")
    print("此工具将清理所有对已删除API模块的引用")
    print("包括: real_api_data_system, local_api_collector, pc28_upstream_api")
    
    cleanup = APIReferenceCleanup()
    cleanup.run_cleanup()
    
    print("\n=== 清理完成 ===")
    print("所有本地API采集相关的引用已被移除")
    print("系统现在完全依赖云上数据采集")

if __name__ == "__main__":
    main()