#!/usr/bin/env python3
"""
最终本地API采集代码清理系统
彻底删除所有本地API采集相关代码和配置
"""

import os
import re
import logging
import shutil
from pathlib import Path
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('final_api_cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FinalAPICleanup:
    def __init__(self):
        """初始化最终API清理系统"""
        self.root_dir = "/Users/a606/cloud_function_source"
        self.backup_dir = f"{self.root_dir}/CHANGESETS/BACKUPS/final_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 需要清理的关键词
        self.api_keywords = [
            'upstream_api',
            'real_api', 
            'local_api',
            'pc28_upstream',
            'RealtimeLotteryService',
            'HistoryBackfillService',
            'IntegratedDataAdapter',
            'PC28UpstreamAPI',
            'LocalAPICollector'
        ]
        
        # 需要完全删除的文件
        self.files_to_delete = [
            'python/pc28_upstream_api.py',
            'python/realtime_lottery_service.py', 
            'python/history_backfill_service.py',
            'python/integrated_data_adapter.py',
            'python/integrated_data_adapter_client.py',
            'local_system/local_api_collector.py',
            'real_api_data_system.py'
        ]
        
        # 需要修改的配置文件
        self.config_files = [
            '.env.example',
            'config/integrated_config.json',
            'config/concurrency_config.json',
            'monitoring_config.yaml'
        ]
        
        # 需要修改的测试文件
        self.test_files = [
            'test_field_usage_analysis.py',
            'test_e2e.py',
            'test_data_sync.py',
            'test_e2e_system.py',
            'test_ops_system.py',
            'field_usage_analysis.py',
            'ops_system_main.py',
            'ops_manager_main.py',
            'data_flow_analyzer.py',
            'deploy_pc28_system.py',
            'historical_data_integrity_protector.py'
        ]
        
    def create_backup(self):
        """创建备份目录"""
        try:
            os.makedirs(self.backup_dir, exist_ok=True)
            logger.info(f"创建备份目录: {self.backup_dir}")
            return True
        except Exception as e:
            logger.error(f"创建备份目录失败: {e}")
            return False
    
    def delete_api_files(self):
        """删除API相关文件"""
        deleted_files = []
        
        for file_path in self.files_to_delete:
            full_path = os.path.join(self.root_dir, file_path)
            if os.path.exists(full_path):
                try:
                    # 备份文件
                    backup_path = os.path.join(self.backup_dir, file_path)
                    os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                    shutil.copy2(full_path, backup_path)
                    
                    # 删除文件
                    os.remove(full_path)
                    deleted_files.append(file_path)
                    logger.info(f"删除文件: {file_path}")
                except Exception as e:
                    logger.error(f"删除文件 {file_path} 失败: {e}")
        
        return deleted_files
    
    def clean_config_files(self):
        """清理配置文件中的API相关配置"""
        cleaned_files = []
        
        for config_file in self.config_files:
            full_path = os.path.join(self.root_dir, config_file)
            if os.path.exists(full_path):
                try:
                    # 备份文件
                    backup_path = os.path.join(self.backup_dir, config_file)
                    os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                    shutil.copy2(full_path, backup_path)
                    
                    # 读取文件内容
                    with open(full_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    original_content = content
                    
                    # 清理不同类型的配置文件
                    if config_file.endswith('.json'):
                        content = self.clean_json_config(content)
                    elif config_file.endswith('.yaml') or config_file.endswith('.yml'):
                        content = self.clean_yaml_config(content)
                    elif config_file.startswith('.env'):
                        content = self.clean_env_config(content)
                    
                    # 如果内容有变化，写回文件
                    if content != original_content:
                        with open(full_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        cleaned_files.append(config_file)
                        logger.info(f"清理配置文件: {config_file}")
                
                except Exception as e:
                    logger.error(f"清理配置文件 {config_file} 失败: {e}")
        
        return cleaned_files
    
    def clean_json_config(self, content):
        """清理JSON配置文件"""
        # 移除upstream_api相关配置
        patterns = [
            r'"upstream_api"\s*:\s*{[^}]*},?\s*',
            r'"use_upstream_api"\s*:\s*[^,\n]*,?\s*',
            r'"upstream_api_[^"]*"\s*:\s*[^,\n]*,?\s*'
        ]
        
        for pattern in patterns:
            content = re.sub(pattern, '', content, flags=re.MULTILINE | re.DOTALL)
        
        # 清理多余的逗号
        content = re.sub(r',\s*}', '}', content)
        content = re.sub(r',\s*]', ']', content)
        
        return content
    
    def clean_yaml_config(self, content):
        """清理YAML配置文件"""
        lines = content.split('\n')
        cleaned_lines = []
        skip_section = False
        
        for line in lines:
            # 检查是否是upstream_api相关的section
            if re.match(r'^upstream_api\s*:', line):
                skip_section = True
                continue
            
            # 检查是否结束了upstream_api section
            if skip_section and re.match(r'^[a-zA-Z]', line) and not line.startswith(' '):
                skip_section = False
            
            # 跳过upstream_api section内的行
            if skip_section:
                continue
            
            # 移除包含upstream_api关键词的行
            if any(keyword in line for keyword in ['upstream_api', 'use_upstream_api']):
                continue
            
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def clean_env_config(self, content):
        """清理环境变量配置文件"""
        lines = content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # 跳过包含API相关关键词的行
            if any(keyword.upper() in line.upper() for keyword in self.api_keywords):
                continue
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def clean_test_files(self):
        """清理测试文件中的API相关代码"""
        cleaned_files = []
        
        for test_file in self.test_files:
            full_path = os.path.join(self.root_dir, test_file)
            if os.path.exists(full_path):
                try:
                    # 备份文件
                    backup_path = os.path.join(self.backup_dir, test_file)
                    os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                    shutil.copy2(full_path, backup_path)
                    
                    # 读取文件内容
                    with open(full_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    original_content = content
                    
                    # 移除import语句
                    import_patterns = [
                        r'from\s+.*(?:' + '|'.join(self.api_keywords) + ').*import.*\n',
                        r'import\s+.*(?:' + '|'.join(self.api_keywords) + ').*\n'
                    ]
                    
                    for pattern in import_patterns:
                        content = re.sub(pattern, '', content, flags=re.MULTILINE)
                    
                    # 移除或注释掉包含API关键词的代码行
                    lines = content.split('\n')
                    cleaned_lines = []
                    
                    for line in lines:
                        # 检查是否包含API关键词
                        if any(keyword in line for keyword in self.api_keywords):
                            # 如果是测试函数或重要逻辑，注释掉而不是删除
                            if 'def test_' in line or 'class ' in line:
                                cleaned_lines.append(f"# DISABLED: {line}")
                            elif 'upstream_api' in line and ('config' in line or 'get(' in line):
                                # 替换配置引用为空字典
                                cleaned_lines.append(line.replace("config['upstream_api']", "{}").replace("config.get('upstream_api'", "config.get('disabled_upstream_api'"))
                            # 否则跳过这行
                        else:
                            cleaned_lines.append(line)
                    
                    content = '\n'.join(cleaned_lines)
                    
                    # 如果内容有变化，写回文件
                    if content != original_content:
                        with open(full_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        cleaned_files.append(test_file)
                        logger.info(f"清理测试文件: {test_file}")
                
                except Exception as e:
                    logger.error(f"清理测试文件 {test_file} 失败: {e}")
        
        return cleaned_files
    
    def verify_cleanup(self):
        """验证清理结果"""
        logger.info("验证清理结果...")
        
        # 搜索剩余的API关键词
        remaining_references = {}
        
        for root, dirs, files in os.walk(self.root_dir):
            # 跳过备份目录
            if 'BACKUPS' in root:
                continue
            
            for file in files:
                if file.endswith(('.py', '.json', '.yaml', '.yml', '.env')):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        for keyword in self.api_keywords:
                            if keyword in content:
                                if file_path not in remaining_references:
                                    remaining_references[file_path] = []
                                remaining_references[file_path].append(keyword)
                    
                    except Exception as e:
                        logger.warning(f"无法读取文件 {file_path}: {e}")
        
        # 报告剩余引用
        if remaining_references:
            logger.warning("发现剩余的API引用:")
            for file_path, keywords in remaining_references.items():
                logger.warning(f"  {file_path}: {', '.join(keywords)}")
        else:
            logger.info("✅ 所有API引用已清理完成!")
        
        return remaining_references
    
    def run_final_cleanup(self):
        """运行最终清理流程"""
        logger.info("开始最终API清理流程...")
        
        # 1. 创建备份
        if not self.create_backup():
            logger.error("创建备份失败，终止清理流程")
            return False
        
        # 2. 删除API文件
        deleted_files = self.delete_api_files()
        logger.info(f"删除了 {len(deleted_files)} 个API文件")
        
        # 3. 清理配置文件
        cleaned_configs = self.clean_config_files()
        logger.info(f"清理了 {len(cleaned_configs)} 个配置文件")
        
        # 4. 清理测试文件
        cleaned_tests = self.clean_test_files()
        logger.info(f"清理了 {len(cleaned_tests)} 个测试文件")
        
        # 5. 验证清理结果
        remaining_refs = self.verify_cleanup()
        
        logger.info("最终API清理流程完成!")
        logger.info(f"备份位置: {self.backup_dir}")
        
        return len(remaining_refs) == 0

def main():
    """主函数"""
    cleanup_system = FinalAPICleanup()
    
    # 运行最终清理流程
    success = cleanup_system.run_final_cleanup()
    
    if success:
        logger.info("✅ 最终API清理成功完成!")
    else:
        logger.warning("⚠️ 最终API清理完成，但仍有部分引用需要手动处理")

if __name__ == "__main__":
    main()