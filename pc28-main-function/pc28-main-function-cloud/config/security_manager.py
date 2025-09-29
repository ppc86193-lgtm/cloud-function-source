"""
安全密钥管理和环境变量配置系统
提供安全的密钥存储、加密、轮换和访问控制功能
"""

import os
import json
import hashlib
import secrets
from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SecurityManager:
    """安全管理器 - 处理密钥加密、存储和访问控制"""
    
    def __init__(self, master_key: Optional[str] = None):
        """
        初始化安全管理器
        
        Args:
            master_key: 主密钥，用于加密其他密钥
        """
        self.master_key = master_key or os.environ.get('MASTER_KEY')
        self.key_storage_path = os.path.join(os.getcwd(), 'config', 'encrypted_keys.json')
        self.key_metadata_path = os.path.join(os.getcwd(), 'config', 'key_metadata.json')
        
        # 确保配置目录存在
        os.makedirs(os.path.dirname(self.key_storage_path), exist_ok=True)
        
        # 初始化加密器
        self._cipher_suite = self._initialize_cipher()
        
        # 加载密钥元数据
        self.key_metadata = self._load_key_metadata()
    
    def _initialize_cipher(self) -> Optional[Fernet]:
        """初始化加密套件"""
        if not self.master_key:
            logger.warning("No master key provided, encryption disabled")
            return None
        
        try:
            # 使用 PBKDF2 从主密钥生成加密密钥
            password = self.master_key.encode()
            salt = b'pc28_security_salt'  # 在生产环境中应该使用随机盐
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(password))
            return Fernet(key)
        except Exception as e:
            logger.error(f"Failed to initialize cipher: {e}")
            return None
    
    def _load_key_metadata(self) -> Dict[str, Any]:
        """加载密钥元数据"""
        if os.path.exists(self.key_metadata_path):
            try:
                with open(self.key_metadata_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load key metadata: {e}")
        
        return {}
    
    def _save_key_metadata(self):
        """保存密钥元数据"""
        try:
            with open(self.key_metadata_path, 'w') as f:
                json.dump(self.key_metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save key metadata: {e}")
    
    def store_encrypted_key(self, key_name: str, key_value: str, 
                          description: str = "", expires_days: int = 90) -> bool:
        """
        存储加密的密钥
        
        Args:
            key_name: 密钥名称
            key_value: 密钥值
            description: 密钥描述
            expires_days: 过期天数
            
        Returns:
            bool: 存储是否成功
        """
        if not self._cipher_suite:
            logger.error("Encryption not available")
            return False
        
        try:
            # 加密密钥值
            encrypted_value = self._cipher_suite.encrypt(key_value.encode())
            
            # 加载现有的加密密钥存储
            encrypted_keys = {}
            if os.path.exists(self.key_storage_path):
                with open(self.key_storage_path, 'r') as f:
                    encrypted_keys = json.load(f)
            
            # 存储加密的密钥
            encrypted_keys[key_name] = base64.b64encode(encrypted_value).decode()
            
            # 保存到文件
            with open(self.key_storage_path, 'w') as f:
                json.dump(encrypted_keys, f, indent=2)
            
            # 更新元数据
            self.key_metadata[key_name] = {
                'description': description,
                'created_at': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(days=expires_days)).isoformat(),
                'last_accessed': None,
                'access_count': 0,
                'key_hash': hashlib.sha256(key_value.encode()).hexdigest()[:16]
            }
            
            self._save_key_metadata()
            logger.info(f"Key '{key_name}' stored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store key '{key_name}': {e}")
            return False
    
    def retrieve_decrypted_key(self, key_name: str) -> Optional[str]:
        """
        检索并解密密钥
        
        Args:
            key_name: 密钥名称
            
        Returns:
            Optional[str]: 解密后的密钥值
        """
        if not self._cipher_suite:
            logger.error("Encryption not available")
            return None
        
        try:
            # 检查密钥是否过期
            if not self._is_key_valid(key_name):
                logger.warning(f"Key '{key_name}' is expired or invalid")
                return None
            
            # 加载加密的密钥
            if not os.path.exists(self.key_storage_path):
                logger.error("No encrypted keys found")
                return None
            
            with open(self.key_storage_path, 'r') as f:
                encrypted_keys = json.load(f)
            
            if key_name not in encrypted_keys:
                logger.error(f"Key '{key_name}' not found")
                return None
            
            # 解密密钥
            encrypted_value = base64.b64decode(encrypted_keys[key_name])
            decrypted_value = self._cipher_suite.decrypt(encrypted_value).decode()
            
            # 更新访问记录
            self._update_access_record(key_name)
            
            return decrypted_value
            
        except Exception as e:
            logger.error(f"Failed to retrieve key '{key_name}': {e}")
            return None
    
    def _is_key_valid(self, key_name: str) -> bool:
        """检查密钥是否有效（未过期）"""
        if key_name not in self.key_metadata:
            return False
        
        expires_at = datetime.fromisoformat(self.key_metadata[key_name]['expires_at'])
        return datetime.now() < expires_at
    
    def _update_access_record(self, key_name: str):
        """更新密钥访问记录"""
        if key_name in self.key_metadata:
            self.key_metadata[key_name]['last_accessed'] = datetime.now().isoformat()
            self.key_metadata[key_name]['access_count'] += 1
            self._save_key_metadata()
    
    def rotate_key(self, key_name: str, new_key_value: str) -> bool:
        """
        轮换密钥
        
        Args:
            key_name: 密钥名称
            new_key_value: 新的密钥值
            
        Returns:
            bool: 轮换是否成功
        """
        if key_name not in self.key_metadata:
            logger.error(f"Key '{key_name}' not found for rotation")
            return False
        
        # 备份旧密钥信息
        old_metadata = self.key_metadata[key_name].copy()
        
        # 存储新密钥
        success = self.store_encrypted_key(
            key_name, 
            new_key_value,
            old_metadata.get('description', ''),
            90  # 默认90天过期
        )
        
        if success:
            # 更新轮换记录
            self.key_metadata[key_name]['rotated_at'] = datetime.now().isoformat()
            self.key_metadata[key_name]['rotation_count'] = old_metadata.get('rotation_count', 0) + 1
            self._save_key_metadata()
            logger.info(f"Key '{key_name}' rotated successfully")
        
        return success
    
    def list_keys(self) -> List[Dict[str, Any]]:
        """列出所有密钥的元数据（不包含实际密钥值）"""
        keys_info = []
        
        for key_name, metadata in self.key_metadata.items():
            key_info = {
                'name': key_name,
                'description': metadata.get('description', ''),
                'created_at': metadata.get('created_at'),
                'expires_at': metadata.get('expires_at'),
                'last_accessed': metadata.get('last_accessed'),
                'access_count': metadata.get('access_count', 0),
                'is_valid': self._is_key_valid(key_name),
                'key_hash': metadata.get('key_hash', '')
            }
            keys_info.append(key_info)
        
        return keys_info
    
    def delete_key(self, key_name: str) -> bool:
        """
        删除密钥
        
        Args:
            key_name: 密钥名称
            
        Returns:
            bool: 删除是否成功
        """
        try:
            # 从加密存储中删除
            if os.path.exists(self.key_storage_path):
                with open(self.key_storage_path, 'r') as f:
                    encrypted_keys = json.load(f)
                
                if key_name in encrypted_keys:
                    del encrypted_keys[key_name]
                    
                    with open(self.key_storage_path, 'w') as f:
                        json.dump(encrypted_keys, f, indent=2)
            
            # 从元数据中删除
            if key_name in self.key_metadata:
                del self.key_metadata[key_name]
                self._save_key_metadata()
            
            logger.info(f"Key '{key_name}' deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete key '{key_name}': {e}")
            return False


class EnvironmentManager:
    """环境变量管理器"""
    
    def __init__(self, security_manager: SecurityManager):
        self.security_manager = security_manager
        self.env_config_path = os.path.join(os.getcwd(), 'config', 'environment_config.json')
        
        # 环境配置模板
        self.env_templates = {
            'development': {
                'SUPABASE_URL': 'https://your-project.supabase.co',
                'SUPABASE_ANON_KEY': 'your-anon-key',
                'SUPABASE_SERVICE_ROLE_KEY': 'your-service-role-key',
                'SQLITE_DB_PATH': 'pc28_data.db',
                'BIGQUERY_PROJECT_ID': 'your-project-id',
                'BIGQUERY_DATASET_ID': 'pc28_dataset',
                'LOG_LEVEL': 'DEBUG',
                'SYNC_INTERVAL_MINUTES': '30',
                'MAX_RETRY_ATTEMPTS': '3'
            },
            'testing': {
                'SUPABASE_URL': 'https://your-test-project.supabase.co',
                'SUPABASE_ANON_KEY': 'your-test-anon-key',
                'SUPABASE_SERVICE_ROLE_KEY': 'your-test-service-role-key',
                'SQLITE_DB_PATH': 'test_pc28_data.db',
                'LOG_LEVEL': 'INFO',
                'SYNC_INTERVAL_MINUTES': '60',
                'MAX_RETRY_ATTEMPTS': '2'
            },
            'production': {
                'SUPABASE_URL': 'https://your-prod-project.supabase.co',
                'SUPABASE_ANON_KEY': 'encrypted:supabase_anon_key',
                'SUPABASE_SERVICE_ROLE_KEY': 'encrypted:supabase_service_role_key',
                'SQLITE_DB_PATH': '/data/pc28_data.db',
                'BIGQUERY_PROJECT_ID': 'encrypted:bigquery_project_id',
                'LOG_LEVEL': 'WARNING',
                'SYNC_INTERVAL_MINUTES': '15',
                'MAX_RETRY_ATTEMPTS': '5'
            }
        }
    
    def generate_env_file(self, environment: str = 'development') -> bool:
        """
        生成环境变量文件
        
        Args:
            environment: 环境名称 (development, testing, production)
            
        Returns:
            bool: 生成是否成功
        """
        if environment not in self.env_templates:
            logger.error(f"Unknown environment: {environment}")
            return False
        
        try:
            env_file_path = f'.env.{environment}'
            template = self.env_templates[environment]
            
            with open(env_file_path, 'w') as f:
                f.write(f"# PC28 System Environment Configuration - {environment.upper()}\n")
                f.write(f"# Generated at: {datetime.now().isoformat()}\n\n")
                
                # 按类别组织环境变量
                categories = {
                    'Supabase Configuration': ['SUPABASE_URL', 'SUPABASE_ANON_KEY', 'SUPABASE_SERVICE_ROLE_KEY'],
                    'Database Configuration': ['SQLITE_DB_PATH', 'BIGQUERY_PROJECT_ID', 'BIGQUERY_DATASET_ID'],
                    'Application Configuration': ['LOG_LEVEL', 'SYNC_INTERVAL_MINUTES', 'MAX_RETRY_ATTEMPTS']
                }
                
                for category, keys in categories.items():
                    f.write(f"# {category}\n")
                    for key in keys:
                        if key in template:
                            value = template[key]
                            
                            # 处理加密的值
                            if value.startswith('encrypted:'):
                                encrypted_key_name = value.replace('encrypted:', '')
                                decrypted_value = self.security_manager.retrieve_decrypted_key(encrypted_key_name)
                                if decrypted_value:
                                    f.write(f"{key}={decrypted_value}\n")
                                else:
                                    f.write(f"{key}=# ENCRYPTED_KEY_NOT_FOUND: {encrypted_key_name}\n")
                            else:
                                f.write(f"{key}={value}\n")
                    f.write("\n")
            
            logger.info(f"Environment file '.env.{environment}' generated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate environment file: {e}")
            return False
    
    def validate_environment(self, environment: str = 'development') -> Dict[str, Any]:
        """
        验证环境配置
        
        Args:
            environment: 环境名称
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        validation_result = {
            'environment': environment,
            'is_valid': True,
            'missing_keys': [],
            'invalid_keys': [],
            'warnings': []
        }
        
        env_file_path = f'.env.{environment}'
        
        if not os.path.exists(env_file_path):
            validation_result['is_valid'] = False
            validation_result['warnings'].append(f"Environment file '.env.{environment}' not found")
            return validation_result
        
        # 读取环境文件
        env_vars = {}
        try:
            with open(env_file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key] = value
        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['warnings'].append(f"Failed to read environment file: {e}")
            return validation_result
        
        # 检查必需的密钥
        required_keys = self.env_templates.get(environment, {}).keys()
        
        for key in required_keys:
            if key not in env_vars:
                validation_result['missing_keys'].append(key)
                validation_result['is_valid'] = False
            elif not env_vars[key] or env_vars[key].startswith('your-'):
                validation_result['invalid_keys'].append(key)
                validation_result['warnings'].append(f"Key '{key}' has placeholder value")
        
        return validation_result
    
    def setup_github_secrets(self) -> Dict[str, str]:
        """
        生成 GitHub Secrets 配置指南
        
        Returns:
            Dict[str, str]: GitHub Secrets 配置
        """
        secrets_config = {
            'SUPABASE_URL': 'Your Supabase project URL',
            'SUPABASE_ANON_KEY': 'Your Supabase anonymous key',
            'SUPABASE_SERVICE_ROLE_KEY': 'Your Supabase service role key',
            'SUPABASE_ACCESS_TOKEN': 'Your Supabase access token for CLI',
            'SUPABASE_DB_PASSWORD': 'Your Supabase database password',
            'SUPABASE_PROJECT_ID': 'Your Supabase project ID',
            'MASTER_KEY': 'Master key for encrypting other secrets'
        }
        
        # 生成 GitHub Secrets 设置指南
        guide_path = 'config/github_secrets_setup.md'
        
        try:
            with open(guide_path, 'w') as f:
                f.write("# GitHub Secrets 配置指南\n\n")
                f.write("在 GitHub 仓库中设置以下 Secrets，用于 CI/CD 流程：\n\n")
                f.write("## 必需的 Secrets\n\n")
                
                for secret_name, description in secrets_config.items():
                    f.write(f"### `{secret_name}`\n")
                    f.write(f"- **描述**: {description}\n")
                    f.write(f"- **设置路径**: Repository Settings → Secrets and variables → Actions → New repository secret\n\n")
                
                f.write("## 设置步骤\n\n")
                f.write("1. 进入 GitHub 仓库页面\n")
                f.write("2. 点击 Settings 标签\n")
                f.write("3. 在左侧菜单中选择 'Secrets and variables' → 'Actions'\n")
                f.write("4. 点击 'New repository secret'\n")
                f.write("5. 输入 Secret 名称和值\n")
                f.write("6. 点击 'Add secret'\n\n")
                f.write("## 安全注意事项\n\n")
                f.write("- 不要在代码中硬编码任何密钥\n")
                f.write("- 定期轮换密钥\n")
                f.write("- 使用最小权限原则\n")
                f.write("- 监控密钥使用情况\n")
            
            logger.info(f"GitHub Secrets setup guide created: {guide_path}")
            
        except Exception as e:
            logger.error(f"Failed to create GitHub Secrets guide: {e}")
        
        return secrets_config


# 全局实例
security_manager = SecurityManager()
environment_manager = EnvironmentManager(security_manager)

# 便捷函数
def get_secure_config(key_name: str) -> Optional[str]:
    """获取安全配置值"""
    return security_manager.retrieve_decrypted_key(key_name)

def store_secure_config(key_name: str, key_value: str, description: str = "") -> bool:
    """存储安全配置值"""
    return security_manager.store_encrypted_key(key_name, key_value, description)

def setup_environment(env_name: str = 'development') -> bool:
    """设置环境配置"""
    return environment_manager.generate_env_file(env_name)

def validate_environment_config(env_name: str = 'development') -> Dict[str, Any]:
    """验证环境配置"""
    return environment_manager.validate_environment(env_name)


if __name__ == "__main__":
    # 演示用法
    print("🔐 PC28 Security Manager Demo")
    
    # 设置主密钥（在实际使用中应该从安全的地方获取）
    os.environ['MASTER_KEY'] = 'demo_master_key_for_pc28_system'
    
    # 创建安全管理器实例
    sm = SecurityManager()
    
    # 存储一些示例密钥
    demo_keys = {
        'supabase_anon_key': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
        'supabase_service_role_key': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
        'bigquery_project_id': 'pc28-analytics-project'
    }
    
    for key_name, key_value in demo_keys.items():
        success = sm.store_encrypted_key(key_name, key_value, f"Demo {key_name}")
        print(f"✅ Stored {key_name}: {success}")
    
    # 列出所有密钥
    print("\n📋 Stored Keys:")
    for key_info in sm.list_keys():
        print(f"  - {key_info['name']}: {key_info['description']} (Valid: {key_info['is_valid']})")
    
    # 生成环境文件
    em = EnvironmentManager(sm)
    
    for env in ['development', 'testing', 'production']:
        success = em.generate_env_file(env)
        print(f"✅ Generated .env.{env}: {success}")
        
        # 验证环境
        validation = em.validate_environment(env)
        print(f"  Validation: {'✅ Valid' if validation['is_valid'] else '❌ Invalid'}")
        if validation['warnings']:
            for warning in validation['warnings']:
                print(f"    ⚠️ {warning}")
    
    # 生成 GitHub Secrets 指南
    secrets_config = em.setup_github_secrets()
    print(f"\n🔑 GitHub Secrets configuration guide created")
    print(f"Required secrets: {list(secrets_config.keys())}")