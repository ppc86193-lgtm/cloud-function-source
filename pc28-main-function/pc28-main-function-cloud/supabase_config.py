"""
Supabase 配置和连接管理器
用于 PC28 系统与 Supabase 的集成
"""

import os
import logging
from typing import Optional, Dict, Any
from supabase import create_client, Client
from datetime import datetime
import json
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupabaseConfig:
    """Supabase 配置管理类"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.anon_key = os.getenv('SUPABASE_ANON_KEY')
        self.service_role_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        # 验证必要的环境变量
        if not self.url:
            raise ValueError("SUPABASE_URL environment variable is required")
        
        if not self.anon_key and not self.service_role_key:
            raise ValueError("Either SUPABASE_ANON_KEY or SUPABASE_SERVICE_ROLE_KEY is required")
    
    def get_client(self, use_service_role: bool = False) -> Client:
        """
        创建 Supabase 客户端
        
        Args:
            use_service_role: 是否使用服务角色密钥（用于管理操作）
            
        Returns:
            Supabase 客户端实例
        """
        key = self.service_role_key if use_service_role else self.anon_key
        
        if not key:
            role_type = "service role" if use_service_role else "anon"
            raise ValueError(f"Supabase {role_type} key not found in environment variables")
        
        return create_client(self.url, key)

class SupabaseConnectionManager:
    """Supabase 连接管理器"""
    
    def __init__(self):
        self.config = SupabaseConfig()
        self._client = None
        self._service_client = None
        self.connection_pool_size = int(os.getenv('SUPABASE_POOL_SIZE', '10'))
        
    @property
    def client(self) -> Client:
        """获取标准客户端（使用 anon key）"""
        if self._client is None:
            self._client = self.config.get_client(use_service_role=False)
            logger.info("Supabase client initialized with anon key")
        return self._client
    
    @property
    def service_client(self) -> Client:
        """获取服务客户端（使用 service role key）"""
        if self._service_client is None:
            self._service_client = self.config.get_client(use_service_role=True)
            logger.info("Supabase service client initialized")
        return self._service_client
    
    def test_connection(self) -> Dict[str, Any]:
        """
        测试 Supabase 连接
        
        Returns:
            连接测试结果
        """
        try:
            # 测试基本连接 - 使用现有的表
            response = self.client.table('users').select('id').limit(1).execute()
            
            result = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'url': self.config.url,
                'connection_type': 'anon_key',
                'message': 'Connection successful'
            }
            
            logger.info("Supabase connection test passed")
            return result
            
        except Exception as e:
            result = {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'url': self.config.url,
                'error': str(e),
                'message': 'Connection failed'
            }
            
            logger.error(f"Supabase connection test failed: {e}")
            return result
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        获取数据库信息
        
        Returns:
            数据库信息字典
        """
        try:
            # 获取数据库版本和基本信息
            response = self.service_client.rpc('version').execute()
            
            info = {
                'timestamp': datetime.now().isoformat(),
                'database_version': response.data if response.data else 'Unknown',
                'connection_status': 'active',
                'pool_size': self.connection_pool_size,
                'url': self.config.url
            }
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'connection_status': 'error'
            }
    
    def close_connections(self):
        """关闭所有连接"""
        if self._client:
            # Supabase Python 客户端会自动管理连接
            self._client = None
            logger.info("Supabase client connection closed")
        
        if self._service_client:
            self._service_client = None
            logger.info("Supabase service client connection closed")

# 全局连接管理器实例
supabase_manager = SupabaseConnectionManager()

def get_supabase_client(use_service_role: bool = False) -> Client:
    """
    获取 Supabase 客户端的便捷函数
    
    Args:
        use_service_role: 是否使用服务角色
        
    Returns:
        Supabase 客户端
    """
    if use_service_role:
        return supabase_manager.service_client
    else:
        return supabase_manager.client

def test_supabase_connection() -> bool:
    """
    测试 Supabase 连接的便捷函数
    
    Returns:
        连接是否成功
    """
    result = supabase_manager.test_connection()
    return result['status'] == 'success'

if __name__ == "__main__":
    # 测试连接
    print("Testing Supabase connection...")
    
    try:
        manager = SupabaseConnectionManager()
        result = manager.test_connection()
        
        print(f"Connection test result: {result['status']}")
        if result['status'] == 'success':
            print("✅ Supabase connection successful")
            
            # 获取数据库信息
            db_info = manager.get_database_info()
            print(f"Database info: {json.dumps(db_info, indent=2)}")
        else:
            print(f"❌ Connection failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")
    
    finally:
        if 'manager' in locals():
            manager.close_connections()