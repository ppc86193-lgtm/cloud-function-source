#!/usr/bin/env python3
"""
PC28 Cloud Function 主函数
集成数据采集、处理和推送功能
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

# 导入本地模块
try:
    from api_auto_fetch import PC28DataFetcher
except ImportError:
    # 如果在Cloud Function环境中，可能需要不同的导入方式
    PC28DataFetcher = None

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def pc28_main_handler(request) -> Dict[str, Any]:
    """
    Cloud Function 主处理函数
    """
    try:
        logger.info("PC28 Cloud Function 启动")
        
        # 解析请求参数
        request_json = request.get_json(silent=True)
        request_args = request.args
        
        # 获取操作类型
        action = (request_json.get('action') if request_json else None) or request_args.get('action', 'fetch_data')
        
        logger.info(f"执行操作: {action}")
        
        if action == 'fetch_data':
            return handle_fetch_data()
        elif action == 'health_check':
            return handle_health_check()
        elif action == 'push_telegram':
            return handle_telegram_push()
        else:
            return {
                'status': 'error',
                'message': f'未知操作: {action}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
    except Exception as e:
        logger.error(f"Cloud Function 执行异常: {e}")
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

def handle_fetch_data() -> Dict[str, Any]:
    """处理数据获取请求"""
    try:
        if PC28DataFetcher:
            fetcher = PC28DataFetcher()
            success = fetcher.run_fetch_cycle()
            
            return {
                'status': 'success' if success else 'error',
                'message': '数据获取完成' if success else '数据获取失败',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        else:
            return {
                'status': 'error',
                'message': 'PC28DataFetcher 模块未找到',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
    except Exception as e:
        logger.error(f"数据获取处理异常: {e}")
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

def handle_health_check() -> Dict[str, Any]:
    """处理健康检查请求"""
    try:
        # 简单的健康检查
        return {
            'status': 'healthy',
            'message': 'Cloud Function 运行正常',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'version': '1.0'
        }
        
    except Exception as e:
        logger.error(f"健康检查异常: {e}")
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

def handle_telegram_push() -> Dict[str, Any]:
    """处理Telegram推送请求"""
    try:
        import subprocess
        
        # 执行Telegram推送脚本
        result = subprocess.run(
            ['bash', 'pc28_tg_realtime_bridge.sh'],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        return {
            'status': 'success' if result.returncode == 0 else 'error',
            'message': 'Telegram推送完成' if result.returncode == 0 else 'Telegram推送失败',
            'output': result.stdout,
            'error': result.stderr if result.returncode != 0 else None,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Telegram推送处理异常: {e}")
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

# Cloud Function 入口点
def main(request):
    """Cloud Function 入口点"""
    return pc28_main_handler(request)

# 本地测试入口点
if __name__ == "__main__":
    class MockRequest:
        def __init__(self):
            self.args = {'action': 'health_check'}
        
        def get_json(self, silent=True):
            return None
    
    result = main(MockRequest())
    print(json.dumps(result, indent=2, ensure_ascii=False))