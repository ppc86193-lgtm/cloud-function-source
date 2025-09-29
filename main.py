import functions_framework
import os
import sys
from flask import Flask

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

#!/usr/bin/env python3
"""
PC28 Cloud Function 入口文件
简化版本，专注于核心功能
"""

import json
import logging
import hashlib
import requests
import time
from datetime import datetime, timezone
from flask import Request

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_signature(params: dict, wapi_key: str) -> str:
    """生成API签名"""
    sorted_params = sorted(params.items())
    param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
    param_string += wapi_key
    return hashlib.md5(param_string.encode('utf-8')).hexdigest()

def fetch_pc28_data():
    """获取PC28数据"""
    try:
        api_url = "https://rijb.api.storeapi.net/api/119/259"
        wapi_key = "ca9edbfee35c22a0d6c4cf6722506af0"
        wapi_id = "45928"
        
        current_time = str(int(time.time()))
        params = {
            'appid': wapi_id,
            'format': 'json',
            'time': current_time
        }
        params['sign'] = generate_signature(params, wapi_key)
        
        response = requests.get(api_url, params=params, timeout=10)
        
        if response.status_code == 200:
            return {
                'status': 'success',
                'data': response.json(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        else:
            return {
                'status': 'error',
                'message': f'API请求失败: {response.status_code}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

def pc28_main(request=None):
    """Cloud Function 主入口"""
    try:
        logger.info("PC28 Cloud Function 启动")
        
        # 获取请求参数
        if request and hasattr(request, 'method'):
            if request.method == 'POST':
                request_json = request.get_json(silent=True)
                action = request_json.get('action', 'fetch_data') if request_json else 'fetch_data'
            else:
                action = request.args.get('action', 'fetch_data')
        else:
            action = 'fetch_data'
        
        logger.info(f"执行操作: {action}")
        
        if action == 'fetch_data':
            result = fetch_pc28_data()
        elif action == 'health_check':
            result = {
                'status': 'healthy',
                'message': 'Cloud Function 运行正常',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        else:
            result = {
                'status': 'error',
                'message': f'未知操作: {action}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        return result
        
    except Exception as e:
        logger.error(f"Cloud Function 异常: {e}")
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

# 本地测试
if __name__ == "__main__":
    class MockRequest:
        def __init__(self):
            self.method = 'GET'
            self.args = {'action': 'fetch_data'}
        
        def get_json(self, silent=True):
            return None
    
    result = pc28_main(MockRequest())
    print(json.dumps(result, ensure_ascii=False, indent=2))

# 创建Flask应用以支持健康检查
app = Flask(__name__)

@app.route('/')
def health_check():
    """健康检查端点"""
    return {'status': 'healthy', 'service': 'pc28-e2e-function'}, 200

@functions_framework.http
def pc28_trigger(request):
    """Cloud Function入口点"""
    try:
        # 设置环境变量
        os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'wprojectl')
        
        # 执行主程序
        result = pc28_main()
        return {'status': 'success', 'result': result}, 200
    except Exception as e:
        import traceback
        error_msg = f"Error: {str(e)}\nTraceback: {traceback.format_exc()}"
        print(error_msg)
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == "__main__":
    # 本地运行时启动Flask服务器
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
