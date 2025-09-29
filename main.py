import functions_framework
import os
import sys
from flask import Flask

# 添加python目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

from main_pc28_e2e import main as pc28_main

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
