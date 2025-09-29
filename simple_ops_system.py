#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 运维管理系统 - 简化版本
用于本地测试和演示
"""

import os
import sys
import json
import time
import psutil
import logging
from datetime import datetime
from typing import Dict, List, Any
from flask import Flask, jsonify, render_template_string
from flask_cors import CORS

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleOpsSystem:
    """简化版运维管理系统"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.config = self._load_config()
        self.metrics = {
            'requests_count': 0,
            'errors_count': 0,
            'last_health_check': None
        }
        logger.info("简化版运维系统初始化完成")
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置"""
        default_config = {
            'system_name': 'PC28 运维管理系统',
            'version': '1.0.0',
            'monitoring': {
                'enabled': True,
                'check_interval': 60
            },
            'data_quality': {
                'enabled': True,
                'check_interval': 300
            },
            'concurrency': {
                'max_workers': 4,
                'queue_size': 100
            },
            'alerts': {
                'enabled': True,
                'email_notifications': False
            }
        }
        
        config_file = 'config/system_config.json'
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    loaded_config = json.load(f)
                    default_config.update(loaded_config)
                    logger.info(f"配置文件加载成功: {config_file}")
            except Exception as e:
                logger.warning(f"配置文件加载失败，使用默认配置: {e}")
        
        return default_config
    
    def get_system_health(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        try:
            # 获取系统资源信息
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # 计算运行时间
            uptime = datetime.now() - self.start_time
            uptime_seconds = int(uptime.total_seconds())
            
            health_status = {
                'overall_health': 'healthy' if cpu_percent < 80 and memory.percent < 80 else 'warning',
                'timestamp': datetime.now().isoformat(),
                'uptime_seconds': uptime_seconds,
                'system_resources': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_available_gb': round(memory.available / (1024**3), 2),
                    'disk_percent': disk.percent,
                    'disk_free_gb': round(disk.free / (1024**3), 2)
                },
                'metrics': self.metrics
            }
            
            self.metrics['last_health_check'] = datetime.now().isoformat()
            logger.info("系统健康检查完成")
            return health_status
            
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {
                'overall_health': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            status = {
                'system_info': {
                    'name': self.config['system_name'],
                    'version': self.config['version'],
                    'running': True,
                    'start_time': self.start_time.isoformat()
                },
                'modules': {
                    'monitoring': self.config['monitoring']['enabled'],
                    'data_quality': self.config['data_quality']['enabled'],
                    'concurrency_tuning': True,
                    'component_updates': True,
                    'alert_system': self.config['alerts']['enabled']
                },
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info("系统状态获取完成")
            return status
            
        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_data_quality_check(self) -> Dict[str, Any]:
        """运行数据质量检查"""
        try:
            # 模拟数据质量检查
            checks = [
                {'name': '数据完整性检查', 'status': 'passed', 'score': 95},
                {'name': '数据一致性检查', 'status': 'passed', 'score': 92},
                {'name': '数据准确性检查', 'status': 'warning', 'score': 88},
                {'name': '数据时效性检查', 'status': 'passed', 'score': 96}
            ]
            
            overall_score = sum(check['score'] for check in checks) / len(checks)
            
            result = {
                'overall_score': round(overall_score, 2),
                'status': 'passed' if overall_score >= 90 else 'warning',
                'checks': checks,
                'timestamp': datetime.now().isoformat(),
                'recommendations': [
                    '建议优化数据准确性检查规则',
                    '定期清理过期数据',
                    '加强数据验证机制'
                ]
            }
            
            logger.info(f"数据质量检查完成，总分: {overall_score}")
            return result
            
        except Exception as e:
            logger.error(f"数据质量检查失败: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_concurrency_recommendations(self) -> Dict[str, Any]:
        """获取并发参数调优建议"""
        try:
            # 获取当前系统负载
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            
            # 基于系统负载给出建议
            recommendations = []
            current_workers = self.config['concurrency']['max_workers']
            
            if cpu_percent > 80:
                recommendations.append({
                    'type': 'reduce_workers',
                    'message': f'CPU使用率过高({cpu_percent}%)，建议减少工作线程数',
                    'suggested_value': max(2, current_workers - 1)
                })
            elif cpu_percent < 30 and memory_percent < 50:
                recommendations.append({
                    'type': 'increase_workers',
                    'message': f'系统资源充足，建议增加工作线程数以提高并发性能',
                    'suggested_value': min(8, current_workers + 1)
                })
            
            if memory_percent > 85:
                recommendations.append({
                    'type': 'reduce_queue_size',
                    'message': f'内存使用率过高({memory_percent}%)，建议减少队列大小',
                    'suggested_value': max(50, self.config['concurrency']['queue_size'] - 20)
                })
            
            result = {
                'current_config': self.config['concurrency'],
                'system_metrics': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory_percent
                },
                'recommendations': recommendations,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"并发参数分析完成，生成{len(recommendations)}条建议")
            return result
            
        except Exception as e:
            logger.error(f"并发参数分析失败: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_component_status(self) -> Dict[str, Any]:
        """获取组件状态和更新建议"""
        try:
            # 模拟组件状态检查
            components = [
                {
                    'name': '监控仪表板',
                    'version': '1.2.3',
                    'status': 'running',
                    'last_updated': '2024-01-15',
                    'update_available': True,
                    'latest_version': '1.3.0'
                },
                {
                    'name': '数据质量检查器',
                    'version': '2.1.0',
                    'status': 'running',
                    'last_updated': '2024-01-20',
                    'update_available': False,
                    'latest_version': '2.1.0'
                },
                {
                    'name': '并发调优器',
                    'version': '1.0.5',
                    'status': 'running',
                    'last_updated': '2024-01-10',
                    'update_available': True,
                    'latest_version': '1.1.0'
                },
                {
                    'name': '告警通知系统',
                    'version': '3.0.2',
                    'status': 'running',
                    'last_updated': '2024-01-25',
                    'update_available': False,
                    'latest_version': '3.0.2'
                }
            ]
            
            updates_available = sum(1 for comp in components if comp['update_available'])
            
            result = {
                'components': components,
                'summary': {
                    'total_components': len(components),
                    'running_components': len([c for c in components if c['status'] == 'running']),
                    'updates_available': updates_available
                },
                'recommendations': [
                    '建议在低峰期进行组件更新',
                    '更新前请备份当前配置',
                    '逐个更新组件以确保系统稳定性'
                ] if updates_available > 0 else ['所有组件都是最新版本'],
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"组件状态检查完成，{updates_available}个组件有更新")
            return result
            
        except Exception as e:
            logger.error(f"组件状态检查失败: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_end_to_end_test(self) -> Dict[str, Any]:
        """运行端到端测试"""
        try:
            test_results = []
            
            # 测试1: 系统健康检查
            health = self.get_system_health()
            test_results.append({
                'test_name': '系统健康检查',
                'status': 'passed' if health.get('overall_health') != 'error' else 'failed',
                'details': f"系统状态: {health.get('overall_health', 'unknown')}"
            })
            
            # 测试2: 数据质量检查
            data_quality = self.run_data_quality_check()
            test_results.append({
                'test_name': '数据质量检查',
                'status': 'passed' if data_quality.get('status') != 'error' else 'failed',
                'details': f"质量分数: {data_quality.get('overall_score', 'N/A')}"
            })
            
            # 测试3: 并发参数分析
            concurrency = self.get_concurrency_recommendations()
            test_results.append({
                'test_name': '并发参数分析',
                'status': 'passed' if 'error' not in concurrency else 'failed',
                'details': f"生成建议: {len(concurrency.get('recommendations', []))}条"
            })
            
            # 测试4: 组件状态检查
            components = self.get_component_status()
            test_results.append({
                'test_name': '组件状态检查',
                'status': 'passed' if 'error' not in components else 'failed',
                'details': f"运行组件: {components.get('summary', {}).get('running_components', 0)}"
            })
            
            passed_tests = len([t for t in test_results if t['status'] == 'passed'])
            total_tests = len(test_results)
            
            result = {
                'overall_status': 'passed' if passed_tests == total_tests else 'partial',
                'passed_tests': passed_tests,
                'total_tests': total_tests,
                'test_results': test_results,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"端到端测试完成: {passed_tests}/{total_tests} 通过")
            return result
            
        except Exception as e:
            logger.error(f"端到端测试失败: {e}")
            return {
                'overall_status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

# Flask Web应用
app = Flask(__name__)
CORS(app)
ops_system = SimpleOpsSystem()

# HTML模板
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PC28 运维管理系统</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }
        .card { background: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .status-good { color: #28a745; }
        .status-warning { color: #ffc107; }
        .status-error { color: #dc3545; }
        .metric { display: inline-block; margin: 10px 20px 10px 0; }
        .metric-label { font-weight: bold; }
        .btn { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; margin: 5px; }
        .btn:hover { background: #0056b3; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 PC28 运维管理系统</h1>
            <p>实时监控 • 数据质量 • 并发优化 • 组件管理</p>
        </div>
        
        <div class="grid">
            <div class="card">
                <h3>📊 系统健康状态</h3>
                <div id="health-status">加载中...</div>
                <button class="btn" onclick="refreshHealth()">刷新状态</button>
            </div>
            
            <div class="card">
                <h3>🔍 数据质量检查</h3>
                <div id="data-quality">加载中...</div>
                <button class="btn" onclick="runDataQualityCheck()">运行检查</button>
            </div>
            
            <div class="card">
                <h3>⚡ 并发参数优化</h3>
                <div id="concurrency-info">加载中...</div>
                <button class="btn" onclick="getConcurrencyRecommendations()">获取建议</button>
            </div>
            
            <div class="card">
                <h3>🔧 组件管理</h3>
                <div id="component-status">加载中...</div>
                <button class="btn" onclick="checkComponents()">检查组件</button>
            </div>
        </div>
        
        <div class="card">
            <h3>🧪 端到端测试</h3>
            <div id="e2e-test">点击按钮运行完整测试</div>
            <button class="btn" onclick="runE2ETest()">运行端到端测试</button>
        </div>
    </div>
    
    <script>
        function refreshHealth() {
            fetch('/api/health')
                .then(response => response.json())
                .then(data => {
                    const status = data.overall_health;
                    const statusClass = status === 'healthy' ? 'status-good' : status === 'warning' ? 'status-warning' : 'status-error';
                    document.getElementById('health-status').innerHTML = `
                        <div class="${statusClass}">状态: ${status}</div>
                        <div class="metric"><span class="metric-label">CPU:</span> ${data.system_resources?.cpu_percent || 'N/A'}%</div>
                        <div class="metric"><span class="metric-label">内存:</span> ${data.system_resources?.memory_percent || 'N/A'}%</div>
                        <div class="metric"><span class="metric-label">运行时间:</span> ${Math.floor((data.uptime_seconds || 0) / 60)}分钟</div>
                    `;
                })
                .catch(error => {
                    document.getElementById('health-status').innerHTML = '<div class="status-error">获取状态失败</div>';
                });
        }
        
        function runDataQualityCheck() {
            fetch('/api/data-quality')
                .then(response => response.json())
                .then(data => {
                    const status = data.status;
                    const statusClass = status === 'passed' ? 'status-good' : status === 'warning' ? 'status-warning' : 'status-error';
                    document.getElementById('data-quality').innerHTML = `
                        <div class="${statusClass}">状态: ${status}</div>
                        <div class="metric"><span class="metric-label">总分:</span> ${data.overall_score || 'N/A'}</div>
                        <div class="metric"><span class="metric-label">检查项:</span> ${data.checks?.length || 0}</div>
                    `;
                })
                .catch(error => {
                    document.getElementById('data-quality').innerHTML = '<div class="status-error">检查失败</div>';
                });
        }
        
        function getConcurrencyRecommendations() {
            fetch('/api/concurrency')
                .then(response => response.json())
                .then(data => {
                    const recommendations = data.recommendations || [];
                    document.getElementById('concurrency-info').innerHTML = `
                        <div class="metric"><span class="metric-label">当前工作线程:</span> ${data.current_config?.max_workers || 'N/A'}</div>
                        <div class="metric"><span class="metric-label">队列大小:</span> ${data.current_config?.queue_size || 'N/A'}</div>
                        <div class="metric"><span class="metric-label">优化建议:</span> ${recommendations.length}条</div>
                    `;
                })
                .catch(error => {
                    document.getElementById('concurrency-info').innerHTML = '<div class="status-error">获取建议失败</div>';
                });
        }
        
        function checkComponents() {
            fetch('/api/components')
                .then(response => response.json())
                .then(data => {
                    const summary = data.summary || {};
                    document.getElementById('component-status').innerHTML = `
                        <div class="metric"><span class="metric-label">总组件:</span> ${summary.total_components || 0}</div>
                        <div class="metric"><span class="metric-label">运行中:</span> ${summary.running_components || 0}</div>
                        <div class="metric"><span class="metric-label">可更新:</span> ${summary.updates_available || 0}</div>
                    `;
                })
                .catch(error => {
                    document.getElementById('component-status').innerHTML = '<div class="status-error">检查失败</div>';
                });
        }
        
        function runE2ETest() {
            document.getElementById('e2e-test').innerHTML = '测试运行中...';
            fetch('/api/e2e-test')
                .then(response => response.json())
                .then(data => {
                    const status = data.overall_status;
                    const statusClass = status === 'passed' ? 'status-good' : status === 'partial' ? 'status-warning' : 'status-error';
                    document.getElementById('e2e-test').innerHTML = `
                        <div class="${statusClass}">测试结果: ${status}</div>
                        <div class="metric"><span class="metric-label">通过:</span> ${data.passed_tests || 0}/${data.total_tests || 0}</div>
                    `;
                })
                .catch(error => {
                    document.getElementById('e2e-test').innerHTML = '<div class="status-error">测试失败</div>';
                });
        }
        
        // 页面加载时自动刷新状态
        window.onload = function() {
            refreshHealth();
            runDataQualityCheck();
            getConcurrencyRecommendations();
            checkComponents();
        };
        
        // 每30秒自动刷新健康状态
        setInterval(refreshHealth, 30000);
    </script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    """主仪表板"""
    ops_system.metrics['requests_count'] += 1
    return render_template_string(DASHBOARD_TEMPLATE)

@app.route('/api/health')
def api_health():
    """健康检查API"""
    ops_system.metrics['requests_count'] += 1
    return jsonify(ops_system.get_system_health())

@app.route('/api/status')
def api_status():
    """系统状态API"""
    ops_system.metrics['requests_count'] += 1
    return jsonify(ops_system.get_system_status())

@app.route('/api/data-quality')
def api_data_quality():
    """数据质量检查API"""
    ops_system.metrics['requests_count'] += 1
    return jsonify(ops_system.run_data_quality_check())

@app.route('/api/concurrency')
def api_concurrency():
    """并发参数建议API"""
    ops_system.metrics['requests_count'] += 1
    return jsonify(ops_system.get_concurrency_recommendations())

@app.route('/api/components')
def api_components():
    """组件状态API"""
    ops_system.metrics['requests_count'] += 1
    return jsonify(ops_system.get_component_status())

@app.route('/api/e2e-test')
def api_e2e_test():
    """端到端测试API"""
    ops_system.metrics['requests_count'] += 1
    return jsonify(ops_system.run_end_to_end_test())

if __name__ == '__main__':
    print("="*60)
    print("🚀 PC28 运维管理系统启动中...")
    print("="*60)
    print(f"📊 系统版本: {ops_system.config['version']}")
    print(f"🌐 访问地址: http://localhost:8080")
    print(f"📈 API文档: http://localhost:8080/api/health")
    print("="*60)
    
    try:
        app.run(host='0.0.0.0', port=8080, debug=False)
    except KeyboardInterrupt:
        print("\n👋 系统已停止")
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        sys.exit(1)