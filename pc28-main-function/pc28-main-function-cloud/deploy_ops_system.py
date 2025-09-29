#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28运维管理系统部署脚本
用于在Google Cloud Platform上部署完整的运维管理系统
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

class PC28OpsDeployer:
    """PC28运维系统部署器"""
    
    def __init__(self, project_id: str = None, region: str = 'us-central1'):
        self.project_id = project_id or 'pc28-data-platform'
        self.region = region
        self.deployment_dir = os.path.dirname(__file__)
        
        # 设置日志
        self._setup_logging()
        
        # 部署配置
        self.deployment_config = {
            'cloud_functions': [
                {
                    'name': 'pc28-ops-monitor',
                    'source': 'monitoring_dashboard.py',
                    'entry_point': 'monitor_handler',
                    'runtime': 'python39',
                    'memory': '512MB',
                    'timeout': '300s',
                    'trigger': 'http'
                },
                {
                    'name': 'pc28-data-quality',
                    'source': 'data_quality_checker.py',
                    'entry_point': 'quality_check_handler',
                    'runtime': 'python39',
                    'memory': '1GB',
                    'timeout': '540s',
                    'trigger': 'pubsub',
                    'topic': 'pc28-quality-check'
                },
                {
                    'name': 'pc28-alert-system',
                    'source': 'alert_notification_system.py',
                    'entry_point': 'alert_handler',
                    'runtime': 'python39',
                    'memory': '256MB',
                    'timeout': '60s',
                    'trigger': 'pubsub',
                    'topic': 'pc28-alerts'
                }
            ],
            'cloud_scheduler': [
                {
                    'name': 'pc28-health-check',
                    'schedule': '*/15 * * * *',
                    'target_function': 'pc28-ops-monitor',
                    'payload': {'action': 'health_check'}
                },
                {
                    'name': 'pc28-quality-check',
                    'schedule': '0 */2 * * *',
                    'target_topic': 'pc28-quality-check',
                    'payload': {'action': 'full_check'}
                },
                {
                    'name': 'pc28-component-update',
                    'schedule': '0 6 * * *',
                    'target_function': 'pc28-ops-monitor',
                    'payload': {'action': 'check_updates'}
                }
            ],
            'pubsub_topics': [
                'pc28-quality-check',
                'pc28-alerts',
                'pc28-system-events'
            ],
            'cloud_storage': {
                'buckets': [
                    {
                        'name': f'{self.project_id}-ops-logs',
                        'location': self.region,
                        'storage_class': 'STANDARD'
                    },
                    {
                        'name': f'{self.project_id}-ops-backups',
                        'location': self.region,
                        'storage_class': 'NEARLINE'
                    }
                ]
            },
            'cloud_monitoring': {
                'dashboards': [
                    {
                        'name': 'PC28-Operations-Dashboard',
                        'config_file': 'monitoring_dashboard_config.json'
                    }
                ],
                'alert_policies': [
                    {
                        'name': 'PC28-High-Error-Rate',
                        'condition': 'error_rate > 5%',
                        'notification_channels': ['email', 'slack']
                    },
                    {
                        'name': 'PC28-Low-Data-Quality',
                        'condition': 'data_quality_score < 80',
                        'notification_channels': ['email']
                    }
                ]
            }
        }
        
        self.logger.info(f"PC28运维系统部署器初始化完成 - 项目: {self.project_id}, 区域: {self.region}")
    
    def _setup_logging(self):
        """设置日志系统"""
        log_dir = os.path.join(self.deployment_dir, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'deployment_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def run_command(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        """执行命令"""
        self.logger.info(f"执行命令: {command}")
        
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                check=check
            )
            
            if result.stdout:
                self.logger.info(f"输出: {result.stdout.strip()}")
            
            if result.stderr and result.returncode != 0:
                self.logger.error(f"错误: {result.stderr.strip()}")
            
            return result
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"命令执行失败: {e}")
            if e.stdout:
                self.logger.error(f"标准输出: {e.stdout}")
            if e.stderr:
                self.logger.error(f"错误输出: {e.stderr}")
            raise
    
    def check_prerequisites(self) -> bool:
        """检查部署前提条件"""
        self.logger.info("检查部署前提条件...")
        
        # 检查gcloud CLI
        try:
            result = self.run_command('gcloud version', check=False)
            if result.returncode != 0:
                self.logger.error("gcloud CLI未安装或配置不正确")
                return False
            self.logger.info("✅ gcloud CLI已安装")
        except Exception as e:
            self.logger.error(f"检查gcloud CLI失败: {e}")
            return False
        
        # 检查项目访问权限
        try:
            result = self.run_command(f'gcloud config set project {self.project_id}')
            self.logger.info(f"✅ 项目 {self.project_id} 访问正常")
        except Exception as e:
            self.logger.error(f"无法访问项目 {self.project_id}: {e}")
            return False
        
        # 检查必要的API是否启用
        required_apis = [
            'cloudfunctions.googleapis.com',
            'cloudscheduler.googleapis.com',
            'pubsub.googleapis.com',
            'storage.googleapis.com',
            'monitoring.googleapis.com',
            'bigquery.googleapis.com'
        ]
        
        for api in required_apis:
            try:
                result = self.run_command(f'gcloud services list --enabled --filter="name:{api}" --format="value(name)"')
                if api not in result.stdout:
                    self.logger.warning(f"API {api} 未启用，正在启用...")
                    self.run_command(f'gcloud services enable {api}')
                    time.sleep(5)  # 等待API启用
                self.logger.info(f"✅ API {api} 已启用")
            except Exception as e:
                self.logger.error(f"检查/启用API {api} 失败: {e}")
                return False
        
        # 检查必要文件
        required_files = [
            'ops_manager_main.py',
            'monitoring_dashboard.py',
            'data_quality_checker.py',
            'alert_notification_system.py',
            'concurrency_tuner.py',
            'component_updater.py',
            'config/ops_config.json'
        ]
        
        for file_path in required_files:
            full_path = os.path.join(self.deployment_dir, file_path)
            if not os.path.exists(full_path):
                self.logger.error(f"必要文件不存在: {file_path}")
                return False
            self.logger.info(f"✅ 文件存在: {file_path}")
        
        self.logger.info("✅ 所有前提条件检查通过")
        return True
    
    def create_requirements_txt(self):
        """创建requirements.txt文件"""
        requirements = [
            'google-cloud-bigquery>=3.4.0',
            'google-cloud-storage>=2.7.0',
            'google-cloud-monitoring>=2.11.1',
            'google-cloud-pubsub>=2.15.1',
            'google-cloud-functions>=1.8.1',
            'requests>=2.28.0',
            'pandas>=1.5.0',
            'numpy>=1.21.0',
            'psutil>=5.9.0',
            'schedule>=1.2.0',
            'slack-sdk>=3.19.0',
            'functions-framework>=3.2.0'
        ]
        
        requirements_file = os.path.join(self.deployment_dir, 'requirements.txt')
        with open(requirements_file, 'w') as f:
            f.write('\n'.join(requirements))
        
        self.logger.info(f"✅ 创建requirements.txt: {requirements_file}")
    
    def deploy_pubsub_topics(self):
        """部署Pub/Sub主题"""
        self.logger.info("部署Pub/Sub主题...")
        
        for topic in self.deployment_config['pubsub_topics']:
            try:
                # 检查主题是否存在
                result = self.run_command(
                    f'gcloud pubsub topics describe {topic} --project={self.project_id}',
                    check=False
                )
                
                if result.returncode != 0:
                    # 创建主题
                    self.run_command(f'gcloud pubsub topics create {topic} --project={self.project_id}')
                    self.logger.info(f"✅ 创建Pub/Sub主题: {topic}")
                else:
                    self.logger.info(f"✅ Pub/Sub主题已存在: {topic}")
                    
            except Exception as e:
                self.logger.error(f"部署Pub/Sub主题 {topic} 失败: {e}")
                raise
    
    def deploy_cloud_storage(self):
        """部署Cloud Storage存储桶"""
        self.logger.info("部署Cloud Storage存储桶...")
        
        for bucket_config in self.deployment_config['cloud_storage']['buckets']:
            bucket_name = bucket_config['name']
            
            try:
                # 检查存储桶是否存在
                result = self.run_command(
                    f'gsutil ls -b gs://{bucket_name}',
                    check=False
                )
                
                if result.returncode != 0:
                    # 创建存储桶
                    location = bucket_config.get('location', self.region)
                    storage_class = bucket_config.get('storage_class', 'STANDARD')
                    
                    self.run_command(
                        f'gsutil mb -p {self.project_id} -c {storage_class} -l {location} gs://{bucket_name}'
                    )
                    self.logger.info(f"✅ 创建存储桶: {bucket_name}")
                else:
                    self.logger.info(f"✅ 存储桶已存在: {bucket_name}")
                    
            except Exception as e:
                self.logger.error(f"部署存储桶 {bucket_name} 失败: {e}")
                raise
    
    def create_cloud_function_handler(self, function_config: Dict[str, Any]):
        """为Cloud Function创建处理函数"""
        function_name = function_config['name']
        source_file = function_config['source']
        entry_point = function_config['entry_point']
        
        # 创建main.py文件
        main_py_content = f"""
import json
import logging
from typing import Any, Dict

# 导入源模块
try:
    from {source_file.replace('.py', '')} import *
except ImportError as e:
    logging.error(f"导入模块失败: {{e}}")
    raise

def {entry_point}(request=None, context=None):
    \"\"\"Cloud Function入口点\"\"\"
    try:
        # 解析请求数据
        if hasattr(request, 'get_json'):
            data = request.get_json() or {{}}
        elif hasattr(request, 'data'):
            data = json.loads(request.data.decode('utf-8')) if request.data else {{}}
        else:
            data = {{}}
        
        logging.info(f"处理请求: {{data}}")
        
        # 根据不同的函数执行相应逻辑
        if '{function_name}' == 'pc28-ops-monitor':
            return handle_monitoring_request(data)
        elif '{function_name}' == 'pc28-data-quality':
            return handle_quality_check_request(data)
        elif '{function_name}' == 'pc28-alert-system':
            return handle_alert_request(data)
        else:
            return {{'error': 'Unknown function'}}
            
    except Exception as e:
        logging.error(f"处理请求失败: {{e}}")
        return {{'error': str(e)}}

def handle_monitoring_request(data: Dict[str, Any]):
    \"\"\"处理监控请求\"\"\"
    action = data.get('action', 'health_check')
    
    if action == 'health_check':
        dashboard = MonitoringDashboard()
        health = dashboard.get_system_health()
        return {{'status': 'success', 'data': health}}
    elif action == 'check_updates':
        updater = ComponentUpdater()
        updates = updater.check_updates()
        return {{'status': 'success', 'data': updates}}
    else:
        return {{'error': f'Unknown action: {{action}}'}}

def handle_quality_check_request(data: Dict[str, Any]):
    \"\"\"处理数据质量检查请求\"\"\"
    action = data.get('action', 'full_check')
    
    if action == 'full_check':
        checker = DataQualityChecker()
        issues = checker.check_all_tables()
        return {{'status': 'success', 'issues_count': len(issues), 'issues': issues}}
    else:
        return {{'error': f'Unknown action: {{action}}'}}

def handle_alert_request(data: Dict[str, Any]):
    \"\"\"处理告警请求\"\"\"
    alert_system = AlertNotificationSystem()
    
    alert_type = data.get('type', 'system')
    level = data.get('level', 'medium')
    title = data.get('title', '系统告警')
    message = data.get('message', '检测到系统异常')
    source = data.get('source', 'cloud_function')
    
    alert_system.create_alert(
        alert_type=alert_type,
        level=level,
        title=title,
        message=message,
        source=source,
        metadata=data.get('metadata', {{}})
    )
    
    return {{'status': 'success', 'message': 'Alert sent'}}
"""
        
        # 创建函数目录
        function_dir = os.path.join(self.deployment_dir, 'functions', function_name)
        os.makedirs(function_dir, exist_ok=True)
        
        # 写入main.py
        with open(os.path.join(function_dir, 'main.py'), 'w', encoding='utf-8') as f:
            f.write(main_py_content)
        
        # 复制源文件
        source_path = os.path.join(self.deployment_dir, source_file)
        if os.path.exists(source_path):
            import shutil
            shutil.copy2(source_path, function_dir)
        
        # 复制requirements.txt
        requirements_path = os.path.join(self.deployment_dir, 'requirements.txt')
        if os.path.exists(requirements_path):
            import shutil
            shutil.copy2(requirements_path, function_dir)
        
        # 复制配置文件
        config_dir = os.path.join(self.deployment_dir, 'config')
        if os.path.exists(config_dir):
            import shutil
            shutil.copytree(config_dir, os.path.join(function_dir, 'config'), dirs_exist_ok=True)
        
        self.logger.info(f"✅ 创建Cloud Function处理函数: {function_name}")
        return function_dir
    
    def deploy_cloud_functions(self):
        """部署Cloud Functions"""
        self.logger.info("部署Cloud Functions...")
        
        for function_config in self.deployment_config['cloud_functions']:
            function_name = function_config['name']
            
            try:
                # 创建函数处理代码
                function_dir = self.create_cloud_function_handler(function_config)
                
                # 构建部署命令
                deploy_cmd = [
                    'gcloud', 'functions', 'deploy', function_name,
                    f'--source={function_dir}',
                    f'--entry-point={function_config["entry_point"]}',
                    f'--runtime={function_config["runtime"]}',
                    f'--memory={function_config["memory"]}',
                    f'--timeout={function_config["timeout"]}',
                    f'--region={self.region}',
                    f'--project={self.project_id}'
                ]
                
                # 添加触发器
                if function_config['trigger'] == 'http':
                    deploy_cmd.extend(['--trigger-http', '--allow-unauthenticated'])
                elif function_config['trigger'] == 'pubsub':
                    deploy_cmd.extend([f'--trigger-topic={function_config["topic"]}'])
                
                # 执行部署
                self.run_command(' '.join(deploy_cmd))
                self.logger.info(f"✅ 部署Cloud Function: {function_name}")
                
            except Exception as e:
                self.logger.error(f"部署Cloud Function {function_name} 失败: {e}")
                raise
    
    def deploy_cloud_scheduler(self):
        """部署Cloud Scheduler任务"""
        self.logger.info("部署Cloud Scheduler任务...")
        
        for job_config in self.deployment_config['cloud_scheduler']:
            job_name = job_config['name']
            
            try:
                # 检查任务是否存在
                result = self.run_command(
                    f'gcloud scheduler jobs describe {job_name} --location={self.region} --project={self.project_id}',
                    check=False
                )
                
                if result.returncode == 0:
                    # 删除现有任务
                    self.run_command(
                        f'gcloud scheduler jobs delete {job_name} --location={self.region} --project={self.project_id} --quiet'
                    )
                
                # 创建新任务
                if 'target_function' in job_config:
                    # HTTP触发器
                    function_url = f'https://{self.region}-{self.project_id}.cloudfunctions.net/{job_config["target_function"]}'
                    payload = json.dumps(job_config['payload'])
                    
                    self.run_command(
                        f'gcloud scheduler jobs create http {job_name} '
                        f'--schedule="{job_config["schedule"]}" '
                        f'--uri="{function_url}" '
                        f'--http-method=POST '
                        f'--headers="Content-Type=application/json" '
                        f'--message-body=\'{payload}\' '
                        f'--location={self.region} '
                        f'--project={self.project_id}'
                    )
                elif 'target_topic' in job_config:
                    # Pub/Sub触发器
                    payload = json.dumps(job_config['payload'])
                    
                    self.run_command(
                        f'gcloud scheduler jobs create pubsub {job_name} '
                        f'--schedule="{job_config["schedule"]}" '
                        f'--topic={job_config["target_topic"]} '
                        f'--message-body=\'{payload}\' '
                        f'--location={self.region} '
                        f'--project={self.project_id}'
                    )
                
                self.logger.info(f"✅ 部署Scheduler任务: {job_name}")
                
            except Exception as e:
                self.logger.error(f"部署Scheduler任务 {job_name} 失败: {e}")
                raise
    
    def deploy_monitoring_dashboard(self):
        """部署监控仪表板"""
        self.logger.info("部署监控仪表板...")
        
        # 创建监控仪表板配置
        dashboard_config = {
            "displayName": "PC28 Operations Dashboard",
            "mosaicLayout": {
                "tiles": [
                    {
                        "width": 6,
                        "height": 4,
                        "widget": {
                            "title": "System Health Score",
                            "scorecard": {
                                "timeSeriesQuery": {
                                    "timeSeriesFilter": {
                                        "filter": 'resource.type="cloud_function"',
                                        "aggregation": {
                                            "alignmentPeriod": "60s",
                                            "perSeriesAligner": "ALIGN_MEAN"
                                        }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "width": 6,
                        "height": 4,
                        "xPos": 6,
                        "widget": {
                            "title": "Error Rate",
                            "xyChart": {
                                "dataSets": [{
                                    "timeSeriesQuery": {
                                        "timeSeriesFilter": {
                                            "filter": 'resource.type="cloud_function"',
                                            "aggregation": {
                                                "alignmentPeriod": "60s",
                                                "perSeriesAligner": "ALIGN_RATE"
                                            }
                                        }
                                    }
                                }]
                            }
                        }
                    }
                ]
            }
        }
        
        # 保存配置文件
        config_file = os.path.join(self.deployment_dir, 'monitoring_dashboard_config.json')
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(dashboard_config, f, ensure_ascii=False, indent=2)
        
        try:
            # 创建仪表板
            self.run_command(
                f'gcloud monitoring dashboards create --config-from-file={config_file} --project={self.project_id}'
            )
            self.logger.info("✅ 部署监控仪表板")
            
        except Exception as e:
            self.logger.warning(f"部署监控仪表板失败: {e}")
    
    def verify_deployment(self) -> Dict[str, Any]:
        """验证部署结果"""
        self.logger.info("验证部署结果...")
        
        verification_results = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'success',
            'components': {}
        }
        
        # 验证Cloud Functions
        try:
            result = self.run_command(
                f'gcloud functions list --regions={self.region} --project={self.project_id} --format="value(name)"'
            )
            deployed_functions = result.stdout.strip().split('\n') if result.stdout.strip() else []
            
            expected_functions = [f['name'] for f in self.deployment_config['cloud_functions']]
            
            verification_results['components']['cloud_functions'] = {
                'expected': len(expected_functions),
                'deployed': len([f for f in deployed_functions if any(ef in f for ef in expected_functions)]),
                'status': 'success' if all(any(ef in f for f in deployed_functions) for ef in expected_functions) else 'partial'
            }
            
        except Exception as e:
            verification_results['components']['cloud_functions'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # 验证Pub/Sub主题
        try:
            result = self.run_command(
                f'gcloud pubsub topics list --project={self.project_id} --format="value(name)"'
            )
            deployed_topics = [t.split('/')[-1] for t in result.stdout.strip().split('\n')] if result.stdout.strip() else []
            
            expected_topics = self.deployment_config['pubsub_topics']
            
            verification_results['components']['pubsub_topics'] = {
                'expected': len(expected_topics),
                'deployed': len([t for t in deployed_topics if t in expected_topics]),
                'status': 'success' if all(t in deployed_topics for t in expected_topics) else 'partial'
            }
            
        except Exception as e:
            verification_results['components']['pubsub_topics'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # 验证Cloud Scheduler任务
        try:
            result = self.run_command(
                f'gcloud scheduler jobs list --location={self.region} --project={self.project_id} --format="value(name)"'
            )
            deployed_jobs = [j.split('/')[-1] for j in result.stdout.strip().split('\n')] if result.stdout.strip() else []
            
            expected_jobs = [j['name'] for j in self.deployment_config['cloud_scheduler']]
            
            verification_results['components']['cloud_scheduler'] = {
                'expected': len(expected_jobs),
                'deployed': len([j for j in deployed_jobs if j in expected_jobs]),
                'status': 'success' if all(j in deployed_jobs for j in expected_jobs) else 'partial'
            }
            
        except Exception as e:
            verification_results['components']['cloud_scheduler'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # 验证存储桶
        try:
            result = self.run_command('gsutil ls')
            deployed_buckets = [b.replace('gs://', '').rstrip('/') for b in result.stdout.strip().split('\n')] if result.stdout.strip() else []
            
            expected_buckets = [b['name'] for b in self.deployment_config['cloud_storage']['buckets']]
            
            verification_results['components']['cloud_storage'] = {
                'expected': len(expected_buckets),
                'deployed': len([b for b in deployed_buckets if b in expected_buckets]),
                'status': 'success' if all(b in deployed_buckets for b in expected_buckets) else 'partial'
            }
            
        except Exception as e:
            verification_results['components']['cloud_storage'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # 确定整体状态
        component_statuses = [comp.get('status', 'error') for comp in verification_results['components'].values()]
        if all(status == 'success' for status in component_statuses):
            verification_results['overall_status'] = 'success'
        elif any(status == 'success' for status in component_statuses):
            verification_results['overall_status'] = 'partial'
        else:
            verification_results['overall_status'] = 'failed'
        
        self.logger.info(f"部署验证完成，整体状态: {verification_results['overall_status']}")
        
        return verification_results
    
    def deploy_full_system(self) -> Dict[str, Any]:
        """部署完整系统"""
        self.logger.info("开始部署PC28运维管理系统...")
        
        deployment_report = {
            'start_time': datetime.now().isoformat(),
            'project_id': self.project_id,
            'region': self.region,
            'steps': [],
            'status': 'in_progress'
        }
        
        try:
            # 1. 检查前提条件
            self.logger.info("步骤 1/8: 检查前提条件")
            if not self.check_prerequisites():
                raise Exception("前提条件检查失败")
            deployment_report['steps'].append({'step': 1, 'name': '前提条件检查', 'status': 'success'})
            
            # 2. 创建requirements.txt
            self.logger.info("步骤 2/8: 创建依赖文件")
            self.create_requirements_txt()
            deployment_report['steps'].append({'step': 2, 'name': '创建依赖文件', 'status': 'success'})
            
            # 3. 部署Pub/Sub主题
            self.logger.info("步骤 3/8: 部署Pub/Sub主题")
            self.deploy_pubsub_topics()
            deployment_report['steps'].append({'step': 3, 'name': 'Pub/Sub主题部署', 'status': 'success'})
            
            # 4. 部署Cloud Storage
            self.logger.info("步骤 4/8: 部署Cloud Storage")
            self.deploy_cloud_storage()
            deployment_report['steps'].append({'step': 4, 'name': 'Cloud Storage部署', 'status': 'success'})
            
            # 5. 部署Cloud Functions
            self.logger.info("步骤 5/8: 部署Cloud Functions")
            self.deploy_cloud_functions()
            deployment_report['steps'].append({'step': 5, 'name': 'Cloud Functions部署', 'status': 'success'})
            
            # 6. 部署Cloud Scheduler
            self.logger.info("步骤 6/8: 部署Cloud Scheduler")
            self.deploy_cloud_scheduler()
            deployment_report['steps'].append({'step': 6, 'name': 'Cloud Scheduler部署', 'status': 'success'})
            
            # 7. 部署监控仪表板
            self.logger.info("步骤 7/8: 部署监控仪表板")
            self.deploy_monitoring_dashboard()
            deployment_report['steps'].append({'step': 7, 'name': '监控仪表板部署', 'status': 'success'})
            
            # 8. 验证部署
            self.logger.info("步骤 8/8: 验证部署结果")
            verification_results = self.verify_deployment()
            deployment_report['verification'] = verification_results
            deployment_report['steps'].append({'step': 8, 'name': '部署验证', 'status': 'success'})
            
            deployment_report['status'] = 'success'
            deployment_report['end_time'] = datetime.now().isoformat()
            
            self.logger.info("✅ PC28运维管理系统部署完成")
            
        except Exception as e:
            deployment_report['status'] = 'failed'
            deployment_report['error'] = str(e)
            deployment_report['end_time'] = datetime.now().isoformat()
            
            self.logger.error(f"❌ 部署失败: {e}")
            raise
        
        return deployment_report

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28运维管理系统部署工具')
    parser.add_argument('--project-id', type=str, help='Google Cloud项目ID')
    parser.add_argument('--region', type=str, default='us-central1', help='部署区域')
    parser.add_argument('--verify-only', action='store_true', help='仅验证现有部署')
    
    args = parser.parse_args()
    
    try:
        deployer = PC28OpsDeployer(args.project_id, args.region)
        
        if args.verify_only:
            print("验证现有部署...")
            results = deployer.verify_deployment()
            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            print("开始完整系统部署...")
            report = deployer.deploy_full_system()
            
            # 保存部署报告
            report_file = os.path.join(
                os.path.dirname(__file__),
                'logs',
                f'deployment_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            
            os.makedirs(os.path.dirname(report_file), exist_ok=True)
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            print(f"\n部署报告已保存: {report_file}")
            print(f"部署状态: {report['status']}")
            
            if report['status'] == 'success':
                print("\n🎉 PC28运维管理系统部署成功！")
                print("\n下一步操作:")
                print("1. 配置告警通知渠道 (config/ops_config.json)")
                print("2. 运行健康检查: python ops_manager_main.py health")
                print("3. 启动监控服务: python ops_manager_main.py start")
            else:
                print(f"\n❌ 部署失败: {report.get('error', '未知错误')}")
                sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n部署已取消")
    except Exception as e:
        print(f"❌ 部署失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()