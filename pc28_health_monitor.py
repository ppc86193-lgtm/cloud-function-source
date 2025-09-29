#!/usr/bin/env python3
"""
PC28 系统健康监控和报警脚本
检查数据流、API状态、BigQuery表状态等
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from typing import Dict, List, Any, Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/health_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PC28HealthMonitor:
    def __init__(self):
        self.project_id = 'wprojectl'
        self.dataset_id = 'pc28_lab'
        
        # Telegram配置
        self.bot_token = os.getenv('BOT_TOKEN', '')
        self.chat_id = os.getenv('CHAT_ID', '')
        
        # 初始化BigQuery客户端
        try:
            self.bq_client = bigquery.Client(project=self.project_id)
            logger.info("BigQuery客户端初始化成功")
        except Exception as e:
            logger.error(f"BigQuery客户端初始化失败: {e}")
            self.bq_client = None
    
    def check_api_status(self) -> Dict[str, Any]:
        """检查API状态"""
        try:
            api_url = "https://rijb.api.storeapi.net/api/119/259"
            
            # 构建测试请求
            current_time = str(int(time.time()))
            params = {
                'appid': '45928',
                'format': 'json',
                'time': current_time
            }
            
            # 生成签名
            import hashlib
            wapi_key = 'ca9edbfee35c22a0d6c4cf6722506af0'
            sorted_params = sorted(params.items())
            param_string = ''.join([f"{k}{v}" for k, v in sorted_params])
            param_string += wapi_key
            params['sign'] = hashlib.md5(param_string.encode('utf-8')).hexdigest()
            
            # 发送请求
            start_time = time.time()
            response = requests.get(api_url, params=params, timeout=10)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'status': 'healthy',
                    'response_time': response_time,
                    'status_code': response.status_code,
                    'message': data.get('message', ''),
                    'codeid': data.get('codeid', 0)
                }
            else:
                return {
                    'status': 'error',
                    'response_time': response_time,
                    'status_code': response.status_code,
                    'error': response.text
                }
                
        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e),
                'response_time': None
            }
    
    def check_bigquery_data_freshness(self) -> Dict[str, Any]:
        """检查BigQuery数据新鲜度 - 修复字段名"""
        if not self.bq_client:
            return {'status': 'error', 'error': 'BigQuery客户端未初始化'}
        
        try:
            # 检查主要数据表的最新数据 - 使用正确的字段名
            tables_config = {
                'draws_14w_clean': {
                    'timestamp_field': 'timestamp',
                    'description': '开奖数据表'
                },
                'p_cloud_clean_merged_dedup_v': {
                    'timestamp_field': 'ts_utc',
                    'description': '云端预测数据'
                },
                'signal_pool_union_v3': {
                    'timestamp_field': 'ts_utc',
                    'description': '信号池数据'
                }
            }
            
            results = {}
            
            for table, config in tables_config.items():
                try:
                    timestamp_field = config['timestamp_field']
                    query = f"""
                    SELECT 
                        MAX({timestamp_field}) as latest_timestamp,
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT DATE({timestamp_field}, 'Asia/Shanghai')) as days_covered
                    FROM `{self.project_id}.{self.dataset_id}.{table}`
                    WHERE DATE({timestamp_field}, 'Asia/Shanghai') >= DATE_SUB(CURRENT_DATE('Asia/Shanghai'), INTERVAL 7 DAY)
                    """
                    
                    query_job = self.bq_client.query(query)
                    rows = list(query_job)
                    
                    if rows:
                        row = rows[0]
                        latest_time = row.latest_timestamp
                        
                        # 计算数据延迟
                        if latest_time:
                            now = datetime.now(timezone.utc)
                            delay = now - latest_time
                            delay_minutes = delay.total_seconds() / 60
                            
                            # 根据表类型设置不同的延迟阈值
                            if table == 'draws_14w_clean':
                                threshold = 5  # 开奖数据5分钟内
                            else:
                                threshold = 15  # 其他数据15分钟内
                            
                            status = 'healthy' if delay_minutes < threshold else 'stale'
                        else:
                            status = 'no_data'
                            delay_minutes = None
                        
                        results[table] = {
                            'status': status,
                            'description': config['description'],
                            'latest_timestamp': latest_time.isoformat() if latest_time else None,
                            'delay_minutes': round(delay_minutes, 2) if delay_minutes else None,
                            'total_rows': row.total_rows,
                            'days_covered': row.days_covered,
                            'threshold_minutes': threshold
                        }
                    else:
                        results[table] = {
                            'status': 'no_data', 
                            'error': '查询无结果',
                            'description': config['description']
                        }
                        
                except Exception as e:
                    results[table] = {
                        'status': 'error', 
                        'error': str(e),
                        'description': config['description']
                    }
            
            return results
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def check_cloud_functions(self) -> Dict[str, Any]:
        """检查Cloud Functions状态"""
        try:
            import subprocess
            
            # 获取Cloud Functions列表
            result = subprocess.run(
                ['gcloud', 'functions', 'list', '--format=json'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                functions = json.loads(result.stdout)
                
                function_status = {}
                for func in functions:
                    name = func.get('name', '').split('/')[-1]
                    status = func.get('status', 'UNKNOWN')
                    update_time = func.get('updateTime', '')
                    
                    function_status[name] = {
                        'status': status,
                        'update_time': update_time,
                        'healthy': status == 'ACTIVE'
                    }
                
                return {
                    'status': 'success',
                    'functions': function_status,
                    'total_functions': len(functions),
                    'healthy_functions': sum(1 for f in function_status.values() if f['healthy'])
                }
            else:
                return {
                    'status': 'error',
                    'error': result.stderr
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def check_scheduler_jobs(self) -> Dict[str, Any]:
        """检查Cloud Scheduler作业状态"""
        try:
            import subprocess
            
            result = subprocess.run(
                ['gcloud', 'scheduler', 'jobs', 'list', '--format=json'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                jobs = json.loads(result.stdout)
                
                job_status = {}
                for job in jobs:
                    name = job.get('name', '').split('/')[-1]
                    state = job.get('state', 'UNKNOWN')
                    schedule = job.get('schedule', '')
                    
                    job_status[name] = {
                        'state': state,
                        'schedule': schedule,
                        'healthy': state == 'ENABLED'
                    }
                
                return {
                    'status': 'success',
                    'jobs': job_status,
                    'total_jobs': len(jobs),
                    'enabled_jobs': sum(1 for j in job_status.values() if j['healthy'])
                }
            else:
                return {
                    'status': 'error',
                    'error': result.stderr
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def send_alert(self, message: str) -> bool:
        """发送告警消息"""
        if not self.bot_token or not self.chat_id:
            logger.warning("Telegram配置未设置，无法发送告警")
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': f"🚨 *PC28系统告警*\n\n{message}",
                'parse_mode': 'Markdown'
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info("告警消息发送成功")
                return True
            else:
                logger.error(f"告警消息发送失败: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"发送告警异常: {e}")
            return False
    
    def generate_health_report(self) -> Dict[str, Any]:
        """生成健康检查报告"""
        logger.info("🔍 开始系统健康检查...")
        
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'overall_status': 'healthy',
            'checks': {}
        }
        
        issues = []
        
        # 1. 检查API状态
        logger.info("检查API状态...")
        api_status = self.check_api_status()
        report['checks']['api'] = api_status
        
        if api_status['status'] != 'healthy':
            issues.append(f"API状态异常: {api_status.get('error', 'Unknown')}")
        
        # 2. 检查BigQuery数据新鲜度
        logger.info("检查BigQuery数据新鲜度...")
        bq_status = self.check_bigquery_data_freshness()
        report['checks']['bigquery'] = bq_status
        
        if isinstance(bq_status, dict):
            for table, status in bq_status.items():
                if isinstance(status, dict) and status.get('status') != 'healthy':
                    issues.append(f"表 {table} 数据异常: {status.get('error', '数据过期')}")
        
        # 3. 检查Cloud Functions
        logger.info("检查Cloud Functions状态...")
        cf_status = self.check_cloud_functions()
        report['checks']['cloud_functions'] = cf_status
        
        if cf_status['status'] == 'success':
            unhealthy_functions = [
                name for name, info in cf_status['functions'].items() 
                if not info['healthy']
            ]
            if unhealthy_functions:
                issues.append(f"Cloud Functions异常: {', '.join(unhealthy_functions)}")
        
        # 4. 检查Scheduler作业
        logger.info("检查Cloud Scheduler状态...")
        scheduler_status = self.check_scheduler_jobs()
        report['checks']['scheduler'] = scheduler_status
        
        if scheduler_status['status'] == 'success':
            disabled_jobs = [
                name for name, info in scheduler_status['jobs'].items()
                if not info['healthy']
            ]
            if disabled_jobs:
                issues.append(f"Scheduler作业异常: {', '.join(disabled_jobs)}")
        
        # 确定整体状态
        if issues:
            report['overall_status'] = 'unhealthy'
            report['issues'] = issues
            
            # 发送告警
            alert_message = "系统健康检查发现问题:\n\n" + "\n".join([f"• {issue}" for issue in issues])
            self.send_alert(alert_message)
        
        return report
    
    def save_report(self, report: Dict[str, Any]) -> None:
        """保存健康检查报告"""
        try:
            report_file = f"logs/health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"健康检查报告已保存: {report_file}")
            
        except Exception as e:
            logger.error(f"保存报告失败: {e}")

def main():
    """主函数"""
    logger.info("🏥 PC28系统健康监控启动")
    
    # 确保日志目录存在
    os.makedirs('logs', exist_ok=True)
    
    try:
        monitor = PC28HealthMonitor()
        
        # 生成健康检查报告
        report = monitor.generate_health_report()
        
        # 保存报告
        monitor.save_report(report)
        
        # 打印摘要
        status = report['overall_status']
        if status == 'healthy':
            logger.info("✅ 系统健康状态良好")
            print("✅ 所有系统组件运行正常")
        else:
            logger.warning("⚠️ 系统存在健康问题")
            print("❌ 发现系统问题:")
            for issue in report.get('issues', []):
                print(f"  • {issue}")
        
        # 返回适当的退出码
        sys.exit(0 if status == 'healthy' else 1)
        
    except Exception as e:
        logger.error(f"健康检查异常: {e}")
        print(f"💥 健康检查失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()