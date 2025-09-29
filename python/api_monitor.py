#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PC28 上游API监控模块"""

import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pc28_upstream_api import PC28UpstreamAPI

@dataclass
class APIHealthStatus:
    """API健康状态数据类"""
    timestamp: str
    api_type: str  # 'realtime' or 'history'
    status: str  # 'healthy', 'degraded', 'unhealthy'
    response_time_ms: float
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None

class APIMonitor:
    """API监控器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.api_client = PC28UpstreamAPI(
            appid=config['upstream_api']['appid'],
            secret_key=config['upstream_api']['secret_key']
        )
        self.logger = self._setup_logger()
        self.health_history: List[APIHealthStatus] = []
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('api_monitor')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def check_realtime_api_health(self) -> APIHealthStatus:
        """检查实时API健康状态"""
        start_time = time.time()
        timestamp = datetime.now().isoformat()
        
        try:
            # 调用实时API
            response = self.api_client.get_realtime_lottery()
            response_time = (time.time() - start_time) * 1000
            
            # 检查响应状态
            if response and response.get('codeid') == 10000:
                # API调用成功
                data_quality = self._evaluate_realtime_data_quality(response)
                status = 'healthy' if data_quality > 0.8 else 'degraded'
                
                health_status = APIHealthStatus(
                    timestamp=timestamp,
                    api_type='realtime',
                    status=status,
                    response_time_ms=response_time,
                    data_quality_score=data_quality
                )
            else:
                # API调用失败
                error_code = response.get('codeid') if response else 'NO_RESPONSE'
                error_message = response.get('message') if response else 'No response received'
                
                health_status = APIHealthStatus(
                    timestamp=timestamp,
                    api_type='realtime',
                    status='unhealthy',
                    response_time_ms=response_time,
                    error_code=str(error_code),
                    error_message=error_message
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            health_status = APIHealthStatus(
                timestamp=timestamp,
                api_type='realtime',
                status='unhealthy',
                response_time_ms=response_time,
                error_code='EXCEPTION',
                error_message=str(e)
            )
            
        self.health_history.append(health_status)
        self._log_health_status(health_status)
        return health_status
    
    def check_history_api_health(self) -> APIHealthStatus:
        """检查历史API健康状态"""
        start_time = time.time()
        timestamp = datetime.now().isoformat()
        
        try:
            # 调用历史API（获取昨天的数据）
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            response = self.api_client.get_history_lottery(date=yesterday, limit=10)
            response_time = (time.time() - start_time) * 1000
            
            # 检查响应状态
            if response and response.get('codeid') == 10000:
                # API调用成功
                data_quality = self._evaluate_history_data_quality(response)
                status = 'healthy' if data_quality > 0.8 else 'degraded'
                
                health_status = APIHealthStatus(
                    timestamp=timestamp,
                    api_type='history',
                    status=status,
                    response_time_ms=response_time,
                    data_quality_score=data_quality
                )
            else:
                # API调用失败
                error_code = response.get('codeid') if response else 'NO_RESPONSE'
                error_message = response.get('message') if response else 'No response received'
                
                health_status = APIHealthStatus(
                    timestamp=timestamp,
                    api_type='history',
                    status='unhealthy',
                    response_time_ms=response_time,
                    error_code=str(error_code),
                    error_message=error_message
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            health_status = APIHealthStatus(
                timestamp=timestamp,
                api_type='history',
                status='unhealthy',
                response_time_ms=response_time,
                error_code='EXCEPTION',
                error_message=str(e)
            )
            
        self.health_history.append(health_status)
        self._log_health_status(health_status)
        return health_status
    
    def _evaluate_realtime_data_quality(self, response: Dict[str, Any]) -> float:
        """评估实时数据质量"""
        score = 0.0
        
        # 检查必要字段
        required_fields = ['curent', 'next', 'award_time']
        for field in required_fields:
            if field in response and response[field] is not None:
                score += 0.3
        
        # 检查当前开奖数据
        if 'curent' in response and response['curent']:
            current_data = response['curent']
            if 'long_issue' in current_data and 'number' in current_data:
                score += 0.2
                
        # 检查下期数据
        if 'next' in response and response['next']:
            next_data = response['next']
            if 'next_issue' in next_data and 'next_time' in next_data:
                score += 0.2
                
        return min(score, 1.0)
    
    def _evaluate_history_data_quality(self, response: Dict[str, Any]) -> float:
        """评估历史数据质量"""
        score = 0.0
        
        # 检查返回数据
        if 'retdata' in response and response['retdata']:
            retdata = response['retdata']
            if isinstance(retdata, list) and len(retdata) > 0:
                score += 0.5
                
                # 检查数据完整性
                valid_records = 0
                for record in retdata:
                    if ('long_issue' in record and 'number' in record and 
                        'kjtime' in record):
                        valid_records += 1
                        
                if len(retdata) > 0:
                    score += 0.5 * (valid_records / len(retdata))
                    
        return min(score, 1.0)
    
    def _log_health_status(self, status: APIHealthStatus):
        """记录健康状态日志"""
        if status.status == 'healthy':
            self.logger.info(
                f"{status.api_type.upper()} API健康 - 响应时间: {status.response_time_ms:.2f}ms, "
                f"数据质量: {status.data_quality_score:.2f}"
            )
        elif status.status == 'degraded':
            self.logger.warning(
                f"{status.api_type.upper()} API性能下降 - 响应时间: {status.response_time_ms:.2f}ms, "
                f"数据质量: {status.data_quality_score:.2f}"
            )
        else:
            self.logger.error(
                f"{status.api_type.upper()} API不健康 - 错误代码: {status.error_code}, "
                f"错误信息: {status.error_message}, 响应时间: {status.response_time_ms:.2f}ms"
            )
    
    def get_health_summary(self, hours: int = 24) -> Dict[str, Any]:
        """获取健康状态摘要"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_statuses = [
            status for status in self.health_history
            if datetime.fromisoformat(status.timestamp) > cutoff_time
        ]
        
        if not recent_statuses:
            return {'status': 'no_data', 'message': 'No recent health data available'}
        
        # 按API类型分组
        realtime_statuses = [s for s in recent_statuses if s.api_type == 'realtime']
        history_statuses = [s for s in recent_statuses if s.api_type == 'history']
        
        summary = {
            'period_hours': hours,
            'total_checks': len(recent_statuses),
            'realtime_api': self._get_api_summary(realtime_statuses),
            'history_api': self._get_api_summary(history_statuses),
            'overall_status': self._determine_overall_status(recent_statuses)
        }
        
        return summary
    
    def _get_api_summary(self, statuses: List[APIHealthStatus]) -> Dict[str, Any]:
        """获取单个API的摘要"""
        if not statuses:
            return {'status': 'no_data', 'checks': 0}
        
        healthy_count = len([s for s in statuses if s.status == 'healthy'])
        degraded_count = len([s for s in statuses if s.status == 'degraded'])
        unhealthy_count = len([s for s in statuses if s.status == 'unhealthy'])
        
        avg_response_time = sum(s.response_time_ms for s in statuses) / len(statuses)
        
        quality_scores = [s.data_quality_score for s in statuses if s.data_quality_score is not None]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else None
        
        return {
            'checks': len(statuses),
            'healthy': healthy_count,
            'degraded': degraded_count,
            'unhealthy': unhealthy_count,
            'health_rate': healthy_count / len(statuses),
            'avg_response_time_ms': avg_response_time,
            'avg_data_quality': avg_quality
        }
    
    def _determine_overall_status(self, statuses: List[APIHealthStatus]) -> str:
        """确定整体状态"""
        if not statuses:
            return 'unknown'
        
        recent_statuses = statuses[-10:]  # 最近10次检查
        unhealthy_count = len([s for s in recent_statuses if s.status == 'unhealthy'])
        
        if unhealthy_count >= len(recent_statuses) * 0.5:
            return 'critical'
        elif unhealthy_count > 0:
            return 'warning'
        else:
            return 'healthy'
    
    def run_health_check(self) -> Dict[str, APIHealthStatus]:
        """运行完整的健康检查"""
        self.logger.info("开始API健康检查...")
        
        results = {}
        
        # 检查实时API
        try:
            results['realtime'] = self.check_realtime_api_health()
        except Exception as e:
            self.logger.error(f"实时API健康检查失败: {e}")
        
        # 等待一秒避免API限制
        time.sleep(1)
        
        # 检查历史API
        try:
            results['history'] = self.check_history_api_health()
        except Exception as e:
            self.logger.error(f"历史API健康检查失败: {e}")
        
        self.logger.info("API健康检查完成")
        return results