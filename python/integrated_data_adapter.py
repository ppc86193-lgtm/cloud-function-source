#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IntegratedDataAdapter
- 集成新的上游API与现有BigQuery数据适配器
- 提供统一的数据获取接口
- 支持实时数据获取和历史数据回填
- 自动数据同步和验证
"""
from __future__ import annotations
import os, json, time, datetime, logging
from typing import Any, Dict, List, Optional, Tuple
from bigquery_data_adapter import BQ
from pc28_upstream_api import PC28UpstreamAPI
from realtime_lottery_service import RealtimeLotteryService
from history_backfill_service import HistoryBackfillService
from data_sync_validator import DataSyncValidator

class IntegratedDataAdapter:
    """集成数据适配器 - 统一管理BigQuery和上游API数据源"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化集成数据适配器
        
        Args:
            config: 配置字典，包含BigQuery和API配置
        """
        # 初始化BigQuery适配器
        self.bq = BQ(
            project=config['bigquery']['project'],
            ds_lab=config['bigquery']['dataset_lab'],
            ds_draw=config['bigquery']['dataset_draw'],
            bqloc=config['bigquery']['location'],
            tz=config['bigquery']['timezone']
        )
        
        # 初始化上游API客户端
        self.api_client = PC28UpstreamAPI(
            appid=config['upstream_api']['appid'],
            secret_key=config['upstream_api']['secret_key']
        )
        
        # 初始化实时服务
        self.realtime_service = RealtimeLotteryService(self.api_client)
        
        # 初始化历史回填服务
        self.backfill_service = HistoryBackfillService(self.api_client)
        
        # 初始化数据同步验证器
        self.sync_validator = DataSyncValidator(config)
        
        # 配置日志
        self.logger = logging.getLogger(__name__)
        
        # 数据源优先级配置
        self.use_upstream_api = config.get('data_source', {}).get('use_upstream_api', True)
        self.fallback_to_bigquery = config.get('data_source', {}).get('fallback_to_bigquery', True)
        self.sync_to_bigquery = config.get('data_source', {}).get('sync_to_bigquery', True)
        self.validation_enabled = config.get('data_source', {}).get('validation_enabled', True)
    
    def get_current_draw_info(self) -> Optional[Dict[str, Any]]:
        """
        获取当前开奖信息
        
        Returns:
            当前开奖信息字典，包含期号、开奖时间、号码等
        """
        if self.use_upstream_api:
            try:
                # 优先使用上游API获取实时数据
                current_data = self.realtime_service.fetch_current_draw()
                if current_data:
                    return current_data
                    
                self.logger.warning("上游API获取当前开奖信息失败，尝试回退到BigQuery")
            except Exception as e:
                self.logger.error(f"上游API调用异常: {e}")
        
        if self.fallback_to_bigquery:
            try:
                # 回退到BigQuery获取数据
                return self._get_current_draw_from_bigquery()
            except Exception as e:
                self.logger.error(f"BigQuery获取当前开奖信息失败: {e}")
        
        return None
    
    def get_next_draw_info(self) -> Optional[Dict[str, Any]]:
        """
        获取下期开奖信息
        
        Returns:
            下期开奖信息字典
        """
        if self.use_upstream_api:
            try:
                next_data = self.realtime_service.get_next_draw_info()
                if next_data:
                    return next_data
            except Exception as e:
                self.logger.error(f"获取下期开奖信息失败: {e}")
        
        return None
    
    def check_for_new_draws(self) -> List[Dict[str, Any]]:
        """
        检查是否有新的开奖数据
        
        Returns:
            新开奖数据列表
        """
        if self.use_upstream_api:
            try:
                new_draw = self.realtime_service.check_new_draw()
                return [new_draw] if new_draw else []
            except Exception as e:
                self.logger.error(f"检查新开奖数据失败: {e}")
        
        return []
    
    def backfill_historical_data(self, start_date: str, end_date: str = None) -> Dict[str, Any]:
        """
        回填历史数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)，默认为今天
            
        Returns:
            回填结果统计
        """
        if not self.use_upstream_api:
            self.logger.warning("上游API未启用，无法执行历史数据回填")
            return {'status': 'skipped', 'reason': 'upstream_api_disabled'}
        
        try:
            if end_date is None:
                end_date = datetime.datetime.now().strftime('%Y-%m-%d')
            
            result = self.backfill_service.backfill_date_range(start_date, end_date)
            
            # 如果回填成功，验证并同步数据到BigQuery
            if result.get('status') == 'success' and result.get('total_records', 0) > 0:
                data_list = result.get('data', [])
                
                # 数据验证
                if self.validation_enabled:
                    validation_result = self.sync_validator.validate_batch_data(data_list, 'upstream_api')
                    result['validation'] = validation_result
                    self.logger.info(f"数据验证完成: 成功率 {validation_result['summary']['success_rate']}")
                
                # 数据同步
                if self.sync_to_bigquery:
                    sync_result = self._sync_to_bigquery_with_validation(data_list)
                    result['sync'] = sync_result
            
            return result
        except Exception as e:
            self.logger.error(f"历史数据回填失败: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def get_draws_today(self) -> int:
        """
        获取今日开奖次数
        
        Returns:
            今日开奖次数
        """
        # 优先使用上游API获取实时数据
        if self.use_upstream_api:
            try:
                today = datetime.datetime.now().strftime('%Y-%m-%d')
                today_data = self.backfill_service.fetch_data_by_date(today)
                if today_data.get('status') == 'success':
                    return len(today_data.get('data', []))
            except Exception as e:
                self.logger.error(f"从上游API获取今日开奖次数失败: {e}")
        
        # 回退到BigQuery
        if self.fallback_to_bigquery:
            try:
                return self.bq.draws_today()
            except Exception as e:
                self.logger.error(f"从BigQuery获取今日开奖次数失败: {e}")
        
        return 0
    
    def get_kpi_window(self, window_min: int = 60) -> Dict[str, Any]:
        """
        获取KPI窗口数据
        
        Args:
            window_min: 时间窗口（分钟）
            
        Returns:
            KPI数据字典
        """
        # KPI数据主要来自BigQuery的score_ledger表
        try:
            return self.bq.kpi_window(window_min)
        except Exception as e:
            self.logger.error(f"获取KPI窗口数据失败: {e}")
            return {"_meta": {"window_min": window_min, "error": str(e)}}
    
    def read_candidates(self) -> List[Dict[str, Any]]:
        """
        读取正EV候选
        
        Returns:
            候选数据列表
        """
        # 候选数据来自BigQuery的lab_push_candidates_v2视图
        try:
            return self.bq.read_candidates()
        except Exception as e:
            self.logger.error(f"读取候选数据失败: {e}")
            return []
    
    def validate_data_consistency(self) -> Dict[str, Any]:
        """
        验证数据一致性
        
        Returns:
            验证结果
        """
        validation_result = {
            'timestamp': datetime.datetime.now().isoformat(),
            'checks': {},
            'overall_status': 'unknown'
        }
        
        try:
            # 检查上游API连接状态
            if self.use_upstream_api:
                api_status = self.api_client.test_connection()
                validation_result['checks']['upstream_api'] = {
                    'status': 'ok' if api_status.get('status') == 'success' else 'error',
                    'details': api_status
                }
            
            # 检查BigQuery连接状态
            if self.fallback_to_bigquery:
                try:
                    bq_draws = self.bq.draws_today()
                    validation_result['checks']['bigquery'] = {
                        'status': 'ok',
                        'draws_today': bq_draws
                    }
                except Exception as e:
                    validation_result['checks']['bigquery'] = {
                        'status': 'error',
                        'error': str(e)
                    }
            
            # 检查数据时效性
            current_draw = self.get_current_draw_info()
            if current_draw:
                validation_result['checks']['data_freshness'] = {
                    'status': 'ok',
                    'latest_draw': current_draw.get('long_issue'),
                    'draw_time': current_draw.get('kjtime')
                }
            else:
                validation_result['checks']['data_freshness'] = {
                    'status': 'warning',
                    'message': 'No current draw data available'
                }
            
            # 计算整体状态
            error_count = sum(1 for check in validation_result['checks'].values() 
                            if check.get('status') == 'error')
            warning_count = sum(1 for check in validation_result['checks'].values() 
                              if check.get('status') == 'warning')
            
            if error_count > 0:
                validation_result['overall_status'] = 'error'
            elif warning_count > 0:
                validation_result['overall_status'] = 'warning'
            else:
                validation_result['overall_status'] = 'ok'
                
        except Exception as e:
            validation_result['checks']['validation_error'] = {
                'status': 'error',
                'error': str(e)
            }
            validation_result['overall_status'] = 'error'
        
        return validation_result
    
    def _get_current_draw_from_bigquery(self) -> Optional[Dict[str, Any]]:
        """
        从BigQuery获取当前开奖信息
        
        Returns:
            当前开奖信息
        """
        sql = f"""
        SELECT 
            draw_id as long_issue,
            timestamp as kjtime,
            numbers,
            sum_value,
            is_odd,
            is_big
        FROM `{self.bq.proj}.{self.bq.ds_draw}.draws_14w_dedup_v`
        WHERE DATE(timestamp, '{self.bq.tz}') = CURRENT_DATE('{self.bq.tz}')
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        try:
            results = self.bq._run_json(sql)
            if results:
                result = results[0]
                # 转换为与上游API一致的格式
                return {
                    'long_issue': result.get('long_issue'),
                    'kjtime': result.get('kjtime'),
                    'number': json.loads(result.get('numbers', '[]')),
                    'sum_value': result.get('sum_value'),
                    'is_odd': result.get('is_odd'),
                    'is_big': result.get('is_big')
                }
        except Exception as e:
            self.logger.error(f"从BigQuery获取当前开奖信息失败: {e}")
        
        return None
    
    def _sync_to_bigquery_with_validation(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        将数据验证后同步到BigQuery
        
        Args:
            data: 要同步的数据列表
            
        Returns:
            同步结果详情
        """
        try:
            # 使用数据同步验证器进行同步
            target_table = f"{self.bq.proj}.{self.bq.ds_draw}.draws_realtime"
            sync_result = self.sync_validator.sync_data_to_bigquery(data, target_table)
            
            self.logger.info(f"数据同步完成: 状态 {sync_result.status}, "
                           f"成功 {sync_result.synced_count}, 失败 {sync_result.failed_count}")
            
            return {
                'status': sync_result.status,
                'synced_count': sync_result.synced_count,
                'failed_count': sync_result.failed_count,
                'skipped_count': sync_result.skipped_count,
                'errors': sync_result.errors,
                'timestamp': sync_result.timestamp
            }
            
        except Exception as e:
            self.logger.error(f"数据同步异常: {e}")
            return {
                'status': 'error',
                'synced_count': 0,
                'failed_count': len(data),
                'skipped_count': 0,
                'errors': [str(e)],
                'timestamp': datetime.datetime.now().isoformat()
            }
    
    def _sync_to_bigquery(self, data: List[Dict[str, Any]]) -> bool:
        """
        将数据同步到BigQuery（简化版本，保持向后兼容）
        
        Args:
            data: 要同步的数据列表
            
        Returns:
            同步是否成功
        """
        sync_result = self._sync_to_bigquery_with_validation(data)
        return sync_result.get('status') in ['success', 'partial']