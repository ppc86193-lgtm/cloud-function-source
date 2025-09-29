#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import logging
import traceback
from datetime import datetime
from main_pc28_e2e import main

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main_handler(request):
    """Cloud Function入口点 - 增强错误处理和健康检查"""
    start_time = datetime.now()
    
    try:
        # 记录请求信息
        logger.info(f"收到请求: {request.method} - {request.url}")
        
        if request.method == 'POST':
            # 处理POST请求
            data = request.get_json(silent=True) or {}
            action = data.get('action', 'sync_data')
            
            logger.info(f"执行操作: {action}")
            
            if action == 'sync_data':
                # 执行主要的数据同步逻辑
                try:
                    main()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    
                    logger.info(f"数据同步完成，耗时: {execution_time:.2f}秒")
                    
                    return {
                        'status': 'success',
                        'message': 'PC28 E2E processing completed',
                        'execution_time': execution_time,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                except Exception as sync_error:
                    logger.error(f"数据同步失败: {str(sync_error)}")
                    logger.error(traceback.format_exc())
                    
                    # 发送告警通知
                    try:
                        if os.path.exists('telegram_notifier.sh'):
                            os.system(f"bash telegram_notifier.sh 'PC28数据同步失败: {str(sync_error)}'")
                    except:
                        pass
                    
                    return {
                        'status': 'error',
                        'message': f'数据同步失败: {str(sync_error)}',
                        'error_type': 'sync_error',
                        'timestamp': datetime.now().isoformat()
                    }, 500
                    
            elif action == 'health_check':
                # 健康检查
                health_status = perform_health_check()
                return health_status
                
            elif action == 'validate_data':
                # 数据验证检查
                validation_result = perform_data_validation()
                return validation_result
                
            else:
                logger.warning(f"未知操作: {action}")
                return {
                    'status': 'error',
                    'message': f'未知操作: {action}',
                    'supported_actions': ['sync_data', 'health_check', 'validate_data']
                }, 400
                
        elif request.method == 'GET':
            # 处理GET请求 - 健康检查
            health_status = perform_health_check()
            return health_status
            
        else:
            return {
                'status': 'error',
                'message': f'不支持的HTTP方法: {request.method}'
            }, 405
            
    except Exception as e:
        logger.error(f"请求处理异常: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            'status': 'error',
            'message': f'请求处理异常: {str(e)}',
            'error_type': 'handler_error',
            'timestamp': datetime.now().isoformat()
        }, 500

def perform_health_check():
    """执行系统健康检查"""
    health_status = {
        'status': 'healthy',
        'message': 'PC28 E2E Function is running',
        'version': '1.0.0',
        'timestamp': datetime.now().isoformat(),
        'checks': {}
    }
    
    try:
        # 检查环境变量
        required_env_vars = ['GOOGLE_CLOUD_PROJECT']
        for var in required_env_vars:
            if os.getenv(var):
                health_status['checks'][f'env_{var}'] = 'ok'
            else:
                health_status['checks'][f'env_{var}'] = 'missing'
                health_status['status'] = 'degraded'
        
        # 检查配置文件
        config_files = ['pc28_enhanced_config.yaml', 'config/pc28_enhanced_config.yaml']
        config_found = False
        for config_file in config_files:
            if os.path.exists(config_file):
                health_status['checks']['config_file'] = 'ok'
                config_found = True
                break
        
        if not config_found:
            health_status['checks']['config_file'] = 'missing'
            health_status['status'] = 'degraded'
        
        # 检查Python模块
        try:
            import main_pc28_e2e
            health_status['checks']['main_module'] = 'ok'
        except ImportError as e:
            health_status['checks']['main_module'] = f'import_error: {str(e)}'
            health_status['status'] = 'unhealthy'
        
        # 检查数据源连接（简单测试）
        try:
            # 这里可以添加API连接测试
            health_status['checks']['api_connectivity'] = 'not_tested'
        except Exception as e:
            health_status['checks']['api_connectivity'] = f'error: {str(e)}'
            health_status['status'] = 'degraded'
        
    except Exception as e:
        logger.error(f"健康检查异常: {str(e)}")
        health_status['status'] = 'unhealthy'
        health_status['message'] = f'健康检查异常: {str(e)}'
    
    return health_status

def perform_data_validation():
    """执行数据验证检查"""
    validation_result = {
        'status': 'unknown',
        'message': 'Data validation check',
        'timestamp': datetime.now().isoformat(),
        'validation_checks': {}
    }
    
    try:
        # 检查数据验证器模块
        try:
            from python.data_sync_validator import DataSyncValidator
            validation_result['validation_checks']['validator_module'] = 'ok'
        except ImportError as e:
            validation_result['validation_checks']['validator_module'] = f'import_error: {str(e)}'
            validation_result['status'] = 'error'
            return validation_result
        
        # 检查数据质量报告
        quality_report_files = [
            'python/data_quality_report_20250925_045814.json',
            'function-source修复/python/data_quality_report_20250925_045814.json'
        ]
        
        for report_file in quality_report_files:
            if os.path.exists(report_file):
                try:
                    with open(report_file, 'r', encoding='utf-8') as f:
                        report_data = json.load(f)
                    
                    validity_rate = report_data.get('summary', {}).get('validity_rate', '0%')
                    validation_result['validation_checks']['last_quality_report'] = {
                        'file': report_file,
                        'validity_rate': validity_rate,
                        'status': 'found'
                    }
                    break
                except Exception as e:
                    validation_result['validation_checks']['last_quality_report'] = {
                        'file': report_file,
                        'error': str(e),
                        'status': 'parse_error'
                    }
        else:
            validation_result['validation_checks']['last_quality_report'] = 'not_found'
        
        validation_result['status'] = 'ok'
        validation_result['message'] = 'Data validation check completed'
        
    except Exception as e:
        logger.error(f"数据验证检查异常: {str(e)}")
        validation_result['status'] = 'error'
        validation_result['message'] = f'数据验证检查异常: {str(e)}'
    
    return validation_result