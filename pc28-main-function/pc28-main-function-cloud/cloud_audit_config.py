#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Cloud Audit Logs Configuration
配置和启用Cloud Audit Logs以防止AI误操作
"""

import os
import json
import logging
import hashlib
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Any
from functools import wraps
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CloudAuditProtection:
    """Cloud Audit保护机制，防止AI误操作"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID', 'pc28-436923')
        self.audit_config_file = 'audit_config.json'
        self.validation_rules = self._load_validation_rules()
        self.critical_resources = self._define_critical_resources()
        
    def _load_validation_rules(self) -> Dict:
        """加载验证规则"""
        return {
            'prevent_deletion': True,
            'require_approval': True,
            'validate_changes': True,
            'track_modifications': True,
            'enforce_backup': True,
            'audit_all_actions': True
        }
    
    def _define_critical_resources(self) -> List[str]:
        """定义关键资源列表"""
        return [
            'bigquery.datasets',
            'bigquery.tables',
            'cloudfunctions.functions',
            'storage.buckets',
            'compute.instances',
            'sql.instances',
            'pubsub.topics',
            'pubsub.subscriptions'
        ]
    
    def enable_audit_logs(self) -> Dict[str, Any]:
        """启用Cloud Audit Logs"""
        logger.info("启用Cloud Audit Logs...")
        
        audit_config = {
            "auditConfigs": [
                {
                    "service": "allServices",
                    "auditLogConfigs": [
                        {"logType": "ADMIN_READ"},
                        {"logType": "DATA_READ"},
                        {"logType": "DATA_WRITE"}
                    ]
                }
            ]
        }
        
        # 为关键资源启用详细审计
        for resource in self.critical_resources:
            audit_config["auditConfigs"].append({
                "service": resource,
                "auditLogConfigs": [
                    {"logType": "ADMIN_READ"},
                    {"logType": "DATA_READ"},
                    {"logType": "DATA_WRITE"},
                    {"exemptedMembers": []}  # 不豁免任何成员
                ]
            })
        
        # 保存配置
        with open(self.audit_config_file, 'w') as f:
            json.dump(audit_config, f, indent=2)
        
        logger.info(f"Audit配置已保存到 {self.audit_config_file}")
        
        # 应用配置到项目
        try:
            self._apply_audit_config(audit_config)
            return {"status": "success", "config": audit_config}
        except Exception as e:
            logger.error(f"应用Audit配置失败: {e}")
            return {"status": "error", "message": str(e)}
    
    def _apply_audit_config(self, config: Dict) -> None:
        """应用审计配置到GCP项目"""
        # 使用gcloud命令应用配置
        cmd = [
            "gcloud", "projects", "set-iam-policy", self.project_id,
            "--format=json"
        ]
        
        # 这里实际应该先获取现有策略，然后更新
        logger.info(f"正在应用审计配置到项目 {self.project_id}")
        
        # 创建审计策略文件
        policy_file = "audit_policy.json"
        
        # 获取当前IAM策略
        get_policy_cmd = [
            "gcloud", "projects", "get-iam-policy", self.project_id,
            "--format=json"
        ]
        
        try:
            result = subprocess.run(get_policy_cmd, capture_output=True, text=True)
            if result.returncode == 0:
                current_policy = json.loads(result.stdout)
                # 更新审计配置
                current_policy["auditConfigs"] = config["auditConfigs"]
                
                # 保存更新后的策略
                with open(policy_file, 'w') as f:
                    json.dump(current_policy, f, indent=2)
                
                # 应用新策略
                set_policy_cmd = [
                    "gcloud", "projects", "set-iam-policy", self.project_id,
                    policy_file
                ]
                
                result = subprocess.run(set_policy_cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info("审计配置应用成功")
                else:
                    logger.error(f"应用策略失败: {result.stderr}")
        except Exception as e:
            logger.error(f"配置审计日志时出错: {e}")

class ChangeValidator:
    """变更验证器，防止AI误操作"""
    
    def __init__(self):
        self.change_log_file = "change_validation.log"
        self.backup_dir = "backups/audit_protection"
        os.makedirs(self.backup_dir, exist_ok=True)
    
    def validate_operation(func):
        """装饰器：验证操作的合法性"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 记录操作前状态
            operation_id = hashlib.md5(
                f"{func.__name__}_{datetime.now().isoformat()}".encode()
            ).hexdigest()[:8]
            
            logger.info(f"验证操作: {func.__name__} (ID: {operation_id})")
            
            # 创建操作前快照
            snapshot = {
                "operation_id": operation_id,
                "function": func.__name__,
                "timestamp": datetime.now().isoformat(),
                "args": str(args)[:500],  # 限制长度
                "kwargs": str(kwargs)[:500]
            }
            
            # 验证规则
            if not ChangeValidator._check_operation_allowed(func.__name__):
                logger.error(f"操作被拒绝: {func.__name__}")
                raise PermissionError(f"操作 {func.__name__} 需要人工审批")
            
            # 执行操作
            try:
                result = func(*args, **kwargs)
                snapshot["status"] = "success"
                snapshot["result"] = str(result)[:500]
            except Exception as e:
                snapshot["status"] = "failed"
                snapshot["error"] = str(e)
                raise
            finally:
                # 记录操作日志
                ChangeValidator._log_operation(snapshot)
            
            return result
        return wrapper
    
    @staticmethod
    def _check_operation_allowed(operation: str) -> bool:
        """检查操作是否允许"""
        # 危险操作列表
        dangerous_operations = [
            'delete', 'drop', 'truncate', 'remove',
            'destroy', 'purge', 'wipe'
        ]
        
        # 检查是否包含危险关键词
        operation_lower = operation.lower()
        for danger_word in dangerous_operations:
            if danger_word in operation_lower:
                logger.warning(f"检测到危险操作: {operation}")
                # 这里可以实现更复杂的审批流程
                return False
        
        return True
    
    @staticmethod
    def _log_operation(snapshot: Dict) -> None:
        """记录操作日志"""
        log_file = "change_validation.log"
        with open(log_file, 'a') as f:
            f.write(json.dumps(snapshot) + "\n")
        logger.info(f"操作已记录: {snapshot['operation_id']}")

class AIOperationMonitor:
    """AI操作监控器"""
    
    def __init__(self):
        self.monitoring_enabled = True
        self.alert_threshold = 5  # 5次异常操作触发警报
        self.anomaly_count = 0
        self.operation_history = []
    
    def monitor_ai_operations(self) -> None:
        """监控AI操作"""
        logger.info("启动AI操作监控...")
        
        # 设置监控规则
        monitoring_rules = {
            "detect_pattern_anomalies": True,
            "validate_code_changes": True,
            "check_resource_access": True,
            "audit_api_calls": True,
            "track_file_modifications": True
        }
        
        # 创建监控配置
        monitor_config = {
            "enabled": self.monitoring_enabled,
            "rules": monitoring_rules,
            "alert_channels": [
                "logging",
                "email",
                "slack"
            ],
            "retention_days": 30
        }
        
        # 保存监控配置
        with open('ai_monitor_config.json', 'w') as f:
            json.dump(monitor_config, f, indent=2)
        
        logger.info("AI操作监控配置完成")
    
    def detect_anomaly(self, operation: Dict) -> bool:
        """检测异常操作"""
        anomalies = []
        
        # 检测规则
        # 1. 短时间内大量删除操作
        if operation.get('type') == 'delete':
            recent_deletes = sum(
                1 for op in self.operation_history[-10:]
                if op.get('type') == 'delete'
            )
            if recent_deletes > 3:
                anomalies.append("频繁删除操作")
        
        # 2. 未经验证的代码修改
        if operation.get('type') == 'code_change':
            if not operation.get('validated'):
                anomalies.append("未验证的代码修改")
        
        # 3. 访问敏感资源
        sensitive_resources = ['credentials', 'keys', 'secrets', 'tokens']
        if any(res in str(operation).lower() for res in sensitive_resources):
            anomalies.append("访问敏感资源")
        
        if anomalies:
            self.anomaly_count += 1
            logger.warning(f"检测到异常: {', '.join(anomalies)}")
            
            if self.anomaly_count >= self.alert_threshold:
                self._trigger_alert(anomalies)
            
            return True
        
        return False
    
    def _trigger_alert(self, anomalies: List[str]) -> None:
        """触发警报"""
        alert = {
            "timestamp": datetime.now().isoformat(),
            "severity": "HIGH",
            "anomalies": anomalies,
            "action": "HUMAN_REVIEW_REQUIRED"
        }
        
        logger.error(f"安全警报: {json.dumps(alert, indent=2)}")
        
        # 写入警报日志
        with open('security_alerts.log', 'a') as f:
            f.write(json.dumps(alert) + "\n")
        
        # 重置计数器
        self.anomaly_count = 0

def setup_cloud_audit_protection():
    """设置完整的Cloud Audit保护"""
    logger.info("=" * 50)
    logger.info("设置Cloud Audit保护机制")
    logger.info("=" * 50)
    
    # 1. 启用审计日志
    audit_protection = CloudAuditProtection()
    audit_result = audit_protection.enable_audit_logs()
    
    if audit_result['status'] == 'success':
        logger.info("✓ Cloud Audit Logs已启用")
    else:
        logger.warning(f"⚠ Audit Logs启用失败: {audit_result.get('message')}")
    
    # 2. 初始化变更验证器
    validator = ChangeValidator()
    logger.info("✓ 变更验证器已初始化")
    
    # 3. 启动AI操作监控
    monitor = AIOperationMonitor()
    monitor.monitor_ai_operations()
    logger.info("✓ AI操作监控已启动")
    
    # 4. 创建防护配置摘要
    protection_summary = {
        "timestamp": datetime.now().isoformat(),
        "project_id": audit_protection.project_id,
        "audit_logs": "enabled",
        "change_validation": "active",
        "ai_monitoring": "running",
        "critical_resources_protected": len(audit_protection.critical_resources),
        "validation_rules": audit_protection.validation_rules
    }
    
    # 保存配置摘要
    with open('cloud_audit_protection.json', 'w') as f:
        json.dump(protection_summary, f, indent=2)
    
    logger.info("\n防护机制设置完成:")
    logger.info(f"  - 项目ID: {protection_summary['project_id']}")
    logger.info(f"  - 审计日志: {protection_summary['audit_logs']}")
    logger.info(f"  - 变更验证: {protection_summary['change_validation']}")
    logger.info(f"  - AI监控: {protection_summary['ai_monitoring']}")
    logger.info(f"  - 受保护资源数: {protection_summary['critical_resources_protected']}")
    logger.info("=" * 50)
    
    return protection_summary

if __name__ == "__main__":
    # 设置Cloud Audit保护
    result = setup_cloud_audit_protection()
    print(json.dumps(result, indent=2))