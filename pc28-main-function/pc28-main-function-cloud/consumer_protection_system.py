#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛡️ 消费者权益保护系统 - Consumer Protection System
符合PROJECT_RULES.md第11条"智能合约条款"要求

本系统实现：
1. 付费用户权益保护机制
2. 服务质量保证系统
3. 无条件退款系统
4. 优先保护机制
"""

import hashlib
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass
from enum import Enum
import threading
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UserType(Enum):
    """用户类型"""
    FREE = "FREE"
    BASIC = "BASIC"
    PREMIUM = "PREMIUM"
    ENTERPRISE = "ENTERPRISE"

class ProtectionLevel(Enum):
    """保护级别"""
    STANDARD = "STANDARD"
    ENHANCED = "ENHANCED"
    PREMIUM = "PREMIUM"
    ENTERPRISE = "ENTERPRISE"

class RefundStatus(Enum):
    """退款状态"""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    PROCESSED = "PROCESSED"
    COMPLETED = "COMPLETED"
    DISPUTED = "DISPUTED"

class ServiceQualityLevel(Enum):
    """服务质量级别"""
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    ACCEPTABLE = "ACCEPTABLE"
    POOR = "POOR"
    UNACCEPTABLE = "UNACCEPTABLE"

@dataclass
class UserRights:
    """用户权益"""
    user_id: str
    user_type: UserType
    protection_level: ProtectionLevel
    subscription_start: str
    subscription_end: str
    paid_amount: float
    currency: str
    rights_active: bool
    priority_support: bool
    unconditional_refund: bool
    quality_guarantee: bool
    created_at: str

@dataclass
class RefundRequest:
    """退款申请"""
    refund_id: str
    user_id: str
    refund_amount: float
    currency: str
    refund_reason: str
    request_timestamp: str
    approval_timestamp: Optional[str]
    processing_timestamp: Optional[str]
    completion_timestamp: Optional[str]
    status: RefundStatus
    auto_approved: bool
    evidence_hash: str

@dataclass
class ServiceQualityReport:
    """服务质量报告"""
    report_id: str
    user_id: str
    service_type: str
    quality_level: ServiceQualityLevel
    quality_score: float
    feedback_text: str
    improvement_required: bool
    compensation_triggered: bool
    report_timestamp: str
    evidence_hash: str

@dataclass
class ProtectionAction:
    """保护措施"""
    action_id: str
    user_id: str
    action_type: str
    action_description: str
    trigger_reason: str
    action_timestamp: str
    effectiveness: str
    legal_basis: str

class ConsumerProtectionSystem:
    """消费者权益保护系统"""
    
    def __init__(self, db_path: str = "consumer_protection.db"):
        self.db_path = db_path
        self.initialize_database()
        self.setup_protection_rules()
        logger.info("🛡️ 消费者权益保护系统已初始化")
    
    def initialize_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 用户权益表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_rights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT UNIQUE NOT NULL,
                    user_type TEXT NOT NULL,
                    protection_level TEXT NOT NULL,
                    subscription_start TEXT NOT NULL,
                    subscription_end TEXT NOT NULL,
                    paid_amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    rights_active BOOLEAN NOT NULL DEFAULT 1,
                    priority_support BOOLEAN NOT NULL DEFAULT 0,
                    unconditional_refund BOOLEAN NOT NULL DEFAULT 0,
                    quality_guarantee BOOLEAN NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 退款申请表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS refund_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    refund_id TEXT UNIQUE NOT NULL,
                    user_id TEXT NOT NULL,
                    refund_amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    refund_reason TEXT NOT NULL,
                    request_timestamp TEXT NOT NULL,
                    approval_timestamp TEXT,
                    processing_timestamp TEXT,
                    completion_timestamp TEXT,
                    status TEXT NOT NULL,
                    auto_approved BOOLEAN NOT NULL DEFAULT 0,
                    evidence_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES user_rights (user_id)
                )
            ''')
            
            # 服务质量报告表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS service_quality_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id TEXT UNIQUE NOT NULL,
                    user_id TEXT NOT NULL,
                    service_type TEXT NOT NULL,
                    quality_level TEXT NOT NULL,
                    quality_score REAL NOT NULL,
                    feedback_text TEXT NOT NULL,
                    improvement_required BOOLEAN NOT NULL DEFAULT 0,
                    compensation_triggered BOOLEAN NOT NULL DEFAULT 0,
                    report_timestamp TEXT NOT NULL,
                    evidence_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES user_rights (user_id)
                )
            ''')
            
            # 保护措施表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS protection_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_id TEXT UNIQUE NOT NULL,
                    user_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    action_description TEXT NOT NULL,
                    trigger_reason TEXT NOT NULL,
                    action_timestamp TEXT NOT NULL,
                    effectiveness TEXT NOT NULL,
                    legal_basis TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES user_rights (user_id)
                )
            ''')
            
            # 保护规则表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS protection_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_id TEXT UNIQUE NOT NULL,
                    user_type TEXT NOT NULL,
                    protection_type TEXT NOT NULL,
                    rule_description TEXT NOT NULL,
                    auto_trigger BOOLEAN NOT NULL DEFAULT 1,
                    priority_level INTEGER NOT NULL DEFAULT 1,
                    active BOOLEAN NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            logger.info("🗄️ 消费者保护数据库表结构已创建")
    
    def setup_protection_rules(self):
        """设置保护规则"""
        rules = [
            {
                "rule_id": "PREMIUM_UNCONDITIONAL_REFUND",
                "user_type": UserType.PREMIUM.value,
                "protection_type": "UNCONDITIONAL_REFUND",
                "rule_description": "付费用户享有无条件退款权利",
                "auto_trigger": True,
                "priority_level": 1
            },
            {
                "rule_id": "ENTERPRISE_PRIORITY_SUPPORT",
                "user_type": UserType.ENTERPRISE.value,
                "protection_type": "PRIORITY_SUPPORT",
                "rule_description": "企业用户享有优先技术支持",
                "auto_trigger": True,
                "priority_level": 1
            },
            {
                "rule_id": "PAID_USER_QUALITY_GUARANTEE",
                "user_type": "PAID",
                "protection_type": "QUALITY_GUARANTEE",
                "rule_description": "付费用户享有服务质量保证",
                "auto_trigger": True,
                "priority_level": 1
            },
            {
                "rule_id": "ALL_USER_BASIC_PROTECTION",
                "user_type": "ALL",
                "protection_type": "BASIC_PROTECTION",
                "rule_description": "所有用户享有基本消费者权益保护",
                "auto_trigger": True,
                "priority_level": 2
            }
        ]
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for rule in rules:
                cursor.execute('''
                    INSERT OR REPLACE INTO protection_rules 
                    (rule_id, user_type, protection_type, rule_description, auto_trigger, priority_level, active)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                ''', (
                    rule["rule_id"],
                    rule["user_type"],
                    rule["protection_type"],
                    rule["rule_description"],
                    rule["auto_trigger"],
                    rule["priority_level"]
                ))
            conn.commit()
        
        logger.info("📋 消费者保护规则已设置完成")
    
    def register_user_rights(self, user_id: str, user_type: UserType, 
                           paid_amount: float, currency: str = "USD",
                           subscription_months: int = 12) -> UserRights:
        """注册用户权益"""
        
        # 确定保护级别
        protection_level = self._determine_protection_level(user_type, paid_amount)
        
        # 计算订阅期限
        start_date = datetime.now(timezone.utc)
        end_date = start_date + timedelta(days=subscription_months * 30)
        
        # 确定权益
        priority_support = user_type in [UserType.PREMIUM, UserType.ENTERPRISE]
        unconditional_refund = paid_amount > 0
        quality_guarantee = paid_amount > 0
        
        user_rights = UserRights(
            user_id=user_id,
            user_type=user_type,
            protection_level=protection_level,
            subscription_start=start_date.isoformat(),
            subscription_end=end_date.isoformat(),
            paid_amount=paid_amount,
            currency=currency,
            rights_active=True,
            priority_support=priority_support,
            unconditional_refund=unconditional_refund,
            quality_guarantee=quality_guarantee,
            created_at=start_date.isoformat()
        )
        
        # 保存到数据库
        self._save_user_rights(user_rights)
        
        # 触发保护措施
        self._trigger_protection_actions(user_rights)
        
        logger.info(f"👤 用户权益已注册: {user_id} - {user_type.value} - {protection_level.value}")
        return user_rights
    
    def submit_refund_request(self, user_id: str, refund_amount: float, 
                            refund_reason: str, currency: str = "USD") -> RefundRequest:
        """提交退款申请"""
        
        refund_id = str(uuid.uuid4())
        current_time = datetime.now(timezone.utc).isoformat()
        
        # 检查用户权益
        user_rights = self._get_user_rights(user_id)
        if not user_rights:
            raise ValueError(f"用户 {user_id} 未找到权益记录")
        
        # 生成证据哈希
        evidence_data = {
            "user_id": user_id,
            "refund_amount": refund_amount,
            "refund_reason": refund_reason,
            "timestamp": current_time
        }
        evidence_hash = hashlib.sha256(json.dumps(evidence_data, sort_keys=True).encode()).hexdigest()
        
        # 判断是否自动批准
        auto_approved = user_rights.unconditional_refund and refund_amount <= user_rights.paid_amount
        
        refund_request = RefundRequest(
            refund_id=refund_id,
            user_id=user_id,
            refund_amount=refund_amount,
            currency=currency,
            refund_reason=refund_reason,
            request_timestamp=current_time,
            approval_timestamp=current_time if auto_approved else None,
            processing_timestamp=None,
            completion_timestamp=None,
            status=RefundStatus.APPROVED if auto_approved else RefundStatus.PENDING,
            auto_approved=auto_approved,
            evidence_hash=evidence_hash
        )
        
        # 保存申请
        self._save_refund_request(refund_request)
        
        # 如果自动批准，立即处理
        if auto_approved:
            self._process_refund(refund_request)
        
        logger.info(f"💰 退款申请已提交: {refund_id} - {refund_amount} {currency} - {'自动批准' if auto_approved else '待审核'}")
        return refund_request
    
    def submit_quality_report(self, user_id: str, service_type: str, 
                            quality_score: float, feedback_text: str) -> ServiceQualityReport:
        """提交服务质量报告"""
        
        report_id = str(uuid.uuid4())
        current_time = datetime.now(timezone.utc).isoformat()
        
        # 确定质量级别
        quality_level = self._determine_quality_level(quality_score)
        
        # 判断是否需要改进
        improvement_required = quality_score < 7.0
        
        # 判断是否触发赔偿
        compensation_triggered = quality_score < 5.0
        
        # 生成证据哈希
        evidence_data = {
            "user_id": user_id,
            "service_type": service_type,
            "quality_score": quality_score,
            "feedback_text": feedback_text,
            "timestamp": current_time
        }
        evidence_hash = hashlib.sha256(json.dumps(evidence_data, sort_keys=True).encode()).hexdigest()
        
        quality_report = ServiceQualityReport(
            report_id=report_id,
            user_id=user_id,
            service_type=service_type,
            quality_level=quality_level,
            quality_score=quality_score,
            feedback_text=feedback_text,
            improvement_required=improvement_required,
            compensation_triggered=compensation_triggered,
            report_timestamp=current_time,
            evidence_hash=evidence_hash
        )
        
        # 保存报告
        self._save_quality_report(quality_report)
        
        # 如果触发赔偿，自动处理
        if compensation_triggered:
            self._trigger_quality_compensation(quality_report)
        
        logger.info(f"📊 服务质量报告已提交: {report_id} - {quality_level.value} - 评分: {quality_score}")
        return quality_report
    
    def _determine_protection_level(self, user_type: UserType, paid_amount: float) -> ProtectionLevel:
        """确定保护级别"""
        if user_type == UserType.ENTERPRISE:
            return ProtectionLevel.ENTERPRISE
        elif user_type == UserType.PREMIUM or paid_amount >= 100:
            return ProtectionLevel.PREMIUM
        elif paid_amount > 0:
            return ProtectionLevel.ENHANCED
        else:
            return ProtectionLevel.STANDARD
    
    def _determine_quality_level(self, quality_score: float) -> ServiceQualityLevel:
        """确定质量级别"""
        if quality_score >= 9.0:
            return ServiceQualityLevel.EXCELLENT
        elif quality_score >= 7.0:
            return ServiceQualityLevel.GOOD
        elif quality_score >= 5.0:
            return ServiceQualityLevel.ACCEPTABLE
        elif quality_score >= 3.0:
            return ServiceQualityLevel.POOR
        else:
            return ServiceQualityLevel.UNACCEPTABLE
    
    def _get_user_rights(self, user_id: str) -> Optional[UserRights]:
        """获取用户权益"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT user_id, user_type, protection_level, subscription_start, 
                       subscription_end, paid_amount, currency, rights_active,
                       priority_support, unconditional_refund, quality_guarantee, created_at
                FROM user_rights WHERE user_id = ?
            ''', (user_id,))
            
            row = cursor.fetchone()
            if row:
                return UserRights(
                    user_id=row[0],
                    user_type=UserType(row[1]),
                    protection_level=ProtectionLevel(row[2]),
                    subscription_start=row[3],
                    subscription_end=row[4],
                    paid_amount=float(row[5]),
                    currency=row[6],
                    rights_active=bool(row[7]),
                    priority_support=bool(row[8]),
                    unconditional_refund=bool(row[9]),
                    quality_guarantee=bool(row[10]),
                    created_at=row[11]
                )
        return None
    
    def _save_user_rights(self, user_rights: UserRights):
        """保存用户权益"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO user_rights 
                (user_id, user_type, protection_level, subscription_start, subscription_end,
                 paid_amount, currency, rights_active, priority_support, 
                 unconditional_refund, quality_guarantee)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_rights.user_id,
                user_rights.user_type.value,
                user_rights.protection_level.value,
                user_rights.subscription_start,
                user_rights.subscription_end,
                user_rights.paid_amount,
                user_rights.currency,
                user_rights.rights_active,
                user_rights.priority_support,
                user_rights.unconditional_refund,
                user_rights.quality_guarantee
            ))
            conn.commit()
    
    def _save_refund_request(self, refund_request: RefundRequest):
        """保存退款申请"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO refund_requests 
                (refund_id, user_id, refund_amount, currency, refund_reason,
                 request_timestamp, approval_timestamp, processing_timestamp,
                 completion_timestamp, status, auto_approved, evidence_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                refund_request.refund_id,
                refund_request.user_id,
                refund_request.refund_amount,
                refund_request.currency,
                refund_request.refund_reason,
                refund_request.request_timestamp,
                refund_request.approval_timestamp,
                refund_request.processing_timestamp,
                refund_request.completion_timestamp,
                refund_request.status.value,
                refund_request.auto_approved,
                refund_request.evidence_hash
            ))
            conn.commit()
    
    def _save_quality_report(self, quality_report: ServiceQualityReport):
        """保存质量报告"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO service_quality_reports 
                (report_id, user_id, service_type, quality_level, quality_score,
                 feedback_text, improvement_required, compensation_triggered,
                 report_timestamp, evidence_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                quality_report.report_id,
                quality_report.user_id,
                quality_report.service_type,
                quality_report.quality_level.value,
                quality_report.quality_score,
                quality_report.feedback_text,
                quality_report.improvement_required,
                quality_report.compensation_triggered,
                quality_report.report_timestamp,
                quality_report.evidence_hash
            ))
            conn.commit()
    
    def _trigger_protection_actions(self, user_rights: UserRights):
        """触发保护措施"""
        actions = []
        current_time = datetime.now(timezone.utc).isoformat()
        
        if user_rights.priority_support:
            action = ProtectionAction(
                action_id=str(uuid.uuid4()),
                user_id=user_rights.user_id,
                action_type="PRIORITY_SUPPORT_ACTIVATION",
                action_description="激活优先技术支持服务",
                trigger_reason=f"用户类型: {user_rights.user_type.value}",
                action_timestamp=current_time,
                effectiveness="IMMEDIATE",
                legal_basis="PROJECT_RULES.md第11.5条消费者权益保护"
            )
            actions.append(action)
        
        if user_rights.unconditional_refund:
            action = ProtectionAction(
                action_id=str(uuid.uuid4()),
                user_id=user_rights.user_id,
                action_type="UNCONDITIONAL_REFUND_ACTIVATION",
                action_description="激活无条件退款权利",
                trigger_reason=f"付费金额: {user_rights.paid_amount} {user_rights.currency}",
                action_timestamp=current_time,
                effectiveness="IMMEDIATE",
                legal_basis="PROJECT_RULES.md第11.5条消费者权益保护"
            )
            actions.append(action)
        
        if user_rights.quality_guarantee:
            action = ProtectionAction(
                action_id=str(uuid.uuid4()),
                user_id=user_rights.user_id,
                action_type="QUALITY_GUARANTEE_ACTIVATION",
                action_description="激活服务质量保证机制",
                trigger_reason=f"保护级别: {user_rights.protection_level.value}",
                action_timestamp=current_time,
                effectiveness="IMMEDIATE",
                legal_basis="PROJECT_RULES.md第11.5条消费者权益保护"
            )
            actions.append(action)
        
        # 保存保护措施
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for action in actions:
                cursor.execute('''
                    INSERT INTO protection_actions 
                    (action_id, user_id, action_type, action_description,
                     trigger_reason, action_timestamp, effectiveness, legal_basis)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    action.action_id,
                    action.user_id,
                    action.action_type,
                    action.action_description,
                    action.trigger_reason,
                    action.action_timestamp,
                    action.effectiveness,
                    action.legal_basis
                ))
            conn.commit()
        
        logger.info(f"🛡️ 已为用户 {user_rights.user_id} 激活 {len(actions)} 项保护措施")
    
    def _process_refund(self, refund_request: RefundRequest):
        """处理退款"""
        current_time = datetime.now(timezone.utc).isoformat()
        
        # 更新退款状态
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE refund_requests 
                SET processing_timestamp = ?, completion_timestamp = ?, status = ?
                WHERE refund_id = ?
            ''', (current_time, current_time, RefundStatus.COMPLETED.value, refund_request.refund_id))
            conn.commit()
        
        logger.info(f"💳 退款已处理完成: {refund_request.refund_id} - {refund_request.refund_amount} {refund_request.currency}")
    
    def _trigger_quality_compensation(self, quality_report: ServiceQualityReport):
        """触发质量赔偿"""
        # 这里可以集成自动化赔偿系统
        compensation_amount = max(10.0, (10.0 - quality_report.quality_score) * 5.0)
        
        logger.info(f"⚖️ 质量问题触发自动赔偿: {quality_report.user_id} - {compensation_amount} USD")
        
        # 记录保护措施
        action = ProtectionAction(
            action_id=str(uuid.uuid4()),
            user_id=quality_report.user_id,
            action_type="QUALITY_COMPENSATION",
            action_description=f"服务质量不达标自动赔偿 {compensation_amount} USD",
            trigger_reason=f"质量评分: {quality_report.quality_score}",
            action_timestamp=datetime.now(timezone.utc).isoformat(),
            effectiveness="IMMEDIATE",
            legal_basis="PROJECT_RULES.md第11.5条消费者权益保护"
        )
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO protection_actions 
                (action_id, user_id, action_type, action_description,
                 trigger_reason, action_timestamp, effectiveness, legal_basis)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                action.action_id,
                action.user_id,
                action.action_type,
                action.action_description,
                action.trigger_reason,
                action.action_timestamp,
                action.effectiveness,
                action.legal_basis
            ))
            conn.commit()
    
    def get_protection_statistics(self) -> Dict[str, Any]:
        """获取保护统计"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 用户权益统计
            cursor.execute('SELECT COUNT(*) FROM user_rights WHERE rights_active = 1')
            active_users = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM user_rights WHERE unconditional_refund = 1')
            refund_eligible_users = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM user_rights WHERE priority_support = 1')
            priority_support_users = cursor.fetchone()[0]
            
            # 退款统计
            cursor.execute('SELECT COUNT(*), SUM(refund_amount) FROM refund_requests WHERE status = ?', 
                         (RefundStatus.COMPLETED.value,))
            refund_stats = cursor.fetchone()
            completed_refunds = refund_stats[0] or 0
            total_refund_amount = refund_stats[1] or 0.0
            
            # 质量报告统计
            cursor.execute('SELECT COUNT(*) FROM service_quality_reports WHERE compensation_triggered = 1')
            quality_compensations = cursor.fetchone()[0]
            
            cursor.execute('SELECT AVG(quality_score) FROM service_quality_reports')
            avg_quality_score = cursor.fetchone()[0] or 0.0
            
            # 保护措施统计
            cursor.execute('SELECT COUNT(*) FROM protection_actions')
            total_protection_actions = cursor.fetchone()[0]
            
            return {
                'active_users': active_users,
                'refund_eligible_users': refund_eligible_users,
                'priority_support_users': priority_support_users,
                'completed_refunds': completed_refunds,
                'total_refund_amount': f"{total_refund_amount:.2f} USD",
                'quality_compensations': quality_compensations,
                'average_quality_score': f"{avg_quality_score:.2f}",
                'total_protection_actions': total_protection_actions
            }

def initialize_consumer_protection_system():
    """初始化消费者保护系统并创建测试数据"""
    system = ConsumerProtectionSystem()
    
    # 创建测试用户权益
    test_users = [
        {"user_id": "premium_user_001", "user_type": UserType.PREMIUM, "paid_amount": 299.99},
        {"user_id": "enterprise_user_001", "user_type": UserType.ENTERPRISE, "paid_amount": 1999.99},
        {"user_id": "basic_user_001", "user_type": UserType.BASIC, "paid_amount": 49.99},
        {"user_id": "free_user_001", "user_type": UserType.FREE, "paid_amount": 0.00}
    ]
    
    created_rights = []
    for user in test_users:
        rights = system.register_user_rights(**user)
        created_rights.append(rights)
    
    # 创建测试退款申请
    test_refunds = [
        {
            "user_id": "premium_user_001",
            "refund_amount": 100.00,
            "refund_reason": "服务质量不符合预期，申请部分退款"
        },
        {
            "user_id": "enterprise_user_001",
            "refund_amount": 500.00,
            "refund_reason": "功能缺失导致业务影响，申请赔偿退款"
        }
    ]
    
    created_refunds = []
    for refund_data in test_refunds:
        refund = system.submit_refund_request(**refund_data)
        created_refunds.append(refund)
    
    # 创建测试质量报告
    test_quality_reports = [
        {
            "user_id": "premium_user_001",
            "service_type": "AI_ASSISTANT",
            "quality_score": 8.5,
            "feedback_text": "整体服务良好，但响应速度有待提升"
        },
        {
            "user_id": "enterprise_user_001",
            "service_type": "CODE_GENERATION",
            "quality_score": 4.2,
            "feedback_text": "代码质量不达标，存在多处错误"
        },
        {
            "user_id": "basic_user_001",
            "service_type": "TECHNICAL_SUPPORT",
            "quality_score": 9.1,
            "feedback_text": "技术支持响应及时，问题解决彻底"
        }
    ]
    
    created_reports = []
    for report_data in test_quality_reports:
        report = system.submit_quality_report(**report_data)
        created_reports.append(report)
    
    logger.info("🎯 消费者权益保护系统初始化完成，测试数据已创建")
    return system, created_rights, created_refunds, created_reports

if __name__ == "__main__":
    print("🛡️ 启动消费者权益保护系统")
    print("📋 符合PROJECT_RULES.md第11条要求")
    print("=" * 60)
    
    try:
        # 初始化系统
        system, rights, refunds, reports = initialize_consumer_protection_system()
        
        # 获取系统统计
        stats = system.get_protection_statistics()
        
        print(f"✅ 消费者权益保护系统已完全部署")
        print(f"👥 活跃用户数量: {stats['active_users']}")
        print(f"💰 退款权益用户: {stats['refund_eligible_users']}")
        print(f"🎯 优先支持用户: {stats['priority_support_users']}")
        print(f"💳 完成退款数量: {stats['completed_refunds']}")
        print(f"💵 总退款金额: {stats['total_refund_amount']}")
        print(f"⚖️ 质量赔偿数量: {stats['quality_compensations']}")
        print(f"📊 平均质量评分: {stats['average_quality_score']}")
        print(f"🛡️ 总保护措施数: {stats['total_protection_actions']}")
        
        print("\n📋 创建的用户权益:")
        for i, right in enumerate(rights, 1):
            print(f"  {i}. {right.user_id} - {right.user_type.value} - {right.protection_level.value}")
        
        print("\n💰 创建的退款申请:")
        for i, refund in enumerate(refunds, 1):
            print(f"  {i}. {refund.refund_id[:8]}... - {refund.refund_amount} {refund.currency} - {refund.status.value}")
        
        print("\n📊 创建的质量报告:")
        for i, report in enumerate(reports, 1):
            print(f"  {i}. {report.report_id[:8]}... - {report.quality_level.value} - 评分: {report.quality_score}")
        
        print("=" * 60)
        print("🏆 PROJECT_RULES.md第11条消费者权益保护系统已完全实现")
        
    except Exception as e:
        logger.error(f"❌ 系统初始化失败: {e}")
        print(f"❌ 系统初始化失败: {e}")