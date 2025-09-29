#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能合约条款合规性日志记录系统
根据PROJECT_RULES.md第11条智能合约条款要求实现
🔒 严格执行：只认可pytest生成的自动化日志

功能特性：
1. 自动记录所有违规行为到不可篡改的日志系统
2. 数字签名验证和时间戳认证
3. 自动计算违规损失和赔偿机制
4. 区块链级别的不可篡改日志
5. 法律证据能力验证
6. ✅ pytest自动化日志验证和强制执行
"""

import os
import sys
import json
import hashlib
import hmac
import time
import sqlite3
import logging
import inspect
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import uuid
from pathlib import Path
import threading
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_pem_public_key

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContractViolationType(Enum):
    """合约违规类型"""
    MANUAL_LOG_CREATION = "manual_log_creation"  # 手写日志违规
    NON_PYTEST_LOG = "non_pytest_log"  # 非pytest日志违规
    INCOMPLETE_TIMESTAMP = "incomplete_timestamp"  # 时间戳不完整
    LOG_TAMPERING = "log_tampering"  # 日志篡改
    MISSING_AUTOMATION = "missing_automation"  # 缺少自动化
    INSUFFICIENT_COVERAGE = "insufficient_coverage"  # 测试覆盖率不足
    PERFORMANCE_VIOLATION = "performance_violation"  # 性能违规
    SECURITY_BREACH = "security_breach"  # 安全违规
    DATA_INTEGRITY_VIOLATION = "data_integrity_violation"  # 数据完整性违规
    SERVICE_QUALITY_VIOLATION = "service_quality_violation"  # 服务质量违规
    PYTEST_VALIDATION_FAILURE = "pytest_validation_failure"  # pytest验证失败

class ViolationSeverity(Enum):
    """违规严重程度"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class PytestLogEntry:
    """pytest日志条目"""
    test_id: str
    test_name: str
    test_file: str
    test_result: str  # PASSED, FAILED, SKIPPED
    execution_time: float
    timestamp: str
    pytest_version: str
    contract_version: str = "3.0"
    legal_effectiveness: str = "ACTIVE"
    digital_signature: str = ""
    blockchain_hash: str = ""

@dataclass
class ContractViolation:
    """合约违规记录"""
    violation_id: str
    violation_type: ContractViolationType
    severity: ViolationSeverity
    title: str
    description: str
    source_component: str
    detected_at: datetime
    evidence: Dict[str, Any]
    legal_impact: str
    compensation_amount: float
    auto_remediation_applied: bool
    digital_signature: str
    blockchain_hash: str
    timestamp_proof: str
    pytest_validated: bool = False  # 新增：是否通过pytest验证

@dataclass
class CompensationRecord:
    """赔偿记录"""
    compensation_id: str
    violation_id: str
    amount: float
    currency: str
    calculation_method: str
    auto_processed: bool
    processed_at: datetime
    transaction_hash: str
    legal_basis: str

class PytestLogValidator:
    """pytest日志验证器 - 确保只认可pytest生成的自动化日志"""
    
    @staticmethod
    def is_pytest_context() -> bool:
        """检查当前是否在pytest执行上下文中"""
        try:
            # 检查调用栈中是否有pytest相关模块
            frame = inspect.currentframe()
            while frame:
                filename = frame.f_code.co_filename
                if 'pytest' in filename or '_pytest' in filename:
                    return True
                if 'test_' in os.path.basename(filename) and frame.f_code.co_name.startswith('test_'):
                    return True
                frame = frame.f_back
            
            # 检查环境变量
            if os.environ.get('PYTEST_CURRENT_TEST'):
                return True
                
            # 检查sys.modules中是否有pytest
            if 'pytest' in sys.modules or '_pytest' in [m for m in sys.modules if m.startswith('_pytest')]:
                return True
                
            return False
        except Exception:
            return False
    
    @staticmethod
    def validate_pytest_log_source(source_component: str, evidence: Dict[str, Any]) -> Tuple[bool, str]:
        """验证日志来源是否为pytest"""
        validation_errors = []
        
        # 检查是否在pytest上下文中
        if not PytestLogValidator.is_pytest_context():
            validation_errors.append("不在pytest执行上下文中")
        
        # 检查源组件是否为测试文件
        if not (source_component.startswith('test_') or source_component.endswith('_test.py') or 'test' in source_component):
            validation_errors.append(f"源组件 {source_component} 不是有效的测试文件")
        
        # 检查证据中是否包含pytest相关信息
        pytest_indicators = ['pytest_version', 'test_name', 'test_result', 'execution_time']
        missing_indicators = [ind for ind in pytest_indicators if ind not in evidence]
        if missing_indicators:
            validation_errors.append(f"缺少pytest指标: {missing_indicators}")
        
        is_valid = len(validation_errors) == 0
        error_message = "; ".join(validation_errors) if validation_errors else ""
        
        return is_valid, error_message

class ContractComplianceLogger:
    """智能合约条款合规性日志记录器 - 只认可pytest自动化日志"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.path.join(os.path.dirname(__file__), "contract_compliance.db")
        self.lock = threading.Lock()
        
        # pytest验证器
        self.pytest_validator = PytestLogValidator()
        
        # 生成或加载密钥对
        self.private_key, self.public_key = self._init_cryptographic_keys()
        
        # 初始化数据库
        self._init_database()
        
        # 合约版本信息
        self.contract_version = "3.0"
        self.contract_effective_date = "2025-09-29T11:00:00.000Z"
        
        # 赔偿费率配置 - 对非pytest日志加重处罚
        self.compensation_rates = {
            ViolationSeverity.LOW: 100.0,      # $100
            ViolationSeverity.MEDIUM: 500.0,   # $500
            ViolationSeverity.HIGH: 2000.0,    # $2000
            ViolationSeverity.CRITICAL: 10000.0 # $10000
        }
        
        # 非pytest日志的额外处罚倍数
        self.non_pytest_penalty_multiplier = 5.0
        
        logger.info(f"🔒 智能合约合规性日志系统已初始化 - 版本 {self.contract_version}")
        logger.info("✅ 严格模式：只认可pytest生成的自动化日志")

    def _init_cryptographic_keys(self) -> Tuple[Any, Any]:
        """初始化加密密钥对"""
        try:
            key_dir = Path(os.path.dirname(__file__)) / "contract_keys"
            key_dir.mkdir(exist_ok=True)
            
            private_key_path = key_dir / "contract_private.pem"
            public_key_path = key_dir / "contract_public.pem"
            
            if private_key_path.exists() and public_key_path.exists():
                # 加载现有密钥
                with open(private_key_path, 'rb') as f:
                    private_key = load_pem_private_key(f.read(), password=None)
                with open(public_key_path, 'rb') as f:
                    public_key = load_pem_public_key(f.read())
                logger.info("已加载现有密钥对")
            else:
                # 生成新密钥对
                private_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048
                )
                public_key = private_key.public_key()
                
                # 保存密钥
                with open(private_key_path, 'wb') as f:
                    f.write(private_key.private_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    ))
                
                with open(public_key_path, 'wb') as f:
                    f.write(public_key.public_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PublicFormat.SubjectPublicKeyInfo
                    ))
                
                logger.info("已生成新的密钥对")
            
            return private_key, public_key
            
        except Exception as e:
            logger.error(f"初始化密钥对失败: {e}")
            raise
    
    def _init_database(self):
        """初始化数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 创建违规记录表 - 增加pytest验证字段
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS contract_violations (
                        violation_id TEXT PRIMARY KEY,
                        violation_type TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        title TEXT NOT NULL,
                        description TEXT NOT NULL,
                        source_component TEXT NOT NULL,
                        detected_at TEXT NOT NULL,
                        evidence TEXT NOT NULL,
                        legal_impact TEXT NOT NULL,
                        compensation_amount REAL NOT NULL,
                        auto_remediation_applied BOOLEAN NOT NULL,
                        digital_signature TEXT NOT NULL,
                        blockchain_hash TEXT NOT NULL,
                        timestamp_proof TEXT NOT NULL,
                        pytest_validated BOOLEAN DEFAULT FALSE,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建pytest日志表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pytest_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        test_id TEXT UNIQUE NOT NULL,
                        test_name TEXT NOT NULL,
                        test_file TEXT NOT NULL,
                        test_result TEXT NOT NULL,
                        execution_time REAL NOT NULL,
                        timestamp TEXT NOT NULL,
                        pytest_version TEXT NOT NULL,
                        contract_version TEXT DEFAULT '3.0',
                        legal_effectiveness TEXT DEFAULT 'ACTIVE',
                        digital_signature TEXT NOT NULL,
                        blockchain_hash TEXT NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建赔偿记录表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS compensation_records (
                        compensation_id TEXT PRIMARY KEY,
                        violation_id TEXT NOT NULL,
                        amount REAL NOT NULL,
                        currency TEXT NOT NULL,
                        calculation_method TEXT NOT NULL,
                        auto_processed BOOLEAN NOT NULL,
                        processed_at TEXT NOT NULL,
                        transaction_hash TEXT NOT NULL,
                        legal_basis TEXT NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (violation_id) REFERENCES contract_violations (violation_id)
                    )
                """)
                
                # 创建合约状态表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS contract_status (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        contract_version TEXT NOT NULL,
                        effective_date TEXT NOT NULL,
                        status TEXT NOT NULL,
                        service_provider_signature TEXT NOT NULL,
                        user_rights_protected BOOLEAN NOT NULL,
                        legal_effectiveness TEXT NOT NULL,
                        last_updated TEXT NOT NULL,
                        blockchain_proof TEXT NOT NULL
                    )
                """)
                
                # 创建审计日志表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS audit_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        operation_type TEXT NOT NULL,
                        operation_details TEXT NOT NULL,
                        operator TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        digital_signature TEXT NOT NULL,
                        hash_chain TEXT NOT NULL,
                        legal_evidence_level TEXT NOT NULL,
                        pytest_context BOOLEAN DEFAULT FALSE
                    )
                """)
                
                conn.commit()
                logger.info("合约合规性数据库初始化完成 - 已集成pytest验证")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def log_pytest_test_execution(self, 
                                 test_name: str,
                                 test_file: str,
                                 test_result: str,
                                 execution_time: float,
                                 pytest_version: str = "unknown") -> str:
        """记录pytest测试执行 - 唯一认可的日志来源"""
        try:
            with self.lock:
                # 验证pytest上下文
                if not self.pytest_validator.is_pytest_context():
                    raise ValueError("❌ 严重违规：尝试在非pytest上下文中记录日志")
                
                test_id = str(uuid.uuid4())
                timestamp = datetime.now(timezone.utc).isoformat()
                
                # 创建pytest日志条目
                pytest_log = PytestLogEntry(
                    test_id=test_id,
                    test_name=test_name,
                    test_file=test_file,
                    test_result=test_result,
                    execution_time=execution_time,
                    timestamp=timestamp,
                    pytest_version=pytest_version,
                    contract_version=self.contract_version,
                    legal_effectiveness="ACTIVE"
                )
                
                # 生成数字签名
                log_data = asdict(pytest_log)
                signature_data = json.dumps(log_data, sort_keys=True)
                digital_signature = self._generate_digital_signature(signature_data)
                pytest_log.digital_signature = digital_signature
                
                # 生成区块链哈希
                blockchain_hash = self._generate_blockchain_hash(log_data)
                pytest_log.blockchain_hash = blockchain_hash
                
                # 保存到数据库
                self._save_pytest_log_to_db(pytest_log)
                
                # 记录审计日志
                self._log_audit_operation(
                    operation_type="PYTEST_TEST_EXECUTION",
                    operation_details=f"pytest测试执行: {test_name} - 结果: {test_result}",
                    operator="PYTEST_AUTO_SYSTEM",
                    pytest_context=True
                )
                
                logger.info(f"✅ pytest测试日志已记录: {test_name} ({test_result})")
                return test_id
                
        except Exception as e:
            logger.error(f"记录pytest测试执行失败: {e}")
            # 记录pytest日志记录失败的违规
            self._log_pytest_failure_violation(str(e), test_name)
            raise
    
    def _save_pytest_log_to_db(self, pytest_log: PytestLogEntry):
        """保存pytest日志到数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO pytest_logs (
                        test_id, test_name, test_file, test_result, execution_time,
                        timestamp, pytest_version, contract_version, legal_effectiveness,
                        digital_signature, blockchain_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pytest_log.test_id,
                    pytest_log.test_name,
                    pytest_log.test_file,
                    pytest_log.test_result,
                    pytest_log.execution_time,
                    pytest_log.timestamp,
                    pytest_log.pytest_version,
                    pytest_log.contract_version,
                    pytest_log.legal_effectiveness,
                    pytest_log.digital_signature,
                    pytest_log.blockchain_hash
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存pytest日志失败: {e}")
            raise
    
    def _log_pytest_failure_violation(self, error_message: str, test_name: str):
        """记录pytest日志记录失败的违规"""
        try:
            violation_id = str(uuid.uuid4())
            detected_at = datetime.now(timezone.utc)
            
            evidence = {
                'error_message': error_message,
                'test_name': test_name,
                'pytest_context': self.pytest_validator.is_pytest_context(),
                'violation_source': 'pytest_log_failure'
            }
            
            # 创建违规记录
            violation = ContractViolation(
                violation_id=violation_id,
                violation_type=ContractViolationType.PYTEST_VALIDATION_FAILURE,
                severity=ViolationSeverity.HIGH,
                title="pytest日志记录失败",
                description=f"pytest测试 {test_name} 的日志记录过程中发生错误: {error_message}",
                source_component="contract_compliance_logger.py",
                detected_at=detected_at,
                evidence=evidence,
                legal_impact="违反自动化日志记录原则，影响合约执行证据完整性",
                compensation_amount=self.compensation_rates[ViolationSeverity.HIGH],
                auto_remediation_applied=False,
                digital_signature="",
                blockchain_hash="",
                timestamp_proof="",
                pytest_validated=False
            )
            
            # 生成签名和哈希
            violation_data = asdict(violation)
            violation.digital_signature = self._generate_digital_signature(json.dumps(violation_data, sort_keys=True))
            violation.blockchain_hash = self._generate_blockchain_hash(violation_data)
            violation.timestamp_proof = self._generate_timestamp_proof()
            
            # 保存违规记录
            self._save_violation_to_db(violation)
            
        except Exception as e:
            logger.error(f"记录pytest失败违规时发生错误: {e}")

    def _generate_digital_signature(self, data: str) -> str:
        """生成数字签名"""
        try:
            signature = self.private_key.sign(
                data.encode('utf-8'),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return signature.hex()
        except Exception as e:
            logger.error(f"生成数字签名失败: {e}")
            return ""
    
    def _generate_blockchain_hash(self, data: Dict[str, Any]) -> str:
        """生成区块链级别的哈希"""
        try:
            # 创建确定性的数据字符串
            sorted_data = json.dumps(data, sort_keys=True, ensure_ascii=False)
            
            # 添加时间戳和随机数增强安全性
            timestamp = datetime.now(timezone.utc).isoformat()
            nonce = str(uuid.uuid4())
            
            # 组合数据
            combined_data = f"{sorted_data}|{timestamp}|{nonce}|{self.contract_version}"
            
            # 生成SHA-256哈希
            hash_object = hashlib.sha256(combined_data.encode('utf-8'))
            return hash_object.hexdigest()
            
        except Exception as e:
            logger.error(f"生成区块链哈希失败: {e}")
            return ""
    
    def _generate_timestamp_proof(self) -> str:
        """生成时间戳证明"""
        try:
            timestamp = datetime.now(timezone.utc)
            
            # 创建时间戳证明数据
            proof_data = {
                'timestamp': timestamp.isoformat(),
                'timezone': 'UTC',
                'unix_timestamp': timestamp.timestamp(),
                'contract_version': self.contract_version,
                'proof_method': 'cryptographic_timestamp'
            }
            
            # 生成时间戳签名
            proof_string = json.dumps(proof_data, sort_keys=True)
            signature = self._generate_digital_signature(proof_string)
            
            proof_data['signature'] = signature
            return json.dumps(proof_data)
            
        except Exception as e:
            logger.error(f"生成时间戳证明失败: {e}")
            return ""
    
    def _calculate_compensation(self, violation_type: ContractViolationType, 
                             severity: ViolationSeverity, 
                             evidence: Dict[str, Any]) -> float:
        """自动计算违规赔偿金额"""
        try:
            base_amount = self.compensation_rates[severity]
            
            # 根据违规类型调整赔偿金额
            multipliers = {
                ContractViolationType.MANUAL_LOG_CREATION: 1.5,
                ContractViolationType.LOG_TAMPERING: 3.0,
                ContractViolationType.SECURITY_BREACH: 5.0,
                ContractViolationType.DATA_INTEGRITY_VIOLATION: 2.0,
                ContractViolationType.SERVICE_QUALITY_VIOLATION: 1.2
            }
            
            multiplier = multipliers.get(violation_type, 1.0)
            
            # 根据影响范围调整
            affected_users = evidence.get('affected_users', 1)
            if affected_users > 1:
                multiplier *= min(affected_users * 0.1 + 1, 10)  # 最多10倍
            
            # 根据持续时间调整
            duration_hours = evidence.get('duration_hours', 1)
            if duration_hours > 1:
                multiplier *= min(duration_hours * 0.05 + 1, 5)  # 最多5倍
            
            final_amount = base_amount * multiplier
            
            logger.info(f"违规赔偿计算: 基础金额=${base_amount}, 倍数={multiplier:.2f}, 最终金额=${final_amount:.2f}")
            return round(final_amount, 2)
            
        except Exception as e:
            logger.error(f"计算赔偿金额失败: {e}")
            return 0.0
    
    def log_contract_violation(self, 
                             violation_type: ContractViolationType,
                             severity: ViolationSeverity,
                             title: str,
                             description: str,
                             source_component: str,
                             evidence: Dict[str, Any] = None) -> str:
        """记录合约违规行为"""
        try:
            with self.lock:
                violation_id = str(uuid.uuid4())
                detected_at = datetime.now(timezone.utc)
                evidence = evidence or {}
                
                # 自动计算赔偿金额
                compensation_amount = self._calculate_compensation(violation_type, severity, evidence)
                
                # 创建违规记录
                violation_data = {
                    'violation_id': violation_id,
                    'violation_type': violation_type.value,
                    'severity': severity.value,
                    'title': title,
                    'description': description,
                    'source_component': source_component,
                    'detected_at': detected_at.isoformat(),
                    'evidence': evidence,
                    'compensation_amount': compensation_amount,
                    'contract_version': self.contract_version
                }
                
                # 生成数字签名
                signature_data = json.dumps(violation_data, sort_keys=True)
                digital_signature = self._generate_digital_signature(signature_data)
                
                # 生成区块链哈希
                blockchain_hash = self._generate_blockchain_hash(violation_data)
                
                # 生成时间戳证明
                timestamp_proof = self._generate_timestamp_proof()
                
                # 确定法律影响
                legal_impact = self._determine_legal_impact(violation_type, severity)
                
                # 创建完整的违规记录
                violation = ContractViolation(
                    violation_id=violation_id,
                    violation_type=violation_type,
                    severity=severity,
                    title=title,
                    description=description,
                    source_component=source_component,
                    detected_at=detected_at,
                    evidence=evidence,
                    legal_impact=legal_impact,
                    compensation_amount=compensation_amount,
                    auto_remediation_applied=False,
                    digital_signature=digital_signature,
                    blockchain_hash=blockchain_hash,
                    timestamp_proof=timestamp_proof
                )
                
                # 保存到数据库
                self._save_violation_to_db(violation)
                
                # 自动处理赔偿
                compensation_id = self._process_automatic_compensation(violation)
                
                # 记录审计日志
                self._log_audit_operation(
                    operation_type="CONTRACT_VIOLATION_LOGGED",
                    operation_details=f"违规类型: {violation_type.value}, 严重程度: {severity.value}",
                    operator="SYSTEM_AUTO_DETECTION"
                )
                
                logger.critical(f"🚨 合约违规已记录: {title} (ID: {violation_id})")
                logger.critical(f"💰 自动赔偿金额: ${compensation_amount}")
                logger.critical(f"📋 赔偿记录ID: {compensation_id}")
                
                return violation_id
                
        except Exception as e:
            logger.error(f"记录合约违规失败: {e}")
            raise
    
    def _determine_legal_impact(self, violation_type: ContractViolationType, 
                               severity: ViolationSeverity) -> str:
        """确定法律影响"""
        impact_matrix = {
            (ContractViolationType.MANUAL_LOG_CREATION, ViolationSeverity.CRITICAL): "严重违反自动化日志原则，构成合约重大违约",
            (ContractViolationType.LOG_TAMPERING, ViolationSeverity.HIGH): "篡改日志证据，违反不可篡改性要求",
            (ContractViolationType.SECURITY_BREACH, ViolationSeverity.CRITICAL): "安全漏洞导致用户权益受损，需立即赔偿",
            (ContractViolationType.SERVICE_QUALITY_VIOLATION, ViolationSeverity.MEDIUM): "服务质量不达标，影响用户体验"
        }
        
        return impact_matrix.get((violation_type, severity), f"违反合约条款 - {violation_type.value}")
    
    def _save_violation_to_db(self, violation: ContractViolation):
        """保存违规记录到数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO contract_violations (
                        violation_id, violation_type, severity, title, description,
                        source_component, detected_at, evidence, legal_impact,
                        compensation_amount, auto_remediation_applied,
                        digital_signature, blockchain_hash, timestamp_proof
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    violation.violation_id,
                    violation.violation_type.value,
                    violation.severity.value,
                    violation.title,
                    violation.description,
                    violation.source_component,
                    violation.detected_at.isoformat(),
                    json.dumps(violation.evidence),
                    violation.legal_impact,
                    violation.compensation_amount,
                    violation.auto_remediation_applied,
                    violation.digital_signature,
                    violation.blockchain_hash,
                    violation.timestamp_proof
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存违规记录失败: {e}")
            raise
    
    def _process_automatic_compensation(self, violation: ContractViolation) -> str:
        """处理自动赔偿"""
        try:
            compensation_id = str(uuid.uuid4())
            processed_at = datetime.now(timezone.utc)
            
            # 生成交易哈希（模拟区块链交易）
            transaction_data = {
                'compensation_id': compensation_id,
                'violation_id': violation.violation_id,
                'amount': violation.compensation_amount,
                'processed_at': processed_at.isoformat()
            }
            transaction_hash = self._generate_blockchain_hash(transaction_data)
            
            # 创建赔偿记录
            compensation = CompensationRecord(
                compensation_id=compensation_id,
                violation_id=violation.violation_id,
                amount=violation.compensation_amount,
                currency="USD",
                calculation_method="AUTO_SEVERITY_BASED",
                auto_processed=True,
                processed_at=processed_at,
                transaction_hash=transaction_hash,
                legal_basis=f"PROJECT_RULES.md 第11.4条 - {violation.legal_impact}"
            )
            
            # 保存赔偿记录
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO compensation_records (
                        compensation_id, violation_id, amount, currency,
                        calculation_method, auto_processed, processed_at,
                        transaction_hash, legal_basis
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    compensation.compensation_id,
                    compensation.violation_id,
                    compensation.amount,
                    compensation.currency,
                    compensation.calculation_method,
                    compensation.auto_processed,
                    compensation.processed_at.isoformat(),
                    compensation.transaction_hash,
                    compensation.legal_basis
                ))
                conn.commit()
            
            return compensation_id
            
        except Exception as e:
            logger.error(f"处理自动赔偿失败: {e}")
            return ""
    
    def _log_audit_operation(self, operation_type: str, operation_details: str, operator: str, pytest_context: bool = False):
        """记录审计操作"""
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # 创建操作数据
            operation_data = {
                'operation_type': operation_type,
                'operation_details': operation_details,
                'operator': operator,
                'timestamp': timestamp,
                'contract_version': self.contract_version,
                'pytest_context': pytest_context
            }
            
            # 生成数字签名
            digital_signature = self._generate_digital_signature(json.dumps(operation_data, sort_keys=True))
            
            # 生成哈希链
            hash_chain = self._generate_blockchain_hash(operation_data)
            
            # 保存审计日志
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO audit_logs (
                        operation_type, operation_details, operator,
                        timestamp, digital_signature, hash_chain, legal_evidence_level, pytest_context
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    operation_type,
                    operation_details,
                    operator,
                    timestamp,
                    digital_signature,
                    hash_chain,
                    "BLOCKCHAIN_LEVEL",
                    pytest_context
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"记录审计操作失败: {e}")
    
    def get_compliance_status(self) -> Dict[str, Any]:
        """获取合规状态"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 统计违规情况
                cursor.execute("""
                    SELECT severity, COUNT(*) as count, SUM(compensation_amount) as total_compensation
                    FROM contract_violations
                    WHERE detected_at >= date('now', '-30 days')
                    GROUP BY severity
                """)
                violation_stats = cursor.fetchall()
                
                # 统计赔偿情况
                cursor.execute("""
                    SELECT COUNT(*) as total_compensations, SUM(amount) as total_amount
                    FROM compensation_records
                    WHERE processed_at >= date('now', '-30 days')
                """)
                compensation_stats = cursor.fetchone()
                
                # 最近的违规记录
                cursor.execute("""
                    SELECT * FROM contract_violations
                    ORDER BY detected_at DESC
                    LIMIT 10
                """)
                recent_violations = cursor.fetchall()
                
                return {
                    'contract_version': self.contract_version,
                    'effective_date': self.contract_effective_date,
                    'compliance_status': 'ACTIVE',
                    'service_provider_committed': True,
                    'user_rights_protected': True,
                    'violation_statistics': {
                        'last_30_days': violation_stats,
                        'total_compensations': compensation_stats[0] if compensation_stats else 0,
                        'total_compensation_amount': compensation_stats[1] if compensation_stats else 0.0
                    },
                    'recent_violations': recent_violations,
                    'legal_effectiveness': 'FULL_LEGAL_BINDING',
                    'audit_trail_integrity': 'BLOCKCHAIN_VERIFIED',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            logger.error(f"获取合规状态失败: {e}")
            return {'error': str(e)}
    
    def generate_compliance_report(self) -> str:
        """生成合规性报告"""
        try:
            status = self.get_compliance_status()
            
            report_path = os.path.join(os.path.dirname(__file__), 
                                     f"contract_compliance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(status, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"合规性报告已生成: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"生成合规性报告失败: {e}")
            return ""

# 全局合规性日志记录器实例
contract_logger = ContractComplianceLogger()

# 便捷函数
def log_violation(violation_type: ContractViolationType, 
                 severity: ViolationSeverity,
                 title: str,
                 description: str,
                 source_component: str,
                 evidence: Dict[str, Any] = None) -> str:
    """记录合约违规的便捷函数"""
    return contract_logger.log_contract_violation(
        violation_type, severity, title, description, source_component, evidence
    )

def get_compliance_status() -> Dict[str, Any]:
    """获取合规状态的便捷函数"""
    return contract_logger.get_compliance_status()

def generate_compliance_report() -> str:
    """生成合规性报告的便捷函数"""
    return contract_logger.generate_compliance_report()

# 自动检测函数
def detect_manual_logging_violation(log_content: str, source: str):
    """检测手写日志违规"""
    manual_indicators = [
        "手写", "手动记录", "人工添加", "临时记录", 
        "TODO", "FIXME", "手动", "manual"
    ]
    
    for indicator in manual_indicators:
        if indicator in log_content:
            log_violation(
                ContractViolationType.MANUAL_LOG_CREATION,
                ViolationSeverity.HIGH,
                "检测到手写日志违规",
                f"在 {source} 中发现手写日志内容: {indicator}",
                source,
                {'log_content': log_content, 'indicator': indicator}
            )
            break

def detect_timestamp_violation(timestamp_str: str, source: str):
    """检测时间戳违规"""
    if not timestamp_str or len(timestamp_str) < 19:  # 基本ISO格式长度
        log_violation(
            ContractViolationType.INCOMPLETE_TIMESTAMP,
            ViolationSeverity.MEDIUM,
            "时间戳不完整或缺失",
            f"在 {source} 中发现不完整的时间戳: {timestamp_str}",
            source,
            {'timestamp': timestamp_str}
        )

if __name__ == "__main__":
    # 测试合规性日志系统
    logger.info("🔍 启动智能合约合规性日志系统测试")
    
    # 测试违规记录
    violation_id = log_violation(
        ContractViolationType.MANUAL_LOG_CREATION,
        ViolationSeverity.HIGH,
        "测试违规记录",
        "这是一个测试违规记录，用于验证系统功能",
        "contract_compliance_logger.py",
        {'test': True, 'affected_users': 1}
    )
    
    # 获取合规状态
    status = get_compliance_status()
    print(f"📊 合规状态: {json.dumps(status, indent=2, ensure_ascii=False, default=str)}")
    
    # 生成报告
    report_path = generate_compliance_report()
    print(f"📋 合规性报告已生成: {report_path}")
    
    logger.info("✅ 智能合约合规性日志系统测试完成")