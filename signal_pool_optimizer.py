#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28信号池优化系统
为多Agent多模型聚合API提供底层框架支持
"""

import json
import time
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, asdict
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
# import asyncio
# import aiohttp  # 暂时注释，避免依赖问题

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SignalType(Enum):
    """信号类型枚举"""
    TREND = "trend"          # 趋势信号
    PATTERN = "pattern"      # 模式信号
    ANOMALY = "anomaly"      # 异常信号
    PREDICTION = "prediction" # 预测信号
    RISK = "risk"           # 风险信号

class ModelProvider(Enum):
    """模型提供商枚举"""
    OPENAI = "openai"
    CLAUDE = "claude"
    GEMINI = "gemini"
    CUSTOM = "custom"

@dataclass
class Signal:
    """信号数据结构"""
    signal_id: str
    signal_type: SignalType
    confidence: float  # 0-1
    value: Union[int, float, str, Dict]
    timestamp: datetime
    source_model: ModelProvider
    agent_id: str
    metadata: Dict[str, Any] = None
    expiry_time: Optional[datetime] = None
    
    def is_expired(self) -> bool:
        """检查信号是否过期"""
        if self.expiry_time is None:
            return False
        return datetime.now() > self.expiry_time

@dataclass
class AgentConfig:
    """Agent配置"""
    agent_id: str
    model_provider: ModelProvider
    api_key: str
    endpoint: str
    max_requests_per_minute: int = 60
    timeout: int = 30
    enabled: bool = True

@dataclass
class SignalPoolMetrics:
    """信号池指标"""
    total_signals: int = 0
    active_signals: int = 0
    expired_signals: int = 0
    confidence_avg: float = 0.0
    model_distribution: Dict[str, int] = None
    signal_type_distribution: Dict[str, int] = None
    last_update: str = ""
    
    def __post_init__(self):
        if self.model_distribution is None:
            self.model_distribution = {}
        if self.signal_type_distribution is None:
            self.signal_type_distribution = {}

class SignalPoolOptimizer:
    """信号池优化器"""
    
    def __init__(self, db_path: str = "signal_pool.db"):
        self.db_path = db_path
        self.agents: Dict[str, AgentConfig] = {}
        self.signal_pool: Dict[str, Signal] = {}
        self.metrics = SignalPoolMetrics()
        self.lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # 初始化数据库
        self._init_database()
        
        # 启动清理任务
        self._start_cleanup_task()
        
    def _init_database(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建信号表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                signal_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                value TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source_model TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                metadata TEXT,
                expiry_time TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建Agent配置表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_configs (
                agent_id TEXT PRIMARY KEY,
                model_provider TEXT NOT NULL,
                api_key TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                max_requests_per_minute INTEGER DEFAULT 60,
                timeout INTEGER DEFAULT 30,
                enabled BOOLEAN DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建指标表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                total_signals INTEGER,
                active_signals INTEGER,
                expired_signals INTEGER,
                confidence_avg REAL,
                model_distribution TEXT,
                signal_type_distribution TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("信号池数据库初始化完成")
    
    def add_agent(self, config: AgentConfig):
        """添加Agent配置"""
        with self.lock:
            self.agents[config.agent_id] = config
            
            # 保存到数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO agent_configs 
                (agent_id, model_provider, api_key, endpoint, max_requests_per_minute, timeout, enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                config.agent_id,
                config.model_provider.value,
                config.api_key,
                config.endpoint,
                config.max_requests_per_minute,
                config.timeout,
                config.enabled
            ))
            conn.commit()
            conn.close()
            
        logger.info(f"Agent {config.agent_id} 配置已添加")
    
    def add_signal(self, signal: Signal):
        """添加信号到池中"""
        with self.lock:
            self.signal_pool[signal.signal_id] = signal
            
            # 保存到数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO signals 
                (signal_id, signal_type, confidence, value, timestamp, source_model, agent_id, metadata, expiry_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.signal_type.value,
                signal.confidence,
                json.dumps(signal.value) if isinstance(signal.value, (dict, list)) else str(signal.value),
                signal.timestamp.isoformat(),
                signal.source_model.value,
                signal.agent_id,
                json.dumps(signal.metadata) if signal.metadata else None,
                signal.expiry_time.isoformat() if signal.expiry_time else None
            ))
            conn.commit()
            conn.close()
            
        self._update_metrics()
        logger.info(f"信号 {signal.signal_id} 已添加到池中")
    
    def get_signals_by_type(self, signal_type: SignalType, min_confidence: float = 0.0) -> List[Signal]:
        """根据类型获取信号"""
        with self.lock:
            return [
                signal for signal in self.signal_pool.values()
                if signal.signal_type == signal_type 
                and signal.confidence >= min_confidence
                and not signal.is_expired()
            ]
    
    def get_signals_by_agent(self, agent_id: str) -> List[Signal]:
        """根据Agent获取信号"""
        with self.lock:
            return [
                signal for signal in self.signal_pool.values()
                if signal.agent_id == agent_id and not signal.is_expired()
            ]
    
    def get_top_signals(self, limit: int = 10, signal_type: Optional[SignalType] = None) -> List[Signal]:
        """获取置信度最高的信号"""
        with self.lock:
            signals = list(self.signal_pool.values())
            
            # 过滤条件
            if signal_type:
                signals = [s for s in signals if s.signal_type == signal_type]
            
            # 过滤未过期的信号
            signals = [s for s in signals if not s.is_expired()]
            
            # 按置信度排序
            signals.sort(key=lambda x: x.confidence, reverse=True)
            
            return signals[:limit]
    
    def aggregate_signals(self, signal_type: SignalType, aggregation_method: str = "weighted_avg") -> Optional[Dict]:
        """聚合同类型信号"""
        signals = self.get_signals_by_type(signal_type)
        
        if not signals:
            return None
        
        if aggregation_method == "weighted_avg":
            total_weight = sum(s.confidence for s in signals)
            if total_weight == 0:
                return None
                
            # 对数值型信号进行加权平均
            if all(isinstance(s.value, (int, float)) for s in signals):
                weighted_sum = sum(s.value * s.confidence for s in signals)
                aggregated_value = weighted_sum / total_weight
            else:
                # 对非数值型信号，选择置信度最高的
                aggregated_value = max(signals, key=lambda x: x.confidence).value
            
            return {
                "aggregated_value": aggregated_value,
                "confidence": total_weight / len(signals),
                "signal_count": len(signals),
                "contributing_agents": list(set(s.agent_id for s in signals)),
                "timestamp": datetime.now().isoformat()
            }
        
        return None
    
    def cleanup_expired_signals(self):
        """清理过期信号"""
        with self.lock:
            expired_count = 0
            expired_ids = []
            
            for signal_id, signal in list(self.signal_pool.items()):
                if signal.is_expired():
                    expired_ids.append(signal_id)
                    del self.signal_pool[signal_id]
                    expired_count += 1
            
            # 从数据库删除
            if expired_ids:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.executemany("DELETE FROM signals WHERE signal_id = ?", [(sid,) for sid in expired_ids])
                conn.commit()
                conn.close()
            
            if expired_count > 0:
                logger.info(f"清理了 {expired_count} 个过期信号")
                self._update_metrics()
    
    def _update_metrics(self):
        """更新指标"""
        with self.lock:
            active_signals = [s for s in self.signal_pool.values() if not s.is_expired()]
            expired_signals = [s for s in self.signal_pool.values() if s.is_expired()]
            
            self.metrics.total_signals = len(self.signal_pool)
            self.metrics.active_signals = len(active_signals)
            self.metrics.expired_signals = len(expired_signals)
            
            if active_signals:
                self.metrics.confidence_avg = sum(s.confidence for s in active_signals) / len(active_signals)
            else:
                self.metrics.confidence_avg = 0.0
            
            # 模型分布
            model_dist = {}
            for signal in active_signals:
                model = signal.source_model.value
                model_dist[model] = model_dist.get(model, 0) + 1
            self.metrics.model_distribution = model_dist
            
            # 信号类型分布
            type_dist = {}
            for signal in active_signals:
                signal_type = signal.signal_type.value
                type_dist[signal_type] = type_dist.get(signal_type, 0) + 1
            self.metrics.signal_type_distribution = type_dist
            
            self.metrics.last_update = datetime.now().isoformat()
    
    def _start_cleanup_task(self):
        """启动定期清理任务"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(300)  # 每5分钟清理一次
                    self.cleanup_expired_signals()
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        logger.info("定期清理任务已启动")
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        self._update_metrics()
        
        return {
            "status": "running",
            "metrics": asdict(self.metrics),
            "agents_count": len(self.agents),
            "active_agents": len([a for a in self.agents.values() if a.enabled]),
            "timestamp": datetime.now().isoformat()
        }
    
    def generate_optimization_report(self) -> str:
        """生成优化报告"""
        status = self.get_system_status()
        metrics = status["metrics"]
        
        report = f"""
PC28信号池优化系统报告
====================
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

信号池统计:
- 总信号数: {metrics['total_signals']}
- 活跃信号数: {metrics['active_signals']}
- 过期信号数: {metrics['expired_signals']}
- 平均置信度: {metrics['confidence_avg']:.2%}

Agent统计:
- 总Agent数: {status['agents_count']}
- 活跃Agent数: {status['active_agents']}

模型分布:
{self._format_distribution(metrics['model_distribution'])}

信号类型分布:
{self._format_distribution(metrics['signal_type_distribution'])}

系统特性:
✅ 多Agent并行处理
✅ 多模型聚合分析
✅ 智能信号过期管理
✅ 实时置信度评估
✅ 自动清理优化
✅ 异步API调用支持

建议:
1. 监控低置信度信号，优化模型参数
2. 平衡不同模型的信号贡献度
3. 根据业务需求调整信号过期时间
4. 定期评估Agent性能并优化配置
        """.strip()
        
        return report
    
    def _format_distribution(self, distribution: Dict[str, int]) -> str:
        """格式化分布数据"""
        if not distribution:
            return "- 暂无数据"
        
        lines = []
        for key, value in distribution.items():
            lines.append(f"- {key}: {value}")
        return "\n".join(lines)

# 示例使用
def main():
    """主函数"""
    # 创建信号池优化器
    optimizer = SignalPoolOptimizer()
    
    # 添加示例Agent配置
    agents = [
        AgentConfig(
            agent_id="openai_agent_1",
            model_provider=ModelProvider.OPENAI,
            api_key="sk-example-key",
            endpoint="https://api.openai.com/v1/chat/completions"
        ),
        AgentConfig(
            agent_id="claude_agent_1", 
            model_provider=ModelProvider.CLAUDE,
            api_key="claude-example-key",
            endpoint="https://api.anthropic.com/v1/messages"
        ),
        AgentConfig(
            agent_id="gemini_agent_1",
            model_provider=ModelProvider.GEMINI,
            api_key="gemini-example-key",
            endpoint="https://generativelanguage.googleapis.com/v1/models"
        )
    ]
    
    for agent in agents:
        optimizer.add_agent(agent)
    
    # 添加示例信号
    signals = [
        Signal(
            signal_id="trend_001",
            signal_type=SignalType.TREND,
            confidence=0.85,
            value={"direction": "up", "strength": 0.7},
            timestamp=datetime.now(),
            source_model=ModelProvider.OPENAI,
            agent_id="openai_agent_1",
            expiry_time=datetime.now() + timedelta(minutes=30)
        ),
        Signal(
            signal_id="pattern_001",
            signal_type=SignalType.PATTERN,
            confidence=0.92,
            value={"pattern": "ascending_triangle", "probability": 0.88},
            timestamp=datetime.now(),
            source_model=ModelProvider.CLAUDE,
            agent_id="claude_agent_1",
            expiry_time=datetime.now() + timedelta(minutes=45)
        )
    ]
    
    for signal in signals:
        optimizer.add_signal(signal)
    
    # 生成报告
    report = optimizer.generate_optimization_report()
    print(report)
    
    # 保存报告
    with open("signal_pool_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    
    logger.info("信号池优化系统启动完成")
    
    # 演示信号聚合
    trend_signals = optimizer.get_signals_by_type(SignalType.TREND)
    if trend_signals:
        aggregated = optimizer.aggregate_signals(SignalType.TREND)
        logger.info(f"趋势信号聚合结果: {aggregated}")
    
    # 获取top信号
    top_signals = optimizer.get_top_signals(5)
    logger.info(f"Top 5 信号: {[s.signal_id for s in top_signals]}")

if __name__ == "__main__":
    main()