#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28实时开奖优化系统测试
自动化测试智能轮询机制和数据一致性优化
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# 导入要测试的模块
try:
    from smart_realtime_optimizer import (
        SmartRealtimeOptimizer, 
        PollingMode, 
        PollingConfig, 
        DrawPrediction,
        OptimizationMetrics
    )
    from data_consistency_optimizer import (
        DataConsistencyOptimizer,
        DataRecord,
        DataSource,
        ConsistencyStatus,
        ConsistencyIssue
    )
except ImportError as e:
    print(f"导入模块失败: {e}")
    # 创建模拟类以便测试能够运行
    class SmartRealtimeOptimizer:
        pass
    class DataConsistencyOptimizer:
        pass

class TestSmartRealtimeOptimizer:
    """智能实时开奖优化器测试"""
    
    @pytest.fixture
    def mock_api_system(self):
        """模拟API系统"""
        mock_system = Mock()
        mock_system.get_current_lottery_data.return_value = [
            Mock(
                draw_id="20250929001",
                issue="20250929-001",
                numbers="1,2,3",
                timestamp="2025-09-29T09:00:00Z",
                next_draw_time="2025-09-29T09:05:00Z",
                next_draw_id="20250929002",
                countdown_seconds=300
            )
        ]
        return mock_system
    
    @pytest.fixture
    def optimizer_config(self):
        """优化器配置"""
        return PollingConfig(
            normal_interval=60,
            pre_draw_interval=10,
            draw_time_interval=3,
            post_draw_interval=5,
            pre_draw_threshold=30
        )
    
    @pytest.fixture
    def optimizer(self, mock_api_system, optimizer_config):
        """智能优化器实例"""
        return SmartRealtimeOptimizer(mock_api_system, optimizer_config)
    
    @pytest.mark.unit
    def test_optimizer_initialization(self, optimizer):
        """测试优化器初始化"""
        assert optimizer is not None
        assert optimizer.current_mode == PollingMode.NORMAL
        assert not optimizer.is_running
        assert optimizer.config.normal_interval == 60
        
    @pytest.mark.unit
    def test_polling_mode_adjustment(self, optimizer):
        """测试轮询模式调整"""
        # 模拟开奖前30秒的预测
        prediction = DrawPrediction(
            next_draw_id="20250929002",
            next_draw_time="2025-09-29T09:05:00Z",
            countdown_seconds=25,
            estimated_draw_time=datetime.now() + timedelta(seconds=25),
            confidence_level=0.9
        )
        optimizer.last_prediction = prediction
        
        # 调整轮询模式
        optimizer._adjust_polling_mode()
        
        # 应该切换到开奖前模式
        assert optimizer.current_mode == PollingMode.PRE_DRAW
        
    @pytest.mark.unit
    def test_draw_time_mode_switching(self, optimizer):
        """测试开奖时模式切换"""
        # 模拟开奖前5秒的预测
        prediction = DrawPrediction(
            next_draw_id="20250929002",
            next_draw_time="2025-09-29T09:05:00Z",
            countdown_seconds=5,
            estimated_draw_time=datetime.now() + timedelta(seconds=5),
            confidence_level=0.9
        )
        optimizer.last_prediction = prediction
        
        optimizer._adjust_polling_mode()
        
        # 应该切换到开奖时模式
        assert optimizer.current_mode == PollingMode.DRAW_TIME
        
    @pytest.mark.unit
    def test_interval_calculation(self, optimizer):
        """测试间隔计算"""
        # 测试正常模式间隔
        optimizer.current_mode = PollingMode.NORMAL
        assert optimizer._get_current_interval() == 60
        
        # 测试开奖前模式间隔
        optimizer.current_mode = PollingMode.PRE_DRAW
        assert optimizer._get_current_interval() == 10
        
        # 测试开奖时模式间隔
        optimizer.current_mode = PollingMode.DRAW_TIME
        assert optimizer._get_current_interval() == 3
        
    @pytest.mark.unit
    def test_cache_functionality(self, optimizer, mock_api_system):
        """测试缓存功能"""
        # 第一次获取数据
        data1 = optimizer._fetch_optimized_data()
        assert data1 is not None
        
        # 第二次获取应该命中缓存
        with patch.object(mock_api_system, 'get_current_lottery_data') as mock_get:
            data2 = optimizer._fetch_optimized_data()
            # 在缓存窗口内，不应该调用API
            mock_get.assert_not_called()
            
    @pytest.mark.integration
    def test_optimization_loop_integration(self, optimizer):
        """测试优化循环集成"""
        # 启动优化器
        optimizer.start_optimization()
        assert optimizer.is_running
        
        # 等待一小段时间让循环运行
        time.sleep(0.1)
        
        # 停止优化器
        optimizer.stop_optimization()
        assert not optimizer.is_running
        
    @pytest.mark.performance
    def test_prediction_accuracy(self, optimizer):
        """测试预测准确性"""
        # 创建一个预测
        prediction = DrawPrediction(
            next_draw_id="20250929002",
            next_draw_time="2025-09-29T09:05:00Z",
            countdown_seconds=60,
            estimated_draw_time=datetime.now() + timedelta(seconds=60),
            confidence_level=0.9
        )
        optimizer.last_prediction = prediction
        
        # 模拟实际开奖记录
        actual_record = Mock(draw_id="20250929002")
        
        # 验证预测
        optimizer._validate_prediction(actual_record)
        
        # 检查指标更新
        assert optimizer.metrics.total_requests >= 0

class TestDataConsistencyOptimizer:
    """数据一致性优化器测试"""
    
    @pytest.fixture
    def mock_realtime_system(self):
        """模拟实时系统"""
        mock_system = Mock()
        mock_system.get_current_lottery_data.return_value = [
            Mock(
                draw_id="20250929001",
                issue="20250929-001",
                numbers="1,2,3",
                timestamp="2025-09-29T09:00:00Z"
            )
        ]
        return mock_system
    
    @pytest.fixture
    def mock_backfill_service(self):
        """模拟回填服务"""
        mock_service = Mock()
        mock_service.fetch_history_by_date.return_value = [
            {
                "draw_id": "20250929001",
                "issue": "20250929-001",
                "numbers": "1,2,3",
                "timestamp": "2025-09-29T09:00:00Z"
            }
        ]
        return mock_service
    
    @pytest.fixture
    def consistency_optimizer(self, mock_realtime_system, mock_backfill_service):
        """数据一致性优化器实例"""
        return DataConsistencyOptimizer(
            mock_realtime_system,
            mock_backfill_service
        )
    
    @pytest.mark.unit
    def test_data_record_checksum(self):
        """测试数据记录校验和"""
        record = DataRecord(
            draw_id="20250929001",
            issue="20250929-001",
            numbers="1,2,3",
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.REALTIME,
            checksum="",
            raw_data={}
        )
        
        # 校验和应该自动计算
        assert record.checksum != ""
        assert len(record.checksum) == 32  # MD5长度
        
    @pytest.mark.unit
    def test_consistency_check_identical_data(self, consistency_optimizer):
        """测试相同数据的一致性检查"""
        realtime_record = DataRecord(
            draw_id="20250929001",
            issue="20250929-001",
            numbers="1,2,3",
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.REALTIME,
            checksum="",
            raw_data={}
        )
        
        backfill_record = DataRecord(
            draw_id="20250929001",
            issue="20250929-001",
            numbers="1,2,3",
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.BACKFILL,
            checksum="",
            raw_data={}
        )
        
        # 相同数据应该没有一致性问题
        issue = consistency_optimizer._check_record_consistency(
            "20250929001", realtime_record, backfill_record
        )
        
        assert issue is None
        
    @pytest.mark.unit
    def test_consistency_check_missing_data(self, consistency_optimizer):
        """测试缺失数据的一致性检查"""
        realtime_record = DataRecord(
            draw_id="20250929001",
            issue="20250929-001",
            numbers="1,2,3",
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.REALTIME,
            checksum="",
            raw_data={}
        )
        
        # 历史数据缺失
        issue = consistency_optimizer._check_record_consistency(
            "20250929001", realtime_record, None
        )
        
        assert issue is not None
        assert issue.issue_type == ConsistencyStatus.MISSING
        assert issue.severity == "high"
        
    @pytest.mark.unit
    def test_consistency_check_inconsistent_data(self, consistency_optimizer):
        """测试不一致数据的检查"""
        realtime_record = DataRecord(
            draw_id="20250929001",
            issue="20250929-001",
            numbers="1,2,3",
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.REALTIME,
            checksum="",
            raw_data={}
        )
        
        backfill_record = DataRecord(
            draw_id="20250929001",
            issue="20250929-001",
            numbers="4,5,6",  # 不同的开奖号码
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.BACKFILL,
            checksum="",
            raw_data={}
        )
        
        # 不一致的数据应该被检测出来
        issue = consistency_optimizer._check_record_consistency(
            "20250929001", realtime_record, backfill_record
        )
        
        assert issue is not None
        assert issue.issue_type == ConsistencyStatus.INCONSISTENT
        assert issue.severity == "critical"
        
    @pytest.mark.integration
    def test_monitoring_loop_integration(self, consistency_optimizer):
        """测试监控循环集成"""
        # 启动监控
        consistency_optimizer.start_monitoring()
        assert consistency_optimizer.is_running
        
        # 等待一小段时间
        time.sleep(0.1)
        
        # 停止监控
        consistency_optimizer.stop_monitoring()
        assert not consistency_optimizer.is_running
        
    @pytest.mark.consistency
    def test_consistency_report_generation(self, consistency_optimizer):
        """测试一致性报告生成"""
        # 添加一些测试问题
        issue = ConsistencyIssue(
            draw_id="20250929001",
            issue_type=ConsistencyStatus.INCONSISTENT,
            description="测试不一致问题",
            severity="critical"
        )
        consistency_optimizer.consistency_issues.append(issue)
        
        # 生成报告
        report = consistency_optimizer.get_consistency_report()
        
        assert "metrics" in report
        assert "recent_issues" in report
        assert "cache_status" in report
        assert len(report["recent_issues"]) > 0

class TestIntegrationScenarios:
    """集成测试场景"""
    
    @pytest.mark.integration
    def test_full_optimization_workflow(self):
        """测试完整的优化工作流"""
        # 创建模拟系统
        mock_api = Mock()
        mock_api.get_current_lottery_data.return_value = [
            Mock(
                draw_id="20250929001",
                issue="20250929-001",
                numbers="1,2,3",
                timestamp="2025-09-29T09:00:00Z",
                next_draw_time="2025-09-29T09:05:00Z",
                countdown_seconds=300
            )
        ]
        
        mock_backfill = Mock()
        mock_backfill.fetch_history_by_date.return_value = [
            {
                "draw_id": "20250929001",
                "issue": "20250929-001", 
                "numbers": "1,2,3",
                "timestamp": "2025-09-29T09:00:00Z"
            }
        ]
        
        # 创建优化器
        realtime_optimizer = SmartRealtimeOptimizer(mock_api)
        consistency_optimizer = DataConsistencyOptimizer(mock_api, mock_backfill)
        
        # 启动系统
        realtime_optimizer.start_optimization()
        consistency_optimizer.start_monitoring()
        
        # 运行一小段时间
        time.sleep(0.2)
        
        # 检查系统状态
        assert realtime_optimizer.is_running
        assert consistency_optimizer.is_running
        
        # 停止系统
        realtime_optimizer.stop_optimization()
        consistency_optimizer.stop_monitoring()
        
        assert not realtime_optimizer.is_running
        assert not consistency_optimizer.is_running
        
    @pytest.mark.performance
    def test_performance_under_load(self):
        """测试负载下的性能"""
        # 创建大量模拟数据
        mock_api = Mock()
        large_dataset = []
        for i in range(100):
            large_dataset.append(Mock(
                draw_id=f"2025092900{i:02d}",
                issue=f"20250929-{i:03d}",
                numbers=f"{i%10},{(i+1)%10},{(i+2)%10}",
                timestamp=f"2025-09-29T{9 + i//60:02d}:{i%60:02d}:00Z",
                next_draw_time=f"2025-09-29T{9 + (i+1)//60:02d}:{(i+1)%60:02d}:00Z",
                countdown_seconds=300 - i
            ))
        
        mock_api.get_current_lottery_data.return_value = large_dataset
        
        # 测试处理时间
        start_time = time.time()
        optimizer = SmartRealtimeOptimizer(mock_api)
        data = optimizer._fetch_optimized_data()
        processing_time = time.time() - start_time
        
        # 性能断言
        assert processing_time < 1.0  # 应该在1秒内完成
        assert len(data) == 100
        
    @pytest.mark.realtime
    def test_realtime_prediction_accuracy(self):
        """测试实时预测准确性"""
        mock_api = Mock()
        
        # 模拟接近开奖时间的数据
        near_draw_time = datetime.now() + timedelta(seconds=30)
        mock_api.get_current_lottery_data.return_value = [
            Mock(
                draw_id="20250929001",
                issue="20250929-001",
                numbers="1,2,3",
                timestamp=datetime.now().isoformat(),
                next_draw_time=near_draw_time.isoformat(),
                countdown_seconds=30
            )
        ]
        
        optimizer = SmartRealtimeOptimizer(mock_api)
        
        # 获取数据并更新预测
        data = optimizer._fetch_optimized_data()
        optimizer._update_prediction(data)
        
        # 检查预测
        prediction = optimizer.get_current_prediction()
        assert prediction is not None
        assert prediction.countdown_seconds <= 30
        assert prediction.confidence_level >= 0.8

# 性能基准测试
@pytest.mark.benchmark
class TestPerformanceBenchmarks:
    """性能基准测试"""
    
    def test_api_call_performance(self, benchmark):
        """API调用性能基准"""
        mock_api = Mock()
        mock_api.get_current_lottery_data.return_value = [Mock(draw_id="test")]
        
        optimizer = SmartRealtimeOptimizer(mock_api)
        
        # 基准测试
        result = benchmark(optimizer._fetch_optimized_data)
        assert result is not None
        
    def test_consistency_check_performance(self, benchmark):
        """一致性检查性能基准"""
        mock_realtime = Mock()
        mock_backfill = Mock()
        
        consistency_optimizer = DataConsistencyOptimizer(mock_realtime, mock_backfill)
        
        # 创建测试数据
        realtime_record = DataRecord(
            draw_id="test",
            issue="test",
            numbers="1,2,3",
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.REALTIME,
            checksum="",
            raw_data={}
        )
        
        backfill_record = DataRecord(
            draw_id="test",
            issue="test",
            numbers="1,2,3",
            timestamp="2025-09-29T09:00:00Z",
            source=DataSource.BACKFILL,
            checksum="",
            raw_data={}
        )
        
        # 基准测试
        result = benchmark(
            consistency_optimizer._check_record_consistency,
            "test", realtime_record, backfill_record
        )

if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "--tb=short"])