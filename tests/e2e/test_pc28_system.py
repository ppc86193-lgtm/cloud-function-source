#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统功能测试脚本
测试回填机制和实时开奖功能
"""

import os
import sys
import os
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

# 添加python目录到路径
sys.path.insert(0, str(Path(__file__).parent / "python"))

from enhanced_backfill_service import EnhancedBackfillService, BackfillMode
from enhanced_realtime_service import EnhancedRealtimeService
from system_integration_manager import SystemIntegrationManager

class PC28SystemTester:
    """PC28系统测试器"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or "python/pc28_system_config.json"
        self.config = self._load_config()
        self._setup_logging()
        
        # 初始化服务
        self.backfill_service = EnhancedBackfillService(
            appid=self.config.get('appid', '45928'),
            secret_key=self.config.get('secret_key', 'ca9edbfee35c22a0d6c4cf6722506af0'),
            config=self.config
        )
        self.realtime_service = EnhancedRealtimeService(
            appid=self.config.get('appid', '45928'),
            secret_key=self.config.get('secret_key', 'ca9edbfee35c22a0d6c4cf6722506af0'),
            config=self.config
        )
        self.integration_manager = SystemIntegrationManager(config=self.config)
        
    def _load_config(self):
        """加载配置"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"配置加载失败: {e}")
            return {}
    
    def _setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def test_api_connectivity(self):
        """测试API连接"""
        self.logger.info("=== 测试API连接 ===")
        
        try:
            # 测试实时API
            realtime_data = self.realtime_service.fetch_current_draw()
            if realtime_data:
                # 处理字典或对象格式的数据
                draw_id = realtime_data.draw_id if hasattr(realtime_data, 'draw_id') else realtime_data.get('draw_id', 'N/A')
                self.logger.info(f"✓ 实时API连接成功，获取到数据: {draw_id}")
                return True
            else:
                self.logger.error("✗ 实时API连接失败")
                return False
        except Exception as e:
            self.logger.error(f"✗ API连接测试异常: {e}")
            return False
    
    def test_backfill_functionality(self):
        """测试回填功能"""
        self.logger.info("=== 测试回填功能 ===")
        
        try:
            # 测试数据缺失检测
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
            
            self.logger.info(f"检测数据缺失: {start_date} 到 {end_date}")
            gaps = self.backfill_service.detect_data_gaps(start_date, end_date)
            
            if gaps:
                self.logger.info(f"✓ 检测到 {len(gaps)} 个数据缺失区间")
                for gap in gaps:
                    self.logger.info(f"  缺失区间: {gap.start_date} - {gap.end_date} ({gap.missing_count}天)")
            else:
                self.logger.info("✓ 未发现数据缺失")
            
            # 测试智能回填
            self.logger.info("测试智能回填功能...")
            task_id = self.backfill_service.create_backfill_task(
                mode=BackfillMode.SMART,
                start_date=start_date,
                end_date=end_date
            )
            
            if task_id:
                self.logger.info(f"✓ 回填任务创建成功: {task_id}")
                
                # 等待任务开始
                time.sleep(2)
                
                # 检查任务状态
                task = self.backfill_service.get_task_status(task_id)
                if task:
                    # 处理返回的字典格式数据
                    if isinstance(task, dict):
                        status = task.get('status', 'unknown')
                        progress = task.get('progress', 0)
                        self.logger.info(f"✓ 任务状态: {status}, 进度: {progress:.1f}%")
                    else:
                        # 处理对象格式数据
                        self.logger.info(f"✓ 任务状态: {task.status.value}, 进度: {task.progress:.1f}%")
                    return True
                else:
                    self.logger.error("✗ 无法获取任务状态")
                    return False
            else:
                self.logger.error("✗ 回填任务创建失败")
                return False
                
        except Exception as e:
            self.logger.error(f"✗ 回填功能测试异常: {e}")
            return False
    
    def test_realtime_functionality(self):
        """测试实时功能"""
        self.logger.info("=== 测试实时功能 ===")
        
        try:
            # 测试实时数据获取
            self.logger.info("测试实时数据获取...")
            current_draw = self.realtime_service.fetch_current_draw()
            
            if current_draw:
                # 处理字典或对象格式的数据
                draw_id = current_draw.draw_id if hasattr(current_draw, 'draw_id') else current_draw.get('draw_id', 'N/A')
                result_numbers = current_draw.result_numbers if hasattr(current_draw, 'result_numbers') else current_draw.get('result_numbers', 'N/A')
                result_sum = current_draw.result_sum if hasattr(current_draw, 'result_sum') else current_draw.get('result_sum', 'N/A')
                big_small = current_draw.big_small if hasattr(current_draw, 'big_small') else current_draw.get('big_small', 'N/A')
                odd_even = current_draw.odd_even if hasattr(current_draw, 'odd_even') else current_draw.get('odd_even', 'N/A')
                dragon_tiger = current_draw.dragon_tiger if hasattr(current_draw, 'dragon_tiger') else current_draw.get('dragon_tiger', 'N/A')
                
                self.logger.info(f"✓ 获取当前开奖: {draw_id}")
                self.logger.info(f"  开奖号码: {result_numbers}")
                self.logger.info(f"  号码和值: {result_sum}")
                self.logger.info(f"  大小: {big_small}")
                self.logger.info(f"  单双: {odd_even}")
                self.logger.info(f"  龙虎: {dragon_tiger}")
            else:
                self.logger.error("✗ 无法获取实时数据")
                return False
            
            # 添加延迟避免API频率限制
            time.sleep(2)
            
            # 测试下期信息
            self.logger.info("测试下期信息获取...")
            next_info = self.realtime_service.get_next_draw_info()
            
            if next_info:
                self.logger.info(f"✓ 下期信息: {next_info}")
            else:
                self.logger.info("! 暂无下期信息")
            
            # 测试缓存功能
            self.logger.info("测试缓存功能...")
            # 处理字典或对象格式的数据
            draw_id = current_draw.draw_id if hasattr(current_draw, 'draw_id') else current_draw.get('draw_id', 'N/A')
            cached_draw = self.realtime_service.get_cached_draw(draw_id)
            
            if cached_draw:
                self.logger.info(f"✓ 缓存命中: {cached_draw['draw_id']}")
            else:
                self.logger.info("! 缓存未命中")
            
            return True
            
        except Exception as e:
            self.logger.error(f"✗ 实时功能测试异常: {e}")
            return False
    
    def test_integration_manager(self):
        """测试集成管理器"""
        self.logger.info("=== 测试集成管理器 ===")
        
        try:
            # 测试系统状态
            status = self.integration_manager.get_system_status()
            self.logger.info(f"✓ 系统状态: {status}")
            
            # 测试服务健康检查
            health = self.integration_manager.check_service_health()
            self.logger.info(f"✓ 服务健康状态: {health}")
            
            # 测试指标收集
            metrics = self.integration_manager.collect_metrics()
            if metrics:
                self.logger.info(f"✓ 系统指标收集成功")
                for key, value in metrics.items():
                    if isinstance(value, (int, float)):
                        self.logger.info(f"  {key}: {value}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"✗ 集成管理器测试异常: {e}")
            return False
    
    def test_field_utilization(self):
        """测试API字段利用率"""
        self.logger.info("=== 测试API字段利用率 ===")
        
        try:
            # 添加延迟避免API频率限制
            time.sleep(3)
            
            # 获取实时数据
            current_draw = self.realtime_service.fetch_current_draw()
            
            if not current_draw:
                self.logger.error("✗ 无法获取数据进行字段分析")
                return False
            
            # 分析字段利用情况
            utilized_fields = []
            total_fields = 0
            
            # 处理字典或对象格式的数据
            if hasattr(current_draw, '__dict__'):
                # 对象格式
                data_dict = current_draw.__dict__
            else:
                # 字典格式
                data_dict = current_draw if isinstance(current_draw, dict) else {}
            
            for field_name, field_value in data_dict.items():
                total_fields += 1
                if field_value is not None and field_value != "":
                    utilized_fields.append(field_name)
            
            utilization_rate = len(utilized_fields) / total_fields * 100 if total_fields > 0 else 0
            
            self.logger.info(f"✓ 字段利用率分析:")
            self.logger.info(f"  总字段数: {total_fields}")
            self.logger.info(f"  已利用字段: {len(utilized_fields)}")
            self.logger.info(f"  利用率: {utilization_rate:.1f}%")
            
            self.logger.info(f"  已利用字段列表:")
            for field in utilized_fields:
                if hasattr(current_draw, field):
                    value = getattr(current_draw, field)
                else:
                    value = data_dict.get(field, 'N/A')
                self.logger.info(f"    {field}: {value}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"✗ 字段利用率测试异常: {e}")
            return False
    
    def run_comprehensive_test(self):
        """运行综合测试"""
        self.logger.info("\n" + "="*50)
        self.logger.info("开始PC28系统综合功能测试")
        self.logger.info("="*50)
        
        test_results = []
        
        # 执行各项测试
        tests = [
            ("API连接测试", self.test_api_connectivity),
            ("实时功能测试", self.test_realtime_functionality),
            ("回填功能测试", self.test_backfill_functionality),
            ("集成管理器测试", self.test_integration_manager),
            ("字段利用率测试", self.test_field_utilization)
        ]
        
        for i, (test_name, test_func) in enumerate(tests):
            self.logger.info(f"\n--- {test_name} ---")
            try:
                result = test_func()
                test_results.append((test_name, result))
                if result:
                    self.logger.info(f"✓ {test_name} 通过")
                else:
                    self.logger.error(f"✗ {test_name} 失败")
                
                # 在测试之间添加延迟，避免API频率限制
                if i < len(tests) - 1:  # 不是最后一个测试
                    time.sleep(2)
                    
            except Exception as e:
                self.logger.error(f"✗ {test_name} 异常: {e}")
                test_results.append((test_name, False))
        
        # 汇总测试结果
        self.logger.info("\n" + "="*50)
        self.logger.info("测试结果汇总")
        self.logger.info("="*50)
        
        passed_tests = sum(1 for _, result in test_results if result)
        total_tests = len(test_results)
        
        for test_name, result in test_results:
            status = "✓ 通过" if result else "✗ 失败"
            self.logger.info(f"{test_name}: {status}")
        
        self.logger.info(f"\n总体结果: {passed_tests}/{total_tests} 项测试通过")
        
        if passed_tests == total_tests:
            self.logger.info("🎉 所有测试通过！系统运行正常")
            return True
        else:
            self.logger.warning(f"⚠️  有 {total_tests - passed_tests} 项测试失败，请检查系统")
            return False

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PC28系统功能测试')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--test', choices=['api', 'realtime', 'backfill', 'integration', 'fields', 'all'], 
                       default='all', help='指定测试类型')
    
    args = parser.parse_args()
    
    tester = PC28SystemTester(config_path=args.config)
    
    if args.test == 'all':
        success = tester.run_comprehensive_test()
    elif args.test == 'api':
        success = tester.test_api_connectivity()
    elif args.test == 'realtime':
        success = tester.test_realtime_functionality()
    elif args.test == 'backfill':
        success = tester.test_backfill_functionality()
    elif args.test == 'integration':
        success = tester.test_integration_manager()
    elif args.test == 'fields':
        success = tester.test_field_utilization()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()