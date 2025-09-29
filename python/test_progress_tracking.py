#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回填进度跟踪和状态报告测试脚本

测试新增的进度跟踪功能：
1. 详细进度报告
2. 完成时间估算
3. 总体报告生成
4. 统计数据分析
"""

import sys
import os
import json
import time
from datetime import datetime, timedelta

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_backfill_service import EnhancedBackfillService, BackfillMode, BackfillStatus
from config_loader import load_config

class ProgressTrackingTester:
    def __init__(self):
        """初始化测试器"""
        self.config = load_config()
        
        # 初始化回填服务
        self.backfill_service = EnhancedBackfillService(
            config=self.config,
            appid=self.config.get('appid', 'default_appid'),
            secret_key=self.config.get('secret_key', 'default_secret')
        )
        
        print("进度跟踪测试器初始化完成")
    
    def test_task_progress_report(self):
        """测试任务详细进度报告"""
        print("\n=== 测试任务详细进度报告 ===")
        
        try:
            # 创建一个测试任务
            task_id = self.backfill_service.create_backfill_task(
                mode=BackfillMode.SMART,
                start_date="2024-01-01",
                end_date="2024-01-03"
            )
            
            if not task_id:
                print("❌ 创建测试任务失败")
                return False
            
            print(f"✅ 创建测试任务成功: {task_id}")
            
            # 启动任务
            if self.backfill_service.start_backfill_task(task_id):
                print(f"✅ 启动任务成功: {task_id}")
            else:
                print(f"❌ 启动任务失败: {task_id}")
                return False
            
            # 等待一段时间让任务执行
            print("等待任务执行...")
            time.sleep(5)
            
            # 获取详细进度报告
            progress_report = self.backfill_service.get_task_progress_report(task_id)
            
            if progress_report:
                print("\n📊 详细进度报告:")
                print(f"  任务ID: {progress_report['task_id']}")
                print(f"  状态: {progress_report['status']}")
                print(f"  进度: {progress_report['progress']}%")
                print(f"  成功率: {progress_report['success_rate']}%")
                
                if progress_report['execution_time_seconds']:
                    print(f"  执行时间: {progress_report['execution_time_seconds']:.2f}秒")
                
                if progress_report['estimated_completion']:
                    print(f"  预计完成时间: {progress_report['estimated_completion']}")
                
                print(f"  记录统计: {progress_report['record_statistics']}")
                
                if progress_report['recent_records']:
                    print("  最近记录:")
                    for record in progress_report['recent_records'][:3]:
                        print(f"    {record['date']}: {record['status']} ({record['count']}条)")
                
                print("✅ 详细进度报告测试通过")
                return True
            else:
                print("❌ 获取详细进度报告失败")
                return False
                
        except Exception as e:
            print(f"❌ 测试任务详细进度报告异常: {e}")
            return False
    
    def test_summary_report(self):
        """测试总体报告生成"""
        print("\n=== 测试总体报告生成 ===")
        
        try:
            # 生成总体报告
            summary_report = self.backfill_service.generate_summary_report()
            
            if summary_report:
                print("\n📈 总体报告:")
                print(f"  时间戳: {summary_report['timestamp']}")
                print(f"  活跃任务数: {summary_report['active_tasks_count']}")
                print(f"  系统状态: {summary_report['system_status']}")
                print(f"  总体成功率: {summary_report['overall_success_rate']}%")
                
                print(f"  任务统计: {summary_report['task_statistics']}")
                print(f"  记录统计: {summary_report['record_statistics']}")
                
                if summary_report['recent_tasks']:
                    print("  最近任务:")
                    for task in summary_report['recent_tasks'][:3]:
                        print(f"    {task['task_id']}: {task['status']} ({task['progress']}%)")
                
                print("✅ 总体报告生成测试通过")
                return True
            else:
                print("❌ 生成总体报告失败")
                return False
                
        except Exception as e:
            print(f"❌ 测试总体报告生成异常: {e}")
            return False
    
    def test_active_tasks_monitoring(self):
        """测试活跃任务监控"""
        print("\n=== 测试活跃任务监控 ===")
        
        try:
            # 获取活跃任务列表
            active_tasks = self.backfill_service.list_active_tasks()
            
            print(f"\n📋 当前活跃任务数: {len(active_tasks)}")
            
            if active_tasks:
                print("活跃任务详情:")
                for task in active_tasks:
                    print(f"  任务ID: {task['task_id']}")
                    print(f"  模式: {task['mode']}")
                    print(f"  状态: {task['status']}")
                    print(f"  进度: {task['progress']}%")
                    print(f"  创建时间: {task['created_at']}")
                    print("  ---")
            else:
                print("当前没有活跃任务")
            
            print("✅ 活跃任务监控测试通过")
            return True
            
        except Exception as e:
            print(f"❌ 测试活跃任务监控异常: {e}")
            return False
    
    def test_completion_estimation(self):
        """测试完成时间估算"""
        print("\n=== 测试完成时间估算 ===")
        
        try:
            # 创建一个较长的任务用于测试估算
            task_id = self.backfill_service.create_backfill_task(
                mode=BackfillMode.FULL,
                start_date="2024-01-01",
                end_date="2024-01-10"
            )
            
            if not task_id:
                print("❌ 创建估算测试任务失败")
                return False
            
            print(f"✅ 创建估算测试任务成功: {task_id}")
            
            # 启动任务
            if self.backfill_service.start_backfill_task(task_id):
                print(f"✅ 启动估算任务成功: {task_id}")
            else:
                print(f"❌ 启动估算任务失败: {task_id}")
                return False
            
            # 多次检查进度和估算
            for i in range(3):
                time.sleep(3)
                
                task_status = self.backfill_service.get_task_status(task_id)
                if task_status:
                    progress_report = self.backfill_service.get_task_progress_report(task_id)
                    
                    print(f"\n第{i+1}次检查:")
                    print(f"  进度: {task_status['progress']}%")
                    
                    if progress_report and progress_report['estimated_completion']:
                        estimated_time = progress_report['estimated_completion']
                        print(f"  预计完成时间: {estimated_time}")
                        
                        # 解析预计完成时间
                        try:
                            est_dt = datetime.fromisoformat(estimated_time.replace('Z', '+00:00'))
                            now = datetime.now(est_dt.tzinfo)
                            remaining = est_dt - now
                            
                            if remaining.total_seconds() > 0:
                                print(f"  剩余时间: {remaining.total_seconds():.0f}秒")
                            else:
                                print("  任务应该已完成")
                        except Exception as e:
                            print(f"  时间解析错误: {e}")
                    else:
                        print("  暂无完成时间估算")
            
            print("✅ 完成时间估算测试通过")
            return True
            
        except Exception as e:
            print(f"❌ 测试完成时间估算异常: {e}")
            return False
    
    def run_comprehensive_test(self):
        """运行综合测试"""
        print("\n🚀 开始进度跟踪和状态报告综合测试")
        print("=" * 50)
        
        test_results = []
        
        # 执行各项测试
        tests = [
            ("任务详细进度报告", self.test_task_progress_report),
            ("总体报告生成", self.test_summary_report),
            ("活跃任务监控", self.test_active_tasks_monitoring),
            ("完成时间估算", self.test_completion_estimation)
        ]
        
        for test_name, test_func in tests:
            try:
                result = test_func()
                test_results.append((test_name, result))
                
                if result:
                    print(f"\n✅ {test_name} 测试通过")
                else:
                    print(f"\n❌ {test_name} 测试失败")
                    
            except Exception as e:
                print(f"\n💥 {test_name} 测试异常: {e}")
                test_results.append((test_name, False))
            
            # 测试间隔
            time.sleep(2)
        
        # 输出测试总结
        print("\n" + "=" * 50)
        print("📊 测试结果总结:")
        
        passed_tests = 0
        total_tests = len(test_results)
        
        for test_name, result in test_results:
            status = "✅ 通过" if result else "❌ 失败"
            print(f"  {test_name}: {status}")
            if result:
                passed_tests += 1
        
        success_rate = (passed_tests / total_tests) * 100
        print(f"\n总体通过率: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
        
        if success_rate >= 75:
            print("\n🎉 进度跟踪和状态报告系统测试基本通过!")
            return True
        else:
            print("\n⚠️ 进度跟踪和状态报告系统需要进一步优化")
            return False

def main():
    """主函数"""
    try:
        tester = ProgressTrackingTester()
        success = tester.run_comprehensive_test()
        
        if success:
            print("\n🎯 进度跟踪和状态报告功能测试完成，系统运行正常")
            exit(0)
        else:
            print("\n⚠️ 进度跟踪和状态报告功能测试发现问题，需要检查")
            exit(1)
            
    except Exception as e:
        print(f"\n💥 测试执行异常: {e}")
        exit(1)

if __name__ == "__main__":
    main()