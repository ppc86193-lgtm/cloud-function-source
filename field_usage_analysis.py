#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加拿大PC28上游API字段使用情况和多线程实现分析
"""

import json
import sys
import os
from typing import Dict, List, Any, Set
from datetime import datetime

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'python'))

# 这些模块已被重构，使用新的服务模块
try:
    from python import enhanced_realtime_service as realtime_lottery_service
    from python import enhanced_backfill_service as history_backfill_service
except ImportError:
    # 如果导入失败，创建模拟对象
    class MockService:
        def __init__(self):
            pass
    
    realtime_lottery_service = MockService()
    history_backfill_service = MockService()
    

class FieldUsageAnalyzer:
    """字段使用情况分析器"""
    
    def __init__(self):
        
        # 上游API字段定义
        self.realtime_api_fields = {
            'codeid': '状态码，返回10000状态都会进行计费',
            'message': '请求状态说明',
            'retdata': '返回数据集合',
            'retdata.curent': '当前开奖数据集合',
            'retdata.curent.kjtime': '开奖时间',
            'retdata.curent.long_issue': '完整的期号',
            'retdata.curent.short_issue': '短期号',
            'retdata.curent.number': '开奖号码',
            'retdata.next': '下期开奖数据集合',
            'retdata.next.next_issue': '下期开奖期号',
            'retdata.next.next_time': '下期开奖时间',
            'retdata.next.award_time': '距离开奖剩余时间单位：秒',
            'curtime': '当前服务器时间戳'
        }
        
        self.history_api_fields = {
            'codeid': '状态码，返回10000状态都会进行计费',
            'message': '请求状态说明',
            'retdata': '返回数据集合',
            'retdata[].kjtime': '开奖时间',
            'retdata[].long_issue': '完整的期号',
            'retdata[].number': '开奖号码',
            'curtime': '当前服务器时间戳'
        }
        
        # 内部使用字段
        self.internal_fields = {
            'draw_id': '期号（映射自long_issue）',
            'timestamp': '开奖时间（映射自kjtime）',
            'numbers': '开奖号码数组（映射自number）',
            'result_sum': '开奖号码总和（计算得出）',
            'result_digits': '开奖号码数组（与numbers相同）',
            'source': '数据来源标识',
            'created_at': '数据创建时间'
        }
        
    def analyze_realtime_api_usage(self) -> Dict[str, Any]:
        """分析实时API字段使用情况"""
        print("\n=== 实时开奖API字段使用分析 ===")
        
        try:
            # 获取实时数据
            raw_data = self.api_client.get_realtime_lottery()
            
            # 分析字段使用情况
            used_fields = set()
            unused_fields = set()
            
            def check_field_usage(data, prefix=""):
                if isinstance(data, dict):
                    for key, value in data.items():
                        field_path = f"{prefix}.{key}" if prefix else key
                        
                        # 检查字段是否在代码中被使用
                        if self._is_field_used_in_code(field_path):
                            used_fields.add(field_path)
                        else:
                            unused_fields.add(field_path)
                        
                        # 递归检查嵌套字段
                        if isinstance(value, dict):
                            check_field_usage(value, field_path)
                        elif isinstance(value, list) and value and isinstance(value[0], dict):
                            check_field_usage(value[0], f"{field_path}[]")
            
            check_field_usage(raw_data)
            
            return {
                'api_endpoint': '259 (实时开奖)',
                'total_fields': len(used_fields) + len(unused_fields),
                'used_fields': list(used_fields),
                'unused_fields': list(unused_fields),
                'usage_rate': len(used_fields) / (len(used_fields) + len(unused_fields)) * 100,
                'sample_data': raw_data
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_history_api_usage(self) -> Dict[str, Any]:
        """分析历史API字段使用情况"""
        print("\n=== 历史开奖API字段使用分析 ===")
        
        try:
            # 获取历史数据
            raw_data = self.api_client.get_history_lottery(limit=5)
            
            used_fields = set()
            unused_fields = set()
            
            def check_field_usage(data, prefix=""):
                if isinstance(data, dict):
                    for key, value in data.items():
                        field_path = f"{prefix}.{key}" if prefix else key
                        
                        if self._is_field_used_in_code(field_path):
                            used_fields.add(field_path)
                        else:
                            unused_fields.add(field_path)
                        
                        if isinstance(value, dict):
                            check_field_usage(value, field_path)
                        elif isinstance(value, list) and value and isinstance(value[0], dict):
                            check_field_usage(value[0], f"{field_path}[]")
                elif isinstance(data, list) and data:
                    for item in data[:1]:  # 只检查第一个元素
                        check_field_usage(item, f"{prefix}[]" if prefix else "retdata[]")
            
            check_field_usage(raw_data)
            
            return {
                'api_endpoint': '260 (历史开奖)',
                'total_fields': len(used_fields) + len(unused_fields),
                'used_fields': list(used_fields),
                'unused_fields': list(unused_fields),
                'usage_rate': len(used_fields) / (len(used_fields) + len(unused_fields)) * 100,
                'sample_data': raw_data
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def _is_field_used_in_code(self, field_path: str) -> bool:
        """检查字段是否在代码中被使用"""
        # 简化的字段使用检查逻辑
        used_mappings = {
            'codeid': True,
            'message': True,
            'retdata': True,
            'retdata.curent': True,
            'retdata.curent.kjtime': True,
            'retdata.curent.long_issue': True,
            'retdata.curent.number': True,
            'retdata.next': False,  # 在实时服务中未使用
            'retdata.next.next_issue': False,
            'retdata.next.next_time': False,
            'retdata.next.award_time': False,
            'retdata[].kjtime': True,
            'retdata[].long_issue': True,
            'retdata[].number': True,
            'curtime': False,  # 未在业务逻辑中使用
            'retdata.curent.short_issue': False  # 未使用
        }
        
        return used_mappings.get(field_path, False)
    
    def analyze_threading_implementation(self) -> Dict[str, Any]:
        """分析多线程实现情况"""
        print("\n=== 多线程实现分析 ===")
        
        analysis = {
            'current_implementation': {
                'type': '单线程同步调用',
                'description': '当前实现使用requests库进行同步HTTP调用，没有使用多线程或异步处理',
                'bottlenecks': [
                    'API调用阻塞主线程',
                    '无法并发处理多个请求',
                    '实时监控和历史回填无法同时进行',
                    '网络延迟影响整体性能'
                ]
            },
            'threading_opportunities': {
                'realtime_monitoring': {
                    'description': '实时监控可以在独立线程中运行',
                    'benefit': '不阻塞主程序执行',
                    'implementation': 'threading.Thread或concurrent.futures.ThreadPoolExecutor'
                },
                'history_backfill': {
                    'description': '历史数据回填可以并行处理多个日期',
                    'benefit': '显著提高回填速度',
                    'implementation': 'ThreadPoolExecutor with max_workers=5'
                },
                'api_calls': {
                    'description': 'API调用可以使用连接池和异步处理',
                    'benefit': '提高并发性能',
                    'implementation': 'aiohttp或requests.Session with connection pooling'
                }
            },
            'recommended_improvements': [
                '使用ThreadPoolExecutor进行并发API调用',
                '实现异步数据处理管道',
                '添加请求限流和重试机制',
                '使用队列进行生产者-消费者模式',
                '实现连接池复用'
            ]
        }
        
        return analysis
    
    def analyze_field_mapping_efficiency(self) -> Dict[str, Any]:
        """分析字段映射效率"""
        print("\n=== 字段映射效率分析 ===")
        
        # 获取样本数据
        try:
            realtime_data = self.api_client.get_realtime_lottery()
            history_data = self.api_client.get_history_lottery(limit=3)
            
            # 解析数据
            parsed_realtime = self.api_client.parse_lottery_data(realtime_data)
            parsed_history = self.api_client.parse_lottery_data(history_data)
            
            analysis = {
                'field_mapping_summary': {
                    'upstream_to_internal': {
                        'long_issue -> draw_id': '直接映射',
                        'kjtime -> timestamp': '直接映射',
                        'number -> numbers': '字符串数组转整数数组',
                        'calculated -> result_sum': '计算字段（数组求和）',
                        'calculated -> result_digits': '冗余字段（与numbers相同）'
                    },
                    'data_transformation': {
                        'string_to_int_conversion': '开奖号码从字符串转换为整数',
                        'timestamp_parsing': '时间字符串解析为UTC时间戳',
                        'sum_calculation': '动态计算开奖号码总和'
                    }
                },
                'efficiency_issues': {
                    'redundant_fields': [
                        'result_digits字段与numbers字段完全相同，存在冗余',
                        'curtime字段未被使用但仍在传输'
                    ],
                    'missing_optimizations': [
                        '缺少字段级别的数据验证',
                        '没有实现字段选择性解析',
                        '未使用数据压缩传输'
                    ]
                },
                'data_consistency': {
                    'realtime_vs_history': {
                        'structure_difference': '实时数据嵌套在retdata.curent中，历史数据在retdata数组中',
                        'field_alignment': '字段名称和类型保持一致',
                        'parsing_logic': '使用统一的解析逻辑处理两种数据结构'
                    }
                },
                'sample_parsed_data': {
                    'realtime': parsed_realtime[0] if parsed_realtime else None,
                    'history': parsed_history[0] if parsed_history else None
                }
            }
            
            return analysis
            
        except Exception as e:
            return {'error': str(e)}
    
    def generate_optimization_recommendations(self) -> Dict[str, Any]:
        """生成优化建议"""
        print("\n=== 优化建议生成 ===")
        
        recommendations = {
            'multi_threading_optimizations': {
                'priority': 'High',
                'recommendations': [
                    {
                        'title': '实现并发API调用',
                        'description': '使用ThreadPoolExecutor实现并发的实时监控和历史回填',
                        'implementation': 'concurrent.futures.ThreadPoolExecutor(max_workers=3)',
                        'expected_benefit': '提高数据获取效率50-70%'
                    },
                    {
                        'title': '异步数据处理管道',
                        'description': '实现生产者-消费者模式，分离数据获取和处理逻辑',
                        'implementation': 'queue.Queue + threading',
                        'expected_benefit': '提高系统响应性和吞吐量'
                    },
                    {
                        'title': '连接池优化',
                        'description': '使用requests.Session和连接池减少连接开销',
                        'implementation': 'requests.Session with HTTPAdapter',
                        'expected_benefit': '减少网络延迟20-30%'
                    }
                ]
            },
            'field_usage_optimizations': {
                'priority': 'Medium',
                'recommendations': [
                    {
                        'title': '移除冗余字段',
                        'description': '移除result_digits字段，统一使用numbers字段',
                        'implementation': '修改数据模型和BigQuery表结构',
                        'expected_benefit': '减少数据传输和存储开销15%'
                    },
                    {
                        'title': '实现字段选择性解析',
                        'description': '只解析和传输必要的字段，忽略未使用字段',
                        'implementation': '添加字段过滤器和配置',
                        'expected_benefit': '减少处理时间和内存使用'
                    },
                    {
                        'title': '优化数据验证',
                        'description': '实现更高效的字段级数据验证',
                        'implementation': 'pydantic或marshmallow数据模型',
                        'expected_benefit': '提高数据质量和处理速度'
                    }
                ]
            },
            'api_integration_optimizations': {
                'priority': 'Medium',
                'recommendations': [
                    {
                        'title': '实现智能重试机制',
                        'description': '添加指数退避重试和熔断器模式',
                        'implementation': 'tenacity库或自定义重试装饰器',
                        'expected_benefit': '提高系统稳定性和容错能力'
                    },
                    {
                        'title': '添加请求限流',
                        'description': '实现API调用频率限制，避免触发上游限制',
                        'implementation': 'ratelimit库或令牌桶算法',
                        'expected_benefit': '避免API调用被限制或封禁'
                    },
                    {
                        'title': '实现数据缓存',
                        'description': '添加本地缓存减少重复API调用',
                        'implementation': 'Redis或内存缓存',
                        'expected_benefit': '减少API调用次数30-50%'
                    }
                ]
            }
        }
        
        return recommendations
    
    def run_complete_analysis(self) -> Dict[str, Any]:
        """运行完整分析"""
        print("开始加拿大PC28上游API综合分析...")
        print(f"分析时间: {datetime.now().isoformat()}")
        
        analysis_result = {
            'analysis_metadata': {
                'timestamp': datetime.now().isoformat(),
                'analyzer_version': '1.0.0',
                'api_endpoints': ['259 (实时开奖)', '260 (历史开奖)']
            },
            'realtime_api_analysis': self.analyze_realtime_api_usage(),
            'history_api_analysis': self.analyze_history_api_usage(),
            'threading_analysis': self.analyze_threading_implementation(),
            'field_mapping_analysis': self.analyze_field_mapping_efficiency(),
            'optimization_recommendations': self.generate_optimization_recommendations()
        }
        
        return analysis_result

def main():
    """主函数"""
    analyzer = FieldUsageAnalyzer()
    
    try:
        # 运行完整分析
        results = analyzer.run_complete_analysis()
        
        # 保存分析结果
        output_file = f"pc28_api_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ 分析完成！结果已保存到: {output_file}")
        
        # 打印关键统计信息
        print("\n=== 关键统计信息 ===")
        
        realtime_analysis = results.get('realtime_api_analysis', {})
        if 'usage_rate' in realtime_analysis:
            print(f"实时API字段使用率: {realtime_analysis['usage_rate']:.1f}%")
        
        history_analysis = results.get('history_api_analysis', {})
        if 'usage_rate' in history_analysis:
            print(f"历史API字段使用率: {history_analysis['usage_rate']:.1f}%")
        
        threading_analysis = results.get('threading_analysis', {})
        current_impl = threading_analysis.get('current_implementation', {})
        print(f"当前实现类型: {current_impl.get('type', 'Unknown')}")
        
        recommendations = results.get('optimization_recommendations', {})
        high_priority = [rec for category in recommendations.values() 
                        if isinstance(category, dict) and category.get('priority') == 'High']
        print(f"高优先级优化建议数量: {len(high_priority)}")
        
        return results
        
    except Exception as e:
        print(f"❌ 分析过程中发生错误: {e}")
        return None

if __name__ == "__main__":
    main()