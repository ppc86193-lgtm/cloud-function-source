#!/usr/bin/env python3
"""
PC28数据流程图生成器
创建完整的数据流程图，展示从API采集到最终输出的完整链路
"""

import json
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PC28DataFlowDiagram:
    """PC28数据流程图生成器"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.flow_data = {}
        
    def run_bq_query(self, query: str) -> Dict[str, Any]:
        """执行BigQuery查询"""
        try:
            cmd = ['bq', 'query', '--use_legacy_sql=false', '--format=json', query]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return {"success": True, "data": json.loads(result.stdout) if result.stdout.strip() else []}
        except subprocess.CalledProcessError as e:
            logger.error(f"BigQuery查询失败: {e.stderr}")
            return {"success": False, "error": e.stderr}
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            return {"success": False, "error": str(e)}
    
    def analyze_data_flow(self) -> Dict[str, Any]:
        """分析数据流程"""
        logger.info("🔍 分析PC28数据流程...")
        
        flow_analysis = {
            "timestamp": datetime.now().isoformat(),
            "layers": {
                "1_raw_data": self._analyze_raw_data_layer(),
                "2_prediction_views": self._analyze_prediction_layer(),
                "3_canonical_views": self._analyze_canonical_layer(),
                "4_ensemble_layer": self._analyze_ensemble_layer(),
                "5_signal_pool": self._analyze_signal_pool_layer(),
                "6_decision_layer": self._analyze_decision_layer()
            },
            "dependencies": self._analyze_dependencies(),
            "data_volumes": self._analyze_data_volumes(),
            "bottlenecks": self._identify_bottlenecks()
        }
        
        return flow_analysis
    
    def _analyze_raw_data_layer(self) -> Dict[str, Any]:
        """分析原始数据层"""
        logger.info("分析原始数据层...")
        
        raw_tables = [
            'cloud_pred_today_norm',
            'p_cloud_clean_merged_dedup_v',
            'p_map_clean_merged_dedup_v', 
            'p_size_clean_merged_dedup_v'
        ]
        
        layer_info = {
            "description": "原始数据采集层 - 从API直接写入的数据",
            "tables": {},
            "total_rows": 0,
            "data_sources": ["Cloud API", "Map API", "Size API"],
            "update_frequency": "实时"
        }
        
        for table in raw_tables:
            table_info = self._get_table_info(table)
            layer_info["tables"][table] = table_info
            layer_info["total_rows"] += table_info.get("row_count", 0)
        
        return layer_info
    
    def _analyze_prediction_layer(self) -> Dict[str, Any]:
        """分析预测视图层"""
        logger.info("分析预测视图层...")
        
        prediction_views = [
            'p_cloud_today_v',
            'p_map_today_v',
            'p_size_today_v'
        ]
        
        layer_info = {
            "description": "预测视图层 - 基于原始数据的预测结果",
            "views": {},
            "total_rows": 0,
            "transformations": ["时间过滤", "数据清洗", "预测计算"]
        }
        
        for view in prediction_views:
            view_info = self._get_table_info(view)
            view_info["dependencies"] = self._get_view_dependencies(view)
            layer_info["views"][view] = view_info
            layer_info["total_rows"] += view_info.get("row_count", 0)
        
        return layer_info
    
    def _analyze_canonical_layer(self) -> Dict[str, Any]:
        """分析标准化视图层"""
        logger.info("分析标准化视图层...")
        
        canonical_views = [
            'p_map_today_canon_v',
            'p_size_today_canon_v'
        ]
        
        layer_info = {
            "description": "标准化视图层 - 统一格式的预测结果",
            "views": {},
            "total_rows": 0,
            "transformations": ["格式标准化", "概率计算", "决策映射"]
        }
        
        for view in canonical_views:
            view_info = self._get_table_info(view)
            view_info["dependencies"] = self._get_view_dependencies(view)
            layer_info["views"][view] = view_info
            layer_info["total_rows"] += view_info.get("row_count", 0)
        
        return layer_info
    
    def _analyze_ensemble_layer(self) -> Dict[str, Any]:
        """分析集成层"""
        logger.info("分析集成层...")
        
        ensemble_views = [
            'ensemble_pool_today_v2'
        ]
        
        layer_info = {
            "description": "集成层 - 多模型融合预测",
            "views": {},
            "total_rows": 0,
            "transformations": ["权重计算", "模型融合", "集成预测"]
        }
        
        for view in ensemble_views:
            view_info = self._get_table_info(view)
            view_info["dependencies"] = self._get_view_dependencies(view)
            layer_info["views"][view] = view_info
            layer_info["total_rows"] += view_info.get("row_count", 0)
        
        return layer_info
    
    def _analyze_signal_pool_layer(self) -> Dict[str, Any]:
        """分析信号池层"""
        logger.info("分析信号池层...")
        
        signal_views = [
            'signal_pool_union_v3'
        ]
        
        layer_info = {
            "description": "信号池层 - 统一的交易信号集合",
            "views": {},
            "total_rows": 0,
            "transformations": ["信号合并", "格式统一", "元数据添加"]
        }
        
        for view in signal_views:
            view_info = self._get_table_info(view)
            view_info["dependencies"] = self._get_view_dependencies(view)
            layer_info["views"][view] = view_info
            layer_info["total_rows"] += view_info.get("row_count", 0)
        
        return layer_info
    
    def _analyze_decision_layer(self) -> Dict[str, Any]:
        """分析决策层"""
        logger.info("分析决策层...")
        
        decision_views = [
            'lab_push_candidates_v2'
        ]
        
        layer_info = {
            "description": "决策层 - 最终的交易决策候选",
            "views": {},
            "total_rows": 0,
            "transformations": ["风险评估", "Kelly公式", "决策过滤"],
            "parameters": self._get_runtime_params()
        }
        
        for view in decision_views:
            view_info = self._get_table_info(view)
            view_info["dependencies"] = self._get_view_dependencies(view)
            layer_info["views"][view] = view_info
            layer_info["total_rows"] += view_info.get("row_count", 0)
        
        return layer_info
    
    def _get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        # 获取行数
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
        result = self.run_bq_query(query)
        
        row_count = 0
        if result["success"] and result["data"]:
            row_count = int(result["data"][0]["count"])
        
        # 获取最新数据时间
        query = f"""
        SELECT 
            MAX(DATE(ts_utc, 'Asia/Shanghai')) as latest_date,
            MIN(DATE(ts_utc, 'Asia/Shanghai')) as earliest_date
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE ts_utc IS NOT NULL
        """
        
        date_result = self.run_bq_query(query)
        latest_date = None
        earliest_date = None
        
        if date_result["success"] and date_result["data"]:
            data = date_result["data"][0]
            latest_date = data.get("latest_date")
            earliest_date = data.get("earliest_date")
        
        return {
            "row_count": row_count,
            "latest_date": latest_date,
            "earliest_date": earliest_date,
            "status": "healthy" if row_count > 0 else "empty"
        }
    
    def _get_view_dependencies(self, view_name: str) -> List[str]:
        """获取视图依赖关系"""
        try:
            cmd = ['bq', 'show', '--view', f'{self.project_id}:{self.dataset_id}.{view_name}']
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # 简单解析SQL中的表名
            sql = result.stdout
            dependencies = []
            
            # 查找FROM和JOIN子句中的表名
            import re
            pattern = r'`([^`]+\.[^`]+\.[^`]+)`'
            matches = re.findall(pattern, sql)
            
            for match in matches:
                table_name = match.split('.')[-1]  # 只取表名部分
                if table_name not in dependencies:
                    dependencies.append(table_name)
            
            return dependencies
            
        except subprocess.CalledProcessError:
            return []
    
    def _get_runtime_params(self) -> Dict[str, Any]:
        """获取运行时参数"""
        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.runtime_params`"
        result = self.run_bq_query(query)
        
        if result["success"] and result["data"]:
            return {"count": len(result["data"]), "markets": list(set([r["market"] for r in result["data"]]))}
        
        return {"count": 0, "markets": []}
    
    def _analyze_dependencies(self) -> Dict[str, List[str]]:
        """分析依赖关系"""
        logger.info("分析依赖关系...")
        
        dependencies = {}
        
        # 主要视图的依赖关系
        views_to_analyze = [
            'p_cloud_today_v',
            'p_map_today_v',
            'p_size_today_v',
            'p_map_today_canon_v',
            'p_size_today_canon_v',
            'ensemble_pool_today_v2',
            'signal_pool_union_v3',
            'lab_push_candidates_v2'
        ]
        
        for view in views_to_analyze:
            dependencies[view] = self._get_view_dependencies(view)
        
        return dependencies
    
    def _analyze_data_volumes(self) -> Dict[str, Any]:
        """分析数据量"""
        logger.info("分析数据量...")
        
        # 获取各层数据量
        query = f"""
        SELECT 
            'raw_data' as layer,
            SUM(CASE WHEN table_name IN ('cloud_pred_today_norm', 'p_cloud_clean_merged_dedup_v', 'p_map_clean_merged_dedup_v', 'p_size_clean_merged_dedup_v') THEN row_count ELSE 0 END) as total_rows
        FROM `{self.project_id}.{self.dataset_id}.__TABLES__`
        WHERE table_name IN ('cloud_pred_today_norm', 'p_cloud_clean_merged_dedup_v', 'p_map_clean_merged_dedup_v', 'p_size_clean_merged_dedup_v')
        """
        
        # 由于__TABLES__可能不可用，我们手动计算
        volumes = {
            "raw_data_layer": 0,
            "prediction_layer": 0,
            "canonical_layer": 0,
            "signal_pool": 0,
            "decision_layer": 0
        }
        
        # 原始数据层
        raw_tables = ['cloud_pred_today_norm', 'p_cloud_clean_merged_dedup_v', 'p_map_clean_merged_dedup_v', 'p_size_clean_merged_dedup_v']
        for table in raw_tables:
            info = self._get_table_info(table)
            volumes["raw_data_layer"] += info.get("row_count", 0)
        
        # 预测层
        pred_tables = ['p_cloud_today_v', 'p_map_today_v', 'p_size_today_v']
        for table in pred_tables:
            info = self._get_table_info(table)
            volumes["prediction_layer"] += info.get("row_count", 0)
        
        # 标准化层
        canon_tables = ['p_map_today_canon_v', 'p_size_today_canon_v']
        for table in canon_tables:
            info = self._get_table_info(table)
            volumes["canonical_layer"] += info.get("row_count", 0)
        
        # 信号池
        signal_info = self._get_table_info('signal_pool_union_v3')
        volumes["signal_pool"] = signal_info.get("row_count", 0)
        
        # 决策层
        decision_info = self._get_table_info('lab_push_candidates_v2')
        volumes["decision_layer"] = decision_info.get("row_count", 0)
        
        return volumes
    
    def _identify_bottlenecks(self) -> List[Dict[str, Any]]:
        """识别瓶颈"""
        logger.info("识别系统瓶颈...")
        
        bottlenecks = []
        
        # 检查空表
        empty_tables = []
        critical_tables = [
            'lab_push_candidates_v2',
            'ensemble_pool_today_v2',
            'signal_pool_union_v3'
        ]
        
        for table in critical_tables:
            info = self._get_table_info(table)
            if info["row_count"] == 0:
                empty_tables.append(table)
        
        if empty_tables:
            bottlenecks.append({
                "type": "empty_tables",
                "severity": "high",
                "description": f"关键表无数据: {', '.join(empty_tables)}",
                "tables": empty_tables
            })
        
        # 检查数据新鲜度
        stale_tables = []
        for table in ['p_cloud_today_v', 'p_map_today_v', 'p_size_today_v']:
            info = self._get_table_info(table)
            if info["latest_date"]:
                from datetime import datetime, date
                latest = datetime.strptime(info["latest_date"], "%Y-%m-%d").date()
                today = date.today()
                days_behind = (today - latest).days
                
                if days_behind > 1:
                    stale_tables.append({"table": table, "days_behind": days_behind})
        
        if stale_tables:
            bottlenecks.append({
                "type": "stale_data",
                "severity": "medium",
                "description": "数据过期",
                "details": stale_tables
            })
        
        return bottlenecks
    
    def generate_mermaid_diagram(self, flow_analysis: Dict[str, Any]) -> str:
        """生成Mermaid流程图"""
        logger.info("生成Mermaid流程图...")
        
        mermaid = """
graph TD
    %% 原始数据层
    API1[Cloud API] --> T1[cloud_pred_today_norm]
    API2[Map API] --> T2[p_map_clean_merged_dedup_v]
    API3[Size API] --> T3[p_size_clean_merged_dedup_v]
    
    %% 预测视图层
    T1 --> V1[p_cloud_today_v]
    T2 --> V2[p_map_today_v]
    T3 --> V3[p_size_today_v]
    
    %% 标准化视图层
    V2 --> C1[p_map_today_canon_v]
    V3 --> C2[p_size_today_canon_v]
    
    %% 集成层
    V1 --> E1[ensemble_pool_today_v2]
    V2 --> E1
    V3 --> E1
    
    %% 信号池层
    E1 --> S1[signal_pool_union_v3]
    C1 --> S1
    C2 --> S1
    
    %% 决策层
    S1 --> D1[lab_push_candidates_v2]
    P1[runtime_params] --> D1
    
    %% 样式
    classDef apiClass fill:#e1f5fe
    classDef rawClass fill:#f3e5f5
    classDef viewClass fill:#e8f5e8
    classDef canonClass fill:#fff3e0
    classDef ensembleClass fill:#fce4ec
    classDef signalClass fill:#e0f2f1
    classDef decisionClass fill:#ffebee
    
    class API1,API2,API3 apiClass
    class T1,T2,T3 rawClass
    class V1,V2,V3 viewClass
    class C1,C2 canonClass
    class E1 ensembleClass
    class S1 signalClass
    class D1,P1 decisionClass
"""
        
        return mermaid.strip()
    
    def generate_ascii_diagram(self, flow_analysis: Dict[str, Any]) -> str:
        """生成ASCII流程图"""
        logger.info("生成ASCII流程图...")
        
        volumes = flow_analysis["data_volumes"]
        
        ascii_diagram = f"""
PC28数据流程图
================

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Cloud API     │    │    Map API      │    │   Size API      │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│cloud_pred_today │    │p_map_clean_     │    │p_size_clean_    │
│_norm            │    │merged_dedup_v   │    │merged_dedup_v   │
│({volumes["raw_data_layer"]} rows)        │    │                 │    │                 │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│p_cloud_today_v  │    │p_map_today_v    │    │p_size_today_v   │
│                 │    │                 │    │                 │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │                      ▼                      ▼
          │            ┌─────────────────┐    ┌─────────────────┐
          │            │p_map_today_     │    │p_size_today_    │
          │            │canon_v          │    │canon_v          │
          │            └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                                 ▼
                       ┌─────────────────┐
                       │ensemble_pool_   │
                       │today_v2         │
                       └─────────┬───────┘
                                 │
                                 ▼
                       ┌─────────────────┐
                       │signal_pool_     │
                       │union_v3         │
                       │({volumes["signal_pool"]} rows)        │
                       └─────────┬───────┘
                                 │
                                 ▼
                       ┌─────────────────┐
                       │lab_push_        │
                       │candidates_v2    │
                       │({volumes["decision_layer"]} rows)        │
                       └─────────────────┘

数据流程说明:
1. 原始数据层: API直接写入 ({volumes["raw_data_layer"]} 总行数)
2. 预测视图层: 基础预测处理 ({volumes["prediction_layer"]} 总行数)
3. 标准化层: 格式统一 ({volumes["canonical_layer"]} 总行数)
4. 信号池层: 信号合并 ({volumes["signal_pool"]} 行数)
5. 决策层: 最终决策 ({volumes["decision_layer"]} 行数)
"""
        
        return ascii_diagram
    
    def save_analysis_report(self, flow_analysis: Dict[str, Any]):
        """保存分析报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存JSON报告
        json_filename = f"pc28_data_flow_analysis_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(flow_analysis, f, indent=2, ensure_ascii=False)
        
        # 生成Markdown报告
        md_filename = f"pc28_data_flow_report_{timestamp}.md"
        with open(md_filename, 'w', encoding='utf-8') as f:
            f.write(self._generate_markdown_report(flow_analysis))
        
        # 生成Mermaid图
        mermaid_filename = f"pc28_data_flow_diagram_{timestamp}.mmd"
        with open(mermaid_filename, 'w', encoding='utf-8') as f:
            f.write(self.generate_mermaid_diagram(flow_analysis))
        
        logger.info(f"分析报告已保存:")
        logger.info(f"  JSON: {json_filename}")
        logger.info(f"  Markdown: {md_filename}")
        logger.info(f"  Mermaid: {mermaid_filename}")
    
    def _generate_markdown_report(self, flow_analysis: Dict[str, Any]) -> str:
        """生成Markdown报告"""
        layers = flow_analysis["layers"]
        volumes = flow_analysis["data_volumes"]
        bottlenecks = flow_analysis["bottlenecks"]
        
        report = f"""# PC28数据流程分析报告

**生成时间**: {flow_analysis["timestamp"]}

## 概览

PC28系统采用分层架构，数据从API采集开始，经过多层处理最终生成交易决策。

### 数据量统计
- 原始数据层: {volumes["raw_data_layer"]:,} 行
- 预测视图层: {volumes["prediction_layer"]:,} 行  
- 标准化层: {volumes["canonical_layer"]:,} 行
- 信号池: {volumes["signal_pool"]:,} 行
- 决策层: {volumes["decision_layer"]:,} 行

## 数据流程层级

### 1. 原始数据层
{layers["1_raw_data"]["description"]}

**数据源**: {", ".join(layers["1_raw_data"]["data_sources"])}
**更新频率**: {layers["1_raw_data"]["update_frequency"]}
**总行数**: {layers["1_raw_data"]["total_rows"]:,}

**表详情**:
"""
        
        for table, info in layers["1_raw_data"]["tables"].items():
            report += f"- `{table}`: {info['row_count']:,} 行 ({info['status']})\n"
        
        report += f"""
### 2. 预测视图层
{layers["2_prediction_views"]["description"]}

**转换操作**: {", ".join(layers["2_prediction_views"]["transformations"])}
**总行数**: {layers["2_prediction_views"]["total_rows"]:,}

**视图详情**:
"""
        
        for view, info in layers["2_prediction_views"]["views"].items():
            deps = ", ".join(info.get("dependencies", []))
            report += f"- `{view}`: {info['row_count']:,} 行, 依赖: {deps}\n"
        
        report += f"""
### 3. 标准化视图层
{layers["3_canonical_views"]["description"]}

**转换操作**: {", ".join(layers["3_canonical_views"]["transformations"])}
**总行数**: {layers["3_canonical_views"]["total_rows"]:,}

**视图详情**:
"""
        
        for view, info in layers["3_canonical_views"]["views"].items():
            deps = ", ".join(info.get("dependencies", []))
            report += f"- `{view}`: {info['row_count']:,} 行, 依赖: {deps}\n"
        
        report += f"""
### 4. 集成层
{layers["4_ensemble_layer"]["description"]}

**转换操作**: {", ".join(layers["4_ensemble_layer"]["transformations"])}
**总行数**: {layers["4_ensemble_layer"]["total_rows"]:,}

### 5. 信号池层
{layers["5_signal_pool"]["description"]}

**转换操作**: {", ".join(layers["5_signal_pool"]["transformations"])}
**总行数**: {layers["5_signal_pool"]["total_rows"]:,}

### 6. 决策层
{layers["6_decision_layer"]["description"]}

**转换操作**: {", ".join(layers["6_decision_layer"]["transformations"])}
**总行数**: {layers["6_decision_layer"]["total_rows"]:,}
**参数配置**: {layers["6_decision_layer"]["parameters"]["count"]} 个参数, 支持市场: {", ".join(layers["6_decision_layer"]["parameters"]["markets"])}

## 系统瓶颈分析

"""
        
        if bottlenecks:
            for bottleneck in bottlenecks:
                report += f"### {bottleneck['type']} ({bottleneck['severity']})\n"
                report += f"{bottleneck['description']}\n\n"
        else:
            report += "✅ 未发现明显瓶颈\n\n"
        
        report += """
## ASCII数据流程图

```
""" + self.generate_ascii_diagram(flow_analysis) + """
```

## 建议

1. **监控数据新鲜度**: 确保原始数据及时更新
2. **优化空表问题**: 重点关注决策层数据生成
3. **建立告警机制**: 对关键节点设置监控告警
4. **定期健康检查**: 建议每小时检查一次系统状态

"""
        
        return report

def main():
    """主函数"""
    import sys
    
    diagram = PC28DataFlowDiagram()
    
    if len(sys.argv) > 1 and sys.argv[1] == "analyze":
        print("🔍 开始分析PC28数据流程...")
        flow_analysis = diagram.analyze_data_flow()
        
        print("\n📊 数据流程分析完成!")
        print(f"总层级: {len(flow_analysis['layers'])}")
        print(f"数据量: {sum(flow_analysis['data_volumes'].values()):,} 总行数")
        print(f"瓶颈: {len(flow_analysis['bottlenecks'])} 个")
        
        # 保存报告
        diagram.save_analysis_report(flow_analysis)
        
        # 显示ASCII图
        print("\n" + "="*60)
        print(diagram.generate_ascii_diagram(flow_analysis))
        print("="*60)
        
    else:
        print("用法: python pc28_data_flow_diagram.py analyze")

if __name__ == "__main__":
    main()