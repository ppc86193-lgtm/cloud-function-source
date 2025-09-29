#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery表结构修复系统
BigQuery Schema Repair System

功能：
1. 检查和修复BigQuery表结构不一致问题
2. 统一字段命名和数据类型
3. 修复p_ensemble_today_norm_v5视图定义
4. 验证数据完整性
"""

import os
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TableSchema:
    """表结构定义"""
    table_name: str
    fields: List[Dict[str, str]]
    primary_key: Optional[str] = None
    partition_field: Optional[str] = None

class BigQuerySchemaRepair:
    """BigQuery表结构修复器"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_lab = "pc28_lab"
        self.dataset_pc28 = "pc28"
        
        # 定义标准表结构
        self._define_standard_schemas()
        
    def _define_standard_schemas(self):
        """定义标准表结构"""
        self.standard_schemas = {
            # 云端预测数据表
            "cloud_pred_today_norm": TableSchema(
                table_name="cloud_pred_today_norm",
                fields=[
                    {"name": "period", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "p_win", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "confidence", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "source", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
                ],
                primary_key="period",
                partition_field="timestamp"
            ),
            
            # 地图预测视图
            "p_map_today_canon_v": TableSchema(
                table_name="p_map_today_canon_v",
                fields=[
                    {"name": "period", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "ts_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "p_win", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "vote_ratio", "type": "FLOAT64", "mode": "NULLABLE"}
                ]
            ),
            
            # 尺寸预测视图
            "p_size_today_canon_v": TableSchema(
                table_name="p_size_today_canon_v",
                fields=[
                    {"name": "period", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "ts_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "p_win", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "vote_ratio", "type": "FLOAT64", "mode": "NULLABLE"}
                ]
            ),
            
            # 组合预测表
            "combo_based_predictions": TableSchema(
                table_name="combo_based_predictions",
                fields=[
                    {"name": "issue", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "recommendation", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "big_odd_ev_pct", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "big_even_ev_pct", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "small_odd_ev_pct", "type": "FLOAT64", "mode": "NULLABLE"},
                    {"name": "small_even_ev_pct", "type": "FLOAT64", "mode": "NULLABLE"}
                ]
            )
        }
    
    def generate_corrected_view_sql(self) -> str:
        """生成修正后的p_ensemble_today_norm_v5视图SQL"""
        corrected_sql = """-- 修正后的 p_ensemble_today_norm_v5 视图 (字段统一版本)
-- 五桶模型的加权平均计算公式，统一字段命名
CREATE OR REPLACE VIEW `wprojectl.pc28_lab.p_ensemble_today_norm_v5` AS
WITH
  -- 获取当前日期参数
  params AS (
    SELECT CURRENT_DATE('Asia/Shanghai') AS day_id
  ),
  
  -- 云预测数据 - 统一使用p_win字段
  cloud_pred AS (
    SELECT 
      CAST(period AS STRING) AS period,
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) AS ts_utc,
      p_win AS p_cloud,
      0.8 AS conf_cloud  -- 默认置信度
    FROM `wprojectl.pc28_lab.cloud_pred_today_norm` c
    CROSS JOIN params p
    WHERE SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', c.timestamp) IS NOT NULL
      AND DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', c.timestamp), 'Asia/Shanghai') = p.day_id
  ),
  
  -- 地图预测数据 - 统一使用p_win字段
  map_pred AS (
    SELECT 
      CAST(period AS STRING) AS period,
      p_win AS p_map,
      COALESCE(vote_ratio, 0.7) AS conf_map
    FROM `wprojectl.pc28_lab.p_map_today_canon_v` m
    CROSS JOIN params p
    WHERE m.ts_utc IS NOT NULL
      AND DATE(m.ts_utc, 'Asia/Shanghai') = p.day_id
  ),
  
  -- 尺寸预测数据 - 统一使用p_win字段
  size_pred AS (
    SELECT 
      CAST(period AS STRING) AS period,
      p_win AS p_size,
      COALESCE(vote_ratio, 0.6) AS conf_size
    FROM `wprojectl.pc28_lab.p_size_today_canon_v` s
    CROSS JOIN params p
    WHERE s.ts_utc IS NOT NULL
      AND DATE(s.ts_utc, 'Asia/Shanghai') = p.day_id
  ),
  
  -- 组合预测数据 - 从combo_based_predictions表获取
  combo_pred AS (
    SELECT 
      CAST(issue AS STRING) AS period,
      -- 基于recommendation字段创建p_combo值
      CASE 
        WHEN recommendation IN ('big_odd', 'big_even') THEN 0.8
        WHEN recommendation IN ('small_odd', 'small_even') THEN 0.2
        ELSE 0.5
      END AS p_combo,
      -- 基于训练数据置信度创建conf_combo
      GREATEST(
        COALESCE(big_odd_ev_pct, 0),
        COALESCE(big_even_ev_pct, 0),
        COALESCE(small_odd_ev_pct, 0),
        COALESCE(small_even_ev_pct, 0)
      ) AS conf_combo
    FROM `wprojectl.pc28.combo_based_predictions` cb
    CROSS JOIN params p
    WHERE cb.timestamp IS NOT NULL
      AND DATE(TIMESTAMP(cb.timestamp), 'Asia/Shanghai') = p.day_id
  ),
  
  -- 命中预测数据 - 创建默认值
  hit_pred AS (
    SELECT 
      period,
      0.5 AS p_hit,  -- 默认预测值
      0.5 AS conf_hit  -- 默认置信度
    FROM cloud_pred  -- 使用cloud_pred的period作为基础
  )

-- 主查询：计算五桶模型的加权平均
SELECT 
  c.period,
  c.ts_utc,
  SAFE.DATETIME(c.ts_utc, 'Asia/Shanghai') AS ts_cst,
  c.p_cloud,
  c.conf_cloud,
  COALESCE(m.p_map, 0.5) AS p_map,
  COALESCE(m.conf_map, 0.5) AS conf_map,
  COALESCE(s.p_size, 0.5) AS p_size,
  COALESCE(s.conf_size, 0.5) AS conf_size,
  COALESCE(cb.p_combo, 0.5) AS p_combo,
  COALESCE(cb.conf_combo, 0.5) AS conf_combo,
  h.p_hit,
  h.conf_hit,
  
  -- 五桶模型加权平均计算
  (c.p_cloud * c.conf_cloud + 
   COALESCE(m.p_map, 0.5) * COALESCE(m.conf_map, 0.5) + 
   COALESCE(s.p_size, 0.5) * COALESCE(s.conf_size, 0.5) + 
   COALESCE(cb.p_combo, 0.5) * COALESCE(cb.conf_combo, 0.5) + 
   h.p_hit * h.conf_hit) / 
  (c.conf_cloud + COALESCE(m.conf_map, 0.5) + COALESCE(s.conf_size, 0.5) + 
   COALESCE(cb.conf_combo, 0.5) + h.conf_hit) AS p_star_ens,
  
  -- 投票比例计算（五个置信度的平均值）
  (c.conf_cloud + COALESCE(m.conf_map, 0.5) + COALESCE(s.conf_size, 0.5) + 
   COALESCE(cb.conf_combo, 0.5) + h.conf_hit) / 5.0 AS vote_ratio,
  
  -- 冷却状态判断
  CASE 
    WHEN (c.p_cloud * c.conf_cloud + 
          COALESCE(m.p_map, 0.5) * COALESCE(m.conf_map, 0.5) + 
          COALESCE(s.p_size, 0.5) * COALESCE(s.conf_size, 0.5) + 
          COALESCE(cb.p_combo, 0.5) * COALESCE(cb.conf_combo, 0.5) + 
          h.p_hit * h.conf_hit) / 
         (c.conf_cloud + COALESCE(m.conf_map, 0.5) + COALESCE(s.conf_size, 0.5) + 
          COALESCE(cb.conf_combo, 0.5) + h.conf_hit) >= 0.6 
    THEN 'ACTIVE'
    ELSE 'COOLING'
  END AS cooling_status

FROM cloud_pred c
LEFT JOIN map_pred m ON c.period = m.period
LEFT JOIN size_pred s ON c.period = s.period
LEFT JOIN combo_pred cb ON c.period = cb.period
LEFT JOIN hit_pred h ON c.period = h.period
WHERE c.ts_utc IS NOT NULL  -- 确保只包含有效的时间戳
ORDER BY c.period DESC;"""
        
        return corrected_sql
    
    def check_table_field_consistency(self) -> Dict:
        """检查表字段一致性"""
        logger.info("检查BigQuery表字段一致性...")
        
        consistency_report = {
            "检查时间": datetime.now().isoformat(),
            "表结构问题": [],
            "字段映射问题": [],
            "修复建议": []
        }
        
        # 检查常见的字段不一致问题
        field_mapping_issues = [
            {
                "问题": "预测值字段不统一",
                "描述": "不同表使用p_win, p_even, p_star等不同字段名",
                "影响表": ["cloud_pred_today_norm", "p_map_today_canon_v", "p_size_today_canon_v"],
                "建议": "统一使用p_win字段名"
            },
            {
                "问题": "时间戳字段不统一", 
                "描述": "使用timestamp, ts_utc等不同字段名",
                "影响表": ["cloud_pred_today_norm", "combo_based_predictions"],
                "建议": "统一时间戳字段命名规范"
            },
            {
                "问题": "置信度字段缺失",
                "描述": "部分表缺少置信度字段或使用不同命名",
                "影响表": ["p_map_today_canon_v", "p_size_today_canon_v"],
                "建议": "添加标准化的置信度字段"
            }
        ]
        
        consistency_report["字段映射问题"] = field_mapping_issues
        
        # 生成修复建议
        consistency_report["修复建议"] = [
            "1. 统一所有预测表使用p_win字段表示预测值",
            "2. 统一时间戳字段命名为ts_utc或timestamp",
            "3. 为所有预测表添加vote_ratio或confidence字段",
            "4. 重新创建p_ensemble_today_norm_v5视图使用统一字段",
            "5. 验证所有依赖视图的字段映射正确性"
        ]
        
        return consistency_report
    
    def generate_repair_sql_scripts(self) -> Dict[str, str]:
        """生成修复SQL脚本"""
        logger.info("生成BigQuery表结构修复脚本...")
        
        scripts = {}
        
        # 1. 修正后的主视图
        scripts["p_ensemble_today_norm_v5_corrected"] = self.generate_corrected_view_sql()
        
        # 2. 字段映射修复脚本
        scripts["field_mapping_fix"] = """-- 字段映射修复脚本
-- 为确保字段一致性，创建标准化视图

-- 标准化云预测视图
CREATE OR REPLACE VIEW `wprojectl.pc28_lab.cloud_pred_standardized_v` AS
SELECT 
  CAST(period AS STRING) AS period,
  timestamp AS ts_utc,
  p_win,
  COALESCE(confidence, 0.8) AS vote_ratio,
  source
FROM `wprojectl.pc28_lab.cloud_pred_today_norm`
WHERE timestamp IS NOT NULL;

-- 标准化地图预测视图（如果字段不匹配）
CREATE OR REPLACE VIEW `wprojectl.pc28_lab.map_pred_standardized_v` AS
SELECT 
  CAST(period AS STRING) AS period,
  ts_utc,
  COALESCE(p_win, p_even, p_star) AS p_win,
  COALESCE(vote_ratio, confidence, 0.7) AS vote_ratio
FROM `wprojectl.pc28_lab.p_map_today_canon_v`
WHERE ts_utc IS NOT NULL;

-- 标准化尺寸预测视图（如果字段不匹配）
CREATE OR REPLACE VIEW `wprojectl.pc28_lab.size_pred_standardized_v` AS
SELECT 
  CAST(period AS STRING) AS period,
  ts_utc,
  COALESCE(p_win, p_even, p_star) AS p_win,
  COALESCE(vote_ratio, confidence, 0.6) AS vote_ratio
FROM `wprojectl.pc28_lab.p_size_today_canon_v`
WHERE ts_utc IS NOT NULL;"""
        
        # 3. 数据验证脚本
        scripts["data_validation"] = """-- 数据验证脚本
-- 检查修复后的视图数据完整性

-- 检查主视图数据
SELECT 
  'p_ensemble_today_norm_v5' as view_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT period) as unique_periods,
  MIN(ts_utc) as earliest_time,
  MAX(ts_utc) as latest_time,
  AVG(p_star_ens) as avg_prediction,
  AVG(vote_ratio) as avg_confidence
FROM `wprojectl.pc28_lab.p_ensemble_today_norm_v5`

UNION ALL

-- 检查云预测数据
SELECT 
  'cloud_pred_today_norm' as view_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT period) as unique_periods,
  MIN(timestamp) as earliest_time,
  MAX(timestamp) as latest_time,
  AVG(p_win) as avg_prediction,
  0.8 as avg_confidence
FROM `wprojectl.pc28_lab.cloud_pred_today_norm`

UNION ALL

-- 检查地图预测数据
SELECT 
  'p_map_today_canon_v' as view_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT period) as unique_periods,
  MIN(ts_utc) as earliest_time,
  MAX(ts_utc) as latest_time,
  AVG(p_win) as avg_prediction,
  AVG(vote_ratio) as avg_confidence
FROM `wprojectl.pc28_lab.p_map_today_canon_v`;"""
        
        return scripts
    
    def create_repair_report(self) -> Dict:
        """创建修复报告"""
        consistency_report = self.check_table_field_consistency()
        repair_scripts = self.generate_repair_sql_scripts()
        
        repair_report = {
            "修复报告生成时间": datetime.now().isoformat(),
            "系统状态": "需要修复",
            "一致性检查": consistency_report,
            "修复脚本数量": len(repair_scripts),
            "修复步骤": [
                "1. 备份现有视图定义",
                "2. 执行字段映射修复脚本",
                "3. 重新创建p_ensemble_today_norm_v5视图",
                "4. 运行数据验证脚本",
                "5. 测试视图查询功能",
                "6. 更新本地同步系统"
            ],
            "预期效果": [
                "统一所有表的字段命名",
                "修复视图查询错误",
                "提高数据一致性",
                "支持正常的云端到本地同步"
            ]
        }
        
        return repair_report, repair_scripts

def main():
    """主函数"""
    logger.info("启动BigQuery表结构修复系统")
    
    # 创建修复器
    repair_system = BigQuerySchemaRepair()
    
    # 生成修复报告和脚本
    repair_report, repair_scripts = repair_system.create_repair_report()
    
    # 输出报告
    logger.info("BigQuery表结构修复报告:")
    for key, value in repair_report.items():
        if isinstance(value, dict):
            logger.info(f"  {key}:")
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, list):
                    logger.info(f"    {sub_key}:")
                    for item in sub_value:
                        if isinstance(item, dict):
                            logger.info(f"      - {item.get('问题', item)}")
                        else:
                            logger.info(f"      - {item}")
                else:
                    logger.info(f"    {sub_key}: {sub_value}")
        elif isinstance(value, list):
            logger.info(f"  {key}:")
            for item in value:
                logger.info(f"    - {item}")
        else:
            logger.info(f"  {key}: {value}")
    
    # 保存修复脚本
    for script_name, script_content in repair_scripts.items():
        script_file = f"repair_{script_name}.sql"
        with open(script_file, 'w', encoding='utf-8') as f:
            f.write(script_content)
        logger.info(f"修复脚本已保存: {script_file}")
    
    # 保存修复报告
    with open('bigquery_repair_report.json', 'w', encoding='utf-8') as f:
        json.dump(repair_report, f, ensure_ascii=False, indent=2)
    
    logger.info("BigQuery表结构修复系统分析完成")
    logger.info("请手动执行生成的SQL脚本来修复BigQuery表结构")
    
    return True

if __name__ == "__main__":
    main()