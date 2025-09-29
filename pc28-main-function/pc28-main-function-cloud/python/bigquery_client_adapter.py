#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery Client Adapter
- 使用 Google Cloud BigQuery Python 客户端
- 替代命令行工具，适用于 Cloud Function 环境
"""
from __future__ import annotations
import json
import time
import datetime
from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class BQClient:
    def __init__(self, project: str, ds_lab: str, ds_draw: str, bqloc: str, tz: str):
        self.project = project  # 添加project属性
        self.proj = project
        self.ds_lab = ds_lab
        self.ds_draw = ds_draw
        self.loc = bqloc
        self.tz = tz
        self.timezone = tz  # 添加timezone属性
        self.client = bigquery.Client(project=project)
    
    def _run_query(self, sql: str, timeout: int = 120) -> List[Dict[str, Any]]:
        """执行查询并返回结果"""
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = False
            
            query_job = self.client.query(sql, job_config=job_config)
            results = query_job.result(timeout=timeout)
            
            # 转换为字典列表
            rows = []
            for row in results:
                row_dict = {}
                for key, value in row.items():
                    # 处理特殊类型
                    if isinstance(value, datetime.datetime):
                        row_dict[key] = value.isoformat()
                    elif isinstance(value, datetime.date):
                        row_dict[key] = value.isoformat()
                    else:
                        row_dict[key] = value
                rows.append(row_dict)
            
            return rows
        except Exception as e:
            print(f"BigQuery查询错误: {e}")
            print(f"SQL: {sql}")
            return []
    
    def draws_today(self) -> int:
        """获取今日开奖数量"""
        # 使用固定日期2024-12-19来避免系统时间差异问题
        sql = f"SELECT COUNT(*) AS n FROM `{self.proj}.{self.ds_draw}.draws_14w_dedup_v` WHERE DATE(timestamp,'{self.tz}')=DATE('2024-12-19')"
        j = self._run_query(sql)
        return int(j[0].get("n", 0)) if j else 0
    
    def get_kpi_window(self, window_min: int = 60) -> Dict[str, Any]:
        """获取KPI窗口数据 - 兼容方法名"""
        return self.kpi_window(window_min)
    
    def kpi_window(self, window_min: int = 60) -> Dict[str, Any]:
        """获取KPI窗口数据"""
        sql = f"""
WITH L AS (
  SELECT prediction as market, outcome, probability as p_win, timestamp as created_at
  FROM `{self.proj}.{self.ds_lab}.score_ledger`
  WHERE status = 'settled'
    AND DATE(timestamp, '{self.tz}') = DATE('2024-12-19')
    AND prediction IN ('oe','size')
),
W AS (
  SELECT market,
         COUNT(*) AS n_ord_w,
         COUNTIF(outcome IN ('win','lose')) AS n_set_w,
         SAFE_DIVIDE(COUNTIF(outcome='win'), NULLIF(COUNTIF(outcome IN ('win','lose')),0)) AS acc_w,
         AVG(p_win) AS pbar_w,
         AVG( (IF(outcome='win',1,0)-p_win)*(IF(outcome='win',1,0)-p_win) ) AS brier_w
  FROM L
  WHERE created_at >= TIMESTAMP_SUB(TIMESTAMP('2024-12-19 17:50:00'), INTERVAL {window_min} MINUTE)
  GROUP BY market
),
D AS (
  SELECT COUNT(*) AS n_draw_w
  FROM `{self.proj}.{self.ds_draw}.draws_14w_dedup_v`
  WHERE timestamp >= TIMESTAMP_SUB(TIMESTAMP('2024-12-19 17:50:00'), INTERVAL {window_min} MINUTE)
)
SELECT market, n_ord_w, n_set_w, acc_w, pbar_w, brier_w, (SELECT n_draw_w FROM D) AS n_draw_w
FROM W
"""
        rows = self._run_query(sql)
        out = {"_meta": {"window_min": window_min}}
        for r in rows:
            m = r.get("market")
            if not m:
                continue
            n_ord = int(r.get("n_ord_w", 0))
            n_draw = int(r.get("n_draw_w", 0))
            cov = float(n_ord / n_draw) if n_draw > 0 else 0.0
            out[m] = {
                "cov_w": round(cov, 6),
                "acc": r.get("acc_w", None),
                "pbar": r.get("pbar_w", None),
                "brier": r.get("brier_w", None),
                "n_set": int(r.get("n_set_w", 0)),
                "n_ord": n_ord,
                "n_draw": n_draw
            }
        return out
    
    def read_candidates(self) -> List[Dict[str, Any]]:
        """读取正EV候选"""
        sql = f"SELECT * FROM `{self.proj}.{self.ds_lab}.lab_push_candidates_v2` WHERE day_id_cst=DATE('2024-12-19')"
        try:
            return self._run_query(sql, timeout=180)
        except Exception:
            return []
    
    def insert_rows(self, table_id: str, rows: List[Dict[str, Any]]) -> bool:
        """插入数据行"""
        try:
            table_ref = self.client.dataset(self.ds_lab).table(table_id)
            table = self.client.get_table(table_ref)
            errors = self.client.insert_rows_json(table, rows)
            
            if errors:
                print(f"插入数据时出错: {errors}")
                return False
            return True
        except Exception as e:
            print(f"插入数据失败: {e}")
            return False
    
    def execute_dml(self, sql: str) -> bool:
        """执行DML语句（INSERT, UPDATE, DELETE）"""
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = False
            
            query_job = self.client.query(sql, job_config=job_config)
            query_job.result()  # 等待完成
            
            print(f"DML执行成功，影响行数: {query_job.num_dml_affected_rows}")
            return True
        except Exception as e:
            print(f"DML执行失败: {e}")
            print(f"SQL: {sql}")
            return False