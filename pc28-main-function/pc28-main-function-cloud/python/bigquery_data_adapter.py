#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQueryDataAdapter
- 仅依赖 bq CLI：通过子进程执行 SQL，返回 JSON
- 提供：基本查询、KPI快查、rolling指标、候选读取、账本写入SQL模板拼装（交由 ledger_io 调用）
"""
from __future__ import annotations
import os, json, subprocess, shlex, time, datetime
from typing import Any, Dict, List, Optional

class BQ:
    def __init__(self, project:str, ds_lab:str, ds_draw:str, bqloc:str, tz:str):
        self.proj, self.ds_lab, self.ds_draw, self.loc, self.tz = project, ds_lab, ds_draw, bqloc, tz

    def _run_json(self, sql:str, timeout:int=120)->List[Dict[str,Any]]:
        cmd = f"bq --location={shlex.quote(self.loc)} query --use_legacy_sql=false --format=json {shlex.quote(sql)}"
        out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, timeout=timeout)
        return json.loads(out.decode("utf-8") or "[]")

    def draws_today(self)->int:
        sql = f"SELECT COUNT(*) AS n FROM `{self.proj}.{self.ds_draw}.draws_14w_dedup_v` WHERE DATE(timestamp,'{self.tz}')=CURRENT_DATE('{self.tz}')"
        j = self._run_json(sql)
        return int(j[0].get("n",0)) if j else 0

    def kpi_window(self, window_min:int=60)->Dict[str,Any]:
        sql = f"""
WITH L AS (
  SELECT prediction as market, outcome, probability as p_win, timestamp as created_at
  FROM `{self.proj}.{self.ds_lab}.score_ledger`
  WHERE (tag IS NULL OR tag='prod')
    AND day_id_cst=CURRENT_DATE('{self.tz}')
    AND market IN ('oe','size')
),
W AS (
  SELECT market,
         COUNT(*) AS n_ord_w,
         COUNTIF(outcome IN ('win','lose')) AS n_set_w,
         SAFE_DIVIDE(COUNTIF(outcome='win'), NULLIF(COUNTIF(outcome IN ('win','lose')),0)) AS acc_w,
         AVG(p_win) AS pbar_w,
         AVG( (IF(outcome='win',1,0)-p_win)*(IF(outcome='win',1,0)-p_win) ) AS brier_w
  FROM L
  WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_min} MINUTE)
  GROUP BY market
),
D AS (
  SELECT COUNT(*) AS n_draw_w
  FROM `{self.proj}.{self.ds_draw}.draws_14w_dedup_v`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_min} MINUTE)
)
SELECT market, n_ord_w, n_set_w, acc_w, pbar_w, brier_w, (SELECT n_draw_w FROM D) AS n_draw_w
FROM W
"""
        rows = self._run_json(sql)
        out = {"_meta":{"window_min":window_min}}
        for r in rows:
            m = r.get("market")
            if not m: continue
            n_ord = int(r.get("n_ord_w",0)); n_draw = int(r.get("n_draw_w",0))
            cov = float(n_ord/n_draw) if n_draw>0 else 0.0
            out[m] = {
                "cov_w": round(cov,6),
                "acc": r.get("acc_w",None),
                "pbar": r.get("pbar_w",None),
                "brier": r.get("brier_w",None),
                "n_set": int(r.get("n_set_w",0)),
                "n_ord": n_ord,
                "n_draw": n_draw
            }
        return out

    def read_candidates(self)->List[Dict[str,Any]]:
        """读取正EV候选（视图需预先创建）。返回字段至少包含：
           draw_id, market, p_cloud,p_map,p_size, session, tail, p_even (或统一 p)
        """
        sql = f"SELECT * FROM `{self.proj}.{self.ds_lab}.lab_push_candidates_v2` WHERE day_id_cst=CURRENT_DATE('{self.tz}')"
        try:
            return self._run_json(sql, timeout=180)
        except Exception:
            return []
