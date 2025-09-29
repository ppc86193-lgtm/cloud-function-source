#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json, subprocess, shlex, time, datetime
from typing import Dict, Any, List

def _bq(sql:str, loc:str):
    cmd = f"bq --location={shlex.quote(loc)} query --use_legacy_sql=false --format=none {shlex.quote(sql)}"
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        if "Permission" in str(e) or "not exist" in str(e):
            print(f"警告: 数据库操作失败 - {e}")
            return False
        raise
    return True

def upsert_order(order:Dict[str,Any], env:Dict[str,str]):
    proj, ds, loc, tz = env["PROJECT"], env["DS_LAB"], env["BQLOC"], env["TZ"]
    sql = f"""
INSERT INTO `{proj}.{ds}.score_ledger` (id,day_id_cst,market,draw_id,created_at,p_win,ev,kelly_frac,stake_u,outcome,pnl_u,tag,note)
VALUES (
  '{order["id"]}',
  CURRENT_DATE('{tz}'),
  '{order["market"]}',
  {order["draw_id"]},
  CURRENT_TIMESTAMP(),
  {order["p_win"]},
  {order["ev"]},
  {order["kelly_frac"]},
  {order["stake_u"]},
  NULL,
  NULL,
  'prod',
  '{order.get("note","")}'
)
"""
    _bq(sql, loc)

# 结算（示例：依托 draws 视图；这里仅给出示意 SQL，实际结果映射需按业务定制）
def settle_orders(env:Dict[str,str]):
    proj, ds, dsd, loc, tz = env["PROJECT"], env["DS_LAB"], env["DS_DRAW"], env["BQLOC"], env["TZ"]
    # 示例：按 draw_id 与市场胜负规则结算
    sql = f"""
UPDATE `{proj}.{ds}.score_ledger` L
SET outcome=CASE
      WHEN L.market='oe'  THEN IF(MOD(d.sum28,2)=0,'win','lose')
      WHEN L.market='size' THEN IF(d.sum28>=14,'win','lose')
      ELSE NULL END,
    pnl_u = CASE
      WHEN L.outcome='win'  THEN L.stake_u
      WHEN L.outcome='lose' THEN -L.stake_u
      ELSE NULL END
FROM (
  SELECT draw_id, sum28
  FROM `{proj}.{dsd}.draws_14w_dedup_v`
  WHERE DATE(timestamp,'{tz}')=CURRENT_DATE('{tz}')
) d
WHERE L.draw_id=d.draw_id
  AND L.outcome IS NULL
"""
    _bq(sql, loc)
