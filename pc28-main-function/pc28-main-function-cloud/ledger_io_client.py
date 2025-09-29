#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json, time, datetime
from typing import Dict, Any, List
from python.bigquery_client_adapter import BQClient

def upsert_order(order: Dict[str, Any], env: Dict[str, str]):
    """插入订单到BigQuery"""
    proj, ds, loc, tz = env["PROJECT"], env["DS_LAB"], env["BQLOC"], env["TZ"]
    
    # 创建BigQuery客户端
    bq_client = BQClient(proj, ds, "draw_dataset", loc, tz)
    
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
  '{order.get("note", "")}'
)
"""
    
    success = bq_client.execute_dml(sql)
    if success:
        print(f"订单插入成功: {order['id']}")
    else:
        print(f"订单插入失败: {order['id']}")
    return success

def settle_orders(env: Dict[str, str]):
    """结算订单"""
    proj, ds, dsd, loc, tz = env["PROJECT"], env["DS_LAB"], env["DS_DRAW"], env["BQLOC"], env["TZ"]
    
    # 创建BigQuery客户端
    bq_client = BQClient(proj, ds, dsd, loc, tz)
    
    # 示例：按 draw_id 与市场胜负规则结算
    sql = f"""
UPDATE `{proj}.{ds}.score_ledger` L
SET outcome=CASE
      WHEN L.market='oe'  THEN IF(MOD(d.result_sum,2)=0,'win','lose')
      WHEN L.market='size' THEN IF(d.result_sum>=14,'win','lose')
      ELSE NULL END,
    pnl_u = CASE
      WHEN L.outcome='win'  THEN L.stake_u
      WHEN L.outcome='lose' THEN -L.stake_u
      ELSE NULL END
FROM (
  SELECT issue as draw_id, result_sum
  FROM `{proj}.{dsd}.draws_14w_dedup_v`
  WHERE DATE(timestamp,'{tz}')=CURRENT_DATE('{tz}')
) d
WHERE L.draw_id=d.draw_id
  AND L.outcome IS NULL
"""
    
    success = bq_client.execute_dml(sql)
    if success:
        print("订单结算成功")
    else:
        print("订单结算失败")
    return success