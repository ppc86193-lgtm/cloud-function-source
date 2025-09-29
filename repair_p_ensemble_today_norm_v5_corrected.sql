-- 修正后的 p_ensemble_today_norm_v5 视图 (字段统一版本)
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
ORDER BY c.period DESC;