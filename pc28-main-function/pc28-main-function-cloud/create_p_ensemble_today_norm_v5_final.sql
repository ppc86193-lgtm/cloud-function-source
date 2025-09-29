-- 创建或替换视图 p_ensemble_today_norm_v5
-- 三桶集成模型的升级版本，基于实际存在的视图结构
CREATE OR REPLACE VIEW `wprojectl.pc28_lab.p_ensemble_today_norm_v5` AS 
WITH params AS (
  SELECT MAX(DATE(ts_utc,'Asia/Shanghai')) AS day_id
  FROM `wprojectl.pc28_lab.p_cloud_clean_merged_dedup_v`
),
cloud_pred AS ( 
  SELECT 
    CAST(period AS STRING) AS period, 
    ts_utc,
    p_even AS p_cloud,
    0.8 AS conf_cloud  -- 默认置信度
  FROM `wprojectl.pc28_lab.p_cloud_today_v` 
), map_pred AS ( 
  SELECT 
    CAST(period AS STRING) AS period,
    ts_utc,
    p_even AS p_map,
    0.7 AS conf_map  -- 默认置信度
  FROM `wprojectl.pc28_lab.p_map_today_v` 
), size_pred AS ( 
  SELECT 
    CAST(period AS STRING) AS period,
    timestamp AS ts_utc,
    p_even AS p_size,
    0.6 AS conf_size  -- 默认置信度
  FROM `wprojectl.pc28_lab.p_size_today_v` 
) 
SELECT 
  c.period, 
  c.ts_utc, 
  DATETIME(c.ts_utc, 'Asia/Shanghai') AS ts_cst, 
  -- 加权平均的三桶模型预测分数（p_cloud, p_map, p_size） 
  (c.p_cloud * c.conf_cloud + m.p_map * m.conf_map + s.p_size * s.conf_size) / 
  (c.conf_cloud + m.conf_map + s.conf_size) AS p_star_ens, 
  -- 投票比例：基于三个模型的置信度加权 
  (c.conf_cloud + m.conf_map + s.conf_size) / 3.0 AS vote_ratio, 
  -- 激活状态：p_star_ens >= 0.6 激活，否则冷却 
  CASE 
    WHEN (c.p_cloud * c.conf_cloud + m.p_map * m.conf_map + s.p_size * s.conf_size) / 
         (c.conf_cloud + m.conf_map + s.conf_size) >= 0.6 THEN 'ACTIVE' 
    ELSE 'COOLING' 
  END AS cooling_status 
FROM cloud_pred c 
JOIN map_pred m ON c.period = m.period AND c.ts_utc = m.ts_utc
JOIN size_pred s ON c.period = s.period AND c.ts_utc = s.ts_utc, params
WHERE DATE(c.ts_utc,'Asia/Shanghai') = params.day_id;