-- 创建或替换视图 p_ensemble_today_norm_v5
-- 五桶集成模型的升级版本，包含cloud, map, size, combo, hit五个预测模型
CREATE OR REPLACE VIEW `wprojectl.pc28.p_ensemble_today_norm_v5` AS 
WITH cloud_pred AS ( 
  SELECT 
    period, 
    ts_utc, 
    p_star AS p_cloud, 
    confidence AS conf_cloud 
  FROM `wprojectl.pc28.p_cloud_today_canon_v` 
), map_pred AS ( 
  SELECT 
    period, 
    ts_utc, 
    p_star AS p_map, 
    confidence AS conf_map 
  FROM `wprojectl.pc28.p_map_today_canon_v` 
), size_pred AS ( 
  SELECT 
    period, 
    ts_utc, 
    p_star AS p_size, 
    confidence AS conf_size 
  FROM `wprojectl.pc28.p_size_today_canon_v` 
), combo_pred AS ( 
  SELECT 
    period, 
    ts_utc, 
    p_star AS p_combo, 
    confidence AS conf_combo 
  FROM `wprojectl.pc28.p_combo_today_canon_v` 
), hit_pred AS ( 
  SELECT 
    period, 
    ts_utc, 
    p_star AS p_hit, 
    confidence AS conf_hit 
  FROM `wprojectl.pc28.p_hit_today_canon_v` 
) 
SELECT 
  c.period, 
  c.ts_utc, 
  DATETIME(c.ts_utc, 'Asia/Shanghai') AS ts_cst, 
  -- 加权平均的五桶模型预测分数（p_cloud, p_map, p_size, p_combo, p_hit） 
  (c.p_cloud * c.conf_cloud + m.p_map * m.conf_map + s.p_size * s.conf_size + 
   cb.p_combo * cb.conf_combo + h.p_hit * h.conf_hit) / 
  (c.conf_cloud + m.conf_map + s.conf_size + cb.conf_combo + h.conf_hit) AS p_star_ens, 
  -- 投票比例：基于五个模型的置信度加权 
  (c.conf_cloud + m.conf_map + s.conf_size + cb.conf_combo + h.conf_hit) / 5.0 AS vote_ratio, 
  -- 激活状态：p_star_ens >= 0.6 激活，否则冷却 
  CASE 
    WHEN (c.p_cloud * c.conf_cloud + m.p_map * m.conf_map + s.p_size * s.conf_size + 
          cb.p_combo * cb.conf_combo + h.p_hit * h.conf_hit) / 
         (c.conf_cloud + m.conf_map + s.conf_size + cb.conf_combo + h.conf_hit) >= 0.6 THEN 'ACTIVE' 
    ELSE 'COOLING' 
  END AS cooling_status 
FROM cloud_pred c 
JOIN map_pred m ON c.period = m.period 
JOIN size_pred s ON c.period = s.period 
JOIN combo_pred cb ON c.period = cb.period 
JOIN hit_pred h ON c.period = h.period 
WHERE DATE(c.ts_utc, 'Asia/Shanghai') = CURRENT_DATE('Asia/Shanghai');