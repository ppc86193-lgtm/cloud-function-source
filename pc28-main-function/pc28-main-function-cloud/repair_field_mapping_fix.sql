-- 字段映射修复脚本
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
WHERE ts_utc IS NOT NULL;