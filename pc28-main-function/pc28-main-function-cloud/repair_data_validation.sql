-- 数据验证脚本
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
FROM `wprojectl.pc28_lab.p_map_today_canon_v`;