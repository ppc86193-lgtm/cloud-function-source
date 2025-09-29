-- 插入测试候选数据
INSERT INTO `wprojectl.draw_dataset.candidates` (draw_id, candidate_id, p_cloud, p_model_a, p_model_b, p_model_c)
VALUES 
  ('draw_001', 'candidate_1', 0.65, 0.62, 0.68, 0.63),
  ('draw_001', 'candidate_2', 0.35, 0.38, 0.32, 0.37),
  ('draw_002', 'candidate_1', 0.72, 0.70, 0.74, 0.71),
  ('draw_002', 'candidate_2', 0.28, 0.30, 0.26, 0.29);

-- 插入测试KPI数据
INSERT INTO `wprojectl.lab_dataset.kpi_data` (date, coverage, accuracy, profit_loss, total_bets)
VALUES 
  (CURRENT_DATE(), 0.75, 0.68, 150.50, 25),
  (DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), 0.80, 0.72, 200.75, 30),
  (DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), 0.70, 0.65, 100.25, 20);

-- 插入测试抽奖结果
INSERT INTO `wprojectl.draw_dataset.draw_results` (draw_id, result)
VALUES 
  ('draw_001', 'candidate_1'),
  ('draw_002', 'candidate_2');