-- 清除测试数据
DELETE FROM `wprojectl.draw_dataset.candidates` WHERE draw_id IN ('draw_001', 'draw_002');
DELETE FROM `wprojectl.lab_dataset.kpi_data` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY);
DELETE FROM `wprojectl.draw_dataset.draw_results` WHERE draw_id IN ('draw_001', 'draw_002');

-- 验证表已清空
SELECT COUNT(*) as candidates_count FROM `wprojectl.draw_dataset.candidates`;
SELECT COUNT(*) as kpi_count FROM `wprojectl.lab_dataset.kpi_data`;
SELECT COUNT(*) as results_count FROM `wprojectl.draw_dataset.draw_results`;
SELECT COUNT(*) as ledger_count FROM `wprojectl.lab_dataset.score_ledger`;