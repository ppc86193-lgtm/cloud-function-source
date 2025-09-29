CREATE OR REPLACE VIEW `wprojectl.pc28_lab.draws_today_v2` AS
SELECT
  CAST(issue AS STRING) AS period,
  timestamp
FROM `wprojectl.pc28.draws_14w_dedup_v`
WHERE DATE(timestamp,'Asia/Shanghai') = CURRENT_DATE('Asia/Shanghai');
