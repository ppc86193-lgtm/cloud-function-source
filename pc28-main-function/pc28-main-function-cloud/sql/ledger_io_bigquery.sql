-- 账本表（生产口径）
CREATE TABLE IF NOT EXISTS \`\${PROJECT}.\${DS_LAB}.score_ledger\` (
  id STRING,
  day_id_cst DATE,
  market STRING,           -- 'oe' or 'size'
  draw_id INT64,           -- 期号
  created_at TIMESTAMP,
  p_win FLOAT64,
  ev FLOAT64,
  kelly_frac FLOAT64,
  stake_u INT64,
  outcome STRING,          -- 'win'/'lose'/NULL
  pnl_u FLOAT64,
  tag STRING,              -- 'prod' 或 'test'
  note STRING
) PARTITION BY day_id_cst;

CREATE OR REPLACE VIEW \`\${PROJECT}.\${DS_LAB}.score_ledger_prod_v\` AS
SELECT * FROM \`\${PROJECT}.\${DS_LAB}.score_ledger\`
WHERE (tag IS NULL OR tag='prod');

-- 当日KPI视图（示例，用于快查）
CREATE OR REPLACE VIEW \`\${PROJECT}.\${DS_LAB}.kpi_today_v\` AS
WITH L AS (
  SELECT * FROM \`\${PROJECT}.\${DS_LAB}.score_ledger_prod_v\`
  WHERE day_id_cst = CURRENT_DATE('\${TZ}')
),
D AS (
  SELECT COUNT(*) AS n_draws
  FROM \`\${PROJECT}.\${DS_DRAW}.draws_14w_dedup_v\`
  WHERE DATE(timestamp,'\${TZ}')=CURRENT_DATE('\${TZ}')
),
S AS (
  SELECT market,
         COUNT(*) AS n_orders,
         COUNTIF(outcome IN ('win','lose')) AS n_settled,
         SAFE_DIVIDE(COUNTIF(outcome='win'), NULLIF(COUNTIF(outcome IN ('win','lose')),0)) AS acc,
         SAFE_DIVIDE(COUNT(*),(SELECT n_draws FROM D)) AS cov,
         AVG(p_win) AS pbar,
         AVG( (IF(outcome='win',1,0)-p_win)*(IF(outcome='win',1,0)-p_win) ) AS brier
  FROM L GROUP BY market
)
SELECT * FROM S;
