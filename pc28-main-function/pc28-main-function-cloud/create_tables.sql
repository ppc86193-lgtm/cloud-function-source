-- 创建score_ledger表
CREATE TABLE `wprojectl.lab_dataset.score_ledger` (
  order_id STRING NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  draw_id STRING,
  prediction STRING,
  probability FLOAT64,
  stake_units FLOAT64,
  outcome STRING,
  profit_loss FLOAT64,
  status STRING DEFAULT 'pending'
);

-- 创建candidates表
CREATE TABLE `wprojectl.draw_dataset.candidates` (
  draw_id STRING NOT NULL,
  candidate_id STRING NOT NULL,
  p_cloud FLOAT64,
  p_model_a FLOAT64,
  p_model_b FLOAT64,
  p_model_c FLOAT64,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 创建kpi_data表
CREATE TABLE `wprojectl.lab_dataset.kpi_data` (
  date DATE,
  coverage FLOAT64,
  accuracy FLOAT64,
  profit_loss FLOAT64,
  total_bets INT64,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 创建draw_results表
CREATE TABLE `wprojectl.draw_dataset.draw_results` (
  draw_id STRING NOT NULL,
  result STRING,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);