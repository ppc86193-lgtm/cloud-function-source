-- PC28数据库优化SQL
-- 1. 添加关键索引
CREATE INDEX IF NOT EXISTS idx_betting_user_period ON betting_records(user_id, period);
CREATE INDEX IF NOT EXISTS idx_draw_results_period ON draw_results(period);
CREATE INDEX IF NOT EXISTS idx_user_balance_updated ON user_balance(updated_at);

-- 2. 优化查询性能
-- 分区表建议（按期次分区）
-- ALTER TABLE betting_records PARTITION BY RANGE (period);

-- 3. 清理冗余数据
-- DELETE FROM log_table WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- 4. 更新表统计信息
-- ANALYZE TABLE betting_records, draw_results, user_balance;
