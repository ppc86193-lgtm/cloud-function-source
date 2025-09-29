-- PC28 系统初始数据库 Schema
-- 创建核心数据表，支持从 BigQuery/SQLite 同步

-- 启用必要的扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- 创建 lab_push_candidates_v2 表
CREATE TABLE IF NOT EXISTS lab_push_candidates_v2 (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    draw_id VARCHAR(50) NOT NULL,
    market VARCHAR(20) NOT NULL,
    p_cloud DECIMAL(10, 8),
    p_map DECIMAL(10, 8),
    p_size DECIMAL(10, 8),
    session VARCHAR(20),
    tail INTEGER,
    p_even DECIMAL(10, 8),
    day_id_cst DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_source VARCHAR(20) DEFAULT 'bigquery',
    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建 cloud_pred_today_norm 表
CREATE TABLE IF NOT EXISTS cloud_pred_today_norm (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    draw_id VARCHAR(50) NOT NULL,
    prediction_value DECIMAL(10, 8),
    normalized_value DECIMAL(10, 8),
    confidence_score DECIMAL(5, 4),
    model_version VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_source VARCHAR(20) DEFAULT 'bigquery',
    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建 signal_pool_union_v3 表
CREATE TABLE IF NOT EXISTS signal_pool_union_v3 (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    signal_id VARCHAR(50) NOT NULL,
    signal_type VARCHAR(30),
    signal_value DECIMAL(12, 6),
    strength DECIMAL(5, 4),
    frequency INTEGER,
    last_seen TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_source VARCHAR(20) DEFAULT 'bigquery',
    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建 p_size_clean_merged_dedup_v 表
CREATE TABLE IF NOT EXISTS p_size_clean_merged_dedup_v (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    size_category VARCHAR(20),
    probability DECIMAL(10, 8),
    sample_count INTEGER,
    variance DECIMAL(10, 8),
    last_updated TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_source VARCHAR(20) DEFAULT 'bigquery',
    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建 draws_14w_dedup_v 表
CREATE TABLE IF NOT EXISTS draws_14w_dedup_v (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    draw_id VARCHAR(50) NOT NULL UNIQUE,
    draw_date DATE,
    draw_time TIME,
    result_numbers INTEGER[],
    week_number INTEGER,
    is_duplicate BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_source VARCHAR(20) DEFAULT 'bigquery',
    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建 score_ledger 表
CREATE TABLE IF NOT EXISTS score_ledger (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    draw_id VARCHAR(50) NOT NULL,
    model_name VARCHAR(50),
    score DECIMAL(8, 6),
    rank INTEGER,
    percentile DECIMAL(5, 4),
    evaluation_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_source VARCHAR(20) DEFAULT 'bigquery',
    sync_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建同步状态跟踪表
CREATE TABLE IF NOT EXISTS sync_status (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    last_sync_timestamp TIMESTAMP WITH TIME ZONE,
    sync_mode VARCHAR(20), -- 'incremental' or 'full'
    records_synced INTEGER DEFAULT 0,
    sync_duration_seconds DECIMAL(8, 2),
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'running', 'completed', 'failed'
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建审计日志表
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    audit_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(100),
    operation VARCHAR(20), -- 'INSERT', 'UPDATE', 'DELETE', 'SYNC'
    record_count INTEGER,
    details JSONB,
    status VARCHAR(20) DEFAULT 'completed',
    created_by VARCHAR(100) DEFAULT 'system',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_lab_push_candidates_draw_id ON lab_push_candidates_v2(draw_id);
CREATE INDEX IF NOT EXISTS idx_lab_push_candidates_created_at ON lab_push_candidates_v2(created_at);
CREATE INDEX IF NOT EXISTS idx_cloud_pred_draw_id ON cloud_pred_today_norm(draw_id);
CREATE INDEX IF NOT EXISTS idx_signal_pool_signal_id ON signal_pool_union_v3(signal_id);
CREATE INDEX IF NOT EXISTS idx_draws_draw_id ON draws_14w_dedup_v(draw_id);
CREATE INDEX IF NOT EXISTS idx_draws_draw_date ON draws_14w_dedup_v(draw_date);
CREATE INDEX IF NOT EXISTS idx_score_ledger_draw_id ON score_ledger(draw_id);
CREATE INDEX IF NOT EXISTS idx_sync_status_table_name ON sync_status(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at);

-- 创建更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为所有表添加更新时间触发器
CREATE TRIGGER update_lab_push_candidates_updated_at BEFORE UPDATE ON lab_push_candidates_v2 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_cloud_pred_updated_at BEFORE UPDATE ON cloud_pred_today_norm FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_signal_pool_updated_at BEFORE UPDATE ON signal_pool_union_v3 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_p_size_updated_at BEFORE UPDATE ON p_size_clean_merged_dedup_v FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_draws_updated_at BEFORE UPDATE ON draws_14w_dedup_v FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_score_ledger_updated_at BEFORE UPDATE ON score_ledger FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_sync_status_updated_at BEFORE UPDATE ON sync_status FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 启用行级安全 (RLS)
ALTER TABLE lab_push_candidates_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE cloud_pred_today_norm ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_pool_union_v3 ENABLE ROW LEVEL SECURITY;
ALTER TABLE p_size_clean_merged_dedup_v ENABLE ROW LEVEL SECURITY;
ALTER TABLE draws_14w_dedup_v ENABLE ROW LEVEL SECURITY;
ALTER TABLE score_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;

-- 创建基本的 RLS 策略（允许所有认证用户访问）
CREATE POLICY "Allow authenticated users to read all data" ON lab_push_candidates_v2 FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated users to insert data" ON lab_push_candidates_v2 FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "Allow authenticated users to update data" ON lab_push_candidates_v2 FOR UPDATE TO authenticated USING (true);

CREATE POLICY "Allow authenticated users to read cloud pred" ON cloud_pred_today_norm FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated users to insert cloud pred" ON cloud_pred_today_norm FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "Allow authenticated users to update cloud pred" ON cloud_pred_today_norm FOR UPDATE TO authenticated USING (true);

CREATE POLICY "Allow authenticated users to read signals" ON signal_pool_union_v3 FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated users to insert signals" ON signal_pool_union_v3 FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "Allow authenticated users to update signals" ON signal_pool_union_v3 FOR UPDATE TO authenticated USING (true);

CREATE POLICY "Allow authenticated users to read p_size" ON p_size_clean_merged_dedup_v FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated users to insert p_size" ON p_size_clean_merged_dedup_v FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "Allow authenticated users to update p_size" ON p_size_clean_merged_dedup_v FOR UPDATE TO authenticated USING (true);

CREATE POLICY "Allow authenticated users to read draws" ON draws_14w_dedup_v FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated users to insert draws" ON draws_14w_dedup_v FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "Allow authenticated users to update draws" ON draws_14w_dedup_v FOR UPDATE TO authenticated USING (true);

CREATE POLICY "Allow authenticated users to read scores" ON score_ledger FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated users to insert scores" ON score_ledger FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "Allow authenticated users to update scores" ON score_ledger FOR UPDATE TO authenticated USING (true);

CREATE POLICY "Allow authenticated users to read sync status" ON sync_status FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated users to manage sync status" ON sync_status FOR ALL TO authenticated USING (true);

CREATE POLICY "Allow authenticated users to read audit logs" ON audit_logs FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow system to insert audit logs" ON audit_logs FOR INSERT TO authenticated WITH CHECK (true);

-- 授予必要的权限
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO anon;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO authenticated;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO anon;