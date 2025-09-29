-- 创建核心数据表在 Supabase 中
-- 确保与本地数据库表结构匹配

-- 1. lab_push_candidates_v2 表
CREATE TABLE IF NOT EXISTS lab_push_candidates_v2 (
    draw_id TEXT PRIMARY KEY,
    issue TEXT NOT NULL,
    numbers TEXT,
    sum_value INTEGER,
    big_small TEXT,
    odd_even TEXT,
    dragon_tiger TEXT,
    prediction_score REAL,
    confidence_level REAL,
    algorithm_version TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. cloud_pred_today_norm 表
CREATE TABLE IF NOT EXISTS cloud_pred_today_norm (
    draw_id TEXT PRIMARY KEY,
    issue TEXT NOT NULL,
    predicted_numbers TEXT,
    prediction_type TEXT,
    confidence_score REAL,
    normalization_factor REAL,
    model_version TEXT,
    prediction_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. signal_pool_union_v3 表
CREATE TABLE IF NOT EXISTS signal_pool_union_v3 (
    signal_id TEXT PRIMARY KEY,
    signal_type TEXT NOT NULL,
    signal_strength REAL,
    pattern_match TEXT,
    frequency_score REAL,
    reliability_index REAL,
    source_algorithm TEXT,
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. p_size_clean_merged_dedup_v 表
CREATE TABLE IF NOT EXISTS p_size_clean_merged_dedup_v (
    size_category TEXT PRIMARY KEY,
    category_name TEXT NOT NULL,
    size_range TEXT,
    frequency_count INTEGER,
    probability_score REAL,
    trend_indicator TEXT,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. draws_14w_dedup_v 表
CREATE TABLE IF NOT EXISTS draws_14w_dedup_v (
    draw_id TEXT PRIMARY KEY,
    issue TEXT NOT NULL,
    numbers TEXT NOT NULL,
    sum_value INTEGER,
    big_small TEXT,
    odd_even TEXT,
    dragon_tiger TEXT,
    week_number INTEGER,
    day_of_week INTEGER,
    is_duplicate BOOLEAN DEFAULT FALSE,
    dedup_hash TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 6. score_ledger 表
CREATE TABLE IF NOT EXISTS score_ledger (
    draw_id TEXT PRIMARY KEY,
    issue TEXT NOT NULL,
    actual_numbers TEXT,
    predicted_numbers TEXT,
    accuracy_score REAL,
    prediction_method TEXT,
    evaluation_date TIMESTAMPTZ DEFAULT NOW(),
    performance_metrics TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_lab_push_created_at ON lab_push_candidates_v2(created_at);
CREATE INDEX IF NOT EXISTS idx_cloud_pred_created_at ON cloud_pred_today_norm(created_at);
CREATE INDEX IF NOT EXISTS idx_signal_pool_last_seen ON signal_pool_union_v3(last_seen);
CREATE INDEX IF NOT EXISTS idx_p_size_last_updated ON p_size_clean_merged_dedup_v(last_updated);
CREATE INDEX IF NOT EXISTS idx_draws_14w_created_at ON draws_14w_dedup_v(created_at);
CREATE INDEX IF NOT EXISTS idx_score_ledger_evaluation_date ON score_ledger(evaluation_date);

-- 启用行级安全性 (RLS)
ALTER TABLE lab_push_candidates_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE cloud_pred_today_norm ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_pool_union_v3 ENABLE ROW LEVEL SECURITY;
ALTER TABLE p_size_clean_merged_dedup_v ENABLE ROW LEVEL SECURITY;
ALTER TABLE draws_14w_dedup_v ENABLE ROW LEVEL SECURITY;
ALTER TABLE score_ledger ENABLE ROW LEVEL SECURITY;

-- 创建基本的 RLS 策略（允许服务角色访问）
CREATE POLICY "Enable all access for service role" ON lab_push_candidates_v2
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all access for service role" ON cloud_pred_today_norm
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all access for service role" ON signal_pool_union_v3
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all access for service role" ON p_size_clean_merged_dedup_v
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all access for service role" ON draws_14w_dedup_v
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all access for service role" ON score_ledger
    FOR ALL USING (auth.role() = 'service_role');