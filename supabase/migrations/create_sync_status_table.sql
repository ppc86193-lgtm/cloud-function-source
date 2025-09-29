-- 创建同步状态表
CREATE TABLE IF NOT EXISTS sync_status (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    records_synced INTEGER DEFAULT 0,
    error_message TEXT,
    sync_duration DECIMAL(10,3) DEFAULT 0.0,
    sync_mode VARCHAR(50) DEFAULT 'incremental',
    last_sync_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_sync_status_table_name ON sync_status(table_name);
CREATE INDEX IF NOT EXISTS idx_sync_status_created_at ON sync_status(created_at);

-- 创建核心数据表（如果不存在）
CREATE TABLE IF NOT EXISTS lab_push_candidates_v2 (
    draw_id VARCHAR(255) PRIMARY KEY,
    period VARCHAR(255),
    numbers VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sync_source VARCHAR(50) DEFAULT 'sqlite',
    sync_timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cloud_pred_today_norm (
    draw_id VARCHAR(255) PRIMARY KEY,
    period VARCHAR(255),
    prediction_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sync_source VARCHAR(50) DEFAULT 'sqlite',
    sync_timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_pool_union_v3 (
    signal_id VARCHAR(255) PRIMARY KEY,
    signal_data JSONB,
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    sync_source VARCHAR(50) DEFAULT 'sqlite',
    sync_timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS p_size_clean_merged_dedup_v (
    size_category VARCHAR(255) PRIMARY KEY,
    size_data JSONB,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    sync_source VARCHAR(50) DEFAULT 'sqlite',
    sync_timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS draws_14w_dedup_v (
    draw_id VARCHAR(255) PRIMARY KEY,
    period VARCHAR(255),
    numbers VARCHAR(255),
    draw_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sync_source VARCHAR(50) DEFAULT 'sqlite',
    sync_timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS score_ledger (
    draw_id VARCHAR(255) PRIMARY KEY,
    period VARCHAR(255),
    score_data JSONB,
    evaluation_date TIMESTAMPTZ DEFAULT NOW(),
    sync_source VARCHAR(50) DEFAULT 'sqlite',
    sync_timestamp TIMESTAMPTZ DEFAULT NOW()
);