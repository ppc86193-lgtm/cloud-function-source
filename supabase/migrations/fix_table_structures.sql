-- 修复表结构不匹配问题
-- 1. 删除signal_pool_union_v3表中的created_at列（Supabase中不存在）
-- 2. 创建runtime_params表

-- 删除signal_pool_union_v3表中的created_at列
ALTER TABLE signal_pool_union_v3 DROP COLUMN IF EXISTS created_at;

-- 创建runtime_params表
CREATE TABLE IF NOT EXISTS runtime_params (
    param_id VARCHAR PRIMARY KEY,
    param_name VARCHAR,
    param_value TEXT,
    param_type VARCHAR,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    sync_source VARCHAR DEFAULT 'sqlite',
    sync_timestamp TIMESTAMPTZ DEFAULT now()
);

-- 为runtime_params表启用RLS
ALTER TABLE runtime_params ENABLE ROW LEVEL SECURITY;

-- 创建runtime_params表的索引
CREATE INDEX IF NOT EXISTS idx_runtime_params_param_name ON runtime_params(param_name);
CREATE INDEX IF NOT EXISTS idx_runtime_params_is_active ON runtime_params(is_active);
CREATE INDEX IF NOT EXISTS idx_runtime_params_sync_timestamp ON runtime_params(sync_timestamp);

-- 添加注释
COMMENT ON TABLE runtime_params IS '运行时参数配置表';
COMMENT ON COLUMN runtime_params.param_id IS '参数唯一标识';
COMMENT ON COLUMN runtime_params.param_name IS '参数名称';
COMMENT ON COLUMN runtime_params.param_value IS '参数值';
COMMENT ON COLUMN runtime_params.param_type IS '参数类型';
COMMENT ON COLUMN runtime_params.description IS '参数描述';
COMMENT ON COLUMN runtime_params.is_active IS '是否激活';
COMMENT ON COLUMN runtime_params.sync_source IS '同步来源';
COMMENT ON COLUMN runtime_params.sync_timestamp IS '同步时间戳';