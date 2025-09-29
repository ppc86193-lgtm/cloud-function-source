-- 最终修复所有表结构不匹配问题

-- 1. 重新创建cloud_pred_today_norm表以匹配本地结构
DROP TABLE IF EXISTS public.cloud_pred_today_norm;

CREATE TABLE public.cloud_pred_today_norm (
    id SERIAL PRIMARY KEY,
    draw_id TEXT NOT NULL,
    timestamp TEXT,
    period TEXT,
    market TEXT,
    pick TEXT,
    p_win REAL,
    source TEXT,
    created_at TEXT,
    data_date TEXT
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_cloud_pred_today_norm_draw_id ON public.cloud_pred_today_norm(draw_id);
CREATE INDEX IF NOT EXISTS idx_cloud_pred_today_norm_period ON public.cloud_pred_today_norm(period);
CREATE INDEX IF NOT EXISTS idx_cloud_pred_today_norm_market ON public.cloud_pred_today_norm(market);
CREATE INDEX IF NOT EXISTS idx_cloud_pred_today_norm_data_date ON public.cloud_pred_today_norm(data_date);

-- 启用RLS
ALTER TABLE public.cloud_pred_today_norm ENABLE ROW LEVEL SECURITY;

-- 添加表注释
COMMENT ON TABLE public.cloud_pred_today_norm IS '云端预测今日标准化数据表';
COMMENT ON COLUMN public.cloud_pred_today_norm.id IS '自增主键';
COMMENT ON COLUMN public.cloud_pred_today_norm.draw_id IS '开奖ID';
COMMENT ON COLUMN public.cloud_pred_today_norm.timestamp IS '时间戳';
COMMENT ON COLUMN public.cloud_pred_today_norm.period IS '期数';
COMMENT ON COLUMN public.cloud_pred_today_norm.market IS '市场类型';
COMMENT ON COLUMN public.cloud_pred_today_norm.pick IS '预测选择';
COMMENT ON COLUMN public.cloud_pred_today_norm.p_win IS '获胜概率';
COMMENT ON COLUMN public.cloud_pred_today_norm.source IS '数据源';
COMMENT ON COLUMN public.cloud_pred_today_norm.created_at IS '创建时间';
COMMENT ON COLUMN public.cloud_pred_today_norm.data_date IS '数据日期';

-- 2. 确保signal_pool_union_v3表结构正确（已在之前的迁移中修复）
-- 验证表存在且结构正确
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'signal_pool_union_v3') THEN
        RAISE EXCEPTION 'signal_pool_union_v3 table does not exist';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'signal_pool_union_v3' AND column_name = 'id') THEN
        RAISE EXCEPTION 'signal_pool_union_v3 table missing id column';
    END IF;
END $$;