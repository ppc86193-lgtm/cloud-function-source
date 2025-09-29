-- 修复signal_pool_union_v3表结构以匹配本地数据库
-- 删除现有表并重新创建以确保结构完全匹配

DROP TABLE IF EXISTS public.signal_pool_union_v3;

CREATE TABLE public.signal_pool_union_v3 (
    id TEXT PRIMARY KEY,
    draw_id TEXT,
    ts_utc TEXT,
    period INTEGER,
    market TEXT,
    pick TEXT,
    p_win REAL,
    source TEXT,
    vote_ratio REAL,
    pick_zh TEXT,
    day_id_cst TEXT,
    created_at TEXT
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_signal_pool_union_v3_draw_id ON public.signal_pool_union_v3(draw_id);
CREATE INDEX IF NOT EXISTS idx_signal_pool_union_v3_period ON public.signal_pool_union_v3(period);
CREATE INDEX IF NOT EXISTS idx_signal_pool_union_v3_market ON public.signal_pool_union_v3(market);
CREATE INDEX IF NOT EXISTS idx_signal_pool_union_v3_day_id_cst ON public.signal_pool_union_v3(day_id_cst);
CREATE INDEX IF NOT EXISTS idx_signal_pool_union_v3_created_at ON public.signal_pool_union_v3(created_at);

-- 启用RLS
ALTER TABLE public.signal_pool_union_v3 ENABLE ROW LEVEL SECURITY;

-- 添加表注释
COMMENT ON TABLE public.signal_pool_union_v3 IS '信号池联合表v3 - 包含预测信号和投票数据';
COMMENT ON COLUMN public.signal_pool_union_v3.id IS '唯一标识符';
COMMENT ON COLUMN public.signal_pool_union_v3.draw_id IS '开奖ID';
COMMENT ON COLUMN public.signal_pool_union_v3.ts_utc IS 'UTC时间戳';
COMMENT ON COLUMN public.signal_pool_union_v3.period IS '期数';
COMMENT ON COLUMN public.signal_pool_union_v3.market IS '市场类型';
COMMENT ON COLUMN public.signal_pool_union_v3.pick IS '预测选择';
COMMENT ON COLUMN public.signal_pool_union_v3.p_win IS '获胜概率';
COMMENT ON COLUMN public.signal_pool_union_v3.source IS '数据源';
COMMENT ON COLUMN public.signal_pool_union_v3.vote_ratio IS '投票比例';
COMMENT ON COLUMN public.signal_pool_union_v3.pick_zh IS '中文预测选择';
COMMENT ON COLUMN public.signal_pool_union_v3.day_id_cst IS 'CST日期ID';
COMMENT ON COLUMN public.signal_pool_union_v3.created_at IS '创建时间';
COMMENT ON COLUMN public.signal_pool_union_v3.created_at IS '创建时间';