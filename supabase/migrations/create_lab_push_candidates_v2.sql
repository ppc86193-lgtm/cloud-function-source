-- 创建lab_push_candidates_v2视图
-- 用于支持正EV候选数据读取功能

CREATE OR REPLACE VIEW `wprojectl.lab_dataset.lab_push_candidates_v2` AS
SELECT 
    'test_draw_001' as draw_id,
    'oe' as market,
    0.65 as p_cloud,
    0.62 as p_map,
    0.68 as p_size,
    'session_1' as session,
    'tail_1' as tail,
    0.55 as p_even,
    DATE('2024-12-19') as day_id_cst,
    CURRENT_TIMESTAMP() as created_at
UNION ALL
SELECT 
    'test_draw_002' as draw_id,
    'size' as market,
    0.72 as p_cloud,
    0.70 as p_map,
    0.74 as p_size,
    'session_2' as session,
    'tail_2' as tail,
    0.48 as p_even,
    DATE('2024-12-19') as day_id_cst,
    CURRENT_TIMESTAMP() as created_at;

-- 视图创建完成