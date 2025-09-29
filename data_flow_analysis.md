# PC28系统数据流程分析

## 数据流向图

```
API数据采集 → BigQuery原始表 → 视图层 → signal_pool → 决策引擎 → 下单执行
     ↓              ↓           ↓          ↓          ↓          ↓
  实时数据      cloud_pred_    预测视图    信号池     投票决策    订单记录
  历史数据      today_norm     p_*_v      union      校准概率    结算系统
```

## 核心数据表和视图依赖关系

### 1. 原始数据层
- `wprojectl.pc28.draws_14w_dedup_v` - 开奖历史数据（14周去重）
- `wprojectl.pc28_lab.cloud_pred_today_norm` - 云端预测数据（383行）

### 2. 预测视图层
- `p_cloud_today_v` - 云端预测视图（基于cloud_pred_today_norm）
- `p_map_today_v` - 地图预测视图
- `p_size_today_v` - 大小预测视图（基于p_cloud_today_v + adaptive_weights）

### 3. 标准化视图层
- `p_map_today_canon_v` - 标准化地图预测
- `p_size_today_canon_v` - 标准化大小预测（基于p_size_today_v）

### 4. 信号池层
- `signal_pool_union_v3` - 信号池联合视图（UNION p_map_today_canon_v + p_size_today_canon_v）
- `signal_pool` - 最终信号池表（当前0行）

### 5. 决策层
- `lab_push_candidates_v2` - 候选决策视图（基于signal_pool_union_v3）
- `runtime_params` - 运行时参数表

## 数据流问题分析

### 当前状态
1. ✅ `cloud_pred_today_norm` 有数据（383行）
2. ✅ `p_cloud_today_v` 视图正常
3. ✅ `p_size_today_v` 视图已修复
4. ✅ `p_size_today_canon_v` 视图已修复
5. ❌ `signal_pool_union_v3` 返回0行
6. ❌ `signal_pool` 表为空

### 问题根因
1. **p_map_today_canon_v** 可能没有数据或有问题
2. **p_size_today_canon_v** 虽然修复了，但可能过滤条件太严格
3. **数据时间范围** 可能不匹配当前日期

### 数据依赖链
```
cloud_pred_today_norm (383行)
    ↓
p_cloud_today_v (需要验证)
    ↓
p_size_today_v (已修复)
    ↓
p_size_today_canon_v (已修复，但可能无数据)
    ↓
signal_pool_union_v3 (0行) ← 问题点
    ↓
signal_pool (0行)
```

## 下一步行动计划
1. 检查p_map_today_canon_v视图状态和数据
2. 验证p_cloud_today_v和p_size_today_v的数据输出
3. 调试signal_pool_union_v3为什么返回0行
4. 建立数据质量监控和自动修复机制