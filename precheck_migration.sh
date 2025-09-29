#!/bin/bash
# PC28数据库优化迁移计划 - 预检查脚本

echo "执行迁移前预检查..."

# 检查必要的环境变量
if [ -z "${PROJECT_ID:-}" ]; then
    echo "错误: PROJECT_ID 环境变量未设置"
    exit 1
fi

# 检查BigQuery连接
echo "检查BigQuery连接..."
bq ls --project_id="$PROJECT_ID" > /dev/null
echo "BigQuery连接正常"

# 检查表存在性
echo "检查目标表存在性..."
bq show "$PROJECT_ID:p_size_clean_merged_dedup_v" > /dev/null || echo "警告: 表 p_size_clean_merged_dedup_v 不存在"
bq show "$PROJECT_ID:draws_14w_dedup_v" > /dev/null || echo "警告: 表 draws_14w_dedup_v 不存在"
bq show "$PROJECT_ID:score_ledger" > /dev/null || echo "警告: 表 score_ledger 不存在"

echo "预检查完成"