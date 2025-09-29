#!/bin/bash
# PC28数据库优化迁移计划
# 生成时间: 2025-09-29T05:18:08.182038
# 预计执行时间: 9小时35分钟
# 风险评估: HIGH - 包含不可逆操作，需要谨慎执行

set -e  # 遇到错误立即退出
set -u  # 使用未定义变量时退出

# 配置变量
PROJECT_ID=${PROJECT_ID:-your_project_id}
DATASET_ID=${DATASET_ID:-your_dataset_id}
LOG_FILE="migration_$(date +%Y%m%d_%H%M%S).log"

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "开始数据库迁移: {plan.plan_name}"

# ========== 步骤 1/29: 备份 score_ledger.移除冗余字段 字段数据 ==========
log "执行步骤 step_001: 备份 score_ledger.移除冗余字段 字段数据"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 备份包含待删除字段的数据 CREATE TABLE `score_ledger_移除冗余字段_backup_20250929_051808` AS SELECT 移除冗余字段, order_id FROM `score_ledger` WHERE 移除冗余字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT COUNT(*) FROM `score_ledger_移除冗余字段_backup_20250929_051808`;"

log "步骤 step_001 完成"

# ========== 步骤 2/29: 为 score_ledger.result_sum 创建索引 ==========
log "执行步骤 step_007: 为 score_ledger.result_sum 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_score_ledger_result_sum CREATE INDEX idx_score_ledger_result_sum ON `score_ledger` (result_sum);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_result_sum';"

log "步骤 step_007 完成"

# ========== 步骤 3/29: 为 score_ledger.numbers 创建索引 ==========
log "执行步骤 step_008: 为 score_ledger.numbers 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_score_ledger_numbers CREATE INDEX idx_score_ledger_numbers ON `score_ledger` (numbers);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_numbers';"

log "步骤 step_008 完成"

# ========== 步骤 4/29: 为 score_ledger.status 创建索引 ==========
log "执行步骤 step_009: 为 score_ledger.status 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_score_ledger_status CREATE INDEX idx_score_ledger_status ON `score_ledger` (status);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_status';"

log "步骤 step_009 完成"

# ========== 步骤 5/29: 为 score_ledger.profit_loss 创建索引 ==========
log "执行步骤 step_010: 为 score_ledger.profit_loss 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_score_ledger_profit_loss CREATE INDEX idx_score_ledger_profit_loss ON `score_ledger` (profit_loss);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_profit_loss';"

log "步骤 step_010 完成"

# ========== 步骤 6/29: 为 score_ledger.outcome 创建索引 ==========
log "执行步骤 step_011: 为 score_ledger.outcome 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_score_ledger_outcome CREATE INDEX idx_score_ledger_outcome ON `score_ledger` (outcome);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_outcome';"

log "步骤 step_011 完成"

# ========== 步骤 7/29: 备份 draws_14w_dedup_v.移除冗余字段 字段数据 ==========
log "执行步骤 step_012: 备份 draws_14w_dedup_v.移除冗余字段 字段数据"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 备份包含待删除字段的数据 CREATE TABLE `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808` AS SELECT 移除冗余字段, draw_id FROM `draws_14w_dedup_v` WHERE 移除冗余字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT COUNT(*) FROM `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808`;"

log "步骤 step_012 完成"

# ========== 步骤 8/29: 备份 draws_14w_dedup_v.移除冗余字段 字段数据 ==========
log "执行步骤 step_016: 备份 draws_14w_dedup_v.移除冗余字段 字段数据"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 备份包含待删除字段的数据 CREATE TABLE `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808` AS SELECT 移除冗余字段, draw_id FROM `draws_14w_dedup_v` WHERE 移除冗余字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT COUNT(*) FROM `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808`;"

log "步骤 step_016 完成"

# ========== 步骤 9/29: 为 draws_14w_dedup_v.result_sum 创建索引 ==========
log "执行步骤 step_020: 为 draws_14w_dedup_v.result_sum 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_draws_14w_dedup_v_result_sum CREATE INDEX idx_draws_14w_dedup_v_result_sum ON `draws_14w_dedup_v` (result_sum);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'draws_14w_dedup_v' AND index_name = 'idx_draws_14w_dedup_v_result_sum';"

log "步骤 step_020 完成"

# ========== 步骤 10/29: 为 draws_14w_dedup_v.numbers 创建索引 ==========
log "执行步骤 step_021: 为 draws_14w_dedup_v.numbers 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_draws_14w_dedup_v_numbers CREATE INDEX idx_draws_14w_dedup_v_numbers ON `draws_14w_dedup_v` (numbers);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'draws_14w_dedup_v' AND index_name = 'idx_draws_14w_dedup_v_numbers';"

log "步骤 step_021 完成"

# ========== 步骤 11/29: 为 p_size_clean_merged_dedup_v.size_prediction 创建索引 ==========
log "执行步骤 step_028: 为 p_size_clean_merged_dedup_v.size_prediction 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_p_size_clean_merged_dedup_v_size_prediction CREATE INDEX idx_p_size_clean_merged_dedup_v_size_prediction ON `p_size_clean_merged_dedup_v` (size_prediction);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'p_size_clean_merged_dedup_v' AND index_name = 'idx_p_size_clean_merged_dedup_v_size_prediction';"

log "步骤 step_028 完成"

# ========== 步骤 12/29: 为 p_size_clean_merged_dedup_v.prediction_data 创建索引 ==========
log "执行步骤 step_029: 为 p_size_clean_merged_dedup_v.prediction_data 创建索引"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建索引 idx_p_size_clean_merged_dedup_v_prediction_data CREATE INDEX idx_p_size_clean_merged_dedup_v_prediction_data ON `p_size_clean_merged_dedup_v` (prediction_data);"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'p_size_clean_merged_dedup_v' AND index_name = 'idx_p_size_clean_merged_dedup_v_prediction_data';"

log "步骤 step_029 完成"

# ========== 步骤 13/29: 创建 归档低使用率字段 字段归档表 ==========
log "执行步骤 step_003: 创建 归档低使用率字段 字段归档表"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建归档表 CREATE TABLE `score_ledger_归档低使用率字段_archive` AS SELECT * FROM `score_ledger` WHERE 归档低使用率字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `score_ledger_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `score_ledger`) as remaining_count;"

log "步骤 step_003 完成"

# ========== 步骤 14/29: 创建 归档低使用率字段 字段归档表 ==========
log "执行步骤 step_005: 创建 归档低使用率字段 字段归档表"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建归档表 CREATE TABLE `score_ledger_归档低使用率字段_archive` AS SELECT * FROM `score_ledger` WHERE 归档低使用率字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `score_ledger_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `score_ledger`) as remaining_count;"

log "步骤 step_005 完成"

# ========== 步骤 15/29: 创建 归档低使用率字段 字段归档表 ==========
log "执行步骤 step_014: 创建 归档低使用率字段 字段归档表"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建归档表 CREATE TABLE `draws_14w_dedup_v_归档低使用率字段_archive` AS SELECT * FROM `draws_14w_dedup_v` WHERE 归档低使用率字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `draws_14w_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `draws_14w_dedup_v`) as remaining_count;"

log "步骤 step_014 完成"

# ========== 步骤 16/29: 创建 归档低使用率字段 字段归档表 ==========
log "执行步骤 step_018: 创建 归档低使用率字段 字段归档表"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建归档表 CREATE TABLE `draws_14w_dedup_v_归档低使用率字段_archive` AS SELECT * FROM `draws_14w_dedup_v` WHERE 归档低使用率字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `draws_14w_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `draws_14w_dedup_v`) as remaining_count;"

log "步骤 step_018 完成"

# ========== 步骤 17/29: 创建 归档低使用率字段 字段归档表 ==========
log "执行步骤 step_022: 创建 归档低使用率字段 字段归档表"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建归档表 CREATE TABLE `p_size_clean_merged_dedup_v_归档低使用率字段_archive` AS SELECT * FROM `p_size_clean_merged_dedup_v` WHERE 归档低使用率字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v`) as remaining_count;"

log "步骤 step_022 完成"

# ========== 步骤 18/29: 创建 归档低使用率字段 字段归档表 ==========
log "执行步骤 step_024: 创建 归档低使用率字段 字段归档表"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建归档表 CREATE TABLE `p_size_clean_merged_dedup_v_归档低使用率字段_archive` AS SELECT * FROM `p_size_clean_merged_dedup_v` WHERE 归档低使用率字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v`) as remaining_count;"

log "步骤 step_024 完成"

# ========== 步骤 19/29: 创建 归档低使用率字段 字段归档表 ==========
log "执行步骤 step_026: 创建 归档低使用率字段 字段归档表"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 创建归档表 CREATE TABLE `p_size_clean_merged_dedup_v_归档低使用率字段_archive` AS SELECT * FROM `p_size_clean_merged_dedup_v` WHERE 归档低使用率字段 IS NOT NULL;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v`) as remaining_count;"

log "步骤 step_026 完成"

# ========== 步骤 20/29: 删除 score_ledger.移除冗余字段 字段 ==========
log "执行步骤 step_002: 删除 score_ledger.移除冗余字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 删除字段 移除冗余字段 ALTER TABLE `score_ledger` DROP COLUMN 移除冗余字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证字段删除 SELECT column_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'score_ledger' AND column_name = '移除冗余字段';"

log "步骤 step_002 完成"

# ========== 步骤 21/29: 删除已归档的 归档低使用率字段 字段 ==========
log "执行步骤 step_004: 删除已归档的 归档低使用率字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "ALTER TABLE `score_ledger` DROP COLUMN 归档低使用率字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'score_ledger' AND column_name = '归档低使用率字段';"

log "步骤 step_004 完成"

# ========== 步骤 22/29: 删除已归档的 归档低使用率字段 字段 ==========
log "执行步骤 step_006: 删除已归档的 归档低使用率字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "ALTER TABLE `score_ledger` DROP COLUMN 归档低使用率字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'score_ledger' AND column_name = '归档低使用率字段';"

log "步骤 step_006 完成"

# ========== 步骤 23/29: 删除 draws_14w_dedup_v.移除冗余字段 字段 ==========
log "执行步骤 step_013: 删除 draws_14w_dedup_v.移除冗余字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 删除字段 移除冗余字段 ALTER TABLE `draws_14w_dedup_v` DROP COLUMN 移除冗余字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证字段删除 SELECT column_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '移除冗余字段';"

log "步骤 step_013 完成"

# ========== 步骤 24/29: 删除已归档的 归档低使用率字段 字段 ==========
log "执行步骤 step_015: 删除已归档的 归档低使用率字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "ALTER TABLE `draws_14w_dedup_v` DROP COLUMN 归档低使用率字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '归档低使用率字段';"

log "步骤 step_015 完成"

# ========== 步骤 25/29: 删除 draws_14w_dedup_v.移除冗余字段 字段 ==========
log "执行步骤 step_017: 删除 draws_14w_dedup_v.移除冗余字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 删除字段 移除冗余字段 ALTER TABLE `draws_14w_dedup_v` DROP COLUMN 移除冗余字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "-- 验证字段删除 SELECT column_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '移除冗余字段';"

log "步骤 step_017 完成"

# ========== 步骤 26/29: 删除已归档的 归档低使用率字段 字段 ==========
log "执行步骤 step_019: 删除已归档的 归档低使用率字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "ALTER TABLE `draws_14w_dedup_v` DROP COLUMN 归档低使用率字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '归档低使用率字段';"

log "步骤 step_019 完成"

# ========== 步骤 27/29: 删除已归档的 归档低使用率字段 字段 ==========
log "执行步骤 step_023: 删除已归档的 归档低使用率字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "ALTER TABLE `p_size_clean_merged_dedup_v` DROP COLUMN 归档低使用率字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'p_size_clean_merged_dedup_v' AND column_name = '归档低使用率字段';"

log "步骤 step_023 完成"

# ========== 步骤 28/29: 删除已归档的 归档低使用率字段 字段 ==========
log "执行步骤 step_025: 删除已归档的 归档低使用率字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "ALTER TABLE `p_size_clean_merged_dedup_v` DROP COLUMN 归档低使用率字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'p_size_clean_merged_dedup_v' AND column_name = '归档低使用率字段';"

log "步骤 step_025 完成"

# ========== 步骤 29/29: 删除已归档的 归档低使用率字段 字段 ==========
log "执行步骤 step_027: 删除已归档的 归档低使用率字段 字段"

bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "ALTER TABLE `p_size_clean_merged_dedup_v` DROP COLUMN 归档低使用率字段;"

# 验证步骤执行结果
bq query --use_legacy_sql=false \
  --project_id="$PROJECT_ID" \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'p_size_clean_merged_dedup_v' AND column_name = '归档低使用率字段';"

log "步骤 step_027 完成"

log "数据库迁移完成！"
log "请执行 validate_migration.sh 验证迁移结果"