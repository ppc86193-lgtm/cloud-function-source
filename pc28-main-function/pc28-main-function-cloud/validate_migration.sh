#!/bin/bash
# PC28数据库优化迁移计划 - 验证脚本

echo "开始验证迁移结果..."

echo "验证: 备份 score_ledger.移除冗余字段 字段数据"
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM `score_ledger_移除冗余字段_backup_20250929_051808`;"

echo "验证: 为 score_ledger.result_sum 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_result_sum';"

echo "验证: 为 score_ledger.numbers 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_numbers';"

echo "验证: 为 score_ledger.status 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_status';"

echo "验证: 为 score_ledger.profit_loss 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_profit_loss';"

echo "验证: 为 score_ledger.outcome 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'score_ledger' AND index_name = 'idx_score_ledger_outcome';"

echo "验证: 备份 draws_14w_dedup_v.移除冗余字段 字段数据"
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808`;"

echo "验证: 备份 draws_14w_dedup_v.移除冗余字段 字段数据"
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808`;"

echo "验证: 为 draws_14w_dedup_v.result_sum 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'draws_14w_dedup_v' AND index_name = 'idx_draws_14w_dedup_v_result_sum';"

echo "验证: 为 draws_14w_dedup_v.numbers 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'draws_14w_dedup_v' AND index_name = 'idx_draws_14w_dedup_v_numbers';"

echo "验证: 为 p_size_clean_merged_dedup_v.size_prediction 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'p_size_clean_merged_dedup_v' AND index_name = 'idx_p_size_clean_merged_dedup_v_size_prediction';"

echo "验证: 为 p_size_clean_merged_dedup_v.prediction_data 创建索引"
bq query --use_legacy_sql=false \
  "-- 验证索引创建 SELECT index_name, table_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.INDEXES` WHERE table_name = 'p_size_clean_merged_dedup_v' AND index_name = 'idx_p_size_clean_merged_dedup_v_prediction_data';"

echo "验证: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `score_ledger_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `score_ledger`) as remaining_count;"

echo "验证: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `score_ledger_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `score_ledger`) as remaining_count;"

echo "验证: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `draws_14w_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `draws_14w_dedup_v`) as remaining_count;"

echo "验证: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `draws_14w_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `draws_14w_dedup_v`) as remaining_count;"

echo "验证: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v`) as remaining_count;"

echo "验证: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v`) as remaining_count;"

echo "验证: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "-- 验证归档结果 SELECT    (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v_归档低使用率字段_archive`) as archived_count,   (SELECT COUNT(*) FROM `p_size_clean_merged_dedup_v`) as remaining_count;"

echo "验证: 删除 score_ledger.移除冗余字段 字段"
bq query --use_legacy_sql=false \
  "-- 验证字段删除 SELECT column_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'score_ledger' AND column_name = '移除冗余字段';"

echo "验证: 删除已归档的 归档低使用率字段 字段"
bq query --use_legacy_sql=false \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'score_ledger' AND column_name = '归档低使用率字段';"

echo "验证: 删除已归档的 归档低使用率字段 字段"
bq query --use_legacy_sql=false \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'score_ledger' AND column_name = '归档低使用率字段';"

echo "验证: 删除 draws_14w_dedup_v.移除冗余字段 字段"
bq query --use_legacy_sql=false \
  "-- 验证字段删除 SELECT column_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '移除冗余字段';"

echo "验证: 删除已归档的 归档低使用率字段 字段"
bq query --use_legacy_sql=false \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '归档低使用率字段';"

echo "验证: 删除 draws_14w_dedup_v.移除冗余字段 字段"
bq query --use_legacy_sql=false \
  "-- 验证字段删除 SELECT column_name  FROM `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '移除冗余字段';"

echo "验证: 删除已归档的 归档低使用率字段 字段"
bq query --use_legacy_sql=false \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'draws_14w_dedup_v' AND column_name = '归档低使用率字段';"

echo "验证: 删除已归档的 归档低使用率字段 字段"
bq query --use_legacy_sql=false \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'p_size_clean_merged_dedup_v' AND column_name = '归档低使用率字段';"

echo "验证: 删除已归档的 归档低使用率字段 字段"
bq query --use_legacy_sql=false \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'p_size_clean_merged_dedup_v' AND column_name = '归档低使用率字段';"

echo "验证: 删除已归档的 归档低使用率字段 字段"
bq query --use_legacy_sql=false \
  "SELECT column_name FROM `INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'p_size_clean_merged_dedup_v' AND column_name = '归档低使用率字段';"

echo "验证完成"