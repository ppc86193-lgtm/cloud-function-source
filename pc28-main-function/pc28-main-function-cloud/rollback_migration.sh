#!/bin/bash
# PC28数据库优化迁移计划 - 回滚脚本
# 生成时间: 2025-09-29T05:18:08.182073

set -e

echo "警告: 即将执行数据库迁移回滚"
echo "这将撤销所有迁移更改"
read -p "确认继续? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "回滚已取消"
    exit 1
fi

echo "回滚步骤 11: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "DROP TABLE `p_size_clean_merged_dedup_v_归档低使用率字段_archive`;"

echo "回滚步骤 12: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "DROP TABLE `p_size_clean_merged_dedup_v_归档低使用率字段_archive`;"

echo "回滚步骤 13: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "DROP TABLE `p_size_clean_merged_dedup_v_归档低使用率字段_archive`;"

echo "回滚步骤 14: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "DROP TABLE `draws_14w_dedup_v_归档低使用率字段_archive`;"

echo "回滚步骤 15: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "DROP TABLE `draws_14w_dedup_v_归档低使用率字段_archive`;"

echo "回滚步骤 16: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "DROP TABLE `score_ledger_归档低使用率字段_archive`;"

echo "回滚步骤 17: 创建 归档低使用率字段 字段归档表"
bq query --use_legacy_sql=false \
  "DROP TABLE `score_ledger_归档低使用率字段_archive`;"

echo "回滚步骤 18: 为 p_size_clean_merged_dedup_v.prediction_data 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_p_size_clean_merged_dedup_v_prediction_data;"

echo "回滚步骤 19: 为 p_size_clean_merged_dedup_v.size_prediction 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_p_size_clean_merged_dedup_v_size_prediction;"

echo "回滚步骤 20: 为 draws_14w_dedup_v.numbers 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_draws_14w_dedup_v_numbers;"

echo "回滚步骤 21: 为 draws_14w_dedup_v.result_sum 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_draws_14w_dedup_v_result_sum;"

echo "回滚步骤 22: 备份 draws_14w_dedup_v.移除冗余字段 字段数据"
bq query --use_legacy_sql=false \
  "DROP TABLE `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808`;"

echo "回滚步骤 23: 备份 draws_14w_dedup_v.移除冗余字段 字段数据"
bq query --use_legacy_sql=false \
  "DROP TABLE `draws_14w_dedup_v_移除冗余字段_backup_20250929_051808`;"

echo "回滚步骤 24: 为 score_ledger.outcome 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_score_ledger_outcome;"

echo "回滚步骤 25: 为 score_ledger.profit_loss 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_score_ledger_profit_loss;"

echo "回滚步骤 26: 为 score_ledger.status 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_score_ledger_status;"

echo "回滚步骤 27: 为 score_ledger.numbers 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_score_ledger_numbers;"

echo "回滚步骤 28: 为 score_ledger.result_sum 创建索引"
bq query --use_legacy_sql=false \
  "DROP INDEX idx_score_ledger_result_sum;"

echo "回滚步骤 29: 备份 score_ledger.移除冗余字段 字段数据"
bq query --use_legacy_sql=false \
  "DROP TABLE `score_ledger_移除冗余字段_backup_20250929_051808`;"

echo "回滚完成"