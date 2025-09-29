#!/bin/bash
# PC28系统字段归档脚本
# 安全地将低使用率字段迁移到归档表

set -e  # 遇到错误立即退出

echo '=== PC28字段归档开始 ==='
echo "开始时间: $(date)"

# 备份原始数据
echo '1. 创建备份...'
sqlite3 pc28_local.db '.backup pc28_archive_backup_$(date +%Y%m%d_%H%M%S).db'

# 为每个需要归档的字段创建归档表和迁移数据

echo '2. 处理 score_ledger.result_digits...'

# 创建归档表 score_ledger_result_digits_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS score_ledger_result_digits_archive AS
SELECT rowid, result_digits, timestamp FROM score_ledger WHERE result_digits IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM score_ledger_result_digits_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM score_ledger WHERE result_digits IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ score_ledger.result_digits 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ score_ledger.result_digits 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '2. 处理 score_ledger.source...'

# 创建归档表 score_ledger_source_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS score_ledger_source_archive AS
SELECT rowid, source, timestamp FROM score_ledger WHERE source IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM score_ledger_source_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM score_ledger WHERE source IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ score_ledger.source 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ score_ledger.source 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '2. 处理 draws_14w_dedup_v.ts_utc...'

# 创建归档表 draws_14w_dedup_v_ts_utc_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS draws_14w_dedup_v_ts_utc_archive AS
SELECT rowid, ts_utc, timestamp FROM draws_14w_dedup_v WHERE ts_utc IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM draws_14w_dedup_v_ts_utc_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM draws_14w_dedup_v WHERE ts_utc IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ draws_14w_dedup_v.ts_utc 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ draws_14w_dedup_v.ts_utc 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '2. 处理 draws_14w_dedup_v.legacy_format...'

# 创建归档表 draws_14w_dedup_v_legacy_format_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS draws_14w_dedup_v_legacy_format_archive AS
SELECT rowid, legacy_format, timestamp FROM draws_14w_dedup_v WHERE legacy_format IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM draws_14w_dedup_v_legacy_format_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM draws_14w_dedup_v WHERE legacy_format IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ draws_14w_dedup_v.legacy_format 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ draws_14w_dedup_v.legacy_format 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '2. 处理 draws_14w_dedup_v.data_source...'

# 创建归档表 draws_14w_dedup_v_data_source_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS draws_14w_dedup_v_data_source_archive AS
SELECT rowid, data_source, timestamp FROM draws_14w_dedup_v WHERE data_source IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM draws_14w_dedup_v_data_source_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM draws_14w_dedup_v WHERE data_source IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ draws_14w_dedup_v.data_source 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ draws_14w_dedup_v.data_source 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '2. 处理 p_size_clean_merged_dedup_v.model_version...'

# 创建归档表 p_size_clean_merged_dedup_v_model_version_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS p_size_clean_merged_dedup_v_model_version_archive AS
SELECT rowid, model_version, timestamp FROM p_size_clean_merged_dedup_v WHERE model_version IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM p_size_clean_merged_dedup_v_model_version_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM p_size_clean_merged_dedup_v WHERE model_version IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ p_size_clean_merged_dedup_v.model_version 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ p_size_clean_merged_dedup_v.model_version 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '2. 处理 p_size_clean_merged_dedup_v.raw_features...'

# 创建归档表 p_size_clean_merged_dedup_v_raw_features_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS p_size_clean_merged_dedup_v_raw_features_archive AS
SELECT rowid, raw_features, timestamp FROM p_size_clean_merged_dedup_v WHERE raw_features IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM p_size_clean_merged_dedup_v_raw_features_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM p_size_clean_merged_dedup_v WHERE raw_features IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ p_size_clean_merged_dedup_v.raw_features 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ p_size_clean_merged_dedup_v.raw_features 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '2. 处理 p_size_clean_merged_dedup_v.processing_time...'

# 创建归档表 p_size_clean_merged_dedup_v_processing_time_archive
sqlite3 pc28_local.db """
CREATE TABLE IF NOT EXISTS p_size_clean_merged_dedup_v_processing_time_archive AS
SELECT rowid, processing_time, timestamp FROM p_size_clean_merged_dedup_v WHERE processing_time IS NOT NULL;
"""

# 验证归档数据
ARCHIVE_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM p_size_clean_merged_dedup_v_processing_time_archive;")
ORIGINAL_COUNT=$(sqlite3 pc28_local.db "SELECT COUNT(*) FROM p_size_clean_merged_dedup_v WHERE processing_time IS NOT NULL;")

if [ "$ARCHIVE_COUNT" -eq "$ORIGINAL_COUNT" ]; then
    echo "✓ p_size_clean_merged_dedup_v.processing_time 归档成功: $ARCHIVE_COUNT 条记录"
else
    echo "✗ p_size_clean_merged_dedup_v.processing_time 归档失败: 预期 $ORIGINAL_COUNT，实际 $ARCHIVE_COUNT"
    exit 1
fi

echo '=== 归档完成 ==='
echo "完成时间: $(date)"
echo '请等待1周监控期，确认无业务影响后执行字段删除脚本'