#!/bin/bash
# PC28字段清理回滚脚本
# 在出现问题时恢复字段和数据

set -e

echo '=== PC28字段清理回滚 ==='
echo "回滚开始时间: $(date)"

# 检查备份文件
BACKUP_FILE=$(ls -t pc28_archive_backup_*.db 2>/dev/null | head -1)

if [ -z "$BACKUP_FILE" ]; then
    echo "错误: 未找到备份文件"
    exit 1
fi

echo "使用备份文件: $BACKUP_FILE"

# 创建当前状态备份
echo "1. 创建当前状态备份..."
sqlite3 pc28_local.db ".backup pc28_rollback_backup_$(date +%Y%m%d_%H%M%S).db"

# 恢复数据库
echo "2. 恢复数据库..."
cp "$BACKUP_FILE" pc28_local_restored.db

echo "3. 验证恢复..."
RESTORED_TABLES=$(sqlite3 pc28_local_restored.db "SELECT COUNT(*) FROM sqlite_master WHERE type='table';")
echo "恢复的表数量: $RESTORED_TABLES"

echo "=== 回滚完成 ==="
echo "完成时间: $(date)"
echo "请手动验证数据完整性后，将 pc28_local_restored.db 重命名为 pc28_local.db"
