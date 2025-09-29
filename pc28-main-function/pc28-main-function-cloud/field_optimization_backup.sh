#!/bin/bash
# PC28系统字段优化 - 数据备份脚本
# 生成时间: $(date)

set -e  # 遇到错误立即退出

# 配置
BACKUP_DIR="./optimization_backups/$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$BACKUP_DIR/backup.log"

# 创建备份目录
mkdir -p "$BACKUP_DIR"

echo "=== PC28系统字段优化备份开始 ===" | tee "$LOG_FILE"
echo "备份目录: $BACKUP_DIR" | tee -a "$LOG_FILE"
echo "开始时间: $(date)" | tee -a "$LOG_FILE"

# 1. 备份SQLite数据库文件
echo "1. 备份SQLite数据库文件..." | tee -a "$LOG_FILE"

# 查找所有.db文件并备份
find . -name "*.db" -not -path "./optimization_backups/*" -not -path "./venv/*" | while read db_file; do
    echo "  备份: $db_file" | tee -a "$LOG_FILE"
    cp "$db_file" "$BACKUP_DIR/$(basename $db_file).backup"
done

# 2. 备份关键Python文件
echo "2. 备份关键Python文件..." | tee -a "$LOG_FILE"

critical_files=(
    "field_usage_analysis.py"
    "database_table_optimizer.py" 
    "python/pc28_upstream_api.py"
    "python/realtime_lottery_service.py"
    "python/history_backfill_service.py"
    "real_api_data_system.py"
    "online_data_validator.py"
    "database_manager.py"
)

for file in "${critical_files[@]}"; do
    if [ -f "$file" ]; then
        echo "  备份: $file" | tee -a "$LOG_FILE"
        cp "$file" "$BACKUP_DIR/$(basename $file).backup"
    fi
done

# 3. 创建表结构备份SQL
echo "3. 创建表结构备份..." | tee -a "$LOG_FILE"

cat > "$BACKUP_DIR/table_structure_backup.sql" << 'EOF'
-- PC28系统表结构备份
-- 生成时间: $(date)

-- 备份score_ledger表结构和数据
.output score_ledger_backup.sql
.dump score_ledger

-- 备份draws_14w_dedup_v表结构和数据  
.output draws_14w_dedup_v_backup.sql
.dump draws_14w_dedup_v

-- 备份p_size_clean_merged_dedup_v表结构和数据
.output p_size_clean_merged_dedup_v_backup.sql
.dump p_size_clean_merged_dedup_v

.output stdout
EOF

# 4. 创建回滚脚本
echo "4. 创建回滚脚本..." | tee -a "$LOG_FILE"

cat > "$BACKUP_DIR/rollback.sh" << 'EOF'
#!/bin/bash
# PC28系统字段优化回滚脚本

set -e

BACKUP_DIR=$(dirname "$0")
echo "=== PC28系统字段优化回滚开始 ==="
echo "从备份目录恢复: $BACKUP_DIR"

# 恢复数据库文件
echo "1. 恢复数据库文件..."
for backup_file in "$BACKUP_DIR"/*.db.backup; do
    if [ -f "$backup_file" ]; then
        original_file=$(basename "$backup_file" .backup)
        echo "  恢复: $original_file"
        cp "$backup_file" "../$original_file"
    fi
done

# 恢复Python文件
echo "2. 恢复Python文件..."
for backup_file in "$BACKUP_DIR"/*.py.backup; do
    if [ -f "$backup_file" ]; then
        original_file=$(basename "$backup_file" .backup)
        echo "  恢复: $original_file"
        
        # 根据文件路径恢复到正确位置
        if [[ "$original_file" == pc28_upstream_api.py ]] || [[ "$original_file" == realtime_lottery_service.py ]] || [[ "$original_file" == history_backfill_service.py ]]; then
            cp "$backup_file" "../python/$original_file"
        else
            cp "$backup_file" "../$original_file"
        fi
    fi
done

echo "=== 回滚完成 ==="
echo "请运行系统测试验证功能正常"
EOF

chmod +x "$BACKUP_DIR/rollback.sh"

# 5. 创建验证脚本
echo "5. 创建验证脚本..." | tee -a "$LOG_FILE"

cat > "$BACKUP_DIR/verify_backup.py" << 'EOF'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28系统备份验证脚本
"""

import os
import sqlite3
import sys
from pathlib import Path

def verify_database_backup(backup_dir):
    """验证数据库备份完整性"""
    print("=== 验证数据库备份 ===")
    
    backup_files = list(Path(backup_dir).glob("*.db.backup"))
    
    for backup_file in backup_files:
        print(f"验证: {backup_file.name}")
        
        try:
            # 尝试连接备份数据库
            conn = sqlite3.connect(str(backup_file))
            cursor = conn.cursor()
            
            # 获取表列表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            print(f"  包含 {len(tables)} 个表")
            
            # 验证每个表的记录数
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"    {table_name}: {count} 条记录")
            
            conn.close()
            print(f"  ✓ {backup_file.name} 验证通过")
            
        except Exception as e:
            print(f"  ✗ {backup_file.name} 验证失败: {e}")
            return False
    
    return True

def verify_file_backup(backup_dir):
    """验证文件备份完整性"""
    print("\n=== 验证文件备份 ===")
    
    backup_files = list(Path(backup_dir).glob("*.py.backup"))
    
    for backup_file in backup_files:
        print(f"验证: {backup_file.name}")
        
        if backup_file.stat().st_size > 0:
            print(f"  ✓ {backup_file.name} 验证通过 ({backup_file.stat().st_size} bytes)")
        else:
            print(f"  ✗ {backup_file.name} 文件为空")
            return False
    
    return True

if __name__ == "__main__":
    backup_dir = os.path.dirname(os.path.abspath(__file__))
    
    print(f"验证备份目录: {backup_dir}")
    
    db_ok = verify_database_backup(backup_dir)
    file_ok = verify_file_backup(backup_dir)
    
    if db_ok and file_ok:
        print("\n✓ 所有备份验证通过")
        sys.exit(0)
    else:
        print("\n✗ 备份验证失败")
        sys.exit(1)
EOF

chmod +x "$BACKUP_DIR/verify_backup.py"

# 6. 执行备份验证
echo "6. 执行备份验证..." | tee -a "$LOG_FILE"
cd "$BACKUP_DIR"
python3 verify_backup.py | tee -a "$LOG_FILE"
cd - > /dev/null

echo "=== 备份完成 ===" | tee -a "$LOG_FILE"
echo "备份目录: $BACKUP_DIR" | tee -a "$LOG_FILE"
echo "完成时间: $(date)" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "回滚命令: $BACKUP_DIR/rollback.sh" | tee -a "$LOG_FILE"
echo "验证命令: $BACKUP_DIR/verify_backup.py" | tee -a "$LOG_FILE"