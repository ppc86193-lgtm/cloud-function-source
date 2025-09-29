#!/usr/bin/env python3
"""
简单的Supabase同步测试
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from supabase_sync_manager import SupabaseSyncManager
    
    print('🔄 开始Supabase同步测试...')
    
    # 创建同步管理器
    sync_manager = SupabaseSyncManager(sqlite_db_path='local_system/pc28_local.db')
    
    # 测试signal_pool_union_v3表的全量同步
    print('📊 同步signal_pool_union_v3表...')
    success, records, error = sync_manager.sync_table_full('signal_pool_union_v3')
    
    print(f'结果: 成功={success}, 记录数={records}')
    if error:
        print(f'错误: {error}')
    else:
        print('✅ 同步成功!')
        
    # 测试其他表
    other_tables = ['cloud_pred_today_norm', 'runtime_params']
    for table in other_tables:
        print(f'📊 同步{table}表...')
        success, records, error = sync_manager.sync_table_full(table)
        print(f'{table}: 成功={success}, 记录数={records}')
        if error:
            print(f'错误: {error}')
    
    print('✅ 所有测试完成!')
    
except Exception as e:
    print(f'❌ 测试失败: {e}')
    import traceback
    traceback.print_exc()