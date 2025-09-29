#!/usr/bin/env python3
"""
测试Supabase同步功能
"""

from supabase_sync_manager import SupabaseSyncManager
import json

def main():
    print('🔄 开始执行本地到Supabase的全量数据同步...')
    
    try:
        # 创建同步管理器
        sync_manager = SupabaseSyncManager(sqlite_db_path='local_system/pc28_local.db')
        
        # 先同步signal_pool_union_v3表（有最多数据）
        print('\n📊 同步signal_pool_union_v3表...')
        success, records, error = sync_manager.sync_table_full('signal_pool_union_v3')
        print(f'signal_pool_union_v3: {"成功" if success else "失败"} - {records} 条记录')
        if error:
            print(f'错误: {error}')
        
        # 同步其他表
        tables_to_sync = ['cloud_pred_today_norm', 'runtime_params']
        
        for table_name in tables_to_sync:
            print(f'\n📊 同步{table_name}表...')
            success, records, error = sync_manager.sync_table_full(table_name)
            print(f'{table_name}: {"成功" if success else "失败"} - {records} 条记录')
            if error:
                print(f'错误: {error}')
        
        # 获取同步统计
        stats = sync_manager.get_sync_stats()
        print(f'\n📈 同步统计: {json.dumps(stats, indent=2, ensure_ascii=False)}')
        
        print('\n✅ 全量同步测试完成!')
        
    except Exception as e:
        print(f'❌ 同步测试失败: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()