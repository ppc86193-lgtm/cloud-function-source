#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PC28 端到端黑盒解决方案 - 主程序"""
from __future__ import annotations
import sys, os, yaml, json, time, datetime, traceback
from typing import Dict, Any, List

# 添加模块路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'CHANGESETS/python'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'integration'))

from bigquery_data_adapter import BQ
from enhanced_voting import WeightedVoting
from advanced_calibration import hybrid_calibrate
from adaptive_pi_controller import PIController
from risk_management import kelly_fraction, stake_units
from ledger_io import upsert_order, settle_orders
from state_storage import load_state, save_state

def load_config(path:str='pc28_enhanced_config.yaml')->Dict[str,Any]:
    with open(path,'r',encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_env_vars():
    """加载环境变量"""
    import subprocess
    try:
        # 尝试加载 ~/.pc28.env
        env_file = os.path.expanduser('~/.pc28.env')
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('"\'')
    except Exception as e:
        print(f"警告: 加载环境变量失败: {e}")

def main():
    # 加载环境变量
    load_env_vars()
    
    cfg = load_config()
    # 使用实际环境变量替换配置中的占位符
    env = {
        'PROJECT': os.environ.get('PROJECT', 'wprojectl'),
        'DS_LAB': os.environ.get('DS_LAB', 'pc28_lab'),
        'DS_DRAW': os.environ.get('DS_DRAW', 'pc28'),
        'BQLOC': os.environ.get('BQLOC', 'us-central1'),
        'TZ': os.environ.get('TZ', 'Asia/Shanghai')
    }
    
    # 初始化组件
    bq = BQ(
        project=env.get('PROJECT', 'wprojectl'),
        ds_lab=env.get('DS_LAB', 'pc28_lab'), 
        ds_draw=env.get('DS_DRAW', 'pc28'),
        bqloc=env.get('BQLOC', 'us-central1'),
        tz=env.get('TZ', 'Asia/Shanghai')
    )
    voting = WeightedVoting(cfg)
    pi_ctrl = PIController(cfg)
    
    # 加载状态
    state_dir = cfg['paths'].get('state_dir', '~/.pc28_state')
    state_path = os.path.expanduser(os.path.join(state_dir, 'main_state.json'))
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    state = load_state(state_path)
    
    print(f"[{datetime.datetime.now()}] PC28 E2E 黑盒启动")
    
    try:
        # 1. 获取数据 - 禁止模拟数据，必须使用真实数据源
        print("正在获取KPI数据...")
        kpi_data = bq.kpi_window()
        print(f"KPI数据获取成功: {kpi_data}")
        
        print("正在获取候选数据...")
        candidates = bq.read_candidates()
        print(f"候选数据获取成功: {len(candidates)} 条记录")
        
        # 2. 投票决策
        decision = voting.vote(candidates)
        
        # 3. 校准概率
        if decision and 'p_win' in decision:
            cal_params = cfg.get('calibration', {})
            decision['p_win'] = hybrid_calibrate(decision['p_win'], cal_params)
        
        # 4. PI控制调整
        if kpi_data:
            cov = kpi_data.get('coverage')
            acc = kpi_data.get('accuracy')
            pi_result = pi_ctrl.step(cov, acc)
            print(f"PI控制: {pi_result}")
        
        # 5. 风险管理
        if decision and decision.get('p_win', 0) >= pi_ctrl.state['min_accept']:
            p_win = decision['p_win']
            kelly_cap = cfg['risk']['kelly_cap']
            unit_size = cfg['risk']['unit_size']
            
            kelly_f = kelly_fraction(p_win, kelly_cap)
            stake = stake_units(p_win, unit_size, kelly_cap)
            
            if stake > 0:
                # 6. 下单记录
                order = {
                    'id': f"ord_{int(time.time())}_{decision['draw_id']}",
                    'market': decision['market'],
                    'draw_id': decision['draw_id'],
                    'p_win': p_win,
                    'ev': 2*p_win - 1,
                    'kelly_frac': kelly_f,
                    'stake_u': stake,
                    'note': f"auto_e2e_{decision.get('reason','')}"
                }
                
                upsert_order(order, env)
                print(f"下单: {order}")
        
        # 7. 结算历史订单
        settle_orders(env)
        
        # 8. 保存状态
        state['last_run'] = datetime.datetime.now().isoformat()
        state['last_decision'] = decision
        save_state(state_path, state)
        
        print(f"[{datetime.datetime.now()}] 运行完成")
        
    except Exception as e:
        print(f"错误: {e}")
        traceback.print_exc()
        # 发送告警
        if os.path.exists('telegram_notifier.sh'):
            os.system(f"bash telegram_notifier.sh 'PC28 E2E 错误: {str(e)}'")


if __name__ == '__main__':
    main()
