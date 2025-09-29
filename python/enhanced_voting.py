#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import math
from typing import Dict, Any, Tuple

def _clip(x:float, lo:float, hi:float)->float:
    return max(lo, min(hi, x))

def combine_probs(p_cloud:float, p_map:float, p_size:float, w:Dict[str,float])->float:
    """带权合成概率（logit 空间更鲁棒），并限制权重在[wf,wc]内"""
    def logit(p:float)->float:
        p = _clip(p, 1e-6, 1-1e-6)
        return math.log(p/(1-p))
    lc, lm, ls = logit(p_cloud), logit(p_map), logit(p_size)
    s = w["cloud"]+w["map"]+w["size"]
    z = (w["cloud"]*lc + w["map"]*lm + w["size"]*ls) / max(1e-9,s)
    p = 1/(1+math.exp(-z))
    return p

def vote_bucket(p:float, buckets)->Tuple[float,str]:
    """返回(投票桶阈值, 桶标签)"""
    b = 0.50
    lbl = "0.50"
    if p>=0.67: b, lbl=0.67,"0.67"
    if p>=1.00: b, lbl=1.00,"1.00"
    return b,lbl

def adapt_weights(w:Dict[str,float], perf:Dict[str,float], eta:float, wf:float, wc:float)->Dict[str,float]:
    """根据三源近期表现（如 ACC 差值）做微调；perf>0 提升权重、<0 降权重。"""
    ww = dict(w)
    for k in ("cloud","map","size"):
        delta = eta * perf.get(k,0.0)
        ww[k] = _clip(ww[k] + delta, wf, wc)
    s = sum(ww.values()) or 1.0
    for k in ww: ww[k] = ww[k]/s
    return ww

def decide(p_cloud, p_map, p_size, cfg, perf):
    w = dict(cfg["voting"].get("weights_init", {"cloud":0.5,"map":0.3,"size":0.2}))
    wf = float(cfg["voting"].get("weight_floor",0.10))
    wc = float(cfg["voting"].get("weight_ceiling",0.70))
    eta= float(cfg["voting"].get("weight_eta",0.02))
    w = adapt_weights(w, perf or {}, eta, wf, wc)
    p_star = combine_probs(p_cloud, p_map, p_size, w)

    # 极端门：增强/削弱门控（可自定）
    if cfg["voting"]["extreme_gate"]["enable"]:
        hi, lo = float(cfg["voting"]["extreme_gate"]["hi"]), float(cfg["voting"]["extreme_gate"]["lo"])
        if p_star>=hi: p_star = min(0.999, p_star + 0.02)
        if p_star<=lo: p_star = max(0.001, p_star - 0.02)

    # 桶决策 + 可被 AutoSwitch 降 accept_floor（bucket_floor）
    accept_floor = float(cfg["voting"].get("accept_floor",0.50))
    # 外部请求文件可能降低 accept_floor（由主系统读取并应用，这里仅使用 cfg 值）
    b, lbl = vote_bucket(p_star, cfg["voting"]["buckets"])
    accept = (p_star >= max(accept_floor, 0.33))
    return {
        "p_star": p_star,
        "bucket": lbl,
        "accept": bool(accept),
        "weights": w
    }

class WeightedVoting:
    """加权投票类，包装投票决策逻辑"""
    
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
    
    def vote(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """对候选项进行投票决策"""
        if not candidates:
            return None
            
        # 假设candidates包含三个源的概率预测
        # 这里需要根据实际数据结构调整
        p_cloud = 0.5  # 默认值
        p_map = 0.5
        p_size = 0.5
        
        # 如果candidates有具体的概率数据，从中提取
        for candidate in candidates:
            if candidate.get('source') == 'cloud':
                p_cloud = candidate.get('p_win', 0.5)
            elif candidate.get('source') == 'map':
                p_map = candidate.get('p_win', 0.5)
            elif candidate.get('source') == 'size':
                p_size = candidate.get('p_win', 0.5)
        
        # 使用现有的decide函数
        result = decide(p_cloud, p_map, p_size, self.cfg, {})
        
        # 添加必要的字段
        if candidates:
            result['draw_id'] = candidates[0].get('draw_id', 'unknown')
            result['market'] = candidates[0].get('market', 'oe')
            result['p_win'] = result['p_star']
        
        return result
