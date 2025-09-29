#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import math, json, os, time
from typing import Dict, Any, List, Tuple

def sigmoid(z:float)->float:
    return 1.0/(1.0+math.exp(-z))

def platt_fit(samples:List[Tuple[float,int]], iters:int=200, lr:float=0.05)->Tuple[float,float]:
    """最简 SGD 拟合 Platt：logit(p)=A*x+B，其中 x=logit(raw_p)
       样本：[(raw_p, y), ...]，y∈{0,1}"""
    def logit(p): p=min(max(p,1e-6),1-1e-6); return math.log(p/(1-p))
    A, B = 1.0, 0.0
    for _ in range(iters):
        for p,y in samples:
            x = logit(p)
            pred = sigmoid(A*x+B)
            g = (pred - y)
            A -= lr * g * x
            B -= lr * g * 1.0
    return A,B

def temp_scale_fit(samples:List[Tuple[float,int]], iters:int=200, lr:float=0.05)->float:
    """温度标定，优化交叉熵，logit(p)/T"""
    def logit(p): p=min(max(p,1e-6),1-1e-6); return math.log(p/(1-p))
    T=1.0
    for _ in range(iters):
        for p,y in samples:
            z=logit(p)/max(1e-6,T)
            q=1/(1+math.exp(-z))
            g=(q-y)*(-z)/max(1e-6,T)  # 近似一阶
            T -= lr * g
            T = max(0.1, min(5.0, T))
    return T

def apply_platt(p:float, A:float, B:float)->float:
    z = A*math.log(p/(1-p)) + B
    return 1/(1+math.exp(-z))

def apply_temp(p:float, T:float)->float:
    z = math.log(p/(1-p))/max(1e-6,T)
    return 1/(1+math.exp(-z))

def hybrid_calibrate(p_raw:float, params:Dict[str,Any])->float:
    """先 Platt 再温度"""
    A=params.get("A",1.0); B=params.get("B",0.0); T=params.get("T",1.0)
    p1 = apply_platt(p_raw, A,B)
    p2 = apply_temp(p1, T)
    return min(max(p2,1e-6),1-1e-6)
