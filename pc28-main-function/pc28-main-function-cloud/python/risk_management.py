#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any

def kelly_fraction(p_win:float, cap:float=0.05)->float:
    # 2p-1
    ev = 2.0*p_win - 1.0
    return max(0.0, min(cap, ev))

def stake_units(p_win:float, unit:int, cap:float)->int:
    f = kelly_fraction(p_win, cap)
    su = int(round(f / max(1e-9, cap))) * unit
    return max(0, su)
