#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any

class PIController:
    def __init__(self, cfg:Dict[str,Any]):
        self.cfg = cfg
        self.mode = cfg["meta"].get("run_mode","balanced")  # conservative/balanced/aggressive
        self.state = {"min_accept": cfg["voting"].get("accept_floor",0.50)}

    def set_mode(self, mode:str):
        self.mode = mode

    def step(self, cov:float, acc:float)->Dict[str,Any]:
        t_cov = float(self.cfg["controller"]["targets"]["cov"])
        t_acc = float(self.cfg["controller"]["targets"]["acc"])
        knobs = self.cfg["controller"][self.mode]
        k_cov = float(knobs["k_cov"]); k_up=float(knobs["k_acc_up"]); k_dn=float(knobs["k_acc_dn"])
        min_b,max_b = float(self.cfg["controller"]["knobs_bounds"]["min_accept"]), float(self.cfg["controller"]["knobs_bounds"]["max_accept"])

        err_cov = t_cov - (cov or 0.0)
        err_acc = (t_acc - acc) if (acc is not None) else 0.0

        delta = k_cov*err_cov + (k_dn*max(err_acc,0.0) - k_up*max(-err_acc,0.0))
        new_floor = self.state["min_accept"] - delta  # 覆盖不足（err>0）→降低 floor；准确不足（err>0）→也降低；准确偏高（err<0）→提高
        new_floor = max(min_b, min(max_b, new_floor))
        changed = abs(new_floor - self.state["min_accept"]) >= 1e-3
        self.state["min_accept"] = new_floor
        return {"min_accept": new_floor, "changed": changed, "err_cov": err_cov, "err_acc": err_acc, "mode": self.mode}
