#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, os, tempfile, shutil
from typing import Dict, Any

def load_state(path:str)->Dict[str,Any]:
    if not os.path.exists(path): return {}
    with open(path,'r',encoding='utf-8') as f:
        return json.load(f)

def save_state(path:str, data:Dict[str,Any]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=os.path.dirname(path),delete=False) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp_path = tmp.name
    shutil.move(tmp_path, path)
