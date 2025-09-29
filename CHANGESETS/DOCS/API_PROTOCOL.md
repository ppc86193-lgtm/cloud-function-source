# API_PROTOCOL（请求文件协议）
- bucket_floor_request.json: {"market":"oe|size|both","bucket_floor":0.33,"ttl_min":60}
- mode_switch_request.json: {"mode":"conservative|balanced|aggressive","ttl_min":60}
- 协议：由桥接器消费写入 runtime_params/runtime_mode；冲突：后到覆盖前到；TTL超时由上游清理。
