-- 修复同步状态表结构
ALTER TABLE sync_status 
ADD COLUMN IF NOT EXISTS sync_duration_seconds DECIMAL(10,3) DEFAULT 0.0;

-- 更新现有记录
UPDATE sync_status 
SET sync_duration_seconds = sync_duration 
WHERE sync_duration_seconds IS NULL;