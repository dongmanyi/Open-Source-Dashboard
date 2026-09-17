-- Apply before deploying the atomic publisher and generation-aware API cache.
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS snapshot_generation BIGINT NOT NULL DEFAULT 0;
