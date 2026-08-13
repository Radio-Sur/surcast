-- Preserve the historical GStreamer migrations and add the upload-job schema
-- forward-only for both existing and fresh installations.
CREATE TABLE IF NOT EXISTS upload_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'processing',
    total INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    current_file TEXT,
    error TEXT,
    song_ids JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_upload_jobs_user ON upload_jobs(user_id, created_at DESC);

-- Match the current main schema without editing the published initial migration.
ALTER TABLE icecast_settings
    ALTER COLUMN id SET DEFAULT uuid_generate_v4();
