-- Cache of the distinct cover art found in each folder, so browsing the
-- library does not read audio tags on every request. `signature` is derived
-- from the covered songs' count and latest `updated_at`; a mismatch triggers a
-- recompute on the next listing.
CREATE TABLE IF NOT EXISTS folder_cover_cache (
    folder_path TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    covers UUID[] NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
