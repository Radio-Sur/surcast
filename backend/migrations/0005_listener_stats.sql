CREATE TABLE IF NOT EXISTS listener_stats (
    id BIGSERIAL PRIMARY KEY,
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    listeners INTEGER NOT NULL DEFAULT 0,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_listener_stats_station_time ON listener_stats(station_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_listener_stats_recorded_at ON listener_stats(recorded_at);
