CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------
-- Users & API keys
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name VARCHAR(255) NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);

-- ---------------------------------------------------------------
-- Stations
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    slug VARCHAR(255) NOT NULL DEFAULT '',
    stream_url TEXT,
    current_song_index INTEGER NOT NULL DEFAULT 0,
    current_queue_item_id UUID,
    consumed_queue_item_ids UUID[] NOT NULL DEFAULT '{}',
    current_queue_cursor_format SMALLINT NOT NULL DEFAULT 0
        CONSTRAINT stations_current_queue_cursor_format_check CHECK (current_queue_cursor_format IN (0, 1)),
    prebuffer_bytes INTEGER NOT NULL DEFAULT 16384,
    played_limit INTEGER NOT NULL DEFAULT 100,
    default_fade_ms INTEGER NOT NULL DEFAULT 3000,
    transition_mode TEXT NOT NULL DEFAULT 'autocue',
    autocue_fade_max_ms INTEGER NOT NULL DEFAULT 5000,
    is_started BOOLEAN NOT NULL DEFAULT FALSE,
    created_by UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stations_created_by ON stations(created_by);
CREATE INDEX IF NOT EXISTS idx_stations_slug ON stations(slug);

-- Persistent desired lifecycle state (started/stopped); re-applied safely on
-- existing databases. Never the transient pipeline state.
ALTER TABLE stations ADD COLUMN IF NOT EXISTS is_started BOOLEAN NOT NULL DEFAULT FALSE;

-- ---------------------------------------------------------------
-- Songs (incl. autocue analysis columns)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS songs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title VARCHAR(255) NOT NULL,
    artist VARCHAR(255) NOT NULL DEFAULT '',
    album VARCHAR(255) NOT NULL DEFAULT '',
    duration INTEGER NOT NULL DEFAULT 0,
    file_path TEXT NOT NULL,
    file_size BIGINT NOT NULL DEFAULT 0,
    mime_type VARCHAR(100) NOT NULL DEFAULT 'audio/mpeg',
    cover_path TEXT NOT NULL DEFAULT '',
    uploaded_by UUID NOT NULL REFERENCES users(id),
    cue_in DOUBLE PRECISION NOT NULL DEFAULT 0,
    cue_out DOUBLE PRECISION NOT NULL DEFAULT 0,
    cross_start_next DOUBLE PRECISION NOT NULL DEFAULT 0,
    loudness REAL,
    loudness_range REAL,
    true_peak REAL,
    true_peak_db REAL,
    amplify REAL,
    sustained_ending BOOLEAN NOT NULL DEFAULT false,
    longtail BOOLEAN NOT NULL DEFAULT false,
    analyzed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_songs_uploaded_by ON songs(uploaded_by);

-- ---------------------------------------------------------------
-- Playlists
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS playlists (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    slug VARCHAR(255),
    created_by UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_playlists_created_by ON playlists(created_by);
CREATE INDEX IF NOT EXISTS idx_playlists_slug ON playlists(slug);

CREATE TABLE IF NOT EXISTS playlist_songs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    playlist_id UUID NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(playlist_id, song_id)
);

CREATE INDEX IF NOT EXISTS idx_playlist_songs_playlist_id ON playlist_songs(playlist_id);
CREATE INDEX IF NOT EXISTS idx_playlist_songs_song_id ON playlist_songs(song_id);

-- ---------------------------------------------------------------
-- Station library / queue / scheduling / auto-fill
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS station_songs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(station_id, song_id)
);

CREATE INDEX IF NOT EXISTS idx_station_songs_station_id ON station_songs(station_id);
CREATE INDEX IF NOT EXISTS idx_station_songs_song_id ON station_songs(song_id);

CREATE TABLE IF NOT EXISTS station_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    song_id UUID NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    is_auto_dj BOOLEAN NOT NULL DEFAULT false,
    origin_playlist_id UUID REFERENCES playlists(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_station_queue_order ON station_queue(station_id, position);
CREATE INDEX IF NOT EXISTS idx_station_queue_origin_playlist ON station_queue(origin_playlist_id);

CREATE TABLE IF NOT EXISTS station_schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    source_type VARCHAR NOT NULL DEFAULT 'playlist',
    playlist_id UUID REFERENCES playlists(id) ON DELETE SET NULL,
    auto_dj_mode VARCHAR,
    auto_dj_avoid_repeat BOOLEAN,
    auto_dj_min_gap INTEGER,
    auto_dj_songs_ahead INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_station_schedules_station ON station_schedules(station_id);

CREATE TABLE IF NOT EXISTS station_auto_fill (
    station_id UUID PRIMARY KEY REFERENCES stations(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT true,
    mode TEXT NOT NULL DEFAULT 'random' CHECK (mode IN ('random', 'sequential', 'reverse')),
    source_type VARCHAR NOT NULL DEFAULT 'station_library',
    source_playlist_id UUID REFERENCES playlists(id) ON DELETE SET NULL,
    avoid_artist_repeat BOOLEAN NOT NULL DEFAULT true,
    min_song_gap INTEGER NOT NULL DEFAULT 3,
    songs_ahead INTEGER NOT NULL DEFAULT 4
);

CREATE TABLE IF NOT EXISTS station_auto_fill_playlists (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    playlist_id UUID NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
    weight INTEGER NOT NULL DEFAULT 1,
    UNIQUE(station_id, playlist_id)
);

CREATE TABLE IF NOT EXISTS station_schedule_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    title TEXT,
    start_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    source_type TEXT NOT NULL DEFAULT 'playlist' CHECK (source_type IN ('playlist', 'station_library', 'global_library', 'weighted_playlists')),
    playlist_id UUID REFERENCES playlists(id) ON DELETE SET NULL,
    auto_dj_mode TEXT,
    auto_dj_avoid_repeat BOOLEAN,
    auto_dj_min_gap INTEGER,
    auto_dj_songs_ahead INTEGER,
    recurrence_type TEXT NOT NULL DEFAULT 'none' CHECK (recurrence_type IN ('none', 'daily', 'every_n_days', 'weekly', 'biweekly', 'monthly', 'custom_days')),
    recurrence_interval INTEGER,
    recurrence_days TEXT,
    recurrence_end_date DATE,
    recurrence_count INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedule_events_station ON station_schedule_events(station_id);
CREATE INDEX IF NOT EXISTS idx_schedule_events_dates ON station_schedule_events(station_id, start_date);

-- ---------------------------------------------------------------
-- Icecast (managed, auto-start by default)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS icecast_settings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    enabled BOOLEAN NOT NULL DEFAULT true,
    mode TEXT NOT NULL DEFAULT 'managed',
    port INTEGER NOT NULL DEFAULT 8000,
    source_password TEXT NOT NULL DEFAULT 'surcast',
    admin_user TEXT NOT NULL DEFAULT 'admin',
    admin_password TEXT NOT NULL DEFAULT 'surcast',
    external_url TEXT,
    external_source_pw TEXT,
    external_admin_pw TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO icecast_settings (id, enabled)
VALUES ('00000000-0000-0000-0000-000000000001', true)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------
-- Listener stats
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS listener_stats (
    id BIGSERIAL PRIMARY KEY,
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    listeners INTEGER NOT NULL DEFAULT 0,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_listener_stats_station_time ON listener_stats(station_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_listener_stats_recorded_at ON listener_stats(recorded_at);

-- ---------------------------------------------------------------
-- Upload jobs (async upload + analysis progress)
-- ---------------------------------------------------------------
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
