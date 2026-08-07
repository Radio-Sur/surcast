import type {
  ApiKey,
  Playlist,
  QueueItem,
  ScheduleEvent,
  Song,
  Station,
  StationSong,
  StreamStatus,
  User,
} from "@/types";

let nextId = 1;
export function resetIds() {
  nextId = 1;
}
function genId() {
  return String(nextId++);
}

export function fakeUser(overrides?: Partial<User>): User {
  return {
    id: genId(),
    username: "admin",
    name: "Admin User",
    role: "admin",
    created_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function fakeStation(overrides?: Partial<Station>): Station {
  const id = genId();
  return {
    id,
    name: "Test Station",
    description: "A test station",
    slug: `test-station-${id}`,
    stream_url: `http://localhost:8000/${id}`,
    current_song_index: 0,
    prebuffer_bytes: 0,
    played_limit: 100,
    default_fade_ms: 2000,
    transition_mode: "crossfade",
    autocue_fade_max_ms: 5000,
    created_by: "1",
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function fakeSong(overrides?: Partial<Song>): Song {
  return {
    id: genId(),
    title: "Test Song",
    artist: "Test Artist",
    album: "Test Album",
    duration: 180,
    file_size: 5000000,
    mime_type: "audio/mpeg",
    has_cover: false,
    uploaded_by: "1",
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    station_ids: [],
    ...overrides,
  };
}

export function fakeStationSong(overrides?: Partial<StationSong>): StationSong {
  return {
    id: genId(),
    song_id: genId(),
    title: "Test Song",
    artist: "Test Artist",
    album: "Test Album",
    duration: 180,
    has_cover: false,
    mime_type: "audio/mpeg",
    ...overrides,
  };
}

export function fakeQueueItem(overrides?: Partial<QueueItem>): QueueItem {
  return {
    id: genId(),
    station_id: "1",
    song_id: genId(),
    position: 0,
    title: "Test Song",
    artist: "Test Artist",
    album: "Test Album",
    duration: 180,
    has_cover: false,
    mime_type: "audio/mpeg",
    origin_playlist_id: null,
    playlist_name: null,
    is_auto_dj: false,
    ...overrides,
  };
}

export function fakePlaylist(overrides?: Partial<Playlist>): Playlist {
  return {
    id: genId(),
    name: "Test Playlist",
    description: "A test playlist",
    slug: "test-playlist",
    song_count: 0,
    total_duration_seconds: 0,
    created_by: "1",
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function fakeApiKey(overrides?: Partial<ApiKey>): ApiKey {
  return {
    id: genId(),
    name: "Test Key",
    key_prefix: "sk_test",
    is_active: true,
    last_used_at: null,
    expires_at: null,
    created_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function fakeScheduleEvent(overrides?: Partial<ScheduleEvent>): ScheduleEvent {
  return {
    id: genId(),
    station_id: "1",
    title: null,
    start_date: "2026-01-01",
    start_time: "09:00",
    end_time: "10:00",
    source_type: "playlist",
    playlist_id: null,
    playlist_name: null,
    auto_dj_mode: null,
    auto_dj_avoid_repeat: null,
    auto_dj_min_gap: null,
    auto_dj_songs_ahead: null,
    recurrence_type: "none",
    recurrence_interval: null,
    recurrence_days: null,
    recurrence_end_date: null,
    recurrence_count: null,
    created_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function fakeStreamStatus(overrides?: Partial<StreamStatus>): StreamStatus {
  return {
    playing: true,
    song_index: 0,
    total: 5,
    elapsed: 30,
    title: "Test Song",
    artist: "Test Artist",
    duration: 180,
    ...overrides,
  };
}

export function fakeAuthResponse(overrides?: { user?: Partial<User>; access_token?: string; refresh_token?: string }) {
  return {
    access_token: "mock-access-token",
    refresh_token: "mock-refresh-token",
    user: fakeUser(overrides?.user),
  };
}
