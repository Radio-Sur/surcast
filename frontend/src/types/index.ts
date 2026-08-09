export interface User {
  id: string;
  username: string;
  name: string;
  role: "admin" | "manager" | "viewer";
  created_at: string;
}

export interface Station {
  id: string;
  name: string;
  description: string;
  slug: string;
  stream_url: string | null;
  current_song_index: number;
  prebuffer_bytes: number;
  played_limit: number;
  default_fade_ms: number;
  transition_mode: "crossfade" | "autocue" | "off";
  autocue_fade_max_ms: number;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface AuthResponse {
  access_token: string;
  refresh_token: string;
  user: User;
}

export interface ApiKey {
  id: string;
  name: string;
  key_prefix: string;
  is_active: boolean;
  last_used_at: string | null;
  expires_at: string | null;
  created_at: string;
}

export interface ApiKeyCreated extends ApiKey {
  key: string;
}

export interface Song {
  id: string;
  title: string;
  artist: string;
  album: string;
  duration: number;
  file_size: number;
  mime_type: string;
  has_cover: boolean;
  uploaded_by: string;
  created_at: string;
  updated_at: string;
  station_ids: string[];
}

export type UploadJobStatusValue = "processing" | "done" | "error";

export interface UploadJobStatus {
  id: string;
  status: UploadJobStatusValue;
  total: number;
  processed: number;
  failed: number;
  current_file: string | null;
  error: string | null;
  song_ids: string[];
}

export interface StationSong {
  id: string;
  song_id: string;
  title: string;
  artist: string;
  album: string;
  duration: number;
  has_cover: boolean;
  mime_type: string;
}

export interface PaginatedSongs {
  songs: Song[];
  total: number;
  page: number;
  per_page: number;
}

export interface ArtistEntry {
  name: string;
  album_count: number;
  song_count: number;
}

export interface PaginatedArtists {
  artists: ArtistEntry[];
  total: number;
  page: number;
  per_page: number;
}

export interface AlbumSelector {
  artist: string;
  album: string;
}

export interface SongSelections {
  songIds: string[];
  artistNames: string[];
  albumSelectors: AlbumSelector[];
}

export interface QueueItem {
  id: string;
  station_id: string;
  song_id: string;
  position: number;
  title: string;
  artist: string;
  album: string;
  duration: number;
  has_cover: boolean;
  mime_type: string;
  origin_playlist_id: string | null;
  playlist_name: string | null;
  is_auto_dj: boolean;
}

export interface PlaylistGroup {
  kind: "playlist_group";
  playlist_id: string;
  playlist_name: string;
  songs: QueueItem[];
  total_duration: number;
  current_song_index?: number;
}

export function isPlaylistGroup(item: QueueItem | PlaylistGroup): item is PlaylistGroup {
  return (item as PlaylistGroup).kind === "playlist_group";
}

export interface Playlist {
  id: string;
  name: string;
  description: string;
  slug: string;
  song_count: number;
  total_duration_seconds: number;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface PlaylistSong {
  id: string;
  playlist_id: string;
  song_id: string;
  position: number;
  title: string;
  artist: string;
  album: string;
  duration: number;
  has_cover: boolean;
  mime_type: string;
}

export interface PaginatedPlaylistSongs {
  songs: PlaylistSong[];
  total: number;
  page: number;
  per_page: number;
}

export interface PaginatedStationSongs {
  songs: StationSong[];
  total: number;
  page: number;
  per_page: number;
}

export interface StreamStatus {
  playing: boolean;
  song_index: number;
  total: number;
  elapsed: number;
  title: string;
  artist: string;
  duration: number;
}

export type ListenerRange = "24h" | "7d" | "30d";

export interface LiveListeners {
  station_id: string;
  listeners: number;
  updated_at: string | null;
  online: boolean;
}

export interface ListenersHistoryPoint {
  time: string;
  listeners: number;
}

export interface ListenersHourStat {
  hour: number;
  avg_listeners: number;
}

export interface ListenersWeekdayStat {
  weekday: number;
  avg_listeners: number;
}

export interface ListenersStationRow {
  station_id: string;
  name: string;
  listeners: number;
  updated_at: string | null;
  online: boolean;
}

export interface ListenersOverview {
  range: ListenerRange;
  total_now: number;
  stations: ListenersStationRow[];
  by_hour: ListenersHourStat[];
  by_weekday: ListenersWeekdayStat[];
  series: ListenersHistoryPoint[];
}

export type ScheduleSourceType = "playlist" | "station_library" | "global_library" | "weighted_playlists";

export interface ScheduleEntry {
  id: string;
  station_id: string;
  day_of_week: number;
  start_time: string;
  end_time: string;
  source_type: ScheduleSourceType;
  playlist_id: string | null;
  playlist_name: string | null;
  auto_dj_mode: string | null;
  auto_dj_avoid_repeat: boolean | null;
  auto_dj_min_gap: number | null;
  auto_dj_songs_ahead: number | null;
}

export type RecurrenceType = "none" | "daily" | "every_n_days" | "weekly" | "biweekly" | "monthly" | "custom_days";

export interface ScheduleEvent {
  id: string;
  station_id: string;
  title: string | null;
  start_date: string;
  start_time: string;
  end_time: string;
  source_type: ScheduleSourceType;
  playlist_id: string | null;
  playlist_name: string | null;
  auto_dj_mode: string | null;
  auto_dj_avoid_repeat: boolean | null;
  auto_dj_min_gap: number | null;
  auto_dj_songs_ahead: number | null;
  recurrence_type: RecurrenceType;
  recurrence_interval: number | null;
  recurrence_days: number[] | null;
  recurrence_end_date: string | null;
  recurrence_count: number | null;
  created_at: string;
}

export interface CreateScheduleEventRequest {
  title?: string | null;
  start_date: string;
  start_time: string;
  end_time: string;
  source_type?: ScheduleSourceType;
  playlist_id?: string | null;
  auto_dj_mode?: string | null;
  auto_dj_avoid_repeat?: boolean | null;
  auto_dj_min_gap?: number | null;
  auto_dj_songs_ahead?: number | null;
  recurrence_type?: RecurrenceType;
  recurrence_interval?: number | null;
  recurrence_days?: number[] | null;
  recurrence_end_date?: string | null;
  recurrence_count?: number | null;
}

export interface AutoFillPlaylistEntry {
  id: string;
  playlist_id: string;
  playlist_name: string;
  weight: number;
}

export interface AutoFillConfig {
  station_id: string;
  enabled: boolean;
  mode: "random" | "sequential" | "reverse";
  source_type: ScheduleSourceType;
  source_playlist_id: string | null;
  source_playlist_name: string | null;
  avoid_artist_repeat: boolean;
  min_song_gap: number;
  songs_ahead: number;
  weighted_playlists: AutoFillPlaylistEntry[];
}
