use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::util::slugify;

#[derive(Debug, sqlx::FromRow, Serialize)]
pub struct Station {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub slug: String,
    pub stream_url: Option<String>,
    pub current_song_index: i32,
    pub prebuffer_bytes: i32,
    pub played_limit: i32,
    pub default_fade_ms: i32,
    pub transition_mode: String,
    pub autocue_fade_max_ms: i32,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Station {
    /// The Icecast mount point this station streams to.
    pub fn mount(&self) -> String {
        match self.stream_url.as_deref() {
            Some(raw) if !raw.is_empty() => {
                if raw.ends_with(".mp3") {
                    raw.to_string()
                } else {
                    format!("{raw}.mp3")
                }
            }
            _ => format!("{}.mp3", self.name),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateStationRequest {
    pub name: String,
    pub description: Option<String>,
    pub stream_url: Option<String>,
    pub prebuffer_bytes: Option<i32>,
    pub played_limit: Option<i32>,
    pub default_fade_ms: Option<i32>,
    pub transition_mode: Option<String>,
    pub autocue_fade_max_ms: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStationRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub stream_url: Option<String>,
    pub prebuffer_bytes: Option<i32>,
    pub played_limit: Option<i32>,
    pub default_fade_ms: Option<i32>,
    pub transition_mode: Option<String>,
    pub autocue_fade_max_ms: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct StationResponse {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub slug: String,
    pub stream_url: Option<String>,
    pub current_song_index: i32,
    pub prebuffer_bytes: i32,
    pub played_limit: i32,
    pub default_fade_ms: i32,
    pub transition_mode: String,
    pub autocue_fade_max_ms: i32,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<Station> for StationResponse {
    fn from(s: Station) -> Self {
        Self {
            id: s.id,
            name: s.name,
            description: s.description,
            slug: s.slug,
            stream_url: s.stream_url,
            current_song_index: s.current_song_index,
            prebuffer_bytes: s.prebuffer_bytes,
            played_limit: s.played_limit,
            default_fade_ms: s.default_fade_ms,
            transition_mode: s.transition_mode,
            autocue_fade_max_ms: s.autocue_fade_max_ms,
            created_by: s.created_by,
            created_at: s.created_at,
            updated_at: s.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StationSongResponse {
    pub id: Uuid,
    pub song_id: Uuid,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i32,
    pub has_cover: bool,
    pub mime_type: String,
}

#[derive(Debug, Serialize)]
pub struct QueueItemResponse {
    pub id: Uuid,
    pub station_id: Uuid,
    pub song_id: Uuid,
    pub position: i32,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i32,
    pub has_cover: bool,
    pub mime_type: String,
    pub origin_playlist_id: Option<Uuid>,
    pub playlist_name: Option<String>,
    pub is_auto_dj: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedStationSongs {
    pub songs: Vec<StationSongResponse>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

#[derive(Debug, Default, Deserialize)]
pub struct PaginationParams {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AddToQueueRequest {
    pub song_ids: Vec<Uuid>,
    pub playlist_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct AlbumSelector {
    pub artist: String,
    pub album: String,
}

#[derive(Debug, Deserialize)]
pub struct AddStationSongsRequest {
    #[serde(default)]
    pub song_ids: Vec<Uuid>,
    #[serde(default)]
    pub artist_names: Vec<String>,
    #[serde(default)]
    pub album_selectors: Vec<AlbumSelector>,
}

#[derive(Debug, Deserialize)]
pub struct ReorderQueueRequest {
    pub queue_item_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct InsertIntoQueueRequest {
    pub song_id: Uuid,
    pub position: i32,
}
