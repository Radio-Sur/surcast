use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::util::slugify;

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Playlist {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub slug: Option<String>,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlaylistResponse {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub slug: String,
    pub song_count: i64,
    pub total_duration_seconds: i64,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreatePlaylistRequest {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePlaylistRequest {
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlaylistSongResponse {
    pub id: Uuid,
    pub playlist_id: Uuid,
    pub song_id: Uuid,
    pub position: i32,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i32,
    pub has_cover: bool,
    pub mime_type: String,
}

#[derive(Debug, Deserialize)]
pub struct AlbumSelector {
    pub artist: String,
    pub album: String,
}

#[derive(Debug, Deserialize)]
pub struct AddPlaylistSongsRequest {
    #[serde(default)]
    pub song_ids: Vec<Uuid>,
    #[serde(default)]
    pub artist_names: Vec<String>,
    #[serde(default)]
    pub album_selectors: Vec<AlbumSelector>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedPlaylistSongs {
    pub songs: Vec<PlaylistSongResponse>,
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
pub struct BatchRemovePlaylistSongsRequest {
    pub song_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct ReorderPlaylistSongsRequest {
    pub song_ids: Vec<Uuid>,
}
