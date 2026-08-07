use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct BatchDeleteSongsRequest {
    pub ids: Vec<Uuid>,
}

#[derive(Debug, sqlx::FromRow, Serialize)]
pub struct Song {
    pub id: Uuid,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i32,
    pub file_path: String,
    pub file_size: i64,
    pub mime_type: String,
    pub cover_path: String,
    pub uploaded_by: Uuid,
    pub cue_in: f64,
    pub cue_out: f64,
    pub cross_start_next: f64,
    pub loudness: Option<f32>,
    pub loudness_range: Option<f32>,
    pub true_peak: Option<f32>,
    pub true_peak_db: Option<f32>,
    pub amplify: Option<f32>,
    pub sustained_ending: bool,
    pub longtail: bool,
    pub analyzed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct SongResponse {
    pub id: Uuid,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i32,
    pub file_size: i64,
    pub mime_type: String,
    pub has_cover: bool,
    pub uploaded_by: Uuid,
    pub cue_in: f64,
    pub cue_out: f64,
    pub cross_start_next: f64,
    pub loudness: Option<f32>,
    pub loudness_range: Option<f32>,
    pub true_peak: Option<f32>,
    pub true_peak_db: Option<f32>,
    pub amplify: Option<f32>,
    pub sustained_ending: bool,
    pub longtail: bool,
    pub analyzed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub station_ids: Vec<Uuid>,
}

impl From<(Song, Vec<Uuid>)> for SongResponse {
    fn from((s, station_ids): (Song, Vec<Uuid>)) -> Self {
        Self {
            id: s.id,
            title: s.title,
            artist: s.artist,
            album: s.album,
            duration: s.duration,
            file_size: s.file_size,
            mime_type: s.mime_type,
            has_cover: !s.cover_path.is_empty(),
            uploaded_by: s.uploaded_by,
            cue_in: s.cue_in,
            cue_out: s.cue_out,
            cross_start_next: s.cross_start_next,
            loudness: s.loudness,
            loudness_range: s.loudness_range,
            true_peak: s.true_peak,
            true_peak_db: s.true_peak_db,
            amplify: s.amplify,
            sustained_ending: s.sustained_ending,
            longtail: s.longtail,
            analyzed_at: s.analyzed_at,
            created_at: s.created_at,
            updated_at: s.updated_at,
            station_ids,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct SongSearchParams {
    pub q: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct PaginatedSongs {
    pub songs: Vec<SongResponse>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

#[derive(Debug, Default, Deserialize)]
pub struct ArtistParams {
    pub q: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ArtistEntry {
    pub name: String,
    pub album_count: i64,
    pub song_count: i64,
}

#[derive(Debug, Serialize)]
pub struct PaginatedArtists {
    pub artists: Vec<ArtistEntry>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSongRequest {
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct AssignStationsRequest {
    pub station_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct AlbumSelector {
    pub artist: String,
    pub album: String,
}

#[derive(Debug, Deserialize)]
pub struct CountSongsRequest {
    #[serde(default)]
    pub artist_names: Vec<String>,
    #[serde(default)]
    pub album_selectors: Vec<AlbumSelector>,
    #[serde(default)]
    pub exclude_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct CountSongsResponse {
    pub count: i64,
}
