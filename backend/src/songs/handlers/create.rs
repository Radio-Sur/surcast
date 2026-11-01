use axum::extract::{Multipart, Query, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::io::Read;
use uuid::Uuid;

use super::assign::assign_song_to_stations;
use super::upload_helper::is_audio_file;
use crate::auth::middleware::AuthUser;
use crate::config::Config;
use crate::errors::{AppError, DbResult};
use crate::songs::import::{store_upload_bytes, DuplicateInfo, ImportOrigin, ImportOutcome, ImportRequest};
use crate::songs::models::*;
use crate::songs::repository;

#[derive(Debug, Default, Deserialize)]
pub struct UploadFlags {
    /// When true, an already-known audio hash inserts a second song row
    /// (sharing the file) instead of reporting the duplicate.
    #[serde(default)]
    pub allow_duplicate: bool,
}

/// Unified single-upload result. `duplicate` is `Some` exactly when the
/// uploaded bytes matched an already-known song and `allow_duplicate`
/// was not set — `song` then carries the existing row.
#[derive(Debug, Serialize)]
pub struct UploadSongResult {
    pub created: bool,
    pub song: SongResponse,
    pub duplicate: Option<DuplicateInfo>,
}

fn overrides(title: &str, artist: &str, album: &str) -> (Option<String>, Option<String>, Option<String>) {
    (
        if title.is_empty() { None } else { Some(title.to_string()) },
        if artist.is_empty() { None } else { Some(artist.to_string()) },
        if album.is_empty() { None } else { Some(album.to_string()) },
    )
}

async fn fetch_response(db: &PgPool, song_id: Uuid, station_ids: &[Uuid]) -> Result<SongResponse, AppError> {
    let assigned = assign_song_to_stations(db, song_id, station_ids).await?;
    let song = repository::find_song_by_id(db, song_id).await?.ok_or_else(|| {
        tracing::error!("Insert succeeded but fetch returned None");
        AppError::internal("".into())
    })?;
    Ok(SongResponse::from((song, assigned)))
}

async fn import_stored_file(
    db: &PgPool,
    config: &Config,
    user_id: Uuid,
    relative_path: String,
    allow_duplicate: bool,
    title_override: Option<String>,
    artist_override: Option<String>,
    album_override: Option<String>,
    station_ids: &[Uuid],
) -> Result<UploadSongResult, AppError> {
    let outcome = crate::songs::import::import_song_file(
        db,
        &config.music_dir,
        &ImportRequest {
            relative_path,
            origin: ImportOrigin::Api,
            uploaded_by: Some(user_id),
            allow_duplicate,
            title_override,
            artist_override,
            album_override,
            lastfm_api_key: config.lastfm_api_key.clone(),
        },
    )
    .await?;

    match outcome {
        ImportOutcome::Created { song_id } => Ok(UploadSongResult {
            created: true,
            song: fetch_response(db, song_id, station_ids).await?,
            duplicate: None,
        }),
        ImportOutcome::Duplicate { duplicate } => Ok(UploadSongResult {
            created: false,
            song: fetch_response(db, duplicate.song_id, station_ids).await?,
            duplicate: Some(duplicate),
        }),
    }
}

pub async fn upload_song(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    Query(flags): Query<UploadFlags>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<UploadSongResult>), AppError> {
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut original_name = String::new();
    let mut title = String::new();
    let mut artist = String::new();
    let mut album = String::new();
    let mut assign_to_all = false;
    let mut station_ids: Vec<Uuid> = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::bad_request(format!("Invalid multipart: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                original_name = field.file_name().unwrap_or("unknown").to_string();
                file_bytes = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| AppError::bad_request(format!("Failed to read file: {e}")))?
                        .to_vec(),
                );
            }
            "title" => title = field.text().await.unwrap_or_default(),
            "artist" => artist = field.text().await.unwrap_or_default(),
            "album" => album = field.text().await.unwrap_or_default(),
            "assign_to_all" => assign_to_all = field.text().await.unwrap_or_default() == "true",
            "station_ids" => {
                let text = field.text().await.unwrap_or_default();
                if !text.is_empty() {
                    station_ids =
                        serde_json::from_str(&text).map_err(|e| AppError::bad_request(format!("Invalid station_ids JSON: {e}")))?;
                }
            }
            _ => {}
        }
    }

    let bytes = file_bytes.ok_or_else(|| AppError::bad_request("No file provided".into()))?;

    if assign_to_all {
        station_ids = crate::stations::repository::find_all_station_ids(&db)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect();
    }

    let relative_path = store_upload_bytes(&config.music_dir, &original_name, &bytes).await?;
    let (title_override, artist_override, album_override) = overrides(&title, &artist, &album);
    let result = import_stored_file(
        &db,
        &config,
        auth_user.id,
        relative_path,
        flags.allow_duplicate,
        title_override,
        artist_override,
        album_override,
        &station_ids,
    )
    .await?;

    let status = if result.created { StatusCode::CREATED } else { StatusCode::OK };
    Ok((status, Json(result)))
}

pub async fn upload_zip(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    Query(flags): Query<UploadFlags>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Vec<UploadSongResult>>), AppError> {
    let mut zip_bytes: Option<Vec<u8>> = None;
    let mut assign_to_all = false;
    let mut station_ids: Vec<Uuid> = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::bad_request(format!("Invalid multipart: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                zip_bytes = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| AppError::bad_request(format!("Failed to read zip: {e}")))?
                        .to_vec(),
                );
            }
            "assign_to_all" => assign_to_all = field.text().await.unwrap_or_default() == "true",
            "station_ids" => {
                let text = field.text().await.unwrap_or_default();
                if !text.is_empty() {
                    station_ids =
                        serde_json::from_str(&text).map_err(|e| AppError::bad_request(format!("Invalid station_ids JSON: {e}")))?;
                }
            }
            _ => {}
        }
    }

    let bytes = zip_bytes.ok_or_else(|| AppError::bad_request("No zip file provided".into()))?;

    if assign_to_all {
        station_ids = crate::stations::repository::find_all_station_ids(&db)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect();
    }

    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|e| AppError::bad_request(format!("Invalid zip archive: {e}")))?;

    let entries: Vec<(String, Vec<u8>)> = {
        let mut result = Vec::new();
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).db_error("failed to process uploaded file")?;

            if entry.is_dir() || !is_audio_file(entry.name()) {
                continue;
            }

            let original_name = entry.name().to_string();
            let mut entry_bytes = Vec::new();
            entry.read_to_end(&mut entry_bytes).db_error("failed to process uploaded file")?;

            result.push((original_name, entry_bytes));
        }
        result
    };

    if entries.is_empty() {
        return Err(AppError::bad_request("No audio files found in archive".into()));
    }

    let mut results: Vec<UploadSongResult> = Vec::with_capacity(entries.len());

    for (original_name, entry_bytes) in entries {
        let relative_path = store_upload_bytes(&config.music_dir, &original_name, &entry_bytes).await?;
        let name_without_ext = match original_name.rfind('.') {
            Some(dot) => original_name[..dot].to_string(),
            None => original_name.clone(),
        };
        results.push(
            import_stored_file(
                &db,
                &config,
                auth_user.id,
                relative_path,
                flags.allow_duplicate,
                Some(name_without_ext),
                None,
                None,
                &station_ids,
            )
            .await?,
        );
    }

    let status = if results.iter().any(|r| r.created) {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(results)))
}
