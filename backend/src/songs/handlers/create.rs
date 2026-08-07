use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use sqlx::PgPool;
use std::io::Read;
use uuid::Uuid;

use super::assign::assign_song_to_stations;
use super::upload_helper::is_audio_file;
use crate::auth::middleware::AuthUser;
use crate::config::Config;
use crate::errors::{AppError, DbResult};
use crate::songs::models::*;
use crate::songs::repository;
use crate::songs::upload::process_song_upload;

pub async fn upload_song(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<SongResponse>), AppError> {
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
        .map_err(|e| AppError::BadRequest(format!("Invalid multipart: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                original_name = field.file_name().unwrap_or("unknown").to_string();
                file_bytes = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| AppError::BadRequest(format!("Failed to read file: {e}")))?
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
                        serde_json::from_str(&text).map_err(|e| AppError::BadRequest(format!("Invalid station_ids JSON: {e}")))?;
                }
            }
            _ => {}
        }
    }

    let bytes = file_bytes.ok_or_else(|| AppError::BadRequest("No file provided".into()))?;

    let title_override = if title.is_empty() { None } else { Some(title.as_str()) };
    let artist_override = if artist.is_empty() { None } else { Some(artist.as_str()) };
    let album_override = if album.is_empty() { None } else { Some(album.as_str()) };

    let processed = process_song_upload(
        &db,
        &original_name,
        &bytes,
        &config.upload_dir,
        auth_user.id,
        config.lastfm_api_key.as_deref(),
        title_override,
        artist_override,
        album_override,
    )
    .await?;

    if assign_to_all {
        station_ids = crate::stations::repository::find_all_station_ids(&db)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect();
    }

    let assigned = assign_song_to_stations(&db, processed.id, &station_ids).await?;

    let song = repository::find_song_by_id(&db, processed.id).await?.ok_or_else(|| {
        tracing::error!("Insert succeeded but fetch returned None");
        AppError::Internal("".into())
    })?;

    Ok((StatusCode::CREATED, Json(SongResponse::from((song, assigned)))))
}

pub async fn upload_zip(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Vec<SongResponse>>), AppError> {
    let mut zip_bytes: Option<Vec<u8>> = None;
    let mut assign_to_all = false;
    let mut station_ids: Vec<Uuid> = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(format!("Invalid multipart: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                zip_bytes = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| AppError::BadRequest(format!("Failed to read zip: {e}")))?
                        .to_vec(),
                );
            }
            "assign_to_all" => assign_to_all = field.text().await.unwrap_or_default() == "true",
            "station_ids" => {
                let text = field.text().await.unwrap_or_default();
                if !text.is_empty() {
                    station_ids =
                        serde_json::from_str(&text).map_err(|e| AppError::BadRequest(format!("Invalid station_ids JSON: {e}")))?;
                }
            }
            _ => {}
        }
    }

    let bytes = zip_bytes.ok_or_else(|| AppError::BadRequest("No zip file provided".into()))?;

    if assign_to_all {
        station_ids = crate::stations::repository::find_all_station_ids(&db)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect();
    }

    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|e| AppError::BadRequest(format!("Invalid zip archive: {e}")))?;

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
        return Err(AppError::BadRequest("No audio files found in archive".into()));
    }

    let mut created: Vec<SongResponse> = Vec::with_capacity(entries.len());

    for (original_name, entry_bytes) in entries {
        let name_without_ext = match original_name.rfind('.') {
            Some(dot) => original_name[..dot].to_string(),
            None => original_name.clone(),
        };

        let processed = process_song_upload(
            &db,
            &original_name,
            &entry_bytes,
            &config.upload_dir,
            auth_user.id,
            config.lastfm_api_key.as_deref(),
            Some(&name_without_ext),
            None,
            None,
        )
        .await?;

        let assigned = assign_song_to_stations(&db, processed.id, &station_ids).await?;

        let song = repository::find_song_by_id(&db, processed.id).await?.ok_or_else(|| {
            tracing::error!("Insert succeeded but fetch returned None");
            AppError::Internal("".into())
        })?;

        created.push(SongResponse::from((song, assigned)));
    }

    Ok((StatusCode::CREATED, Json(created)))
}
