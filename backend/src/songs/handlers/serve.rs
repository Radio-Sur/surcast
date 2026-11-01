use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use sqlx::PgPool;
use uuid::Uuid;

use super::upload_helper::resolve_audio_path;
use crate::config::Config;
use crate::errors::AppError;
use crate::songs::repository;

pub async fn serve_song_file(State(db): State<PgPool>, State(config): State<Config>, Path(id): Path<Uuid>) -> Result<Response, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::not_found("Song not found".into()))?;

    let song_path = resolve_audio_path(&config.music_dir, &song.file_path);
    let content = tokio::fs::read(&song_path)
        .await
        .map_err(|e| AppError::not_found(format!("File not found on disk: {e}")))?;

    Ok(([(header::CONTENT_TYPE, song.mime_type.clone())], content).into_response())
}

/// Serves cover art read straight from the audio file's tags. Nothing is
/// cached on disk; a strong ETag lets browsers keep it for a day.
pub async fn serve_song_cover(
    State(db): State<PgPool>,
    State(config): State<Config>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::not_found("Song not found".into()))?;

    if song.missing_since.is_some() || !song.has_cover {
        return Err(AppError::not_found("No cover art".into()));
    }

    let etag = format!("\"{}\"", song.audio_hash.clone().unwrap_or_else(|| song.id.to_string()));
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if if_none_match.split(',').any(|tag| tag.trim() == etag) {
            return Ok(StatusCode::NOT_MODIFIED.into_response());
        }
    }

    let audio_path = resolve_audio_path(&config.music_dir, &song.file_path);
    let cover = crate::songs::cover::read_embedded_cover(audio_path)
        .await
        .ok_or_else(|| AppError::not_found("No cover art".into()))?;

    Ok((
        [
            (header::CONTENT_TYPE, cover.mime),
            (header::ETAG, etag),
            (header::CACHE_CONTROL, "public, max-age=86400".to_string()),
        ],
        cover.data,
    )
        .into_response())
}
