use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use sqlx::PgPool;
use uuid::Uuid;

use super::upload_helper::{resolve_audio_path, resolve_cover_path};
use crate::config::Config;
use crate::errors::AppError;
use crate::songs::repository;

pub async fn serve_song_file(State(db): State<PgPool>, State(config): State<Config>, Path(id): Path<Uuid>) -> Result<Response, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::NotFound("Song not found".into()))?;

    let song_path = resolve_audio_path(&config.upload_dir, &song.file_path);
    let content = tokio::fs::read(&song_path)
        .await
        .map_err(|e| AppError::NotFound(format!("File not found on disk: {e}")))?;

    Ok(([(axum::http::header::CONTENT_TYPE, song.mime_type.clone())], content).into_response())
}

pub async fn serve_song_cover(State(db): State<PgPool>, State(config): State<Config>, Path(id): Path<Uuid>) -> Result<Response, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::NotFound("Song not found".into()))?;

    if song.cover_path.is_empty() {
        return Err(AppError::NotFound("No cover art".into()));
    }

    let cover_path = resolve_cover_path(&config.upload_dir, &song.cover_path);
    let content = tokio::fs::read(&cover_path)
        .await
        .map_err(|_| AppError::NotFound("Cover file not found on disk".into()))?;

    let mime = if cover_path.ends_with(".png") { "image/png" } else { "image/jpeg" };

    Ok(([(axum::http::header::CONTENT_TYPE, mime)], content).into_response())
}
