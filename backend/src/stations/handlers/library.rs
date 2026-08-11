use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::api::StreamersMap;
use crate::auth::middleware::AuthUser;
use crate::config::Config;
use crate::errors::AppError;
use crate::stations::models::*;
use crate::stations::queue_repo;
use crate::stations::repository;

use super::stream::{resolve_station_id, sync_streamer_songs};

pub async fn list_station_songs(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<PaginatedStationSongs>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    repository::verify_station_exists(&db, station_id).await?;

    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(10000).clamp(1, 100000);
    let offset = (page - 1) * per_page;

    let mut conn = db.acquire().await.map_err(|e| AppError::Internal(e.to_string()))?;
    let rows = repository::find_station_songs_joined_paginated(&mut conn, station_id, per_page, offset).await?;
    let total = repository::count_station_songs(&mut conn, station_id).await?;

    let songs = rows
        .into_iter()
        .map(
            |(id, song_id, title, artist, album, duration, mime_type, cover_path)| StationSongResponse {
                id,
                song_id,
                title,
                artist,
                album,
                duration,
                has_cover: !cover_path.is_empty(),
                mime_type,
            },
        )
        .collect();

    Ok(Json(PaginatedStationSongs {
        songs,
        total,
        page,
        per_page,
    }))
}

pub async fn add_station_songs(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<String>,
    Json(req): Json<AddStationSongsRequest>,
) -> Result<(StatusCode, Json<Vec<StationSongResponse>>), AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    repository::verify_station_exists(&db, station_id).await?;

    let mut tx = db.begin().await.map_err(|e| AppError::Internal(e.to_string()))?;

    for artist_name in &req.artist_names {
        repository::insert_station_songs_by_artist(&mut tx, station_id, artist_name).await?;
    }

    for sel in &req.album_selectors {
        repository::insert_station_songs_by_album(&mut tx, station_id, &sel.artist, &sel.album).await?;
    }

    for &song_id in &req.song_ids {
        repository::insert_station_song(&mut tx, station_id, song_id).await?;
    }

    let rows = repository::find_station_songs_joined(&mut tx, station_id).await?;

    tx.commit().await.map_err(|e| AppError::Internal(e.to_string()))?;

    let songs = rows
        .into_iter()
        .map(
            |(id, song_id, title, artist, album, duration, mime_type, cover_path)| StationSongResponse {
                id,
                song_id,
                title,
                artist,
                album,
                duration,
                has_cover: !cover_path.is_empty(),
                mime_type,
            },
        )
        .collect();

    Ok((StatusCode::CREATED, Json(songs)))
}

pub async fn remove_station_song(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    Path((station_id, song_id)): Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    queue_repo::delete_station_queue_by_song(&db, station_id, song_id).await?;
    repository::delete_station_song(&db, station_id, song_id).await?;

    sync_streamer_songs(&db, &streamers, &config.upload_dir, station_id, false).await?;

    Ok(StatusCode::NO_CONTENT)
}
