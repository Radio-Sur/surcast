use std::sync::Arc;

use axum::extract::{Path, State};
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

use super::stream::{resolve_station_id, sync_streamer_songs, StationLifecycleLocks};

async fn fetch_queue_items(db: &sqlx::PgPool, station_id: Uuid, song_ids: Option<&[Uuid]>) -> Result<Vec<QueueItemResponse>, AppError> {
    let rows = if let Some(song_ids) = song_ids {
        queue_repo::find_queue_items_by_song_ids(db, station_id, song_ids).await?
    } else {
        queue_repo::find_queue_items_all(db, station_id).await?
    };

    let items = rows
        .into_iter()
        .map(
            |(
                id,
                station_id,
                song_id,
                position,
                title,
                artist,
                album,
                duration,
                mime_type,
                cover_path,
                origin_playlist_id,
                playlist_name,
                is_auto_dj,
            )| {
                QueueItemResponse {
                    id,
                    station_id,
                    song_id,
                    position,
                    title,
                    artist,
                    album,
                    duration,
                    has_cover: !cover_path.is_empty(),
                    mime_type,
                    origin_playlist_id,
                    playlist_name,
                    is_auto_dj,
                }
            },
        )
        .collect();

    Ok(items)
}

pub async fn list_queue(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<String>,
) -> Result<Json<Vec<QueueItemResponse>>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    repository::verify_station_exists(&db, station_id).await?;
    let items = fetch_queue_items(&db, station_id, None).await?;

    Ok(Json(items))
}

pub async fn add_songs_to_queue(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path(station_id): Path<String>,
    Json(req): Json<AddToQueueRequest>,
) -> Result<(StatusCode, Json<Vec<QueueItemResponse>>), AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    repository::verify_station_exists(&db, station_id).await?;

    for &song_id in &req.song_ids {
        let in_library = repository::check_song_in_library(&db, station_id, song_id).await?;
        if !in_library {
            return Err(AppError::BadRequest(format!("Song {song_id} is not in this station's library")));
        }
    }

    queue_repo::trim_consumed_queue_items(&db, station_id).await?;
    let start_position = queue_repo::queue_next_position(&db, station_id).await?;
    queue_repo::insert_queue_items_batch(&db, station_id, &req.song_ids, start_position, req.playlist_id).await?;

    for &song_id in &req.song_ids {
        crate::songs::analysis::spawn_analysis(&db, song_id, station_id, &config.upload_dir);
    }

    let items = fetch_queue_items(&db, station_id, Some(&req.song_ids)).await?;

    sync_streamer_songs(&db, &streamers, &lifecycle, &config.upload_dir, station_id, false).await?;

    Ok((StatusCode::CREATED, Json(items)))
}

pub async fn remove_song_from_queue(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path((station_id, item_id)): Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    queue_repo::delete_queue_by_id(&db, item_id, station_id).await?;

    sync_streamer_songs(&db, &streamers, &lifecycle, &config.upload_dir, station_id, true).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn reorder_queue(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path(station_id): Path<String>,
    Json(req): Json<ReorderQueueRequest>,
) -> Result<Json<Vec<QueueItemResponse>>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    for (i, &item_id) in req.queue_item_ids.iter().enumerate() {
        queue_repo::set_queue_position(&db, item_id, i as i32).await?;
    }
    queue_repo::sync_current_song_index_after_renumber(&db, station_id).await?;

    let items = fetch_queue_items(&db, station_id, None).await?;

    sync_streamer_songs(&db, &streamers, &lifecycle, &config.upload_dir, station_id, true).await?;

    Ok(Json(items))
}

pub async fn insert_song_at_queue_position(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path(station_id): Path<String>,
    Json(req): Json<InsertIntoQueueRequest>,
) -> Result<Json<Vec<QueueItemResponse>>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;

    if req.position < 0 {
        return Err(AppError::BadRequest("Position must be non-negative".into()));
    }

    let in_library = repository::check_song_in_library(&db, station_id, req.song_id).await?;
    if !in_library {
        return Err(AppError::BadRequest(format!(
            "Song {} is not in this station's library",
            req.song_id
        )));
    }

    queue_repo::shift_queue_positions_from(&db, station_id, req.position).await?;
    queue_repo::insert_queue_item_at(&db, station_id, req.song_id, req.position).await?;
    queue_repo::sync_current_song_index_after_renumber(&db, station_id).await?;

    crate::songs::analysis::spawn_analysis(&db, req.song_id, station_id, &config.upload_dir);

    let items = fetch_queue_items(&db, station_id, None).await?;

    sync_streamer_songs(&db, &streamers, &lifecycle, &config.upload_dir, station_id, true).await?;

    Ok(Json(items))
}

pub async fn remove_playlist_songs_from_queue(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path((station_id, playlist_id)): Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;

    queue_repo::delete_queue_by_playlist(&db, station_id, playlist_id).await?;

    let ids = queue_repo::list_ordered_queue_ids(&db, station_id).await?;

    for (i, (id,)) in ids.iter().enumerate() {
        queue_repo::set_queue_position(&db, *id, i as i32).await?;
    }
    queue_repo::sync_current_song_index_after_renumber(&db, station_id).await?;

    sync_streamer_songs(&db, &streamers, &lifecycle, &config.upload_dir, station_id, true).await?;

    Ok(StatusCode::NO_CONTENT)
}
