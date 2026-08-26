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
use crate::songs::models::*;
use crate::songs::repository;
use crate::stations::handlers::{sync_streamer_songs, StationLifecycleLocks};
use std::sync::Arc;

pub(super) async fn assign_song_to_stations(db: &PgPool, song_id: Uuid, station_ids: &[Uuid]) -> Result<Vec<Uuid>, AppError> {
    for &sid in station_ids {
        repository::assign_song_to_station(db, sid, song_id).await?;
    }
    Ok(station_ids.to_vec())
}

pub async fn add_song_stations(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(song_id): Path<Uuid>,
    Json(req): Json<AssignStationsRequest>,
) -> Result<Json<Vec<Uuid>>, AppError> {
    let _song = repository::find_song_by_id(&db, song_id)
        .await?
        .ok_or_else(|| AppError::NotFound("Song not found".into()))?;

    let _ = _song;

    let assigned = assign_song_to_stations(&db, song_id, &req.station_ids).await?;
    Ok(Json(assigned))
}

pub async fn remove_song_station(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path((song_id, station_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, AppError> {
    repository::delete_song_from_station_queue(&db, song_id, station_id).await?;
    repository::delete_song_from_station_songs(&db, song_id, station_id).await?;

    // Same removal semantics as `remove_station_song` (station library
    // removal): reload a live runtime's queue and notify no-runtime
    // observers — a stopped subscriber must see the queue shrink.
    sync_streamer_songs(&db, &streamers, &lifecycle, &config.upload_dir, station_id, true).await?;

    Ok(StatusCode::NO_CONTENT)
}
