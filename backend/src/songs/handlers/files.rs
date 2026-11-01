use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::api::StreamersMap;
use crate::auth::middleware::AuthUser;
use crate::config::Config;
use crate::errors::AppError;
use crate::songs::details::{self, TrackDetails};
use crate::songs::files::{self, DirectoryListing, FileSort, SortOrder};
use crate::songs::models::SongResponse;
use crate::songs::repository;
use crate::stations::handlers::{sync_station_queues, StationLifecycleLocks};

#[derive(Debug, Deserialize)]
pub struct ListFilesParams {
    pub path: Option<String>,
    pub sort: Option<String>,
    pub order: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PathQuery {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateFolderRequest {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct MoveRequest {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Serialize)]
pub struct DeletedFilesResponse {
    pub deleted: i64,
}

#[derive(Debug, Serialize)]
pub struct SongDetailsResponse {
    #[serde(flatten)]
    pub song: SongResponse,
    pub extension: Option<String>,
    pub track: Option<TrackDetails>,
}

/// Browse the music directory.
pub async fn list_files(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    Query(params): Query<ListFilesParams>,
) -> Result<Json<DirectoryListing>, AppError> {
    let path = params.path.unwrap_or_default();
    let sort = FileSort::parse(params.sort.as_deref());
    let order = SortOrder::parse(params.order.as_deref());
    let listing = files::list_directory(&db, &config.music_dir, &path, sort, order).await?;
    Ok(Json(listing))
}

/// All folders under the library (used by the move picker).
pub async fn list_folders(Extension(_auth_user): Extension<AuthUser>, State(config): State<Config>) -> Result<Json<Vec<String>>, AppError> {
    Ok(Json(files::list_all_folders(&config.music_dir).await?))
}

pub async fn create_folder(
    Extension(_auth_user): Extension<AuthUser>,
    State(config): State<Config>,
    Json(req): Json<CreateFolderRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    let path = files::create_folder(&config.music_dir, &req.path).await?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "path": path }))))
}

/// Deletes a folder and everything below it: files from disk, their song rows
/// (and queue entries) and the scanner tracking rows.
pub async fn delete_folder(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Query(query): Query<PathQuery>,
) -> Result<Json<DeletedFilesResponse>, AppError> {
    let rel = files::validate_relative_path(&query.path)?;
    if rel.is_empty() {
        return Err(AppError::bad_request("Cannot delete the library root".into()));
    }

    let songs = repository::find_songs_under_path(&db, &rel).await?;
    let ids: Vec<Uuid> = songs.iter().map(|song| song.id).collect();

    files::delete_folder_on_disk(&config.music_dir, &rel).await?;

    let affected = repository::delete_songs_globally(&db, &ids).await?;
    repository::delete_library_files_under_path(&db, &rel).await?;
    sync_station_queues(&db, &streamers, &lifecycle, &config.music_dir, affected, true).await?;

    Ok(Json(DeletedFilesResponse { deleted: ids.len() as i64 }))
}

pub async fn delete_file(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Query(query): Query<PathQuery>,
) -> Result<Json<DeletedFilesResponse>, AppError> {
    let rel = files::validate_relative_path(&query.path)?;
    if rel.is_empty() {
        return Err(AppError::bad_request("Cannot delete the library root".into()));
    }

    let songs = repository::find_songs_by_paths(&db, std::slice::from_ref(&rel)).await?;
    let ids: Vec<Uuid> = songs.iter().map(|song| song.id).collect();

    files::delete_file_on_disk(&config.music_dir, &rel).await?;

    let affected = repository::delete_songs_globally(&db, &ids).await?;
    repository::delete_library_file(&db, &rel).await?;
    sync_station_queues(&db, &streamers, &lifecycle, &config.music_dir, affected, true).await?;

    Ok(Json(DeletedFilesResponse { deleted: ids.len() as i64 }))
}

/// Move or rename a file/folder. Song paths and scanner tracking follow.
pub async fn move_entry(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    Json(req): Json<MoveRequest>,
) -> Result<StatusCode, AppError> {
    files::move_entry(&db, &config.music_dir, &req.from, &req.to).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Removes every song flagged missing from the database (their files are
/// already gone) and clears them out of station queues.
pub async fn purge_missing(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
) -> Result<Json<DeletedFilesResponse>, AppError> {
    let songs = repository::find_all_missing_songs(&db).await?;
    let ids: Vec<Uuid> = songs.iter().map(|song| song.id).collect();

    let affected = repository::delete_songs_globally(&db, &ids).await?;
    for song in &songs {
        let _ = repository::delete_library_file(&db, &song.file_path).await;
    }
    sync_station_queues(&db, &streamers, &lifecycle, &config.music_dir, affected, true).await?;

    Ok(Json(DeletedFilesResponse { deleted: ids.len() as i64 }))
}

pub async fn get_song_details(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> Result<Json<SongDetailsResponse>, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::not_found("Song not found".into()))?;

    let station_ids: Vec<Uuid> = repository::find_station_ids_for_song(&db, song.id)
        .await?
        .into_iter()
        .map(|row| row.0)
        .collect();

    let extension = std::path::Path::new(&song.file_path)
        .extension()
        .map(|ext| ext.to_string_lossy().to_lowercase());
    let track = details::read_track_details(&config.music_dir, &song.file_path).await;

    Ok(Json(SongDetailsResponse {
        song: SongResponse::from((song, station_ids)),
        extension,
        track,
    }))
}
