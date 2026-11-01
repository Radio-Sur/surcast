use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use sqlx::PgPool;
use uuid::Uuid;

use super::upload_helper::resolve_audio_path;

/// Unlinks a song's audio file only when no other song row still references
/// it (files are shared on forced duplicates). Also drops the scanner
/// tracking row.
async fn unlink_song_files(db: &PgPool, config: &Config, song: &crate::songs::models::Song) {
    if repository::count_songs_with_file_path(db, &song.file_path, song.id)
        .await
        .unwrap_or(1)
        == 0
    {
        let song_path = resolve_audio_path(&config.music_dir, &song.file_path);
        let _ = tokio::fs::remove_file(&song_path).await;
    }
    let _ = repository::delete_library_file(db, &song.file_path).await;
}
use crate::api::StreamersMap;
use crate::auth::middleware::AuthUser;
use crate::config::Config;
use crate::errors::AppError;
use crate::errors::DbResult;
use crate::songs::models::*;
use crate::songs::repository;
use crate::stations::handlers::{sync_station_queues, StationLifecycleLocks};
use std::sync::Arc;

pub async fn list_songs(Extension(_auth_user): Extension<AuthUser>, State(db): State<PgPool>) -> Result<Json<Vec<SongResponse>>, AppError> {
    let songs = repository::find_all_songs(&db).await?;

    let mut results = Vec::new();
    for song in songs {
        let station_ids: Vec<Uuid> = repository::find_station_ids_for_song(&db, song.id)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect();

        results.push(SongResponse::from((song, station_ids)));
    }

    Ok(Json(results))
}

/// Lazy library scan: imports files dropped into the music directory
/// (FTP/manual copies), follows renames by content hash and flags deleted
/// files as missing. Fast for steady state (only new/changed files are
/// read); never runs autocue analysis.
pub async fn scan_library_endpoint(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
) -> Result<Json<crate::songs::scan::ScanReport>, AppError> {
    let report = crate::songs::scan::scan_library(&db, &config.music_dir, config.lastfm_api_key.as_deref()).await?;
    Ok(Json(report))
}

pub async fn search_songs(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Query(params): Query<SongSearchParams>,
) -> Result<Json<PaginatedSongs>, AppError> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    let offset = (page - 1) * per_page;

    let songs = repository::find_songs_search(
        &db,
        params.q.as_deref(),
        params.artist.as_deref(),
        params.album.as_deref(),
        per_page,
        offset,
    )
    .await?;

    let total = repository::count_songs_search(&db, params.q.as_deref(), params.artist.as_deref(), params.album.as_deref()).await?;

    let mut results = Vec::new();
    for song in songs {
        let station_ids: Vec<Uuid> = repository::find_station_ids_for_song(&db, song.id)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect();
        results.push(SongResponse::from((song, station_ids)));
    }

    Ok(Json(PaginatedSongs {
        songs: results,
        total,
        page,
        per_page,
    }))
}

pub async fn list_artists(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Query(params): Query<ArtistParams>,
) -> Result<Json<PaginatedArtists>, AppError> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    let offset = (page - 1) * per_page;

    let artists = repository::find_artists(&db, params.q.as_deref(), per_page, offset).await?;
    let total = repository::count_artists(&db, params.q.as_deref()).await?;

    Ok(Json(PaginatedArtists {
        artists,
        total,
        page,
        per_page,
    }))
}

pub async fn count_songs(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Json(req): Json<CountSongsRequest>,
) -> Result<Json<CountSongsResponse>, AppError> {
    let (album_artists, album_names): (Vec<String>, Vec<String>) =
        req.album_selectors.into_iter().map(|sel| (sel.artist, sel.album)).unzip();

    let count = repository::count_songs_by_selectors(&db, &req.artist_names, &album_artists, &album_names, &req.exclude_ids).await?;

    Ok(Json(CountSongsResponse { count }))
}

pub async fn get_song(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<Json<SongResponse>, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::not_found("Song not found".into()))?;

    let station_ids: Vec<Uuid> = repository::find_station_ids_for_song(&db, song.id)
        .await?
        .into_iter()
        .map(|r| r.0)
        .collect();

    Ok(Json(SongResponse::from((song, station_ids))))
}

pub async fn update_song(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateSongRequest>,
) -> Result<Json<SongResponse>, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::not_found("Song not found".into()))?;

    let title = req.title.unwrap_or(song.title);
    let artist = req.artist.unwrap_or(song.artist);
    let album = req.album.unwrap_or(song.album);
    let duration = req.duration.unwrap_or(song.duration);

    repository::update_song_fields(&db, id, &title, &artist, &album, duration).await?;

    let updated = repository::find_song_by_id(&db, id).await?.ok_or_else(|| {
        tracing::error!("Update succeeded but fetch returned None");
        AppError::internal("".into())
    })?;

    let station_ids: Vec<Uuid> = repository::find_station_ids_for_song(&db, updated.id)
        .await?
        .into_iter()
        .map(|r| r.0)
        .collect();

    Ok(Json(SongResponse::from((updated, station_ids))))
}

pub async fn delete_song(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let song = repository::find_song_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::not_found("Song not found".into()))?;

    // The affected stations come from the rows the destructive operation
    // actually removed (DELETE ... RETURNING) — never from a pre-delete
    // snapshot. The song row is locked inside the same transaction, so a
    // concurrent enqueue cannot slip a queue row past the returned set.
    let affected = repository::delete_song_globally(&db, id).await?;

    unlink_song_files(&db, &config, &song).await;

    sync_station_queues(&db, &streamers, &lifecycle, &config.music_dir, affected, true).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_songs_batch(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Json(req): Json<BatchDeleteSongsRequest>,
) -> Result<StatusCode, AppError> {
    let songs = sqlx::query_as::<_, crate::songs::models::Song>("SELECT * FROM songs WHERE id = ANY($1)")
        .bind(&req.ids)
        .fetch_all(&db)
        .await
        .db_error("failed to fetch songs for batch delete")?;

    // Same destructive semantics as the single delete: affected stations
    // come from the actually deleted rows, deduplicated by DISTINCT.
    let affected = repository::delete_songs_globally(&db, &req.ids).await?;

    for song in &songs {
        unlink_song_files(&db, &config, song).await;
    }

    sync_station_queues(&db, &streamers, &lifecycle, &config.music_dir, affected, true).await?;

    Ok(StatusCode::NO_CONTENT)
}
