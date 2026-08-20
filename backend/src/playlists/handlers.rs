use std::sync::Arc;

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
use crate::playlists::models::*;
use crate::playlists::repository;
use crate::stations::handlers::stream::StationLifecycleLocks;
use crate::stations::queue_repo;

fn song_has_cover(cover_path: &str) -> bool {
    !cover_path.is_empty()
}

fn build_response(p: &Playlist, count: i64, total_dur: i64) -> PlaylistResponse {
    PlaylistResponse {
        id: p.id,
        name: p.name.clone(),
        description: p.description.clone(),
        slug: p.slug.clone().unwrap_or_default(),
        song_count: count,
        total_duration_seconds: total_dur,
        created_by: p.created_by,
        created_at: p.created_at,
        updated_at: p.updated_at,
    }
}

pub async fn list_playlists(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
) -> Result<Json<Vec<PlaylistResponse>>, AppError> {
    let playlists = repository::find_all_playlists(&db).await?;

    let mut results = Vec::new();
    for p in playlists {
        let (count, total_dur) = repository::playlist_song_stats(&db, p.id).await?;
        results.push(build_response(&p, count, total_dur));
    }

    Ok(Json(results))
}

pub async fn create_playlist(
    Extension(user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Json(req): Json<CreatePlaylistRequest>,
) -> Result<(StatusCode, Json<PlaylistResponse>), AppError> {
    let id = Uuid::new_v4();
    let description = req.description.unwrap_or_default();
    let slug = slugify(&req.name);

    repository::insert_playlist(&db, id, &req.name, &description, &slug, user.id).await?;

    let playlist = repository::find_playlist_by_id(&db, id).await?.ok_or_else(|| {
        tracing::error!("Insert succeeded but fetch returned None");
        AppError::Internal("".into())
    })?;

    Ok((StatusCode::CREATED, Json(build_response(&playlist, 0, 0))))
}

pub async fn get_playlist(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id_or_slug): Path<String>,
) -> Result<Json<PlaylistResponse>, AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let playlist = repository::find_playlist_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::NotFound("Playlist not found".into()))?;

    let (count, total_dur) = repository::playlist_song_stats(&db, playlist.id).await?;

    Ok(Json(build_response(&playlist, count, total_dur)))
}

pub async fn update_playlist(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id_or_slug): Path<String>,
    Json(req): Json<UpdatePlaylistRequest>,
) -> Result<Json<PlaylistResponse>, AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let playlist = repository::find_playlist_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::NotFound("Playlist not found".into()))?;

    let name = req.name.unwrap_or(playlist.name);
    let description = req.description.unwrap_or(playlist.description);
    let slug = slugify(&name);

    repository::update_playlist_fields(&db, id, &name, &description, &slug).await?;

    let updated = repository::find_playlist_by_id(&db, id).await?.ok_or_else(|| {
        tracing::error!("Update succeeded but fetch returned None");
        AppError::Internal("".into())
    })?;

    let (count, total_dur) = repository::playlist_song_stats(&db, updated.id).await?;

    Ok(Json(build_response(&updated, count, total_dur)))
}

pub async fn delete_playlist(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id_or_slug): Path<String>,
) -> Result<StatusCode, AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let affected = repository::delete_playlist(&db, id).await?;

    if affected == 0 {
        return Err(AppError::NotFound("Playlist not found".into()));
    }

    Ok(StatusCode::NO_CONTENT)
}

fn build_playlist_song(r: (Uuid, Uuid, Uuid, i32, String, String, String, i32, String)) -> PlaylistSongResponse {
    PlaylistSongResponse {
        id: r.0,
        playlist_id: r.1,
        song_id: r.2,
        position: r.3,
        title: r.4,
        artist: r.5,
        album: r.6,
        duration: r.7,
        has_cover: song_has_cover(&r.8),
        mime_type: "audio/mpeg".to_string(),
    }
}

pub async fn list_playlist_songs(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id_or_slug): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<PaginatedPlaylistSongs>, AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let exists = repository::playlist_exists(&db, id).await?;
    if !exists {
        return Err(AppError::NotFound("Playlist not found".into()));
    }

    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(10000).clamp(1, 100000);
    let offset = (page - 1) * per_page;

    let mut conn = db.acquire().await.map_err(|e| AppError::Internal(e.to_string()))?;
    let songs = repository::find_playlist_songs_with_details_paginated(&mut conn, id, per_page, offset).await?;
    let total = repository::count_playlist_songs(&mut conn, id).await?;
    let songs = songs.into_iter().map(build_playlist_song).collect();

    Ok(Json(PaginatedPlaylistSongs {
        songs,
        total,
        page,
        per_page,
    }))
}

pub async fn add_playlist_songs(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id_or_slug): Path<String>,
    Json(req): Json<AddPlaylistSongsRequest>,
) -> Result<(StatusCode, Json<Vec<PlaylistSongResponse>>), AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let exists = repository::playlist_exists(&db, id).await?;
    if !exists {
        return Err(AppError::NotFound("Playlist not found".into()));
    }

    let mut tx = db.begin().await.map_err(|e| AppError::Internal(e.to_string()))?;

    let mut pos = repository::compute_max_playlist_position(&mut tx, id).await?;

    for artist_name in &req.artist_names {
        let added = repository::insert_playlist_songs_by_artist(&mut tx, id, artist_name, pos).await?;
        pos += added;
    }

    for sel in &req.album_selectors {
        let added = repository::insert_playlist_songs_by_album(&mut tx, id, &sel.artist, &sel.album, pos).await?;
        pos += added;
    }

    for &song_id in &req.song_ids {
        repository::insert_playlist_song(&mut tx, id, song_id, pos).await?;
        pos += 1;
    }

    let songs = repository::find_playlist_songs_with_details(&mut tx, id).await?;
    let songs = songs.into_iter().map(build_playlist_song).collect();

    tx.commit().await.map_err(|e| AppError::Internal(e.to_string()))?;

    Ok((StatusCode::CREATED, Json(songs)))
}

pub async fn remove_playlist_song(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path((id_or_slug, song_id)): Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let mut conn = db.acquire().await.map_err(|e| AppError::Internal(e.to_string()))?;
    repository::delete_playlist_song(&mut conn, id, song_id).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_playlist_songs_batch(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id_or_slug): Path<String>,
    Json(req): Json<BatchRemovePlaylistSongsRequest>,
) -> Result<StatusCode, AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let mut conn = db.acquire().await.map_err(|e| AppError::Internal(e.to_string()))?;
    repository::delete_playlist_songs_batch(&mut conn, id, &req.song_ids).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn reorder_playlist_songs(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id_or_slug): Path<String>,
    Json(req): Json<ReorderPlaylistSongsRequest>,
) -> Result<Json<Vec<PlaylistSongResponse>>, AppError> {
    let id = repository::resolve_playlist_id(&db, &id_or_slug).await?;

    let mut tx = db.begin().await.map_err(|e| AppError::Internal(e.to_string()))?;

    repository::reorder_playlist_songs(&mut tx, id, &req.song_ids).await?;

    let songs = repository::find_playlist_songs_with_details(&mut tx, id).await?;
    let songs = songs.into_iter().map(build_playlist_song).collect();

    tx.commit().await.map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(songs))
}

pub async fn add_playlist_to_queue(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path((playlist_id_or_slug, station_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    let playlist_id = repository::resolve_playlist_id(&db, &playlist_id_or_slug).await?;
    let station_id = crate::stations::handlers::resolve_station_id(&db, &station_id).await?;
    let exists = repository::playlist_exists(&db, playlist_id).await?;
    if !exists {
        return Err(AppError::NotFound("Playlist not found".into()));
    }

    let song_ids = repository::find_playlist_song_ids(&db, playlist_id).await?;
    queue_repo::trim_consumed_queue_items(&db, station_id).await?;
    let pos = queue_repo::queue_next_position(&db, station_id).await?;
    queue_repo::insert_queue_items_batch(&db, station_id, &song_ids, pos, Some(playlist_id)).await?;

    for &song_id in &song_ids {
        crate::songs::analysis::spawn_analysis(&db, song_id, station_id, &config.upload_dir);
    }

    crate::stations::handlers::sync_streamer_songs(&db, &streamers, &lifecycle, &config.upload_dir, station_id, false).await?;

    Ok((StatusCode::CREATED, Json(serde_json::json!({ "added": song_ids.len() as i32 }))))
}
