use axum::extract::{Path, State};
use axum::Json;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::StreamersMap;
use crate::config::Config;
use crate::errors::AppError;
use crate::stations::repository;
use crate::streamer::connection::IcecastBackend;
use crate::streamer::{SongInfo, StationStreamer};

pub(crate) async fn resolve_station_id(db: &PgPool, id_or_slug: &str) -> Result<Uuid, AppError> {
    if let Ok(uuid) = Uuid::parse_str(id_or_slug) {
        return Ok(uuid);
    }
    repository::resolve_station_id_from_slug(db, id_or_slug).await
}

async fn get_or_create_streamer(
    db: &PgPool,
    streamers: &StreamersMap,
    upload_dir: &str,
    station_id: Uuid,
    station_name: &str,
    songs: Vec<SongInfo>,
    prebuffer_bytes: i32,
) -> Arc<StationStreamer> {
    {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = map.get(&station_id) {
            return existing.clone();
        }
    }
    let streamer = StationStreamer::new(
        songs,
        station_name,
        station_id,
        db.clone(),
        prebuffer_bytes,
        upload_dir,
        Arc::new(IcecastBackend),
    )
    .await;
    let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    map.insert(station_id, streamer.clone());
    streamer
}

pub(crate) async fn sync_streamer_songs(db: &PgPool, streamers: &StreamersMap, upload_dir: &str, station_id: Uuid) -> Result<(), AppError> {
    {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        if map.get(&station_id).is_none() {
            return Ok(());
        }
    }

    let rows = repository::find_station_song_info(db, station_id).await?;
    let songs = rows
        .into_iter()
        .map(|r| SongInfo {
            file_path: crate::songs::handlers::resolve_audio_path(upload_dir, &r.0),
            title: r.1,
            artist: r.2,
            duration: r.3,
            song_id: r.4,
            position: r.5,
            cue_in: r.6,
            cue_out: r.7,
            cross_start_next: r.8,
            analyzed: r.9,
        })
        .collect();

    {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(streamer) = map.get(&station_id) {
            streamer.reload_songs(songs);
        }
    }
    if let Some(streamer) = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    } {
        streamer.trim_played_items().await;
        streamer.push_queue_update().await;
    }
    Ok(())
}

pub(crate) async fn get_or_create_streamer_for_station(
    db: &PgPool,
    streamers: &StreamersMap,
    upload_dir: &str,
    station_id: Uuid,
) -> Result<Arc<StationStreamer>, AppError> {
    {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(s) = map.get(&station_id) {
            return Ok(s.clone());
        }
    }

    let station = repository::find_station_by_id(db, station_id)
        .await?
        .ok_or_else(|| AppError::NotFound("Station not found".into()))?;

    let rows = repository::find_station_song_info(db, station_id).await?;
    let songs: Vec<SongInfo> = rows
        .into_iter()
        .map(|r| SongInfo {
            file_path: crate::songs::handlers::resolve_audio_path(upload_dir, &r.0),
            title: r.1,
            artist: r.2,
            duration: r.3,
            song_id: r.4,
            position: r.5,
            cue_in: r.6,
            cue_out: r.7,
            cross_start_next: r.8,
            analyzed: r.9,
        })
        .collect();

    if songs.is_empty() {
        tracing::info!("Station queue is empty, creating idle streamer for {station_id}");
    }

    let mount = station.mount();
    Ok(get_or_create_streamer(db, streamers, upload_dir, station_id, &mount, songs, station.prebuffer_bytes).await)
}

pub async fn stream_skip(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        streamer.skip().await;
        Ok(Json(serde_json::json!({ "ok": true, "song_index": streamer.current_song_index() })))
    } else {
        Err(AppError::BadRequest("No active stream".into()))
    }
}

pub async fn stream_play(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(streamer) = map.get(&station_id) {
        streamer.play();
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err(AppError::BadRequest("No active stream".into()))
    }
}

pub async fn stream_pause(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(streamer) = map.get(&station_id) {
        streamer.pause();
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err(AppError::BadRequest("No active stream".into()))
    }
}

pub async fn stream_stop(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(streamer) = map.remove(&station_id) {
        streamer.stop();
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err(AppError::BadRequest("No active stream".into()))
    }
}

pub async fn stream_restart(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(config): State<Config>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    {
        let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(streamer) = map.remove(&station_id) {
            streamer.stop();
        }
    }
    get_or_create_streamer_for_station(&db, &streamers, &config.upload_dir, station_id).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn stream_status(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(s) = map.get(&station_id) {
        Ok(Json(s.status_json()))
    } else {
        Ok(Json(serde_json::json!({
            "playing": false, "song_index": 0, "total": 0, "title": "", "artist": "", "duration": 0,
        })))
    }
}
