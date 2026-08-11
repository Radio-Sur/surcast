use axum::extract::{Path, State};
use axum::Json;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::StreamersMap;
use crate::config::Config;
use crate::errors::AppError;
use crate::stations::models::Station;
use crate::stations::repository;
use crate::streamer::gstreamer::GStreamerPipelineFactory;
use crate::streamer::pipeline::StationPlaybackConfig;
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
) -> Result<Arc<StationStreamer>, AppError> {
    if let Some(existing) = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    } {
        existing.play().await.map_err(|error| {
            tracing::error!(station_id = %station_id, %error, "stream playback failed");
            AppError::Internal("Stream playback failed".into())
        })?;
        return Ok(existing);
    }
    let streamer = StationStreamer::new(
        songs,
        station_name,
        station_id,
        db.clone(),
        prebuffer_bytes,
        upload_dir,
        Arc::new(GStreamerPipelineFactory::default()),
    )
    .await
    .map_err(|error| {
        tracing::error!(station_id = %station_id, error = %error, "GStreamer pipeline initialization failed");
        AppError::Internal("Stream initialization failed".into())
    })?;
    let winner = {
        let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(station_id).or_insert_with(|| streamer.clone()).clone()
    };
    winner.play().await.map_err(|error| {
        tracing::error!(station_id = %station_id, %error, "stream playback failed");
        AppError::Internal("Stream playback failed".into())
    })?;
    Ok(winner)
}

pub(crate) async fn sync_streamer_songs(
    db: &PgPool,
    streamers: &StreamersMap,
    upload_dir: &str,
    station_id: Uuid,
    align_next: bool,
) -> Result<(), AppError> {
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
            queue_item_id: r.4,
            song_id: r.5,
            position: r.6,
            cue_in: r.7,
            cue_out: r.8,
            cross_start_next: r.9,
            analyzed: r.10,
        })
        .collect();

    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        streamer
            .reload_songs(songs, align_next)
            .await
            .map_err(|_| AppError::Internal("Stream reload failed".into()))?;
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

pub(crate) async fn sync_streamer_playback_config(streamers: &StreamersMap, station: &Station) -> Result<(), AppError> {
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|error| error.into_inner());
        map.get(&station.id).cloned()
    };
    let Some(streamer) = streamer else {
        return Ok(());
    };

    let config = StationPlaybackConfig::from_persisted(
        &station.transition_mode,
        station.default_fade_ms,
        station.autocue_fade_max_ms,
        station.prebuffer_bytes,
    )
    .map_err(|error| {
        tracing::error!(station_id = %station.id, %error, "invalid persisted playback configuration");
        AppError::Internal("Stream configuration failed".into())
    })?;
    streamer.update_config(config).await.map_err(|error| {
        tracing::error!(station_id = %station.id, %error, "stream configuration update failed");
        AppError::Internal("Stream configuration failed".into())
    })
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
            queue_item_id: r.4,
            song_id: r.5,
            position: r.6,
            cue_in: r.7,
            cue_out: r.8,
            cross_start_next: r.9,
            analyzed: r.10,
        })
        .collect();

    if songs.is_empty() {
        tracing::info!("Station queue is empty, creating idle streamer for {station_id}");
    }
    let mount = station.mount();

    get_or_create_streamer(db, streamers, upload_dir, station_id, &mount, songs, station.prebuffer_bytes).await
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
        streamer.skip().await.map_err(|_| AppError::Internal("Stream skip failed".into()))?;
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
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        streamer
            .play()
            .await
            .map_err(|_| AppError::Internal("Stream playback failed".into()))?;
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
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        streamer
            .pause()
            .await
            .map_err(|_| AppError::Internal("Stream pause failed".into()))?;
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
    let streamer = {
        let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.remove(&station_id)
    };
    if let Some(streamer) = streamer {
        streamer.stop().await.map_err(|_| AppError::Internal("Stream stop failed".into()))?;
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
    let stopped = {
        let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.remove(&station_id)
    };
    if let Some(streamer) = stopped {
        streamer.stop().await.map_err(|_| AppError::Internal("Stream stop failed".into()))?;
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
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        Ok(Json(streamer.status_json().await))
    } else {
        Ok(Json(serde_json::json!({
            "playing": false, "song_index": 0, "total": 0, "elapsed": 0, "title": "", "artist": "", "duration": 0,
        })))
    }
}
