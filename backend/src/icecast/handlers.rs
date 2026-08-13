use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpStream;

use crate::api::router::StreamersMap;
use crate::errors::AppError;
use crate::icecast::IcecastManager;
use crate::streamer::StationStreamer;

use super::models::{self, IcecastMode, IcecastSettingsUpdate};

async fn quiesce_streamers(streamers: &StreamersMap) -> Vec<(Arc<StationStreamer>, bool)> {
    let active = {
        let streamers = streamers.lock().unwrap_or_else(|error| error.into_inner());
        streamers.values().cloned().collect::<Vec<_>>()
    };
    futures::future::join_all(active.into_iter().map(|streamer| async move {
        let was_playing = streamer
            .status_json()
            .await
            .map_or(false, |status| status["playing"].as_bool().unwrap_or(false));
        if was_playing {
            if let Err(error) = streamer.pause().await {
                tracing::warn!(%error, "failed to pause streamer before Icecast restart");
            }
        }
        (streamer, was_playing)
    }))
    .await
}

async fn reconnect_streamers(active: Vec<(Arc<StationStreamer>, bool)>) {
    futures::future::join_all(active.into_iter().map(|(streamer, was_playing)| async move {
        if let Err(error) = streamer.reconnect().await {
            tracing::warn!(%error, "failed to reconnect streamer after Icecast restart");
        } else if was_playing {
            if let Err(error) = streamer.play().await {
                tracing::warn!(%error, "failed to resume streamer after Icecast restart");
            }
        }
    }))
    .await;
}

pub async fn get_settings(State(db): State<PgPool>, State(icecast_manager): State<IcecastManager>) -> Result<impl IntoResponse, AppError> {
    let settings = models::get_settings(&db).await?;
    let running = if settings.mode == IcecastMode::External {
        check_external_reachable(&settings).await
    } else {
        icecast_manager.is_running_on_port(settings.port as u16).await
    };
    Ok(Json(json!({
        "settings": settings,
        "running": running,
    })))
}

pub async fn patch_settings(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(icecast_manager): State<IcecastManager>,
    Json(update): Json<IcecastSettingsUpdate>,
) -> Result<impl IntoResponse, AppError> {
    let settings = models::update_settings(&db, &update).await?;

    if settings.mode == IcecastMode::External {
        if let Err(e) = icecast_manager.stop().await {
            tracing::warn!("Failed to stop icecast during patch: {e}");
        }
    } else if settings.enabled {
        let active = quiesce_streamers(&streamers).await;
        if let Err(error) = icecast_manager
            .restart(
                settings.port,
                &settings.source_password,
                &settings.admin_user,
                &settings.admin_password,
            )
            .await
        {
            tracing::warn!(%error, "failed to restart Icecast during patch");
        } else {
            reconnect_streamers(active).await;
        }
    } else {
        if let Err(e) = icecast_manager.stop().await {
            tracing::warn!("Failed to stop icecast during patch: {e}");
        }
    }

    let running = if settings.mode == IcecastMode::External {
        check_external_reachable(&settings).await
    } else {
        icecast_manager.is_running_on_port(settings.port as u16).await
    };
    Ok(Json(json!({
        "settings": settings,
        "running": running,
    })))
}

pub async fn start_icecast(State(db): State<PgPool>, State(icecast_manager): State<IcecastManager>) -> Result<impl IntoResponse, AppError> {
    let settings = models::get_settings(&db).await?;
    if settings.mode == IcecastMode::External {
        return Err(AppError::BadRequest("Cannot start Icecast in external mode".into()));
    }
    let _ = models::update_settings(
        &db,
        &IcecastSettingsUpdate {
            enabled: Some(true),
            ..Default::default()
        },
    )
    .await;
    match icecast_manager
        .start(
            settings.port,
            &settings.source_password,
            &settings.admin_user,
            &settings.admin_password,
        )
        .await
    {
        Ok(msg) => Ok(Json(json!({ "ok": true, "message": msg }))),
        Err(msg) => Err(AppError::BadRequest(msg)),
    }
}

pub async fn stop_icecast(State(icecast_manager): State<IcecastManager>) -> Result<impl IntoResponse, AppError> {
    match icecast_manager.stop().await {
        Ok(msg) => Ok(Json(json!({ "ok": true, "message": msg }))),
        Err(msg) => Err(AppError::BadRequest(msg)),
    }
}

pub async fn test_connection(
    State(db): State<PgPool>,
    State(icecast_manager): State<IcecastManager>,
) -> Result<impl IntoResponse, AppError> {
    let settings = models::get_settings(&db).await?;
    if settings.mode == IcecastMode::External {
        let reachable = check_external_reachable(&settings).await;
        if reachable {
            Ok(Json(json!({ "ok": true, "message": "External Icecast is reachable" })))
        } else {
            Err(AppError::BadRequest("External Icecast is not reachable".into()))
        }
    } else {
        let running = icecast_manager.is_running_on_port(settings.port as u16).await;
        if running {
            Ok(Json(json!({ "ok": true, "message": "Icecast is running" })))
        } else {
            Err(AppError::BadRequest("Icecast is not running".into()))
        }
    }
}

async fn check_external_reachable(settings: &models::IcecastSettings) -> bool {
    let addr = match settings.external_url.as_deref() {
        Some(url) => {
            let url = url.trim_start_matches("http://").trim_start_matches("https://");
            url.split('/').next().unwrap_or(url)
        }
        None => return false,
    };
    tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .ok()
        .and_then(|r| r.ok())
        .is_some()
}
