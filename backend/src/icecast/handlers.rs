use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;
use tokio::net::TcpStream;

use crate::errors::AppError;
use crate::icecast::IcecastManager;

use super::models::{self, IcecastMode, IcecastSettingsUpdate};

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
    State(icecast_manager): State<IcecastManager>,
    Json(update): Json<IcecastSettingsUpdate>,
) -> Result<impl IntoResponse, AppError> {
    let settings = models::update_settings(&db, &update).await?;

    if settings.mode == IcecastMode::External {
        if let Err(e) = icecast_manager.stop().await {
            tracing::warn!("Failed to stop icecast during patch: {e}");
        }
    } else if settings.enabled {
        if let Err(e) = icecast_manager
            .restart(
                settings.port,
                &settings.source_password,
                &settings.admin_user,
                &settings.admin_password,
            )
            .await
        {
            tracing::warn!("Failed to restart icecast during patch: {e}");
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
