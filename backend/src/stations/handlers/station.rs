use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::errors::AppError;
use crate::stations::models::*;
use crate::stations::repository;

use super::stream::resolve_station_id;

/// Valid transition modes. Anything else is rejected.
const TRANSITION_MODES: [&str; 3] = ["crossfade", "autocue", "off"];

fn normalize_transition_mode(value: Option<String>) -> Result<String, AppError> {
    let mode = value.unwrap_or_else(|| "crossfade".to_string());
    if TRANSITION_MODES.contains(&mode.as_str()) {
        Ok(mode)
    } else {
        Err(AppError::BadRequest(format!(
            "Invalid transition_mode '{mode}', expected one of: {}",
            TRANSITION_MODES.join(", ")
        )))
    }
}

pub async fn list_stations(State(db): State<PgPool>) -> Result<Json<Vec<StationResponse>>, AppError> {
    let stations = repository::find_all_stations(&db).await?;
    Ok(Json(stations.into_iter().map(Into::into).collect()))
}

pub async fn get_station(State(db): State<PgPool>, Path(id): Path<String>) -> Result<Json<StationResponse>, AppError> {
    let id = resolve_station_id(&db, &id).await?;
    let station = repository::find_station_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::NotFound("Station not found".into()))?;
    Ok(Json(station.into()))
}

pub async fn create_station(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Json(req): Json<CreateStationRequest>,
) -> Result<(StatusCode, Json<StationResponse>), AppError> {
    if req.name.is_empty() {
        return Err(AppError::BadRequest("Station name is required".into()));
    }

    let station_id = Uuid::new_v4();
    let slug = slugify(&req.name);
    let prebuffer_bytes = req.prebuffer_bytes.unwrap_or(16384);
    let played_limit = req.played_limit.unwrap_or(100).clamp(0, 500);
    let default_fade_ms = req.default_fade_ms.unwrap_or(3000).clamp(0, 15000);
    let transition_mode = normalize_transition_mode(req.transition_mode)?;
    let autocue_fade_max_ms = req.autocue_fade_max_ms.unwrap_or(5000).clamp(0, 15000);
    repository::insert_station(
        &db,
        &repository::CreateStationParams {
            id: station_id,
            name: req.name.clone(),
            description: req.description.clone().unwrap_or_default(),
            slug: slug.clone(),
            stream_url: req.stream_url.clone(),
            prebuffer_bytes,
            played_limit,
            default_fade_ms,
            transition_mode,
            autocue_fade_max_ms,
            created_by: auth_user.id,
        },
    )
    .await?;

    let station = repository::find_station_by_id(&db, station_id).await?.ok_or_else(|| {
        tracing::error!("Insert succeeded but fetch returned None");
        AppError::Internal("".into())
    })?;

    Ok((StatusCode::CREATED, Json(station.into())))
}

pub async fn update_station(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id): Path<String>,
    Json(req): Json<UpdateStationRequest>,
) -> Result<Json<StationResponse>, AppError> {
    let id = resolve_station_id(&db, &id).await?;
    let station = repository::find_station_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::NotFound("Station not found".into()))?;

    let name = req.name.as_deref().unwrap_or(&station.name).to_string();
    let description = req.description.unwrap_or(station.description);
    let slug = slugify(&name);
    let stream_url = req.stream_url.or(station.stream_url);
    let prebuffer_bytes = req.prebuffer_bytes.unwrap_or(station.prebuffer_bytes);
    let played_limit = req.played_limit.unwrap_or(station.played_limit).clamp(0, 500);
    let default_fade_ms = req.default_fade_ms.unwrap_or(station.default_fade_ms).clamp(0, 15000);
    let transition_mode = normalize_transition_mode(req.transition_mode.or(Some(station.transition_mode)))?;
    let autocue_fade_max_ms = req.autocue_fade_max_ms.unwrap_or(station.autocue_fade_max_ms).clamp(0, 15000);

    repository::update_station_fields(
        &db,
        &repository::UpdateStationParams {
            id,
            name,
            description,
            slug,
            stream_url,
            prebuffer_bytes,
            played_limit,
            default_fade_ms,
            transition_mode,
            autocue_fade_max_ms,
        },
    )
    .await?;

    let updated = repository::find_station_by_id(&db, id).await?.ok_or_else(|| {
        tracing::error!("Update succeeded but fetch returned None");
        AppError::Internal("".into())
    })?;

    Ok(Json(updated.into()))
}

pub async fn delete_station(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let id = resolve_station_id(&db, &id).await?;
    repository::delete_station(&db, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

use crate::util;

pub async fn get_station_playlist_m3u(
    Extension(_auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<String>,
) -> Result<(StatusCode, [(&'static str, &'static str); 1], String), AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let station = repository::find_station_by_id(&db, station_id)
        .await?
        .ok_or_else(|| AppError::NotFound("Station not found".into()))?;

    let (addr, _) = crate::icecast::models::get_connection_config(&db).await.map_err(|e| {
        tracing::error!("Failed to get icecast config: {e:?}");
        {
            tracing::error!("Failed to connect to stream server");
            AppError::Internal("".into())
        }
    })?;
    let mut playlist = String::from("#EXTM3U\n");
    playlist.push_str(&format!("#EXTINF:-1,{} Stream\n", station.name));
    let mount_name = station.mount();
    playlist.push_str(&format!("http://{}/{}\n", addr, util::url_encode(&mount_name)));

    Ok((StatusCode::OK, [("Content-Type", "audio/x-mpegurl")], playlist))
}
