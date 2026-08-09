use axum::extract::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{NaiveTime, Timelike};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::config::Config;
use crate::errors::AppError;
use crate::scheduling::models::*;
use crate::scheduling::repository;
use crate::scheduling::service::fill_queue_from_schedule;

pub fn time_to_sec(t: NaiveTime) -> i32 {
    t.hour() as i32 * 3600 + t.minute() as i32 * 60 + t.second() as i32
}

pub fn times_overlap(a_start: NaiveTime, a_end: NaiveTime, b_start: NaiveTime, b_end: NaiveTime) -> bool {
    let a_s = time_to_sec(a_start);
    let a_e = time_to_sec(a_end);
    let b_s = time_to_sec(b_start);
    let b_e = time_to_sec(b_end);

    let a_overnight = a_e <= a_s;
    let b_overnight = b_e <= b_s;

    match (a_overnight, b_overnight) {
        (false, false) => a_s < b_e && a_e > b_s,
        (true, false) => b_s < a_e || b_e > a_s,
        (false, true) => a_s < b_e || a_e > b_s,
        (true, true) => true,
    }
}

async fn get_auto_fill_response(db: &PgPool, station_id: Uuid) -> Result<AutoFillResponse, AppError> {
    let config = repository::find_auto_fill_config(db, station_id).await?.unwrap_or(StationAutoFill {
        station_id,
        enabled: true,
        mode: AutoDjMode::Random,
        source_type: SourceType::StationLibrary,
        source_playlist_id: None,
        avoid_artist_repeat: true,
        min_song_gap: 3,
        songs_ahead: 4,
    });

    let source_playlist_name = if let Some(pid) = config.source_playlist_id {
        repository::find_playlist_name(db, pid).await?
    } else {
        None
    };

    let weighted = repository::find_auto_fill_playlists(db, station_id).await?;
    let mut weighted_playlists = Vec::new();
    for row in weighted {
        let playlist_name = repository::find_playlist_name_string(db, row.playlist_id).await?;

        weighted_playlists.push(AutoFillPlaylistResponse {
            id: row.id,
            playlist_id: row.playlist_id,
            playlist_name,
            weight: row.weight,
        });
    }

    Ok(AutoFillResponse {
        station_id,
        enabled: config.enabled,
        mode: config.mode,
        source_type: config.source_type,
        source_playlist_id: config.source_playlist_id,
        source_playlist_name,
        avoid_artist_repeat: config.avoid_artist_repeat,
        min_song_gap: config.min_song_gap,
        songs_ahead: config.songs_ahead,
        weighted_playlists,
    })
}

pub async fn get_auto_fill(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<Uuid>,
) -> Result<Json<AutoFillResponse>, AppError> {
    Ok(Json(get_auto_fill_response(&db, station_id).await?))
}

pub async fn update_auto_fill(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<Uuid>,
    Json(req): Json<UpdateAutoFillRequest>,
) -> Result<Json<AutoFillResponse>, AppError> {
    let current = repository::find_auto_fill_config(&db, station_id).await?;

    let enabled = req.enabled.unwrap_or(current.as_ref().map(|c| c.enabled).unwrap_or(true));
    let mode = req
        .mode
        .unwrap_or_else(|| current.as_ref().map(|c| c.mode.clone()).unwrap_or(AutoDjMode::Random));
    let source_type = req.source_type.unwrap_or_else(|| {
        current
            .as_ref()
            .map(|c| c.source_type.clone())
            .unwrap_or(SourceType::StationLibrary)
    });
    let source_playlist_id = req
        .source_playlist_id
        .unwrap_or(current.as_ref().and_then(|c| c.source_playlist_id));
    let avoid_artist_repeat = req
        .avoid_artist_repeat
        .unwrap_or(current.as_ref().map(|c| c.avoid_artist_repeat).unwrap_or(true));
    let min_song_gap = req.min_song_gap.unwrap_or(current.as_ref().map(|c| c.min_song_gap).unwrap_or(3));
    let songs_ahead = req.songs_ahead.unwrap_or(current.as_ref().map(|c| c.songs_ahead).unwrap_or(4));

    repository::upsert_auto_fill_config(
        &db,
        station_id,
        enabled,
        &mode,
        &source_type,
        source_playlist_id,
        avoid_artist_repeat,
        min_song_gap,
        songs_ahead,
    )
    .await?;

    Ok(Json(get_auto_fill_response(&db, station_id).await?))
}

pub async fn add_auto_fill_playlist(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<Uuid>,
    Json(req): Json<AddAutoFillPlaylistRequest>,
) -> Result<(StatusCode, Json<AutoFillPlaylistResponse>), AppError> {
    let weight = req.weight.unwrap_or(1);

    let row = repository::insert_auto_fill_playlist(&db, station_id, req.playlist_id, weight).await?;

    let playlist_name = repository::find_playlist_name_string(&db, row.playlist_id).await?;

    Ok((
        StatusCode::CREATED,
        Json(AutoFillPlaylistResponse {
            id: row.id,
            playlist_id: row.playlist_id,
            playlist_name,
            weight: row.weight,
        }),
    ))
}

pub async fn update_auto_fill_playlist(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path((_station_id, playlist_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateAutoFillPlaylistRequest>,
) -> Result<Json<AutoFillPlaylistResponse>, AppError> {
    let row = repository::update_auto_fill_playlist_weight(&db, playlist_id, req.weight.unwrap_or(1))
        .await?
        .ok_or_else(|| AppError::NotFound("Auto-fill playlist not found".into()))?;

    let playlist_name = repository::find_playlist_name_string(&db, row.playlist_id).await?;

    Ok(Json(AutoFillPlaylistResponse {
        id: row.id,
        playlist_id: row.playlist_id,
        playlist_name,
        weight: row.weight,
    }))
}

pub async fn delete_auto_fill_playlist(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path((_station_id, playlist_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, AppError> {
    let affected = repository::delete_auto_fill_playlist(&db, playlist_id).await?;

    if affected == 0 {
        return Err(AppError::NotFound("Auto-fill playlist not found".into()));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn trigger_auto_fill(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    Path(station_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let sid: Uuid = station_id.parse().map_err(|_| AppError::BadRequest("Invalid station ID".into()))?;
    fill_queue_from_schedule(&db, sid, None, &config.upload_dir).await?;
    Ok(Json(json!({ "status": "ok" })))
}
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;

    #[test]
    fn test_time_to_sec_midnight() {
        let t = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        assert_eq!(time_to_sec(t), 0);
    }

    #[test]
    fn test_time_to_sec_one_thirty() {
        let t = NaiveTime::from_hms_opt(1, 30, 0).unwrap();
        assert_eq!(time_to_sec(t), 5400);
    }

    #[test]
    fn test_time_to_sec_almost_midnight() {
        let t = NaiveTime::from_hms_opt(23, 59, 0).unwrap();
        assert_eq!(time_to_sec(t), 86340);
    }

    #[test]
    fn test_times_overlap_full() {
        let a_s = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let a_e = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let b_s = NaiveTime::from_hms_opt(10, 30, 0).unwrap();
        let b_e = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        assert!(times_overlap(a_s, a_e, b_s, b_e));
    }

    #[test]
    fn test_times_overlap_partial() {
        let a_s = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let a_e = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let b_s = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        let b_e = NaiveTime::from_hms_opt(13, 0, 0).unwrap();
        assert!(times_overlap(a_s, a_e, b_s, b_e));
    }

    #[test]
    fn test_times_overlap_non_overlapping() {
        let a_s = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let a_e = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let b_s = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let b_e = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
        assert!(!times_overlap(a_s, a_e, b_s, b_e));
    }

    #[test]
    fn test_times_overlap_overnight_a() {
        let a_s = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
        let a_e = NaiveTime::from_hms_opt(2, 0, 0).unwrap();
        let b_s = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let b_e = NaiveTime::from_hms_opt(1, 0, 0).unwrap();
        assert!(times_overlap(a_s, a_e, b_s, b_e));
    }

    #[test]
    fn test_times_overlap_overnight_b() {
        let a_s = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let a_e = NaiveTime::from_hms_opt(1, 0, 0).unwrap();
        let b_s = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
        let b_e = NaiveTime::from_hms_opt(2, 0, 0).unwrap();
        assert!(times_overlap(a_s, a_e, b_s, b_e));
    }
}
