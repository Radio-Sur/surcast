use axum::extract::Extension;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{Local, NaiveDate, NaiveTime};
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

use super::auto_fill::times_overlap;
use crate::auth::middleware::AuthUser;
use crate::errors::AppError;
use crate::scheduling::models::*;
use crate::scheduling::repository;
use crate::scheduling::service::matches_recurrence;

struct EventOverlapParams {
    db: PgPool,
    station_id: Uuid,
    start_date: NaiveDate,
    start_time: NaiveTime,
    end_time: NaiveTime,
    recurrence_type: RecurrenceType,
    recurrence_interval: Option<i32>,
    recurrence_days: Option<String>,
    recurrence_end_date: Option<NaiveDate>,
    recurrence_count: Option<i32>,
    exclude_id: Option<Uuid>,
}

fn validate_event_input(
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: &SourceType,
    playlist_id: Option<Uuid>,
    _recurrence_type: &RecurrenceType,
) -> Result<(), AppError> {
    if end_time == start_time {
        return Err(AppError::BadRequest("end_time must differ from start_time".into()));
    }

    if *source_type == SourceType::Playlist && playlist_id.is_none() {
        return Err(AppError::BadRequest("playlist_id is required when source_type is playlist".into()));
    }

    Ok(())
}

async fn check_event_overlap(params: &EventOverlapParams) -> Result<(), AppError> {
    let all = repository::find_all_events_for_station(&params.db, params.station_id).await?;

    if all.is_empty() {
        return Ok(());
    }

    let exclude = params.exclude_id.unwrap_or_default();
    let mut conflicts = Vec::new();

    for event in &all {
        if event.id == exclude {
            continue;
        }

        for day_offset in 0..365 {
            let date = params.start_date + chrono::Duration::days(day_offset);

            if !matches_recurrence(
                date,
                params.start_date,
                &params.recurrence_type,
                params.recurrence_interval,
                params.recurrence_days.as_deref(),
                params.recurrence_end_date,
                params.recurrence_count,
            ) {
                continue;
            }
            if !matches_recurrence(
                date,
                event.start_date,
                &event.recurrence_type,
                event.recurrence_interval,
                event.recurrence_days.as_deref(),
                event.recurrence_end_date,
                event.recurrence_count,
            ) {
                continue;
            }

            if times_overlap(params.start_time, params.end_time, event.start_time, event.end_time) {
                let label = event.title.as_deref().unwrap_or("playlist");
                let detail = format!(
                    "{} ({}–{} on {})",
                    label,
                    event.start_time.format("%H:%M"),
                    event.end_time.format("%H:%M"),
                    date
                );
                conflicts.push(detail);
                break;
            }
        }
    }

    if !conflicts.is_empty() {
        return Err(AppError::Conflict(format!(
            "Time range overlaps with existing event(s): {}",
            conflicts.join(", ")
        )));
    }

    Ok(())
}

async fn build_event_response(db: &sqlx::PgPool, row: StationScheduleEvent) -> Result<ScheduleEventResponse, AppError> {
    let playlist_name = if let Some(pid) = row.playlist_id {
        repository::find_playlist_name_for_schedule(db, pid).await?
    } else {
        None
    };

    let recurrence_days: Option<Vec<i32>> = row
        .recurrence_days
        .as_ref()
        .map(|s| s.split(',').filter_map(|v| v.trim().parse::<i32>().ok()).collect());

    Ok(ScheduleEventResponse {
        id: row.id,
        station_id: row.station_id,
        title: row.title,
        start_date: row.start_date.format("%Y-%m-%d").to_string(),
        start_time: row.start_time.format("%H:%M").to_string(),
        end_time: row.end_time.format("%H:%M").to_string(),
        source_type: row.source_type,
        playlist_id: row.playlist_id,
        playlist_name,
        auto_dj_mode: row.auto_dj_mode,
        auto_dj_avoid_repeat: row.auto_dj_avoid_repeat,
        auto_dj_min_gap: row.auto_dj_min_gap,
        auto_dj_songs_ahead: row.auto_dj_songs_ahead,
        recurrence_type: row.recurrence_type,
        recurrence_interval: row.recurrence_interval,
        recurrence_days,
        recurrence_end_date: row.recurrence_end_date.map(|d| d.format("%Y-%m-%d").to_string()),
        recurrence_count: row.recurrence_count,
        created_at: row.created_at,
    })
}

pub async fn list_schedule_events(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<Uuid>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<ScheduleEventResponse>>, AppError> {
    let _from = params
        .get("from")
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        .unwrap_or_else(|| Local::now().date_naive());
    let _to = params
        .get("to")
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        .unwrap_or_else(|| _from + chrono::Duration::days(7));

    let rows = repository::find_events_for_station(&db, station_id).await?;

    let mut responses = Vec::new();
    for row in rows {
        responses.push(build_event_response(&db, row).await?);
    }

    Ok(Json(responses))
}

pub async fn create_schedule_event(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<Uuid>,
    Json(req): Json<CreateScheduleEventRequest>,
) -> Result<(StatusCode, Json<ScheduleEventResponse>), AppError> {
    let start_date = NaiveDate::parse_from_str(&req.start_date, "%Y-%m-%d")
        .map_err(|_| AppError::BadRequest("Invalid start_date format, use YYYY-MM-DD".into()))?;

    let start_time = NaiveTime::parse_from_str(&req.start_time, "%H:%M")
        .map_err(|_| AppError::BadRequest("Invalid start_time format, use HH:MM".into()))?;

    let end_time =
        NaiveTime::parse_from_str(&req.end_time, "%H:%M").map_err(|_| AppError::BadRequest("Invalid end_time format, use HH:MM".into()))?;

    let source_type = req.source_type.unwrap_or(SourceType::Playlist);
    let recurrence_type = req.recurrence_type.unwrap_or(RecurrenceType::None);
    validate_event_input(start_time, end_time, &source_type, req.playlist_id, &recurrence_type)?;

    let recurrence_days_str = req
        .recurrence_days
        .as_ref()
        .map(|days| days.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(","));

    let recurrence_end_date = match req.recurrence_end_date {
        Some(ref d) => Some(
            NaiveDate::parse_from_str(d, "%Y-%m-%d")
                .map_err(|_| AppError::BadRequest("Invalid recurrence_end_date format, use YYYY-MM-DD".into()))?,
        ),
        None => None,
    };

    check_event_overlap(&EventOverlapParams {
        db: db.clone(),
        station_id,
        start_date,
        start_time,
        end_time,
        recurrence_type: recurrence_type.clone(),
        recurrence_interval: req.recurrence_interval,
        recurrence_days: recurrence_days_str.clone(),
        recurrence_end_date,
        recurrence_count: req.recurrence_count,
        exclude_id: None,
    })
    .await?;

    let row = repository::insert_event(
        &db,
        station_id,
        &req.title,
        start_date,
        start_time,
        end_time,
        &source_type,
        req.playlist_id,
        req.auto_dj_mode,
        req.auto_dj_avoid_repeat,
        req.auto_dj_min_gap,
        req.auto_dj_songs_ahead,
        &recurrence_type,
        req.recurrence_interval,
        recurrence_days_str,
        recurrence_end_date,
        req.recurrence_count,
    )
    .await?;

    Ok((StatusCode::CREATED, Json(build_event_response(&db, row).await?)))
}

pub async fn update_schedule_event(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path((_station_id, event_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateScheduleEventRequest>,
) -> Result<Json<ScheduleEventResponse>, AppError> {
    let existing = repository::find_event_by_id(&db, event_id)
        .await?
        .ok_or_else(|| AppError::NotFound("Schedule event not found".into()))?;

    let start_date = match req.start_date {
        Some(ref d) => NaiveDate::parse_from_str(d, "%Y-%m-%d").map_err(|_| AppError::BadRequest("Invalid start_date format".into()))?,
        None => existing.start_date,
    };

    let start_time = match req.start_time {
        Some(ref t) => NaiveTime::parse_from_str(t, "%H:%M").map_err(|_| AppError::BadRequest("Invalid start_time".into()))?,
        None => existing.start_time,
    };

    let end_time = match req.end_time {
        Some(ref t) => NaiveTime::parse_from_str(t, "%H:%M").map_err(|_| AppError::BadRequest("Invalid end_time".into()))?,
        None => existing.end_time,
    };

    let source_type = req.source_type.unwrap_or(existing.source_type);
    let playlist_id = req.playlist_id.unwrap_or(existing.playlist_id);
    let recurrence_type = req.recurrence_type.unwrap_or(existing.recurrence_type);
    validate_event_input(start_time, end_time, &source_type, playlist_id, &recurrence_type)?;

    let recurrence_interval = req.recurrence_interval.or(existing.recurrence_interval);

    let recurrence_days_str = match req.recurrence_days {
        Some(Some(ref d)) => Some(d.clone()),
        Some(None) => None,
        None => existing.recurrence_days,
    };

    let recurrence_end_date = match req.recurrence_end_date {
        Some(Some(ref d)) => {
            Some(NaiveDate::parse_from_str(d, "%Y-%m-%d").map_err(|_| AppError::BadRequest("Invalid recurrence_end_date".into()))?)
        }
        Some(None) => None,
        None => existing.recurrence_end_date,
    };

    let recurrence_count = req.recurrence_count.or(existing.recurrence_count);

    check_event_overlap(&EventOverlapParams {
        db: db.clone(),
        station_id: existing.station_id,
        start_date,
        start_time,
        end_time,
        recurrence_type: recurrence_type.clone(),
        recurrence_interval,
        recurrence_days: recurrence_days_str.clone(),
        recurrence_end_date,
        recurrence_count,
        exclude_id: Some(existing.id),
    })
    .await?;

    let row = repository::update_event(
        &db,
        event_id,
        req.title.or(existing.title),
        start_date,
        start_time,
        end_time,
        &source_type,
        playlist_id,
        req.auto_dj_mode.unwrap_or(existing.auto_dj_mode),
        req.auto_dj_avoid_repeat.unwrap_or(existing.auto_dj_avoid_repeat),
        req.auto_dj_min_gap.unwrap_or(existing.auto_dj_min_gap),
        req.auto_dj_songs_ahead.unwrap_or(existing.auto_dj_songs_ahead),
        &recurrence_type,
        recurrence_interval,
        recurrence_days_str,
        recurrence_end_date,
        recurrence_count,
    )
    .await?;

    Ok(Json(build_event_response(&db, row).await?))
}

pub async fn delete_schedule_event(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path((_station_id, event_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, AppError> {
    let affected = repository::delete_event(&db, event_id).await?;

    if affected == 0 {
        return Err(AppError::NotFound("Schedule event not found".into()));
    }

    Ok(StatusCode::NO_CONTENT)
}
