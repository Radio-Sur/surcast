use axum::extract::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{NaiveTime, Timelike};
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::errors::AppError;
use crate::scheduling::models::*;
use crate::scheduling::repository;

fn to_sec(t: NaiveTime) -> i32 {
    t.hour() as i32 * 3600 + t.minute() as i32 * 60 + t.second() as i32
}

fn ranges_overlap(a_start: i32, a_end: i32, b_start: i32, b_end: i32) -> bool {
    let a_overnight = a_end <= a_start;
    let b_overnight = b_end <= b_start;

    match (a_overnight, b_overnight) {
        (false, false) => a_start < b_end && a_end > b_start,
        (true, false) => b_start < a_end || b_end > a_start,
        (false, true) => a_start < b_end || a_end > b_start,
        (true, true) => true,
    }
}

async fn check_schedule_overlap(
    db: &sqlx::PgPool,
    station_id: Uuid,
    day_of_week: i16,
    start_time: NaiveTime,
    end_time: NaiveTime,
    exclude_id: Option<Uuid>,
) -> Result<Vec<StationSchedule>, AppError> {
    let all = repository::find_schedules_by_station_and_day(db, station_id, day_of_week, exclude_id).await?;

    let a_start = to_sec(start_time);
    let a_end = to_sec(end_time);

    let overlapping = all
        .into_iter()
        .filter(|existing| {
            let b_start = to_sec(existing.start_time);
            let b_end = to_sec(existing.end_time);
            ranges_overlap(a_start, a_end, b_start, b_end)
        })
        .collect();

    Ok(overlapping)
}

fn validate_schedule_input(
    day_of_week: i16,
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: &SourceType,
    playlist_id: Option<Uuid>,
) -> Result<(), AppError> {
    if !(0..=6).contains(&day_of_week) {
        return Err(AppError::BadRequest("day_of_week must be 0-6".into()));
    }

    if end_time == start_time {
        return Err(AppError::BadRequest("end_time must differ from start_time".into()));
    }

    if *source_type == SourceType::Playlist && playlist_id.is_none() {
        return Err(AppError::BadRequest("playlist_id is required when source_type is playlist".into()));
    }

    Ok(())
}

async fn build_schedule_response(db: &sqlx::PgPool, row: StationSchedule) -> Result<ScheduleResponse, AppError> {
    let playlist_name = if let Some(pid) = row.playlist_id {
        repository::find_playlist_name_for_schedule(db, pid).await?
    } else {
        None
    };

    Ok(ScheduleResponse {
        id: row.id,
        station_id: row.station_id,
        day_of_week: row.day_of_week,
        start_time: row.start_time.format("%H:%M").to_string(),
        end_time: row.end_time.format("%H:%M").to_string(),
        source_type: row.source_type,
        playlist_id: row.playlist_id,
        playlist_name,
        auto_dj_mode: row.auto_dj_mode,
        auto_dj_avoid_repeat: row.auto_dj_avoid_repeat,
        auto_dj_min_gap: row.auto_dj_min_gap,
        auto_dj_songs_ahead: row.auto_dj_songs_ahead,
    })
}

pub async fn list_schedules(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<Uuid>,
) -> Result<Json<Vec<ScheduleResponse>>, AppError> {
    let rows = repository::find_schedules_for_station(&db, station_id).await?;

    let mut responses = Vec::new();
    for row in rows {
        responses.push(build_schedule_response(&db, row).await?);
    }

    Ok(Json(responses))
}

pub async fn create_schedule(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(station_id): Path<Uuid>,
    Json(req): Json<CreateScheduleRequest>,
) -> Result<(StatusCode, Json<ScheduleResponse>), AppError> {
    let start_time = NaiveTime::parse_from_str(&req.start_time, "%H:%M")
        .map_err(|_| AppError::BadRequest("Invalid start_time format, use HH:MM".into()))?;

    let end_time =
        NaiveTime::parse_from_str(&req.end_time, "%H:%M").map_err(|_| AppError::BadRequest("Invalid end_time format, use HH:MM".into()))?;

    let source_type = req.source_type.unwrap_or(SourceType::Playlist);
    validate_schedule_input(req.day_of_week, start_time, end_time, &source_type, req.playlist_id)?;

    let overlapping = check_schedule_overlap(&db, station_id, req.day_of_week, start_time, end_time, None).await?;
    if !overlapping.is_empty() {
        let names: Vec<String> = overlapping
            .iter()
            .map(|s| {
                s.playlist_id
                    .map(|pid| format!("{}", pid))
                    .unwrap_or_else(|| s.source_type.to_string())
            })
            .collect();
        return Err(AppError::Conflict(format!(
            "Time range overlaps with existing schedule(s): {}",
            names.join(", ")
        )));
    }

    let row = repository::insert_schedule(
        &db,
        station_id,
        req.day_of_week,
        start_time,
        end_time,
        &source_type,
        req.playlist_id,
        req.auto_dj_mode,
        req.auto_dj_avoid_repeat,
        req.auto_dj_min_gap,
        req.auto_dj_songs_ahead,
    )
    .await?;

    Ok((StatusCode::CREATED, Json(build_schedule_response(&db, row).await?)))
}

pub async fn update_schedule(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path((_station_id, schedule_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateScheduleRequest>,
) -> Result<Json<ScheduleResponse>, AppError> {
    let existing = repository::find_schedule_by_id(&db, schedule_id)
        .await?
        .ok_or_else(|| AppError::NotFound("Schedule not found".into()))?;

    let day_of_week = req.day_of_week.unwrap_or(existing.day_of_week);
    let start_time = match req.start_time {
        Some(ref t) => {
            NaiveTime::parse_from_str(t, "%H:%M").map_err(|_| AppError::BadRequest("Invalid start_time format, use HH:MM".into()))?
        }
        None => existing.start_time,
    };
    let end_time = match req.end_time {
        Some(ref t) => {
            NaiveTime::parse_from_str(t, "%H:%M").map_err(|_| AppError::BadRequest("Invalid end_time format, use HH:MM".into()))?
        }
        None => existing.end_time,
    };
    let source_type = req.source_type.unwrap_or(existing.source_type);
    let playlist_id = req.playlist_id.unwrap_or(existing.playlist_id);
    validate_schedule_input(day_of_week, start_time, end_time, &source_type, playlist_id)?;

    let overlapping = check_schedule_overlap(&db, existing.station_id, day_of_week, start_time, end_time, Some(schedule_id)).await?;
    if !overlapping.is_empty() {
        let names: Vec<String> = overlapping
            .iter()
            .map(|s| {
                s.playlist_id
                    .map(|pid| format!("{}", pid))
                    .unwrap_or_else(|| s.source_type.to_string())
            })
            .collect();
        return Err(AppError::Conflict(format!(
            "Time range overlaps with existing schedule(s): {}",
            names.join(", ")
        )));
    }

    let auto_dj_mode = req.auto_dj_mode.unwrap_or(existing.auto_dj_mode);
    let auto_dj_avoid_repeat = req.auto_dj_avoid_repeat.unwrap_or(existing.auto_dj_avoid_repeat);
    let auto_dj_min_gap = req.auto_dj_min_gap.unwrap_or(existing.auto_dj_min_gap);
    let auto_dj_songs_ahead = req.auto_dj_songs_ahead.unwrap_or(existing.auto_dj_songs_ahead);

    let row = repository::update_schedule(
        &db,
        schedule_id,
        day_of_week,
        start_time,
        end_time,
        &source_type,
        playlist_id,
        &auto_dj_mode,
        auto_dj_avoid_repeat,
        auto_dj_min_gap,
        auto_dj_songs_ahead,
    )
    .await?;

    Ok(Json(build_schedule_response(&db, row).await?))
}

pub async fn delete_schedule(
    Extension(_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path((_station_id, schedule_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, AppError> {
    let affected = repository::delete_schedule(&db, schedule_id).await?;

    if affected == 0 {
        return Err(AppError::NotFound("Schedule not found".into()));
    }

    Ok(StatusCode::NO_CONTENT)
}
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;

    #[test]
    fn test_to_sec_midnight() {
        let t = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        assert_eq!(to_sec(t), 0);
    }

    #[test]
    fn test_to_sec_one_thirty() {
        let t = NaiveTime::from_hms_opt(1, 30, 0).unwrap();
        assert_eq!(to_sec(t), 5400);
    }

    #[test]
    fn test_to_sec_almost_midnight() {
        let t = NaiveTime::from_hms_opt(23, 59, 0).unwrap();
        assert_eq!(to_sec(t), 86340);
    }

    #[test]
    fn test_ranges_overlap_full() {
        assert!(ranges_overlap(100, 200, 120, 180));
    }

    #[test]
    fn test_ranges_overlap_partial() {
        assert!(ranges_overlap(100, 200, 150, 250));
    }

    #[test]
    fn test_ranges_overlap_non_overlapping() {
        assert!(!ranges_overlap(100, 200, 200, 300));
        assert!(!ranges_overlap(100, 200, 300, 400));
    }

    #[test]
    fn test_ranges_overlap_adjacent() {
        assert!(!ranges_overlap(100, 200, 200, 300));
    }

    #[test]
    fn test_ranges_overlap_overnight_a() {
        assert!(ranges_overlap(36000, 7200, 0, 3600));
    }

    #[test]
    fn test_ranges_overlap_overnight_b() {
        assert!(ranges_overlap(0, 3600, 36000, 7200));
    }

    #[test]
    fn test_ranges_overlap_both_overnight() {
        assert!(ranges_overlap(36000, 7200, 40000, 6000));
    }
}
