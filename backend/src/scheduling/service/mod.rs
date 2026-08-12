pub mod auto_fill;
pub mod recurrence;

pub use recurrence::*;

use chrono::{Datelike, Local, NaiveDate, NaiveTime, Timelike};
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::{AppError, DbResult};
use crate::scheduling::models::{AutoDjMode, RecurrenceType, SourceType};

#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct StationScheduleRow {
    id: Uuid,
    day_of_week: i16,
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: SourceType,
    playlist_id: Option<Uuid>,
    auto_dj_mode: Option<AutoDjMode>,
    auto_dj_avoid_repeat: Option<bool>,
    auto_dj_min_gap: Option<i32>,
    auto_dj_songs_ahead: Option<i32>,
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct StationScheduleEventRow {
    id: Uuid,
    title: String,
    start_date: NaiveDate,
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: SourceType,
    playlist_id: Option<Uuid>,
    auto_dj_mode: Option<AutoDjMode>,
    auto_dj_avoid_repeat: Option<bool>,
    auto_dj_min_gap: Option<i32>,
    auto_dj_songs_ahead: Option<i32>,
    recurrence_type: RecurrenceType,
    recurrence_interval: Option<i32>,
    recurrence_days: Option<String>,
    recurrence_end_date: Option<NaiveDate>,
    recurrence_count: Option<i32>,
}

#[allow(dead_code)]
struct ActiveSchedule {
    pub source_type: SourceType,
    pub playlist_id: Option<Uuid>,
    pub auto_dj_mode: Option<AutoDjMode>,
    pub auto_dj_avoid_repeat: Option<bool>,
    pub auto_dj_min_gap: Option<i32>,
    pub auto_dj_songs_ahead: Option<i32>,
}

fn find_active_schedule(schedules: &[StationScheduleRow], current_day: i16, current_time: NaiveTime) -> Option<ActiveSchedule> {
    let current_seconds = current_time.hour() as i32 * 3600 + current_time.minute() as i32 * 60 + current_time.second() as i32;

    let mut best: Option<(i32, &StationScheduleRow)> = None;

    for entry in schedules {
        if entry.day_of_week != current_day {
            continue;
        }

        let start_seconds = entry.start_time.hour() as i32 * 3600 + entry.start_time.minute() as i32 * 60;
        let end_seconds = entry.end_time.hour() as i32 * 3600 + entry.end_time.minute() as i32 * 60;

        let is_active = if end_seconds > start_seconds {
            current_seconds >= start_seconds && current_seconds < end_seconds
        } else {
            current_seconds >= start_seconds || current_seconds < end_seconds
        };

        if is_active {
            match best {
                Some((best_start, _)) if start_seconds > best_start => {
                    best = Some((start_seconds, entry));
                }
                None => {
                    best = Some((start_seconds, entry));
                }
                _ => {}
            }
        }
    }

    best.map(|(_, entry)| ActiveSchedule {
        source_type: entry.source_type.clone(),
        playlist_id: entry.playlist_id,
        auto_dj_mode: entry.auto_dj_mode.clone(),
        auto_dj_avoid_repeat: entry.auto_dj_avoid_repeat,
        auto_dj_min_gap: entry.auto_dj_min_gap,
        auto_dj_songs_ahead: entry.auto_dj_songs_ahead,
    })
}

async fn fill_from_schedule_entry(
    db: &PgPool,
    station_id: Uuid,
    source_type: &SourceType,
    playlist_id: Option<Uuid>,
    auto_dj_mode: Option<AutoDjMode>,
    auto_dj_avoid_repeat: Option<bool>,
    auto_dj_min_gap: Option<i32>,
    auto_dj_songs_ahead: Option<i32>,
    upcoming_count: Option<i64>,
    upload_dir: &str,
) -> Result<(), AppError> {
    match source_type {
        SourceType::Playlist => {
            if let Some(pid) = playlist_id {
                if auto_dj_mode.is_some() {
                    let config = self::auto_fill::AutoFillConfig {
                        source_type: SourceType::Playlist,
                        source_playlist_id: Some(pid),
                        mode: auto_dj_mode.unwrap_or_default(),
                        avoid_repeat: auto_dj_avoid_repeat.unwrap_or(true),
                        min_gap: auto_dj_min_gap.unwrap_or(3),
                        songs_ahead: auto_dj_songs_ahead.unwrap_or(5),
                    };
                    self::auto_fill::fill_from_auto_dj_source(db, station_id, &config, upcoming_count, upload_dir).await?;
                } else {
                    self::auto_fill::fill_from_playlist(db, station_id, pid, auto_dj_songs_ahead, upload_dir).await?;
                }
            }
        }
        SourceType::StationLibrary | SourceType::GlobalLibrary | SourceType::WeightedPlaylists => {
            let config = self::auto_fill::AutoFillConfig {
                source_type: source_type.clone(),
                source_playlist_id: None,
                mode: auto_dj_mode.unwrap_or(AutoDjMode::Random),
                avoid_repeat: auto_dj_avoid_repeat.unwrap_or(true),
                min_gap: auto_dj_min_gap.unwrap_or(3),
                songs_ahead: auto_dj_songs_ahead.unwrap_or(5),
            };
            self::auto_fill::fill_from_auto_dj_source(db, station_id, &config, upcoming_count, upload_dir).await?;
        }
    }
    Ok(())
}

pub async fn fill_queue_from_schedule(
    db: &PgPool,
    station_id: Uuid,
    upcoming_count: Option<i64>,
    upload_dir: &str,
) -> Result<(), AppError> {
    let now = Local::now();
    let current_day = now.weekday().num_days_from_monday() as i16;
    let current_time = now.time();
    let today = now.date_naive();

    let schedules = sqlx::query_as::<_, StationScheduleRow>(
        "SELECT id, day_of_week, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead FROM station_schedules WHERE station_id = $1 ORDER BY day_of_week, start_time",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to load station schedules")?;

    let active = find_active_schedule(&schedules, current_day, current_time);

    if let Some(active) = active {
        fill_from_schedule_entry(
            db,
            station_id,
            &active.source_type,
            active.playlist_id,
            active.auto_dj_mode,
            active.auto_dj_avoid_repeat,
            active.auto_dj_min_gap,
            active.auto_dj_songs_ahead,
            upcoming_count,
            upload_dir,
        )
        .await?;
        return Ok(());
    }

    let events = sqlx::query_as::<_, StationScheduleEventRow>(
        "SELECT id, title, start_date, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, recurrence_type, recurrence_interval, recurrence_days, recurrence_end_date, recurrence_count FROM station_schedule_events WHERE station_id = $1 ORDER BY start_date, start_time",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to load station events")?;

    for event in &events {
        if !self::recurrence::matches_recurrence(
            today,
            event.start_date,
            &event.recurrence_type,
            event.recurrence_interval,
            event.recurrence_days.as_deref(),
            event.recurrence_end_date,
            event.recurrence_count,
        ) {
            continue;
        }

        let current_secs = current_time.hour() as i32 * 3600 + current_time.minute() as i32 * 60 + current_time.second() as i32;
        let start_secs = event.start_time.hour() as i32 * 3600 + event.start_time.minute() as i32 * 60;
        let end_secs = event.end_time.hour() as i32 * 3600 + event.end_time.minute() as i32 * 60;

        let is_active = if end_secs > start_secs {
            current_secs >= start_secs && current_secs < end_secs
        } else {
            current_secs >= start_secs || current_secs < end_secs
        };

        if is_active {
            fill_from_schedule_entry(
                db,
                station_id,
                &event.source_type,
                event.playlist_id,
                event.auto_dj_mode.clone(),
                event.auto_dj_avoid_repeat,
                event.auto_dj_min_gap,
                event.auto_dj_songs_ahead,
                upcoming_count,
                upload_dir,
            )
            .await?;
            return Ok(());
        }
    }

    self::auto_fill::fill_from_auto_config(db, station_id, upcoming_count, upload_dir).await?;

    Ok(())
}
