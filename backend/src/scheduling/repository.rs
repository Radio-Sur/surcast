use chrono::{NaiveDate, NaiveTime};
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::{AppError, DbResult};
use crate::scheduling::models::*;

pub async fn find_schedules_for_station(db: &PgPool, station_id: Uuid) -> Result<Vec<StationSchedule>, AppError> {
    sqlx::query_as::<_, StationSchedule>(
        "SELECT id, station_id, day_of_week, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, created_at FROM station_schedules WHERE station_id = $1 ORDER BY day_of_week, start_time",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to list schedules")
}

pub async fn find_schedule_by_id(db: &PgPool, id: Uuid) -> Result<Option<StationSchedule>, AppError> {
    sqlx::query_as::<_, StationSchedule>(
        "SELECT id, station_id, day_of_week, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, created_at FROM station_schedules WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(db)
    .await
    .db_error("failed to find schedule for update")
}

pub async fn find_schedules_by_station_and_day(
    db: &PgPool,
    station_id: Uuid,
    day_of_week: i16,
    exclude_id: Option<Uuid>,
) -> Result<Vec<StationSchedule>, AppError> {
    sqlx::query_as::<_, StationSchedule>(
        "SELECT id, station_id, day_of_week, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, created_at FROM station_schedules WHERE station_id = $1 AND day_of_week = $2 AND id != COALESCE($3, '00000000-0000-0000-0000-000000000000')",
    )
    .bind(station_id)
    .bind(day_of_week)
    .bind(exclude_id.unwrap_or_default())
    .fetch_all(db)
    .await
    .db_error("failed to check schedule overlap")
}

pub async fn insert_schedule(
    db: &PgPool,
    station_id: Uuid,
    day_of_week: i16,
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: &SourceType,
    playlist_id: Option<Uuid>,
    auto_dj_mode: Option<AutoDjMode>,
    auto_dj_avoid_repeat: Option<bool>,
    auto_dj_min_gap: Option<i32>,
    auto_dj_songs_ahead: Option<i32>,
) -> Result<StationSchedule, AppError> {
    sqlx::query_as::<_, StationSchedule>(
        "INSERT INTO station_schedules (station_id, day_of_week, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id, station_id, day_of_week, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, created_at",
    )
    .bind(station_id)
    .bind(day_of_week)
    .bind(start_time)
    .bind(end_time)
    .bind(source_type)
    .bind(playlist_id)
    .bind(auto_dj_mode)
    .bind(auto_dj_avoid_repeat)
    .bind(auto_dj_min_gap)
    .bind(auto_dj_songs_ahead)
    .fetch_one(db)
    .await
    .db_error("failed to create schedule")
}

pub async fn update_schedule(
    db: &PgPool,
    id: Uuid,
    day_of_week: i16,
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: &SourceType,
    playlist_id: Option<Uuid>,
    auto_dj_mode: &Option<AutoDjMode>,
    auto_dj_avoid_repeat: Option<bool>,
    auto_dj_min_gap: Option<i32>,
    auto_dj_songs_ahead: Option<i32>,
) -> Result<StationSchedule, AppError> {
    sqlx::query_as::<_, StationSchedule>(
        "UPDATE station_schedules SET day_of_week = $1, start_time = $2, end_time = $3, source_type = $4, playlist_id = $5, auto_dj_mode = $6, auto_dj_avoid_repeat = $7, auto_dj_min_gap = $8, auto_dj_songs_ahead = $9 WHERE id = $10 RETURNING id, station_id, day_of_week, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, created_at",
    )
    .bind(day_of_week)
    .bind(start_time)
    .bind(end_time)
    .bind(source_type)
    .bind(playlist_id)
    .bind(auto_dj_mode)
    .bind(auto_dj_avoid_repeat)
    .bind(auto_dj_min_gap)
    .bind(auto_dj_songs_ahead)
    .bind(id)
    .fetch_one(db)
    .await
    .db_error("failed to update schedule")
}

pub async fn delete_schedule(db: &PgPool, id: Uuid) -> Result<u64, AppError> {
    let result = sqlx::query("DELETE FROM station_schedules WHERE id = $1")
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to delete schedule")?;
    Ok(result.rows_affected())
}

pub async fn find_events_for_station(db: &PgPool, station_id: Uuid) -> Result<Vec<StationScheduleEvent>, AppError> {
    sqlx::query_as::<_, StationScheduleEvent>(
        "SELECT id, station_id, title, start_date, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, recurrence_type, recurrence_interval, recurrence_days, recurrence_end_date, recurrence_count, created_at, updated_at FROM station_schedule_events WHERE station_id = $1 ORDER BY start_date, start_time",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to list schedule events")
}

pub async fn find_event_by_id(db: &PgPool, id: Uuid) -> Result<Option<StationScheduleEvent>, AppError> {
    sqlx::query_as::<_, StationScheduleEvent>(
        "SELECT id, station_id, title, start_date, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, recurrence_type, recurrence_interval, recurrence_days, recurrence_end_date, recurrence_count, created_at, updated_at FROM station_schedule_events WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(db)
    .await
    .db_error("failed to find event for update")
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_event(
    db: &PgPool,
    station_id: Uuid,
    title: &Option<String>,
    start_date: NaiveDate,
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: &SourceType,
    playlist_id: Option<Uuid>,
    auto_dj_mode: Option<AutoDjMode>,
    auto_dj_avoid_repeat: Option<bool>,
    auto_dj_min_gap: Option<i32>,
    auto_dj_songs_ahead: Option<i32>,
    recurrence_type: &RecurrenceType,
    recurrence_interval: Option<i32>,
    recurrence_days: Option<String>,
    recurrence_end_date: Option<NaiveDate>,
    recurrence_count: Option<i32>,
) -> Result<StationScheduleEvent, AppError> {
    sqlx::query_as::<_, StationScheduleEvent>(
        "INSERT INTO station_schedule_events (station_id, title, start_date, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, recurrence_type, recurrence_interval, recurrence_days, recurrence_end_date, recurrence_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING id, station_id, title, start_date, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, recurrence_type, recurrence_interval, recurrence_days, recurrence_end_date, recurrence_count, created_at, updated_at",
    )
    .bind(station_id)
    .bind(title)
    .bind(start_date)
    .bind(start_time)
    .bind(end_time)
    .bind(source_type)
    .bind(playlist_id)
    .bind(auto_dj_mode)
    .bind(auto_dj_avoid_repeat)
    .bind(auto_dj_min_gap)
    .bind(auto_dj_songs_ahead)
    .bind(recurrence_type)
    .bind(recurrence_interval)
    .bind(recurrence_days)
    .bind(recurrence_end_date)
    .bind(recurrence_count)
    .fetch_one(db)
    .await
    .db_error("failed to create schedule event")
}

#[allow(clippy::too_many_arguments)]
pub async fn update_event(
    db: &PgPool,
    id: Uuid,
    title: Option<String>,
    start_date: NaiveDate,
    start_time: NaiveTime,
    end_time: NaiveTime,
    source_type: &SourceType,
    playlist_id: Option<Uuid>,
    auto_dj_mode: Option<AutoDjMode>,
    auto_dj_avoid_repeat: Option<bool>,
    auto_dj_min_gap: Option<i32>,
    auto_dj_songs_ahead: Option<i32>,
    recurrence_type: &RecurrenceType,
    recurrence_interval: Option<i32>,
    recurrence_days: Option<String>,
    recurrence_end_date: Option<NaiveDate>,
    recurrence_count: Option<i32>,
) -> Result<StationScheduleEvent, AppError> {
    sqlx::query_as::<_, StationScheduleEvent>(
        "UPDATE station_schedule_events SET title = $1, start_date = $2, start_time = $3, end_time = $4, source_type = $5, playlist_id = $6, auto_dj_mode = $7, auto_dj_avoid_repeat = $8, auto_dj_min_gap = $9, auto_dj_songs_ahead = $10, recurrence_type = $11, recurrence_interval = $12, recurrence_days = $13, recurrence_end_date = $14, recurrence_count = $15, updated_at = NOW() WHERE id = $16 RETURNING id, station_id, title, start_date, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, recurrence_type, recurrence_interval, recurrence_days, recurrence_end_date, recurrence_count, created_at, updated_at",
    )
    .bind(title)
    .bind(start_date)
    .bind(start_time)
    .bind(end_time)
    .bind(source_type)
    .bind(playlist_id)
    .bind(auto_dj_mode)
    .bind(auto_dj_avoid_repeat)
    .bind(auto_dj_min_gap)
    .bind(auto_dj_songs_ahead)
    .bind(recurrence_type)
    .bind(recurrence_interval)
    .bind(recurrence_days)
    .bind(recurrence_end_date)
    .bind(recurrence_count)
    .bind(id)
    .fetch_one(db)
    .await
    .db_error("failed to update schedule event")
}

pub async fn delete_event(db: &PgPool, id: Uuid) -> Result<u64, AppError> {
    let result = sqlx::query("DELETE FROM station_schedule_events WHERE id = $1")
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to delete schedule event")?;
    Ok(result.rows_affected())
}

pub async fn find_all_events_for_station(db: &PgPool, station_id: Uuid) -> Result<Vec<StationScheduleEvent>, AppError> {
    sqlx::query_as::<_, StationScheduleEvent>(
        "SELECT id, station_id, title, start_date, start_time, end_time, source_type, playlist_id, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead, recurrence_type, recurrence_interval, recurrence_days, recurrence_end_date, recurrence_count, created_at, updated_at FROM station_schedule_events WHERE station_id = $1 ORDER BY start_date, start_time",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to check event overlap")
}

pub async fn find_auto_fill_config(db: &PgPool, station_id: Uuid) -> Result<Option<StationAutoFill>, AppError> {
    sqlx::query_as::<_, StationAutoFill>(
        "SELECT station_id, enabled, mode, source_type, source_playlist_id, avoid_artist_repeat, min_song_gap, songs_ahead FROM station_auto_fill WHERE station_id = $1",
    )
    .bind(station_id)
    .fetch_optional(db)
    .await
    .db_error("failed to pick from weighted playlist")
}

pub async fn upsert_auto_fill_config(
    db: &PgPool,
    station_id: Uuid,
    enabled: bool,
    mode: &AutoDjMode,
    source_type: &SourceType,
    source_playlist_id: Option<Uuid>,
    avoid_artist_repeat: bool,
    min_song_gap: i32,
    songs_ahead: i32,
) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO station_auto_fill (station_id, enabled, mode, source_type, source_playlist_id, avoid_artist_repeat, min_song_gap, songs_ahead)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (station_id) DO UPDATE SET enabled = $2, mode = $3, source_type = $4, source_playlist_id = $5, avoid_artist_repeat = $6, min_song_gap = $7, songs_ahead = $8",
    )
    .bind(station_id)
    .bind(enabled)
    .bind(mode)
    .bind(source_type)
    .bind(source_playlist_id)
    .bind(avoid_artist_repeat)
    .bind(min_song_gap)
    .bind(songs_ahead)
    .execute(db)
    .await
    .db_error("failed to find recent queue entries")?;
    Ok(())
}

pub async fn find_auto_fill_playlists(db: &PgPool, station_id: Uuid) -> Result<Vec<StationAutoFillPlaylist>, AppError> {
    sqlx::query_as::<_, StationAutoFillPlaylist>(
        "SELECT safp.id, safp.station_id, safp.playlist_id, safp.weight FROM station_auto_fill_playlists safp WHERE safp.station_id = $1 ORDER BY safp.weight DESC",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to find last sequential position")
}

pub async fn find_playlist_name(db: &PgPool, playlist_id: Uuid) -> Result<Option<String>, AppError> {
    let result: Option<String> = sqlx::query_scalar("SELECT name FROM playlists WHERE id = $1")
        .bind(playlist_id)
        .fetch_optional(db)
        .await
        .db_error("failed to get playlist name")?;
    Ok(result)
}

pub async fn find_playlist_name_string(db: &PgPool, playlist_id: Uuid) -> Result<String, AppError> {
    Ok(sqlx::query_scalar::<_, String>("SELECT name FROM playlists WHERE id = $1")
        .bind(playlist_id)
        .fetch_optional(db)
        .await
        .db_error("failed to find playlist name")?
        .unwrap_or_default())
}

pub async fn insert_auto_fill_playlist(
    db: &PgPool,
    station_id: Uuid,
    playlist_id: Uuid,
    weight: i32,
) -> Result<StationAutoFillPlaylist, AppError> {
    sqlx::query_as::<_, StationAutoFillPlaylist>(
        "INSERT INTO station_auto_fill_playlists (station_id, playlist_id, weight) VALUES ($1, $2, $3) ON CONFLICT (station_id, playlist_id) DO UPDATE SET weight = $3 RETURNING id, station_id, playlist_id, weight",
    )
    .bind(station_id)
    .bind(playlist_id)
    .bind(weight)
    .fetch_one(db)
    .await
    .db_error("failed to check candidate artist")
}

pub async fn update_auto_fill_playlist_weight(db: &PgPool, id: Uuid, weight: i32) -> Result<Option<StationAutoFillPlaylist>, AppError> {
    sqlx::query_as::<_, StationAutoFillPlaylist>(
        "UPDATE station_auto_fill_playlists SET weight = $1 WHERE id = $2 RETURNING id, station_id, playlist_id, weight",
    )
    .bind(weight)
    .bind(id)
    .fetch_optional(db)
    .await
    .db_error("failed to insert auto-DJ selection")
}

pub async fn delete_auto_fill_playlist(db: &PgPool, id: Uuid) -> Result<u64, AppError> {
    let result = sqlx::query("DELETE FROM station_auto_fill_playlists WHERE id = $1")
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to load auto-fill config")?;
    Ok(result.rows_affected())
}

pub async fn find_playlist_name_for_schedule(db: &PgPool, playlist_id: Uuid) -> Result<Option<String>, AppError> {
    sqlx::query_scalar::<_, Option<String>>("SELECT name FROM playlists WHERE id = $1")
        .bind(playlist_id)
        .fetch_optional(db)
        .await
        .db_error("failed to get playlist name for schedule")?
        .flatten()
        .map_or(Ok(None), |n| Ok(Some(n)))
}
