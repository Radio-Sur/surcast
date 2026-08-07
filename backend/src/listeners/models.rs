use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::{AppError, DbResult};

/// One raw listener sample for a station.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ListenerSample {
    pub station_id: Uuid,
    pub listeners: i32,
    pub recorded_at: DateTime<Utc>,
}

/// A single point on a downsampled history chart.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct HistoryPoint {
    pub time: DateTime<Utc>,
    pub listeners: i64,
}

/// Average listeners for one hour of the day.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct HourStat {
    pub hour: i32,
    pub avg_listeners: f64,
}

/// Average listeners for one day of the week (ISO 1=Mon..7=Sun).
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct WeekdayStat {
    pub weekday: i32,
    pub avg_listeners: f64,
}

/// Inserts a batch of samples sharing a single timestamp.
pub async fn insert_samples(db: &PgPool, samples: &[(Uuid, i32)], recorded_at: DateTime<Utc>) -> Result<(), AppError> {
    for (station_id, listeners) in samples {
        sqlx::query("INSERT INTO listener_stats (station_id, listeners, recorded_at) VALUES ($1, $2, $3)")
            .bind(station_id)
            .bind(listeners)
            .bind(recorded_at)
            .execute(db)
            .await
            .db_error("failed to insert listener sample")?;
    }
    Ok(())
}

/// Deletes samples older than `days`.
pub async fn prune_older_than(db: &PgPool, days: i64) -> Result<(), AppError> {
    sqlx::query("DELETE FROM listener_stats WHERE recorded_at < NOW() - $1 * INTERVAL '1 day'")
        .bind(days)
        .execute(db)
        .await
        .db_error("failed to prune listener stats")?;
    Ok(())
}

/// Latest raw sample for a station.
pub async fn latest_for_station(db: &PgPool, station_id: Uuid) -> Result<Option<ListenerSample>, AppError> {
    sqlx::query_as::<_, ListenerSample>(
        "SELECT station_id, listeners, recorded_at FROM listener_stats \
         WHERE station_id = $1 ORDER BY recorded_at DESC LIMIT 1",
    )
    .bind(station_id)
    .fetch_optional(db)
    .await
    .db_error("failed to fetch latest listener sample")
}

/// Downsampled history for a single station. The `bucket_seconds` step
/// is aligned to Unix epoch boundaries via `to_timestamp(... * step)`.
pub async fn history_for_station(
    db: &PgPool,
    station_id: Uuid,
    since_days: i64,
    bucket_seconds: i64,
) -> Result<Vec<HistoryPoint>, AppError> {
    sqlx::query_as::<_, HistoryPoint>(
        "SELECT \
           to_timestamp(floor(extract(epoch FROM recorded_at) / $3) * $3)::timestamptz AS time, \
           ROUND(AVG(listeners))::bigint AS listeners \
         FROM listener_stats \
         WHERE station_id = $1 AND recorded_at >= NOW() - $2 * INTERVAL '1 day' \
         GROUP BY 1 ORDER BY 1",
    )
    .bind(station_id)
    .bind(since_days)
    .bind(bucket_seconds)
    .fetch_all(db)
    .await
    .db_error("failed to fetch listener history")
}

/// Average listeners per hour of day, across all stations, over the window.
pub async fn by_hour(db: &PgPool, since_days: i64) -> Result<Vec<HourStat>, AppError> {
    sqlx::query_as::<_, HourStat>(
        "SELECT EXTRACT(HOUR FROM recorded_at)::int AS hour, AVG(listeners)::float8 AS avg_listeners \
         FROM listener_stats \
         WHERE recorded_at >= NOW() - $1 * INTERVAL '1 day' \
         GROUP BY 1 ORDER BY 1",
    )
    .bind(since_days)
    .fetch_all(db)
    .await
    .db_error("failed to aggregate listeners by hour")
}

/// Average listeners per day of week, across all stations, over the window.
pub async fn by_weekday(db: &PgPool, since_days: i64) -> Result<Vec<WeekdayStat>, AppError> {
    sqlx::query_as::<_, WeekdayStat>(
        "SELECT EXTRACT(ISODOW FROM recorded_at)::int AS weekday, AVG(listeners)::float8 AS avg_listeners \
         FROM listener_stats \
         WHERE recorded_at >= NOW() - $1 * INTERVAL '1 day' \
         GROUP BY 1 ORDER BY 1",
    )
    .bind(since_days)
    .fetch_all(db)
    .await
    .db_error("failed to aggregate listeners by weekday")
}

/// Aggregate (all stations summed) history over the window.
pub async fn aggregate_history(db: &PgPool, since_days: i64, bucket_seconds: i64) -> Result<Vec<HistoryPoint>, AppError> {
    sqlx::query_as::<_, HistoryPoint>(
        "SELECT \
           to_timestamp(floor(extract(epoch FROM recorded_at) / $2) * $2)::timestamptz AS time, \
           SUM(listeners)::bigint AS listeners \
         FROM listener_stats \
         WHERE recorded_at >= NOW() - $1 * INTERVAL '1 day' \
         GROUP BY 1 ORDER BY 1",
    )
    .bind(since_days)
    .bind(bucket_seconds)
    .fetch_all(db)
    .await
    .db_error("failed to fetch aggregate listener history")
}
