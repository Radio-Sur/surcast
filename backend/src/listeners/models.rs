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
/// One sample = concurrent connections at that moment (per connection, not
/// per IP: several people behind one NAT count separately). Concurrent
/// pollers writing the same timestamp are ignored via `ON CONFLICT`.
pub async fn insert_samples(db: &PgPool, samples: &[(Uuid, i32)], recorded_at: DateTime<Utc>) -> Result<(), AppError> {
    for (station_id, listeners) in samples {
        sqlx::query(
            "INSERT INTO listener_stats (station_id, listeners, recorded_at) VALUES ($1, $2, $3) \
             ON CONFLICT (station_id, recorded_at) DO NOTHING",
        )
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
/// Each raw row is one 30s sample of concurrent connections, so summing raw
/// rows would multiply by the sample count (120x per 1h bucket). Instead we
/// average per station per bucket first (mean concurrent connections in that
/// chunk), then sum the per-station means.
pub async fn aggregate_history(db: &PgPool, since_days: i64, bucket_seconds: i64) -> Result<Vec<HistoryPoint>, AppError> {
    sqlx::query_as::<_, HistoryPoint>(
        "SELECT to_timestamp(bucket)::timestamptz AS time, SUM(avg_per_station)::bigint AS listeners \
         FROM (SELECT floor(extract(epoch FROM recorded_at) / $2) * $2 AS bucket, station_id, \
                      AVG(listeners)::bigint AS avg_per_station \
               FROM listener_stats \
               WHERE recorded_at >= NOW() - $1 * INTERVAL '1 day' \
               GROUP BY 1, 2) sub \
         GROUP BY 1 ORDER BY 1",
    )
    .bind(since_days)
    .bind(bucket_seconds)
    .fetch_all(db)
    .await
    .db_error("failed to fetch aggregate listener history")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2 stations x 10 concurrent connections x 120 samples (one 1h bucket)
    /// must aggregate to 20, not 2400. Guards the per-connection semantics:
    /// samples are moments in time, not unique listeners to sum.
    #[tokio::test]
    async fn aggregate_history_sums_per_station_averages_not_raw_samples() {
        crate::test_db::run_with_test_db(async |db| {
            let user_id = Uuid::new_v4();
            sqlx::query("INSERT INTO users (id, username, password_hash, name) VALUES ($1, 'agg-test', 'x', 'agg')")
                .bind(user_id)
                .execute(&db.pool)
                .await
                .expect("user insert");
            let mut station_ids = Vec::new();
            for name in ["agg-a", "agg-b"] {
                let station_id = Uuid::new_v4();
                sqlx::query("INSERT INTO stations (id, name, created_by) VALUES ($1, $2, $3)")
                    .bind(station_id)
                    .bind(name)
                    .bind(user_id)
                    .execute(&db.pool)
                    .await
                    .expect("station insert");
                station_ids.push(station_id);
            }

            let now = Utc::now();
            for i in 0..120 {
                let recorded_at = now - chrono::Duration::seconds(i * 30);
                let samples = vec![(station_ids[0], 10), (station_ids[1], 10)];
                insert_samples(&db.pool, &samples, recorded_at).await.expect("insert");
            }

            // Same timestamp twice must not double-count (concurrent pollers).
            insert_samples(&db.pool, &[(station_ids[0], 10)], now).await.expect("reinsert");
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM listener_stats WHERE station_id = $1 AND recorded_at = $2")
                .bind(station_ids[0])
                .bind(now)
                .fetch_one(&db.pool)
                .await
                .expect("count");
            assert_eq!(count, 1, "duplicate timestamp must be ignored");

            let points = aggregate_history(&db.pool, 1, 3600).await.expect("aggregate");
            let total: i64 = points.iter().map(|p| p.listeners).sum();
            // 240 raw rows x 10 listeners would SUM to 2400; the correct
            // total concurrent connections is 10 + 10 = 20 per bucket.
            assert!(
                total <= 40,
                "aggregate must average per station first, got total {total} over {} buckets",
                points.len()
            );
            assert!(
                points.iter().any(|p| p.listeners == 20),
                "expected a bucket with 20 concurrent connections, got {points:?}"
            );
        })
        .await;
    }
}
