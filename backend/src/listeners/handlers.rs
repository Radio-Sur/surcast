use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;

use crate::api::StreamersMap;
use crate::errors::AppError;
use crate::listeners::ListenersState;
use crate::stations::repository;

use super::models;

/// Supported history windows. Each maps to (days, bucket seconds).
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Range {
    #[serde(rename = "24h")]
    H24,
    #[default]
    #[serde(rename = "7d")]
    D7,
    #[serde(rename = "30d")]
    D30,
}

impl Range {
    pub fn window(self) -> (i64, i64) {
        match self {
            Range::H24 => (1, 3_600),
            Range::D7 => (7, 6 * 3_600),
            Range::D30 => (30, 86_400),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct RangeQuery {
    pub range: Option<Range>,
}

/// Live listener count for a single station.
pub async fn station_listeners_live(
    State(db): State<PgPool>,
    State(state): State<Arc<ListenersState>>,
    State(_streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = crate::stations::handlers::stream::resolve_station_id(&db, &station_id).await?;

    if let Some(live) = state.live(station_id).await {
        return Ok(Json(json!({
            "station_id": station_id,
            "listeners": live.listeners,
            "updated_at": live.updated_at,
            "online": live.online,
        })));
    }

    let sample = models::latest_for_station(&db, station_id).await?;
    Ok(Json(json!({
        "station_id": station_id,
        "listeners": sample.as_ref().map_or(0, |s| s.listeners),
        "updated_at": sample.map_or(serde_json::Value::Null, |s| serde_json::to_value(s.recorded_at).unwrap_or_default()),
        "online": false,
    })))
}

/// Downsampled listener history for a single station.
pub async fn station_listeners_history(
    State(db): State<PgPool>,
    Path(station_id): Path<String>,
    Query(query): Query<RangeQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = crate::stations::handlers::stream::resolve_station_id(&db, &station_id).await?;
    let (days, bucket) = query.range.unwrap_or_default().window();
    let points = models::history_for_station(&db, station_id, days, bucket).await?;
    Ok(Json(json!({ "points": points })))
}

/// Dashboard overview: current totals plus hour/day-of-week aggregates.
pub async fn listeners_overview(
    State(db): State<PgPool>,
    State(state): State<Arc<ListenersState>>,
    Query(query): Query<RangeQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let (days, bucket) = query.range.unwrap_or_default().window();
    let stations = repository::find_all_stations(&db).await?;
    let live_map = state.live_all().await;

    let mut total_now = 0i32;
    let station_rows = stations
        .into_iter()
        .map(|s| {
            let live = live_map.get(&s.id);
            let listeners = live.map_or(0, |l| l.listeners);
            total_now += listeners;
            json!({
                "station_id": s.id,
                "name": s.name,
                "listeners": listeners,
                "updated_at": live.map_or(serde_json::Value::Null, |l| serde_json::to_value(l.updated_at).unwrap_or_default()),
                "online": live.is_some_and(|l| l.online),
            })
        })
        .collect::<Vec<_>>();

    let by_hour = models::by_hour(&db, days).await?;
    let by_weekday = models::by_weekday(&db, days).await?;
    let series = models::aggregate_history(&db, days, bucket).await?;

    Ok(Json(json!({
        "range": query.range.map_or("7d", |r| match r {
            Range::H24 => "24h",
            Range::D7 => "7d",
            Range::D30 => "30d",
        }),
        "total_now": total_now,
        "stations": station_rows,
        "by_hour": by_hour,
        "by_weekday": by_weekday,
        "series": series,
    })))
}
