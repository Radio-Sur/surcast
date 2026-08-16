mod api_common;
mod common;

use axum_test::TestServer;
use serde_json::json;
use uuid::Uuid;

async fn setup_with_station() -> (TestServer, String, Uuid, sqlx::PgPool) {
    let pool = common::setup_db().await;
    let app = api_common::create_test_app(pool.clone());
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let login_resp = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    let token = login_resp.json::<serde_json::Value>()["access_token"].as_str().unwrap().to_string();

    let station_resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Stream Station"}))
        .await;
    let station_id: Uuid = station_resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();

    (server, token, station_id, pool)
}

#[tokio::test]
async fn test_stream_status_no_streamer_returns_200() {
    let (server, token, station_id, _pool) = setup_with_station().await;

    let resp = server
        .get(&format!("/api/stations/{station_id}/stream/status"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let body = resp.json::<serde_json::Value>();
    assert!(!body["playing"].as_bool().unwrap());
}

#[tokio::test]
async fn test_stream_stop_without_runtime_persists_stopped() {
    let (server, token, station_id, pool) = setup_with_station().await;

    // Stop is idempotent: it must persist is_started=false even when no
    // runtime exists, and answer success (no "no active stream" error).
    let resp = server
        .post(&format!("/api/stations/{station_id}/stream/stop"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let started: bool = sqlx::query_scalar("SELECT is_started FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!started, "stop without a runtime must persist is_started=false");
}

#[tokio::test]
async fn test_stream_skip_no_active_stream_returns_400() {
    let (server, token, station_id, _pool) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/stream/skip"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 400);
}

#[tokio::test]
async fn test_stream_status_nonexistent_station_returns_404() {
    let (server, token, _station_id, _pool) = setup_with_station().await;

    let resp = server
        .get("/api/stations/nonexistent-slug/stream/status")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[tokio::test]
async fn test_stream_stop_nonexistent_station_returns_404() {
    let (server, token, _station_id, _pool) = setup_with_station().await;

    let resp = server
        .post("/api/stations/nonexistent-slug/stream/stop")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[tokio::test]
async fn test_stream_skip_nonexistent_station_returns_404() {
    let (server, token, _station_id, _pool) = setup_with_station().await;

    let resp = server
        .post("/api/stations/nonexistent-slug/stream/skip")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[tokio::test]
async fn test_new_station_defaults_to_stopped() {
    let (server, token, station_id, pool) = setup_with_station().await;

    let started: bool = sqlx::query_scalar("SELECT is_started FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!started, "a new station must default to is_started=false");
    drop((server, token));
}

#[tokio::test]
async fn test_observations_do_not_change_desired_state() {
    let (server, token, station_id, pool) = setup_with_station().await;

    // Pure observations: station data, stream status, playback settings.
    // None of them may persist started or create a runtime.
    for path in [
        format!("/api/stations/{station_id}"),
        format!("/api/stations/{station_id}/stream/status"),
        format!("/api/stations/{station_id}/auto-fill"),
    ] {
        let resp = server.get(&path).add_header("Authorization", &format!("Bearer {token}")).await;
        assert_eq!(resp.status_code(), 200, "observation {path} failed");
    }
    let started: bool = sqlx::query_scalar("SELECT is_started FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!started, "observations must not change the desired state to started");
}
