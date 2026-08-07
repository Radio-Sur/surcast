mod api_common;
mod common;

use axum_test::TestServer;
use serde_json::json;
use uuid::Uuid;

async fn setup_with_station() -> (TestServer, String, Uuid) {
    let pool = common::setup_db().await;
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app).expect("server");

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

    (server, token, station_id)
}

#[tokio::test]
async fn test_stream_status_no_streamer_returns_200() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .get(&format!("/api/stations/{station_id}/stream/status"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let body = resp.json::<serde_json::Value>();
    assert!(!body["playing"].as_bool().unwrap());
}

#[tokio::test]
async fn test_stream_stop_no_active_stream_returns_400() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/stream/stop"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 400);
}

#[tokio::test]
async fn test_stream_skip_no_active_stream_returns_400() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/stream/skip"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 400);
}

#[tokio::test]
async fn test_stream_status_nonexistent_station_returns_404() {
    let (server, token, _) = setup_with_station().await;

    let resp = server
        .get("/api/stations/nonexistent-slug/stream/status")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[tokio::test]
async fn test_stream_stop_nonexistent_station_returns_404() {
    let (server, token, _) = setup_with_station().await;

    let resp = server
        .post("/api/stations/nonexistent-slug/stream/stop")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[tokio::test]
async fn test_stream_skip_nonexistent_station_returns_404() {
    let (server, token, _) = setup_with_station().await;

    let resp = server
        .post("/api/stations/nonexistent-slug/stream/skip")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}
