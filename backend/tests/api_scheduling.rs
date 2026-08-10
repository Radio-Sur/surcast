mod api_common;
mod common;

use axum_test::TestServer;
use serde_json::json;
use uuid::Uuid;

async fn setup_with_station() -> (TestServer, String, Uuid) {
    let pool = common::setup_db().await;
    let app = api_common::create_test_app(pool);
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
        .json(&json!({"name": "Schedule Station"}))
        .await;
    let station_id: Uuid = station_resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();

    (server, token, station_id)
}

#[tokio::test]
async fn test_create_schedule_event_returns_201() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "title": "Morning Show",
            "start_date": "2025-01-01",
            "start_time": "08:00",
            "end_time": "10:00",
            "source_type": "station_library"
        }))
        .await;
    assert_eq!(resp.status_code(), 201);
    let body = resp.json::<serde_json::Value>();
    assert_eq!(body["title"].as_str().unwrap(), "Morning Show");
    assert_eq!(body["source_type"].as_str().unwrap(), "station_library");
}

#[tokio::test]
async fn test_list_schedule_events_returns_200() {
    let (server, token, station_id) = setup_with_station().await;

    let create_resp = server
        .post(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "title": "Event 1",
            "start_date": "2025-01-01",
            "start_time": "08:00",
            "end_time": "10:00",
            "source_type": "station_library"
        }))
        .await;
    assert_eq!(create_resp.status_code(), 201);

    let resp = server
        .get(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let list = resp.json::<Vec<serde_json::Value>>();
    assert!(!list.is_empty());
}

#[tokio::test]
async fn test_create_then_list_then_update_then_delete() {
    let (server, token, station_id) = setup_with_station().await;

    let create_resp = server
        .post(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "title": "My Event",
            "start_date": "2025-06-01",
            "start_time": "14:00",
            "end_time": "16:00",
            "source_type": "station_library"
        }))
        .await;
    assert_eq!(create_resp.status_code(), 201);
    let event_id: Uuid = create_resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();

    let list_resp = server
        .get(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(list_resp.status_code(), 200);
    let list = list_resp.json::<Vec<serde_json::Value>>();
    assert!(list.iter().any(|e| e["id"].as_str().unwrap() == event_id.to_string()));

    let update_resp = server
        .put(&format!("/api/stations/{station_id}/schedule-events/{event_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"title": "Updated Event"}))
        .await;
    assert_eq!(update_resp.status_code(), 200);
    assert_eq!(update_resp.json::<serde_json::Value>()["title"].as_str().unwrap(), "Updated Event");

    let delete_resp = server
        .delete(&format!("/api/stations/{station_id}/schedule-events/{event_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(delete_resp.status_code(), 204);
}

#[tokio::test]
async fn test_create_schedule_returns_201() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/schedules"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "day_of_week": 1,
            "start_time": "09:00",
            "end_time": "17:00",
            "source_type": "station_library"
        }))
        .await;
    assert_eq!(resp.status_code(), 201);
}

#[tokio::test]
async fn test_list_schedules_returns_200() {
    let (server, token, station_id) = setup_with_station().await;

    server
        .post(&format!("/api/stations/{station_id}/schedules"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "day_of_week": 1,
            "start_time": "09:00",
            "end_time": "17:00",
            "source_type": "station_library"
        }))
        .await;

    let resp = server
        .get(&format!("/api/stations/{station_id}/schedules"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let schedules = resp.json::<Vec<serde_json::Value>>();
    assert!(!schedules.is_empty());
}

#[tokio::test]
async fn test_update_schedule_returns_200() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/schedules"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "day_of_week": 1,
            "start_time": "09:00",
            "end_time": "17:00",
            "source_type": "station_library"
        }))
        .await;
    let schedule_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

    let resp = server
        .put(&format!("/api/stations/{station_id}/schedules/{schedule_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"start_time": "10:00", "end_time": "18:00"}))
        .await;
    assert_eq!(resp.status_code(), 200);
}

#[tokio::test]
async fn test_delete_schedule_returns_204() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/schedules"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "day_of_week": 1,
            "start_time": "09:00",
            "end_time": "17:00",
            "source_type": "station_library"
        }))
        .await;
    let schedule_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

    let resp = server
        .delete(&format!("/api/stations/{station_id}/schedules/{schedule_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert!(resp.status_code() == 204 || resp.status_code() == 200);
}

#[tokio::test]
async fn test_get_auto_fill_returns_200() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .get(&format!("/api/stations/{station_id}/auto-fill"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert!(resp.status_code() == 200 || resp.status_code() == 404);
}

#[tokio::test]
async fn test_update_auto_fill_returns_200() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .put(&format!("/api/stations/{station_id}/auto-fill"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "enabled": true,
            "mode": "random",
            "source_type": "station_library",
            "avoid_artist_repeat": true,
            "min_song_gap": 3,
            "songs_ahead": 5
        }))
        .await;
    assert!(resp.status_code() == 200 || resp.status_code() == 201);
}

#[tokio::test]
async fn test_trigger_auto_fill_returns_200() {
    let (server, token, station_id) = setup_with_station().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/auto-fill/trigger"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert!(resp.status_code() == 200 || resp.status_code() == 202);
}

#[tokio::test]
async fn test_create_overlapping_events_returns_409() {
    let (server, token, station_id) = setup_with_station().await;

    let first = server
        .post(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "title": "First Event",
            "start_date": "2025-01-01",
            "start_time": "08:00",
            "end_time": "10:00",
            "source_type": "station_library"
        }))
        .await;
    assert_eq!(first.status_code(), 201);

    let resp = server
        .post(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({
            "title": "Overlapping Event",
            "start_date": "2025-01-01",
            "start_time": "09:00",
            "end_time": "11:00",
            "source_type": "station_library"
        }))
        .await;
    assert_eq!(resp.status_code(), 409);
}
