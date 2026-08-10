mod api_common;
mod common;

use axum_test::TestServer;
use serde_json::json;
use uuid::Uuid;

async fn setup_auth() -> (TestServer, String) {
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

    (server, token)
}

#[tokio::test]
async fn test_create_station_returns_201() {
    let (server, token) = setup_auth().await;

    let resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Test Station", "description": "A test"}))
        .await;
    assert_eq!(resp.status_code(), 201);
    let body = resp.json::<serde_json::Value>();
    assert_eq!(body["name"].as_str().unwrap(), "Test Station");
    assert_eq!(body["description"].as_str().unwrap(), "A test");
    assert!(body["id"].as_str().unwrap().len() > 0);
}

#[tokio::test]
async fn test_list_stations_returns_200() {
    let (server, token) = setup_auth().await;

    server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Station A"}))
        .await;
    server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Station B"}))
        .await;

    let resp = server
        .get("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let list = resp.json::<Vec<serde_json::Value>>();
    assert!(list.len() >= 2);
}

#[tokio::test]
async fn test_get_station_by_id_returns_200() {
    let (server, token) = setup_auth().await;

    let create_resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "My Station"}))
        .await;
    let station_id = create_resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

    let resp = server
        .get(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["name"].as_str().unwrap(), "My Station");
}

#[tokio::test]
async fn test_get_station_wrong_id_returns_404() {
    let (server, token) = setup_auth().await;
    let fake_id = Uuid::nil().to_string();

    let resp = server
        .get(&format!("/api/stations/{fake_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[tokio::test]
async fn test_update_station_returns_200() {
    let (server, token) = setup_auth().await;

    let create_resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Original Name"}))
        .await;
    let station_id = create_resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

    let resp = server
        .put(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Updated Name", "description": "Updated desc"}))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["name"].as_str().unwrap(), "Updated Name");
}

#[tokio::test]
async fn test_delete_station_returns_204() {
    let (server, token) = setup_auth().await;

    let create_resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "To Delete"}))
        .await;
    let station_id = create_resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

    let resp = server
        .delete(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 204);
}

#[tokio::test]
async fn test_get_station_after_delete_returns_404() {
    let (server, token) = setup_auth().await;

    let create_resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "To Delete"}))
        .await;
    let station_id = create_resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

    server
        .delete(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;

    let resp = server
        .get(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[tokio::test]
async fn test_create_station_without_auth_returns_401() {
    let pool = common::setup_db().await;
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    let resp = server.post("/api/stations").json(&json!({"name": "Unauthorized"})).await;
    assert_eq!(resp.status_code(), 401);
}

#[tokio::test]
async fn test_create_station_empty_name_returns_400() {
    let (server, token) = setup_auth().await;

    let resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": ""}))
        .await;
    assert_eq!(resp.status_code(), 400);
}
