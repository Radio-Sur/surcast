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
async fn test_create_api_key_returns_201() {
    let (server, token) = setup_auth().await;

    let resp = server
        .post("/api/api-keys")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Test Key"}))
        .await;
    assert_eq!(resp.status_code(), 201);
    let body = resp.json::<serde_json::Value>();
    assert_eq!(body["name"].as_str().unwrap(), "Test Key");
    assert!(body["key"].as_str().unwrap().starts_with("sur_"));
    assert!(body["id"].as_str().unwrap().len() > 0);
}

#[tokio::test]
async fn test_list_api_keys_returns_200() {
    let (server, token) = setup_auth().await;

    server
        .post("/api/api-keys")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Key 1"}))
        .await;

    let resp = server
        .get("/api/api-keys")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let list = resp.json::<Vec<serde_json::Value>>();
    assert!(!list.is_empty());
    assert_eq!(list[0]["name"].as_str().unwrap(), "Key 1");
}

#[tokio::test]
async fn test_deactivate_api_key_returns_200() {
    let (server, token) = setup_auth().await;

    let create_resp = server
        .post("/api/api-keys")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Deactivate Me"}))
        .await;
    let key_id: Uuid = create_resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();

    let resp = server
        .put(&format!("/api/api-keys/{key_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"is_active": false}))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert!(!resp.json::<serde_json::Value>()["is_active"].as_bool().unwrap());
}

#[tokio::test]
async fn test_delete_api_key_returns_204() {
    let (server, token) = setup_auth().await;

    let create_resp = server
        .post("/api/api-keys")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Delete Me"}))
        .await;
    let key_id: Uuid = create_resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();

    let resp = server
        .delete(&format!("/api/api-keys/{key_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 204);
}

#[tokio::test]
async fn test_use_api_key_for_auth() {
    let (server, token) = setup_auth().await;

    let create_resp = server
        .post("/api/api-keys")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Auth Test Key"}))
        .await;
    let api_key = create_resp.json::<serde_json::Value>()["key"].as_str().unwrap().to_string();

    let resp = server
        .get("/api/stations")
        .add_header("Authorization", &format!("Bearer {api_key}"))
        .await;
    assert_eq!(resp.status_code(), 200);
}
