mod api_common;
mod common;

use axum_test::TestServer;
use serde_json::json;

async fn setup_two_users() -> (TestServer, String, String) {
    let pool = common::setup_db().await;
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let admin_login = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    let admin_token = admin_login.json::<serde_json::Value>()["access_token"]
        .as_str()
        .unwrap()
        .to_string();

    let user_login = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    let user_token = user_login.json::<serde_json::Value>()["access_token"].as_str().unwrap().to_string();

    (server, admin_token, user_token)
}

#[tokio::test]
async fn test_list_users_as_admin_returns_200() {
    let (server, admin_token, _) = setup_two_users().await;

    let resp = server
        .get("/api/users")
        .add_header("Authorization", &format!("Bearer {admin_token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let list = resp.json::<Vec<serde_json::Value>>();
    assert!(!list.is_empty());
    assert_eq!(list[0]["username"].as_str().unwrap(), "admin");
}

#[tokio::test]
async fn test_list_users_without_admin_returns_403() {
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

    let resp = server
        .get("/api/users")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
}

#[tokio::test]
async fn test_update_user_role_returns_200() {
    let (server, admin_token, _) = setup_two_users().await;

    let list_resp = server
        .get("/api/users")
        .add_header("Authorization", &format!("Bearer {admin_token}"))
        .await;
    let users = list_resp.json::<Vec<serde_json::Value>>();
    let target_id = users[0]["id"].as_str().unwrap();

    let resp = server
        .put(&format!("/api/users/{target_id}"))
        .add_header("Authorization", &format!("Bearer {admin_token}"))
        .json(&json!({"role": "viewer"}))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["role"].as_str().unwrap(), "viewer");
}

#[tokio::test]
async fn test_delete_user_returns_204() {
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

    let list_resp = server
        .get("/api/users")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    let users = list_resp.json::<Vec<serde_json::Value>>();
    let target_id = users[0]["id"].as_str().unwrap().to_string();

    let resp = server
        .delete(&format!("/api/users/{target_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 204);
}
