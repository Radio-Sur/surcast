mod api_common;

use axum_test::TestServer;
use serde_json::{json, Value};
use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn every_http_rejection_has_a_machine_readable_code(pool: PgPool) {
    let server = TestServer::new(api_common::create_test_app(pool));
    server
        .post("/api/setup/init")
        .json(&json!({"username":"admin","password":"password123"}))
        .await;
    let login: Value = server
        .post("/api/auth/login")
        .json(&json!({"username":"admin","password":"password123"}))
        .await
        .json();
    let auth = format!("Bearer {}", login["access_token"].as_str().unwrap());
    let wrong = server
        .post("/api/auth/login")
        .json(&json!({"username":"admin","password":"wrong"}))
        .await;
    assert_eq!(wrong.status_code(), 401);
    assert_eq!(wrong.json::<Value>()["code"], "AUTH_INVALID_CREDENTIALS");
    let invalid_body = server
        .post("/api/stations")
        .add_header("Authorization", &auth)
        .json(&json!({"missing_name":true}))
        .await;
    assert_eq!(invalid_body.status_code(), 422);
    assert_eq!(invalid_body.json::<Value>()["code"], "BAD_REQUEST");
    let invalid_path = server.get("/api/songs/not-a-uuid").add_header("Authorization", &auth).await;
    assert_eq!(invalid_path.status_code(), 400);
    assert_eq!(invalid_path.json::<Value>()["code"], "BAD_REQUEST");
    let wrong_method = server.patch("/api/stations").add_header("Authorization", &auth).await;
    assert_eq!(wrong_method.status_code(), 405);
    assert_eq!(wrong_method.json::<Value>()["code"], "METHOD_NOT_ALLOWED");
    let unauthorized = server.get("/api/stations").await;
    assert_eq!(unauthorized.json::<Value>()["code"], "AUTH_MISSING");
    let unknown = server.get("/api/not-an-endpoint").await;
    assert_eq!(unknown.json::<Value>()["code"], "NOT_FOUND");
}
