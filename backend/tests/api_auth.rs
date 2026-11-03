mod api_common;

use axum_test::TestServer;
use serde_json::json;
use sqlx::PgPool;

async fn setup_auth_token(server: &TestServer) -> String {
    let resp = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    resp.json::<serde_json::Value>()["access_token"].as_str().unwrap().to_string()
}

#[sqlx::test(migrations = "./migrations")]
async fn test_setup_status_before_init(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    let resp = server.get("/api/setup/status").await;
    assert_eq!(resp.status_code(), 200);
    let body = resp.json::<serde_json::Value>();
    assert!(!body["setup_complete"].as_bool().unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_setup_init_creates_admin(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    let resp = server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    assert_eq!(resp.status_code(), 201);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_setup_init_twice_returns_400(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let resp = server
        .post("/api/setup/init")
        .json(&json!({"username": "admin2", "password": "admin123"}))
        .await;
    assert_eq!(resp.status_code(), 400);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_setup_status_after_init(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let resp = server.get("/api/setup/status").await;
    assert_eq!(resp.status_code(), 200);
    assert!(resp.json::<serde_json::Value>()["setup_complete"].as_bool().unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_login_correct_returns_tokens(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let resp = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    assert_eq!(resp.status_code(), 200);
    let body = resp.json::<serde_json::Value>();
    assert!(!body["access_token"].as_str().unwrap().is_empty());
    assert!(!body["refresh_token"].as_str().unwrap().is_empty());
    assert_eq!(body["user"]["username"].as_str().unwrap(), "admin");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_login_wrong_password_returns_401(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let resp = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "wrongpass"}))
        .await;
    assert_eq!(resp.status_code(), 401);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_login_wrong_username_returns_401(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let resp = server
        .post("/api/auth/login")
        .json(&json!({"username": "nonexistent", "password": "admin123"}))
        .await;
    assert_eq!(resp.status_code(), 401);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_me_with_token_returns_user(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let token = setup_auth_token(&server).await;

    let resp = server
        .get("/api/auth/me")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["username"].as_str().unwrap(), "admin");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_me_without_token_returns_401(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let resp = server.get("/api/auth/me").await;
    assert_eq!(resp.status_code(), 401);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_refresh_with_valid_token(pool: PgPool) {
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
    let refresh_token = login_resp.json::<serde_json::Value>()["refresh_token"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = server
        .post("/api/auth/refresh")
        .json(&json!({"refresh_token": refresh_token}))
        .await;
    assert_eq!(resp.status_code(), 200);
    let body = resp.json::<serde_json::Value>();
    assert!(!body["access_token"].as_str().unwrap().is_empty());
    assert!(!body["refresh_token"].as_str().unwrap().is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_refresh_with_invalid_token_returns_401(pool: PgPool) {
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;

    let resp = server
        .post("/api/auth/refresh")
        .json(&json!({"refresh_token": "some.invalid.token"}))
        .await;
    assert_eq!(resp.status_code(), 401);
}

#[sqlx::test(migrations = "./migrations")]
async fn token_purposes_are_not_interchangeable(pool: PgPool) {
    let server = TestServer::new(api_common::create_test_app(pool));
    server
        .post("/api/setup/init")
        .json(&json!({"username":"admin", "password":"admin123"}))
        .await
        .assert_status(axum::http::StatusCode::CREATED);
    let tokens = server
        .post("/api/auth/login")
        .json(&json!({"username":"admin", "password":"admin123"}))
        .await
        .json::<serde_json::Value>();
    let access = tokens["access_token"].as_str().unwrap();
    let refresh = tokens["refresh_token"].as_str().unwrap();
    server
        .get("/api/auth/me")
        .add_header("Authorization", &format!("Bearer {refresh}"))
        .await
        .assert_status_unauthorized();
    server
        .post("/api/auth/refresh")
        .json(&json!({"refresh_token":access}))
        .await
        .assert_status_unauthorized();
    let secret = api_common::test_config().jwt_secret;
    assert!(surcast_backend::auth::middleware::verify_token(access, &secret).is_ok());
    assert!(surcast_backend::auth::middleware::verify_token(refresh, &secret).is_err());
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_setup_creates_only_one_admin(pool: PgPool) {
    let server = TestServer::new(api_common::create_test_app(pool.clone()));
    let first = server
        .post("/api/setup/init")
        .json(&json!({"username":"first", "password":"admin123"}));
    let second = server
        .post("/api/setup/init")
        .json(&json!({"username":"second", "password":"admin123"}));
    let (first, second) = tokio::join!(first, second);
    let mut statuses = [first.status_code().as_u16(), second.status_code().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [201, 400]);
    assert_eq!(surcast_backend::auth::repository::count_users(&pool).await.unwrap(), 1);
}
