mod api_common;

#[sqlx::test(migrations = "./migrations")]
async fn create_admin_and_fail_closed_demo(pool: PgPool) {
    let (server, token, _) = setup_two_users(pool).await;
    for role in ["admin", "demo"] {
        let response = server
            .post("/api/users")
            .add_header("Authorization", &format!("Bearer {token}"))
            .json(&json!({"username": format!("new-{role}"), "password": "safe-password", "role": role}))
            .await;
        assert_eq!(response.status_code(), 201);
        let user = response.json::<serde_json::Value>();
        assert_eq!(user["role"], role);
        assert!(user.get("password_hash").is_none());
        assert!(user.get("password").is_none());
        let login = server
            .post("/api/auth/login")
            .json(&json!({"username": format!("new-{role}"), "password": "safe-password"}))
            .await;
        assert_eq!(login.status_code().as_u16(), if role == "demo" { 403 } else { 200 });
    }
    let duplicate = server
        .post("/api/users")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"username":"new-admin", "password":"safe-password", "role":"admin"}))
        .await;
    assert_eq!(duplicate.status_code(), 409);
}

#[sqlx::test(migrations = "./migrations")]
async fn create_users_requires_admin_and_validates_input(pool: PgPool) {
    let (server, token, _) = setup_two_users(pool.clone()).await;
    let body = json!({"username":"new", "password":"safe-password", "role":"admin"});
    assert_eq!(server.post("/api/users").json(&body).await.status_code(), 401);
    for body in [
        json!({"username":"", "password":"safe-password", "role":"admin"}),
        json!({"username":"new", "password":"short", "role":"admin"}),
        json!({"username":"new", "password":"safe-password", "role":"viewer"}),
    ] {
        assert_eq!(
            server
                .post("/api/users")
                .add_header("Authorization", &format!("Bearer {token}"))
                .json(&body)
                .await
                .status_code(),
            400
        );
    }
    sqlx::query("UPDATE users SET role = 'demo'").execute(&pool).await.unwrap();
    assert_eq!(
        server
            .post("/api/users")
            .add_header("Authorization", &format!("Bearer {token}"))
            .json(&body)
            .await
            .status_code(),
        403
    );
    assert_eq!(
        server
            .get("/api/users")
            .add_header("Authorization", &format!("Bearer {token}"))
            .await
            .status_code(),
        403
    );
}

use axum_test::TestServer;
use serde_json::json;
use sqlx::PgPool;

async fn setup_two_users(pool: PgPool) -> (TestServer, String, String) {
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

#[sqlx::test(migrations = "./migrations")]
async fn test_list_users_as_admin_returns_200(pool: PgPool) {
    let (server, admin_token, _) = setup_two_users(pool).await;

    let resp = server
        .get("/api/users")
        .add_header("Authorization", &format!("Bearer {admin_token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let list = resp.json::<Vec<serde_json::Value>>();
    assert!(!list.is_empty());
    assert_eq!(list[0]["username"].as_str().unwrap(), "admin");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_list_users_without_admin_returns_403(pool: PgPool) {
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

#[sqlx::test(migrations = "./migrations")]
async fn test_update_user_role_returns_200(pool: PgPool) {
    let (server, admin_token, _) = setup_two_users(pool.clone()).await;
    surcast_backend::auth::repository::insert_user(
        &pool,
        uuid::Uuid::new_v4(),
        "second-admin",
        "unused",
        &surcast_backend::auth::models::Role::Admin,
    )
    .await
    .unwrap();

    let list_resp = server
        .get("/api/users")
        .add_header("Authorization", &format!("Bearer {admin_token}"))
        .await;
    let users = list_resp.json::<Vec<serde_json::Value>>();
    let target_id = users.iter().find(|user| user["username"] == "second-admin").unwrap()["id"]
        .as_str()
        .unwrap();

    let resp = server
        .put(&format!("/api/users/{target_id}"))
        .add_header("Authorization", &format!("Bearer {admin_token}"))
        .json(&json!({"role": "demo"}))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["role"].as_str().unwrap(), "demo");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_last_admin_returns_409(pool: PgPool) {
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
    assert_eq!(resp.status_code(), 409);
    assert_eq!(resp.json::<serde_json::Value>()["code"], "LAST_ADMIN");
}
