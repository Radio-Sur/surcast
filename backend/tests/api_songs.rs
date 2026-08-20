mod api_common;

use axum_test::multipart::{MultipartForm, Part};
use axum_test::TestServer;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

fn make_song_form(title: &str, artist: &str) -> MultipartForm {
    MultipartForm::new().add_text("title", title).add_text("artist", artist).add_part(
        "file",
        Part::bytes(b"fake audio content" as &[_])
            .file_name("test.mp3")
            .mime_type("audio/mpeg"),
    )
}

async fn setup_auth(pool: PgPool) -> (TestServer, String) {
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

async fn create_song(server: &TestServer, token: &str) -> (Uuid, String) {
    let form = make_song_form("Test Song", "Test Artist");
    let resp = server
        .post("/api/songs")
        .add_header("Authorization", &format!("Bearer {token}"))
        .multipart(form)
        .await;
    assert_eq!(resp.status_code(), 201);
    let body = resp.json::<serde_json::Value>();
    let song_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    (song_id, body["title"].as_str().unwrap().to_string())
}

async fn create_station(server: &TestServer, token: &str) -> Uuid {
    let resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Song Test Station"}))
        .await;
    resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap()
}

#[sqlx::test(migrations = "./migrations")]
async fn test_upload_song_multipart_returns_201(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;

    let form = make_song_form("Uploaded Song", "Uploaded Artist");
    let resp = server
        .post("/api/songs")
        .add_header("Authorization", &format!("Bearer {token}"))
        .multipart(form)
        .await;
    assert_eq!(resp.status_code(), 201);
    let body = resp.json::<serde_json::Value>();
    assert_eq!(body["title"].as_str().unwrap(), "Uploaded Song");
    assert_eq!(body["artist"].as_str().unwrap(), "Uploaded Artist");
    assert!(!body["id"].as_str().unwrap().is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_list_songs_returns_200(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    create_song(&server, &token).await;

    let resp = server
        .get("/api/songs")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let list = resp.json::<Vec<serde_json::Value>>();
    assert!(!list.is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_get_song_by_id_returns_200(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    let (song_id, _) = create_song(&server, &token).await;

    let resp = server
        .get(&format!("/api/songs/{song_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["title"].as_str().unwrap(), "Test Song");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_update_song_returns_200(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    let (song_id, _) = create_song(&server, &token).await;

    let resp = server
        .put(&format!("/api/songs/{song_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"title": "Updated Title", "artist": "Updated Artist"}))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["title"].as_str().unwrap(), "Updated Title");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_song_returns_204(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    let (song_id, _) = create_song(&server, &token).await;

    let resp = server
        .delete(&format!("/api/songs/{song_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 204);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_assign_song_to_station_returns_200(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    let (song_id, _) = create_song(&server, &token).await;
    let station_id = create_station(&server, &token).await;

    let resp = server
        .post(&format!("/api/songs/{song_id}/stations"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"station_ids": [station_id]}))
        .await;
    assert_eq!(resp.status_code(), 200);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_remove_song_from_station_returns_204(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    let (song_id, _) = create_song(&server, &token).await;
    let station_id = create_station(&server, &token).await;

    server
        .post(&format!("/api/songs/{song_id}/stations"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"station_ids": [station_id]}))
        .await;

    let resp = server
        .delete(&format!("/api/songs/{song_id}/stations/{station_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 204);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_get_song_wrong_id_returns_404(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    let fake_id = Uuid::nil();

    let resp = server
        .get(&format!("/api/songs/{fake_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 404);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_get_song_file_no_auth_returns_200_or_404(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;
    let (song_id, _) = create_song(&server, &token).await;

    let resp = server.get(&format!("/api/songs/{song_id}/file")).await;
    let status = resp.status_code();
    assert!(
        status == 200 || status == 404,
        "Expected 200 (file served) or 404 (no file on disk), got {status}"
    );
}
