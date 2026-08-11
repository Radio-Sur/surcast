mod api_common;
mod common;

use axum_test::multipart::{MultipartForm, Part};
use axum_test::TestServer;
use serde_json::json;
use uuid::Uuid;

fn make_song_form() -> MultipartForm {
    MultipartForm::new()
        .add_text("title", "Queue Song")
        .add_text("artist", "Queue Artist")
        .add_part(
            "file",
            Part::bytes(b"fake audio" as &[_]).file_name("test.mp3").mime_type("audio/mpeg"),
        )
}

async fn setup_with_station_and_song() -> (TestServer, String, Uuid, Uuid) {
    let pool = common::setup_db().await;
    let app = api_common::create_test_app(pool);
    let server = TestServer::new(app);

    let init_resp = server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    assert!(
        init_resp.status_code() == 201 || init_resp.status_code() == 400,
        "setup_init failed: {}",
        init_resp.text()
    );

    let login_resp = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    assert_eq!(
        login_resp.status_code(),
        200,
        "login failed: {} {}",
        login_resp.status_code(),
        login_resp.text()
    );
    let token = login_resp.json::<serde_json::Value>()["access_token"]
        .as_str()
        .expect("no access_token in login response")
        .to_string();

    let station_resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Queue Station"}))
        .await;
    assert_eq!(station_resp.status_code(), 201, "create station failed: {}", station_resp.text());
    let station_id: Uuid = station_resp.json::<serde_json::Value>()["id"]
        .as_str()
        .expect("no id in station response")
        .parse()
        .expect("invalid station id");

    let form = make_song_form();
    let song_resp = server
        .post("/api/songs")
        .add_header("Authorization", &format!("Bearer {token}"))
        .multipart(form)
        .await;
    assert_eq!(song_resp.status_code(), 201, "upload song failed: {}", song_resp.text());
    let song_id: Uuid = song_resp.json::<serde_json::Value>()["id"]
        .as_str()
        .expect("no id in song response")
        .parse()
        .expect("invalid song id");

    server
        .post(&format!("/api/songs/{song_id}/stations"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"station_ids": [station_id]}))
        .await;

    (server, token, station_id, song_id)
}

#[tokio::test]
async fn test_add_songs_to_queue_returns_200() {
    let (server, token, station_id, song_id) = setup_with_station_and_song().await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [song_id]}))
        .await;
    assert_eq!(resp.status_code(), 201);
}

#[tokio::test]
async fn test_list_queue_returns_200() {
    let (server, token, station_id, song_id) = setup_with_station_and_song().await;

    server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [song_id]}))
        .await;

    let resp = server
        .get(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let items = resp.json::<Vec<serde_json::Value>>();
    assert!(!items.is_empty());
}

#[tokio::test]
async fn test_reorder_queue_returns_200() {
    let (server, token, station_id, song_id) = setup_with_station_and_song().await;

    let add_resp = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [song_id]}))
        .await;
    let items = add_resp.json::<Vec<serde_json::Value>>();
    let item_id = items[0]["id"].as_str().unwrap().to_string();

    let resp = server
        .put(&format!("/api/stations/{station_id}/queue/reorder"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"queue_item_ids": [item_id]}))
        .await;
    assert_eq!(resp.status_code(), 200);
}

#[tokio::test]
async fn test_insert_song_at_position_returns_200() {
    let (server, token, station_id, song_id) = setup_with_station_and_song().await;

    server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [song_id]}))
        .await;

    let resp = server
        .post(&format!("/api/stations/{station_id}/queue/insert"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_id": song_id, "position": 0}))
        .await;
    assert_eq!(resp.status_code(), 200);
}

#[tokio::test]
async fn test_remove_song_from_queue_returns_204() {
    let (server, token, station_id, song_id) = setup_with_station_and_song().await;

    let add_resp = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [song_id]}))
        .await;
    let items = add_resp.json::<Vec<serde_json::Value>>();
    let item_id: Uuid = items[0]["id"].as_str().unwrap().parse().unwrap();

    let resp = server
        .delete(&format!("/api/stations/{station_id}/queue/{item_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 204);
}

#[tokio::test]
async fn test_add_nonexistent_song_to_queue_returns_400() {
    let (server, token, station_id, _) = setup_with_station_and_song().await;
    let fake_song_id = Uuid::nil();

    let resp = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [fake_song_id]}))
        .await;
    assert_eq!(resp.status_code(), 400);
}

#[tokio::test]
async fn test_remove_playlist_from_queue_returns_204() {
    let (server, token, station_id, song_id) = setup_with_station_and_song().await;

    let playlist_resp = server
        .post("/api/playlists")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Queue Playlist"}))
        .await;
    let playlist_id: Uuid = playlist_resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();

    server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [song_id], "playlist_id": playlist_id}))
        .await;

    let resp = server
        .delete(&format!("/api/stations/{station_id}/queue/playlist/{playlist_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 204);
}

#[tokio::test]
async fn test_add_songs_trims_consumed_queue_items() {
    let pool = common::setup_db().await;
    let app = api_common::create_test_app(pool.clone());
    let server = TestServer::new(app).expect("server");

    let init_resp = server
        .post("/api/setup/init")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    assert!(init_resp.status_code() == 201 || init_resp.status_code() == 400);
    let login_resp = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    assert_eq!(login_resp.status_code(), 200);
    let token = login_resp.json::<serde_json::Value>()["access_token"]
        .as_str()
        .expect("no access token")
        .to_string();

    let station_resp = server
        .post("/api/stations")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"name": "Trim Station"}))
        .await;
    assert_eq!(station_resp.status_code(), 201, "create station failed: {}", station_resp.text());
    let station_id: Uuid = station_resp.json::<serde_json::Value>()["id"]
        .as_str()
        .expect("no station id")
        .parse()
        .unwrap();

    let stale_song = upload_station_song(&server, &token, station_id, "Stale Song").await;
    let fresh_song = upload_station_song(&server, &token, station_id, "Fresh Song").await;

    // queue the stale song, then mark it consumed through the durable cursor
    let add = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [stale_song]}))
        .await;
    assert_eq!(add.status_code(), 201, "add failed: {}", add.text());
    let stale_item_id: Uuid = add.json::<Vec<serde_json::Value>>()[0]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    sqlx::query(
        "UPDATE stations SET current_queue_item_id = $1, consumed_queue_item_ids = $2, \
         current_song_index = 1, current_queue_cursor_format = 1 WHERE id = $3",
    )
    .bind(stale_item_id)
    .bind(&vec![stale_item_id])
    .bind(station_id)
    .execute(&pool)
    .await
    .unwrap();

    // adding a new song must drop the consumed row so the queue no longer
    // counts it in "Song X of Y"
    let add2 = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [fresh_song]}))
        .await;
    assert_eq!(add2.status_code(), 201, "second add failed: {}", add2.text());

    let list = server
        .get(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(list.status_code(), 200);
    let items = list.json::<Vec<serde_json::Value>>();
    assert_eq!(items.len(), 1, "stale consumed item must be trimmed: {items:?}");
    assert_eq!(items[0]["title"], "Fresh Song");
}

async fn upload_station_song(server: &TestServer, token: &str, station_id: Uuid, title: &str) -> Uuid {
    let form = MultipartForm::new()
        .add_text("title", title)
        .add_text("artist", "Queue Artist")
        .add_part(
            "file",
            Part::bytes(b"fake audio" as &[_]).file_name("test.mp3").mime_type("audio/mpeg"),
        );
    let resp = server
        .post("/api/songs")
        .add_header("Authorization", &format!("Bearer {token}"))
        .multipart(form)
        .await;
    assert_eq!(resp.status_code(), 201, "upload failed: {}", resp.text());
    let song_id: Uuid = resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();
    let assign = server
        .post(&format!("/api/songs/{song_id}/stations"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"station_ids": [station_id]}))
        .await;
    assert!(assign.status_code().as_u16() < 300, "assign failed: {}", assign.text());
    song_id
}
