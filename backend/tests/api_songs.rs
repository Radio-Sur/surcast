mod api_common;

use axum_test::multipart::{MultipartForm, Part};
use axum_test::TestServer;
use base64::Engine;
use id3::TagLike;
use serde_json::json;
use sqlx::PgPool;
use std::io::Write;
use surcast_backend::songs::repository::{insert_song_record, InsertSongParams};
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
    assert_eq!(body["created"].as_bool().unwrap(), true);
    assert!(body["duplicate"].is_null());
    let song = &body["song"];
    let song_id: Uuid = song["id"].as_str().unwrap().parse().unwrap();
    (song_id, song["title"].as_str().unwrap().to_string())
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
    assert_eq!(body["created"].as_bool().unwrap(), true);
    let song = &body["song"];
    assert_eq!(song["title"].as_str().unwrap(), "Uploaded Song");
    assert_eq!(song["artist"].as_str().unwrap(), "Uploaded Artist");
    assert!(!song["id"].as_str().unwrap().is_empty());
    assert_eq!(song["origin"].as_str().unwrap(), "api");
    assert_eq!(song["file_path"].as_str().unwrap(), "test.mp3");
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

async fn setup_auth_with_music(pool: PgPool) -> (TestServer, String, String, PgPool) {
    let mut config = api_common::test_config();
    let holder = tempfile::tempdir().expect("test music dir");
    let music_dir = holder.path().to_string_lossy().to_string();
    std::mem::forget(holder);
    config.music_dir = music_dir.clone();
    let db = pool.clone();
    let app = api_common::create_test_app_with_config(pool, config);
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

    (server, token, music_dir, db)
}

async fn upload_named(server: &TestServer, token: &str, filename: &str, bytes: Vec<u8>, allow_duplicate: bool) -> (u16, serde_json::Value) {
    let form = MultipartForm::new().add_part("file", Part::bytes(bytes).file_name(filename.to_string()).mime_type("audio/mpeg"));
    let url = if allow_duplicate {
        "/api/songs?allow_duplicate=true"
    } else {
        "/api/songs"
    };
    let resp = server
        .post(url)
        .add_header("Authorization", &format!("Bearer {token}"))
        .multipart(form)
        .await;
    (resp.status_code().as_u16(), resp.json::<serde_json::Value>())
}

/// Scanner skips files modified in the last seconds (uploads in progress).
/// Age the whole fixture tree so it counts as stable before each scan.
fn stabilize_tree(music_dir: &str) {
    let ok = std::process::Command::new("find")
        .args([music_dir, "-exec", "touch", "-d", "5 minutes ago", "{}", "+"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(ok, "failed to age fixtures in {music_dir}");
}

async fn reset_scan_throttle(pool: &PgPool) {
    sqlx::query("DELETE FROM library_scan")
        .execute(pool)
        .await
        .expect("reset scan throttle");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_upload_duplicate_reports_existing_and_confirms(pool: PgPool) {
    let (server, token) = setup_auth(pool).await;

    let (status, first) = upload_named(&server, &token, "first.mp3", b"identical song bytes".to_vec(), false).await;
    assert_eq!(status, 201);
    assert_eq!(first["created"].as_bool().unwrap(), true);
    let first_id = first["song"]["id"].as_str().unwrap().to_string();
    assert_eq!(first["song"]["file_path"].as_str().unwrap(), "first.mp3");

    // Same content under another name: no second row, duplicate reported.
    let (status, dup) = upload_named(&server, &token, "second.mp3", b"identical song bytes".to_vec(), false).await;
    assert_eq!(status, 200);
    assert_eq!(dup["created"].as_bool().unwrap(), false);
    assert_eq!(dup["duplicate"]["song_id"].as_str().unwrap(), first_id);
    assert_eq!(dup["duplicate"]["file_path"].as_str().unwrap(), "first.mp3");

    // Confirmed: forces a second row sharing the file content.
    let (status, forced) = upload_named(&server, &token, "second.mp3", b"identical song bytes".to_vec(), true).await;
    assert_eq!(status, 201);
    assert_eq!(forced["created"].as_bool().unwrap(), true);
    assert_ne!(forced["song"]["id"].as_str().unwrap(), first_id);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_scan_imports_dropped_files(pool: PgPool) {
    let (server, token, music_dir, _db) = setup_auth_with_music(pool).await;

    std::fs::create_dir_all(format!("{music_dir}/rock")).unwrap();
    std::fs::write(format!("{music_dir}/rock/song-a.mp3"), b"scan audio A").unwrap();
    std::fs::write(format!("{music_dir}/song-b.mp3"), b"scan audio B").unwrap();

    stabilize_tree(&music_dir);
    let resp = server
        .post("/api/songs/scan")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let report = resp.json::<serde_json::Value>();
    assert_eq!(report["throttled"].as_bool().unwrap(), false);
    assert_eq!(report["added"].as_i64().unwrap(), 2);

    let list = server
        .get("/api/songs")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<Vec<serde_json::Value>>();
    let paths: Vec<&str> = list.iter().map(|s| s["file_path"].as_str().unwrap()).collect();
    assert!(paths.contains(&"rock/song-a.mp3"));
    assert!(paths.contains(&"song-b.mp3"));
    assert!(list.iter().all(|s| s["origin"].as_str().unwrap() == "directory"));
    assert!(list.iter().all(|s| s["uploaded_by"].is_null()));

    // Immediate second scan is throttled.
    stabilize_tree(&music_dir);
    let report = server
        .post("/api/songs/scan")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(report["throttled"].as_bool().unwrap(), true);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_scan_detects_rename_deletion_and_restore(pool: PgPool) {
    let (server, token, music_dir, db) = setup_auth_with_music(pool).await;

    std::fs::write(format!("{music_dir}/original.mp3"), b"rename me audio").unwrap();
    stabilize_tree(&music_dir);
    let report = server
        .post("/api/songs/scan")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(report["added"].as_i64().unwrap(), 1);

    let list = server
        .get("/api/songs")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<Vec<serde_json::Value>>();
    let song_id = list[0]["id"].as_str().unwrap().to_string();

    // System-level rename: same id, new path, no new row.
    std::fs::create_dir_all(format!("{music_dir}/moved")).unwrap();
    std::fs::rename(format!("{music_dir}/original.mp3"), format!("{music_dir}/moved/renamed.mp3")).unwrap();
    reset_scan_throttle(&db).await;
    stabilize_tree(&music_dir);
    let report = server
        .post("/api/songs/scan")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(report["renamed"].as_i64().unwrap(), 1);
    assert_eq!(report["added"].as_i64().unwrap(), 0);

    let song = server
        .get(&format!("/api/songs/{song_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(song["id"].as_str().unwrap(), song_id);
    assert_eq!(song["file_path"].as_str().unwrap(), "moved/renamed.mp3");

    // Deletion: row stays, flagged missing.
    std::fs::remove_file(format!("{music_dir}/moved/renamed.mp3")).unwrap();
    reset_scan_throttle(&db).await;
    stabilize_tree(&music_dir);
    let report = server
        .post("/api/songs/scan")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(report["marked_missing"].as_i64().unwrap(), 1);

    let song = server
        .get(&format!("/api/songs/{song_id}"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert!(song["missing_since"].is_string());

    // Missing songs cannot enter a queue.
    let station_id = create_station(&server, &token).await;
    server
        .post(&format!("/api/songs/{song_id}/stations"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"station_ids": [station_id]}))
        .await;
    let resp = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"song_ids": [song_id]}))
        .await;
    assert_eq!(resp.status_code(), 409);

    // File back: flag clears automatically.
    std::fs::write(format!("{music_dir}/moved/renamed.mp3"), b"rename me audio").unwrap();
    reset_scan_throttle(&db).await;
    stabilize_tree(&music_dir);
    let report = server
        .post("/api/songs/scan")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(report["restored"].as_i64().unwrap(), 1);
}

const COVER_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII=";

fn cover_bytes() -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(COVER_BASE64).unwrap()
}

fn mpeg_silent_frame() -> Vec<u8> {
    let mut frame = vec![0xFF, 0xFB, 0x90, 0x64];
    frame.resize(417, 0);
    frame
}

fn write_mp3_with_cover(path: &str) {
    let mut tag = id3::Tag::new();
    tag.set_title("Cover Song");
    tag.add_frame(id3::frame::Picture {
        mime_type: "image/png".to_string(),
        picture_type: id3::frame::PictureType::CoverFront,
        description: String::new(),
        data: cover_bytes(),
    });
    std::fs::write(path, b"").unwrap();
    tag.write_to_path(path, id3::Version::Id3v24).unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(&mpeg_silent_frame())
        .unwrap();
}

async fn insert_song(pool: &PgPool, id: Uuid, file_path: &str, has_cover: bool) {
    insert_song_record(
        pool,
        &InsertSongParams {
            id,
            title: "Cover Song".into(),
            artist: "Artist".into(),
            album: String::new(),
            has_cover,
            file_path: file_path.into(),
            file_size: 1024,
            mime_type: "audio/mpeg".into(),
            duration: 1,
            uploaded_by: None,
            origin: "api".into(),
            audio_hash: Some("coverhash".into()),
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            loudness: None,
            loudness_range: None,
            true_peak: None,
            true_peak_db: None,
            amplify: None,
            sustained_ending: false,
            longtail: false,
            analyzed_at: None,
        },
    )
    .await
    .expect("insert song");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_serve_cover_reads_embedded_art(pool: PgPool) {
    let (server, _token, music_dir, db) = setup_auth_with_music(pool).await;
    let song_id = Uuid::new_v4();
    write_mp3_with_cover(&format!("{music_dir}/cover.mp3"));
    insert_song(&db, song_id, "cover.mp3", true).await;

    let resp = server.get(&format!("/api/songs/{song_id}/cover")).await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(resp.headers().get("etag").unwrap(), "\"coverhash\"");
    assert_eq!(resp.as_bytes().to_vec(), cover_bytes());

    let cached = server
        .get(&format!("/api/songs/{song_id}/cover"))
        .add_header("If-None-Match", "\"coverhash\"")
        .await;
    assert_eq!(cached.status_code(), 304);
    assert!(cached.as_bytes().is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_serve_cover_404_when_flag_absent(pool: PgPool) {
    let (server, _token, music_dir, db) = setup_auth_with_music(pool).await;
    let song_id = Uuid::new_v4();
    write_mp3_with_cover(&format!("{music_dir}/noflag.mp3"));
    insert_song(&db, song_id, "noflag.mp3", false).await;

    let resp = server.get(&format!("/api/songs/{song_id}/cover")).await;
    assert_eq!(resp.status_code(), 404);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_serve_cover_404_when_file_has_no_art(pool: PgPool) {
    let (server, _token, music_dir, db) = setup_auth_with_music(pool).await;
    let song_id = Uuid::new_v4();
    std::fs::write(format!("{music_dir}/plain.mp3"), mpeg_silent_frame()).unwrap();
    insert_song(&db, song_id, "plain.mp3", true).await;

    let resp = server.get(&format!("/api/songs/{song_id}/cover")).await;
    assert_eq!(resp.status_code(), 404);
}
