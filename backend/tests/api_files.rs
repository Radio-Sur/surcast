mod api_common;

use axum_test::TestServer;
use base64::Engine;
use id3::TagLike;
use serde_json::json;
use sqlx::PgPool;
use std::io::Write;
use surcast_backend::songs::repository::{insert_song_record, InsertSongParams};
use uuid::Uuid;

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
    write_mp3_with_cover_data(path, &cover_bytes());
}

fn write_mp3_with_cover_data(path: &str, cover: &[u8]) {
    let mut tag = id3::Tag::new();
    tag.set_title("Cover Song");
    tag.set_artist("Tag Artist");
    tag.set_album("Tag Album");
    tag.add_frame(id3::frame::Picture {
        mime_type: "image/png".to_string(),
        picture_type: id3::frame::PictureType::CoverFront,
        description: String::new(),
        data: cover.to_vec(),
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

async fn setup(pool: PgPool) -> (TestServer, String, String, PgPool) {
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

    let login = server
        .post("/api/auth/login")
        .json(&json!({"username": "admin", "password": "admin123"}))
        .await;
    let token = login.json::<serde_json::Value>()["access_token"].as_str().unwrap().to_string();
    (server, token, music_dir, db)
}

async fn insert_song(pool: &PgPool, id: Uuid, file_path: &str, has_cover: bool) {
    insert_song_record(
        pool,
        &InsertSongParams {
            id,
            title: "File Song".into(),
            artist: "Artist".into(),
            album: String::new(),
            has_cover,
            file_path: file_path.into(),
            file_size: 1024,
            mime_type: "audio/mpeg".into(),
            duration: 1,
            uploaded_by: None,
            origin: "directory".into(),
            audio_hash: None,
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
async fn test_list_and_create_folders(pool: PgPool) {
    let (server, token, music_dir, _db) = setup(pool).await;

    let listing = server
        .get("/api/files")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(listing["path"].as_str().unwrap(), "");
    assert!(listing["parent"].is_null());
    assert_eq!(listing["folders"].as_array().unwrap().len(), 0);

    let created = server
        .post("/api/files/folders")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"path": "rock/2020"}))
        .await;
    assert_eq!(created.status_code(), 201);
    assert!(std::path::Path::new(&format!("{music_dir}/rock/2020")).is_dir());

    let folders = server
        .get("/api/files/folders")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    let folders: Vec<String> = serde_json::from_value(folders).unwrap();
    assert_eq!(folders, vec!["rock".to_string(), "rock/2020".to_string()]);

    let root = server
        .get("/api/files")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(root["folders"][0]["path"].as_str().unwrap(), "rock");

    // Creating it twice conflicts.
    let again = server
        .post("/api/files/folders")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"path": "rock/2020"}))
        .await;
    assert_eq!(again.status_code(), 409);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_listing_attaches_song_metadata(pool: PgPool) {
    let (server, token, music_dir, db) = setup(pool).await;
    std::fs::write(format!("{music_dir}/song.mp3"), b"audio").unwrap();
    std::fs::write(format!("{music_dir}/notes.txt"), b"notes").unwrap();
    let song_id = Uuid::new_v4();
    insert_song(&db, song_id, "song.mp3", true).await;

    let listing = server
        .get("/api/files")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();

    let files = listing["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    let song = files.iter().find(|file| file["path"] == "song.mp3").unwrap();
    assert!(song["is_audio"].as_bool().unwrap());
    assert_eq!(song["song"]["id"].as_str().unwrap(), song_id.to_string());
    assert!(song["song"]["has_cover"].as_bool().unwrap());

    let notes = files.iter().find(|file| file["path"] == "notes.txt").unwrap();
    assert!(!notes["is_audio"].as_bool().unwrap());
    assert!(notes["song"].is_null());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_move_file_updates_song_and_tracking(pool: PgPool) {
    let (server, token, music_dir, db) = setup(pool).await;
    std::fs::create_dir_all(format!("{music_dir}/sub")).unwrap();
    std::fs::write(format!("{music_dir}/a.mp3"), b"audio").unwrap();
    let song_id = Uuid::new_v4();
    insert_song(&db, song_id, "a.mp3", false).await;
    surcast_backend::songs::repository::upsert_library_file(&db, "a.mp3", Some(song_id), None, 5, 1)
        .await
        .unwrap();

    let resp = server
        .post("/api/files/move")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"from": "a.mp3", "to": "sub/a.mp3"}))
        .await;
    assert_eq!(resp.status_code(), 204);
    assert!(!std::path::Path::new(&format!("{music_dir}/a.mp3")).exists());
    assert!(std::path::Path::new(&format!("{music_dir}/sub/a.mp3")).exists());

    let song = surcast_backend::songs::repository::find_song_by_id(&db, song_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(song.file_path, "sub/a.mp3");
    let tracked = surcast_backend::songs::repository::list_library_files(&db).await.unwrap();
    assert_eq!(tracked.len(), 1);
    assert_eq!(tracked[0].path, "sub/a.mp3");

    // Moving into a descendant of the source folder is rejected.
    std::fs::create_dir_all(format!("{music_dir}/sub/deeper")).unwrap();
    let bad = server
        .post("/api/files/move")
        .add_header("Authorization", &format!("Bearer {token}"))
        .json(&json!({"from": "sub", "to": "sub/deeper/sub"}))
        .await;
    assert_eq!(bad.status_code(), 400);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_file_removes_song(pool: PgPool) {
    let (server, token, music_dir, db) = setup(pool).await;
    std::fs::write(format!("{music_dir}/gone.mp3"), b"audio").unwrap();
    let song_id = Uuid::new_v4();
    insert_song(&db, song_id, "gone.mp3", false).await;

    let resp = server
        .delete("/api/files?path=gone.mp3")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["deleted"].as_i64().unwrap(), 1);
    assert!(!std::path::Path::new(&format!("{music_dir}/gone.mp3")).exists());
    assert!(surcast_backend::songs::repository::find_song_by_id(&db, song_id)
        .await
        .unwrap()
        .is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_folder_recursively_removes_songs(pool: PgPool) {
    let (server, token, music_dir, db) = setup(pool).await;
    std::fs::create_dir_all(format!("{music_dir}/rock/2020")).unwrap();
    std::fs::write(format!("{music_dir}/rock/a.mp3"), b"a").unwrap();
    std::fs::write(format!("{music_dir}/rock/2020/b.mp3"), b"b").unwrap();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    insert_song(&db, first, "rock/a.mp3", false).await;
    insert_song(&db, second, "rock/2020/b.mp3", false).await;

    let resp = server
        .delete("/api/files/folders?path=rock")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["deleted"].as_i64().unwrap(), 2);
    assert!(!std::path::Path::new(&format!("{music_dir}/rock")).exists());
    assert!(surcast_backend::songs::repository::find_song_by_id(&db, first)
        .await
        .unwrap()
        .is_none());
    assert!(surcast_backend::songs::repository::find_song_by_id(&db, second)
        .await
        .unwrap()
        .is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_purge_missing_songs(pool: PgPool) {
    let (server, token, music_dir, db) = setup(pool).await;
    std::fs::write(format!("{music_dir}/present.mp3"), b"audio").unwrap();
    let present = Uuid::new_v4();
    let missing = Uuid::new_v4();
    insert_song(&db, present, "present.mp3", false).await;
    insert_song(&db, missing, "vanished.mp3", false).await;
    sqlx::query("UPDATE songs SET missing_since = NOW() WHERE id = $1")
        .bind(missing)
        .execute(&db)
        .await
        .unwrap();

    let resp = server
        .delete("/api/songs/missing")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    assert_eq!(resp.json::<serde_json::Value>()["deleted"].as_i64().unwrap(), 1);
    assert!(surcast_backend::songs::repository::find_song_by_id(&db, missing)
        .await
        .unwrap()
        .is_none());
    assert!(surcast_backend::songs::repository::find_song_by_id(&db, present)
        .await
        .unwrap()
        .is_some());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_song_details_reads_tags_and_properties(pool: PgPool) {
    let (server, token, music_dir, db) = setup(pool).await;
    let song_id = Uuid::new_v4();
    write_mp3_with_cover(&format!("{music_dir}/tagged.mp3"));
    insert_song(&db, song_id, "tagged.mp3", true).await;

    let resp = server
        .get(&format!("/api/songs/{song_id}/details"))
        .add_header("Authorization", &format!("Bearer {token}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    let body = resp.json::<serde_json::Value>();
    assert_eq!(body["extension"].as_str().unwrap(), "mp3");
    assert_eq!(body["track"]["container"].as_str().unwrap(), "Mpeg");
    assert!(body["track"]["has_cover"].as_bool().unwrap());
    let tags = body["track"]["tags"].as_array().unwrap();
    assert!(tags.iter().any(|tag| tag["value"].as_str() == Some("Tag Artist")));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_folder_covers_dedupe_identical_art(pool: PgPool) {
    let (server, token, music_dir, db) = setup(pool).await;
    std::fs::create_dir_all(format!("{music_dir}/rock")).unwrap();

    let first_cover = cover_bytes();
    let mut second_cover = cover_bytes();
    second_cover.extend_from_slice(b"different");

    write_mp3_with_cover_data(&format!("{music_dir}/rock/a.mp3"), &first_cover);
    write_mp3_with_cover_data(&format!("{music_dir}/rock/b.mp3"), &second_cover);
    write_mp3_with_cover_data(&format!("{music_dir}/rock/c.mp3"), &first_cover);

    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let third = Uuid::new_v4();
    insert_song(&db, first, "rock/a.mp3", true).await;
    insert_song(&db, second, "rock/b.mp3", true).await;
    insert_song(&db, third, "rock/c.mp3", true).await;

    let listing = server
        .get("/api/files")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();

    let covers = listing["folders"][0]["covers"].as_array().unwrap();
    assert_eq!(covers.len(), 2);
    assert_eq!(covers[0].as_str().unwrap(), first.to_string());
    assert_eq!(covers[1].as_str().unwrap(), second.to_string());

    // A second listing is served from the cache and stays stable.
    let again = server
        .get("/api/files")
        .add_header("Authorization", &format!("Bearer {token}"))
        .await
        .json::<serde_json::Value>();
    assert_eq!(again["folders"][0]["covers"], listing["folders"][0]["covers"]);
}
