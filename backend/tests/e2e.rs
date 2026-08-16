mod api_common;

use axum_test::TestServer;
use serde_json::Value;
use sqlx::PgPool;

async fn setup(db: PgPool) -> (TestServer, String) {
    let app = api_common::create_test_app(db);
    let server = TestServer::new(app);
    let token = {
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username": "admin", "password": "admin123", "name": "Admin"}))
            .await;
        let resp = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username": "admin", "password": "admin123"}))
            .await;
        resp.json::<Value>()["access_token"].as_str().unwrap().to_string()
    };
    (server, token)
}

fn auth(token: &str) -> String {
    format!("Bearer {token}")
}

#[sqlx::test(migrations = "./migrations")]
async fn test_e2e_full_flow(db: PgPool) {
    let (server, token) = setup(db.clone()).await;

    // 1. Check setup is complete
    let resp = server.get("/api/setup/status").await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    assert!(resp.json::<Value>()["setup_complete"].as_bool().unwrap());

    // 2. Get current user
    let resp = server.get("/api/auth/me").add_header("Authorization", &auth(&token)).await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    assert_eq!(resp.json::<Value>()["username"], "admin");

    // 3. Create station
    let resp = server
        .post("/api/stations")
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"name": "Test Station", "description": "E2E test"}))
        .await;
    assert_eq!(resp.status_code(), 201);
    let station_id = resp.json::<Value>()["id"].as_str().unwrap().to_string();

    // 4. List stations
    let resp = server.get("/api/stations").add_header("Authorization", &auth(&token)).await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    assert!(!resp.json::<Vec<Value>>().is_empty());

    // 5. Get station
    let resp = server
        .get(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    assert_eq!(resp.json::<Value>()["name"], "Test Station");

    // 6. Update station
    let resp = server
        .put(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"name": "Updated Station"}))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 7. Create a song record directly via DB (multipart upload requires file content)
    let admin_id: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username = 'admin'")
        .fetch_one(&db)
        .await
        .expect("admin user not found");
    sqlx::query(
        "INSERT INTO songs (id, title, artist, album, file_path, file_size, mime_type, duration, uploaded_by) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind("00000000-0000-0000-0000-000000000001")
    .bind("Test Song")
    .bind("Test Artist")
    .bind("Test Album")
    .bind("test.mp3")
    .bind(1024i64)
    .bind("audio/mpeg")
    .bind(30i32)
    .bind(admin_id.0)
    .execute(&db)
    .await
    .expect("insert song failed");

    // 8. List songs
    let resp = server.get("/api/songs").add_header("Authorization", &auth(&token)).await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    let songs = resp.json::<Vec<Value>>();
    assert!(!songs.is_empty());

    let song_id = "00000000-0000-0000-0000-000000000001";

    // 9. Assign song to station
    let resp = server
        .post(&format!("/api/songs/{song_id}/stations"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"station_ids": [station_id]}))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 10. Check station has the song in library
    let resp = server
        .get(&format!("/api/stations/{station_id}/songs"))
        .add_header("Authorization", &auth(&token))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    let body = resp.json::<Value>();
    assert!(!body["songs"].as_array().unwrap().is_empty());

    // 11. Add song to station queue
    let resp = server
        .post(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"song_ids": [song_id]}))
        .await;
    assert!(resp.status_code() == 200 || resp.status_code() == 201);

    // 12. List queue
    let resp = server
        .get(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", &auth(&token))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    let queue = resp.json::<Vec<Value>>();
    assert!(!queue.is_empty());

    // 13. Create playlist
    let resp = server
        .post("/api/playlists")
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"name": "Test Playlist", "description": "E2E"}))
        .await;
    assert_eq!(resp.status_code(), 201);
    let playlist_id = resp.json::<Value>()["id"].as_str().unwrap().to_string();

    // 14. Add song to playlist
    let resp = server
        .post(&format!("/api/playlists/{playlist_id}/songs"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"song_ids": [song_id]}))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 15. List playlist songs
    let resp = server
        .get(&format!("/api/playlists/{playlist_id}/songs"))
        .add_header("Authorization", &auth(&token))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 16. Add playlist to queue
    let resp = server
        .post(&format!("/api/playlists/{playlist_id}/add-to-queue/{station_id}"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({}))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 17. Create schedule event
    let resp = server
        .post(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({
            "start_date": "2030-01-01",
            "start_time": "10:00",
            "end_time": "12:00",
            "source_type": "playlist",
            "playlist_id": playlist_id,
            "recurrence_type": "weekly"
        }))
        .await;
    assert_eq!(resp.status_code(), 201);
    let event_id = resp.json::<Value>()["id"].as_str().unwrap().to_string();

    // 18. List schedule events
    let resp = server
        .get(&format!("/api/stations/{station_id}/schedule-events"))
        .add_header("Authorization", &auth(&token))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    let events = resp.json::<Vec<Value>>();
    assert!(events.iter().any(|e| e["id"] == event_id));

    // 19. Update schedule event
    let resp = server
        .put(&format!("/api/stations/{station_id}/schedule-events/{event_id}"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"title": "Updated Event"}))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 20. Delete schedule event
    let resp = server
        .delete(&format!("/api/stations/{station_id}/schedule-events/{event_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    assert_eq!(resp.status_code(), 204);

    // 21. Refresh token
    let login_resp = server
        .post("/api/auth/login")
        .json(&serde_json::json!({"username": "admin", "password": "admin123"}))
        .await;
    let refresh_token = login_resp.json::<Value>()["refresh_token"].as_str().unwrap().to_string();
    let resp = server
        .post("/api/auth/refresh")
        .json(&serde_json::json!({"refresh_token": refresh_token}))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    assert!(resp.json::<Value>()["access_token"].as_str().is_some());

    // 22. Create API key
    let resp = server
        .post("/api/api-keys")
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"name": "Test Key"}))
        .await;
    assert_eq!(resp.status_code(), 201);
    let api_key = resp.json::<Value>()["key"].as_str().unwrap().to_string();
    assert!(api_key.starts_with("sur_"));

    // 23. Use API key for auth
    let resp = server
        .get("/api/stations")
        .add_header("Authorization", &format!("Bearer {api_key}"))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 24. Update song
    let resp = server
        .put(&format!("/api/songs/{song_id}"))
        .add_header("Authorization", &auth(&token))
        .json(&serde_json::json!({"title": "Updated Title"}))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 25. Delete playlist song
    let resp = server
        .delete(&format!("/api/playlists/{playlist_id}/songs/{song_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    assert_eq!(resp.status_code(), 204);

    // 26. Remove song from station
    let resp = server
        .delete(&format!("/api/songs/{song_id}/stations/{station_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");

    // 27. Delete station
    let resp = server
        .delete(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    assert_eq!(resp.status_code(), 204);

    // 28. Verify station is gone
    let resp = server
        .get(&format!("/api/stations/{station_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    assert_eq!(resp.status_code(), 404);

    // 29. Delete playlist
    let resp = server
        .delete(&format!("/api/playlists/{playlist_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    assert_eq!(resp.status_code(), 204);

    // 30. Delete song
    let resp = server
        .delete(&format!("/api/songs/{song_id}"))
        .add_header("Authorization", &auth(&token))
        .await;
    assert_eq!(resp.status_code(), 204);

    // 31. List users (admin only)
    let resp = server.get("/api/users").add_header("Authorization", &auth(&token)).await;
    let status = resp.status_code();
    assert!(status == 200 || status == 201 || status == 204, "expected 200 or 201, got {status}");
    let users = resp.json::<Vec<Value>>();
    assert!(users.iter().any(|u| u["username"] == "admin"));
}
