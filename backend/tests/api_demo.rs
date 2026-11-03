mod api_common;

use axum_test::TestServer;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

async fn ws_json(socket: &mut axum_test::TestWebSocket) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let text = socket.receive_text().await;
            // Heartbeat Ping frames have an empty payload.
            if text.is_empty() {
                continue;
            }
            return serde_json::from_str(&text).unwrap();
        }
    })
    .await
    .unwrap()
}

#[sqlx::test(migrations = "./migrations")]
async fn demo_files_are_limited_to_playlists(pool: PgPool) {
    let config = api_common::test_config();
    for path in ["Included/visible.mp3", "Excluded/hidden.mp3"] {
        let path = std::path::Path::new(&config.music_dir).join(path);
        tokio::fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        tokio::fs::write(path, b"audio").await.unwrap();
    }
    let server = TestServer::builder()
        .http_transport()
        .build(api_common::create_test_app_with_config(pool.clone(), config));
    server
        .post("/api/setup/init")
        .json(&json!({"username":"admin","password":"password123"}))
        .await;
    let admin: Value = server
        .post("/api/auth/login")
        .json(&json!({"username":"admin","password":"password123"}))
        .await
        .json();
    let auth = format!("Bearer {}", admin["access_token"].as_str().unwrap());
    let station: Value = server
        .post("/api/stations")
        .add_header("Authorization", &auth)
        .json(&json!({"name":"Source"}))
        .await
        .json();
    let station = station["id"].as_str().unwrap();
    let visible = Uuid::new_v4();
    let hidden = Uuid::new_v4();
    for (id, path) in [(visible, "Included/visible.mp3"), (hidden, "Excluded/hidden.mp3")] {
        sqlx::query("INSERT INTO songs(id,title,file_path) VALUES($1,$2,$3)")
            .bind(id)
            .bind(path)
            .bind(path)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO station_songs(station_id,song_id) VALUES($1,$2)")
            .bind(station.parse::<Uuid>().unwrap())
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    let playlist = Uuid::new_v4();
    sqlx::query("INSERT INTO playlists(id,name,created_by) VALUES($1,'Selected',(SELECT id FROM users WHERE username='admin'))")
        .bind(playlist)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO playlist_songs(playlist_id,song_id) VALUES($1,$2)")
        .bind(playlist)
        .bind(visible)
        .execute(&pool)
        .await
        .unwrap();
    let user: Value = server
        .post("/api/users")
        .add_header("Authorization", &auth)
        .json(&json!({"username":"demo","password":"password123","role":"demo"}))
        .await
        .json();
    let user = user["id"].as_str().unwrap();
    let prepared = server
        .post(&format!("/api/users/{user}/sandbox"))
        .add_header("Authorization", &auth)
        .json(&json!({"station_id":station,"playlist_ids":[playlist]}))
        .await;
    assert_eq!(prepared.status_code(), 200, "{}", prepared.text());
    let login: Value = server
        .post("/api/auth/login")
        .json(&json!({"username":"demo","password":"password123"}))
        .await
        .json();
    let demo = format!("Bearer {}", login["access_token"].as_str().unwrap());

    let root: Value = server.get("/api/files").add_header("Authorization", &demo).await.json();
    let folders: Vec<&str> = root["folders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|folder| folder["name"].as_str().unwrap())
        .collect();
    assert_eq!(folders, ["Included"]);
    assert!(root["files"].as_array().unwrap().is_empty());
    let included: Value = server
        .get("/api/files?path=Included")
        .add_header("Authorization", &demo)
        .await
        .json();
    let files = included["files"].as_array().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0]["name"], "visible.mp3");
    assert_eq!(files[0]["song"]["title"], "Included/visible.mp3");
    server
        .get("/api/files?path=Excluded")
        .add_header("Authorization", &demo)
        .await
        .assert_status_not_found();
    let folders: Vec<String> = server.get("/api/files/folders").add_header("Authorization", &demo).await.json();
    assert_eq!(folders, ["Included"]);

    // The same sandbox on the admin account remains unrestricted.
    let root: Value = server.get("/api/files").add_header("Authorization", &auth).await.json();
    let folders: Vec<&str> = root["folders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|folder| folder["name"].as_str().unwrap())
        .collect();
    assert_eq!(folders, ["Excluded", "Included"]);
}

#[sqlx::test(migrations = "./migrations")]
async fn sandbox_isolated_configured_and_reset(pool: PgPool) {
    let config = api_common::test_config();
    let source_path = "Rock/Artist - Track.mp3";
    tokio::fs::create_dir_all(std::path::Path::new(&config.music_dir).join("Rock"))
        .await
        .unwrap();
    tokio::fs::write(std::path::Path::new(&config.music_dir).join(source_path), b"original-audio")
        .await
        .unwrap();
    let server = TestServer::builder()
        .http_transport()
        .build(api_common::create_test_app_with_config(pool.clone(), config.clone()));
    let setup = server
        .post("/api/setup/init")
        .json(&json!({"username":"admin","password":"password123"}))
        .await;
    assert_eq!(setup.status_code(), 201);
    let admin: Value = server
        .post("/api/auth/login")
        .json(&json!({"username":"admin","password":"password123"}))
        .await
        .json();
    let auth = format!("Bearer {}", admin["access_token"].as_str().unwrap());
    let source: Value = server
        .post("/api/stations")
        .add_header("Authorization", &auth)
        .json(&json!({"name":"Real station","stream_url":"configured-radio.mp3"}))
        .await
        .json();
    let station: Uuid = source["id"].as_str().unwrap().parse().unwrap();
    let song = Uuid::new_v4();
    sqlx::query("INSERT INTO songs(id,title,file_path) VALUES($1,'Original song',$2)")
        .bind(song)
        .bind(source_path)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO station_songs(station_id,song_id) VALUES($1,$2)")
        .bind(station)
        .bind(song)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE icecast_settings SET source_password='source-secret', admin_password='admin-secret', external_source_pw='external-source-secret', external_admin_pw='external-admin-secret', external_url='http://hidden:password@example.com:8000/?token=secret'")
        .execute(&pool).await.unwrap();
    let mut demos = Vec::new();
    for username in ["demo-one", "demo-two"] {
        let response = server
            .post("/api/users")
            .add_header("Authorization", &auth)
            .json(&json!({"username":username,"password":"password123","role":"demo"}))
            .await;
        assert_eq!(response.status_code(), 201);
        let user: Value = response.json();
        let user = user["id"].as_str().unwrap().to_string();
        let prepared = server
            .post(&format!("/api/users/{user}/sandbox"))
            .add_header("Authorization", &auth)
            .json(&json!({"station_id":station}))
            .await;
        assert_eq!(prepared.status_code(), 200, "{}", prepared.text());
        let login = server
            .post("/api/auth/login")
            .json(&json!({"username":username,"password":"password123"}))
            .await;
        assert_eq!(login.status_code(), 200, "{}", login.text());
        let login: Value = login.json();
        demos.push((user, format!("Bearer {}", login["access_token"].as_str().unwrap())));
    }
    let (user, demo) = &demos[0];
    let other: Vec<Value> = server.get("/api/stations").add_header("Authorization", &demos[1].1).await.json();
    let list = server.get("/api/stations").add_header("Authorization", demo).await;
    assert_eq!(list.status_code(), 200, "{}", list.text());
    let list: Vec<Value> = list.json();
    assert_eq!(list.len(), 1);
    let demo_station = list[0]["id"].as_str().unwrap();
    assert_eq!(list[0]["stream_url"], "configured-radio.mp3");
    let demo_mount = list[0]["output_mount"].as_str().unwrap();
    assert!(demo_mount.starts_with("demo-"));
    assert_ne!(demo_mount, "configured-radio.mp3");
    assert_ne!(list[0]["output_mount"], other[0]["output_mount"]);
    let demo_slug = list[0]["slug"].clone();
    let settings = server.put(&format!("/api/stations/{demo_station}")).add_header("Authorization", demo)
        .json(&json!({"prebuffer_bytes":32768,"played_limit":200,"default_fade_ms":4000,"transition_mode":"crossfade","autocue_fade_max_ms":6000})).await;
    assert_eq!(settings.status_code(), 200, "{}", settings.text());
    assert_eq!(settings.json::<Value>()["prebuffer_bytes"], 32768);
    assert_eq!(settings.json::<Value>()["default_fade_ms"], 4000);
    assert_eq!(settings.json::<Value>()["slug"], demo_slug);
    for field in ["name", "description", "stream_url"] {
        let response = server
            .put(&format!("/api/stations/{demo_station}"))
            .add_header("Authorization", demo)
            .json(&json!({(field):"forbidden change", "prebuffer_bytes":65536}))
            .await;
        assert_eq!(response.status_code(), 403);
        assert_eq!(response.json::<Value>()["code"], "DEMO_STATION_READONLY");
    }
    let unchanged: Value = server
        .get(&format!("/api/stations/{demo_station}"))
        .add_header("Authorization", demo)
        .await
        .json();
    assert_eq!(unchanged["prebuffer_bytes"], 32768);
    assert_eq!(unchanged["name"], "Real station");
    assert_eq!(unchanged["stream_url"], "configured-radio.mp3");
    let root: Value = server
        .get(&format!("/api/stations/{station}"))
        .add_header("Authorization", &auth)
        .await
        .json();
    assert_eq!(root["prebuffer_bytes"], 16384);
    assert_eq!(root["stream_url"], "configured-radio.mp3");
    let settings = server.get("/api/admin/icecast").add_header("Authorization", demo).await;
    assert_eq!(settings.status_code(), 200, "{}", settings.text());
    let settings: Value = settings.json();
    assert_eq!(settings["settings"]["source_password"], "");
    assert_eq!(settings["settings"]["admin_password"], "");
    assert!(settings["settings"]["external_source_pw"].is_null());
    assert!(settings["settings"]["external_admin_pw"].is_null());
    assert_eq!(settings["settings"]["external_url"], "http://example.com:8000/");
    assert!(!settings.to_string().contains("secret"));
    assert_ne!(demo_station, station.to_string());
    server
        .get(&format!("/api/stations/{station}"))
        .add_header("Authorization", demo)
        .await
        .assert_status_not_found();
    server
        .get(&format!("/api/stations/{demo_station}"))
        .add_header("Authorization", &demos[1].1)
        .await
        .assert_status_not_found();
    let songs: Value = server.get("/api/songs").add_header("Authorization", demo).await.json();
    let songs = songs.as_array().or_else(|| songs["songs"].as_array()).unwrap();
    assert_eq!(songs.len(), 1);
    let copied_song = songs[0]["id"].as_str().unwrap();
    assert_ne!(copied_song, song.to_string());
    assert_eq!(songs[0]["file_path"], source_path);
    let details: Value = server
        .get(&format!("/api/songs/{copied_song}/details"))
        .add_header("Authorization", demo)
        .await
        .json();
    assert_eq!(details["file_path"], source_path);
    let foreign_song = server
        .post(&format!("/api/stations/{demo_station}/queue"))
        .add_header("Authorization", demo)
        .json(&json!({"song_ids":[song]}))
        .await;
    assert_eq!(foreign_song.status_code(), 400);
    let audio = server
        .get(&format!("/api/songs/{copied_song}/file"))
        .add_header("Authorization", demo)
        .await;
    assert_eq!(audio.status_code(), 200);
    assert_eq!(audio.as_bytes().as_ref(), b"original-audio");
    server
        .get(&format!("/api/songs/{song}/file"))
        .add_header("Authorization", demo)
        .await
        .assert_status_not_found();
    server.get(&format!("/api/songs/{song}/file")).await.assert_status_unauthorized();
    for (method, path) in [
        ("POST", "/api/stations"),
        ("DELETE", "/api/songs/id"),
        ("GET", "/api/users"),
        ("POST", "/api/admin/icecast/start"),
        ("POST", "/api/admin/icecast/stop"),
        ("POST", "/api/uploads"),
    ] {
        let response = match method {
            "POST" => server.post(path).add_header("Authorization", demo).json(&json!({})).await,
            "DELETE" => server.delete(path).add_header("Authorization", demo).await,
            _ => server.get(path).add_header("Authorization", demo).await,
        };
        assert_eq!(response.status_code(), 403);
        assert_eq!(response.json::<Value>()["code"], "DEMO_RESTRICTED");
    }
    let denied = server
        .patch("/api/admin/icecast")
        .add_header("Authorization", demo)
        .json(&json!({"port":9999}))
        .await;
    assert_eq!(denied.status_code(), 403);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT source_password FROM icecast_settings LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "source-secret"
    );
    let playlist = server
        .post("/api/playlists")
        .add_header("Authorization", demo)
        .json(&json!({"name":"Guest playlist"}))
        .await;
    assert_eq!(playlist.status_code(), 201, "{}", playlist.text());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM playlists")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let auto_fill = format!("/api/stations/{demo_station}/auto-fill");
    let response = server.get(&auto_fill).add_header("Authorization", demo).await;
    assert_eq!(response.json::<Value>()["avoid_artist_repeat"], false);
    for avoid in [true, false] {
        let response = server
            .put(&auto_fill)
            .add_header("Authorization", demo)
            .json(&json!({"avoid_artist_repeat":avoid,"songs_ahead":8}))
            .await;
        assert_eq!(response.status_code(), 200, "{}", response.text());
        assert_eq!(response.json::<Value>()["avoid_artist_repeat"], avoid);
        assert_eq!(response.json::<Value>()["songs_ahead"], 8);
    }
    for (resource, body, update) in [
        (
            "schedule-events",
            json!({"title":"Guest event","start_date":"2030-01-01","start_time":"08:00","end_time":"09:00","source_type":"station_library","auto_dj_mode":"random"}),
            json!({"title":"Updated guest event","auto_dj_avoid_repeat":true,"auto_dj_songs_ahead":9}),
        ),
        (
            "schedules",
            json!({"day_of_week":1,"start_time":"10:00","end_time":"11:00","source_type":"station_library","auto_dj_mode":"random"}),
            json!({"end_time":"12:00","auto_dj_avoid_repeat":true,"auto_dj_songs_ahead":9}),
        ),
    ] {
        let path = format!("/api/stations/{demo_station}/{resource}");
        let response = server.post(&path).add_header("Authorization", demo).json(&body).await;
        assert_eq!(response.status_code(), 201, "{}", response.text());
        let id = response.json::<Value>()["id"].as_str().unwrap().to_owned();
        let response = server
            .put(&format!("{path}/{id}"))
            .add_header("Authorization", demo)
            .json(&update)
            .await;
        assert_eq!(response.status_code(), 200, "{}", response.text());
        assert_eq!(response.json::<Value>()["auto_dj_avoid_repeat"], true);
        assert_eq!(response.json::<Value>()["auto_dj_songs_ahead"], 9);
        server
            .delete(&format!("{path}/{id}"))
            .add_header("Authorization", demo)
            .await
            .assert_status_no_content();
        // Leave a guest entry to verify reset removes it as well.
        let response = server.post(&path).add_header("Authorization", demo).json(&body).await;
        assert_eq!(response.status_code(), 201, "{}", response.text());
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM station_schedule_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM station_schedules")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT avoid_artist_repeat FROM station_auto_fill WHERE station_id=$1")
            .bind(station)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    let mut socket = server.get_websocket("/api/ws").await.into_websocket().await;
    socket
        .send_json(&json!({"type":"auth","token":demo.strip_prefix("Bearer ").unwrap()}))
        .await;
    let message = ws_json(&mut socket).await;
    assert_eq!(message["type"], "auth_ok");
    socket.send_json(&json!({"type":"subscribe","station_id":station})).await;
    let message = ws_json(&mut socket).await;
    assert_eq!(message["type"], "error");
    assert_eq!(message["data"], "unknown station");
    socket.send_json(&json!({"type":"subscribe","station_id":demo_station})).await;
    // A stopped sandbox still emits its own queue snapshot, not real data.
    let mut saw_queue = false;
    for _ in 0..3 {
        let message = ws_json(&mut socket).await;
        assert_eq!(message["station_id"], demo_station);
        if message["type"] == "queue_update" {
            saw_queue = true;
            break;
        }
    }
    assert!(saw_queue);
    let response = server
        .post(&format!("/api/users/{user}/sandbox/reset"))
        .add_header("Authorization", &auth)
        .await;
    assert_eq!(response.status_code(), 200, "{}", response.text());
    let playlists: Vec<Value> = server.get("/api/playlists").add_header("Authorization", demo).await.json();
    assert_eq!(playlists.len(), 1);
    assert_eq!(playlists[0]["name"], "Demo");
    let restored: Value = server
        .get(&format!("/api/stations/{demo_station}"))
        .add_header("Authorization", demo)
        .await
        .json();
    assert_eq!(restored["stream_url"], "configured-radio.mp3");
    assert_ne!(restored["output_mount"], list[0]["output_mount"]);
    assert_eq!(restored["prebuffer_bytes"], 16384);
    for resource in ["schedule-events", "schedules"] {
        let items: Vec<Value> = server
            .get(&format!("/api/stations/{demo_station}/{resource}"))
            .add_header("Authorization", demo)
            .await
            .json();
        assert!(items.is_empty());
    }
    let response = server.get(&auto_fill).add_header("Authorization", demo).await;
    assert_eq!(response.json::<Value>()["avoid_artist_repeat"], false);
    assert_eq!(response.json::<Value>()["songs_ahead"], 4);
    let queue: Vec<Value> = server
        .get(&format!("/api/stations/{demo_station}/queue"))
        .add_header("Authorization", demo)
        .await
        .json();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0]["song_id"], copied_song);
    let details: Value = server
        .get(&format!("/api/songs/{copied_song}/details"))
        .add_header("Authorization", demo)
        .await
        .json();
    assert_eq!(details["file_path"], source_path);
    let audio = server
        .get(&format!("/api/songs/{copied_song}/file"))
        .add_header("Authorization", demo)
        .await;
    assert_eq!(audio.as_bytes().as_ref(), b"original-audio");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'demo_%'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        tokio::fs::read(std::path::Path::new(&config.music_dir).join(source_path))
            .await
            .unwrap(),
        b"original-audio"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM songs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    // Replacing a seed retires its audio without touching other sandboxes.
    let old_library: String = sqlx::query_scalar("SELECT seed->>'library_id' FROM demo_sandboxes WHERE user_id=$1")
        .bind(user.parse::<Uuid>().unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
    let old_audio = std::path::PathBuf::from(format!("{}.sandboxes/{user}", config.music_dir))
        .join(&old_library)
        .join(source_path);
    tokio::fs::write(std::path::Path::new(&config.music_dir).join(source_path), b"updated-source-audio")
        .await
        .unwrap();
    let prepared = server
        .post(&format!("/api/users/{user}/sandbox"))
        .add_header("Authorization", &auth)
        .json(&json!({"station_id":station}))
        .await;
    assert_eq!(prepared.status_code(), 200, "{}", prepared.text());
    assert!(!old_audio.exists());
    let fresh: Vec<Value> = server.get("/api/songs").add_header("Authorization", demo).await.json();
    assert_eq!(fresh[0]["file_path"], source_path);
    let fresh_id = fresh[0]["id"].as_str().unwrap();
    let fresh_audio = server
        .get(&format!("/api/songs/{fresh_id}/file"))
        .add_header("Authorization", demo)
        .await;
    assert_eq!(fresh_audio.as_bytes().as_ref(), b"updated-source-audio");
    let other_songs: Vec<Value> = server.get("/api/songs").add_header("Authorization", &demos[1].1).await.json();
    let other_id = other_songs[0]["id"].as_str().unwrap();
    let other_audio = server
        .get(&format!("/api/songs/{other_id}/file"))
        .add_header("Authorization", &demos[1].1)
        .await;
    assert_eq!(other_audio.as_bytes().as_ref(), b"original-audio");
    // Deleting a demo account also retires its isolated output/schema.
    server
        .delete(&format!("/api/users/{user}"))
        .add_header("Authorization", &auth)
        .await
        .assert_status_no_content();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'demo_%'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    server
        .get("/api/stations")
        .add_header("Authorization", demo)
        .await
        .assert_status_unauthorized();
}
