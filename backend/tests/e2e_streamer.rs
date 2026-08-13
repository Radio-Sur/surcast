#[allow(dead_code)]
mod api_common;
mod common;

use axum_test::TestServer;
use chrono::Datelike;
use serial_test::serial;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use surcast_backend::{
    api::router::{self, StreamersMap},
    icecast::IcecastManager,
    listeners::ListenersState,
};
use tempfile::TempDir;

fn wav(frequency: f32) -> Vec<u8> {
    wav_for(frequency, 10)
}

fn wav_for(frequency: f32, seconds: u32) -> Vec<u8> {
    let rate = 44_100u32;
    let frames = rate * seconds;
    let size = frames * 4;
    let mut bytes = Vec::with_capacity(44 + size as usize);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36 + size).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&2u16.to_le_bytes());
    bytes.extend_from_slice(&rate.to_le_bytes());
    bytes.extend_from_slice(&(rate * 4).to_le_bytes());
    bytes.extend_from_slice(&4u16.to_le_bytes());
    bytes.extend_from_slice(&16u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&size.to_le_bytes());
    for frame in 0..frames {
        let sample = ((frame as f32 * frequency * std::f32::consts::TAU / rate as f32).sin() * 6_000.0) as i16;
        bytes.extend_from_slice(&sample.to_le_bytes());
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    bytes
}
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn failure(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(message.into()))
}

async fn wait_for_status(
    server: &TestServer,
    station_id: &str,
    auth: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = server
            .get(&format!("/api/stations/{station_id}/stream/status"))
            .add_header("Authorization", auth)
            .await;
        let status = response.json::<serde_json::Value>();
        if predicate(&status) {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(failure(format!("stream status did not converge: {status}")));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn open_mount(client: &reqwest::Client, url: &str) -> Result<reqwest::Response, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match client.get(url).header("Icy-MetaData", "1").send().await {
            Ok(response) if response.status().is_success() => return Ok(response),
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(200)).await,
            _ => return Err(failure("Icecast mount did not become available")),
        }
    }
}

fn queue_titles(items: &serde_json::Value) -> Vec<String> {
    items
        .as_array()
        .map(|items| items.iter().map(|item| item["title"].as_str().unwrap_or("").to_string()).collect())
        .unwrap_or_default()
}

async fn fetch_queue(server: &TestServer, station_id: &str, auth: &str) -> serde_json::Value {
    server
        .get(&format!("/api/stations/{station_id}/queue"))
        .add_header("Authorization", auth)
        .await
        .json::<serde_json::Value>()
}

#[tokio::test]
#[serial]
async fn managed_icecast_serves_gstreamer_encoded_mp3() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;

        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "E2E",
                "stream_url": "e2e-stream",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        let first_song = uuid::Uuid::new_v4();
        let second_song = uuid::Uuid::new_v4();
        std::fs::create_dir(files.path().join("audio"))?;
        std::fs::write(files.path().join("audio/first.wav"), wav(440.0))?;
        std::fs::write(files.path().join("audio/second.wav"), wav(660.0))?;
        sqlx::query(
            "INSERT INTO songs \
             (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) VALUES \
             ($1,'first tone','test','first.wav',1,'audio/wav',10,$3), \
             ($2,'second tone','test','second.wav',1,'audio/wav',10,$3)",
        )
        .bind(first_song)
        .bind(second_song)
        .bind(admin.0)
        .execute(&db)
        .await?;

        for song in [first_song, second_song] {
            let response = server
                .post(&format!("/api/songs/{song}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if response.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", response.text())));
            }
        }
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids":[first_song, second_song]}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let first_queue_item_id = queued.json::<serde_json::Value>()[0]["id"]
            .as_str()
            .ok_or_else(|| failure("queue creation response has no first queue item id"))?
            .to_owned();

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"] == "first tone"
        })
        .await?;

        let url = format!("http://127.0.0.1:{port}/e2e-stream.mp3");
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let mut response = open_mount(&client, &url).await?;
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("audio/mpeg"))
        {
            return Err(failure("Icecast mount is not audio/mpeg"));
        }
        let chunk = tokio::time::timeout(Duration::from_secs(15), response.chunk()).await??;
        if chunk.is_none() {
            return Err(failure("Icecast mount returned EOF before an MP3 chunk"));
        }
        drop(response);

        let checkpoint = wait_for_status(&server, &station_id, &auth, |status| status["elapsed"].as_u64().unwrap_or(0) >= 1).await?;
        let checkpoint_elapsed = checkpoint["elapsed"].as_u64().unwrap_or(0);
        let checkpoint_index = checkpoint["song_index"].as_u64();
        let checkpoint_title = checkpoint["title"].as_str().map(str::to_owned);
        let restarted_icecast = server
            .patch("/api/admin/icecast")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"enabled":true}))
            .await;
        if restarted_icecast.status_code() != 200 {
            return Err(failure(format!("Icecast restart failed: {}", restarted_icecast.text())));
        }
        let reconnected = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["elapsed"].as_u64().unwrap_or(0) > checkpoint_elapsed
        })
        .await?;
        if reconnected["song_index"].as_u64() != checkpoint_index || reconnected["title"].as_str() != checkpoint_title.as_deref() {
            return Err(failure(format!(
                "Icecast reconnect changed the active track: {checkpoint} -> {reconnected}"
            )));
        }
        let mut response = open_mount(&client, &url).await?;
        let chunk = tokio::time::timeout(Duration::from_secs(15), response.chunk()).await??;
        if chunk.is_none() {
            return Err(failure("reconnected mount returned EOF before an MP3 chunk"));
        }
        drop(response);

        let paused = server
            .post(&format!("/api/stations/{station_id}/stream/pause"))
            .add_header("Authorization", &auth)
            .await;
        if paused.status_code() != 200 {
            return Err(failure(format!("pause failed: {}", paused.text())));
        }
        let paused_once = wait_for_status(&server, &station_id, &auth, |status| status["playing"] == false).await?;
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let paused_twice = wait_for_status(&server, &station_id, &auth, |status| status["playing"] == false).await?;
        if paused_once["elapsed"] != paused_twice["elapsed"] {
            return Err(failure(format!("elapsed advanced while paused: {paused_once} -> {paused_twice}")));
        }
        let paused_elapsed = paused_twice["elapsed"].as_u64().unwrap_or(0);
        let played = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if played.status_code() != 200 {
            return Err(failure(format!("play failed: {}", played.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["elapsed"].as_u64().unwrap_or(0) > paused_elapsed
        })
        .await?;

        let skipped = server
            .post(&format!("/api/stations/{station_id}/stream/skip"))
            .add_header("Authorization", &auth)
            .await;
        if skipped.status_code() != 200 {
            return Err(failure(format!("skip failed: {}", skipped.text())));
        }
        let second = wait_for_status(&server, &station_id, &auth, |status| {
            status["song_index"] == 1 && status["title"] == "second tone"
        })
        .await?;
        let inserted = server
            .post(&format!("/api/stations/{station_id}/queue/insert"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_id": first_song, "position": 0}))
            .await;
        if inserted.status_code() != 200 {
            return Err(failure(format!("queue insertion before current failed: {}", inserted.text())));
        }
        let removed = server
            .delete(&format!("/api/stations/{station_id}/queue/{first_queue_item_id}"))
            .add_header("Authorization", &auth)
            .await;
        if removed.status_code() != 204 {
            return Err(failure(format!("queue removal before current failed: {}", removed.text())));
        }
        let retained = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"] == "second tone"
        })
        .await?;
        if retained["song_index"].as_u64() != Some(1) {
            return Err(failure(format!("queue reload changed the active track: {retained}")));
        }
        let queue_after_mutation = server
            .get(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .await
            .json::<serde_json::Value>();
        if queue_after_mutation
            .as_array()
            .is_none_or(|items| items.iter().any(|item| item["id"] == first_queue_item_id))
        {
            return Err(failure(format!("deleted queue item returned after reload: {queue_after_mutation}")));
        }
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let second_stable = wait_for_status(&server, &station_id, &auth, |status| status["playing"] == true).await?;
        if second_stable["song_index"] != second["song_index"] || second_stable["title"] != second["title"] {
            return Err(failure(format!("skip advanced more than once: {second} -> {second_stable}")));
        }

        let restarted = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if restarted.status_code() != 200 {
            return Err(failure(format!("stream restart failed: {}", restarted.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| status["playing"] == true).await?;
        if streamers.lock().unwrap().len() != 1 {
            return Err(failure("stream restart created more than one station pipeline"));
        }
        let mut response = open_mount(&client, &url).await?;
        if tokio::time::timeout(Duration::from_secs(15), response.chunk()).await??.is_none() {
            return Err(failure("restarted stream returned EOF before an MP3 chunk"));
        }
        drop(response);

        let stopped = server
            .post(&format!("/api/stations/{station_id}/stream/stop"))
            .add_header("Authorization", &auth)
            .await;
        if stopped.status_code() != 200 {
            return Err(failure(format!("stream stop failed: {}", stopped.text())));
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let unavailable = client
                .get(&url)
                .send()
                .await
                .map_or(true, |response| !response.status().is_success());
            if unavailable {
                break;
            }
            if Instant::now() >= deadline {
                return Err(failure("Icecast mount remained available after stream stop"));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        if !streamers.lock().unwrap().is_empty() {
            return Err(failure("stopped stream reconnected itself"));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn crossfade_naturally_promotes_each_queued_track_once() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Natural crossfade",
                "stream_url": "natural-crossfade",
                "prebuffer_bytes": 1024,
                "transition_mode": "crossfade"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let station_uuid = uuid::Uuid::parse_str(&station_id)?;
        sqlx::query("UPDATE stations SET transition_mode='crossfade', default_fade_ms=500 WHERE id=$1")
            .bind(station_uuid)
            .execute(&db)
            .await?;
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C"), (660.0, "tone D")];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("natural-{index}.wav")),
                    wav_for(*frequency, 10),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title, format!("natural-{index}.wav")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (song_id, title, file_path) in &song_ids {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',10,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(file_path)
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }
        let queue_ids = song_ids.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": queue_ids}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/natural-crossfade.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);

        for (index, (_, title, _)) in song_ids.iter().enumerate() {
            let status = wait_for_status(&server, &station_id, &auth, |status| {
                status["title"].as_str() == Some(*title) && (status["playing"] == true || index + 1 == song_ids.len())
            })
            .await?;
            if status["song_index"].as_u64() != Some(index as u64) {
                return Err(failure(format!("natural transition selected wrong queue index: {status}")));
            }
            if index > 0 && status["elapsed"].as_u64() != Some(0) {
                return Err(failure(format!("promoted track did not reset elapsed: {status}")));
            }
        }
        let stopped = wait_for_status(&server, &station_id, &auth, |status| status["playing"] == false).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let stable = wait_for_status(&server, &station_id, &auth, |status| status["playing"] == false).await?;
        if stable["song_index"] != stopped["song_index"] || stable["title"] != stopped["title"] {
            return Err(failure(format!("exhausted queue advanced or retried: {stopped} -> {stable}")));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn manual_auto_dj_trigger_keeps_an_exhausted_memory_queue_playing() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Auto DJ trigger",
                "stream_url": "auto-dj-trigger",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(files.path().join("audio").join(format!("dj-{index}.wav")), wav_for(*frequency, 10))?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',10,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("dj-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }

        // Queue only the first track; the other two stay in the station library
        // as Auto DJ picks.
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": [song_ids[0].0]}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/auto-dj-trigger.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);

        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;

        // Enable Auto DJ and fill the queue manually. The trigger must refresh
        // the live streamer's in-memory queue, not just the DB.
        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 2,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }
        let triggered = server
            .post(&format!("/api/stations/{station_id}/auto-fill/trigger"))
            .add_header("Authorization", &auth)
            .await;
        if triggered.status_code() != 200 {
            return Err(failure(format!("auto-fill trigger failed: {}", triggered.text())));
        }

        // The streamer must now see 3 tracks (1 queued + 2 Auto DJ picks).
        let synced = wait_for_status(&server, &station_id, &auth, |status| status["total"].as_u64() == Some(3)).await?;
        if synced["song_index"].as_u64() != Some(0) {
            return Err(failure(format!("auto-fill sync moved the cursor: {synced}")));
        }

        // The queued track ends: the controller must reload the queue from the
        // DB and keep playing an Auto DJ pick instead of stopping the radio.
        let advanced = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str() != Some("tone A")
        })
        .await?;
        if advanced["song_index"].as_u64() != Some(1) {
            return Err(failure(format!("auto-fill pick played at the wrong index: {advanced}")));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn play_with_empty_queue_fills_from_auto_dj_and_starts() {
    // Regression: pressing play with an empty queue used to leave the
    // streamer Stopped forever, even with Auto DJ enabled and a library full
    // of songs. play() must give Auto DJ one chance to fill the queue, reload
    // it, and start broadcasting.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Empty play Auto DJ",
                "stream_url": "empty-play-autodj",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        // Songs live only in the station library; the queue itself stays empty.
        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("empty-dj-{index}.wav")),
                    wav_for(*frequency, 4),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("empty-dj-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }

        // Enable Auto DJ before starting; play must trigger the fill itself.
        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 2,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("play on empty queue failed: {}", started.text())));
        }

        // The queue must be populated by Auto DJ and playback must start
        // instead of staying Stopped. songs_ahead=2 means the upcoming window
        // holds two picks: three rows total with the current track.
        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["total"].as_u64() == Some(3)
        })
        .await
        .map_err(|error| failure(format!("streamer stayed stopped after play on an empty queue: {error}")))?;
        let title = playing["title"].as_str().unwrap_or("").to_owned();
        if !["tone A", "tone B", "tone C"].contains(&title.as_str()) {
            return Err(failure(format!("Auto DJ pick has an unexpected title: {playing}")));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/empty-play-autodj.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn natural_queue_exhaustion_refills_from_auto_dj() {
    // Regression: when the last queued track ends and nothing is queued
    // behind it, the controller must give Auto DJ a chance to fill the queue
    // instead of stopping the radio for good.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Natural exhaustion Auto DJ",
                "stream_url": "natural-exhaustion-autodj",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("exhaust-dj-{index}.wav")),
                    wav_for(*frequency, 4),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("exhaust-dj-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }

        // Queue only the first track; the others stay in the station library
        // as Auto DJ picks once the queue runs dry.
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": [song_ids[0].0]}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 2,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/natural-exhaustion-autodj.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);

        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;

        // tone A ends with nothing queued behind it: the controller must
        // refill from Auto DJ and continue instead of stopping.
        let advanced = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str() != Some("tone A")
        })
        .await
        .map_err(|error| failure(format!("playback stopped after queue exhaustion: {error}")))?;
        // The queue must now hold the played track plus at least the two
        // songs_ahead picks; the exact count can grow as later refills top up.
        if advanced["total"].as_u64().unwrap_or(0) < 3 {
            return Err(failure(format!("Auto DJ refill did not populate the queue: {advanced}")));
        }
        if advanced["song_index"].as_u64() != Some(1) {
            return Err(failure(format!("Auto DJ pick played at the wrong index: {advanced}")));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn repro_audio_survives_last_track_refill_after_empty_queue_start() {
    // The user's report: start pressed with an empty queue, AutoDJ added one
    // track, it played to the end, AutoDJ then refilled the queue — the panel
    // showed the new tracks as playing, but the broadcast was actually dead
    // after the first track. Status only reflects controller/pipeline state,
    // so the Icecast mount itself must be cross-checked: it must serve audio
    // after the last-track EOS refill transition, not just report playing.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Empty start audio repro",
                "stream_url": "empty-start-audio-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        // Songs live only in the station library; the queue stays empty.
        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("audio-repro-{index}.wav")),
                    wav_for(*frequency, 4),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("audio-repro-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }
        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 1,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }

        // Start with an empty database queue: play()'s own refill adds the
        // first Auto DJ pick, which plays with nothing staged behind it and
        // ends through the EOS path (no natural handover) — the exact state
        // the user hit.
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("play on empty queue failed: {}", started.text())));
        }
        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            // songs_ahead=1: the current track plus one upcoming pick.
            status["playing"] == true && status["total"].as_u64() == Some(2)
        })
        .await
        .map_err(|error| failure(format!("streamer stayed stopped after play on an empty queue: {error}")))?;
        let first_title = playing["title"].as_str().unwrap_or("").to_owned();
        if first_title.is_empty() {
            return Err(failure(format!("no first Auto DJ pick: {playing}")));
        }

        let url = format!("http://127.0.0.1:{port}/empty-start-audio-repro.mp3");
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let mut response = open_mount(&client, &url).await?;
        let chunk = tokio::time::timeout(Duration::from_secs(15), response.chunk()).await??;
        if chunk.is_none() || chunk.as_ref().is_none_or(|bytes| bytes.is_empty()) {
            return Err(failure("mount returned no audio before the first track ended"));
        }
        drop(response);

        // The first track ends with nothing staged behind it: the controller
        // must refill from Auto DJ and keep the broadcast alive.
        let advanced = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str() != Some(first_title.as_str())
        })
        .await
        .map_err(|error| failure(format!("controller did not advance after the last queued track ended: {error}")))?;
        if advanced["total"].as_u64().unwrap_or(0) < 2 {
            return Err(failure(format!("Auto DJ refill did not populate the queue: {advanced}")));
        }

        // The broadcast itself must survive the transition: the mount has to
        // serve real audio again after the refill, not just report playing.
        let mut response = open_mount(&client, &url).await?;
        let chunk = tokio::time::timeout(Duration::from_secs(15), response.chunk()).await??;
        if chunk.is_none() || chunk.as_ref().is_none_or(|bytes| bytes.is_empty()) {
            return Err(failure(format!(
                "mount served no audio after the refill transition; status claimed: {advanced}"
            )));
        }
        drop(response);
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn repro_ws_feed_reports_stopped_after_queue_exhaustion() {
    // The user's report: after the station stopped itself (queue exhausted,
    // nothing to refill), the panel kept showing the last playing state — the
    // live feed only pushes status on events, and the exhaustion stop pushed
    // none. The feed must report playing=false when the station stops itself.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::builder().http_transport().build(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        let token = auth.trim_start_matches("Bearer ").to_owned();
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "WS stopped feed repro",
                "stream_url": "ws-stopped-feed-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let song_id = uuid::Uuid::new_v4();
        std::fs::write(files.path().join("audio").join("ws-stopped.wav"), wav_for(330.0, 4))?;
        sqlx::query(
            "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
             VALUES ($1,'tone A','test','ws-stopped.wav',1,'audio/wav',4,$2)",
        )
        .bind(song_id)
        .bind(admin.0)
        .execute(&db)
        .await?;
        let _assigned = server
            .post(&format!("/api/songs/{song_id}/stations"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
            .await;
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": [song_id]}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;

        let mut address = server.server_address().ok_or_else(|| failure("no server address"))?;
        address.set_scheme("ws").map_err(|_| failure("bad address"))?;
        let ws_url = address.join("/api/ws").map_err(|_| failure("bad ws url"))?;
        let (mut socket, _) = tokio_tungstenite::connect_async(ws_url.to_string()).await?;
        use futures::SinkExt;
        use tokio_tungstenite::tungstenite::Message as WsMessage;
        socket
            .send(WsMessage::Text(
                serde_json::json!({"type": "auth", "token": token}).to_string().into(),
            ))
            .await?;
        socket
            .send(WsMessage::Text(
                serde_json::json!({"type": "subscribe", "station_id": station_id})
                    .to_string()
                    .into(),
            ))
            .await?;
        let _auth_ok = ws_recv_text(&mut socket, 10).await?;
        // Drain the initial snapshot (status + queue_update); the first
        // status must report the station as playing.
        let mut saw_playing = false;
        for _ in 0..8 {
            let msg: serde_json::Value = serde_json::from_str(&ws_recv_text(&mut socket, 10).await?)?;
            if msg["type"] == "status" && msg["data"]["data"]["playing"] == true {
                saw_playing = true;
            }
            if msg["type"] == "queue_update" {
                break;
            }
        }
        if !saw_playing {
            return Err(failure("initial live status did not report playing"));
        }

        // The single track ends: no Auto DJ is configured, so the controller
        // stops the station. The live feed must push the stopped state; the
        // panel must not keep the last playing snapshot forever.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let msg: serde_json::Value = serde_json::from_str(&ws_recv_text(&mut socket, 10).await?)?;
            if msg["type"] == "status" && msg["data"]["data"]["playing"] == false {
                break;
            }
            if Instant::now() >= deadline {
                return Err(failure(format!(
                    "live feed never reported the exhausted station as stopped; last message: {msg}"
                )));
            }
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error:?}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn mixed_queue_drain_keeps_playing_with_auto_dj_picks() {
    // User report: a station whose queue held manually added songs plus
    // Auto DJ picks stopped broadcasting after those songs finished, even
    // though Auto DJ was enabled. Playback must roll from the manual tail
    // into Auto DJ picks and keep refilling across handovers.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Mixed drain Auto DJ",
                "stream_url": "mixed-drain-autodj",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [
            (330.0, "tone A"),
            (440.0, "tone B"),
            (550.0, "tone C"),
            (660.0, "tone D"),
            (770.0, "tone E"),
        ];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("mixed-dj-{index}.wav")),
                    wav_for(*frequency, 4),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("mixed-dj-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }

        // Queue three manual songs; the last two stay in the library as
        // Auto DJ picks.
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": [song_ids[0].0, song_ids[1].0, song_ids[2].0]}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 2,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/mixed-drain-autodj.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);

        // The manual songs play through one by one...
        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone B") && status["playing"] == true
        })
        .await?;
        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone C") && status["playing"] == true
        })
        .await?;

        // ...and when the manual tail ends the radio must roll into Auto DJ
        // picks instead of stopping.
        let pick = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && matches!(status["title"].as_str(), Some("tone D") | Some("tone E"))
        })
        .await
        .map_err(|error| failure(format!("radio stopped after the manual queue drained: {error}")))?;
        if pick["total"].as_u64().unwrap_or(0) < 4 {
            return Err(failure(format!("Auto DJ did not refill past the manual tail: {pick}")));
        }
        let first_pick = pick["title"].as_str().unwrap_or("").to_owned();
        let second = if first_pick == "tone D" { "tone E" } else { "tone D" };
        // The next handover must keep playing another Auto DJ pick, proving
        // the refill keeps the queue alive across transitions.
        wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str() == Some(second)
        })
        .await
        .map_err(|error| failure(format!("playback stopped between Auto DJ picks: {error}")))?;
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn auto_dj_keeps_songs_ahead_with_crossfade_handovers() {
    // User report: AutoDJ fills the queue only when it is completely empty;
    // as the player consumes tracks one by one the queue shrinks below the
    // configured songs_ahead minimum and nothing tops it up. The queue must
    // be refilled at every handover so upcoming stays at songs_ahead.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Crossfade keep-ahead",
                "stream_url": "crossfade-keep-ahead",
                "prebuffer_bytes": 1024,
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [
            (330.0, "ka A"),
            (440.0, "ka B"),
            (550.0, "ka C"),
            (660.0, "ka D"),
            (770.0, "ka E"),
            (880.0, "ka F"),
        ];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("keep-ahead-{index}.wav")),
                    wav_for(*frequency, 4),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("keep-ahead-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }

        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": true,
                "songs_ahead": 4,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }

        async fn upcoming_rows(db: &sqlx::PgPool, station_id: &str) -> i64 {
            let station_uuid = uuid::Uuid::parse_str(station_id).unwrap();
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM station_queue WHERE station_id = $1 AND position > \
                 (SELECT COALESCE(current_song_index, 0) FROM stations WHERE id = $1)",
            )
            .bind(station_uuid)
            .fetch_one(db)
            .await
            .unwrap_or(0)
        }

        // The queue starts empty: AutoDJ must seed it so the upcoming window
        // holds songs_ahead picks (plus the row that becomes the current one).
        let first = wait_for_status(&server, &station_id, &auth, |status| status["playing"] == true).await?;
        let seeded = upcoming_rows(&db, &station_id).await;
        if seeded < 4 {
            return Err(failure(format!(
                "AutoDJ did not seed songs_ahead upcoming rows: {seeded} (status {first})"
            )));
        }

        // After every handover the DB queue must be topped back up to the
        // songs_ahead minimum — the player may consume one track per song but
        // the upcoming window must never shrink below the configured floor.
        let mut last_index = 0u64;
        for _ in 0..10 {
            let status = wait_for_status(&server, &station_id, &auth, |status| {
                status["playing"] == true && status["song_index"].as_u64().is_some_and(|index| index > last_index)
            })
            .await
            .map_err(|error| failure(format!("playback stalled between handovers: {error}")))?;
            last_index = status["song_index"].as_u64().unwrap_or(last_index);
            let upcoming = upcoming_rows(&db, &station_id).await;
            if upcoming < 4 {
                return Err(failure(format!(
                    "upcoming queue fell below songs_ahead=4 after handover to {:?}: {} rows",
                    status["title"], upcoming
                )));
            }
            // The panel queue view must show the same floor: the frontend
            // splits the queue API rows at song_index % len (played / now /
            // upcoming) — verify the upcoming slice it derives stays >= 4.
            let queue = fetch_queue(&server, &station_id, &auth).await;
            let queue = queue.as_array().cloned().unwrap_or_default();
            let pos = if queue.is_empty() {
                0
            } else {
                (status["song_index"].as_u64().unwrap_or(0) as usize) % queue.len()
            };
            let visible_upcoming = queue.len().saturating_sub(pos + 1);
            if visible_upcoming < 4 {
                return Err(failure(format!(
                    "panel queue view shows {visible_upcoming} upcoming (< 4) after {:?} (queue {} rows, song_index {})",
                    status["title"],
                    queue.len(),
                    status["song_index"]
                )));
            }
        }

        // Songs removed while the station is stopped must be topped back up
        // to the minimum on the next start (play() refills below-target
        // queues, not just empty ones).
        let stopped = server
            .post(&format!("/api/stations/{station_id}/stream/stop"))
            .add_header("Authorization", &auth)
            .await;
        if stopped.status_code() != 200 {
            return Err(failure(format!("stream stop failed: {}", stopped.text())));
        }
        sqlx::query(
            "DELETE FROM station_queue WHERE station_id = $1 AND position > \
             (SELECT COALESCE(current_song_index, 0) FROM stations WHERE id = $1)",
        )
        .bind(uuid::Uuid::parse_str(&station_id).unwrap())
        .execute(&db)
        .await?;
        let restarted = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if restarted.status_code() != 200 {
            return Err(failure(format!("stream play after removal failed: {}", restarted.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["song_index"].as_u64().is_some_and(|index| index > last_index)
        })
        .await
        .map_err(|error| failure(format!("playback did not resume after the queue was trimmed: {error}")))?;
        let refilled = upcoming_rows(&db, &station_id).await;
        if refilled < 2 {
            return Err(failure(format!(
                "start did not top the trimmed queue back up to songs_ahead=2: {refilled} rows"
            )));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn autodj_never_overfills_the_upcoming_window() {
    // User report: with songs_ahead=4 the refill dumped 8 tracks into the
    // queue at once. The fill must add exactly (songs_ahead - visible
    // upcoming) — one track when three are visible, nothing when four are
    // visible — never a batch.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Never overfill",
                "stream_url": "never-overfill",
                "prebuffer_bytes": 1024,
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [
            (330.0, "nf A"),
            (440.0, "nf B"),
            (550.0, "nf C"),
            (660.0, "nf D"),
            (770.0, "nf E"),
            (880.0, "nf F"),
        ];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("never-overfill-{index}.wav")),
                    wav_for(*frequency, 4),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("never-overfill-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }

        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 4,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }

        async fn visible_upcoming(server: &TestServer, station_id: &str, auth: &str) -> i64 {
            let queue = fetch_queue(server, station_id, auth).await;
            let queue = queue.as_array().cloned().unwrap_or_default();
            let status = server
                .get(&format!("/api/stations/{station_id}/stream/status"))
                .add_header("Authorization", auth)
                .await
                .json::<serde_json::Value>();
            let pos = if queue.is_empty() {
                0
            } else {
                (status["song_index"].as_u64().unwrap_or(0) as usize) % queue.len()
            };
            (queue.len().saturating_sub(pos + 1)) as i64
        }

        let mut last_index = 0u64;
        async fn advance(
            server: &TestServer,
            station_id: &str,
            auth: &str,
            last_index: &mut u64,
        ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
            let status = wait_for_status(server, station_id, auth, |status| {
                status["playing"] == true && status["song_index"].as_u64().is_some_and(|index| index > *last_index)
            })
            .await?;
            *last_index = status["song_index"].as_u64().unwrap_or(*last_index);
            Ok(status)
        }

        // Seed from an empty queue: exactly four visible upcoming.
        wait_for_status(&server, &station_id, &auth, |status| status["playing"] == true).await?;
        let seeded = visible_upcoming(&server, &station_id, &auth).await;
        if seeded != 4 {
            return Err(failure(format!("seed produced {seeded} upcoming, expected exactly 4")));
        }

        // Three handovers: the window must stay exactly 4, never grow.
        for _ in 0..3 {
            advance(&server, &station_id, &auth, &mut last_index).await?;
            let upcoming = visible_upcoming(&server, &station_id, &auth).await;
            if upcoming != 4 {
                return Err(failure(format!("after a handover the window holds {upcoming}, expected exactly 4")));
            }
        }

        // The manual trigger must be a no-op at a full window.
        let triggered = server
            .post(&format!("/api/stations/{station_id}/auto-fill/trigger"))
            .add_header("Authorization", &auth)
            .await;
        if triggered.status_code() != 200 {
            return Err(failure(format!("manual trigger failed: {}", triggered.text())));
        }
        let after_trigger = visible_upcoming(&server, &station_id, &auth).await;
        if after_trigger != 4 {
            return Err(failure(format!("manual trigger overfilled the window: {after_trigger} upcoming")));
        }

        // The user removes every queued track while the station plays: the
        // reseed must again produce exactly four upcoming, not a batch.
        let queue = fetch_queue(&server, &station_id, &auth).await;
        let queue = queue.as_array().cloned().unwrap_or_default();
        if queue.is_empty() {
            return Err(failure("expected a non-empty queue before the clear"));
        }
        for item in &queue {
            let item_id = item["id"].as_str().ok_or_else(|| failure("queue item has no id"))?;
            let removed = server
                .delete(&format!("/api/stations/{station_id}/queue/{item_id}"))
                .add_header("Authorization", &auth)
                .await;
            if removed.status_code() != 204 {
                return Err(failure(format!("queue item removal failed: {}", removed.text())));
            }
        }
        advance(&server, &station_id, &auth, &mut last_index).await?;
        let reseeded = visible_upcoming(&server, &station_id, &auth).await;
        if reseeded != 4 {
            let queue = fetch_queue(&server, &station_id, &auth).await;
            let queue = queue.as_array().cloned().unwrap_or_default();
            let status = server
                .get(&format!("/api/stations/{station_id}/stream/status"))
                .add_header("Authorization", &auth)
                .await
                .json::<serde_json::Value>();
            let db_state: Vec<(String, String, bool)> = sqlx::query_as(
                "SELECT sq.id::text, sq.position::text, sq.is_auto_dj FROM station_queue sq \
                 WHERE sq.station_id = $1 ORDER BY sq.position",
            )
            .bind(uuid::Uuid::parse_str(&station_id).unwrap())
            .fetch_all(&db)
            .await
            .unwrap_or_default();
            return Err(failure(format!(
                "after clearing the queue the reseed holds {reseeded} upcoming, expected exactly 4 \
                 (status {status}, queue {} rows, db {:?})",
                queue.len(),
                db_state
            )));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn schedule_playlist_fill_tops_up_instead_of_dumping() {
    // User report: the refill dumped 8 tracks at once. A schedule whose
    // source is a plain playlist (no auto_dj_mode) fed the WHOLE playlist
    // into the queue in one fill. The playlist fill must top up to the
    // songs_ahead window like every other fill.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Schedule top-up",
                "stream_url": "schedule-topup",
                "prebuffer_bytes": 1024,
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let mut song_ids = Vec::new();
        for index in 0..8 {
            let song_id = uuid::Uuid::new_v4();
            std::fs::write(
                files.path().join("audio").join(format!("sched-topup-{index}.wav")),
                wav_for(330.0 + 110.0 * index as f32, 4),
            )?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(format!("sp {index}"))
            .bind(format!("sched-topup-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            song_ids.push(song_id);
        }
        let playlist_id = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO playlists (id, name, created_by) VALUES ($1, 'topup', $2)")
            .bind(playlist_id)
            .bind(admin.0)
            .execute(&db)
            .await?;
        for (position, song_id) in song_ids.iter().enumerate() {
            sqlx::query("INSERT INTO playlist_songs (playlist_id, song_id, position) VALUES ($1, $2, $3)")
                .bind(playlist_id)
                .bind(song_id)
                .bind(position as i32)
                .execute(&db)
                .await?;
        }

        // The user's AutoDJ minimum, plus a permanently active schedule whose
        // source is the 8-song playlist without an auto_dj_mode.
        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 4,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }
        let today = chrono::Local::now();
        let dow = today.weekday().num_days_from_monday() as i16;
        let scheduled = server
            .post(&format!("/api/stations/{station_id}/schedules"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "day_of_week": dow,
                "start_time": "00:00",
                "end_time": "23:59",
                "source_type": "playlist",
                "playlist_id": playlist_id,
            }))
            .await;
        if scheduled.status_code() != 201 {
            return Err(failure(format!("schedule creation failed: {}", scheduled.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| status["playing"] == true).await?;

        // The upcoming window must hold songs_ahead rows, not the whole
        // playlist. (The play() refill runs the schedule fill with the DB
        // count, so the current row is not yet known — five rows total.)
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM station_queue WHERE station_id = $1")
            .bind(uuid::Uuid::parse_str(&station_id).unwrap())
            .fetch_one(&db)
            .await?;
        if rows > 5 {
            return Err(failure(format!(
                "schedule playlist fill dumped {rows} rows into the queue, expected at most 5"
            )));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn stale_cursor_heals_and_queue_keeps_refilling() {
    // User report: after a queue clears, AutoDJ reseeds only songs_ahead
    // tracks (one short), the last played track replays, and during playback
    // nothing is ever added — the queue drains to empty before the next
    // seed. Root cause: a persisted cursor whose id no longer exists (queue
    // cleared / song deleted while stopped) fails the commit guard, and the
    // failing commit suppressed the refill at every handover. The cursor
    // must heal on the first commit and the refill must keep the upcoming
    // window at songs_ahead.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Stale cursor heal",
                "stream_url": "stale-cursor-heal",
                "prebuffer_bytes": 1024,
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [
            (330.0, "sc A"),
            (440.0, "sc B"),
            (550.0, "sc C"),
            (660.0, "sc D"),
            (770.0, "sc E"),
            (880.0, "sc F"),
        ];
        let song_ids = tones
            .iter()
            .enumerate()
            .map(|(index, (frequency, title))| {
                let song_id = uuid::Uuid::new_v4();
                std::fs::write(
                    files.path().join("audio").join(format!("stale-cursor-{index}.wav")),
                    wav_for(*frequency, 4),
                )?;
                Ok::<_, Box<dyn std::error::Error>>((song_id, title))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, (song_id, title)) in song_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',4,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("stale-cursor-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
        }

        let configured = server
            .put(&format!("/api/stations/{station_id}/auto-fill"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "enabled": true,
                "mode": "random",
                "source_type": "station_library",
                "avoid_artist_repeat": false,
                "songs_ahead": 4,
            }))
            .await;
        if configured.status_code() != 200 {
            return Err(failure(format!("auto-fill config failed: {}", configured.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }

        // The panel measure: the frontend splits the queue API rows at
        // song_index % len and shows the slice after it as upcoming. This is
        // what the user watches, and it stays meaningful even when a stale
        // database index would corrupt a position-based SQL count.
        async fn visible_upcoming(server: &TestServer, station_id: &str, auth: &str) -> i64 {
            let queue = fetch_queue(server, station_id, auth).await;
            let queue = queue.as_array().cloned().unwrap_or_default();
            let status = server
                .get(&format!("/api/stations/{station_id}/stream/status"))
                .add_header("Authorization", auth)
                .await
                .json::<serde_json::Value>();
            let pos = if queue.is_empty() {
                0
            } else {
                (status["song_index"].as_u64().unwrap_or(0) as usize) % queue.len()
            };
            (queue.len().saturating_sub(pos + 1)) as i64
        }

        // Seed: five rows (one current + songs_ahead upcoming).
        wait_for_status(&server, &station_id, &auth, |status| status["playing"] == true).await?;
        if visible_upcoming(&server, &station_id, &auth).await < 4 {
            return Err(failure("AutoDJ did not seed the upcoming window"));
        }

        let mut last_index = 0u64;
        async fn advance(
            server: &TestServer,
            station_id: &str,
            auth: &str,
            last_index: &mut u64,
        ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
            let status = wait_for_status(server, station_id, auth, |status| {
                status["playing"] == true && status["song_index"].as_u64().is_some_and(|index| index > *last_index)
            })
            .await?;
            *last_index = status["song_index"].as_u64().unwrap_or(*last_index);
            Ok(status)
        }

        // Two clean handovers first.
        advance(&server, &station_id, &auth, &mut last_index).await?;
        advance(&server, &station_id, &auth, &mut last_index).await?;
        if visible_upcoming(&server, &station_id, &auth).await < 4 {
            return Err(failure("queue fell below songs_ahead during clean playback"));
        }

        // The cursor now references a row that no longer exists — the exact
        // state left behind when the queue is cleared or a song is deleted
        // while the station is stopped. Every later commit must heal it and
        // keep refilling; the queue must not drain.
        sqlx::query(
            "UPDATE stations SET current_queue_item_id = $1, current_song_index = 0, \
             current_queue_cursor_format = 1 WHERE id = $2",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(uuid::Uuid::parse_str(&station_id).unwrap())
        .execute(&db)
        .await?;

        for _ in 0..4 {
            advance(&server, &station_id, &auth, &mut last_index).await?;
            let upcoming = visible_upcoming(&server, &station_id, &auth).await;
            if upcoming < 4 {
                return Err(failure(format!(
                    "queue drained below songs_ahead=4 after the stale cursor: {upcoming} rows"
                )));
            }
        }
        let healed: (uuid::Uuid,) = sqlx::query_as("SELECT current_queue_item_id FROM stations WHERE id = $1")
            .bind(uuid::Uuid::parse_str(&station_id).unwrap())
            .fetch_one(&db)
            .await?;
        let healed_row: Option<(uuid::Uuid,)> = sqlx::query_as("SELECT id FROM station_queue WHERE station_id = $1 AND id = $2")
            .bind(uuid::Uuid::parse_str(&station_id).unwrap())
            .bind(healed.0)
            .fetch_optional(&db)
            .await?;
        if healed_row.is_none() {
            return Err(failure("cursor id was not healed to a live queue row"));
        }

        // Exhausted reseed: every row consumed (they still sit in the table —
        // trimming only starts after played_limit plays) and no current row.
        // The seed must add songs_ahead + 1 so the upcoming window is full,
        // not one short as when the queue was seen as "empty by position".
        let stopped = server
            .post(&format!("/api/stations/{station_id}/stream/stop"))
            .add_header("Authorization", &auth)
            .await;
        if stopped.status_code() != 200 {
            return Err(failure(format!("stream stop failed: {}", stopped.text())));
        }
        let station_uuid = uuid::Uuid::parse_str(&station_id).unwrap();
        let all_ids: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1 ORDER BY position")
            .bind(station_uuid)
            .fetch_all(&db)
            .await?;
        sqlx::query(
            "UPDATE stations SET current_queue_item_id = NULL, \
             consumed_queue_item_ids = $1, current_song_index = $2, \
             current_queue_cursor_format = 1 WHERE id = $3",
        )
        .bind(&all_ids)
        .bind(all_ids.len() as i32)
        .bind(station_uuid)
        .execute(&db)
        .await?;
        let restarted = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if restarted.status_code() != 200 {
            return Err(failure(format!("stream play after exhaustion failed: {}", restarted.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| status["playing"] == true).await?;
        let reseeded = visible_upcoming(&server, &station_id, &auth).await;
        if reseeded < 4 {
            return Err(failure(format!("exhausted reseed left the upcoming window short: {reseeded} rows")));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn reorder_during_playback_plays_the_moved_track_next() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Reorder head",
                "stream_url": "reorder-head",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C"), (880.0, "tone X")];
        let mut song_ids = Vec::new();
        for (index, (frequency, title)) in tones.iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            std::fs::write(
                files.path().join("audio").join(format!("reorder-{index}.wav")),
                wav_for(*frequency, 10),
            )?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',10,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("reorder-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
            song_ids.push((song_id, title.to_string()));
        }

        let queue_ids = song_ids.iter().take(3).map(|(id, _)| *id).collect::<Vec<_>>();
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": queue_ids}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let initial_items = queued.json::<Vec<serde_json::Value>>();
        let a_id = initial_items[0]["id"]
            .as_str()
            .ok_or_else(|| failure("queue item has no id"))?
            .to_owned();
        let b_id = initial_items[1]["id"]
            .as_str()
            .ok_or_else(|| failure("queue item has no id"))?
            .to_owned();
        let c_id = initial_items[2]["id"]
            .as_str()
            .ok_or_else(|| failure("queue item has no id"))?
            .to_owned();

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/reorder-head.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);

        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        if playing["song_index"].as_u64() != Some(0) {
            return Err(failure(format!("unexpected start index: {playing}")));
        }

        // Add a fourth track and move it to the head of the queue while tone A
        // is still playing. The staged next (tone B) must be replaced so the
        // moved track plays right after the current one.
        let added = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": [song_ids[3].0]}))
            .await;
        if added.status_code().as_u16() >= 300 {
            return Err(failure(format!("add failed: {}", added.text())));
        }
        let x_id = added.json::<Vec<serde_json::Value>>()[0]["id"]
            .as_str()
            .ok_or_else(|| failure("added queue item has no id"))?
            .to_owned();
        let reordered = server
            .put(&format!("/api/stations/{station_id}/queue/reorder"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"queue_item_ids": [a_id, x_id, b_id, c_id]}))
            .await;
        if reordered.status_code() != 200 {
            return Err(failure(format!("reorder failed: {}", reordered.text())));
        }

        // The moved track must play NEXT — not the previously staged tone B.
        let next = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str() != Some("tone A")
        })
        .await?;
        if next["title"].as_str() != Some("tone X") {
            return Err(failure(format!("moved track did not play next: {next}")));
        }
        if next["song_index"].as_u64() != Some(1) {
            return Err(failure(format!("moved track played at the wrong index: {next}")));
        }
        let then_b = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone B") && status["playing"] == true
        })
        .await?;
        if then_b["song_index"].as_u64() != Some(2) {
            return Err(failure(format!("queue order broken after the moved track: {then_b}")));
        }
        let then_c = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone C") && status["playing"] == true
        })
        .await?;
        if then_c["song_index"].as_u64() != Some(3) {
            return Err(failure(format!("queue order broken at the tail: {then_c}")));
        }
        wait_for_status(&server, &station_id, &auth, |status| status["playing"] == false).await?;
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn removed_staged_track_is_not_played_next() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Remove staged",
                "stream_url": "remove-staged",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C"), (880.0, "tone X")];
        let mut song_ids = Vec::new();
        for (index, (frequency, title)) in tones.iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            std::fs::write(
                files.path().join("audio").join(format!("remove-staged-{index}.wav")),
                wav_for(*frequency, 10),
            )?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',10,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("remove-staged-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
            song_ids.push((song_id, title.to_string()));
        }

        let queue_ids = song_ids.iter().take(3).map(|(id, _)| *id).collect::<Vec<_>>();
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": queue_ids}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let initial_items = queued.json::<Vec<serde_json::Value>>();
        let a_id = initial_items[0]["id"]
            .as_str()
            .ok_or_else(|| failure("queue item has no id"))?
            .to_owned();
        let b_id = initial_items[1]["id"]
            .as_str()
            .ok_or_else(|| failure("queue item has no id"))?
            .to_owned();
        let c_id = initial_items[2]["id"]
            .as_str()
            .ok_or_else(|| failure("queue item has no id"))?
            .to_owned();

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/remove-staged.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);

        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        if playing["song_index"].as_u64() != Some(0) {
            return Err(failure(format!("unexpected start index: {playing}")));
        }

        // Stage tone X as the next track by moving it to the head, then remove
        // it while tone A still plays. The staged branch must be realigned in
        // the pipeline, not played at the handover.
        let added = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": [song_ids[3].0]}))
            .await;
        if added.status_code().as_u16() >= 300 {
            return Err(failure(format!("add failed: {}", added.text())));
        }
        let x_id = added.json::<Vec<serde_json::Value>>()[0]["id"]
            .as_str()
            .ok_or_else(|| failure("added queue item has no id"))?
            .to_owned();
        let reordered = server
            .put(&format!("/api/stations/{station_id}/queue/reorder"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"queue_item_ids": [a_id, x_id, b_id, c_id]}))
            .await;
        if reordered.status_code() != 200 {
            return Err(failure(format!("reorder failed: {}", reordered.text())));
        }
        let removed = server
            .delete(&format!("/api/stations/{station_id}/queue/{x_id}"))
            .add_header("Authorization", &auth)
            .await;
        if removed.status_code() != 204 {
            return Err(failure(format!("remove failed: {}", removed.text())));
        }

        // The track staged before the removal must NOT play; the original
        // head (tone B) plays next instead.
        let next = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str() != Some("tone A")
        })
        .await?;
        if next["title"].as_str() != Some("tone B") {
            return Err(failure(format!("removed track played next instead of tone B: {next}")));
        }
        if next["song_index"].as_u64() != Some(1) {
            return Err(failure(format!("removed track played at the wrong index: {next}")));
        }
        let then_c = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone C") && status["playing"] == true
        })
        .await?;
        if then_c["song_index"].as_u64() != Some(2) {
            return Err(failure(format!("queue order broken at the tail: {then_c}")));
        }
        wait_for_status(&server, &station_id, &auth, |status| status["playing"] == false).await?;
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn play_starts_with_the_queue_loaded_from_the_database() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Start from DB",
                "stream_url": "start-from-db",
                "prebuffer_bytes": 1024,
                "transition_mode": "off"
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];
        let mut song_ids = Vec::new();
        for (index, (frequency, title)) in tones.iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            std::fs::write(
                files.path().join("audio").join(format!("startdb-{index}.wav")),
                wav_for(*frequency, 10),
            )?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
                 VALUES ($1,$2,'test',$3,1,'audio/wav',10,$4)",
            )
            .bind(song_id)
            .bind(title)
            .bind(format!("startdb-{index}.wav"))
            .bind(admin.0)
            .execute(&db)
            .await?;
            let assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            if assigned.status_code().as_u16() >= 300 {
                return Err(failure(format!("song assignment failed: {}", assigned.text())));
            }
            song_ids.push((song_id, title.to_string()));
        }

        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": song_ids.iter().map(|(id, _)| *id).collect::<Vec<_>>()}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }

        // No streamer exists yet: starting the station must load the queue
        // from the database and begin playback with the first queued track.
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/start-from-db.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);

        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        if playing["song_index"].as_u64() != Some(0) {
            return Err(failure(format!("DB queue did not start at the first track: {playing}")));
        }

        // The loaded queue must advance to the next database row naturally.
        let next = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str() != Some("tone A")
        })
        .await?;
        if next["title"].as_str() != Some("tone B") || next["song_index"].as_u64() != Some(1) {
            return Err(failure(format!("loaded queue did not advance to the second track: {next}")));
        }
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

type TestWs = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_recv_text(socket: &mut TestWs, timeout_secs: u64) -> Result<String, Box<dyn std::error::Error>> {
    use futures::StreamExt;
    loop {
        match tokio::time::timeout(Duration::from_secs(timeout_secs), socket.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => return Ok(text.to_string()),
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(error))) => return Err(failure(format!("ws error: {error}"))),
            Ok(None) => return Err(failure("ws closed")),
            Err(_) => return Err(failure("ws receive timeout")),
        }
    }
}

#[tokio::test]
#[serial]
async fn repro_ws_queue_feed_survives_radio_restart() {
    // The UI Restart button replaces the streamer. The WS forward task
    // subscribes once to the (old) streamer's channels; when the old streamer
    // is stopped those channels close and the station feed dies while the
    // socket stays open — the frontend freezes on the last broadcast and the
    // user keeps seeing the queue from before the restart.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();

    let result: Result<StreamersMap, Box<dyn std::error::Error>> = async {
        let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));
        let server = TestServer::builder().http_transport().build(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        let token = auth.trim_start_matches("Bearer ").to_owned();
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "WS feed repro",
                "stream_url": "ws-feed-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "autocue",
                "autocue_fade_max_ms": 5000
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        let mp3_dir = std::env::temp_dir().join(format!("surcast-wsfeed-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&mp3_dir)?;
        let mp3_path = mp3_dir.join("wsfeed.mp3");
        let status = std::process::Command::new("gst-launch-1.0")
            .args([
                "-q",
                "audiotestsrc",
                "freq=440",
                "wave=sine",
                "samplesperbuffer=44100",
                "num-buffers=10",
                "!",
                "audioconvert",
                "!",
                "lamemp3enc",
                "cbr=true",
                "target=bitrate",
                "bitrate=128",
                "!",
                "filesink",
            ])
            .arg(format!("location={}", mp3_path.display()))
            .status()?;
        if !status.success() {
            return Err(failure("gst-launch mp3 generation failed"));
        }
        let mp3_bytes = std::fs::read(&mp3_path)?;
        std::fs::create_dir(files.path().join("audio"))?;
        let titles = ["tone A", "tone B", "tone C", "tone D"];
        let mut song_ids = Vec::new();
        for (index, title) in titles.iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            let file_name = format!("wsfeed-{index}.mp3");
            std::fs::write(files.path().join("audio").join(&file_name), &mp3_bytes)?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration, \
                 uploaded_by,cue_in,cue_out,cross_start_next,analyzed_at) \
                 VALUES ($1,$2,'test',$3,$4,'audio/mpeg',10,$5,0.5,9.5,7.5,NOW())",
            )
            .bind(song_id)
            .bind(title)
            .bind(file_name)
            .bind(mp3_bytes.len() as i32)
            .bind(admin.0)
            .execute(&db)
            .await?;
            let _assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            song_ids.push((song_id, title.to_string()));
        }

        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": song_ids.iter().map(|(id, _)| *id).collect::<Vec<_>>()}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;

        // Real browser-like WS session: auth, subscribe, then live updates.
        let mut address = server.server_address().ok_or_else(|| failure("no server address"))?;
        address.set_scheme("ws").map_err(|_| failure("bad address"))?;
        let ws_url = address.join("/api/ws").map_err(|_| failure("bad ws url"))?;
        let (mut socket, _) = tokio_tungstenite::connect_async(ws_url.to_string()).await?;
        use futures::SinkExt;
        use tokio_tungstenite::tungstenite::Message as WsMessage;
        socket
            .send(WsMessage::Text(
                serde_json::json!({"type": "auth", "token": token}).to_string().into(),
            ))
            .await?;
        socket
            .send(WsMessage::Text(
                serde_json::json!({"type": "subscribe", "station_id": station_id})
                    .to_string()
                    .into(),
            ))
            .await?;
        let _auth_ok = ws_recv_text(&mut socket, 10).await?;
        // The subscribe reply is a status followed by a queue_update; drain
        // until the initial queue snapshot arrives.
        let _initial_snapshot = loop {
            let text = ws_recv_text(&mut socket, 10).await?;
            let msg: serde_json::Value = serde_json::from_str(&text).map_err(|_| failure("bad ws json"))?;
            if msg["type"] == "queue_update" && msg["data"].as_array().map(|a| a.len()) == Some(4) {
                break msg;
            }
        };

        let first_items = fetch_queue(&server, &station_id, &auth).await;
        let id_of = |title: &str| -> String {
            first_items.as_array().unwrap().iter().find(|item| item["title"] == title).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        };

        // Edit while playing: the WS feed must broadcast the updated queue.
        let removed = server
            .delete(&format!("/api/stations/{station_id}/queue/{}", id_of("tone B")))
            .add_header("Authorization", &auth)
            .await;
        if removed.status_code() != 204 {
            return Err(failure(format!("queue removal failed: {}", removed.status_code())));
        }
        let _live_update = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let text = ws_recv_text(&mut socket, 5).await?;
                let msg: serde_json::Value = serde_json::from_str(&text).map_err(|_| failure("bad ws json"))?;
                if msg["type"] == "queue_update" && msg["data"].as_array().map(|a| a.len()) == Some(3) {
                    return Ok::<serde_json::Value, Box<dyn std::error::Error>>(msg);
                }
            }
        })
        .await
        .map_err(|_| failure("no queue_update after edit (feed already dead)"))??;

        // Radio restart: streamer replaced. Queue edits made after the restart
        // MUST still reach the subscribed client.
        let restarted = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if restarted.status_code() != 200 {
            return Err(failure(format!("radio restart failed: {}", restarted.text())));
        }
        let after_edit = fetch_queue(&server, &station_id, &auth).await;
        let removed_c = server
            .delete(&format!("/api/stations/{station_id}/queue/{}", id_of("tone C")))
            .add_header("Authorization", &auth)
            .await;
        if removed_c.status_code() != 204 {
            return Err(failure(format!("second queue removal failed: {}", removed_c.status_code())));
        }
        let post_restart = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let text = ws_recv_text(&mut socket, 8)
                    .await
                    .map_err(|_| failure("station WS feed died after radio restart; UI freezes on the stale queue"))?;
                let msg: serde_json::Value = serde_json::from_str(&text).map_err(|_| failure("bad ws json"))?;
                if msg["type"] == "queue_update" && msg["data"].as_array().map(|a| a.len()) == Some(2) {
                    return Ok::<serde_json::Value, Box<dyn std::error::Error>>(msg);
                }
            }
        })
        .await;
        let msg = match post_restart {
            Ok(Ok(msg)) => msg,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(failure(format!(
                    "station WS feed died after radio restart; UI freezes on the stale queue (db now: {})",
                    after_edit
                )));
            }
        };
        if msg["data"].as_array().map(|a| a.len()) != Some(2) {
            return Err(failure(format!("unexpected post-restart queue: {}", msg)));
        }
        Ok(streamers)
    }
    .await;

    let icecast_result = icecast.stop().await;
    if let Ok(streamers) = result {
        let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
        futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    } else if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn repro_queue_modifications_persist_across_radio_restart() {
    // The user's exact claim: after modifying the queue (remove + reorder)
    // while the radio runs, a backend restart must read the MODIFIED queue
    // back from the database — not the old queue from before the radio start.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();

    let result: Result<StreamersMap, Box<dyn std::error::Error>> = async {
        let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));
        let server = TestServer::new(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Queue persist repro",
                "stream_url": "queue-persist-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "autocue",
                "autocue_fade_max_ms": 5000
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        let mp3_dir = std::env::temp_dir().join(format!("surcast-qpersist-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&mp3_dir)?;
        let mp3_path = mp3_dir.join("qpersist.mp3");
        let status = std::process::Command::new("gst-launch-1.0")
            .args([
                "-q",
                "audiotestsrc",
                "freq=440",
                "wave=sine",
                "samplesperbuffer=44100",
                "num-buffers=10",
                "!",
                "audioconvert",
                "!",
                "lamemp3enc",
                "cbr=true",
                "target=bitrate",
                "bitrate=128",
                "!",
                "filesink",
            ])
            .arg(format!("location={}", mp3_path.display()))
            .status()?;
        if !status.success() {
            return Err(failure("gst-launch mp3 generation failed"));
        }
        let mp3_bytes = std::fs::read(&mp3_path)?;
        std::fs::create_dir(files.path().join("audio"))?;
        let titles = ["tone A", "tone B", "tone C", "tone D"];
        let mut song_ids = Vec::new();
        for (index, title) in titles.iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            let file_name = format!("qpersist-{index}.mp3");
            std::fs::write(files.path().join("audio").join(&file_name), &mp3_bytes)?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration, \
                 uploaded_by,cue_in,cue_out,cross_start_next,analyzed_at) \
                 VALUES ($1,$2,'test',$3,$4,'audio/mpeg',10,$5,0.5,9.5,7.5,NOW())",
            )
            .bind(song_id)
            .bind(title)
            .bind(file_name)
            .bind(mp3_bytes.len() as i32)
            .bind(admin.0)
            .execute(&db)
            .await?;
            let _assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            song_ids.push((song_id, title.to_string()));
        }

        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": song_ids.iter().map(|(id, _)| *id).collect::<Vec<_>>()}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }

        // First session: start the radio, wait until tone A is actually
        // playing, then modify the queue — remove tone B, move tone D to top.
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("first stream start failed: {}", started.text())));
        }
        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;

        let first_items = fetch_queue(&server, &station_id, &auth).await;
        if queue_titles(&first_items) != ["tone A", "tone B", "tone C", "tone D"] {
            return Err(failure(format!("queue before edits: {}", first_items)));
        }
        let id_of = |title: &str| -> String {
            first_items.as_array().unwrap().iter().find(|item| item["title"] == title).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let removed = server
            .delete(&format!("/api/stations/{station_id}/queue/{}", id_of("tone B")))
            .add_header("Authorization", &auth)
            .await;
        if removed.status_code() != 204 {
            return Err(failure(format!("queue removal failed: {}", removed.status_code())));
        }
        // UI-faithful reorder payload: every displayed row (including the
        // now-playing row) renumbered in the new order.
        let reordered = server
            .put(&format!("/api/stations/{station_id}/queue/reorder"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "queue_item_ids": [id_of("tone D"), id_of("tone A"), id_of("tone C")]
            }))
            .await;
        if reordered.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue reorder failed: {}", reordered.text())));
        }
        let after_edits = fetch_queue(&server, &station_id, &auth).await;
        if queue_titles(&after_edits) != ["tone D", "tone A", "tone C"] {
            return Err(failure(format!("queue after edits: {}", after_edits)));
        }

        // Radio restart: the UI Restart button kills the streamer and spawns a
        // fresh one from the database — the backend stays alive.
        let restarted = server
            .post(&format!("/api/stations/{station_id}/stream/restart"))
            .add_header("Authorization", &auth)
            .await;
        if restarted.status_code() != 200 {
            return Err(failure(format!("radio restart failed: {}", restarted.text())));
        }
        let reloaded = fetch_queue(&server, &station_id, &auth).await;
        if queue_titles(&reloaded) != ["tone D", "tone A", "tone C"] {
            return Err(failure(format!(
                "restart read the OLD queue: {} (expected tone D, tone A, tone C)",
                reloaded
            )));
        }
        // The restarted streamer must also play from the modified queue.
        wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == true && status["title"].as_str().is_some_and(|t| t.starts_with("tone"))
        })
        .await?;
        Ok(streamers)
    }
    .await;

    let icecast_result = icecast.stop().await;
    if let Ok(streamers) = result {
        let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
        futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    } else if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn repro_play_resumes_after_server_restart() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();

    let mut second_streamers: Option<StreamersMap> = None;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));
        let server = TestServer::new(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Restart repro",
                "stream_url": "restart-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "autocue",
                "autocue_fade_max_ms": 5000
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        // Real analyzed mp3s — the exact user scenario (wav tones skip the
        // AutoCue seek path entirely).
        let mp3_dir = std::env::temp_dir().join(format!("surcast-repro-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&mp3_dir)?;
        let mp3_path = mp3_dir.join("repro.mp3");
        let status = std::process::Command::new("gst-launch-1.0")
            .args([
                "-q",
                "audiotestsrc",
                "freq=440",
                "wave=sine",
                "samplesperbuffer=44100",
                "num-buffers=10",
                "!",
                "audioconvert",
                "!",
                "lamemp3enc",
                "cbr=true",
                "target=bitrate",
                "bitrate=128",
                "!",
                "filesink",
            ])
            .arg(format!("location={}", mp3_path.display()))
            .status()?;
        if !status.success() {
            return Err(failure("gst-launch mp3 generation failed"));
        }
        let mp3_bytes = std::fs::read(&mp3_path)?;
        if mp3_bytes.len() < 100_000 {
            return Err(failure(format!("generated mp3 suspiciously small: {} bytes", mp3_bytes.len())));
        }

        std::fs::create_dir(files.path().join("audio"))?;
        let tones = ["tone A", "tone B", "tone C"];
        let mut song_ids = Vec::new();
        for (index, title) in tones.iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            let file_name = format!("repro-{index}.mp3");
            std::fs::write(files.path().join("audio").join(&file_name), &mp3_bytes)?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration, \
                 uploaded_by,cue_in,cue_out,cross_start_next,analyzed_at) \
                 VALUES ($1,$2,'test',$3,$4,'audio/mpeg',10,$5,0.5,9.5,7.5,NOW())",
            )
            .bind(song_id)
            .bind(title)
            .bind(file_name)
            .bind(mp3_bytes.len() as i32)
            .bind(admin.0)
            .execute(&db)
            .await?;
            let _assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            song_ids.push((song_id, title.to_string()));
        }

        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": song_ids.iter().map(|(id, _)| *id).collect::<Vec<_>>()}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }

        // First session: start, wait until tone A is playing, then let the
        // backend "crash" mid-queue (drop server + streamers) and restart.
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("first stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/restart-repro.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);
        // Probe whether icecast actually receives audio bytes.
        let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        sock.write_all(b"GET /restart-repro.mp3 HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut buf = [0u8; 4096];
        let mut total = 0usize;
        let probe_deadline = Instant::now() + Duration::from_secs(5);
        while total < 100 && Instant::now() < probe_deadline {
            match tokio::time::timeout(Duration::from_millis(200), sock.read(&mut buf[total..])).await {
                Ok(Ok(n)) if n > 0 => total += n,
                Ok(Ok(_)) => break,
                _ => {}
            }
        }
        if total < 100 {
            return Err(failure(format!(
                "first session served only {total} bytes; pipeline is not broadcasting"
            )));
        }
        drop(sock);
        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        if playing["song_index"].as_u64() != Some(0) {
            return Err(failure(format!("first session index {playing}")));
        }
        // Give the clock a moment to prove the pipeline is alive.
        wait_for_status(&server, &station_id, &auth, |status| status["elapsed"].as_u64().unwrap_or(0) >= 1)
            .await
            .map_err(|error| failure(format!("first session clock stalled after {playing}: {error}")))?;
        // Model a real backend crash: stop the streamers so the icecast
        // source disconnects (a killed process drops the socket the same way
        // and frees the mount), then drop the whole server.
        let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
        futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
        drop(server);
        drop(streamers);

        // Second session: fresh server + fresh streamer map, same database —
        // exactly like a backend restart. Pressing play must resume from the
        // database cursor, not hang.
        second_streamers = Some(Arc::new(Mutex::new(HashMap::new())));
        let streamers = second_streamers.clone().unwrap();
        let server = TestServer::new(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        let restarted = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if restarted.status_code() != 200 {
            return Err(failure(format!("restart play failed: {}", restarted.text())));
        }
        // The restarted streamer must resume the DB cursor immediately:
        // tone A playing with an advancing clock — the exact user scenario
        // (queue loaded but --:-- and silent would fail here).
        let resumed = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        wait_for_status(&server, &station_id, &auth, |status| status["elapsed"].as_u64().unwrap_or(0) >= 1)
            .await
            .map_err(|error| failure(format!("restarted stream clock stalled after {resumed}: {error}")))?;
        Ok(())
    }
    .await;

    let active = {
        second_streamers
            .map(|map| map.lock().unwrap().values().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}

#[tokio::test]
#[serial]
async fn repro_cold_restart_icecast_comes_back_with_the_backend() {
    // The user's exact flow: the whole server (backend + managed icecast) is
    // restarted, then Start is pressed. A real restart kills the icecast child
    // process; the new backend must re-spawn it through the boot path
    // (kill_zombie_icecast + kill_by_port + spawn) before playback resumes.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();

    let mut second_streamers: Option<StreamersMap> = None;
    let mut second_icecast: Option<IcecastManager> = None;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        // Session 1: icecast spawned by "boot" + streamer broadcasting.
        let icecast = IcecastManager::new(icecast_dir.path().into());
        icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
        let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));
        let server = TestServer::new(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Cold restart repro",
                "stream_url": "cold-restart-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "autocue",
                "autocue_fade_max_ms": 5000
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        let mp3_dir = std::env::temp_dir().join(format!("surcast-cold-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&mp3_dir)?;
        let mp3_path = mp3_dir.join("cold.mp3");
        let status = std::process::Command::new("gst-launch-1.0")
            .args([
                "-q",
                "audiotestsrc",
                "freq=440",
                "wave=sine",
                "samplesperbuffer=44100",
                "num-buffers=10",
                "!",
                "audioconvert",
                "!",
                "lamemp3enc",
                "cbr=true",
                "target=bitrate",
                "bitrate=128",
                "!",
                "filesink",
            ])
            .arg(format!("location={}", mp3_path.display()))
            .status()?;
        if !status.success() {
            return Err(failure("gst-launch mp3 generation failed"));
        }
        let mp3_bytes = std::fs::read(&mp3_path)?;
        if mp3_bytes.len() < 100_000 {
            return Err(failure(format!("generated mp3 suspiciously small: {} bytes", mp3_bytes.len())));
        }

        std::fs::create_dir(files.path().join("audio"))?;
        let mut song_ids = Vec::new();
        for (index, title) in ["tone A", "tone B", "tone C"].iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            let file_name = format!("cold-{index}.mp3");
            std::fs::write(files.path().join("audio").join(&file_name), &mp3_bytes)?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration, \
                 uploaded_by,cue_in,cue_out,cross_start_next,analyzed_at) \
                 VALUES ($1,$2,'test',$3,$4,'audio/mpeg',10,$5,0.5,9.5,7.5,NOW())",
            )
            .bind(song_id)
            .bind(title)
            .bind(file_name)
            .bind(mp3_bytes.len() as i32)
            .bind(admin.0)
            .execute(&db)
            .await?;
            let _assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            song_ids.push(song_id);
        }
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": song_ids}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("first stream start failed: {}", started.text())));
        }
        let url = format!("http://127.0.0.1:{port}/cold-restart-repro.mp3");
        let response = open_mount(&reqwest::Client::new(), &url).await?;
        drop(response);
        wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;

        // Crash: the backend process dies, taking its icecast child with it.
        let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
        futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
        drop(server);
        drop(streamers);
        icecast.stop().await.unwrap();

        // Boot: a fresh backend process starts a brand-new IcecastManager —
        // exactly the main.rs boot path (zombie/port cleanup, config, spawn).
        let icecast = IcecastManager::new(icecast_dir.path().into());
        icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
        second_icecast = Some(icecast.clone());

        // Press Start again: must broadcast on the restarted icecast.
        second_streamers = Some(Arc::new(Mutex::new(HashMap::new())));
        let streamers = second_streamers.clone().unwrap();
        let server = TestServer::new(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        let restarted = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if restarted.status_code() != 200 {
            return Err(failure(format!("restart play failed: {}", restarted.text())));
        }
        let resumed = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        wait_for_status(&server, &station_id, &auth, |status| status["elapsed"].as_u64().unwrap_or(0) >= 1)
            .await
            .map_err(|error| failure(format!("restarted stream clock stalled after {resumed}: {error}")))?;
        let url = format!("http://127.0.0.1:{port}/cold-restart-repro.mp3");
        let response = open_mount(&reqwest::Client::new(), &url).await?;
        drop(response);
        Ok(())
    }
    .await;

    let active = {
        second_streamers
            .map(|map| map.lock().unwrap().values().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    if let Some(icecast) = second_icecast {
        icecast.stop().await.unwrap();
    }
    if let Err(error) = result {
        panic!("{error}");
    }
}

#[tokio::test]
#[serial]
async fn repro_start_with_empty_queue_plays_once_songs_arrive() {
    // The user's report: Start pressed while the database queue is empty,
    // then the queue fills (manual add / Auto DJ / schedule). The streamer
    // must begin broadcasting as soon as songs arrive; otherwise the panel
    // shows the loaded queue stuck at "--:--/xx:xx" with nothing playing.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = TestServer::new(router::create_router(
            db.clone(),
            config,
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Empty start repro",
                "stream_url": "empty-start-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "autocue",
                "autocue_fade_max_ms": 5000
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();

        // Start with an empty database queue: an idle streamer is created.
        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("empty start failed: {}", started.text())));
        }
        let idle = wait_for_status(&server, &station_id, &auth, |status| {
            status["playing"] == false && status["total"].as_u64().unwrap_or(0) == 0
        })
        .await?;
        if idle["elapsed"].as_u64().unwrap_or(0) != 0 {
            return Err(failure(format!("idle streamer must not advance the clock: {idle}")));
        }

        // Songs arrive later; the queue fill must kick the idle streamer off.
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;
        let mp3_dir = std::env::temp_dir().join(format!("surcast-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&mp3_dir)?;
        let mp3_path = mp3_dir.join("empty.mp3");
        let status = std::process::Command::new("gst-launch-1.0")
            .args([
                "-q",
                "audiotestsrc",
                "freq=440",
                "wave=sine",
                "samplesperbuffer=44100",
                "num-buffers=10",
                "!",
                "audioconvert",
                "!",
                "lamemp3enc",
                "cbr=true",
                "target=bitrate",
                "bitrate=128",
                "!",
                "filesink",
            ])
            .arg(format!("location={}", mp3_path.display()))
            .status()?;
        if !status.success() {
            return Err(failure("gst-launch mp3 generation failed"));
        }
        let mp3_bytes = std::fs::read(&mp3_path)?;
        std::fs::create_dir(files.path().join("audio"))?;
        let mut song_ids = Vec::new();
        for (index, title) in ["tone A", "tone B", "tone C"].iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            let file_name = format!("empty-{index}.mp3");
            std::fs::write(files.path().join("audio").join(&file_name), &mp3_bytes)?;
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration, \
                 uploaded_by,cue_in,cue_out,cross_start_next,analyzed_at) \
                 VALUES ($1,$2,'test',$3,$4,'audio/mpeg',10,$5,0.5,9.5,7.5,NOW())",
            )
            .bind(song_id)
            .bind(title)
            .bind(file_name)
            .bind(mp3_bytes.len() as i32)
            .bind(admin.0)
            .execute(&db)
            .await?;
            let _assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            song_ids.push(song_id);
        }
        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": song_ids}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }

        // The idle streamer must now start broadcasting by itself.
        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await
        .map_err(|error| failure(format!("streamer stayed idle after the queue filled: {error}")))?;
        wait_for_status(&server, &station_id, &auth, |status| status["elapsed"].as_u64().unwrap_or(0) >= 1)
            .await
            .map_err(|error| failure(format!("clock stalled after {playing}: {error}")))?;
        let url = format!("http://127.0.0.1:{port}/empty-start-repro.mp3");
        let response = open_mount(&reqwest::Client::new(), &url).await?;
        drop(response);
        Ok(())
    }
    .await;

    let active = { streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    icecast.stop().await.unwrap();
    if let Err(error) = result {
        panic!("{error}");
    }
}

#[tokio::test]
#[serial]
async fn repro_autocue_analyzed_mp3_starts_playing() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let db = common::setup_db().await;
    let files = TempDir::new().unwrap();
    let icecast_dir = TempDir::new().unwrap();
    let port = free_port();
    let icecast = IcecastManager::new(icecast_dir.path().into());
    icecast.start(port.into(), "surcast", "admin", "surcast").await.unwrap();
    let mut config = api_common::test_config();
    config.upload_dir = files.path().display().to_string();

    let mut active_streamers: Option<StreamersMap> = None;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        // Generate a real CBR mp3 (the e2e wav tones never exercise the
        // AutoCue seek path: analyzed=false songs skip it entirely).
        let mp3_dir = std::env::temp_dir().join(format!("surcast-repro-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&mp3_dir)?;
        let mp3_path = mp3_dir.join("repro.mp3");
        let status = std::process::Command::new("gst-launch-1.0")
            .args([
                "-q",
                "audiotestsrc",
                "freq=440",
                "wave=sine",
                "samplesperbuffer=44100",
                "num-buffers=10",
                "!",
                "audioconvert",
                "!",
                "lamemp3enc",
                "cbr=true",
                "target=bitrate",
                "bitrate=128",
                "!",
                "filesink",
            ])
            .arg(format!("location={}", mp3_path.display()))
            .status()?;
        if !status.success() {
            return Err(failure("gst-launch mp3 generation failed"));
        }
        let mp3_bytes = std::fs::read(&mp3_path)?;
        if mp3_bytes.len() < 100_000 {
            return Err(failure(format!("generated mp3 suspiciously small: {} bytes", mp3_bytes.len())));
        }

        active_streamers = Some(Arc::new(Mutex::new(HashMap::new())));
        let streamers = active_streamers.clone().unwrap();
        let server = TestServer::new(router::create_router(
            db.clone(),
            config.clone(),
            streamers.clone(),
            icecast.clone(),
            ListenersState::new(),
        ));
        server
            .post("/api/setup/init")
            .json(&serde_json::json!({"username":"admin","password":"admin123","name":"Admin"}))
            .await;
        let login = server
            .post("/api/auth/login")
            .json(&serde_json::json!({"username":"admin","password":"admin123"}))
            .await;
        let auth = format!(
            "Bearer {}",
            login.json::<serde_json::Value>()["access_token"]
                .as_str()
                .ok_or_else(|| failure("login response has no access token"))?
        );
        sqlx::query(
            "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
             source_password='surcast', admin_user='admin', admin_password='surcast'",
        )
        .bind(port as i32)
        .execute(&db)
        .await?;
        let station = server
            .post("/api/stations")
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({
                "name": "Autocue repro",
                "stream_url": "autocue-repro",
                "prebuffer_bytes": 1024,
                "transition_mode": "autocue",
                "autocue_fade_max_ms": 5000
            }))
            .await;
        if station.status_code() != 201 {
            return Err(failure(format!("station creation failed: {}", station.text())));
        }
        let station_id = station.json::<serde_json::Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        let admin: (uuid::Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'").fetch_one(&db).await?;

        std::fs::create_dir(files.path().join("audio"))?;
        let mut song_ids = Vec::new();
        for (index, title) in ["tone A", "tone B"].iter().enumerate() {
            let song_id = uuid::Uuid::new_v4();
            let file_name = format!("analyzed-{index}.mp3");
            std::fs::write(files.path().join("audio").join(&file_name), &mp3_bytes)?;
            // analyzed=true with realistic cue points: the AutoCue plan seeks
            // both branches and installs volume control bindings.
            sqlx::query(
                "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration, \
                 uploaded_by,cue_in,cue_out,cross_start_next,analyzed_at) \
                 VALUES ($1,$2,'test',$3,$4,'audio/mpeg',10,$5,0.5,9.5,7.5,NOW())",
            )
            .bind(song_id)
            .bind(title)
            .bind(file_name)
            .bind(mp3_bytes.len() as i32)
            .bind(admin.0)
            .execute(&db)
            .await?;
            let _assigned = server
                .post(&format!("/api/songs/{song_id}/stations"))
                .add_header("Authorization", &auth)
                .json(&serde_json::json!({"station_ids":[station_id.clone()]}))
                .await;
            song_ids.push((song_id, title.to_string()));
        }

        let queued = server
            .post(&format!("/api/stations/{station_id}/queue"))
            .add_header("Authorization", &auth)
            .json(&serde_json::json!({"song_ids": song_ids.iter().map(|(id, _)| *id).collect::<Vec<_>>()}))
            .await;
        if queued.status_code().as_u16() >= 300 {
            return Err(failure(format!("queue creation failed: {}", queued.text())));
        }

        let started = server
            .post(&format!("/api/stations/{station_id}/stream/play"))
            .add_header("Authorization", &auth)
            .await;
        if started.status_code() != 200 {
            return Err(failure(format!("stream start failed: {}", started.text())));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let url = format!("http://127.0.0.1:{port}/autocue-repro.mp3");
        let response = open_mount(&client, &url).await?;
        drop(response);
        // Read a chunk straight from TCP: proves the encoder actually pushes
        // data to icecast (reqwest here lacks the `stream` feature).
        let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        sock.write_all(b"GET /autocue-repro.mp3 HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut buf = [0u8; 4096];
        let mut total = 0usize;
        let read_deadline = Instant::now() + Duration::from_secs(5);
        while total < 100 && Instant::now() < read_deadline {
            match tokio::time::timeout(Duration::from_millis(200), sock.read(&mut buf[total..])).await {
                Ok(Ok(n)) if n > 0 => total += n,
                Ok(Ok(_)) => break,
                _ => {}
            }
        }
        if total < 100 {
            return Err(failure(format!(
                "icecast mount served only {total} bytes; pipeline is not broadcasting"
            )));
        }
        drop(sock);
        let playing = wait_for_status(&server, &station_id, &auth, |status| {
            status["title"].as_str() == Some("tone A") && status["playing"] == true
        })
        .await?;
        if playing["song_index"].as_u64() != Some(0) {
            return Err(failure(format!("autocue repro index {playing}")));
        }
        // The user's symptom is a stalled clock (--:--). Verify elapsed
        // actually advances while the pipeline is playing.
        wait_for_status(&server, &station_id, &auth, |status| status["elapsed"].as_u64().unwrap_or(0) >= 1)
            .await
            .map_err(|error| failure(format!("elapsed clock stalled after {playing}: {error}")))?;
        Ok(())
    }
    .await;

    let active = {
        active_streamers
            .map(|map| map.lock().unwrap().values().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    };
    futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
    let icecast_result = icecast.stop().await;
    if let Err(error) = result {
        panic!("{error}");
    }
    icecast_result.unwrap();
}
