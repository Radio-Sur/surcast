#[allow(dead_code)]
mod api_common;
mod common;

use axum_test::TestServer;
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
        ))?;
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
        ))?;
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
        ))?;
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
        let synced = wait_for_status(&server, &station_id, &auth, |status| {
            status["total"].as_u64() == Some(3)
        })
        .await?;
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
        ))?;
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
        let a_id = initial_items[0]["id"].as_str().ok_or_else(|| failure("queue item has no id"))?.to_owned();
        let b_id = initial_items[1]["id"].as_str().ok_or_else(|| failure("queue item has no id"))?.to_owned();
        let c_id = initial_items[2]["id"].as_str().ok_or_else(|| failure("queue item has no id"))?.to_owned();

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
        ))?;
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
        let a_id = initial_items[0]["id"].as_str().ok_or_else(|| failure("queue item has no id"))?.to_owned();
        let b_id = initial_items[1]["id"].as_str().ok_or_else(|| failure("queue item has no id"))?.to_owned();
        let c_id = initial_items[2]["id"].as_str().ok_or_else(|| failure("queue item has no id"))?.to_owned();

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
