#[allow(dead_code)]
mod api_common;
mod common;
mod streamer_common;

use chrono::Datelike;
use serial_test::serial;
use std::time::Duration;
use streamer_common::*;
use surcast_backend::icecast::IcecastManager;
use surcast_backend::listeners::ListenerUpdate;

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

/// The frontend's upcoming-queue measure (see `streamer_common::visible_upcoming`),
/// read through the live API.
async fn visible_upcoming(app: &StreamerTestApp, station: &TestStation) -> Result<i64, Box<dyn std::error::Error>> {
    let queue = app.fetch_queue(station).await?;
    let status = app.status(station).await?;
    Ok(streamer_common::visible_upcoming(&status, &queue))
}

/// Database query for the number of upcoming queue rows past the current song index.
async fn upcoming_rows(db: &sqlx::PgPool, station: &TestStation) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM station_queue WHERE station_id = $1 AND position > \
         (SELECT COALESCE(current_song_index, 0) FROM stations WHERE id = $1)",
    )
    .bind(station.id)
    .fetch_one(db)
    .await
    .unwrap_or(0)
}

/// Standard 3-tone library used by Auto-DJ test scenarios.
const STANDARD_TONES: [(f32, &str); 3] = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];

/// Creates an Auto-DJ station with transition_mode="off", uploads tones, and optionally enqueues the first N tracks.
async fn setup_auto_dj_station(
    app: &StreamerTestApp,
    name: &str,
    slug: &str,
    prefix: &str,
    tones: &[(f32, &str)],
    tone_seconds: u32,
    initial_queue_count: usize,
) -> Result<TestStation, Box<dyn std::error::Error>> {
    let station = app
        .create_station_with(name, slug, serde_json::json!({"transition_mode": "off"}))
        .await?;
    let songs = app.add_tones_to_library(&station, prefix, tones, tone_seconds).await?;
    if initial_queue_count > 0 {
        let to_queue: Vec<_> = songs.iter().take(initial_queue_count).map(|s| s.id).collect();
        app.enqueue(&station, &to_queue).await?;
    }
    Ok(station)
}

/// Action executed during a queue mutation scenario.
enum QueueMutationAction {
    ReorderOnly,
    ReorderThenRemoveStaged,
}

/// Reusable scenario runner for queue reorder / removal integration tests.
struct QueueMutationScenario {
    name: &'static str,
    slug: &'static str,
    action: QueueMutationAction,
}

impl QueueMutationScenario {
    async fn run(self, app: &StreamerTestApp) -> Result<(), Box<dyn std::error::Error>> {
        let name = self.name;
        let station = app
            .create_station_with(self.name, self.slug, serde_json::json!({"transition_mode": "off"}))
            .await?;
        app.disable_auto_fill(&station).await?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C"), (880.0, "tone X")];
        let songs = app.add_tones_to_library(&station, self.slug, &tones, 10).await?;

        let initial = app.enqueue(&station, &[songs[0].id, songs[1].id, songs[2].id]).await?;
        let a_id = initial[0].id.clone();
        let b_id = initial[1].id.clone();
        let c_id = initial[2].id.clone();
        app.restart(&station).await?;
        let url = format!("http://127.0.0.1:{}/{}.mp3", app.port, self.slug);
        app.assert_mount_serves_audio(&url).await?;

        let playing = app.wait_title_playing(&station, "tone A").await?;
        if playing.song_index != 0 {
            return Err(failure(format!("{name}: unexpected start index: {playing:?}")));
        }

        let added = app.enqueue(&station, &[songs[3].id]).await?;
        let x_id = added[0].id.clone();
        app.reorder(&station, &[&a_id, &x_id, &b_id, &c_id]).await?;

        match self.action {
            QueueMutationAction::ReorderOnly => {
                let next = app
                    .wait_status(&station, &format!("{name}: moved track next"), |status| {
                        status.playing && status.title != "tone A"
                    })
                    .await?;
                if next.title != "tone X" {
                    return Err(failure(format!("{name}: moved track did not play next: {next:?}")));
                }
                if next.song_index != 1 {
                    return Err(failure(format!("{name}: moved track played at wrong index: {next:?}")));
                }
                let then_b = app.wait_title_playing(&station, "tone B").await?;
                if then_b.song_index != 2 {
                    return Err(failure(format!("{name}: queue order broken after moved track: {then_b:?}")));
                }
                let then_c = app.wait_title_playing(&station, "tone C").await?;
                if then_c.song_index != 3 {
                    return Err(failure(format!("{name}: queue order broken at tail: {then_c:?}")));
                }
                app.wait_stopped(&station).await?;
            }
            QueueMutationAction::ReorderThenRemoveStaged => {
                app.remove_queue_item(&station, &x_id).await?;
                let next = app
                    .wait_status(&station, &format!("{name}: tone B after removal"), |status| {
                        status.playing && status.title != "tone A"
                    })
                    .await?;
                if next.title != "tone B" {
                    return Err(failure(format!("{name}: removed track played next instead of tone B: {next:?}")));
                }
                if next.song_index != 1 {
                    return Err(failure(format!("{name}: removed track played at wrong index: {next:?}")));
                }
                let then_c = app.wait_title_playing(&station, "tone C").await?;
                if then_c.song_index != 2 {
                    return Err(failure(format!("{name}: queue order broken at tail: {then_c:?}")));
                }
                app.wait_stopped(&station).await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn managed_icecast_serves_gstreamer_encoded_mp3() {
    run_streamer_test(async |app| {
        let station = app
            .create_station_with("E2E", "e2e-stream", serde_json::json!({"transition_mode": "off"}))
            .await?;
        let first_song = app.insert_tone("first tone", "first.wav", 440.0, 10).await?;
        let second_song = app.insert_tone("second tone", "second.wav", 660.0, 10).await?;
        app.assign(&first_song, &station).await?;
        app.assign(&second_song, &station).await?;
        let queued = app.enqueue(&station, &[first_song.id, second_song.id]).await?;
        let first_queue_item_id = queued[0].id.clone();

        app.restart(&station).await?;
        app.wait_title_playing(&station, "first tone").await?;
        let station_uuid = station.id;
        surcast_backend::listeners::spawn_poller(app.db.clone(), app.session().listeners.clone());
        app.wait_until(
            Duration::from_secs(5),
            Duration::from_millis(25),
            "listener poller to publish its initial zero count",
            async |app| {
                app.session()
                    .listeners
                    .live(station_uuid)
                    .await
                    .is_some_and(|live| live.online && live.listeners == 0)
                    .then_some(())
            },
        )
        .await?;

        let url = format!("http://127.0.0.1:{}/e2e-stream.mp3", app.port);
        let mut response = app.open_mount(&url).await?;
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
        app.wait_until(
            Duration::from_secs(8),
            Duration::from_millis(25),
            "live listener count to reach one",
            async |app| {
                app.session()
                    .listeners
                    .live(station_uuid)
                    .await
                    .is_some_and(|live| live.online && live.listeners == 1)
                    .then_some(())
            },
        )
        .await?;
        let persisted_samples: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM listener_stats WHERE station_id = $1")
            .bind(station_uuid)
            .fetch_one(&app.db)
            .await?;
        if persisted_samples != 1 {
            return Err(failure(format!(
                "rapid live polls persisted {persisted_samples} history samples instead of one"
            )));
        }
        drop(response);

        let checkpoint = app.wait_elapsed(&station, 1).await?;
        let checkpoint_elapsed = checkpoint.elapsed;
        let checkpoint_index = checkpoint.song_index;
        let checkpoint_title = checkpoint.title.clone();
        let restarted_port = free_port();
        app.session().expect(
            "Icecast restart",
            app.session()
                .patch(
                    "/api/admin/icecast",
                    Some(serde_json::json!({"enabled": true, "port": restarted_port})),
                )
                .await,
            200,
        )?;
        let reconnected = app
            .wait_status(&station, "reconnect after Icecast restart", |status| {
                status.playing && status.elapsed > checkpoint_elapsed
            })
            .await?;
        if reconnected.song_index != checkpoint_index || reconnected.title != checkpoint_title {
            return Err(failure(format!(
                "Icecast reconnect changed the active track: {checkpoint:?} -> {reconnected:?}"
            )));
        }
        let restarted_url = format!("http://127.0.0.1:{restarted_port}/e2e-stream.mp3");
        app.assert_mount_serves_audio(&restarted_url).await?;

        app.pause(&station).await?;
        let paused_once = app.wait_stopped(&station).await?;
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let paused_twice = app.wait_stopped(&station).await?;
        if paused_once.elapsed != paused_twice.elapsed {
            return Err(failure(format!(
                "elapsed advanced while paused: {paused_once:?} -> {paused_twice:?}"
            )));
        }
        let paused_elapsed = paused_twice.elapsed;
        app.play(&station).await?;
        app.wait_status(&station, "resume after pause", |status| {
            status.playing && status.elapsed > paused_elapsed
        })
        .await?;

        app.skip(&station).await?;
        let second = app
            .wait_status(&station, "second tone after skip", |status| {
                status.song_index == 1 && status.title == "second tone"
            })
            .await?;
        app.insert_queue_item(&station, first_song.id, 0).await?;
        app.remove_queue_item(&station, &first_queue_item_id).await?;
        let retained = app.wait_title_playing(&station, "second tone").await?;
        if retained.song_index != 1 {
            return Err(failure(format!("queue reload changed the active track: {retained:?}")));
        }
        let queue_after_mutation = app.fetch_queue(&station).await?;
        if queue_after_mutation.iter().any(|item| item.id == first_queue_item_id) {
            return Err(failure(format!(
                "deleted queue item returned after reload: {queue_after_mutation:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let second_stable = app.wait_playing(&station).await?;
        if second_stable.song_index != second.song_index || second_stable.title != second.title {
            return Err(failure(format!("skip advanced more than once: {second:?} -> {second_stable:?}")));
        }

        app.restart(&station).await?;
        app.wait_playing(&station).await?;
        if app.session().streamers.lock().unwrap().len() != 1 {
            return Err(failure("stream restart created more than one station pipeline"));
        }
        app.assert_mount_serves_audio(&restarted_url).await?;

        app.stop(&station).await?;
        app.wait_until(
            Duration::from_secs(30),
            Duration::from_millis(200),
            "mount to become unavailable",
            async |app| {
                app.client
                    .get(&url)
                    .send()
                    .await
                    .map_or(true, |response| !response.status().is_success())
                    .then_some(())
            },
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        if !app.session().streamers.lock().unwrap().is_empty() {
            return Err(failure("stopped stream reconnected itself"));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn crossfade_naturally_promotes_each_queued_track_once() {
    run_streamer_test(async |app| {
        let station = app
            .create_station_with(
                "Natural crossfade",
                "natural-crossfade",
                serde_json::json!({"transition_mode": "crossfade"}),
            )
            .await?;
        app.disable_auto_fill(&station).await?;
        sqlx::query("UPDATE stations SET transition_mode='crossfade', default_fade_ms=500 WHERE id=$1")
            .bind(station.id)
            .execute(&app.db)
            .await?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C"), (660.0, "tone D")];
        let songs = app.add_tones_to_library(&station, "natural", &tones, 10).await?;
        app.enqueue_songs(&station, &songs).await?;

        app.restart(&station).await?;
        app.assert_station_serves_audio(&station).await?;

        for (index, song) in songs.iter().enumerate() {
            let status = app
                .wait_status(&station, "natural promotion", |status| {
                    status.title == song.title && (status.playing || index + 1 == songs.len())
                })
                .await?;
            if status.song_index != index as u64 {
                return Err(failure(format!("natural transition selected wrong queue index: {status:?}")));
            }
            if index > 0 && status.elapsed != 0 {
                return Err(failure(format!("promoted track did not reset elapsed: {status:?}")));
            }
        }
        let stopped = app.wait_stopped(&station).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let stable = app.wait_stopped(&station).await?;
        if stable.song_index != stopped.song_index || stable.title != stopped.title {
            return Err(failure(format!("exhausted queue advanced or retried: {stopped:?} -> {stable:?}")));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn manual_auto_dj_trigger_keeps_an_exhausted_memory_queue_playing() {
    run_streamer_test(async |app| {
        let station = setup_auto_dj_station(app, "Auto DJ trigger", "auto-dj-trigger", "dj", &STANDARD_TONES, 10, 1).await?;
        app.disable_auto_fill(&station).await?;
        app.restart(&station).await?;
        app.assert_station_serves_audio(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        app.enable_auto_fill(&station, 2, false).await?;
        app.trigger_auto_fill(&station).await?;

        let synced = app.wait_status(&station, "queue sync", |status| status.total == 3).await?;
        if synced.song_index != 0 {
            return Err(failure(format!("auto-fill sync moved cursor: {synced:?}")));
        }

        let advanced = app
            .wait_status(&station, "Auto DJ pick after exhaustion", |status| {
                status.playing && status.title != "tone A"
            })
            .await?;
        if advanced.song_index != 1 {
            return Err(failure(format!("auto-fill pick played at wrong index: {advanced:?}")));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn play_with_empty_queue_fills_from_auto_dj_and_starts() {
    // Regression: pressing play with an empty queue used to leave the
    // streamer Stopped forever, even with Auto DJ enabled and a library full
    // of songs. play() must give Auto DJ one chance to fill the queue, reload
    // it, and start broadcasting.
    run_streamer_test(async |app| {
        let station = setup_auto_dj_station(app, "Empty play Auto DJ", "empty-play-autodj", "empty-dj", &STANDARD_TONES, 4, 0).await?;
        app.enable_auto_fill(&station, 2, false).await?;
        app.play(&station).await?;

        let playing = app
            .wait_status(&station, "play on empty queue", |status| status.playing && status.total == 3)
            .await
            .map_err(|e| failure(format!("streamer stayed stopped after play on empty queue: {e}")))?;
        if !["tone A", "tone B", "tone C"].contains(&playing.title.as_str()) {
            return Err(failure(format!("Auto DJ pick has unexpected title: {playing:?}")));
        }
        app.assert_station_serves_audio(&station).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn natural_queue_exhaustion_refills_from_auto_dj() {
    // Regression: when the last queued track ends and nothing is queued
    // behind it, the controller must give Auto DJ a chance to fill the queue
    // instead of stopping the radio for good.
    run_streamer_test(async |app| {
        let station = setup_auto_dj_station(
            app,
            "Natural exhaustion Auto DJ",
            "natural-exhaustion-autodj",
            "exhaust-dj",
            &STANDARD_TONES,
            4,
            1,
        )
        .await?;
        app.enable_auto_fill(&station, 2, false).await?;
        app.restart(&station).await?;
        app.assert_station_serves_audio(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let advanced = app
            .wait_status(&station, "refill after queue exhaustion", |status| {
                status.playing && status.title != "tone A"
            })
            .await
            .map_err(|e| failure(format!("playback stopped after queue exhaustion: {e}")))?;
        if advanced.total < 3 {
            return Err(failure(format!("Auto DJ refill did not populate queue: {advanced:?}")));
        }
        if advanced.song_index != 1 {
            return Err(failure(format!("Auto DJ pick played at wrong index: {advanced:?}")));
        }
        Ok(())
    })
    .await
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
    run_streamer_test(async |app| {
        let station = app
            .create_station_with(
                "Empty start audio repro",
                "empty-start-audio-repro",
                serde_json::json!({"transition_mode": "off"}),
            )
            .await?;
        // Songs live only in the station library; the queue stays empty.
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];
        app.add_tones_to_library(&station, "audio-repro", &tones, 4).await?;
        app.enable_auto_fill(&station, 1, false).await?;

        // Start with an empty database queue: play()'s own refill adds the
        // first Auto DJ pick, which plays with nothing staged behind it and
        // ends through the EOS path (no natural handover) — the exact state
        // the user hit.
        app.play(&station).await?;
        let playing = app
            .wait_status(&station, "first pick after empty play", |status| {
                // songs_ahead=1: the current track plus one upcoming pick.
                status.playing && status.total == 2
            })
            .await
            .map_err(|error| failure(format!("streamer stayed stopped after play on an empty queue: {error}")))?;
        let first_title = playing.title.clone();
        if first_title.is_empty() {
            return Err(failure(format!("no first Auto DJ pick: {playing:?}")));
        }

        app.assert_station_serves_audio(&station).await?;

        // The first track ends with nothing staged behind it: the controller
        // must refill from Auto DJ and keep the broadcast alive.
        let advanced = app
            .wait_status(&station, "refill after last track", |status| {
                status.playing && status.title != first_title
            })
            .await
            .map_err(|error| failure(format!("controller did not advance after the last queued track ended: {error}")))?;
        if advanced.total < 2 {
            return Err(failure(format!("Auto DJ refill did not populate the queue: {advanced:?}")));
        }

        // The broadcast itself must survive the transition: the mount has to
        // serve real audio again after the refill, not just report playing.
        app.assert_station_serves_audio(&station).await.map_err(|error| {
            failure(format!(
                "mount served no audio after the refill transition; status claimed: {advanced:?} ({error})"
            ))
        })?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn repro_ws_feed_reports_stopped_after_queue_exhaustion() {
    // The user's report: after the station stopped itself (queue exhausted,
    // nothing to refill), the panel kept showing the last playing state — the
    // live feed only pushes status on events, and the exhaustion stop pushed
    // none. The feed must report playing=false when the station stops itself.
    let app = StreamerTestApp::new_http().await;
    app.run(async |app| {
        let station = app
            .create_station_with(
                "WS stopped feed repro",
                "ws-stopped-feed-repro",
                serde_json::json!({"transition_mode": "off"}),
            )
            .await?;
        sqlx::query("UPDATE station_auto_fill SET enabled = false WHERE station_id = $1")
            .bind(station.id)
            .execute(&app.db)
            .await?;
        app.session()
            .listeners
            .publish(ListenerUpdate {
                station_id: station.id,
                listeners: 1,
                updated_at: chrono::Utc::now(),
                online: true,
            })
            .await;
        let song = app.insert_tone("tone A", "ws-stopped.wav", 330.0, 4).await?;
        app.assign(&song, &station).await?;
        app.enqueue(&station, &[song.id]).await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let token = app.session().auth.trim_start_matches("Bearer ").to_owned();
        let mut address = app.session().server.server_address().ok_or_else(|| failure("no server address"))?;
        address.set_scheme("ws").map_err(|_| failure("bad address"))?;
        let ws_url = address.join("/api/ws").map_err(|_| failure("bad ws url"))?;
        let (mut socket, _) = tokio_tungstenite::connect_async(ws_url.to_string()).await?;
        use futures::SinkExt;
        use tokio_tungstenite::tungstenite::Message as WsMessage;
        socket
            .send(WsMessage::Text(serde_json::json!({"type": "auth", "token": token}).to_string()))
            .await?;
        socket
            .send(WsMessage::Text(
                serde_json::json!({"type": "subscribe", "station_id": station.id}).to_string(),
            ))
            .await?;
        let _auth_ok = ws_recv_text(&mut socket, 10).await?;
        // A station subscription must include the cached listener count along
        // with status and queue snapshots. Otherwise a reconnect shows zero
        // until Icecast is polled again.
        let mut saw_playing = false;
        let mut saw_queue = false;
        let mut saw_listener = false;
        for _ in 0..8 {
            let msg: serde_json::Value = serde_json::from_str(&ws_recv_text(&mut socket, 10).await?)?;
            if msg["type"] == "status" && msg["data"]["data"]["playing"] == true {
                saw_playing = true;
            }
            if msg["type"] == "queue_update" {
                saw_queue = true;
            }
            if msg["type"] == "listeners" && msg["station_id"] == station.id.to_string() && msg["listeners"] == 1 && msg["online"] == true {
                saw_listener = true;
            }
            if saw_playing && saw_queue && saw_listener {
                break;
            }
        }
        if !saw_playing {
            return Err(failure("initial live status did not report playing"));
        }
        if !saw_listener {
            return Err(failure("initial live feed did not report the cached listener count"));
        }

        // The single track ends: no Auto DJ is configured, so the controller
        // stops the station. The live feed must push the stopped state; the
        // panel must not keep the last playing snapshot forever.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let msg: serde_json::Value = serde_json::from_str(&ws_recv_text(&mut socket, 10).await?)?;
            if msg["type"] == "status" && msg["data"]["data"]["playing"] == false {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(failure(format!(
                    "live feed never reported the exhausted station as stopped; last message: {msg}"
                )));
            }
        }
        let queue_item_id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1")
            .bind(station.id)
            .fetch_one(&app.db)
            .await?;
        let (current_queue_item_id, consumed_queue_item_ids): (Option<uuid::Uuid>, Vec<uuid::Uuid>) =
            sqlx::query_as("SELECT current_queue_item_id, consumed_queue_item_ids FROM stations WHERE id = $1")
                .bind(station.id)
                .fetch_one(&app.db)
                .await?;
        if current_queue_item_id.is_some() || !consumed_queue_item_ids.contains(&queue_item_id) {
            return Err(failure(format!(
                "queue exhaustion was not persisted: current={current_queue_item_id:?}, consumed={consumed_queue_item_ids:?}"
            )));
        }

        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn mixed_queue_drain_keeps_playing_with_auto_dj_picks() {
    // User report: a station whose queue held manually added songs plus
    // Auto DJ picks stopped broadcasting after those songs finished, even
    // though Auto DJ was enabled. Playback must roll from the manual tail
    // into Auto DJ picks and keep refilling across handovers.
    run_streamer_test(async |app| {
        let tones = [
            (330.0, "tone A"),
            (440.0, "tone B"),
            (550.0, "tone C"),
            (660.0, "tone D"),
            (770.0, "tone E"),
        ];
        let station = setup_auto_dj_station(app, "Mixed drain Auto DJ", "mixed-drain-autodj", "mixed-dj", &tones, 4, 3).await?;
        app.enable_auto_fill(&station, 2, false).await?;
        app.restart(&station).await?;
        app.assert_station_serves_audio(&station).await?;

        app.wait_title_playing(&station, "tone A").await?;
        app.wait_title_playing(&station, "tone B").await?;
        app.wait_title_playing(&station, "tone C").await?;

        let pick = app
            .wait_status(&station, "Auto DJ pick after manual drain", |status| {
                status.playing && matches!(status.title.as_str(), "tone D" | "tone E")
            })
            .await
            .map_err(|e| failure(format!("radio stopped after manual queue drained: {e}")))?;
        if pick.total < 4 {
            return Err(failure(format!("Auto DJ did not refill past manual tail: {pick:?}")));
        }
        let second = if pick.title == "tone D" { "tone E" } else { "tone D" };
        app.wait_title_playing(&station, second)
            .await
            .map_err(|e| failure(format!("playback stopped between Auto DJ picks: {e}")))?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn auto_dj_keeps_songs_ahead_with_crossfade_handovers() {
    // User report: AutoDJ fills the queue only when it is completely empty;
    // as the player consumes tracks one by one the queue shrinks below the
    // configured songs_ahead minimum and nothing tops it up. The queue must
    // be refilled at every handover so upcoming stays at songs_ahead.
    run_streamer_test(async |app| {
        let station = app.create_station("Crossfade keep-ahead", "crossfade-keep-ahead").await?;
        let tones = [
            (330.0, "ka A"),
            (440.0, "ka B"),
            (550.0, "ka C"),
            (660.0, "ka D"),
            (770.0, "ka E"),
            (880.0, "ka F"),
        ];
        app.add_tones_to_library(&station, "keep-ahead", &tones, 4).await?;
        app.enable_auto_fill(&station, 4, true).await?;
        app.restart(&station).await?;

        let first = app.wait_playing(&station).await?;
        let seeded = upcoming_rows(&app.db, &station).await;
        if seeded < 4 {
            return Err(failure(format!(
                "AutoDJ did not seed songs_ahead upcoming rows: {seeded} (status {first:?})"
            )));
        }

        let mut last_index = 0u64;
        for _ in 0..10 {
            let status = app
                .wait_advance(&station, last_index)
                .await
                .map_err(|e| failure(format!("playback stalled between handovers: {e}")))?;
            last_index = status.song_index;
            let upcoming = upcoming_rows(&app.db, &station).await;
            if upcoming < 4 {
                return Err(failure(format!(
                    "upcoming queue fell below songs_ahead=4 after handover to {:?}: {} rows",
                    status.title, upcoming
                )));
            }
            let queue = app.fetch_queue(&station).await?;
            let visible = streamer_common::visible_upcoming(&status, &queue);
            if visible < 4 {
                return Err(failure(format!(
                    "panel queue view shows {visible} upcoming (< 4) after {:?} (queue {} rows, song_index {})",
                    status.title,
                    queue.len(),
                    status.song_index
                )));
            }
        }

        app.stop(&station).await?;
        sqlx::query(
            "DELETE FROM station_queue WHERE station_id = $1 AND position > \
             (SELECT COALESCE(current_song_index, 0) FROM stations WHERE id = $1)",
        )
        .bind(station.id)
        .execute(&app.db)
        .await?;
        app.play(&station).await?;
        app.wait_advance(&station, last_index)
            .await
            .map_err(|e| failure(format!("playback did not resume after queue trimmed: {e}")))?;
        let refilled = upcoming_rows(&app.db, &station).await;
        if refilled < 2 {
            return Err(failure(format!(
                "start did not top trimmed queue back up to songs_ahead=2: {refilled} rows"
            )));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn autodj_never_overfills_the_upcoming_window() {
    // User report: with songs_ahead=4 the refill dumped 8 tracks into the
    // queue at once. The fill must add exactly (songs_ahead - visible
    // upcoming) — one track when three are visible, nothing when four are
    // visible — never a batch.
    run_streamer_test(async |app| {
        let station = app.create_station("Never overfill", "never-overfill").await?;
        let tones = [
            (330.0, "nf A"),
            (440.0, "nf B"),
            (550.0, "nf C"),
            (660.0, "nf D"),
            (770.0, "nf E"),
            (880.0, "nf F"),
        ];
        app.add_tones_to_library(&station, "never-overfill", &tones, 4).await?;
        app.enable_auto_fill(&station, 4, false).await?;
        app.restart(&station).await?;

        app.wait_playing(&station).await?;
        let seeded = visible_upcoming(app, &station).await?;
        if seeded != 4 {
            return Err(failure(format!("seed produced {seeded} upcoming, expected exactly 4")));
        }

        let mut last_index = 0u64;
        for _ in 0..3 {
            last_index = app.wait_advance(&station, last_index).await?.song_index;
            let upcoming = visible_upcoming(app, &station).await?;
            if upcoming != 4 {
                return Err(failure(format!("after handover window holds {upcoming}, expected exactly 4")));
            }
        }

        app.trigger_auto_fill(&station).await?;
        let after_trigger = visible_upcoming(app, &station).await?;
        if after_trigger != 4 {
            return Err(failure(format!("manual trigger overfilled window: {after_trigger} upcoming")));
        }

        let queue = app.fetch_queue(&station).await?;
        if queue.is_empty() {
            return Err(failure("expected non-empty queue before clear"));
        }
        for item in &queue {
            app.remove_queue_item(&station, &item.id).await?;
        }
        app.wait_advance(&station, last_index).await?;

        let mut reseeded = 0i64;
        for _ in 0..40 {
            reseeded = visible_upcoming(app, &station).await?;
            if reseeded >= 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if reseeded != 4 {
            let queue = app.fetch_queue(&station).await?;
            let status = app.status(&station).await?;
            let db_state: Vec<(String, String, bool)> = sqlx::query_as(
                "SELECT sq.id::text, sq.position::text, sq.is_auto_dj FROM station_queue sq \
                 WHERE sq.station_id = $1 ORDER BY sq.position",
            )
            .bind(station.id)
            .fetch_all(&app.db)
            .await
            .unwrap_or_default();
            return Err(failure(format!(
                "after clearing queue reseed holds {reseeded} upcoming, expected exactly 4 \
                 (status {status:?}, queue {} rows, db {:?})",
                queue.len(),
                db_state
            )));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn schedule_playlist_fill_tops_up_instead_of_dumping() {
    // User report: the refill dumped 8 tracks at once. A schedule whose
    // source is a plain playlist (no auto_dj_mode) fed the WHOLE playlist
    // into the queue in one fill. The playlist fill must top up to the
    // songs_ahead window like every other fill.
    run_streamer_test(async |app| {
        let station = app.create_station("Schedule top-up", "schedule-topup").await?;
        let mut song_ids = Vec::new();
        for index in 0..8 {
            let song = app
                .insert_tone(
                    &format!("sp {index}"),
                    &format!("sched-topup-{index}.wav"),
                    330.0 + 110.0 * index as f32,
                    4,
                )
                .await?;
            song_ids.push(song.id);
        }
        let playlist_id = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO playlists (id, name, created_by) VALUES ($1, 'topup', $2)")
            .bind(playlist_id)
            .bind(app.admin_id)
            .execute(&app.db)
            .await?;
        for (position, song_id) in song_ids.iter().enumerate() {
            sqlx::query("INSERT INTO playlist_songs (playlist_id, song_id, position) VALUES ($1, $2, $3)")
                .bind(playlist_id)
                .bind(song_id)
                .bind(position as i32)
                .execute(&app.db)
                .await?;
        }

        app.enable_auto_fill(&station, 4, false).await?;
        let today = chrono::Local::now();
        let dow = today.weekday().num_days_from_monday() as i16;
        app.session().expect(
            "schedule creation",
            app.session()
                .post(
                    &format!("/api/stations/{}/schedules", station.id),
                    Some(serde_json::json!({
                        "day_of_week": dow,
                        "start_time": "00:00",
                        "end_time": "23:59",
                        "source_type": "playlist",
                        "playlist_id": playlist_id,
                    })),
                )
                .await,
            201,
        )?;
        app.restart(&station).await?;
        app.wait_playing(&station).await?;

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM station_queue WHERE station_id = $1")
            .bind(station.id)
            .fetch_one(&app.db)
            .await?;
        if rows > 5 {
            return Err(failure(format!(
                "schedule playlist fill dumped {rows} rows into queue, expected at most 5"
            )));
        }
        Ok(())
    })
    .await
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
    run_streamer_test(async |app| {
        let station = app.create_station("Stale cursor heal", "stale-cursor-heal").await?;
        let tones = [
            (330.0, "sc A"),
            (440.0, "sc B"),
            (550.0, "sc C"),
            (660.0, "sc D"),
            (770.0, "sc E"),
            (880.0, "sc F"),
        ];
        app.add_tones_to_library(&station, "stale-cursor", &tones, 4).await?;
        app.enable_auto_fill(&station, 4, false).await?;
        app.restart(&station).await?;

        app.wait_playing(&station).await?;
        if visible_upcoming(app, &station).await? < 4 {
            return Err(failure("AutoDJ did not seed upcoming window"));
        }

        let mut last_index = 0u64;
        last_index = app.wait_advance(&station, last_index).await?.song_index;
        last_index = app.wait_advance(&station, last_index).await?.song_index;
        if visible_upcoming(app, &station).await? < 4 {
            return Err(failure("queue fell below songs_ahead during clean playback"));
        }

        sqlx::query(
            "UPDATE stations SET current_queue_item_id = $1, current_song_index = 0, \
             current_queue_cursor_format = 1 WHERE id = $2",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(station.id)
        .execute(&app.db)
        .await?;

        for _ in 0..4 {
            last_index = app.wait_advance(&station, last_index).await?.song_index;
            let upcoming = visible_upcoming(app, &station).await?;
            if upcoming < 4 {
                return Err(failure(format!(
                    "queue drained below songs_ahead=4 after stale cursor: {upcoming} rows"
                )));
            }
        }
        let (healed,): (uuid::Uuid,) = sqlx::query_as("SELECT current_queue_item_id FROM stations WHERE id = $1")
            .bind(station.id)
            .fetch_one(&app.db)
            .await?;
        let healed_row: Option<(uuid::Uuid,)> = sqlx::query_as("SELECT id FROM station_queue WHERE station_id = $1 AND id = $2")
            .bind(station.id)
            .bind(healed)
            .fetch_optional(&app.db)
            .await?;
        if healed_row.is_none() {
            return Err(failure("cursor id was not healed to live queue row"));
        }

        app.stop(&station).await?;
        let all_ids: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1 ORDER BY position")
            .bind(station.id)
            .fetch_all(&app.db)
            .await?;
        sqlx::query(
            "UPDATE stations SET current_queue_item_id = NULL, \
             consumed_queue_item_ids = $1, current_song_index = $2, \
             current_queue_cursor_format = 1 WHERE id = $3",
        )
        .bind(&all_ids)
        .bind(all_ids.len() as i32)
        .bind(station.id)
        .execute(&app.db)
        .await?;
        app.play(&station).await?;
        app.wait_playing(&station).await?;
        let reseeded = visible_upcoming(app, &station).await?;
        if reseeded < 4 {
            return Err(failure(format!("exhausted reseed left upcoming window short: {reseeded} rows")));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn reorder_during_playback_plays_the_moved_track_next() {
    run_streamer_test(async |app| {
        QueueMutationScenario {
            name: "Reorder head",
            slug: "reorder-head",
            action: QueueMutationAction::ReorderOnly,
        }
        .run(app)
        .await
    })
    .await
}

#[tokio::test]
#[serial]
async fn removed_staged_track_is_not_played_next() {
    run_streamer_test(async |app| {
        QueueMutationScenario {
            name: "Remove staged",
            slug: "remove-staged",
            action: QueueMutationAction::ReorderThenRemoveStaged,
        }
        .run(app)
        .await
    })
    .await
}

#[tokio::test]
#[serial]
async fn play_starts_with_the_queue_loaded_from_the_database() {
    run_streamer_test(async |app| {
        let station = app
            .create_station_with("Start from DB", "start-from-db", serde_json::json!({"transition_mode": "off"}))
            .await?;
        let tones = [(330.0, "tone A"), (440.0, "tone B"), (550.0, "tone C")];
        let songs = app.add_tones_to_library(&station, "startdb", &tones, 10).await?;
        app.enqueue_songs(&station, &songs).await?;

        // No streamer exists yet: starting the station must load the queue
        // from the database and begin playback with the first queued track.
        app.play(&station).await?;
        let url = format!("http://127.0.0.1:{}/start-from-db.mp3", app.port);
        app.assert_mount_serves_audio(&url).await?;

        let playing = app.wait_title_playing(&station, "tone A").await?;
        if playing.song_index != 0 {
            return Err(failure(format!("DB queue did not start at the first track: {playing:?}")));
        }

        // The loaded queue must advance to the next database row naturally.
        let next = app
            .wait_status(&station, "second track from DB", |status| {
                status.playing && status.title != "tone A"
            })
            .await?;
        if next.title != "tone B" || next.song_index != 1 {
            return Err(failure(format!("loaded queue did not advance to the second track: {next:?}")));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn repro_ws_queue_feed_survives_radio_restart() {
    // The UI Restart button replaces the streamer. The WS forward task
    // subscribes once to the (old) streamer's channels; when the old streamer
    // is stopped those channels close and the station feed dies while the
    // socket stays open — the frontend freezes on the last broadcast and the
    // user keeps seeing the queue from before the restart.
    let app = StreamerTestApp::new_http().await;
    app.run(async |app| {
        let station = app
            .create_station_with(
                "WS feed repro",
                "ws-feed-repro",
                serde_json::json!({"transition_mode": "autocue", "autocue_fade_max_ms": 5000}),
            )
            .await?;
        // AutoDJ is enabled by default for new stations; the WS feed
        // assertions expect exactly the queued sequence of events, so disable
        // it (its refills would inject extra queue updates and rows).
        app.disable_auto_fill(&station).await?;

        let mp3_bytes = app.generate_mp3().await?;
        let titles = ["tone A", "tone B", "tone C", "tone D"];
        let songs = app.add_analyzed_tracks_to_library(&station, "wsfeed", &titles, &mp3_bytes).await?;
        app.enqueue_songs(&station, &songs).await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        // Real browser-like WS session: auth, subscribe, then live updates.
        let token = app.session().auth.trim_start_matches("Bearer ").to_owned();
        let mut address = app.session().server.server_address().ok_or_else(|| failure("no server address"))?;
        address.set_scheme("ws").map_err(|_| failure("bad address"))?;
        let ws_url = address.join("/api/ws").map_err(|_| failure("bad ws url"))?;
        let (mut socket, _) = tokio_tungstenite::connect_async(ws_url.to_string()).await?;
        use futures::SinkExt;
        use tokio_tungstenite::tungstenite::Message as WsMessage;
        socket
            .send(WsMessage::Text(serde_json::json!({"type": "auth", "token": token}).to_string()))
            .await?;
        socket
            .send(WsMessage::Text(
                serde_json::json!({"type": "subscribe", "station_id": station.id}).to_string(),
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

        let first_items = app.fetch_queue(&station).await?;
        let id_of = |title: &str| -> Result<String, Box<dyn std::error::Error>> { item_id(&first_items, title) };

        // Edit while playing: the WS feed must broadcast the updated queue.
        let b_id = id_of("tone B")?;
        app.remove_queue_item(&station, &b_id).await?;
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
        app.restart(&station).await?;
        let after_edit = app.fetch_queue(&station).await?;
        let c_id = id_of("tone C")?;
        app.remove_queue_item(&station, &c_id).await?;
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
                    "station WS feed died after radio restart; UI freezes on the stale queue (db now: {after_edit:?})"
                )));
            }
        };
        if msg["data"].as_array().map(|a| a.len()) != Some(2) {
            return Err(failure(format!("unexpected post-restart queue: {}", msg)));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn repro_queue_modifications_persist_across_radio_restart() {
    // The user's exact claim: after modifying the queue (remove + reorder)
    // while the radio runs, a backend restart must read the MODIFIED queue
    // back from the database — not the old queue from before the radio start.
    run_streamer_test(async |app| {
        let station = app
            .create_station_with(
                "Queue persist repro",
                "queue-persist-repro",
                serde_json::json!({"transition_mode": "autocue", "autocue_fade_max_ms": 5000}),
            )
            .await?;
        // AutoDJ is enabled by default for new stations; the persistence
        // assertions compare the queue before and after edits, so disable it
        // (its refills would add rows the test never queued).
        app.disable_auto_fill(&station).await?;

        let mp3_bytes = app.generate_mp3().await?;
        let titles = ["tone A", "tone B", "tone C", "tone D"];
        let songs = app
            .add_analyzed_tracks_to_library(&station, "qpersist", &titles, &mp3_bytes)
            .await?;
        app.enqueue_songs(&station, &songs).await?;

        // First session: start the radio, wait until tone A is actually
        // playing, then modify the queue — remove tone B, move tone D to top.
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let first_items = app.fetch_queue(&station).await?;
        if queue_titles(&first_items) != ["tone A", "tone B", "tone C", "tone D"] {
            return Err(failure(format!("queue before edits: {first_items:?}")));
        }
        let id_of = |title: &str| -> Result<String, Box<dyn std::error::Error>> { item_id(&first_items, title) };
        let b_id = id_of("tone B")?;
        app.remove_queue_item(&station, &b_id).await?;
        // UI-faithful reorder payload: every displayed row (including the
        // now-playing row) renumbered in the new order.
        let d_id = id_of("tone D")?;
        let a_id = id_of("tone A")?;
        let c_id = id_of("tone C")?;
        app.reorder(&station, &[&d_id, &a_id, &c_id]).await?;
        let after_edits = app.fetch_queue(&station).await?;
        if queue_titles(&after_edits) != ["tone D", "tone A", "tone C"] {
            return Err(failure(format!("queue after edits: {after_edits:?}")));
        }

        // Radio restart: the UI Restart button kills the streamer and spawns a
        // fresh one from the database — the backend stays alive.
        app.restart(&station).await?;
        let reloaded = app.fetch_queue(&station).await?;
        if queue_titles(&reloaded) != ["tone D", "tone A", "tone C"] {
            return Err(failure(format!(
                "restart read the OLD queue: {reloaded:?} (expected tone D, tone A, tone C)"
            )));
        }
        // The restarted streamer must also play from the modified queue.
        app.wait_status(&station, "playing after restart", |status| {
            status.playing && status.title.starts_with("tone")
        })
        .await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn repro_play_resumes_after_server_restart() {
    run_streamer_test(async |app| {
        let station = app
            .create_station_with(
                "Restart repro",
                "restart-repro",
                serde_json::json!({"transition_mode": "autocue", "autocue_fade_max_ms": 5000}),
            )
            .await?;

        // Real analyzed mp3s — the exact user scenario (wav tones skip the
        // AutoCue seek path entirely).
        let mp3_bytes = app.generate_mp3().await?;
        if mp3_bytes.len() < 100_000 {
            return Err(failure(format!("generated mp3 suspiciously small: {} bytes", mp3_bytes.len())));
        }
        let tones = ["tone A", "tone B", "tone C"];
        let songs = app.add_analyzed_tracks_to_library(&station, "repro", &tones, &mp3_bytes).await?;
        app.enqueue_songs(&station, &songs).await?;

        // First session: start, wait until tone A is playing, then let the
        // backend "crash" mid-queue (drop server + streamers) and restart.
        app.play(&station).await?;
        let url = format!("http://127.0.0.1:{}/restart-repro.mp3", app.port);
        app.assert_mount_serves_audio(&url).await?;
        // Probe whether icecast actually receives audio bytes.
        let total = app.probe_tcp_bytes("/restart-repro.mp3", 100).await?;
        if total < 100 {
            return Err(failure(format!(
                "first session served only {total} bytes; pipeline is not broadcasting"
            )));
        }
        let playing = app.wait_title_playing(&station, "tone A").await?;
        if playing.song_index != 0 {
            return Err(failure(format!("first session index {playing:?}")));
        }
        // Give the clock a moment to prove the pipeline is alive.
        app.wait_elapsed(&station, 1)
            .await
            .map_err(|error| failure(format!("first session clock stalled after {playing:?}: {error}")))?;
        // Model a real backend crash: destroy the whole first session —
        // its streamers are shut down (the Icecast source disconnects,
        // exactly like a killed process dropping the socket), its
        // TestServer and session state are dropped for good.
        app.destroy_session().await?;

        // Second session: a fresh backend process over the same database.
        // Pressing play must resume from the persisted cursor, not hang.
        app.spawn_session(false, false).await?;
        app.play(&station).await?;
        // The restarted streamer must resume the DB cursor immediately:
        // tone A playing with an advancing clock — the exact user scenario
        // (queue loaded but --:-- and silent would fail here).
        let resumed = app.wait_title_playing(&station, "tone A").await?;
        app.wait_elapsed(&station, 1)
            .await
            .map_err(|error| failure(format!("restarted stream clock stalled after {resumed:?}: {error}")))?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn repro_cold_restart_icecast_comes_back_with_the_backend() {
    // The user's exact flow: the whole server (backend + managed icecast) is
    // restarted, then Start is pressed. A real restart kills the icecast child
    // process; the new backend must re-spawn it through the boot path
    // (kill_zombie_icecast + kill_by_port + spawn) before playback resumes.
    run_streamer_test(async |app| {
        let station = app
            .create_station_with(
                "Cold restart repro",
                "cold-restart-repro",
                serde_json::json!({"transition_mode": "autocue", "autocue_fade_max_ms": 5000}),
            )
            .await?;

        let mp3_bytes = app.generate_mp3().await?;
        if mp3_bytes.len() < 100_000 {
            return Err(failure(format!("generated mp3 suspiciously small: {} bytes", mp3_bytes.len())));
        }
        let songs = app
            .add_analyzed_tracks_to_library(&station, "cold", &["tone A", "tone B", "tone C"], &mp3_bytes)
            .await?;
        app.enqueue_songs(&station, &songs).await?;
        app.play(&station).await?;
        let url = format!("http://127.0.0.1:{}/cold-restart-repro.mp3", app.port);
        app.assert_mount_serves_audio(&url).await?;
        app.wait_title_playing(&station, "tone A").await?;

        // Crash: the backend process dies, taking its icecast child with
        // it — destroy the whole first session, then stop the old Icecast.
        app.destroy_session().await?;
        app.icecast.stop().await.unwrap();

        // Boot: a fresh backend process starts a brand-new IcecastManager —
        // exactly the main.rs boot path (zombie/port cleanup, config, spawn).
        let icecast = IcecastManager::new(app.icecast_dir_path.clone());
        icecast.start(app.port.into(), "surcast", "admin", "surcast").await.unwrap();
        app.replace_icecast(icecast);

        // Press Start again: must broadcast on the restarted icecast.
        app.spawn_session(false, false).await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;
        app.wait_elapsed(&station, 1)
            .await
            .map_err(|error| failure(format!("restarted stream clock stalled: {error}")))?;
        let url = format!("http://127.0.0.1:{}/cold-restart-repro.mp3", app.port);
        app.assert_mount_serves_audio(&url).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn repro_start_with_empty_queue_plays_once_songs_arrive() {
    // The user's report: Start pressed while the database queue is empty,
    // then the queue fills (manual add / Auto DJ / schedule). The streamer
    // must begin broadcasting as soon as songs arrive; otherwise the panel
    // shows the loaded queue stuck at "--:--/xx:xx" with nothing playing.
    run_streamer_test(async |app| {
        let station = app
            .create_station_with(
                "Empty start repro",
                "empty-start-repro",
                serde_json::json!({"transition_mode": "autocue", "autocue_fade_max_ms": 5000}),
            )
            .await?;

        // Start with an empty database queue: an idle streamer is created.
        app.play(&station).await?;
        let idle = app
            .wait_status(&station, "idle streamer", |status| !status.playing && status.total == 0)
            .await?;
        if idle.elapsed != 0 {
            return Err(failure(format!("idle streamer must not advance the clock: {idle:?}")));
        }

        // Songs arrive later; the queue fill must kick the idle streamer off.
        let mp3_bytes = app.generate_mp3().await?;
        let songs = app
            .add_analyzed_tracks_to_library(&station, "empty", &["tone A", "tone B", "tone C"], &mp3_bytes)
            .await?;
        app.enqueue_songs(&station, &songs).await?;

        // The idle streamer must now start broadcasting by itself.
        let playing = app
            .wait_title_playing(&station, "tone A")
            .await
            .map_err(|error| failure(format!("streamer stayed idle after the queue filled: {error}")))?;
        app.wait_elapsed(&station, 1)
            .await
            .map_err(|error| failure(format!("clock stalled after {playing:?}: {error}")))?;
        let url = format!("http://127.0.0.1:{}/empty-start-repro.mp3", app.port);
        app.assert_mount_serves_audio(&url).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn repro_autocue_analyzed_mp3_starts_playing() {
    run_streamer_test(async |app| {
        // Generate a real CBR mp3 (the e2e wav tones never exercise the
        // AutoCue seek path: analyzed=false songs skip it entirely).
        let mp3_bytes = app.generate_mp3().await?;
        if mp3_bytes.len() < 100_000 {
            return Err(failure(format!("generated mp3 suspiciously small: {} bytes", mp3_bytes.len())));
        }

        let station = app
            .create_station_with(
                "Autocue repro",
                "autocue-repro",
                serde_json::json!({"transition_mode": "autocue", "autocue_fade_max_ms": 5000}),
            )
            .await?;
        // analyzed=true with realistic cue points: the AutoCue plan seeks
        // both branches and installs volume control bindings.
        let songs = app
            .add_analyzed_tracks_to_library(&station, "analyzed", &["tone A", "tone B"], &mp3_bytes)
            .await?;
        app.enqueue_songs(&station, &songs).await?;

        app.play(&station).await?;
        let url = format!("http://127.0.0.1:{}/autocue-repro.mp3", app.port);
        app.assert_mount_serves_audio(&url).await?;
        // Read a chunk straight from TCP: proves the encoder actually pushes
        // data to icecast (reqwest here lacks the `stream` feature).
        let total = app.probe_tcp_bytes("/autocue-repro.mp3", 100).await?;
        if total < 100 {
            return Err(failure(format!(
                "icecast mount served only {total} bytes; pipeline is not broadcasting"
            )));
        }
        let playing = app.wait_title_playing(&station, "tone A").await?;
        if playing.song_index != 0 {
            return Err(failure(format!("autocue repro index {playing:?}")));
        }
        // The user's symptom is a stalled clock (--:--). Verify elapsed
        // actually advances while the pipeline is playing.
        app.wait_elapsed(&station, 1)
            .await
            .map_err(|error| failure(format!("elapsed clock stalled after {playing:?}: {error}")))?;
        Ok(())
    })
    .await
}
