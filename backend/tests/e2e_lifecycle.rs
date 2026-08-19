//! Lifecycle regression suite: the persistent `is_started` desired state,
//! command/observation separation, per-station lifecycle serialization and
//! the startup restore.
//!
//! The core invariant covered here: observing a station (GET data, settings,
//! status, WebSocket Subscribe) must never start it, never create a runtime
//! and never change the persisted desired state; only explicit Play/Start/
//! Stop/Restart/Delete commands may. Server shutdown must not persist
//! `stopped`, and a backend restart restores exactly the stations whose
//! desired state is still `started` at the moment the restore runs.

// The shared fixtures expose helpers used by the other test binaries; this
// suite only exercises a subset of them.
#![allow(dead_code)]

#[allow(dead_code)]
mod api_common;
mod common;
mod streamer_common;

use serde_json::json;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use streamer_common::*;
use surcast_backend::stations::handlers::stream::LifecycleTestHooks;
use surcast_backend::streamer::StationStreamer;

type TestWs = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

// ---- WebSocket receive helpers ------------------------------------------

async fn ws_recv_text(socket: &mut TestWs, timeout: Duration) -> Result<String, Box<dyn std::error::Error>> {
    use futures::StreamExt;
    loop {
        match tokio::time::timeout(timeout, socket.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => return Ok(text.to_string()),
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(error))) => return Err(failure(format!("ws error: {error}"))),
            Ok(None) => return Err(failure("ws closed")),
            Err(_) => return Err(failure("ws receive timeout")),
        }
    }
}

/// Buffered reader for one WebSocket connection. `recv_until` never drops
/// unmatched messages — they are queued and re-examined by later calls, so
/// the order in which events arrive cannot lose an event (e.g. a
/// QueueUpdate arriving before a Status). Every `recv_until` call shares ONE
/// overall deadline instead of restarting the full timeout per message.
struct WsInbox {
    socket: TestWs,
    pending: std::collections::VecDeque<serde_json::Value>,
}

impl WsInbox {
    fn new(socket: TestWs) -> Self {
        Self {
            socket,
            pending: std::collections::VecDeque::new(),
        }
    }

    /// Reads until `matcher` matches (buffered messages first, then fresh
    /// ones); unmatched messages stay buffered. Bounded by one deadline.
    async fn recv_until<M>(&mut self, what: &str, matcher: M) -> Result<serde_json::Value, Box<dyn std::error::Error>>
    where
        M: Fn(&serde_json::Value) -> bool,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(index) = self.pending.iter().position(&matcher) {
                return Ok(self.pending.remove(index).expect("index from position"));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(failure(format!("did not receive {what} within the deadline")));
            }
            let msg: serde_json::Value = serde_json::from_str(&ws_recv_text(&mut self.socket, remaining).await?)?;
            if matcher(&msg) {
                return Ok(msg);
            }
            self.pending.push_back(msg);
        }
    }

    async fn wait_for_status<F>(&mut self, what: &str, predicate: F) -> Result<serde_json::Value, Box<dyn std::error::Error>>
    where
        F: Fn(&serde_json::Value) -> bool,
    {
        self.recv_until(what, |msg| msg["type"] == "status" && predicate(&msg["data"]["data"]))
            .await
    }

    async fn wait_for_queue_update<F>(&mut self, what: &str, predicate: F) -> Result<serde_json::Value, Box<dyn std::error::Error>>
    where
        F: Fn(&serde_json::Value) -> bool,
    {
        self.recv_until(what, |msg| msg["type"] == "queue_update" && predicate(&msg["data"]))
            .await
    }

    /// Waits for a queue snapshot whose rows are exactly `expected` titles,
    /// in order (the common one-song scenario).
    async fn wait_for_queue_titles(&mut self, what: &str, expected: &[&str]) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.wait_for_queue_update(what, |data| {
            data.as_array().is_some_and(|items| {
                items.len() == expected.len() && items.iter().zip(expected).all(|(item, title)| item["title"] == *title)
            })
        })
        .await
    }

    async fn wait_for_error(
        &mut self,
        what: &str,
        station_id: Option<uuid::Uuid>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        self.recv_until(what, |msg| {
            msg["type"] == "error"
                && match station_id {
                    Some(id) => msg["station_id"].as_str() == Some(id.to_string().as_str()),
                    None => true,
                }
        })
        .await
    }

    /// Shared negative assertion: proves that no message matching `is_bad`
    /// arrives within `window`. Buffered messages are checked first (a
    /// buffered bad message is already the side effect); legal non-bad
    /// messages read during the window are preserved in `pending` for later
    /// waits.
    async fn assert_no_event<F>(&mut self, window: Duration, what: &str, is_bad: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(&serde_json::Value) -> bool,
    {
        use futures::StreamExt;
        if self.pending.iter().any(&is_bad) {
            return Err(failure(format!("{what}: unexpected buffered message")));
        }
        let deadline = tokio::time::Instant::now() + window;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, self.socket.next()).await {
                // Nothing arrived within the window: the side effect is absent.
                Err(_) => return Ok(()),
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                    let msg: serde_json::Value = serde_json::from_str(&text)?;
                    if is_bad(&msg) {
                        return Err(failure(format!("{what}: unexpected message: {msg}")));
                    }
                    // Legal pipeline noise: preserve it for later waits.
                    self.pending.push_back(msg);
                }
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(error))) => return Err(failure(format!("ws error: {error}"))),
                Ok(None) => return Err(failure("ws closed")),
            }
        }
    }

    /// Proves that no `queue_update` arrives within `window`. Used to assert
    /// that a new subscriber does not cause a QueueUpdate for existing ones.
    async fn assert_no_queue_update(&mut self, window: Duration, what: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.assert_no_event(window, what, |msg| msg["type"] == "queue_update").await
    }

    /// Proves that no `error` arrives within `window` (optionally for one
    /// station). Used to assert that a successful transition never emits a
    /// transient no-runtime error.
    async fn assert_no_error(
        &mut self,
        window: Duration,
        what: &str,
        station_id: Option<uuid::Uuid>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.assert_no_event(window, what, |msg| {
            msg["type"] == "error"
                && match station_id {
                    Some(id) => msg["station_id"].as_str() == Some(id.to_string().as_str()),
                    None => true,
                }
        })
        .await
    }
}

/// Opens an authenticated WebSocket and subscribes to the station.
async fn ws_subscribe(app: &StreamerTestApp, station: &TestStation) -> Result<TestWs, Box<dyn std::error::Error>> {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    let token = app.session().auth.trim_start_matches("Bearer ").to_owned();
    let mut address = app.session().server.server_address().ok_or_else(|| failure("no server address"))?;
    address.set_scheme("ws").map_err(|_| failure("bad address"))?;
    let ws_url = address.join("/api/ws").map_err(|_| failure("bad ws url"))?;
    let (mut socket, _) = tokio_tungstenite::connect_async(ws_url.to_string()).await?;
    socket
        .send(WsMessage::Text(serde_json::json!({"type": "auth", "token": token}).to_string()))
        .await?;
    let _auth_ok = ws_recv_text(&mut socket, Duration::from_secs(10)).await?;
    socket
        .send(WsMessage::Text(
            serde_json::json!({"type": "subscribe", "station_id": station.id}).to_string(),
        ))
        .await?;
    Ok(socket)
}

/// Sends one WebSocket command and returns the socket.
async fn ws_send(socket: &mut TestWs, command: serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    socket
        .send(WsMessage::Text(command.to_string()))
        .await
        .map_err(|error| failure(format!("ws send failed: {error}")))
}

// ---- desired state / runtime helpers -------------------------------------

/// Reads the persisted desired state of a station.
async fn is_started(db: &sqlx::PgPool, station: &TestStation) -> Result<bool, Box<dyn std::error::Error>> {
    sqlx::query_scalar("SELECT is_started FROM stations WHERE id = $1")
        .bind(station.id)
        .fetch_one(db)
        .await
        .map_err(|error| failure(format!("is_started read failed: {error}")))
}

/// Writes the persisted desired state directly (test setup; not a real
/// transition, so no runtime is created or stopped).
async fn set_desired_started(app: &StreamerTestApp, station: &TestStation, started: bool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query("UPDATE stations SET is_started = $1 WHERE id = $2")
        .bind(started)
        .bind(station.id)
        .execute(&app.db)
        .await
        .map_err(|error| failure(format!("is_started update failed: {error}")))?;
    Ok(())
}

/// The number of live runtimes in the session's streamers map.
fn live_runtimes(app: &StreamerTestApp) -> usize {
    app.session().streamers.lock().unwrap().len()
}

/// Asserts the persisted desired state AND the live runtime count together.
async fn assert_lifecycle_state(
    app: &StreamerTestApp,
    station: &TestStation,
    started: bool,
    runtime_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let actual = is_started(&app.db, station).await?;
    if actual != started {
        return Err(failure(format!("desired state is {actual}, expected {started}")));
    }
    let runtimes = live_runtimes(app);
    if runtimes != runtime_count {
        return Err(failure(format!("live runtimes: {runtimes}, expected {runtime_count}")));
    }
    Ok(())
}

/// A station with one queued WAV tone, ready to broadcast.
async fn station_with_tone(app: &StreamerTestApp, name: &str, mount: &str) -> Result<TestStation, Box<dyn std::error::Error>> {
    let station = app.create_station(name, mount).await?;
    app.disable_auto_fill(&station).await?;
    let song = app.insert_tone("tone A", &format!("{mount}.wav"), 330.0, 10).await?;
    app.assign(&song, &station).await?;
    app.enqueue(&station, &[song.id]).await?;
    Ok(station)
}

/// Adds one more tone to a station's queue through the real API path
/// (insert → assign → enqueue).
async fn add_tone_to_station(
    app: &StreamerTestApp,
    station: &TestStation,
    title: &str,
    file_name: &str,
    freq: f32,
) -> Result<(), Box<dyn std::error::Error>> {
    let song = app.insert_tone(title, file_name, freq, 10).await?;
    app.assign(&song, station).await?;
    app.enqueue(station, &[song.id]).await?;
    Ok(())
}

// ---- raw HTTP command helpers --------------------------------------------

/// Like [`streamer_common::failure`], but the error is `Send + Sync` so it
/// can cross a `tokio::spawn` boundary (the raw command tasks).
fn failure_send(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(std::io::Error::other(message.into()))
}

/// Raw HTTP/1.1 POST (no body) over TCP, used from spawned tasks. The
/// reqwest client does not reach the axum-test server from spawned tasks in
/// this environment, while plain TCP does; a fresh connection per request
/// also avoids keep-alive interleaving between the concurrent commands.
async fn raw_post(base: &str, path: &str, auth: &str) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
    raw_post_json(base, path, auth, "").await
}

/// Like [`raw_post`], with a JSON request body.
async fn raw_post_json(base: &str, path: &str, auth: &str, body: &str) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = base.trim_start_matches("http://").trim_end_matches('/');
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: {auth}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream.write_all(request.as_bytes()).await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let head = String::from_utf8_lossy(&buf);
        head.lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse::<u16>().ok())
            .ok_or_else(|| std::io::Error::other(format!("malformed response: {head}")))
    })
    .await
    .map_err(|_| failure_send("raw post timed out"))?
    .map_err(|error| failure_send(format!("raw post failed: {error}")))
}

/// Spawns one lifecycle HTTP command (`play`/`stop`/`restart`) against the
/// live session. Transport errors are preserved, not mapped to a magic
/// status code, so a broken request is easy to diagnose.
fn spawn_stream_command(
    app: &StreamerTestApp,
    station_id: uuid::Uuid,
    command: &'static str,
) -> tokio::task::JoinHandle<Result<u16, Box<dyn std::error::Error + Send + Sync>>> {
    let base = app.session().server.server_address().expect("http server address").to_string();
    let auth = app.session().auth.clone();
    tokio::spawn(async move { raw_post(&base, &format!("/api/stations/{station_id}/stream/{command}"), &auth).await })
}

/// Awaits a spawned command and asserts its status code; a task panic or a
/// transport error surfaces with the command name.
async fn expect_command_status(
    task: tokio::task::JoinHandle<Result<u16, Box<dyn std::error::Error + Send + Sync>>>,
    what: &str,
    expected: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = task.await.map_err(|error| failure(format!("{what} task panicked: {error}")))?;
    let status = status.map_err(|error| failure(format!("{what} transport failed: {error}")))?;
    if status != expected {
        return Err(failure(format!("{what} answered {status}, expected {expected}")));
    }
    Ok(())
}

// ---- lifecycle test hooks -------------------------------------------------

/// Waits (bounded) until a lifecycle hook has been entered by a transition.
async fn wait_notified(notify: &tokio::sync::Notify, what: &str) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(10), notify.notified())
        .await
        .map_err(|_| failure(format!("timed out waiting for {what}")))
}

/// Asserts (bounded) that a transition has NOT reached a hook yet. Only
/// valid while the transition is deterministically blocked elsewhere (a
/// parked hook); the bound is a watchdog for the assertion, not a race.
async fn assert_not_entered(notify: &tokio::sync::Notify, what: &str) -> Result<(), Box<dyn std::error::Error>> {
    match tokio::time::timeout(Duration::from_millis(100), notify.notified()).await {
        Err(_) => Ok(()),
        Ok(_) => Err(failure(format!("{what} (expected it to still be blocked)"))),
    }
}

/// Session-scoped lifecycle hooks, armed for the duration of one concurrency
/// test. `disarm` (on drop, including on error) wakes every transition
/// parked at a hook — a failed assertion can never leave a request hung —
/// and leaves no stale state behind, so the next test starts clean.
struct LifecycleHookGuard<'a> {
    hooks: &'a LifecycleTestHooks,
}

impl Drop for LifecycleHookGuard<'_> {
    fn drop(&mut self) {
        self.hooks.before_runtime_create.disarm();
        self.hooks.before_stop.disarm();
    }
}

/// The lifecycle test hooks of the live session (all disarmed).
fn session_hooks(app: &StreamerTestApp) -> Arc<surcast_backend::stations::handlers::stream::StationLifecycleLocks> {
    Arc::clone(&app.session().lifecycle)
}

// ---- scenarios ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn observation_never_starts_a_station() {
    // Subscribing to a stopped station must deliver a stopped snapshot and
    // must NOT create a runtime, start the pipeline or persist started.
    run_http_streamer_test(async |app| {
        let station = app.create_station("Observe only", "observe-only").await?;

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("stopped snapshot", |data| data["playing"] == false).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn ws_play_starts_without_prior_runtime() {
    // WebSocket Play must work even when a previous Subscribe deliberately
    // created no runtime (the old lazy-start coupling is gone).
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "WS play", "ws-play").await?;
        if is_started(&app.db, &station).await? {
            return Err(failure("test setup must leave the station stopped"));
        }

        let mut socket = ws_subscribe(app, &station).await?;
        if live_runtimes(app) != 0 {
            return Err(failure("subscribe must not create a runtime"));
        }
        ws_send(&mut socket, serde_json::json!({"type": "play", "station_id": station.id})).await?;

        app.wait_until(
            Duration::from_secs(10),
            Duration::from_millis(25),
            "runtime after ws play",
            async |app| (live_runtimes(app) == 1).then_some(()),
        )
        .await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn rest_play_persists_started_and_broadcasts() {
    run_streamer_test(async |app| {
        let station = station_with_tone(app, "REST play", "rest-play").await?;
        app.play(&station).await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn stop_removes_runtime_and_persists() {
    run_streamer_test(async |app| {
        let station = station_with_tone(app, "Stop test", "stop-test").await?;
        app.play(&station).await?;
        app.wait_playing(&station).await?;

        app.stop(&station).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;

        // Idempotent: stopping again (no runtime) still answers success and
        // keeps the persisted state stopped.
        app.stop(&station).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn restart_starts_and_keeps_desired_started() {
    run_streamer_test(async |app| {
        let station = station_with_tone(app, "Restart test", "restart-test").await?;
        // Restart of a stopped station: restart implies the user wants it
        // running, so it starts explicitly (no hidden get/create play).
        app.restart(&station).await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.restart(&station).await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn pause_keeps_desired_started() {
    run_streamer_test(async |app| {
        let station = station_with_tone(app, "Pause test", "pause-test").await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;
        app.pause(&station).await?;
        if !is_started(&app.db, &station).await? {
            return Err(failure("pause must not persist stopped; desired state stays started"));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn idle_keeps_desired_started() {
    // A started station with nothing to play falls back to a stopped
    // pipeline state, but its desired state stays started.
    run_streamer_test(async |app| {
        let station = app.create_station("Idle test", "idle-test").await?;
        app.disable_auto_fill(&station).await?;
        app.play(&station).await?;
        app.wait_stopped(&station).await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn server_shutdown_keeps_desired_started() {
    // Graceful shutdown of the runtimes is a technical stop, not a user
    // decision: it must not persist is_started=false.
    run_streamer_test(async |app| {
        let station = station_with_tone(app, "Shutdown test", "shutdown-test").await?;
        app.play(&station).await?;
        app.wait_playing(&station).await?;
        app.destroy_session().await?;
        if !is_started(&app.db, &station).await? {
            return Err(failure("server shutdown must not persist is_started=false"));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn startup_restore_starts_only_started_stations() {
    run_streamer_test(async |app| {
        let started_station = station_with_tone(app, "Restore on", "restore-on").await?;
        let stopped_station = app.create_station("Restore off", "restore-off").await?;
        app.play(&started_station).await?;
        app.wait_playing(&started_station).await?;
        if is_started(&app.db, &stopped_station).await? {
            return Err(failure("test setup must leave the second station stopped"));
        }

        // Backend restart: the fresh session restores started stations only.
        app.destroy_session().await?;
        app.spawn_session(false, false).await?;
        if live_runtimes(app) != 1 {
            return Err(failure(format!(
                "startup restore started {} stations, expected exactly 1",
                live_runtimes(app)
            )));
        }
        if !is_started(&app.db, &started_station).await? {
            return Err(failure("restore lost the started station's desired state"));
        }
        if is_started(&app.db, &stopped_station).await? {
            return Err(failure("restore flipped a stopped station to started"));
        }
        app.wait_title_playing(&started_station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn startup_restore_continues_after_a_failing_station() {
    // One station that fails to construct (invalid persisted transition
    // mode) must be logged and skipped; the other started station still
    // comes up — a single broken station must not block the boot.
    run_streamer_test(async |app| {
        let broken = app.create_station("Broken restore", "broken-restore").await?;
        let healthy = app.create_station("Healthy restore", "healthy-restore").await?;
        sqlx::query("UPDATE stations SET is_started = true, transition_mode = 'bogus' WHERE id = $1")
            .bind(broken.id)
            .execute(&app.db)
            .await?;
        set_desired_started(app, &healthy, true).await?;

        app.destroy_session().await?;
        app.spawn_session(false, false).await?;
        let runtimes: Vec<Arc<StationStreamer>> = app.session().streamers.lock().unwrap().values().cloned().collect();
        if runtimes.len() != 1 {
            return Err(failure(format!(
                "restore after a failing station left {} runtimes, expected exactly the healthy one",
                runtimes.len()
            )));
        }
        let started_id = app.session().streamers.lock().unwrap().keys().next().copied();
        if started_id != Some(healthy.id) {
            return Err(failure(format!(
                "restore started the wrong station after a failure: {started_id:?}, expected {}",
                healthy.id
            )));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn startup_restore_rechecks_desired_state_under_the_lock() {
    // The restore's `find_started_stations` snapshot can be stale by the
    // time the per-station lock is acquired. Here the restore parks on
    // station A's runtime-create hook (lock held, re-check already passed)
    // while a concurrent Stop flips station B to stopped; the restore must
    // re-check B under B's lock and skip it — `is_started=false` must never
    // end up with a live runtime.
    run_http_streamer_test(async |app| {
        let a = station_with_tone(app, "Recheck aaa", "recheck-a").await?;
        let b = station_with_tone(app, "Recheck bbb", "recheck-b").await?;
        set_desired_started(app, &a, true).await?;
        set_desired_started(app, &b, true).await?;

        let lifecycle = Arc::clone(&app.session().lifecycle);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };
        hooks.before_runtime_create.arm();

        // Run the startup restore (the same function main.rs calls) in a
        // task; `find_started_stations` orders by name, so A comes first.
        let db = app.db.clone();
        let streamers = Arc::clone(&app.session().streamers);
        let lifecycle = Arc::clone(&app.session().lifecycle);
        let upload_dir = app.config.upload_dir.clone();
        let restore_task = tokio::spawn(async move {
            surcast_backend::stations::handlers::stream::restore_started_stations(&db, &streamers, &lifecycle, &upload_dir).await
        });
        // A is parked at the runtime-create hook: its re-check passed.
        wait_notified(hooks.before_runtime_create.entered(), "restore to reach station A's runtime hook").await?;
        assert!(!restore_task.is_finished(), "restore finished while station A was parked");

        // Concurrent Stop for B wins the race: B's desired state flips to
        // stopped while the restore still holds A's lock.
        set_desired_started(app, &b, false).await?;

        hooks.before_runtime_create.release();
        restore_task
            .await
            .map_err(|error| failure(format!("restore task panicked: {error}")))?;

        // Station A was restored and is playing; station B was stopped while
        // the restore was waiting on A's lock and must NOT have a runtime.
        assert_lifecycle_state(app, &a, true, 1).await?;
        if is_started(&app.db, &b).await? {
            return Err(failure("the concurrent stop of station B did not persist"));
        }
        if app.session().streamers.lock().unwrap().contains_key(&b.id) {
            return Err(failure("restore started station B after it was stopped concurrently"));
        }
        app.wait_title_playing(&a, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn delete_active_station_removes_runtime_and_record() {
    // Delete is a lifecycle transition: under the same per-station lock as
    // Play/Stop/Restart it stops and removes the live runtime, then deletes
    // the station row — no orphan runtime may keep broadcasting after the
    // station is gone, and a backend restart must not resurrect it.
    run_streamer_test(async |app| {
        let station = station_with_tone(app, "Delete test", "delete-test").await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;
        assert_lifecycle_state(app, &station, true, 1).await?;

        let response = app.session().delete(&format!("/api/stations/{}", station.id)).await;
        app.session().expect("station delete", response, 204)?;

        if live_runtimes(app) != 0 {
            return Err(failure("delete left a live runtime behind"));
        }
        let row: Option<bool> = sqlx::query_scalar("SELECT is_started FROM stations WHERE id = $1")
            .bind(station.id)
            .fetch_optional(&app.db)
            .await?;
        if row.is_some() {
            return Err(failure("delete left the station row behind"));
        }

        // A backend restart must not restore a deleted station.
        app.destroy_session().await?;
        app.spawn_session(false, false).await?;
        if live_runtimes(app) != 0 {
            return Err(failure("backend restart resurrected a deleted station's runtime"));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn subscribe_stopped_station_with_queue_receives_snapshot() {
    // A stopped station with a non-empty queue: Subscribe must deliver the
    // stopped status AND the current queue (read-only, straight from the
    // database) without creating a runtime or changing the desired state.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Stopped with queue", "stopped-queue").await?;
        if live_runtimes(app) != 0 {
            return Err(failure("test setup must leave the station without a runtime"));
        }

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("stopped status", |data| data["playing"] == false).await?;
        inbox.wait_for_queue_titles("queue snapshot", &["tone A"]).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn subscribe_started_station_without_runtime_receives_error_and_queue() {
    // Desired-started but no runtime (e.g. a failed startup restore):
    // Subscribe must NOT start the station, but the subscriber must get an
    // explicit error (not a legal stopped state) AND the current queue
    // (read-only DB snapshot) within a bounded time.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Started no runtime", "started-no-runtime").await?;
        set_desired_started(app, &station, true).await?;

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox
            .wait_for_error("explicit error for a started station without runtime", Some(station.id))
            .await?;
        inbox
            .wait_for_queue_titles("queue snapshot for a started station without runtime", &["tone A"])
            .await?;
        assert_lifecycle_state(app, &station, true, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn subscribe_stopped_then_play_resyncs_runtime_status() {
    // A client subscribed to a stopped station must NOT stay on the initial
    // stopped snapshot once the station starts: when a runtime appears,
    // `forward_station` must re-attach with a fresh current status + queue.
    // The initial snapshot was sent without a runtime, so the first runtime
    // attach is a full resync, never a skip.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Subscribe then play", "subscribe-play").await?;
        if live_runtimes(app) != 0 {
            return Err(failure("test setup must leave the station without a runtime"));
        }

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox
            .wait_for_status("initial stopped status", |data| data["playing"] == false)
            .await?;
        inbox.wait_for_queue_titles("initial queue snapshot", &["tone A"]).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;

        // Play starts the runtime; the SAME connection must receive the new
        // runtime's status + queue without re-subscribing. The runtime
        // broadcasts only SongChange events (no `playing` field) on start,
        // so a `playing == true` State can only come from the attach
        // snapshot — a stale stopped snapshot fails this deterministically.
        app.play(&station).await?;
        inbox
            .wait_for_status("runtime status after play", |data| data["playing"] == true)
            .await?;
        inbox.wait_for_queue_titles("queue after play", &["tone A"]).await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn restart_resyncs_status_and_queue_for_subscriber() {
    // Runtime replacement (Restart) must re-attach the SAME connection with
    // a fresh per-client current status + queue snapshot. A re-attach that
    // refreshes only the queue (not the status) would leave the old
    // runtime's status in the UI. The fresh runtime broadcasts only
    // SongChange events (no `playing` field) on start, so a
    // `playing == true` State after the restart can only come from the
    // attach snapshot — a queue-only re-attach fails this deterministically.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Restart resync", "restart-resync").await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("initial status", |data| data["playing"] == true).await?;
        inbox.wait_for_queue_titles("initial queue", &["tone A"]).await?;

        // Restart replaces the runtime; the existing subscriber must get the
        // new runtime's current status and a fresh queue snapshot.
        app.restart(&station).await?;
        inbox
            .wait_for_status("fresh status after restart", |data| data["playing"] == true)
            .await?;
        inbox.wait_for_queue_titles("fresh queue after restart", &["tone A"]).await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn subscribe_started_without_runtime_then_stop_sends_stopped_status() {
    // A subscriber of a desired-started station WITHOUT a runtime gets the
    // explicit no-runtime error. A Stop that succeeds without a runtime
    // must update the SAME connection: the stale no-runtime error is
    // replaced by a legal stopped status + queue — the no-runtime watcher
    // reacts to `is_started` changes, not only to runtimes appearing.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "No-runtime stop", "no-runtime-stop").await?;
        set_desired_started(app, &station, true).await?;

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_error("explicit no-runtime error", Some(station.id)).await?;
        inbox.wait_for_queue_titles("no-runtime queue snapshot", &["tone A"]).await?;
        assert_lifecycle_state(app, &station, true, 0).await?;

        app.stop(&station).await?;
        inbox
            .wait_for_status("stopped status after stop", |data| data["playing"] == false)
            .await?;
        inbox.wait_for_queue_titles("queue after stop", &["tone A"]).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn delete_while_subscribed_without_runtime_ends_forwarding() {
    // A deleted station never gets a runtime again; the no-runtime watcher
    // must end forwarding instead of polling forever, and the client gets
    // the same explicit "unknown station" error as a Subscribe to an
    // unknown station.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "No-runtime delete", "no-runtime-delete").await?;

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox
            .wait_for_status("initial stopped status", |data| data["playing"] == false)
            .await?;
        inbox.wait_for_queue_titles("initial queue", &["tone A"]).await?;

        let response = app.session().delete(&format!("/api/stations/{}", station.id)).await;
        app.session().expect("station delete", response, 204)?;

        inbox.wait_for_error("unknown station after delete", Some(station.id)).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn play_never_emits_transient_no_runtime_error() {
    // Play persists `is_started=true` and only then creates the runtime; a
    // no-runtime observer must NOT read that intermediate state as a
    // "started without runtime" failure. The observer synchronizes with the
    // same per-station lifecycle lock, so the play transition is finished
    // (runtime present) before any interpretation happens. The hook parks
    // the transition right after persistence/notification to widen the
    // window deterministically — an observer that read raw state would
    // emit the error here.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Play no transient", "play-no-transient").await?;
        let lifecycle = session_hooks(app);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("stopped status", |data| data["playing"] == false).await?;
        inbox.wait_for_queue_titles("initial queue", &["tone A"]).await?;

        // Park the play transition between persistence and runtime
        // creation: exactly the window that used to produce the transient
        // no-runtime error. The contention watcher proves the no-runtime
        // observer actually reached the station lock — and is waiting
        // behind the parked transition — BEFORE it is released; an
        // observer that read raw state would emit the error here.
        let mut contend = hooks.lock_contended.contend_watcher();
        hooks.before_runtime_create.arm();
        let play_task = spawn_stream_command(app, station.id, "play");
        wait_notified(hooks.before_runtime_create.entered(), "play to reach the runtime-create hook").await?;
        contend
            .wait("no-runtime observer to contend on the station lock behind parked play")
            .await?;

        hooks.before_runtime_create.release();
        expect_command_status(play_task, "play", 200).await?;

        inbox
            .wait_for_status("playing status after play", |data| data["playing"] == true)
            .await?;
        inbox.wait_for_queue_titles("queue after play", &["tone A"]).await?;
        inbox
            .assert_no_error(Duration::from_secs(1), "no no-runtime error during play", Some(station.id))
            .await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn restart_never_emits_transient_no_runtime_error() {
    // Restart removes the old runtime and creates a new one; during the gap
    // the station is started but runtime-less. The no-runtime observer must
    // not read that gap as a failure — it synchronizes with the lifecycle
    // lock, and the gap is only interpretable after the transition
    // finished. Parking the restart between persistence/notification and
    // runtime creation widens the gap deterministically.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Restart no transient", "restart-no-transient").await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let lifecycle = session_hooks(app);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("initial status", |data| data["playing"] == true).await?;
        inbox.wait_for_queue_titles("initial queue", &["tone A"]).await?;

        // Park the restart between persistence/notification and runtime
        // creation with the station lock held. The observer, driven by the
        // Lifecycle notification and the periodic recheck (the old runtime
        // is already removed), must reach the lock and wait behind the
        // parked transition — proven by the contention watcher BEFORE the
        // release; an observer that read raw state would emit the error.
        let mut contend = hooks.lock_contended.contend_watcher();
        hooks.before_runtime_create.arm();
        let restart_task = spawn_stream_command(app, station.id, "restart");
        wait_notified(hooks.before_runtime_create.entered(), "restart to reach the runtime-create hook").await?;
        contend
            .wait("no-runtime observer to contend on the station lock behind parked restart")
            .await?;

        hooks.before_runtime_create.release();
        expect_command_status(restart_task, "restart", 200).await?;

        inbox
            .wait_for_status("fresh status after restart", |data| data["playing"] == true)
            .await?;
        inbox.wait_for_queue_titles("fresh queue after restart", &["tone A"]).await?;
        inbox
            .assert_no_error(Duration::from_secs(1), "no no-runtime error during restart", Some(station.id))
            .await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn stopped_subscriber_receives_live_queue_update() {
    // A stopped station has no runtime to broadcast queue changes; the
    // central queue sync must still notify no-runtime observers, which
    // fetch a fresh read-only DB snapshot. The enqueue goes through the
    // real API handler and must not create a runtime.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Stopped queue live", "stopped-queue-live").await?;
        if live_runtimes(app) != 0 {
            return Err(failure("test setup must leave the station without a runtime"));
        }

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("stopped status", |data| data["playing"] == false).await?;
        inbox.wait_for_queue_titles("initial queue", &["tone A"]).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;

        add_tone_to_station(app, &station, "tone B", "stopped-queue-live-b", 440.0).await?;

        inbox.wait_for_queue_titles("queue after enqueue", &["tone A", "tone B"]).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn queue_mutation_serializes_with_inflight_play() {
    // A queue API mutation racing an in-flight Play must not be lost from
    // the runtime: the central queue sync serializes on the SAME station
    // lock as the transition, so it can neither observe the pre-transition
    // state ("no runtime" -> notify and return while Play is about to
    // insert a stale-queue runtime) nor finish before Play does. Old code
    // saw "no runtime" immediately and never contended on the lock.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Queue vs Play", "queue-vs-play").await?;
        let song_b = app.insert_tone("tone B", "queue-vs-play-b.wav", 440.0, 10).await?;
        app.assign(&song_b, &station).await?;

        let lifecycle = session_hooks(app);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("stopped status", |data| data["playing"] == false).await?;
        inbox.wait_for_queue_titles("initial queue", &["tone A"]).await?;

        // Park Play with the station lock held, before runtime creation.
        let mut observer_contended = hooks.lock_contended.contend_watcher();
        hooks.before_runtime_create.arm();
        let play_task = spawn_stream_command(app, station.id, "play");
        wait_notified(hooks.before_runtime_create.entered(), "play to park on the runtime-create hook").await?;
        // The no-runtime observer also contends on the lock (a deterministic
        // first signal); wait for it so the queue-sync contention below
        // cannot be mistaken for it.
        observer_contended
            .wait("no-runtime observer to contend on the station lock behind parked play")
            .await?;

        // Enqueue "tone B" through the real API while Play is parked: the
        // DB write may complete, but the queue sync must contend on the
        // SAME station lock instead of seeing "no runtime" and returning.
        let mut sync_contended = hooks.lock_contended.contend_watcher();
        let base = app.session().server.server_address().expect("http server address").to_string();
        let auth = app.session().auth.clone();
        let station_id = station.id;
        let song_b_id = song_b.id;
        let enqueue_task = tokio::spawn(async move {
            raw_post_json(
                &base,
                &format!("/api/stations/{station_id}/queue"),
                &auth,
                &json!({ "song_ids": [song_b_id] }).to_string(),
            )
            .await
        });
        sync_contended
            .wait("queue sync to contend on the station lock behind in-flight play")
            .await?;

        // The queue request is therefore still in flight; only now release
        // Play. Afterwards the runtime must carry both songs: either it was
        // created from the already-persisted rows, or the sync reloaded it
        // under the lock.
        hooks.before_runtime_create.release();
        expect_command_status(play_task, "play", 200).await?;
        let status = enqueue_task
            .await
            .map_err(|error| failure(format!("enqueue task panicked: {error}")))?;
        let status = status.map_err(|error| failure(format!("enqueue transport failed: {error}")))?;
        if status != 201 {
            return Err(failure(format!("enqueue answered {status}, expected 201")));
        }

        inbox
            .wait_for_queue_titles("queue after play and enqueue", &["tone A", "tone B"])
            .await?;
        assert_lifecycle_state(app, &station, true, 1).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn subscribing_second_client_does_not_broadcast_queue_update() {
    // A new subscriber's initial snapshot is delivered only to the new
    // client: B joining must not produce any QueueUpdate for A (no indirect
    // `push_queue_update` through `forward_station`). Bounded silence for A
    // after B confirmed its own snapshot proves the absence of the side
    // effect.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Two subscribers", "two-subscribers").await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let mut a = WsInbox::new(ws_subscribe(app, &station).await?);
        a.wait_for_status("A initial status", |data| data["playing"] == true).await?;
        a.wait_for_queue_titles("A initial queue", &["tone A"]).await?;
        // A's inbox is clean by itself (both initial messages consumed);
        // `pending` is never cleared by hand — a second unexpected
        // QueueUpdate buffered here must fail the test, not be discarded.
        a.assert_no_queue_update(Duration::from_millis(700), "A to stay silent before B joins")
            .await?;

        let mut b = WsInbox::new(ws_subscribe(app, &station).await?);
        b.wait_for_status("B initial status", |data| data["playing"] == true).await?;
        b.wait_for_queue_titles("B initial queue", &["tone A"]).await?;

        // B's join must not have caused any QueueUpdate for A.
        a.assert_no_queue_update(Duration::from_secs(1), "A to receive no queue update after B joins")
            .await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn concurrent_play_then_stop_serializes_to_stopped() {
    // The exact race from review: Play persists `started` and then parks
    // while creating the runtime; Stop arrives meanwhile. Without the
    // per-station lifecycle lock, Stop would "succeed" (no runtime in the
    // map yet) and Play would then bring up a live runtime under
    // `is_started=false`. With the lock, Stop runs as the next serialized
    // transition after Play and ends the station.
    run_http_streamer_test(async |app| {
        let lifecycle = session_hooks(app);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };
        let station = station_with_tone(app, "Race play-stop", "race-play-stop").await?;

        // Play parks after persisting `started`, while holding the lock.
        hooks.before_runtime_create.arm();
        let play_task = spawn_stream_command(app, station.id, "play");
        wait_notified(hooks.before_runtime_create.entered(), "play to reach the runtime-create hook").await?;

        // Stop arrives while Play holds the lock: it must reach the SAME
        // station mutex, observe it busy (contention signal) and must NOT
        // reach the stop transition yet — the hook itself never blocks it.
        hooks.before_stop.arm();
        let mut contended = hooks.lock_contended.contend_watcher();
        let stop_task = spawn_stream_command(app, station.id, "stop");
        contended.wait("stop to contend on the station mutex").await?;
        assert_not_entered(
            hooks.before_stop.entered(),
            "stop must not reach the stop hook while play holds the lock",
        )
        .await?;
        if stop_task.is_finished() {
            return Err(failure("stop finished while play still held the lifecycle lock"));
        }

        // Let Play finish; Stop then runs as the next serialized transition.
        hooks.before_runtime_create.release();
        expect_command_status(play_task, "play", 200).await?;
        wait_notified(hooks.before_stop.entered(), "stop to run after play").await?;
        hooks.before_stop.release();
        expect_command_status(stop_task, "stop", 200).await?;

        assert_lifecycle_state(app, &station, false, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn concurrent_stop_then_play_serializes_to_started() {
    // Stop parks inside its transition holding the lock; Play arrives
    // meanwhile and must wait. After Stop completes, Play runs as the next
    // serialized transition: desired state `started` and exactly one live
    // runtime.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Race stop-play", "race-stop-play").await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        // Arm only now: the initial play above must not park.
        let lifecycle = session_hooks(app);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };

        // Stop parks inside its transition holding the lock.
        hooks.before_stop.arm();
        let stop_task = spawn_stream_command(app, station.id, "stop");
        wait_notified(hooks.before_stop.entered(), "stop to reach the stop hook").await?;

        // Play arrives while Stop holds the lock: it must reach the SAME
        // station mutex, observe it busy, and not pass it until Stop is done.
        hooks.before_runtime_create.arm();
        let mut contended = hooks.lock_contended.contend_watcher();
        let play_task = spawn_stream_command(app, station.id, "play");
        contended.wait("play to contend on the station mutex").await?;
        assert_not_entered(
            hooks.before_runtime_create.entered(),
            "play must not reach the runtime-create hook while stop holds the lock",
        )
        .await?;
        if play_task.is_finished() {
            return Err(failure("play finished while stop still held the lifecycle lock"));
        }

        // Stop completes; Play runs as the next serialized transition.
        hooks.before_stop.release();
        expect_command_status(stop_task, "stop", 200).await?;
        wait_notified(hooks.before_runtime_create.entered(), "play to run after stop").await?;
        hooks.before_runtime_create.release();
        expect_command_status(play_task, "play", 200).await?;

        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn committed_play_survives_caller_cancellation_after_persistence() {
    // Play persists `is_started=true` inside the committed operation; the
    // request caller is only a JoinHandle awaiter. Killing the caller
    // while the operation parks between persistence and runtime creation
    // must not stop the transition: the operation keeps the lifecycle
    // lock, creates the runtime and only then releases the station.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Committed play", "committed-play").await?;
        let lifecycle = session_hooks(app);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };

        // Play parks inside the committed operation, AFTER persistence and
        // notification, BEFORE the runtime is created.
        hooks.before_runtime_create.arm();
        let play_task = spawn_stream_command(app, station.id, "play");
        wait_notified(hooks.before_runtime_create.entered(), "play to reach the runtime-create hook").await?;
        assert_lifecycle_state(app, &station, true, 0).await?;

        // The request caller dies mid-operation: the committed play must
        // keep the guard and still bring up the runtime.
        play_task.abort();
        assert!(play_task.await.is_err(), "the Play caller must be cancelled");

        // A new transition must NOT pass: it contends on the station lock
        // held by the committed play operation.
        let mut contended = hooks.lock_contended.contend_watcher();
        let next_transition = tokio::spawn({
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                let _guard = lifecycle.lock(station.id).await;
            }
        });
        contended.wait("the next transition to contend behind the committed play").await?;

        hooks.before_runtime_create.release();
        next_transition
            .await
            .expect("the next transition must proceed once the committed play finished");
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn committed_restart_survives_caller_cancellation_after_old_runtime_shutdown() {
    // Restart's terminal boundary is the old runtime's Shutdown admission.
    // The test parks the committed restart tail at the fresh-runtime hook —
    // old runtime already terminal, `is_started=true` persisted — then
    // kills the caller: the tail must keep the lock, start the fresh
    // runtime and only then release the station.
    run_http_streamer_test(async |app| {
        let station = station_with_tone(app, "Committed restart", "committed-restart").await?;
        app.play(&station).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let lifecycle = session_hooks(app);
        let hooks = lifecycle.test_hooks();
        let _guard = LifecycleHookGuard { hooks };

        // The restart tail reaches the fresh-runtime hook only after the
        // old runtime was stopped and removed and the desired state was
        // persisted again.
        hooks.before_runtime_create.arm();
        let restart_task = spawn_stream_command(app, station.id, "restart");
        wait_notified(hooks.before_runtime_create.entered(), "restart to reach the runtime-create hook").await?;
        assert_lifecycle_state(app, &station, true, 0).await?;

        // The request caller dies mid-tail: the committed restart must
        // keep the guard and still start the fresh runtime.
        restart_task.abort();
        assert!(restart_task.await.is_err(), "the Restart caller must be cancelled");

        // A new transition must NOT pass: it contends on the station lock
        // held by the committed restart tail.
        let mut contended = hooks.lock_contended.contend_watcher();
        let next_transition = tokio::spawn({
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                let _guard = lifecycle.lock(station.id).await;
            }
        });
        contended
            .wait("the next transition to contend behind the committed restart")
            .await?;

        hooks.before_runtime_create.release();
        next_transition
            .await
            .expect("the next transition must proceed once the committed restart finished");
        assert_lifecycle_state(app, &station, true, 1).await?;
        app.wait_title_playing(&station, "tone A").await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn restore_does_not_modify_updated_at() {
    // A plain backend restart must not rewrite persisted station data: the
    // startup restore re-creates the persisted intent, it does not persist
    // it again (no UPDATE, no `updated_at` bump).
    run_streamer_test(async |app| {
        let station = station_with_tone(app, "Restore updated_at", "restore-updated-at").await?;
        app.play(&station).await?;
        app.wait_playing(&station).await?;
        let before: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT updated_at FROM stations WHERE id = $1")
            .bind(station.id)
            .fetch_one(&app.db)
            .await?;

        // Backend restart: the fresh session restores started stations.
        app.destroy_session().await?;
        app.spawn_session(false, false).await?;
        app.wait_title_playing(&station, "tone A").await?;

        let after: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT updated_at FROM stations WHERE id = $1")
            .bind(station.id)
            .fetch_one(&app.db)
            .await?;
        if before != after {
            return Err(failure(format!("restore modified updated_at: before {before}, after {after}")));
        }
        if !is_started(&app.db, &station).await? {
            return Err(failure("restore lost the desired started state"));
        }
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn stopped_subscriber_receives_queue_update_after_song_unassign() {
    // Unassigning a song from a station (`DELETE /api/songs/{id}/stations/{sid}`)
    // structurally changes the queue: the stopped subscriber on the SAME
    // socket must get the fresh empty queue (no re-subscribe), no runtime
    // may appear, and the desired state stays false.
    run_http_streamer_test(async |app| {
        let station = app.create_station("Unassign", "unassign").await?;
        app.disable_auto_fill(&station).await?;
        let song = app.insert_tone("tone A", "unassign.wav", 330.0, 10).await?;
        app.assign(&song, &station).await?;
        app.enqueue(&station, &[song.id]).await?;

        let mut inbox = WsInbox::new(ws_subscribe(app, &station).await?);
        inbox.wait_for_status("stopped status", |data| data["playing"] == false).await?;
        inbox.wait_for_queue_titles("queue with the assigned song", &["tone A"]).await?;

        let response = app
            .session()
            .delete(&format!("/api/songs/{}/stations/{}", song.id, station.id))
            .await;
        app.session().expect("song unassign", response, 204)?;

        inbox.wait_for_queue_titles("queue after unassign", &[]).await?;
        assert_lifecycle_state(app, &station, false, 0).await?;
        Ok(())
    })
    .await
}

#[tokio::test]
#[serial]
async fn global_song_delete_fans_out_to_affected_stopped_subscribers() {
    // Deleting a song removes it from every station queue. The delete runs
    // as `DELETE ... RETURNING station_id` inside a transaction, so the
    // affected stations come from the queue rows actually removed; they are
    // deduplicated for the fan-out. This test exercises that whole chain
    // together: both affected stopped stations' subscribers get a fresh
    // empty queue on the SAME socket, no runtime may appear and the desired
    // state of both stations stays false.
    run_http_streamer_test(async |app| {
        let station_a = app.create_station("Delete fan-out A", "fan-a").await?;
        app.disable_auto_fill(&station_a).await?;
        let station_b = app.create_station("Delete fan-out B", "fan-b").await?;
        app.disable_auto_fill(&station_b).await?;
        let song = app.insert_tone("shared tone", "fan-shared.wav", 330.0, 10).await?;
        app.assign(&song, &station_a).await?;
        app.assign(&song, &station_b).await?;
        app.enqueue(&station_a, &[song.id]).await?;
        app.enqueue(&station_b, &[song.id]).await?;

        let mut inbox_a = WsInbox::new(ws_subscribe(app, &station_a).await?);
        let mut inbox_b = WsInbox::new(ws_subscribe(app, &station_b).await?);
        inbox_a.wait_for_status("stopped status A", |data| data["playing"] == false).await?;
        inbox_a.wait_for_queue_titles("initial queue A", &["shared tone"]).await?;
        inbox_b.wait_for_status("stopped status B", |data| data["playing"] == false).await?;
        inbox_b.wait_for_queue_titles("initial queue B", &["shared tone"]).await?;

        let response = app.session().delete(&format!("/api/songs/{}", song.id)).await;
        app.session().expect("global song delete", response, 204)?;

        inbox_a.wait_for_queue_titles("queue A after global delete", &[]).await?;
        inbox_b.wait_for_queue_titles("queue B after global delete", &[]).await?;
        assert_lifecycle_state(app, &station_a, false, 0).await?;
        assert_lifecycle_state(app, &station_b, false, 0).await?;
        Ok(())
    })
    .await
}
