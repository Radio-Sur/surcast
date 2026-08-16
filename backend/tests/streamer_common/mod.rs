//! Shared fixtures for the streamer E2E suite (`e2e_streamer.rs`).
//!
//! [`StreamerTestApp`] owns the durable test infrastructure every streamer
//! test recreates by hand: test database, upload temp dir, managed Icecast
//! on an ephemeral port and the app config. The currently live backend
//! (router/`TestServer`, streamers map, listeners state, authenticated admin
//! session) lives in a [`BackendSession`] that restart scenarios can destroy
//! and respawn against the same database. Domain operations
//! (`create_station`, `insert_tone`, `enqueue`, `play`, `wait_status`, ...)
//! keep scenarios readable; [`StreamerTestApp::run`] guarantees
//! streamer/Icecast teardown even when the scenario returns `Err`.
//!
//! Only repeated infrastructure and routine domain operations live here;
//! scenario-specific state (queue corruption, cursor manipulation, WebSocket
//! sessions, intentional Icecast restarts, direct database mutation) stays
//! visible in the tests.

use axum_test::TestServer;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use surcast_backend::api::router::{self, StreamersMap};
use surcast_backend::config::Config;
use surcast_backend::icecast::IcecastManager;
use surcast_backend::listeners::ListenersState;
use surcast_backend::stations::handlers::stream::StationLifecycleLocks;
use tempfile::TempDir;
use uuid::Uuid;

use crate::common::TestDb;

pub fn failure(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(message.into()))
}

/// A short stereo WAV with a sine tone at `frequency` Hz (44.1 kHz).
pub fn tone_wav(frequency: f32, seconds: u32) -> Vec<u8> {
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

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A station created through the API; `id` formats directly into URL paths
/// and `stream_url` is the mount name the backend serves as `/{stream_url}.mp3`.
#[derive(Clone, Debug)]
pub struct TestStation {
    pub id: Uuid,
    pub stream_url: String,
}

/// A song inserted into the library; `id` is a real database row id.
#[derive(Clone, Debug)]
pub struct TestSong {
    pub id: Uuid,
    pub title: String,
}

/// One row of the queue API response, deserialized strictly like
/// [`StatusView`] — a malformed body, a non-array response or a row missing
/// `id`/`title` fails the test instead of reading as an empty queue.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct QueueItem {
    pub id: String,
    pub title: String,
}

/// Typed view of `GET /stream/status`, deserialized strictly from the real
/// response schema — a missing field or a malformed body fails the test
/// instead of silently reading as a stopped/empty station.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct StatusView {
    pub playing: bool,
    pub title: String,
    pub song_index: u64,
    pub total: u64,
    pub elapsed: u64,
}

/// Durable test infrastructure plus the currently live backend session and
/// managed Icecast. [`StreamerTestApp::destroy_session`] /
/// [`StreamerTestApp::spawn_session`] model backend restarts; everything else
/// (database, upload dir, config, Icecast manager) survives them.
pub struct StreamerTestApp {
    /// Durable infrastructure shared by every backend session.
    pub db: PgPool,
    /// Owner of the isolated test database behind `db`; dropped (closed
    /// pool + DROP DATABASE) at the very end of teardown, after every other
    /// `PgPool` clone is gone.
    test_db: Option<TestDb>,
    pub files: TempDir,
    pub icecast_dir_path: PathBuf,
    pub port: u16,
    pub config: Config,
    pub admin_id: Uuid,
    pub client: reqwest::Client,
    /// The currently live managed Icecast instance.
    pub icecast: IcecastManager,
    /// The live backend session (server, streamers, listeners, auth);
    /// `None` only between `destroy_session` and `spawn_session`.
    session: Option<BackendSession>,
}

/// Teardown diagnostics appended to an initialization error: cleanup
/// failures must not replace the original error, but must not be lost.
fn teardown_failure_suffix(errors: &[String]) -> String {
    if errors.is_empty() {
        String::new()
    } else {
        format!("; teardown also failed: {}", errors.join("; "))
    }
}

/// Runs one fallible `StreamerTestApp::build` activation step. On error the
/// whole app is torn down (session streamers, Icecast child, test database)
/// and the original error is returned, augmented with any teardown failures.
async fn with_build_teardown(
    app: &mut StreamerTestApp,
    what: &str,
    step: impl AsyncFnOnce(&mut StreamerTestApp) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Err(error) = step(app).await {
        let teardown_errors = app.teardown().await;
        return Err(failure(format!("{what}: {error}{}", teardown_failure_suffix(&teardown_errors))));
    }
    Ok(())
}

/// State that belongs to one backend process lifetime: the router/`TestServer`,
/// the streamers map, listener state and the authenticated admin session.
/// Destroying a session shuts down its streamers and drops all of this, so a
/// fresh session can be created against the same durable infrastructure.
pub struct BackendSession {
    pub server: TestServer,
    pub streamers: StreamersMap,
    pub listeners: Arc<ListenersState>,
    pub auth: String,
    /// The session's per-station lifecycle locks; carries the session-scoped
    /// test hooks used by the lifecycle concurrency tests (used by
    /// `e2e_lifecycle` only).
    #[allow(dead_code)]
    pub lifecycle: Arc<StationLifecycleLocks>,
}

impl StreamerTestApp {
    pub async fn new() -> Self {
        Self::build(false)
            .await
            .unwrap_or_else(|error| panic!("test app initialization failed: {error}"))
    }

    /// Builds the server over a real HTTP transport (required for the
    /// WebSocket tests).
    pub async fn new_http() -> Self {
        Self::build(true)
            .await
            .unwrap_or_else(|error| panic!("test app initialization failed: {error}"))
    }

    async fn build(http_transport: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        // Local preparation first: every resource created here is cleaned up
        // by a plain Drop (the IcecastManager exists but has NOT started a
        // child process yet). Then the test DB is acquired immediately before
        // constructing StreamerTestApp — once Self owns TestDb, later
        // fallible resource activation is safe because every build error
        // path goes through teardown().
        let files = TempDir::new().unwrap();
        let icecast_dir = TempDir::new().unwrap();
        let icecast_dir_path = icecast_dir.path().to_path_buf();
        let port = free_port();
        let icecast = IcecastManager::new(icecast_dir.path().into());
        let mut config = crate::api_common::test_config();
        config.upload_dir = files.path().display().to_string();
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
        // Keep test DB acquisition immediately before ownership transfer into
        // Self: there must be no fallible operation in between.
        let test_db = crate::common::setup_db().await;
        let db = test_db.pool.clone();
        let mut app = Self {
            db,
            test_db: Some(test_db),
            files,
            icecast_dir_path,
            port,
            config,
            admin_id: Uuid::new_v4(),
            client,
            icecast,
            session: None,
        };
        // From here on, every operation that activates a durable/external
        // resource (Icecast child, backend session, SQL init) runs under
        // `with_build_teardown`, which tears the whole app down on error.
        with_build_teardown(&mut app, "icecast start failed", async |app| {
            app.icecast
                .start(port.into(), "surcast", "admin", "surcast")
                .await
                .map_err(failure)?;
            Ok(())
        })
        .await?;
        // The first session initializes the schema (fresh test DB per run);
        // restarted sessions skip setup and only log in. Any failure after
        // the Icecast child started must stop it again — `IcecastManager`
        // has no Drop, so a leaked child would keep running for the rest of
        // the test binary.
        with_build_teardown(&mut app, "first backend session failed to spawn", async |app| {
            app.spawn_session(http_transport, true).await
        })
        .await?;
        with_build_teardown(&mut app, "test app initialization failed", async |app| {
            sqlx::query(
                "UPDATE icecast_settings SET enabled=true, mode='managed', port=$1, \
                 source_password='surcast', admin_user='admin', admin_password='surcast'",
            )
            .bind(port as i32)
            .execute(&app.db)
            .await?;
            let (admin_id,): (Uuid,) = sqlx::query_as("SELECT id FROM users WHERE username='admin'")
                .fetch_one(&app.db)
                .await?;
            app.admin_id = admin_id;
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await?;
        Ok(app)
    }

    /// The live backend session. Panics (with a clear message) if the
    /// scenario destroyed the session without spawning a replacement.
    pub fn session(&self) -> &BackendSession {
        self.session.as_ref().expect("no live backend session; call spawn_session() first")
    }

    /// Simulates the backend process dying: shuts down every streamer of the
    /// current session (dropping the Icecast source connections), then drops
    /// the session's `TestServer`, streamers map, listeners state and auth.
    /// Durable infrastructure (database, upload dir, config, Icecast
    /// manager) survives, so [`StreamerTestApp::spawn_session`] can boot a
    /// fresh session against it.
    pub async fn destroy_session(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let session = self.session.take().ok_or_else(|| failure("no live backend session to destroy"))?;
        let active = { session.streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
        futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
        // Dropping the session here drops the TestServer, the streamers map,
        // the listeners state and the auth token for good.
        drop(session);
        Ok(())
    }

    /// Boots a fresh backend session: new `TestServer` over the same
    /// database/config/current Icecast, fresh streamers map, fresh login.
    /// `setup` runs `/api/setup/init` first (only valid on an empty
    /// database — the first session); restarted sessions pass `false`.
    /// Fails loudly if a session is already live (destroy it first).
    pub async fn spawn_session(&mut self, http_transport: bool, setup: bool) -> Result<(), Box<dyn std::error::Error>> {
        if self.session.is_some() {
            return Err(failure("a backend session is already live; call destroy_session() first"));
        }
        let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));
        let listeners = ListenersState::new();
        let lifecycle = Arc::new(surcast_backend::stations::handlers::stream::StationLifecycleLocks::default());
        let server = if http_transport {
            TestServer::builder().http_transport().build(router::create_router(
                self.db.clone(),
                self.config.clone(),
                streamers.clone(),
                lifecycle.clone(),
                self.icecast.clone(),
                listeners.clone(),
            ))
        } else {
            TestServer::new(router::create_router(
                self.db.clone(),
                self.config.clone(),
                streamers.clone(),
                lifecycle.clone(),
                self.icecast.clone(),
                listeners.clone(),
            ))
        };
        if setup {
            let setup_response = server
                .post("/api/setup/init")
                .json(&json!({"username":"admin","password":"admin123","name":"Admin"}))
                .await;
            if setup_response.status_code().as_u16() != 201 {
                return Err(failure(format!(
                    "setup/init failed: {}: {}",
                    setup_response.status_code(),
                    setup_response.text()
                )));
            }
        }
        let login = server
            .post("/api/auth/login")
            .json(&json!({"username":"admin","password":"admin123"}))
            .await;
        if login.status_code().as_u16() != 200 {
            return Err(failure(format!("login failed: {}: {}", login.status_code(), login.text())));
        }
        let body = login.text();
        let auth = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| value["access_token"].as_str().map(str::to_owned))
            .ok_or_else(|| failure(format!("login response has no access token; body: {body}")))?;
        // Model the production boot path: every station persisted as started
        // is started again (main.rs runs the same restore after Icecast is up).
        surcast_backend::stations::handlers::stream::restore_started_stations(&self.db, &streamers, &lifecycle, &self.config.upload_dir)
            .await;
        self.session = Some(BackendSession {
            server,
            streamers,
            listeners,
            auth: format!("Bearer {auth}"),
            lifecycle,
        });
        Ok(())
    }

    /// Replaces the managed Icecast instance (the cold-restart scenario stops
    /// the old one and boots a brand-new manager, mirroring the production
    /// boot path). The old manager must already be stopped; it is dropped
    /// here, so teardown only ever stops the live instance.
    pub fn replace_icecast(&mut self, icecast: IcecastManager) {
        self.icecast = icecast;
    }

    /// Runs the scenario and guarantees teardown afterwards: every active
    /// station streamer (including scenario-created maps), the managed
    /// Icecast and the isolated test database are released even when the
    /// scenario returned `Err` — or panicked (an assertion failure resumes
    /// the original panic after teardown, so the failure report is
    /// unchanged, but the test database is never leaked by a panic). An
    /// Icecast that the scenario already stopped itself is tolerated; any
    /// other teardown failure panics when the scenario succeeded.
    pub async fn run<R>(mut self, scenario: impl AsyncFnOnce(&mut Self) -> Result<R, Box<dyn std::error::Error>>) -> R {
        use futures::FutureExt;
        let result = std::panic::AssertUnwindSafe(async { scenario(&mut self).await })
            .catch_unwind()
            .await;
        let teardown_errors = self.teardown().await;
        match result {
            Ok(Ok(value)) => {
                if !teardown_errors.is_empty() {
                    panic!("scenario failed cleanly, but teardown failed: {}", teardown_errors.join("; "));
                }
                value
            }
            Ok(Err(error)) => {
                if teardown_errors.is_empty() {
                    panic!("{error}");
                }
                panic!("scenario failed: {error}; teardown also failed: {}", teardown_errors.join("; "));
            }
            Err(panic) => {
                // The scenario panicked (e.g. an assertion). Teardown has
                // already released streamers, Icecast and the test database;
                // resume the original panic so the failure report is
                // unchanged.
                if !teardown_errors.is_empty() {
                    eprintln!("scenario panicked; teardown also failed: {}", teardown_errors.join("; "));
                }
                std::panic::resume_unwind(panic);
            }
        }
    }

    /// Shuts down every live resource owned by the app: the current session's
    /// streamers, then the managed Icecast, then the isolated test database.
    /// Cleanup continues past individual failures; all errors are returned so
    /// the runner can report them alongside the scenario result instead of
    /// discarding them.
    async fn teardown(&mut self) -> Vec<String> {
        let mut errors = Vec::new();
        if let Some(session) = self.session.take() {
            let active = { session.streamers.lock().unwrap().values().cloned().collect::<Vec<_>>() };
            futures::future::join_all(active.into_iter().map(|streamer| async move { streamer.shutdown().await })).await;
            // Dropping the session drops the TestServer (AppState), the
            // streamers map and every other clone of the test pool, so the
            // DROP DATABASE below cannot race live connections.
            drop(session);
        }
        match self.icecast.stop().await {
            Err(message) if message.contains("not running") => {}
            Err(message) => errors.push(format!("icecast stop: {message}")),
            Ok(_) => {}
        }
        // The test database is released LAST, after every other PgPool clone
        // is gone: cleanup closes the pool and DROPs the database from the
        // maintenance connection. Also runs when initialization failed after
        // the database was created (build() calls teardown on its error
        // paths).
        if let Some(test_db) = self.test_db.take() {
            if let Err(message) = test_db.cleanup().await {
                errors.push(format!("test database cleanup: {message}"));
            }
        }
        errors
    }
}

impl BackendSession {
    /// Auth-injecting request helpers; every response is returned to the
    /// caller, which must assert its status (`expect`/`expect_success`) —
    /// no response may be silently ignored.
    pub async fn post(&self, path: &str, body: Option<Value>) -> axum_test::TestResponse {
        let request = self.server.post(path).add_header("Authorization", &self.auth);
        match body {
            Some(body) => request.json(&body).await,
            None => request.await,
        }
    }

    pub async fn put(&self, path: &str, body: Option<Value>) -> axum_test::TestResponse {
        let request = self.server.put(path).add_header("Authorization", &self.auth);
        match body {
            Some(body) => request.json(&body).await,
            None => request.await,
        }
    }

    pub async fn patch(&self, path: &str, body: Option<Value>) -> axum_test::TestResponse {
        let request = self.server.patch(path).add_header("Authorization", &self.auth);
        match body {
            Some(body) => request.json(&body).await,
            None => request.await,
        }
    }

    pub async fn delete(&self, path: &str) -> axum_test::TestResponse {
        self.server.delete(path).add_header("Authorization", &self.auth).await
    }

    pub async fn get(&self, path: &str) -> axum_test::TestResponse {
        self.server.get(path).add_header("Authorization", &self.auth).await
    }

    /// Asserts an exact status code; failures carry method/path/status/body.
    pub fn expect(
        &self,
        what: &str,
        response: axum_test::TestResponse,
        status: u16,
    ) -> Result<axum_test::TestResponse, Box<dyn std::error::Error>> {
        if response.status_code().as_u16() != status {
            return Err(failure(format!(
                "{what}: expected {status}, got {}: {}",
                response.status_code(),
                response.text()
            )));
        }
        Ok(response)
    }

    /// Asserts any success status (< 300).
    pub fn expect_success(
        &self,
        what: &str,
        response: axum_test::TestResponse,
    ) -> Result<axum_test::TestResponse, Box<dyn std::error::Error>> {
        if response.status_code().as_u16() >= 300 {
            return Err(failure(format!(
                "{what}: failed with {}: {}",
                response.status_code(),
                response.text()
            )));
        }
        Ok(response)
    }
}

impl StreamerTestApp {
    // ---- stations --------------------------------------------------------

    /// Creates a station with the default API transition mode (crossfade).
    pub async fn create_station(&self, name: &str, stream_url: &str) -> Result<TestStation, Box<dyn std::error::Error>> {
        self.create_station_with(name, stream_url, json!({})).await
    }

    /// Creates a station; `extra` fields are merged into the request body
    /// (e.g. `{"transition_mode": "off"}` or `{"transition_mode": "autocue",
    /// "autocue_fade_max_ms": 5000}`).
    pub async fn create_station_with(&self, name: &str, stream_url: &str, extra: Value) -> Result<TestStation, Box<dyn std::error::Error>> {
        let mut body = json!({ "name": name, "stream_url": stream_url, "prebuffer_bytes": 1024 });
        if let (Some(target), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            for (key, value) in extra {
                target.insert(key.clone(), value.clone());
            }
        }
        let response = self
            .session()
            .expect("station creation", self.session().post("/api/stations", Some(body)).await, 201)?;
        let station_id = response.json::<Value>()["id"]
            .as_str()
            .ok_or_else(|| failure("station response has no id"))?
            .to_owned();
        Ok(TestStation {
            id: Uuid::parse_str(&station_id)?,
            stream_url: stream_url.to_owned(),
        })
    }

    // ---- songs -----------------------------------------------------------

    /// Writes bytes into the upload `audio/` directory.
    pub fn write_audio(&self, file_name: &str, bytes: &[u8]) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let audio = self.files.path().join("audio");
        std::fs::create_dir_all(&audio)?;
        let path = audio.join(file_name);
        std::fs::write(&path, bytes)?;
        Ok(path)
    }

    /// Inserts an un-analyzed WAV tone song into the library.
    pub async fn insert_tone(
        &self,
        title: &str,
        file_name: &str,
        frequency: f32,
        seconds: u32,
    ) -> Result<TestSong, Box<dyn std::error::Error>> {
        self.write_audio(file_name, &tone_wav(frequency, seconds))?;
        let song_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration,uploaded_by) \
             VALUES ($1,$2,'test',$3,1,'audio/wav',$4,$5)",
        )
        .bind(song_id)
        .bind(title)
        .bind(file_name)
        .bind(seconds as i32)
        .bind(self.admin_id)
        .execute(&self.db)
        .await?;
        Ok(TestSong {
            id: song_id,
            title: title.to_owned(),
        })
    }

    /// Inserts an analyzed MP3 with realistic cue points (0.5 / 9.5 / 7.5),
    /// exactly like the AutoCue regression scenarios.
    pub async fn insert_analyzed_mp3(&self, title: &str, file_name: &str, bytes: &[u8]) -> Result<TestSong, Box<dyn std::error::Error>> {
        self.write_audio(file_name, bytes)?;
        let song_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO songs (id,title,artist,file_path,file_size,mime_type,duration, \
             uploaded_by,cue_in,cue_out,cross_start_next,analyzed_at) \
             VALUES ($1,$2,'test',$3,$4,'audio/mpeg',10,$5,0.5,9.5,7.5,NOW())",
        )
        .bind(song_id)
        .bind(title)
        .bind(file_name)
        .bind(bytes.len() as i32)
        .bind(self.admin_id)
        .execute(&self.db)
        .await?;
        Ok(TestSong {
            id: song_id,
            title: title.to_owned(),
        })
    }

    /// Assigns a song to a station's library.
    pub async fn assign(&self, song: &TestSong, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.session().expect_success(
            "song assignment",
            self.session()
                .post(
                    &format!("/api/songs/{}/stations", song.id),
                    Some(json!({"station_ids": [station.id]})),
                )
                .await,
        )?;
        Ok(())
    }

    /// Inserts and assigns `tones` (frequency, title) as sine-tone WAV songs.
    /// Insertion and assignment stay bundled, but the caller decides whether
    /// (and which subset of) the songs are queued.
    pub async fn add_tones_to_library(
        &self,
        station: &TestStation,
        prefix: &str,
        tones: &[(f32, &str)],
        seconds: u32,
    ) -> Result<Vec<TestSong>, Box<dyn std::error::Error>> {
        let mut songs = Vec::new();
        for (index, (frequency, title)) in tones.iter().enumerate() {
            let song = self
                .insert_tone(title, &format!("{prefix}-{index}.wav"), *frequency, seconds)
                .await?;
            self.assign(&song, station).await?;
            songs.push(song);
        }
        Ok(songs)
    }

    /// Inserts and assigns `titles` as analyzed MP3 songs with realistic cue
    /// points (0.5 / 9.5 / 7.5), exactly like the AutoCue regression
    /// scenarios.
    pub async fn add_analyzed_tracks_to_library(
        &self,
        station: &TestStation,
        prefix: &str,
        titles: &[&str],
        bytes: &[u8],
    ) -> Result<Vec<TestSong>, Box<dyn std::error::Error>> {
        let mut songs = Vec::new();
        for (index, title) in titles.iter().enumerate() {
            let song = self.insert_analyzed_mp3(title, &format!("{prefix}-{index}.mp3"), bytes).await?;
            self.assign(&song, station).await?;
            songs.push(song);
        }
        Ok(songs)
    }

    /// Enqueues every song in `songs` (in order) and returns the created
    /// queue rows.
    pub async fn enqueue_songs(&self, station: &TestStation, songs: &[TestSong]) -> Result<Vec<QueueItem>, Box<dyn std::error::Error>> {
        self.enqueue(station, &songs.iter().map(|song| song.id).collect::<Vec<_>>()).await
    }

    /// Generates a real CBR MP3 through `gst-launch-1.0` (the WAV tones do
    /// not exercise the AutoCue seek path).
    pub async fn generate_mp3(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mp3_dir = self.files.path().join("mp3");
        std::fs::create_dir_all(&mp3_dir)?;
        let mp3_path = mp3_dir.join("tone.mp3");
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
        Ok(std::fs::read(&mp3_path)?)
    }

    // ---- queue -----------------------------------------------------------

    /// Queues songs and returns the created queue rows; the response must
    /// succeed and parse strictly.
    pub async fn enqueue(&self, station: &TestStation, song_ids: &[Uuid]) -> Result<Vec<QueueItem>, Box<dyn std::error::Error>> {
        let response = self.session().expect_success(
            "queue creation",
            self.session()
                .post(&format!("/api/stations/{}/queue", station.id), Some(json!({"song_ids": song_ids})))
                .await,
        )?;
        parse_queue_items(&response.text())
    }

    /// Fetches the queue strictly: the endpoint must answer 200 and every row
    /// must deserialize. An error response can no longer look like a valid
    /// empty queue.
    pub async fn fetch_queue(&self, station: &TestStation) -> Result<Vec<QueueItem>, Box<dyn std::error::Error>> {
        let response = self.session().expect(
            "queue fetch",
            self.session().get(&format!("/api/stations/{}/queue", station.id)).await,
            200,
        )?;
        parse_queue_items(&response.text())
    }

    /// Inserts a song at `position` in the queue (position 0 = before the
    /// current track).
    pub async fn insert_queue_item(&self, station: &TestStation, song_id: Uuid, position: i32) -> Result<(), Box<dyn std::error::Error>> {
        self.session().expect(
            "queue insertion",
            self.session()
                .post(
                    &format!("/api/stations/{}/queue/insert", station.id),
                    Some(json!({"song_id": song_id, "position": position})),
                )
                .await,
            200,
        )?;
        Ok(())
    }

    pub async fn reorder(&self, station: &TestStation, item_ids: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        self.session().expect_success(
            "queue reorder",
            self.session()
                .put(
                    &format!("/api/stations/{}/queue/reorder", station.id),
                    Some(json!({"queue_item_ids": item_ids})),
                )
                .await,
        )?;
        Ok(())
    }

    pub async fn remove_queue_item(&self, station: &TestStation, item_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.session().expect(
            "queue removal",
            self.session()
                .delete(&format!("/api/stations/{}/queue/{}", station.id, item_id))
                .await,
            204,
        )?;
        Ok(())
    }

    // ---- playback --------------------------------------------------------

    /// Strict POST of a station-scoped action expecting exactly 200.
    async fn post_action(&self, station: &TestStation, path_suffix: &str, what: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.session().expect(
            what,
            self.session()
                .post(&format!("/api/stations/{}/{}", station.id, path_suffix), None)
                .await,
            200,
        )?;
        Ok(())
    }

    pub async fn play(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.post_action(station, "stream/play", "stream play").await
    }

    pub async fn pause(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.post_action(station, "stream/pause", "stream pause").await
    }

    pub async fn skip(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.post_action(station, "stream/skip", "stream skip").await
    }

    pub async fn stop(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.post_action(station, "stream/stop", "stream stop").await
    }

    pub async fn restart(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.post_action(station, "stream/restart", "stream restart").await
    }

    // ---- AutoDJ / schedule ----------------------------------------------

    pub async fn disable_auto_fill(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.session().expect(
            "auto-fill disable",
            self.session()
                .put(&format!("/api/stations/{}/auto-fill", station.id), Some(json!({"enabled": false})))
                .await,
            200,
        )?;
        Ok(())
    }

    /// Enables random station-library AutoDJ with the given songs-ahead
    /// window.
    pub async fn enable_auto_fill(
        &self,
        station: &TestStation,
        songs_ahead: u32,
        avoid_artist_repeat: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.session().expect(
            "auto-fill config",
            self.session()
                .put(
                    &format!("/api/stations/{}/auto-fill", station.id),
                    Some(json!({
                        "enabled": true,
                        "mode": "random",
                        "source_type": "station_library",
                        "avoid_artist_repeat": avoid_artist_repeat,
                        "songs_ahead": songs_ahead,
                    })),
                )
                .await,
            200,
        )?;
        Ok(())
    }

    pub async fn trigger_auto_fill(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        self.post_action(station, "auto-fill/trigger", "auto-fill trigger").await
    }

    // ---- status & waiting ------------------------------------------------

    /// Fetches `GET /stream/status` strictly: the endpoint must answer 200
    /// and the body must deserialize from the real response schema. A 500,
    /// a malformed body or a missing field fails immediately — it can no
    /// longer look like a valid stopped/empty station.
    pub async fn status(&self, station: &TestStation) -> Result<StatusView, Box<dyn std::error::Error>> {
        let response = self.session().expect(
            "stream status",
            self.session().get(&format!("/api/stations/{}/stream/status", station.id)).await,
            200,
        )?;
        let body = response.text();
        serde_json::from_str(&body).map_err(|error| failure(format!("stream status: malformed body: {error}; body: {body}")))
    }

    /// Generic bounded polling with timeout diagnostics. `poll` receives the
    /// app and returns `Some` once the condition holds.
    pub async fn wait_until<T, F>(
        &self,
        timeout: Duration,
        interval: Duration,
        what: &str,
        mut poll: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: AsyncFnMut(&StreamerTestApp) -> Option<T>,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(value) = poll(self).await {
                return Ok(value);
            }
            if Instant::now() >= deadline {
                return Err(failure(format!("{what} did not converge within {timeout:?}")));
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Polls the stream status until `predicate` holds (30s deadline); the
    /// error carries the last observed status.
    pub async fn wait_status(
        &self,
        station: &TestStation,
        what: &str,
        predicate: impl Fn(&StatusView) -> bool,
    ) -> Result<StatusView, Box<dyn std::error::Error>> {
        let timeout = Duration::from_secs(30);
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.status(station).await?;
            if predicate(&status) {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(failure(format!(
                    "{what} did not converge within {timeout:?}; last status: {status:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Waits until the station is playing.
    pub async fn wait_playing(&self, station: &TestStation) -> Result<StatusView, Box<dyn std::error::Error>> {
        self.wait_status(station, "station playing", |status| status.playing).await
    }

    /// Waits until the station plays a track past `after_index`; returns the
    /// status that crossed it.
    pub async fn wait_advance(&self, station: &TestStation, after_index: u64) -> Result<StatusView, Box<dyn std::error::Error>> {
        self.wait_status(station, "advance", |status| status.playing && status.song_index > after_index)
            .await
    }

    /// Waits until the station is stopped.
    pub async fn wait_stopped(&self, station: &TestStation) -> Result<StatusView, Box<dyn std::error::Error>> {
        self.wait_status(station, "station stopped", |status| !status.playing).await
    }

    /// Waits until `title` is the playing track (playing must be true).
    pub async fn wait_title_playing(&self, station: &TestStation, title: &str) -> Result<StatusView, Box<dyn std::error::Error>> {
        self.wait_status(station, &format!("\"{title}\" playing"), |status| {
            status.title == title && status.playing
        })
        .await
    }

    /// Waits until the playing clock reaches `min_elapsed` seconds.
    pub async fn wait_elapsed(&self, station: &TestStation, min_elapsed: u64) -> Result<StatusView, Box<dyn std::error::Error>> {
        self.wait_status(station, "clock advancing", |status| status.elapsed >= min_elapsed)
            .await
    }

    /// Waits until the Icecast mount answers with a success status.
    pub async fn open_mount(&self, url: &str) -> Result<reqwest::Response, Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.client.get(url).header("Icy-MetaData", "1").send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(200)).await,
                _ => return Err(failure("Icecast mount did not become available")),
            }
        }
    }

    /// Opens the mount and verifies it serves at least one audio chunk —
    /// the cross-check that proves the broadcast, not just the status.
    pub async fn assert_mount_serves_audio(&self, url: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut response = self.open_mount(url).await?;
        let chunk = tokio::time::timeout(Duration::from_secs(15), response.chunk()).await??;
        if chunk.is_none() || chunk.as_ref().is_none_or(|bytes| bytes.is_empty()) {
            return Err(failure("mount served no audio"));
        }
        Ok(())
    }

    /// Asserts the station's own mount (`/{stream_url}.mp3` on the app's
    /// Icecast port) serves audio.
    pub async fn assert_station_serves_audio(&self, station: &TestStation) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("http://127.0.0.1:{}/{}.mp3", self.port, station.stream_url);
        self.assert_mount_serves_audio(&url).await
    }

    /// Reads raw bytes straight from the Icecast TCP port (bypasses the
    /// reqwest client) until `min` bytes arrived or the deadline passes.
    pub async fn probe_tcp_bytes(&self, mount_path: &str, min: usize) -> Result<usize, Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", self.port)).await?;
        sock.write_all(format!("GET {mount_path} HTTP/1.0\r\nHost: localhost\r\n\r\n").as_bytes())
            .await?;
        let mut buf = [0u8; 4096];
        let mut total = 0usize;
        let deadline = Instant::now() + Duration::from_secs(5);
        while total < min && Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), sock.read(&mut buf[total..])).await {
                Ok(Ok(n)) if n > 0 => total += n,
                Ok(Ok(_)) => break,
                _ => {}
            }
        }
        Ok(total)
    }
}

/// Strict queue-row parsing: the body must deserialize directly into
/// `Vec<QueueItem>`. Malformed JSON, a non-array response, a missing `id`/
/// `title` or a wrong field type is an error, never a manufactured default
/// (an empty queue or blank rows could mask a broken endpoint and pass
/// regression tests).
fn parse_queue_items(body: &str) -> Result<Vec<QueueItem>, Box<dyn std::error::Error>> {
    serde_json::from_str::<Vec<QueueItem>>(body).map_err(|error| failure(format!("queue: malformed response: {error}; body: {body}")))
}

/// The queue titles in row order.
pub fn queue_titles(items: &[QueueItem]) -> Vec<String> {
    items.iter().map(|item| item.title.clone()).collect()
}

/// The queue item id of the row with `title`.
pub fn item_id(items: &[QueueItem], title: &str) -> Result<String, Box<dyn std::error::Error>> {
    items
        .iter()
        .find(|item| item.title == title)
        .map(|item| item.id.clone())
        .ok_or_else(|| failure(format!("no queue item titled {title}")))
}

/// The upcoming-window measure the frontend derives from the queue API:
/// `queue.len() - (song_index % len) - 1`.
pub fn visible_upcoming(status: &StatusView, queue: &[QueueItem]) -> i64 {
    let pos = if queue.is_empty() {
        0
    } else {
        (status.song_index as usize) % queue.len()
    };
    (queue.len().saturating_sub(pos + 1)) as i64
}

/// The `run_streamer_test` runner: builds the app, runs the scenario and
/// guarantees streamer/Icecast teardown before propagating the result.
pub async fn run_streamer_test<R>(scenario: impl AsyncFnOnce(&mut StreamerTestApp) -> Result<R, Box<dyn std::error::Error>>) -> R {
    StreamerTestApp::new().await.run(scenario).await
}

/// HTTP-transport variant of [`run_streamer_test`] used by
/// lifecycle/WebSocket tests. Not every test binary uses it.
#[allow(dead_code)]
pub async fn run_http_streamer_test<R>(scenario: impl AsyncFnOnce(&mut StreamerTestApp) -> Result<R, Box<dyn std::error::Error>>) -> R {
    StreamerTestApp::new_http().await.run(scenario).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    /// A panicking scenario must still release the isolated test database:
    /// `run()` catches the unwind, runs teardown (streamers, Icecast, test
    /// database) and only then resumes the original panic.
    #[tokio::test]
    async fn panicking_scenario_still_releases_the_test_database() {
        let app = StreamerTestApp::new().await;
        let db_name = app.test_db.as_ref().expect("the app must own a test database").db_name.clone();

        let outcome = std::panic::AssertUnwindSafe(app.run(async |_app| {
            panic!("simulated scenario panic");
            #[allow(unreachable_code)]
            Ok(())
        }))
        .catch_unwind()
        .await;
        assert!(outcome.is_err(), "the scenario panic must propagate after teardown");

        // The database is gone: the panic did not leak it.
        let Some(options) = surcast_backend::test_db::connection_options() else {
            return;
        };
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(options.database("postgres"))
            .await
            .expect("admin connection must work");
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(&db_name)
            .fetch_one(&admin_pool)
            .await
            .expect("pg_database lookup");
        admin_pool.close().await;
        assert!(!exists, "the panicking scenario leaked its test database '{db_name}'");
    }
}
