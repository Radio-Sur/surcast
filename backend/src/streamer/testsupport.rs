//! Test-only fixtures shared by the streamer unit test modules.
//!
//! Four concerns live here, all `#[cfg(test)]`-gated and invisible to
//! production code:
//! - value builders with sensible defaults (`song`, `track`, `target`,
//!   `playback_config`, `unavailable_db`),
//! - the programmable [`RecordingPipeline`] stand-in for `PlaybackPipeline`
//!   (counts/records calls, injects failures, gates replace/reconnect),
//! - bounded polling helpers (`wait_for`, `wait_for_commands`),
//! - the [`HttpStub`] used by metadata HTTP tests.
//!
//! Controller and executor harnesses live inside their own modules because
//! they need access to private state; this module only holds what can be
//! expressed through public (crate-test) interfaces.

use crate::streamer::pipeline::{
    IcecastTarget, OutputConfig, PairPlan, PipelineConfig, PipelineError, PipelineSnapshot, PipelineState, PipelineTrack, RollingPlan,
    StationPlaybackConfig, TrackKey, TrackMetadata,
};
use crate::streamer::SongInfo;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

/// A normal queued song: distinct queue and song ids, title and position set
/// by the caller, everything else at neutral defaults.
pub(crate) fn queued_song(title: &str, position: i32) -> SongInfo {
    SongInfo {
        queue_item_id: Uuid::new_v4(),
        song_id: Uuid::new_v4(),
        title: title.to_owned(),
        artist: String::new(),
        duration: 1,
        file_path: String::new(),
        position,
        cue_in: 0.0,
        cue_out: 0.0,
        cross_start_next: 0.0,
        analyzed: false,
    }
}

/// A queue of songs named after their titles, in position order.
pub(crate) fn queued_songs(titles: &[&str]) -> Vec<SongInfo> {
    titles
        .iter()
        .enumerate()
        .map(|(position, title)| queued_song(title, position as i32))
        .collect()
}

/// A fresh track identity; the queue item and song ids are unrelated.
pub(crate) fn track_key() -> TrackKey {
    TrackKey {
        queue_item_id: Uuid::new_v4(),
        song_id: Uuid::new_v4(),
    }
}

/// A pipeline track bound to a file, with neutral metadata and cue points.
pub(crate) fn track(path: &Path, position: i32) -> PipelineTrack {
    PipelineTrack {
        key: track_key(),
        metadata: TrackMetadata {
            title: format!("Track {position}"),
            artist: format!("Artist {position}"),
        },
        path: path.to_path_buf(),
        cue_in: Duration::ZERO,
        cue_out: Duration::ZERO,
        cross_start_next: Duration::ZERO,
        analyzed: false,
    }
}

/// The default Icecast target used across the unit tests.
pub(crate) fn target() -> IcecastTarget {
    IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap()
}

/// The default (off, no fade) persisted playback configuration.
pub(crate) fn playback_config() -> StationPlaybackConfig {
    StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap()
}

/// A lazily-connected pool against a port that is never listening: every
/// acquire fails fast, so controller tests that must not touch a real
/// database (or must exercise the AutoDJ failure path) get a pool that can
/// be constructed anywhere.
pub(crate) fn unavailable_db() -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(10))
        .connect_lazy("postgres://surcast:surcast@127.0.0.1:1/surcast")
        .unwrap()
}

/// The default pipeline config for GStreamer harness tests.
pub(crate) fn pipeline_config() -> PipelineConfig {
    PipelineConfig {
        target: target(),
        output: OutputConfig {
            prebuffer_bytes: 1024,
            sample_rate: 44_100,
            channels: 2,
            bitrate_kbps: 128,
        },
    }
}

/// Polls `cond` (with cooperative yields) until it holds, panicking after
/// `timeout` with a descriptive message. Deterministic: no sleeps, so the
/// wait is bounded by actual progress of the condition.
pub(crate) async fn wait_for_timeout(timeout: Duration, what: &str, mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// [`wait_for_timeout`] with the standard two-second deadline.
pub(crate) async fn wait_for(what: &str, cond: impl FnMut() -> bool) {
    wait_for_timeout(Duration::from_secs(2), what, cond).await
}

/// A single pipeline call, as recorded by [`RecordingPipeline`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Call {
    Replace,
    Roll,
    ApplyOutput,
    SetPlaying,
    Reconnect,
    Snapshot,
    Stop,
}

/// Deterministic blocking gate: the pipeline signals `started` when the
/// gated operation enters the pipeline and blocks on `release` until the
/// test lets it through.
///
/// `release()` stores one permit that exactly one gated operation consumes,
/// so a release can never be lost: releasing before the operation reaches
/// the gate lets it pass straight through, releasing after it is blocked
/// wakes it. Tests that assert the operation is physically blocked (the
/// ordering scenarios) must still `wait_started().await` before
/// `release()` — the pipeline signals `started` and proceeds to block on
/// `release` without yielding in between, so once `wait_started` returns the
/// operation is guaranteed to be inside the gate.
pub(crate) struct Gate {
    pub(crate) started: Notify,
    release: Semaphore,
}

impl Gate {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Notify::new(),
            release: Semaphore::new(0),
        })
    }

    pub(crate) async fn wait_started(&self) {
        self.started.notified().await;
    }

    /// Releases exactly one gated operation. Safe at any point of the
    /// operation's lifetime: an early release is retained as a permit.
    pub(crate) fn release(&self) {
        self.release.add_permits(1);
    }

    /// Blocks until a `release()` permit is available, then permanently
    /// consumes it: one release lets exactly one gated operation through.
    pub(crate) async fn wait_released(&self) {
        self.release.acquire().await.expect("gate semaphore is never closed").forget();
    }
}

/// Programmable stand-in for the real GStreamer pipeline.
///
/// Replaces the many hand-rolled counting/failing/blocking pipelines with
/// one object that records every call, can fail a call permanently
/// ([`RecordingPipeline::fail`]), on its first occurrence
/// ([`RecordingPipeline::fail_once`]) or on the N-th invocation
/// ([`RecordingPipeline::fail_nth`]), reports the neutral stopped snapshot,
/// and can physically gate `replace`/`reconnect` for ordering tests.
/// No test currently needs a custom snapshot, so none is settable.
pub(crate) struct RecordingPipeline {
    calls: Mutex<Vec<Call>>,
    fail: Mutex<HashSet<Call>>,
    fail_once: Mutex<HashSet<Call>>,
    /// Zero-based attempt index that must fail for a call, removed after it
    /// fires (one-shot, like `fail_once` but for the N-th invocation).
    fail_nth: Mutex<HashMap<Call, usize>>,
    attempts: Mutex<HashMap<Call, usize>>,
    snapshot: Mutex<PipelineSnapshot>,
    pub(crate) replace_gate: Option<Arc<Gate>>,
    pub(crate) reconnect_gate: Option<Arc<Gate>>,
}

impl RecordingPipeline {
    pub(crate) fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail: Mutex::new(HashSet::new()),
            fail_once: Mutex::new(HashSet::new()),
            fail_nth: Mutex::new(HashMap::new()),
            attempts: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            }),
            replace_gate: None,
            reconnect_gate: None,
        }
    }

    /// A pipeline whose `replace` and `reconnect` block until released.
    pub(crate) fn with_gates() -> Self {
        Self {
            replace_gate: Some(Gate::new()),
            reconnect_gate: Some(Gate::new()),
            ..Self::new()
        }
    }

    /// Every future call of `call` fails with an injected pipeline error.
    pub(crate) fn fail(&self, call: Call) {
        self.fail.lock().insert(call);
    }

    /// The next call of `call` fails; later calls succeed.
    pub(crate) fn fail_once(&self, call: Call) {
        self.fail_once.lock().insert(call);
    }

    /// The `zero_based_attempt`-th invocation of `call` fails (0 = first);
    /// later calls succeed.
    pub(crate) fn fail_nth(&self, call: Call, zero_based_attempt: usize) {
        self.fail_nth.lock().insert(call, zero_based_attempt);
    }

    /// How many times `call` has been invoked (including failed attempts).
    pub(crate) fn count(&self, call: Call) -> usize {
        self.calls.lock().iter().filter(|recorded| **recorded == call).count()
    }

    /// The exact sequence of calls, in invocation order.
    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }

    fn record(&self, call: Call) -> Result<(), PipelineError> {
        self.calls.lock().push(call);
        let mut attempts = self.attempts.lock();
        let attempt = attempts.entry(call).or_default();
        if self.fail_once.lock().remove(&call) {
            *attempt += 1;
            return Err(PipelineError::Pipeline("injected failure".into()));
        }
        if self.fail_nth.lock().get(&call) == Some(attempt) {
            self.fail_nth.lock().remove(&call);
            *attempt += 1;
            return Err(PipelineError::Pipeline("injected failure".into()));
        }
        *attempt += 1;
        if self.fail.lock().contains(&call) {
            return Err(PipelineError::Pipeline("injected failure".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl crate::streamer::pipeline::PlaybackPipeline for RecordingPipeline {
    async fn replace(&self, _: PairPlan) -> Result<(), PipelineError> {
        self.record(Call::Replace)?;
        if let Some(gate) = &self.replace_gate {
            gate.started.notify_one();
            gate.wait_released().await;
        }
        Ok(())
    }

    async fn roll(&self, _: RollingPlan) -> Result<(), PipelineError> {
        self.record(Call::Roll)
    }

    async fn apply_output(&self, _: OutputConfig) -> Result<(), PipelineError> {
        self.record(Call::ApplyOutput)
    }

    async fn set_playing(&self, _: bool) -> Result<(), PipelineError> {
        self.record(Call::SetPlaying)
    }

    async fn reconnect(&self, _: IcecastTarget) -> Result<(), PipelineError> {
        self.record(Call::Reconnect)?;
        if let Some(gate) = &self.reconnect_gate {
            gate.started.notify_one();
            gate.wait_released().await;
        }
        Ok(())
    }

    async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
        self.record(Call::Snapshot)?;
        Ok(self.snapshot.lock().clone())
    }

    async fn stop(&self) -> Result<(), PipelineError> {
        self.record(Call::Stop)
    }
}

/// Writes a short stereo PCM WAV file with a constant sample (44.1 kHz).
pub(crate) fn write_wav(path: &Path, duration: Duration, sample: i16) {
    let frames = (duration.as_secs_f64() * 44_100.0).round() as u32;
    let data_len = frames * 4;
    let mut wav = Vec::with_capacity((44 + data_len) as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&44_100u32.to_le_bytes());
    wav.extend_from_slice(&176_400u32.to_le_bytes());
    wav.extend_from_slice(&4u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    for _ in 0..frames {
        wav.extend_from_slice(&sample.to_le_bytes());
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    std::fs::write(path, wav).unwrap();
}

async fn read_http_request(stream: &mut TcpStream) -> String {
    let mut request = Vec::new();
    loop {
        let mut chunk = vec![0; 1024];
        let length = stream.read(&mut chunk).await.unwrap();
        assert!(length > 0, "request ended before its form body");
        request.extend_from_slice(&chunk[..length]);
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length: ")?.parse::<usize>().ok())
            .unwrap_or_default();
        if request.len() >= header_end + 4 + content_length {
            return String::from_utf8(request).unwrap();
        }
    }
}

/// A minimal HTTP server for metadata tests: binds an ephemeral port,
/// accepts one connection per configured response, captures the full raw
/// request, and answers with the configured status (after an optional
/// delay). Requests are available through [`HttpStub::requests`].
pub(crate) struct HttpStub {
    pub(crate) port: u16,
    requests: Arc<Mutex<Vec<String>>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl HttpStub {
    /// Serves `responses` as `(status_line, delay_before_response)` pairs.
    pub(crate) async fn spawn(responses: &[(&'static str, Duration)]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let responses = responses.to_vec();
        let task = tokio::spawn(async move {
            for (status, delay) in responses {
                let Ok((mut stream, _)) = listener.accept().await else { break };
                let request = read_http_request(&mut stream).await;
                captured.lock().push(request);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                stream
                    .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes())
                    .await
                    .expect("HttpStub failed to write its response");
            }
        });
        Self {
            port,
            requests,
            task: Some(task),
        }
    }

    /// The captured raw requests, in arrival order.
    pub(crate) fn requests(&self) -> Vec<String> {
        self.requests.lock().clone()
    }

    /// Waits for the stub task to finish serving all configured responses.
    /// A panic or cancellation inside the fixture fails the test — it must
    /// never be swallowed.
    pub(crate) async fn join(&mut self) {
        if let Some(task) = self.task.take() {
            task.await.unwrap_or_else(|error| panic!("HttpStub task failed: {error}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Gate;
    use std::time::Duration;

    /// One `release()` permit is permanently consumed by exactly one gated
    /// operation: an early release is retained for the first waiter, and a
    /// second waiter blocks until a fresh release arrives.
    #[tokio::test]
    async fn gate_release_is_consumed_by_exactly_one_operation() {
        let gate = Gate::new();

        gate.release();
        gate.wait_released().await;

        let second_wait = tokio::spawn({
            let gate = gate.clone();
            async move { gate.wait_released().await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!second_wait.is_finished(), "second gated operation passed without a second release");

        gate.release();
        tokio::time::timeout(Duration::from_secs(1), second_wait)
            .await
            .expect("second release did not wake the blocked operation")
            .expect("spawned wait panicked");
    }
}
