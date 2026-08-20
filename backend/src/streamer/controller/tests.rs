use std::sync::Arc;

use super::*;
use crate::streamer::driver::{PipelineDriver, PipelineOperation};
use crate::streamer::runtime::{StationCommand, StationRuntime};
use crate::streamer::testsupport::{self, queued_song, queued_songs, Call, ExpectedPipelineOperations, Gate, RecordingPipeline};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::oneshot::error::TryRecvError as OneshotTryRecvError;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

/// A fluent builder for configuring and initializing controller test scenarios.
///
/// Supports setting initial playback states (`Playing`, `Paused`, `Stopped`, `Idle`),
/// queues from titles or [`SongInfo`] slices, custom pipelines (gates/failure injection),
/// database pools, generation / output epoch offsets, and explicit current or staged tracks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScenarioPreset {
    Stopped,
    Playing,
    Paused,
    Idle,
}

/// A fluent builder for configuring and initializing controller test scenarios.
///
/// Supports setting initial playback states (`Playing`, `Paused`, `Stopped`, `Idle`),
/// queues from titles or [`SongInfo`] slices, custom pipelines (gates/failure injection),
/// database pools, generation / output epoch offsets, and explicit current or staged tracks.
#[allow(dead_code)]
struct ControllerScenario {
    preset: ScenarioPreset,
    db: PgPool,
    pipeline: Arc<RecordingPipeline>,
    songs: Vec<SongInfo>,
    explicit_current: Option<usize>,
    explicit_staged: Option<Option<usize>>,
    explicit_generation: Option<u64>,
    explicit_output_epoch: Option<u64>,
}

#[allow(dead_code)]
impl ControllerScenario {
    /// Creates a scenario in the [`PipelineState::Stopped`] state.
    fn stopped() -> Self {
        Self {
            preset: ScenarioPreset::Stopped,
            db: testsupport::unavailable_db(),
            pipeline: Arc::new(RecordingPipeline::new()),
            songs: Vec::new(),
            explicit_current: None,
            explicit_staged: None,
            explicit_generation: None,
            explicit_output_epoch: None,
        }
    }

    /// Creates a scenario in the [`PipelineState::Playing`] state over standard tracks `A -> B -> C`.
    fn playing() -> Self {
        Self {
            preset: ScenarioPreset::Playing,
            db: testsupport::unavailable_db(),
            pipeline: Arc::new(RecordingPipeline::new()),
            songs: testsupport::queued_songs(&["A", "B", "C"]),
            explicit_current: None,
            explicit_staged: None,
            explicit_generation: None,
            explicit_output_epoch: None,
        }
    }

    /// Creates a scenario in the [`PipelineState::Paused`] state over standard tracks `A -> B -> C`.
    fn paused() -> Self {
        Self {
            preset: ScenarioPreset::Paused,
            db: testsupport::unavailable_db(),
            pipeline: Arc::new(RecordingPipeline::new()),
            songs: testsupport::queued_songs(&["A", "B", "C"]),
            explicit_current: None,
            explicit_staged: None,
            explicit_generation: None,
            explicit_output_epoch: None,
        }
    }

    /// Creates an idle scenario (stopped due to empty queue, auto-resumable).
    fn idle() -> Self {
        Self {
            preset: ScenarioPreset::Idle,
            db: testsupport::unavailable_db(),
            pipeline: Arc::new(RecordingPipeline::new()),
            songs: Vec::new(),
            explicit_current: None,
            explicit_staged: None,
            explicit_generation: None,
            explicit_output_epoch: None,
        }
    }

    fn with_queue(mut self, titles: &[&str]) -> Self {
        self.songs = testsupport::queued_songs(titles);
        self
    }

    fn with_songs(mut self, songs: Vec<SongInfo>) -> Self {
        self.songs = songs;
        self
    }

    fn with_pipeline(mut self, pipeline: Arc<RecordingPipeline>) -> Self {
        self.pipeline = pipeline;
        self
    }

    fn with_db(mut self, db: PgPool) -> Self {
        self.db = db;
        self
    }

    fn with_generation(mut self, generation: u64) -> Self {
        self.explicit_generation = Some(generation);
        self
    }

    fn with_output_epoch(mut self, output_epoch: u64) -> Self {
        self.explicit_output_epoch = Some(output_epoch);
        self
    }

    fn with_current(mut self, index: usize) -> Self {
        self.explicit_current = Some(index);
        self
    }

    fn with_staged_next(mut self, index: usize) -> Self {
        self.explicit_staged = Some(Some(index));
        self
    }

    fn with_no_staged_next(mut self) -> Self {
        self.explicit_staged = Some(None);
        self
    }

    /// Builds the initialized [`ControllerHarness`].
    async fn build(self) -> ControllerHarness {
        if let Some(current_idx) = self.explicit_current {
            assert!(
                current_idx < self.songs.len(),
                "explicit current index {} is out of bounds for queue of length {}",
                current_idx,
                self.songs.len()
            );
        }
        let initial_idx = self.explicit_current.unwrap_or(0);
        let mut harness = match self.preset {
            ScenarioPreset::Stopped => ControllerHarness::new_internal(self.db, self.pipeline, self.songs, initial_idx),
            ScenarioPreset::Idle => {
                let mut harness = ControllerHarness::new_internal(self.db, self.pipeline, self.songs, initial_idx);
                harness.controller.idle = true;
                harness
            }
            ScenarioPreset::Playing => {
                assert!(
                    !self.songs.is_empty(),
                    "ControllerScenario::playing() requires at least one song in the queue"
                );
                let mut harness = ControllerHarness::new_internal(self.db, self.pipeline, self.songs, initial_idx);
                let prepared = harness.controller.play().await.expect("play prepare");
                if let Some(attempt_id) = prepared.play_attempt_id {
                    harness.controller.commit_play(attempt_id, &Ok(()));
                }
                harness
            }
            ScenarioPreset::Paused => {
                assert!(
                    !self.songs.is_empty(),
                    "ControllerScenario::paused() requires at least one song in the queue"
                );
                let mut harness = ControllerHarness::new_internal(self.db, self.pipeline, self.songs, initial_idx);
                let prepared = harness.controller.play().await.expect("play prepare");
                if let Some(attempt_id) = prepared.play_attempt_id {
                    harness.controller.commit_play(attempt_id, &Ok(()));
                }
                harness.controller.pause();
                harness
            }
        };

        if let Some(gen) = self.explicit_generation {
            harness.controller.generation = gen;
        }
        if let Some(epoch) = self.explicit_output_epoch {
            harness.controller.output_epoch = epoch;
        }
        if let Some(staged_override) = self.explicit_staged {
            match staged_override {
                Some(staged_idx) => {
                    let song = harness.controller.queue.song_at(staged_idx).unwrap_or_else(|| {
                        panic!(
                            "explicit staged index {} is out of bounds for queue of length {}",
                            staged_idx,
                            harness.controller.queue.song_count()
                        )
                    });
                    let anchor = harness.controller.queue.anchor_after_current();
                    harness.controller.planned_next = Some((song, anchor));
                }
                None => {
                    harness.controller.planned_next = None;
                }
            }
        }

        harness
    }
}

/// Builds a real `StationController` around the shared recording
/// pipeline, hiding the recurring broadcast channels, queue manager,
/// station id, playback config, driver, target, and reconnect/resume
/// defaults. Tests reach the controller through `harness.controller`
/// and read pipeline effects through `harness.pipeline`.
#[allow(dead_code)]
struct ControllerHarness {
    controller: StationController,
    pipeline: Arc<RecordingPipeline>,
}

type Harness = ControllerHarness;

#[allow(dead_code)]
impl ControllerHarness {
    fn new_internal(db: PgPool, pipeline: Arc<RecordingPipeline>, songs: Vec<SongInfo>, initial_idx: usize) -> Self {
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let station_id = Uuid::new_v4();
        let queue = Arc::new(QueueManager::new(db.clone(), station_id, String::new(), songs, initial_idx));
        let controller = StationController {
            queue,
            db,
            station_id,
            playback: testsupport::playback_config(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: testsupport::target(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 0,
            output_epoch: 0,
            planned_next: None,
            idle: false,
            pending_resume: None,
            resume_attempt_seq: 0,
            pending_play: None,
            play_attempt_seq: 0,
            last_failed_play: None,
            resolved_play_success: None,
            pending_play_resolved_by_skip: None,
            pending_skip: None,
            skip_attempt_seq: 0,
            pending_realign: None,
            realign_seq: 0,
            deferred_terminal: None,
            active_reconnect_retry: None,
            reconnect_retry_seq: 0,
            active_reconnect_output: None,
            reconnect_token_shared: std::sync::Arc::default(),
            known_disconnected_output: None,
            decode_exclusions: None,
        };
        Self { controller, pipeline }
    }

    fn new(db: PgPool, pipeline: Arc<RecordingPipeline>, songs: Vec<SongInfo>) -> Self {
        Self::new_internal(db, pipeline, songs, 0)
    }

    /// A stopped controller with the given queue over the intentionally
    /// unavailable database.
    fn stopped(songs: Vec<SongInfo>) -> Self {
        Self::new(testsupport::unavailable_db(), Arc::new(RecordingPipeline::new()), songs)
    }

    /// A stopped controller over a queue built from track titles.
    fn stopped_queue(titles: &[&str]) -> Self {
        Self::stopped(testsupport::queued_songs(titles))
    }

    /// Like [`Harness::stopped`] but with a pre-configured pipeline
    /// (gates or failure injection) — the driver is spawned around it.
    fn with_pipeline(pipeline: Arc<RecordingPipeline>, songs: Vec<SongInfo>) -> Self {
        Self::new(testsupport::unavailable_db(), pipeline, songs)
    }

    /// A controller over a real test database (reconnect suite).
    fn with_db(db: PgPool, pipeline: Arc<RecordingPipeline>, songs: Vec<SongInfo>) -> Self {
        Self::new(db, pipeline, songs)
    }

    /// Consumes the harness and spawns the station runtime task, returning
    /// the runtime handle and the pipeline-event channel driving it.
    fn into_runtime(self) -> (StationRuntime, mpsc::UnboundedSender<PipelineEvent>) {
        let (events, receiver) = mpsc::unbounded_channel();
        (StationRuntime::spawn(self.controller, receiver), events)
    }

    /// A playing controller: canonical playing scenario initialization.
    async fn playing(songs: Vec<SongInfo>) -> Self {
        ControllerScenario::playing().with_songs(songs).build().await
    }

    /// A playing controller over a queue built from track titles.
    async fn playing_queue(titles: &[&str]) -> Self {
        ControllerScenario::playing().with_queue(titles).build().await
    }

    /// A paused controller over the given songs.
    async fn paused(songs: Vec<SongInfo>) -> Self {
        ControllerScenario::paused().with_songs(songs).build().await
    }

    /// An idle controller.
    async fn idle() -> Self {
        ControllerScenario::idle().build().await
    }

    /// Splits the harness back into its controller and pipeline for
    /// tests that drive the controller directly.
    fn into_parts(self) -> (StationController, Arc<RecordingPipeline>) {
        (self.controller, self.pipeline)
    }

    // --- Helpers for ergonomic access and execution ---

    fn controller(&self) -> &StationController {
        &self.controller
    }

    fn controller_mut(&mut self) -> &mut StationController {
        &mut self.controller
    }

    fn pipeline(&self) -> &Arc<RecordingPipeline> {
        &self.pipeline
    }

    fn song(&self, index: usize) -> SongInfo {
        self.controller
            .queue
            .song_at(index)
            .unwrap_or_else(|| panic!("queue index {index} out of bounds (len {})", self.controller.queue.song_count()))
    }

    fn track(&self, index: usize) -> PipelineTrack {
        StationController::track(self.song(index))
    }

    fn track_key(&self, index: usize) -> TrackKey {
        self.track(index).key
    }

    fn songs(&self) -> Vec<SongInfo> {
        self.controller.queue.songs()
    }
    // --- Controller operations ---

    async fn play(&mut self) -> Result<PreparedOperation, PipelineError> {
        self.controller.play().await
    }

    fn pause(&mut self) -> PipelineOperation {
        self.controller.pause()
    }

    async fn skip(&mut self) -> Result<PreparedOperation, PipelineError> {
        self.controller.skip().await
    }

    fn stop(&mut self) -> PipelineOperation {
        self.controller.stop()
    }
    async fn reload(&mut self, songs: Vec<SongInfo>, align: bool) -> Result<Option<PreparedOperation>, PipelineError> {
        self.controller.reload(songs, align).await
    }

    async fn reload_titles(&mut self, titles: &[&str], align: bool) -> Result<Option<PreparedOperation>, PipelineError> {
        self.controller.reload(testsupport::queued_songs(titles), align).await
    }

    fn make_reloaded_songs(&self, titles: &[&str]) -> Vec<SongInfo> {
        let existing = self.controller.queue.songs();
        let mut result = Vec::with_capacity(titles.len());
        for (next_pos, &title) in titles.iter().enumerate() {
            if let Some(s) = existing.iter().find(|s| s.title == title) {
                let mut song = s.clone();
                song.position = next_pos as i32;
                result.push(song);
            } else {
                result.push(testsupport::queued_song(title, next_pos as i32));
            }
        }
        result
    }

    async fn reload_reordered(&mut self, titles: &[&str], align: bool) -> Result<Option<PreparedOperation>, PipelineError> {
        let songs = self.make_reloaded_songs(titles);
        self.controller.reload(songs, align).await
    }
    async fn handle_event(&mut self, event: PipelineEvent) -> Option<Result<PreparedOperation, PipelineError>> {
        self.controller.handle_event(event).await
    }

    async fn inject_decode_failure(&mut self, generation: u64, track: &TrackKey, message: &str) -> Option<PreparedOperation> {
        inject_decode_failure(&mut self.controller, generation, track, message).await
    }

    async fn staged_decode_failure(&mut self, track: TrackKey, message: &str) -> Option<PreparedOperation> {
        staged_decode_failure(&mut self.controller, track, message).await
    }

    fn commit_play(&mut self, id: u64, res: &Result<(), PipelineError>) -> bool {
        self.controller.commit_play(id, res)
    }

    async fn commit_skip(&mut self, id: u64, res: &Result<(), PipelineError>) -> (bool, SkipFollowup) {
        self.controller.commit_skip(id, res).await
    }

    fn commit_realign(&mut self, id: u64, res: &Result<(), PipelineError>) -> Option<(u64, PreparedOperation)> {
        self.controller.commit_realign(id, res)
    }

    // --- Reconnect helpers ---

    fn begin_reconnect_chain(&mut self) -> u64 {
        self.controller.begin_reconnect_chain()
    }

    fn end_reconnect_chain(&mut self, token: u64) {
        self.controller.end_reconnect_chain(token)
    }

    fn bind_reconnect_to_output(&mut self, generation: u64, output_epoch: u64) {
        self.controller.bind_reconnect_to_output(generation, output_epoch)
    }

    fn reconnect_retry_is_current(&self, token: u64) -> bool {
        self.controller.reconnect_retry_is_current(token)
    }

    fn current_reconnect_token(&self) -> u64 {
        self.controller.current_reconnect_token()
    }

    fn is_output_known_disconnected(&self) -> bool {
        self.controller.is_output_known_disconnected()
    }

    fn on_reconnect_succeeded(&mut self, token: u64) {
        self.controller.on_reconnect_succeeded(token)
    }

    async fn reconnect_if_current(
        &mut self,
        generation: u64,
        output_epoch: u64,
        token: u64,
    ) -> Result<Option<PipelineOperation>, PipelineError> {
        self.controller.reconnect_if_current(generation, output_epoch, token).await
    }

    async fn reconnect(&mut self) -> Result<PipelineOperation, PipelineError> {
        self.controller.reconnect().await
    }

    async fn resume_reconnect_for_break(&mut self) -> Option<Result<PipelineOperation, PipelineError>> {
        self.controller.resume_reconnect_for_break().await
    }

    async fn disconnect(&mut self, generation: u64, output_epoch: u64, message: &str) -> Option<Result<PreparedOperation, PipelineError>> {
        self.handle_event(PipelineEvent::SinkDisconnected {
            generation,
            output_epoch,
            message: message.into(),
        })
        .await
    }

    async fn disconnect_current(&mut self, message: &str) -> Option<Result<PreparedOperation, PipelineError>> {
        let gen = self.controller.generation();
        let epoch = self.controller.output_epoch();
        self.disconnect(gen, epoch, message).await
    }

    // --- State assertions ---

    fn assert_state(&self, expected: PipelineState) {
        assert_eq!(self.controller.state, expected, "state mismatch");
    }

    fn assert_generation(&self, expected: u64) {
        assert_eq!(self.controller.generation, expected, "generation mismatch");
    }

    fn assert_output_epoch(&self, expected: u64) {
        assert_eq!(self.controller.output_epoch, expected, "output_epoch mismatch");
    }

    fn assert_retry_is_current(&self, token: u64) {
        assert!(
            self.reconnect_retry_is_current(token),
            "reconnect retry token {token} must be current"
        );
    }

    fn assert_retry_not_current(&self, token: u64) {
        assert!(
            !self.reconnect_retry_is_current(token),
            "reconnect retry token {token} must not be current"
        );
    }

    fn assert_shared_reconnect_token(&self, expected: u64) {
        assert_eq!(
            self.controller.reconnect_token_shared().token(),
            expected,
            "shared reconnect token mismatch"
        );
    }

    fn assert_output_known_disconnected(&self, expected: bool) {
        assert_eq!(
            self.is_output_known_disconnected(),
            expected,
            "output known disconnected marker mismatch"
        );
    }

    async fn assert_reconnect_if_current_dropped(&mut self, token: u64) {
        let result = self.reconnect_if_current(1, 1, token).await;
        assert!(
            matches!(result, Ok(None)),
            "reconnect_if_current for token {token} must be dropped, got {result:?}"
        );
    }

    fn assert_idle(&self, expected: bool) {
        assert_eq!(self.controller.idle, expected, "idle mismatch");
    }

    fn assert_current_song(&self, expected_title: &str) {
        let actual = self.controller.queue.current_song_info().map(|s| s.title);
        assert_eq!(actual.as_deref(), Some(expected_title), "current song title mismatch");
    }

    fn assert_staged_next(&self, expected_title: &str) {
        let actual = self.controller.planned_next.as_ref().map(|(s, _)| s.title.as_str());
        assert_eq!(actual, Some(expected_title), "staged next song title mismatch");
    }

    fn assert_no_staged_next(&self) {
        assert!(
            self.controller.planned_next.is_none(),
            "expected no staged next, got {:?}",
            self.controller.planned_next
        );
    }

    fn assert_pending_skip(&self, expected: bool) {
        assert_eq!(self.controller.pending_skip.is_some(), expected, "pending_skip presence mismatch");
    }

    fn assert_pending_play(&self, expected: bool) {
        assert_eq!(self.controller.pending_play.is_some(), expected, "pending_play presence mismatch");
    }
    fn assert_pending_skip_id(&self, expected: Option<u64>) {
        assert_eq!(self.controller.pending_skip(), expected, "pending_skip mismatch");
    }

    fn assert_pending_play_id(&self, expected: Option<u64>) {
        assert_eq!(self.controller.pending_play(), expected, "pending_play mismatch");
    }

    fn assert_pending_realign(&self, expected: bool) {
        assert_eq!(
            self.controller.pending_realign.is_some(),
            expected,
            "pending_realign presence mismatch"
        );
    }

    fn assert_pipeline(&self, expected: &testsupport::ExpectedPipelineOperations) {
        self.pipeline.assert_matches(expected);
    }

    fn assert_queue_titles(&self, expected: &[&str]) {
        let actual: Vec<String> = self.controller.queue.songs().into_iter().map(|s| s.title).collect();
        assert_eq!(actual, expected, "queue titles mismatch");
    }

    fn assert_planned_next_key(&self, expected: Option<&TrackKey>) {
        assert_eq!(self.controller.planned_next().as_ref(), expected, "planned_next key mismatch");
    }

    fn assert_pending_realign_id(&self, expected: Option<u64>) {
        assert_eq!(self.controller.pending_realign(), expected, "pending_realign mismatch");
    }

    fn assert_current_song_key(&self, expected: &TrackKey) {
        let actual = self.controller.queue.current_song_info().as_ref().map(StationController::key_of);
        assert_eq!(actual.as_ref(), Some(expected), "current song key mismatch");
    }

    fn assert_rolling_replace_next(&self, prepared: &PreparedOperation, expected: &TrackKey, replacement: Option<&TrackKey>) -> u64 {
        let PipelineOperation::Roll(plan) = &prepared.operation else {
            panic!("expected Roll operation, got {:?}", prepared.operation);
        };
        let realign_id = prepared.realign_id.expect("the roll must be correlated");
        match &plan.change {
            RollingChange::ReplaceNext {
                expected_next: actual_expected,
                replacement: actual_replacement,
            } => {
                assert_eq!(actual_expected, expected, "expected_next mismatch");
                assert_eq!(
                    actual_replacement.as_ref().map(|p| &p.track.key),
                    replacement,
                    "replacement mismatch"
                );
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        realign_id
    }
    fn assert_rolling_attach(&self, prepared: &PreparedOperation, target: &TrackKey) -> u64 {
        let PipelineOperation::Roll(plan) = &prepared.operation else {
            panic!("expected Roll operation, got {:?}", prepared.operation);
        };
        let realign_id = prepared.realign_id.expect("the roll must be correlated");
        match &plan.change {
            RollingChange::Attach(next) => {
                assert_eq!(&next.track.key, target, "attach target mismatch");
            }
            other => panic!("expected Attach, got {other:?}"),
        }
        realign_id
    }
    async fn prepare_skip_attempt(&mut self) -> u64 {
        let prepared = self.skip().await.expect("a skip with a successor must prepare a replacement");
        assert!(
            matches!(prepared.operation, PipelineOperation::Replace(_)),
            "a skip with a successor must issue a replace"
        );
        prepared.attempt_id.expect("a skip operation must carry an attempt id")
    }

    fn snapshot(&self) -> ControllerSnapshot {
        ControllerSnapshot {
            state: self.controller.state,
            generation: self.controller.generation,
            output_epoch: self.controller.output_epoch,
            current_key: self.controller.queue.current_song_info().as_ref().map(StationController::key_of),
            current_title: self.controller.queue.current_song_info().map(|s| s.title),
            staged_title: self.controller.planned_next.as_ref().map(|(s, _)| s.title.clone()),
            planned_next_key: self.controller.planned_next(),
            pending_play: self.controller.pending_play(),
            pending_skip: self.controller.pending_skip(),
            pending_realign: self.controller.pending_realign(),
            is_output_known_disconnected: self.controller.is_output_known_disconnected(),
            idle: self.controller.idle,
        }
    }

    fn assert_snapshot_unchanged(&self, before: &ControllerSnapshot, context: &str) {
        let after = self.snapshot();
        assert_eq!(after, *before, "{context}: controller state must be unchanged by stale input");
    }

    async fn assert_stale_event_is_inert(&mut self, event: PipelineEvent, context: &'static str) {
        let before = self.snapshot();
        let op = self.handle_event(event).await;
        assert!(op.is_none(), "{context}: stale event must be ignored");
        self.assert_snapshot_unchanged(&before, context);
    }
    fn assert_matches(&self, expected: &ExpectedState<'_>) {
        if let Some(state) = expected.state {
            self.assert_state(state);
        }
        if let Some(generation) = expected.generation {
            self.assert_generation(generation);
        }
        if let Some(output_epoch) = expected.output_epoch {
            self.assert_output_epoch(output_epoch);
        }
        if let Some(idle) = expected.idle {
            self.assert_idle(idle);
        }
        if let Some(current) = expected.current {
            self.assert_current_song(current);
        }
        if let Some(staged) = &expected.staged {
            match staged {
                Some(title) => self.assert_staged_next(title),
                None => self.assert_no_staged_next(),
            }
        }
        if let Some(pending_skip) = expected.pending_skip {
            self.assert_pending_skip(pending_skip);
        }
        if let Some(pending_play) = expected.pending_play {
            self.assert_pending_play(pending_play);
        }
        if let Some(pending_realign) = expected.pending_realign {
            self.assert_pending_realign(pending_realign);
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct ControllerSnapshot {
    state: PipelineState,
    generation: u64,
    output_epoch: u64,
    current_key: Option<TrackKey>,
    current_title: Option<String>,
    staged_title: Option<String>,
    planned_next_key: Option<TrackKey>,
    pending_play: Option<u64>,
    pending_skip: Option<u64>,
    pending_realign: Option<u64>,
    is_output_known_disconnected: bool,
    idle: bool,
}

/// Declarative expectations for [`StationController`] state.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
struct ExpectedState<'a> {
    state: Option<PipelineState>,
    generation: Option<u64>,
    output_epoch: Option<u64>,
    idle: Option<bool>,
    current: Option<&'a str>,
    staged: Option<Option<&'a str>>,
    pending_skip: Option<bool>,
    pending_play: Option<bool>,
    pending_realign: Option<bool>,
}

#[allow(dead_code)]
impl<'a> ExpectedState<'a> {
    fn new() -> Self {
        Self::default()
    }

    fn playing() -> Self {
        Self {
            state: Some(PipelineState::Playing),
            ..Self::default()
        }
    }

    fn paused() -> Self {
        Self {
            state: Some(PipelineState::Paused),
            ..Self::default()
        }
    }

    fn stopped() -> Self {
        Self {
            state: Some(PipelineState::Stopped),
            ..Self::default()
        }
    }

    fn idle() -> Self {
        Self {
            state: Some(PipelineState::Stopped),
            idle: Some(true),
            ..Self::default()
        }
    }

    fn state(mut self, state: PipelineState) -> Self {
        self.state = Some(state);
        self
    }

    fn generation(mut self, generation: u64) -> Self {
        self.generation = Some(generation);
        self
    }

    fn output_epoch(mut self, output_epoch: u64) -> Self {
        self.output_epoch = Some(output_epoch);
        self
    }

    fn current(mut self, title: &'a str) -> Self {
        self.current = Some(title);
        self
    }

    fn staged(mut self, title: &'a str) -> Self {
        self.staged = Some(Some(title));
        self
    }

    fn no_staged(mut self) -> Self {
        self.staged = Some(None);
        self
    }

    fn pending_skip(mut self, pending: bool) -> Self {
        self.pending_skip = Some(pending);
        self
    }

    fn pending_play(mut self, pending: bool) -> Self {
        self.pending_play = Some(pending);
        self
    }

    fn pending_realign(mut self, pending: bool) -> Self {
        self.pending_realign = Some(pending);
        self
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum ExpectedReloadOp<'a> {
    None,
    ReplaceNext { expected: &'a str, replacement: Option<&'a str> },
    Attach { target: &'a str },
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct ReloadCase<'a> {
    name: &'static str,
    initial_queue: &'a [&'a str],
    new_queue: &'a [&'a str],
    align: bool,
    expected_op: ExpectedReloadOp<'a>,
    expected_staged_before_commit: Option<&'a str>,
    commit: Option<Result<(), PipelineError>>,
    expected_final_staged: Option<&'a str>,
}

async fn run_reload_case(case: ReloadCase<'_>) {
    let mut harness = ControllerScenario::playing().with_queue(case.initial_queue).build().await;
    let initial_current_key = harness.track_key(0);
    let initial_staged_key = harness.controller.planned_next().clone();

    let reloaded_songs = harness.make_reloaded_songs(case.new_queue);
    let prepared = harness
        .controller
        .reload(reloaded_songs.clone(), case.align)
        .await
        .expect("reload must not error");

    match case.expected_op {
        ExpectedReloadOp::None => {
            assert!(
                prepared.is_none(),
                "{}: expected no operation from reload, got {prepared:?}",
                case.name
            );
        }
        ExpectedReloadOp::ReplaceNext { expected, replacement } => {
            let prepared = prepared.unwrap_or_else(|| panic!("{}: expected ReplaceNext, got None", case.name));
            let PipelineOperation::Roll(plan) = &prepared.operation else {
                panic!("{}: expected Roll operation, got {:?}", case.name, prepared.operation);
            };
            assert_eq!(plan.current, initial_current_key, "{}: plan.current mismatch", case.name);
            assert_eq!(plan.generation, 1, "{}: plan.generation mismatch", case.name);

            let expected_key = initial_staged_key
                .as_ref()
                .unwrap_or_else(|| panic!("{}: expected initial staged next for ReplaceNext", case.name));
            assert_eq!(
                case.initial_queue.get(1).copied(),
                Some(expected),
                "{}: expected title mismatch",
                case.name
            );

            let replacement_key = replacement.map(|rep_title| {
                let song = reloaded_songs
                    .iter()
                    .find(|s| s.title == rep_title)
                    .unwrap_or_else(|| panic!("{}: replacement title {rep_title} not in reloaded songs", case.name));
                StationController::key_of(song)
            });

            let realign_id = harness.assert_rolling_replace_next(&prepared, expected_key, replacement_key.as_ref());

            if let Some(staged_before) = case.expected_staged_before_commit {
                harness.assert_staged_next(staged_before);
            }

            if let Some(res) = case.commit {
                let followup = harness.commit_realign(realign_id, &res);
                assert!(
                    followup.is_none(),
                    "{}: expected no followup after commit, got {followup:?}",
                    case.name
                );
            }
        }
        ExpectedReloadOp::Attach { target } => {
            let prepared = prepared.unwrap_or_else(|| panic!("{}: expected Attach, got None", case.name));
            let PipelineOperation::Roll(plan) = &prepared.operation else {
                panic!("{}: expected Roll operation, got {:?}", case.name, prepared.operation);
            };
            assert_eq!(plan.current, initial_current_key, "{}: plan.current mismatch", case.name);
            assert_eq!(plan.generation, 1, "{}: plan.generation mismatch", case.name);

            let target_song = reloaded_songs
                .iter()
                .find(|s| s.title == target)
                .unwrap_or_else(|| panic!("{}: attach target {target} not in reloaded songs", case.name));
            let target_key = StationController::key_of(target_song);

            let realign_id = harness.assert_rolling_attach(&prepared, &target_key);

            if let Some(staged_before) = case.expected_staged_before_commit {
                harness.assert_staged_next(staged_before);
            }

            if let Some(res) = case.commit {
                let followup = harness.commit_realign(realign_id, &res);
                assert!(
                    followup.is_none(),
                    "{}: expected no followup after commit, got {followup:?}",
                    case.name
                );
            }
        }
    }
    if let Some(staged) = case.expected_final_staged {
        harness.assert_staged_next(staged);
    } else {
        harness.assert_no_staged_next();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SkipInterruption {
    None,
    Pause,
    Stop,
}

#[derive(Clone, Debug)]
struct PendingSkipFailureCase<'a> {
    name: &'static str,
    initial_queue: &'a [&'a str],
    failed_current: bool,
    failed_staged_next: bool,
    interruption: SkipInterruption,
    expected_state_after_skip: PipelineState,
    expected_generation_after_skip: u64,
    expected_has_deferred_terminal: bool,
    expected_has_decode_exclusions: bool,
    expected_realign_roll: Option<(&'a str, &'a str)>,
    expected_immediate_recovery_target: Option<&'a str>,
    resume_recovery_target: Option<&'a str>,
    expected_final_cursor: &'a str,
    expected_final_state: PipelineState,
    expected_final_generation: u64,
    subsequent_play_after_stop: bool,
}

async fn run_pending_skip_failure_case(case: PendingSkipFailureCase<'_>) {
    let mut harness = ControllerHarness::playing_queue(case.initial_queue).await;
    assert_eq!(harness.controller.generation, 1, "{}: initial generation", case.name);
    let b_key = harness.track_key(1);
    let c_key = if case.initial_queue.len() > 2 {
        Some(harness.track_key(2))
    } else {
        None
    };
    let d_key = if case.initial_queue.len() > 3 {
        Some(harness.track_key(3))
    } else {
        None
    };

    let prepared = harness
        .skip()
        .await
        .unwrap_or_else(|e| panic!("{}: skip prepare failed: {e:?}", case.name));
    let skip_id = prepared
        .attempt_id
        .unwrap_or_else(|| panic!("{}: skip attempt id missing", case.name));
    let PipelineOperation::Replace(ref plan) = prepared.operation else {
        panic!("{}: expected Replace operation, got {:?}", case.name, prepared.operation);
    };
    assert_eq!(plan.generation, 2, "{}: prepared skip generation", case.name);
    assert_eq!(plan.current.key, b_key, "{}: prepared skip target", case.name);

    if case.failed_current {
        assert!(
            harness.inject_decode_failure(2, &b_key, "B failed").await.is_none(),
            "{}: B failure injection",
            case.name
        );
    }
    if case.failed_staged_next {
        let key = c_key
            .as_ref()
            .unwrap_or_else(|| panic!("{}: staged next C key needed for failure", case.name));
        assert!(
            harness.inject_decode_failure(2, key, "C failed").await.is_none(),
            "{}: C failure injection",
            case.name
        );
    }

    let expected_failures = match (case.failed_current, case.failed_staged_next) {
        (true, true) => Some((Some(b_key.clone()), c_key.clone())),
        (true, false) => Some((Some(b_key.clone()), None)),
        (false, true) => Some((None, c_key.clone())),
        (false, false) => None,
    };
    assert_eq!(
        harness.controller.pending_skip_failures(),
        expected_failures,
        "{}: pending_skip_failures mismatch",
        case.name
    );

    match case.interruption {
        SkipInterruption::None => {}
        SkipInterruption::Pause => {
            let op = harness.pause();
            assert!(matches!(op, PipelineOperation::SetPlaying(false)), "{}: pause op", case.name);
            harness.assert_state(PipelineState::Paused);
        }
        SkipInterruption::Stop => {
            let op = harness.stop();
            assert!(matches!(op, PipelineOperation::Stop), "{}: stop op", case.name);
            harness.assert_state(PipelineState::Stopped);
        }
    }

    let (applied, followup) = harness.commit_skip(skip_id, &Ok(())).await;
    assert!(applied, "{}: commit_skip must be applied", case.name);
    harness.assert_state(case.expected_state_after_skip);
    harness.assert_generation(case.expected_generation_after_skip);
    assert_eq!(
        harness.controller.has_deferred_terminal(),
        case.expected_has_deferred_terminal,
        "{}: has_deferred_terminal",
        case.name
    );
    assert_eq!(
        harness.controller.has_decode_exclusions(),
        case.expected_has_decode_exclusions,
        "{}: has_decode_exclusions",
        case.name
    );

    if let Some((exp_expected, exp_repl)) = case.expected_realign_roll {
        let (realign_id, roll) = expect_realign_followup(followup);
        assert_eq!(
            harness.controller.pending_realign(),
            Some(realign_id),
            "{}: pending_realign",
            case.name
        );
        match roll.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                let exp_expected_key = if exp_expected == "C" {
                    c_key.as_ref().unwrap()
                } else {
                    panic!("{}: unknown expected_next", case.name)
                };
                let exp_repl_key = if exp_repl == "D" {
                    d_key.as_ref().unwrap()
                } else {
                    panic!("{}: unknown replacement", case.name)
                };
                assert_eq!(&expected_next, exp_expected_key, "{}: realign roll expected_next", case.name);
                assert_eq!(
                    replacement.as_ref().map(|r| &r.track.key),
                    Some(exp_repl_key),
                    "{}: realign roll replacement",
                    case.name
                );
            }
            other => panic!("{}: expected ReplaceNext realign, got {other:?}", case.name),
        }
        assert!(
            harness.commit_realign(realign_id, &Ok(())).is_none(),
            "{}: commit_realign",
            case.name
        );
        harness.assert_state(case.expected_state_after_skip);
        harness.assert_staged_next(exp_repl);
        assert_eq!(harness.controller.pending_realign(), None, "{}: pending_realign cleared", case.name);
        if case.expected_has_deferred_terminal {
            assert!(
                harness.controller.has_deferred_terminal(),
                "{}: deferred terminal must survive staged next realign completion",
                case.name
            );
        }
    } else if let Some(immediate_recovery) = case.expected_immediate_recovery_target {
        assert_eq!(
            harness.controller.pending_realign(),
            None,
            "{}: no orphan pending_realign",
            case.name
        );
        let (recovery_id, recovery_plan) = match followup {
            SkipFollowup::Operation(recovery) => {
                let attempt_id = recovery
                    .attempt_id
                    .unwrap_or_else(|| panic!("{}: recovery skip attempt_id", case.name));
                assert_eq!(
                    harness.controller.pending_skip(),
                    Some(attempt_id),
                    "{}: pending_skip bound to recovery",
                    case.name
                );
                let PipelineOperation::Replace(plan) = recovery.operation else {
                    panic!(
                        "{}: expected Replace operation on recovery, got {:?}",
                        case.name, recovery.operation
                    );
                };
                (attempt_id, plan)
            }
            other => panic!("{}: expected SkipFollowup::Operation, got {other:?}", case.name),
        };
        let target_key = if immediate_recovery == "D" {
            d_key.as_ref().unwrap()
        } else {
            panic!("{}: unknown immediate target", case.name)
        };
        assert_eq!(&recovery_plan.current.key, target_key, "{}: recovery target", case.name);
        assert_eq!(recovery_plan.generation, 3, "{}: recovery generation", case.name);

        let (rec_applied, rec_followup) = harness.commit_skip(recovery_id, &Ok(())).await;
        assert!(rec_applied, "{}: recovery skip commit", case.name);
        assert!(matches!(rec_followup, SkipFollowup::None), "{}: recovery followup None", case.name);
    } else {
        assert!(
            matches!(followup, SkipFollowup::None),
            "{}: followup must be None, got {followup:?}",
            case.name
        );
        assert_eq!(harness.controller.pending_realign(), None, "{}: pending_realign None", case.name);
    }

    if let Some(target_title) = case.resume_recovery_target {
        let play_prepared = harness
            .play()
            .await
            .unwrap_or_else(|e| panic!("{}: play resume prepare failed: {e:?}", case.name));
        harness.assert_state(PipelineState::Paused);
        let rec_id = play_prepared
            .attempt_id
            .unwrap_or_else(|| panic!("{}: resume recovery attempt_id", case.name));
        assert_eq!(
            harness.controller.pending_skip(),
            Some(rec_id),
            "{}: pending_skip on resume recovery",
            case.name
        );
        let PipelineOperation::Replace(plan) = play_prepared.operation else {
            panic!(
                "{}: expected Replace operation on resume recovery, got {:?}",
                case.name, play_prepared.operation
            );
        };
        let target_key = if target_title == "C" {
            c_key.as_ref().unwrap()
        } else if target_title == "D" {
            d_key.as_ref().unwrap()
        } else {
            panic!("{}: unknown resume target", case.name)
        };
        assert_eq!(&plan.current.key, target_key, "{}: resume recovery target", case.name);
        assert_eq!(plan.generation, 3, "{}: resume recovery generation", case.name);

        let (rec_applied, rec_followup) = harness.commit_skip(rec_id, &Ok(())).await;
        assert!(rec_applied, "{}: resume recovery commit", case.name);
        assert!(
            matches!(rec_followup, SkipFollowup::None),
            "{}: resume recovery followup None",
            case.name
        );
        assert!(
            !harness.controller.has_deferred_terminal(),
            "{}: deferred terminal cleared after recovery",
            case.name
        );
    }

    if case.subsequent_play_after_stop {
        assert_eq!(
            harness.controller.state,
            PipelineState::Stopped,
            "{}: stop cleanup state",
            case.name
        );
        harness.assert_current_song_key(&b_key);
        harness.assert_generation(2);
        assert_eq!(harness.controller.pending_skip(), None, "{}: pending_skip None", case.name);
        assert_eq!(harness.controller.pending_realign(), None, "{}: pending_realign None", case.name);
        assert_eq!(
            harness.controller.pending_skip_failures(),
            None,
            "{}: pending_skip_failures None",
            case.name
        );
        assert_eq!(harness.controller.planned_next(), None, "{}: planned_next None", case.name);
        assert!(
            !harness.controller.has_deferred_terminal(),
            "{}: deferred terminal reset on stop",
            case.name
        );
        assert!(
            !harness.controller.has_decode_exclusions(),
            "{}: decode exclusions reset on stop",
            case.name
        );

        let play_prepared = harness.play().await.expect("play must prepare cleanly after stop");
        let play_id = play_prepared.play_attempt_id.expect("play attempt id");
        harness.assert_staged_next("C");
        assert!(harness.commit_play(play_id, &Ok(())));
        harness.assert_state(PipelineState::Playing);
        assert_eq!(harness.controller.pending_realign(), None);
        harness.assert_staged_next("C");
    }
    harness.assert_current_song(case.expected_final_cursor);
    harness.assert_state(case.expected_final_state);
    harness.assert_generation(case.expected_final_generation);
    assert_eq!(harness.controller.pending_skip(), None, "{}: pending_skip None at end", case.name);
    assert_eq!(
        harness.controller.pending_realign(),
        None,
        "{}: pending_realign None at end",
        case.name
    );
}
#[tokio::test]
async fn controller_harness_track_and_song_access_reflects_reloaded_queue() {
    let mut harness = ControllerScenario::playing().with_queue(&["A", "B", "C"]).build().await;

    assert_eq!(harness.song(0).title, "A");
    assert_eq!(harness.song(1).title, "B");
    assert_eq!(harness.song(2).title, "C");
    harness.assert_current_song("A");
    harness.assert_staged_next("B");

    // Reload the queue with a completely new set of titles
    harness.reload_titles(&["X", "Y", "Z"], true).await.unwrap();

    // The harness helper queries the live queue and must reflect the new tracks
    assert_eq!(harness.song(0).title, "X");
    assert_eq!(harness.song(1).title, "Y");
    assert_eq!(harness.song(2).title, "Z");
    assert_eq!(harness.track(0).metadata.title, "X");
    assert_eq!(harness.track_key(0).song_id, harness.song(0).song_id);
}

#[tokio::test]
async fn controller_scenario_presets_build_expected_states() {
    // Stopped scenario with queue
    let stopped = ControllerScenario::stopped().with_queue(&["A", "B", "C"]).build().await;
    stopped.assert_matches(&ExpectedState::stopped().no_staged());
    assert_eq!(stopped.song(0).title, "A");

    // Playing scenario with queue
    let playing = ControllerScenario::playing().with_queue(&["A", "B", "C"]).build().await;
    playing.assert_matches(&ExpectedState::playing().current("A").staged("B"));

    // Paused scenario with queue
    let paused = ControllerScenario::paused().with_queue(&["A", "B", "C"]).build().await;
    paused.assert_matches(&ExpectedState::paused().current("A").staged("B"));

    // Idle scenario
    let idle = ControllerScenario::idle().build().await;
    idle.assert_matches(&ExpectedState::idle().no_staged());
}

#[tokio::test]
#[should_panic(expected = "ControllerScenario::playing() requires at least one song in the queue")]
async fn controller_scenario_playing_on_empty_queue_panics() {
    let _ = ControllerScenario::playing().with_queue(&[]).build().await;
}

#[tokio::test]
#[should_panic(expected = "ControllerScenario::paused() requires at least one song in the queue")]
async fn controller_scenario_paused_on_empty_queue_panics() {
    let _ = ControllerScenario::paused().with_queue(&[]).build().await;
}

#[tokio::test]
#[should_panic(expected = "explicit staged index 99 is out of bounds for queue of length 3")]
async fn controller_scenario_out_of_bounds_staged_index_panics() {
    let _ = ControllerScenario::playing()
        .with_queue(&["A", "B", "C"])
        .with_staged_next(99)
        .build()
        .await;
}

#[tokio::test]
#[should_panic(expected = "explicit current index 5 is out of bounds for queue of length 3")]
async fn controller_scenario_out_of_bounds_current_index_panics() {
    let _ = ControllerScenario::playing()
        .with_queue(&["A", "B", "C"])
        .with_current(5)
        .build()
        .await;
}

#[tokio::test]
async fn controller_scenario_setter_order_independence() {
    // Order 1: with_current before with_queue
    let h1 = ControllerScenario::playing()
        .with_current(1)
        .with_queue(&["A", "B", "C"])
        .with_staged_next(2)
        .with_generation(5)
        .with_output_epoch(7)
        .build()
        .await;

    // Order 2: with_queue before with_current, with_output_epoch before with_generation
    let h2 = ControllerScenario::playing()
        .with_output_epoch(7)
        .with_queue(&["A", "B", "C"])
        .with_generation(5)
        .with_staged_next(2)
        .with_current(1)
        .build()
        .await;

    assert_eq!(h1.controller.state, h2.controller.state);
    assert_eq!(h1.controller.generation, h2.controller.generation);
    assert_eq!(h1.controller.output_epoch, h2.controller.output_epoch);
    assert_eq!(h1.song(h1.controller.queue.current_song_index()).title, "B");
    assert_eq!(h2.song(h2.controller.queue.current_song_index()).title, "B");
    assert_eq!(h1.controller.planned_next.as_ref().unwrap().0.title, "C");
    assert_eq!(h2.controller.planned_next.as_ref().unwrap().0.title, "C");
}

#[tokio::test]
async fn stale_events_do_not_replace_or_reconnect() {
    let pipeline = Arc::new(RecordingPipeline::new());
    let mut harness = ControllerScenario::stopped()
        .with_pipeline(pipeline.clone())
        .with_queue(&["current"])
        .with_generation(1)
        .build()
        .await;
    let current_key = harness.track_key(0);

    harness
        .assert_stale_event_is_inert(
            PipelineEvent::DecodeFailed {
                generation: 0,
                track: current_key,
                message: "stale".into(),
            },
            "stale generation decode failure",
        )
        .await;
    assert_eq!(pipeline.count(Call::Replace), 0);

    harness.controller.state = PipelineState::Playing;
    harness.controller.output_epoch = 3;
    assert!(harness.controller.output_is_current(1, 3));
    assert!(!harness.controller.output_is_current(0, 3));
    harness.controller.state = PipelineState::Paused;
    assert!(!harness.controller.output_is_current(1, 3));
    harness.controller.stop();
    assert!(!harness.controller.output_is_current(1, 3));
}

#[tokio::test]
async fn play_with_an_empty_queue_keeps_the_controller_stopped() {
    let mut harness = ControllerScenario::stopped().build().await;
    let operation = harness.play().await.unwrap();

    assert!(matches!(operation.operation, PipelineOperation::Stop));
    harness.assert_matches(&ExpectedState::stopped());
    harness.assert_pipeline(&ExpectedPipelineOperations::none());
}

#[tokio::test]
async fn next_decode_failure_replaces_only_the_failed_terminal_branch() {
    let mut harness = ControllerScenario::playing()
        .with_queue(&["current", "failed", "successor"])
        .build()
        .await;

    let failed_key = harness.track_key(1);
    let successor_key = harness.track_key(2);

    let operation = harness
        .handle_event(PipelineEvent::DecodeFailed {
            generation: 1,
            track: failed_key.clone(),
            message: "broken next".into(),
        })
        .await
        .unwrap()
        .unwrap();
    let PipelineOperation::Roll(plan) = operation.operation else {
        panic!("next failure must issue a rolling replacement");
    };
    let realign_id = operation.realign_id.expect("the replacement roll must be correlated");
    assert!(operation.attempt_id.is_none(), "a decode replacement is never a skip operation");
    assert_eq!(plan.current.queue_item_id, harness.song(0).queue_item_id);
    let RollingChange::ReplaceNext {
        expected_next,
        replacement: Some(replacement),
    } = plan.change
    else {
        panic!("next failure must replace its terminal branch");
    };
    assert_eq!(expected_next, failed_key);
    assert_eq!(replacement.track.key, successor_key);
    harness.assert_staged_next("failed");

    // The replacement roll succeeds: planned_next advances to the
    // successor; a failed roll would keep the failed branch.
    assert!(harness.commit_realign(realign_id, &Ok(())).is_none());
    harness.assert_staged_next("successor");
}

#[tokio::test]
async fn current_media_branch_fatal_error_skips_to_next_song() {
    let mut harness = ControllerHarness::playing_queue(&["current", "next"]).await;
    let current_key = harness.track_key(0);

    // Current media branch emits DecodeFailed / fatal error
    let operation = harness
        .handle_event(PipelineEvent::DecodeFailed {
            generation: 1,
            track: current_key,
            message: "media read error".into(),
        })
        .await
        .expect("terminal event must produce an operation")
        .expect("skip operation should succeed");

    let PipelineOperation::Replace(plan) = operation.operation else {
        panic!("current track fatal error must issue a replace (skip) operation");
    };
    let attempt_id = operation.attempt_id.expect("skip operation must carry attempt id");
    assert_eq!(plan.current.key.queue_item_id, harness.song(1).queue_item_id);

    // Commit the skip
    let (applied, _) = harness.commit_skip(attempt_id, &Ok(())).await;
    assert!(applied, "skip must be committed");
    harness.assert_current_song("next");
}

#[tokio::test]
async fn fatal_pipeline_event_prepares_stop() {
    let mut harness = ControllerHarness::playing_queue(&["current"]).await;
    harness.assert_state(PipelineState::Playing);

    // Backbone error arrives
    let operation = harness
        .handle_event(PipelineEvent::FatalPipeline {
            pipeline_epoch: harness.controller().output_epoch,
            message: "encoder crashed".into(),
        })
        .await
        .expect("fatal pipeline event must produce an operation")
        .expect("stop operation should succeed");
    assert!(matches!(operation.operation, PipelineOperation::Stop));
    harness.assert_matches(&ExpectedState::stopped());
}

#[tokio::test]
async fn backbone_fatal_error_at_runtime_stops_station_and_pipeline() {
    let pipeline = Arc::new(RecordingPipeline::new());
    let harness = ControllerScenario::stopped()
        .with_queue(&["current"])
        .with_pipeline(pipeline.clone())
        .build()
        .await;
    let (runtime, events) = harness.into_runtime();
    runtime.play().await.unwrap();
    pipeline.assert_count(Call::Replace, 1);

    events
        .send(PipelineEvent::FatalPipeline {
            pipeline_epoch: 1,
            message: "encoder crashed".into(),
        })
        .unwrap();

    testsupport::wait_for("the station to stop after fatal pipeline error", || pipeline.count(Call::Stop) > 0).await;

    assert!(matches!(
        runtime.status().await.unwrap(),
        crate::streamer::StatusEvent::State { playing: false, .. }
    ));
}
#[tokio::test]
async fn fatal_pipeline_during_pending_skip_stops_station_before_commit() {
    let song_a = queued_song("A", 0);
    let song_b = queued_song("B", 1);
    let harness = Harness::playing(vec![song_a.clone(), song_b.clone()]).await;
    let (mut controller, _) = harness.into_parts();
    assert_eq!(controller.state, PipelineState::Playing);
    assert_eq!(controller.generation, 1);
    assert_eq!(controller.output_epoch, 1);

    // Prepare skip to B (generation 2 in flight)
    let _skip_op = controller.skip().await.expect("skip must prepare");
    assert!(controller.pending_skip.is_some());

    // Backbone error arrives during skip Replace BEFORE commit_skip
    let operation = controller
        .handle_event(PipelineEvent::FatalPipeline {
            pipeline_epoch: 1,
            message: "mixer crashed during skip Replace".into(),
        })
        .await
        .expect("fatal pipeline event during skip must produce an operation")
        .expect("stop operation should succeed");

    assert!(matches!(operation.operation, PipelineOperation::Stop));
    assert_eq!(controller.state, PipelineState::Stopped);
}

#[tokio::test]
async fn fatal_pipeline_during_initial_play_stops_station_before_commit() {
    let song = queued_song("A", 0);
    let (mut controller, _) = Harness::stopped(vec![song.clone()]).into_parts();
    assert_eq!(controller.state, PipelineState::Stopped);

    // Prepare initial play (play attempt in flight with pending_generation 1)
    let play_op = controller.play().await.expect("play must prepare");
    assert!(controller.pending_play.is_some());
    assert_eq!(controller.output_epoch, 1);
    let pending_generation = match &play_op.operation {
        PipelineOperation::Replace(plan) => plan.generation,
        _ => panic!("play must prepare a Replace operation"),
    };
    assert_eq!(pending_generation, controller.generation);

    // Backbone error arrives BEFORE commit_play
    let operation = controller
        .handle_event(PipelineEvent::FatalPipeline {
            pipeline_epoch: controller.output_epoch,
            message: "encoder crashed during initial play".into(),
        })
        .await
        .expect("fatal pipeline event during initial play must produce an operation")
        .expect("stop operation should succeed");

    assert!(matches!(operation.operation, PipelineOperation::Stop));
    assert_eq!(controller.state, PipelineState::Stopped);
    assert!(controller.pending_play.is_none());
}

#[tokio::test]
async fn stale_generation_branch_error_does_not_skip_current_track() {
    let mut harness = ControllerHarness::playing_queue(&["current", "next"]).await;
    let current_key = harness.track_key(0);
    // Controller advances to generation 2 (with the same current track key)
    harness.controller.generation = 2;
    harness.assert_state(PipelineState::Playing);

    // A late error arrives with generation 1 for the current track key
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::DecodeFailed {
                generation: 1,
                track: current_key,
                message: "late decode error from generation 1".into(),
            },
            "stale generation media branch error",
        )
        .await;
}

#[tokio::test]
async fn failed_next_branch_from_older_generation_does_not_affect_newer_generation_plan() {
    let current = queued_song("current", 0);
    let staged_g1 = queued_song("staged_g1", 1);
    let staged_g2 = queued_song("staged_g2", 2);

    let mut harness = ControllerScenario::playing()
        .with_songs(vec![current, staged_g2.clone()])
        .build()
        .await;

    let g1_key = StationController::track(staged_g1).key;
    let g2_key = StationController::track(staged_g2).key;

    assert_ne!(g1_key, g2_key, "staged_g1 and staged_g2 must have distinct track keys");
    harness.assert_state(PipelineState::Playing);
    harness.assert_generation(1);
    harness.assert_planned_next_key(Some(&g2_key));

    // A late error arrives for an older staged branch within the same generation
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::DecodeFailed {
                generation: 1,
                track: g1_key,
                message: "stale gen 1 staged error".into(),
            },
            "stale generation next branch error",
        )
        .await;

    harness.assert_planned_next_key(Some(&g2_key));
}

#[tokio::test]
async fn stale_branch_error_with_unknown_track_key_is_inert() {
    let mut harness = ControllerHarness::playing_queue(&["current"]).await;
    harness.assert_state(PipelineState::Playing);

    // Stale branch key error
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::DecodeFailed {
                generation: 1,
                track: TrackKey {
                    queue_item_id: uuid::Uuid::new_v4(),
                    song_id: uuid::Uuid::new_v4(),
                },
                message: "unknown branch error".into(),
            },
            "unknown branch error",
        )
        .await;
}

#[tokio::test]
async fn stale_backbone_error_from_older_pipeline_epoch_is_inert() {
    let mut harness = ControllerHarness::playing_queue(&["current"]).await;
    harness.assert_state(PipelineState::Playing);
    harness.assert_output_epoch(1);

    // Stale older epoch backbone error
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::FatalPipeline {
                pipeline_epoch: 0,
                message: "old epoch error".into(),
            },
            "stale epoch backbone error",
        )
        .await;
}

#[tokio::test]
async fn delayed_fatal_pipeline_error_across_multiple_generations_stops_station() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    harness.assert_state(PipelineState::Playing);
    harness.assert_generation(1);
    harness.assert_output_epoch(1);

    // Advance G1 -> G2
    let skip_prep = harness.skip().await.expect("skip to B");
    harness.commit_skip(skip_prep.attempt_id.unwrap(), &Ok(())).await;
    harness.assert_generation(2);
    harness.assert_output_epoch(1);

    // Advance G2 -> G3
    let skip_prep = harness.skip().await.expect("skip to C");
    harness.commit_skip(skip_prep.attempt_id.unwrap(), &Ok(())).await;
    harness.assert_generation(3);
    harness.assert_output_epoch(1);

    // Delayed fatal error generated during G1/G2 arrives while station is playing G3
    let op = harness
        .handle_event(PipelineEvent::FatalPipeline {
            pipeline_epoch: 1,
            message: "delayed encoder fatal error from early playback".into(),
        })
        .await
        .expect("delayed fatal pipeline error must be processed")
        .expect("stop operation must succeed");

    assert!(matches!(op.operation, PipelineOperation::Stop));
    harness.assert_state(PipelineState::Stopped);
}

#[tokio::test]
async fn stale_fatal_pipeline_error_after_controller_epoch_advance_is_ignored() {
    let mut harness = ControllerScenario::stopped().with_queue(&["A"]).build().await;

    // Lifecycle 1: Play
    let play_prep = harness.play().await.expect("play P1");
    assert!(harness.commit_play(play_prep.play_attempt_id.unwrap(), &Ok(())));
    harness.assert_state(PipelineState::Playing);
    harness.assert_output_epoch(1);

    // Full reset: Stop station
    let _ = harness.stop();
    harness.assert_state(PipelineState::Stopped);

    // Lifecycle 2: Play again from stopped
    let play_prep = harness.play().await.expect("play P2");
    assert!(harness.commit_play(play_prep.play_attempt_id.unwrap(), &Ok(())));
    harness.assert_state(PipelineState::Playing);
    harness.assert_output_epoch(2);

    // Delayed fatal error from lifecycle P1 arrives during lifecycle P2
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::FatalPipeline {
                pipeline_epoch: 1,
                message: "fatal error from old lifecycle P1".into(),
            },
            "fatal error from old pipeline lifecycle P1",
        )
        .await;
}

#[tokio::test]
async fn current_eos_stops_an_exhausted_queue() {
    let song = queued_song("current", 0);
    let pipeline = Arc::new(RecordingPipeline::new());
    let mut harness = Harness::with_pipeline(pipeline.clone(), vec![song.clone()]);
    harness.controller.generation = 1;
    let (runtime, events) = harness.into_runtime();
    runtime.pause().await.unwrap();
    events
        .send(PipelineEvent::CurrentEos {
            generation: 1,
            current: StationController::track(song).key,
        })
        .unwrap();
    // With no database reachable the exhaustion refill fails and is
    // retried (bounded, ~750ms of backoff) before the controller stops.
    testsupport::wait_for("the exhausted station to stop", || pipeline.count(Call::Stop) > 0).await;
    assert_eq!(pipeline.count(Call::SetPlaying), 1);
    assert_eq!(pipeline.count(Call::Stop), 1);
    assert_eq!(pipeline.count(Call::Replace), 0);
    runtime.shutdown().await.unwrap();
    assert!(runtime.play().await.is_err());
}

#[tokio::test]
async fn stale_and_duplicate_events_do_not_supersede_the_current_plan() {
    let song = queued_song("current", 0);
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    let harness = Harness::with_pipeline(pipeline.clone(), vec![song.clone()]);
    let current = StationController::track(song.clone()).key;
    let (runtime, events) = harness.into_runtime();
    let playing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    let gate = pipeline.replace_gate().expect("gated pipeline");
    gate.wait_started().await;
    events
        .send(PipelineEvent::DecodeFailed {
            generation: 0,
            track: TrackKey {
                queue_item_id: Uuid::new_v4(),
                song_id: Uuid::new_v4(),
            },
            message: "stale".into(),
        })
        .unwrap();
    events
        .send(PipelineEvent::CurrentEos {
            generation: 1,
            current: current.clone(),
        })
        .unwrap();
    events.send(PipelineEvent::CurrentEos { generation: 1, current }).unwrap();

    // The matching CurrentEos above exhausts the queue; with no DB the
    // refill fails and is retried (bounded, ~750ms of backoff) before the
    // controller stops, so this command queues behind it.
    tokio::time::timeout(
        Duration::from_secs(2),
        runtime.update_config(StationPlaybackConfig::from_persisted("crossfade", 1000, 1000, 0).unwrap()),
    )
    .await
    .unwrap()
    .unwrap();

    gate.release();
    playing.await.unwrap().unwrap();
    // play() tops up a below-minimum queue through the database, so the
    // stop after the terminal EOS waits on real DB roundtrips; the exact
    // call sequence is asserted below, this is only a wait-for-it window.
    testsupport::wait_for_timeout(Duration::from_secs(5), "the stop sequence to reach the pipeline", || {
        pipeline.calls().len() >= 2
    })
    .await;
    let mut config = StationPlaybackConfig::from_persisted("crossfade", 1000, 1000, 4096).unwrap();
    config.output.prebuffer_bytes = 8192;
    runtime.update_config(config).await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(pipeline.calls(), [Call::Replace, Call::Stop, Call::ApplyOutput, Call::Stop]);
}

#[tokio::test]
async fn reload_into_a_stopped_controller_starts_playback_once_songs_arrive() {
    let mut harness = ControllerScenario::stopped().build().await;
    assert!(matches!(harness.play().await.unwrap().operation, PipelineOperation::Stop));
    harness.assert_state(PipelineState::Stopped);

    let prepared = harness
        .reload_titles(&["A"], false)
        .await
        .unwrap()
        .expect("reload into a stopped controller with songs must issue a replace");
    let PipelineOperation::Replace(plan) = &prepared.operation else {
        panic!("reload into a stopped controller with songs must issue a replace");
    };
    assert!(matches!(plan.mode, ReplaceMode::InitialReplaceFromStopped));
    harness.assert_state(PipelineState::Stopped);
    let play_id = prepared.play_attempt_id.expect("play attempt id");
    assert!(harness.commit_play(play_id, &Ok(())));
    harness.assert_state(PipelineState::Playing);
    harness.assert_current_song("A");
    assert_eq!(
        harness.pipeline.count(Call::Replace),
        0,
        "replace is executed by the runtime, not the controller"
    );

    let mut empty_harness = ControllerScenario::stopped().build().await;
    assert!(matches!(empty_harness.play().await.unwrap().operation, PipelineOperation::Stop));
    let operation = empty_harness.reload_titles(&[], false).await.unwrap();
    assert!(operation.is_none(), "an empty reload must not start anything");
    empty_harness.assert_state(PipelineState::Stopped);
}

#[tokio::test]
async fn reload_realigns_staged_next_to_reordered_head() {
    run_reload_case(ReloadCase {
        name: "reload realigns staged next to reordered head",
        initial_queue: &["A", "B", "C"],
        new_queue: &["A", "X", "B", "C"],
        align: true,
        expected_op: ExpectedReloadOp::ReplaceNext {
            expected: "B",
            replacement: Some("X"),
        },
        expected_staged_before_commit: Some("B"),
        commit: Some(Ok(())),
        expected_final_staged: Some("X"),
    })
    .await;
}

#[tokio::test]
async fn reload_without_align_keeps_the_staged_next() {
    run_reload_case(ReloadCase {
        name: "reload without align keeps the staged next",
        initial_queue: &["A", "B"],
        new_queue: &["A", "X", "B"],
        align: false,
        expected_op: ExpectedReloadOp::None,
        expected_staged_before_commit: None,
        commit: None,
        expected_final_staged: Some("B"),
    })
    .await;
}

#[tokio::test]
async fn reload_with_unchanged_head_does_not_roll() {
    run_reload_case(ReloadCase {
        name: "reload with unchanged head does not roll",
        initial_queue: &["A", "B", "C"],
        new_queue: &["A", "B", "C", "X"],
        align: true,
        expected_op: ExpectedReloadOp::None,
        expected_staged_before_commit: None,
        commit: None,
        expected_final_staged: Some("B"),
    })
    .await;
}

#[tokio::test]
async fn reload_exhausting_queue_drops_the_staged_next() {
    run_reload_case(ReloadCase {
        name: "reload exhausting queue drops the staged next",
        initial_queue: &["A", "B"],
        new_queue: &["A"],
        align: true,
        expected_op: ExpectedReloadOp::ReplaceNext {
            expected: "B",
            replacement: None,
        },
        expected_staged_before_commit: Some("B"),
        commit: Some(Ok(())),
        expected_final_staged: None,
    })
    .await;
}

#[tokio::test]
async fn stale_handover_after_realignment_is_ignored() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;
    let a = harness.song(0);
    let b = harness.song(1);
    let b_key = harness.track_key(1);
    let x = queued_song("X", 3);
    let x_key = StationController::track(x.clone()).key;
    let prepared = harness
        .reload(vec![a, x, b], true)
        .await
        .unwrap()
        .expect("the swap reload must issue a roll");
    let realign_id = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&x_key));
    assert!(harness.commit_realign(realign_id, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&x_key));
    // The pipeline handed over to the OLD staged next (B) right after the swap:
    // the queue must not consume B because it will never play.
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::Handover {
                generation: 1,
                current: b_key,
            },
            "stale handover after realignment",
        )
        .await;
    harness.assert_current_song("A");
    harness.assert_staged_next("X");
}

#[tokio::test]
async fn idle_controller_resumes_when_the_queue_fills_without_a_command() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

    // skip() exhausts the queue: the unreachable DB makes the fill
    // retries fail, so the station becomes idle (auto-resumable, unlike
    // a manual stop).
    let operation = controller.skip().await.unwrap();
    assert!(matches!(operation.operation, PipelineOperation::Stop));
    assert_eq!(controller.state, PipelineState::Stopped);
    assert!(controller.idle());

    controller.queue.reload_songs(vec![fresh], false);
    let (operation, attempt_id) = controller
        .resume_from_idle()
        .await
        .expect("an idle station must resume once the queue fills");
    let PipelineOperation::Replace(plan) = operation else {
        panic!("idle resume must issue an initial replace");
    };
    assert!(matches!(plan.mode, ReplaceMode::InitialReplaceFromStopped));
    // The replace has not run yet: the controller must stay idle until
    // the pipeline executor acknowledges success.
    assert_eq!(controller.state, PipelineState::Stopped);
    assert!(controller.idle());
    controller.on_resume_result(attempt_id, Ok(()));
    assert_eq!(controller.state, PipelineState::Playing);
    assert!(!controller.idle());

    controller.stop();
    assert!(!controller.idle());
    assert!(controller.resume_from_idle().await.is_none());
    assert_eq!(controller.state, PipelineState::Stopped);
}

#[tokio::test]
async fn failed_idle_resume_keeps_the_station_retryable_until_a_replace_succeeds() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

    let operation = controller.skip().await.unwrap();
    assert!(matches!(operation.operation, PipelineOperation::Stop));
    assert!(controller.idle());

    controller.queue.reload_songs(vec![fresh], false);
    let (operation, attempt_id) = controller
        .resume_from_idle()
        .await
        .expect("an idle station must attempt a resume once the queue fills");
    assert!(matches!(operation, PipelineOperation::Replace(_)));
    assert!(controller.idle(), "a pending resume must not clear the idle flag");
    controller.on_resume_result(attempt_id, Err(PipelineError::Pipeline("boom: replace failed".into())));
    // A transient pipeline failure must not turn into a manual-stop-like
    // state: the next tick may retry.
    assert!(controller.idle(), "a failed resume must stay retryable");
    assert_eq!(controller.state, PipelineState::Stopped);

    let (operation, attempt_id) = controller
        .resume_from_idle()
        .await
        .expect("an idle station must retry the resume on the next tick");
    assert!(matches!(operation, PipelineOperation::Replace(_)));
    assert!(controller.idle());
    controller.on_resume_result(attempt_id, Ok(()));
    assert_eq!(controller.state, PipelineState::Playing);
    assert!(!controller.idle());
}

#[tokio::test]
async fn idle_resume_keeps_at_most_one_replace_in_flight() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

    let operation = controller.skip().await.unwrap();
    assert!(matches!(operation.operation, PipelineOperation::Stop));
    assert!(controller.idle());

    controller.queue.reload_songs(vec![fresh], false);
    let (operation, attempt_id) = controller
        .resume_from_idle()
        .await
        .expect("an idle station must resume once the queue fills");
    assert!(matches!(operation, PipelineOperation::Replace(_)));

    assert!(
        controller.resume_from_idle().await.is_none(),
        "a second resume must not start while one is already in flight"
    );
    assert!(
        controller.resume_from_idle().await.is_none(),
        "a third resume must not start while one is already in flight"
    );

    controller.on_resume_result(attempt_id, Err(PipelineError::Pipeline("boom: replace failed".into())));
    assert!(controller.idle());
    let (operation, second_attempt) = controller
        .resume_from_idle()
        .await
        .expect("a failed resume must allow a retry on the next tick");
    assert!(matches!(operation, PipelineOperation::Replace(_)));
    assert_ne!(attempt_id, second_attempt, "every resume attempt must carry a fresh id");
    controller.on_resume_result(second_attempt, Ok(()));
    assert_eq!(controller.state, PipelineState::Playing);
    assert!(!controller.idle());
}

#[tokio::test]
async fn stale_failed_resume_does_not_override_a_manual_play() {
    let mut harness = ControllerHarness::playing_queue(&["A"]).await;
    let fresh = queued_song("B", 1);

    let operation = harness.controller.skip().await.unwrap();
    assert!(matches!(operation.operation, PipelineOperation::Stop));
    harness.assert_idle(true);

    harness.controller.queue.reload_songs(vec![fresh], false);
    let (_operation, attempt_id) = harness
        .controller
        .resume_from_idle()
        .await
        .expect("an idle station must resume once the queue fills");
    harness.assert_idle(true);

    let prepared = harness.play().await.expect("play prepare");
    assert!(matches!(prepared.operation, PipelineOperation::Replace(_)));
    let play_id = prepared.play_attempt_id.expect("initial play attempt");
    harness.assert_state(PipelineState::Stopped);
    harness.assert_idle(false);
    assert!(harness.commit_play(play_id, &Ok(())));
    harness.assert_state(PipelineState::Playing);
    harness.assert_idle(false);

    let before = harness.snapshot();
    harness
        .controller
        .on_resume_result(attempt_id, Err(PipelineError::Pipeline("boom: stale resume failed".into())));
    harness.assert_snapshot_unchanged(&before, "stale failed resume");
}

#[tokio::test]
async fn stale_successful_resume_does_not_override_a_manual_pause() {
    let mut harness = ControllerHarness::playing_queue(&["A"]).await;
    let fresh = queued_song("B", 1);

    let operation = harness.controller.skip().await.unwrap();
    assert!(matches!(operation.operation, PipelineOperation::Stop));
    harness.assert_idle(true);

    harness.controller.queue.reload_songs(vec![fresh], false);
    let (_operation, attempt_id) = harness
        .controller
        .resume_from_idle()
        .await
        .expect("an idle station must resume once the queue fills");

    let operation = harness.pause();
    assert!(matches!(operation, PipelineOperation::SetPlaying(false)));
    harness.assert_state(PipelineState::Paused);

    let before = harness.snapshot();
    harness.controller.on_resume_result(attempt_id, Ok(()));
    harness.assert_snapshot_unchanged(&before, "stale successful resume");
}

#[tokio::test]
async fn reconnect_retry_token_is_invalidated_by_a_newer_chain_and_stop() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token_a = harness.begin_reconnect_chain();
    harness.assert_retry_is_current(token_a);

    let token_b = harness.begin_reconnect_chain();
    harness.assert_retry_not_current(token_a);
    harness.assert_retry_is_current(token_b);

    harness.stop();
    harness.assert_retry_not_current(token_b);
}

#[tokio::test]
async fn duplicate_disconnects_for_the_same_output_start_a_single_chain() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;
    harness.assert_state(PipelineState::Playing);

    let first = harness.disconnect(1, 1, "output dropped").await;
    assert!(first.is_some(), "the first disconnect must produce a reconnect attempt");
    assert!(harness.disconnect(1, 1, "output dropped").await.is_none());
    assert!(harness.disconnect(1, 1, "output dropped").await.is_none());

    harness.assert_retry_is_current(1);
    assert_eq!(harness.current_reconnect_token(), 1);
}

#[tokio::test]
async fn a_new_disconnect_after_a_successful_chain_starts_a_fresh_one() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token = harness.begin_reconnect_chain();
    harness.end_reconnect_chain(token);
    harness.assert_retry_not_current(token);

    let result = harness.disconnect(1, 1, "output dropped again").await;
    assert!(result.is_some(), "a disconnect after a finished chain must start a new chain");
    assert_ne!(harness.current_reconnect_token(), token);
}

#[tokio::test]
async fn failed_manual_reconnect_leaves_the_automatic_chain_untouched() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token_x = harness.begin_reconnect_chain();
    harness.assert_retry_is_current(token_x);

    let result = harness.reconnect().await;
    assert!(result.is_err(), "the manual reconnect must surface its refresh error");
    harness.assert_retry_is_current(token_x);
    harness.assert_shared_reconnect_token(token_x);
    assert!(!harness.controller.reconnect_token_shared().is_current_completed());
}

#[tokio::test]
async fn disconnect_after_completed_reconnect_starts_a_fresh_chain_before_reconnect_succeeded() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token_x = harness.begin_reconnect_chain();
    harness.bind_reconnect_to_output(1, 1);
    // Reconnect X finishes its pipeline call and marks shared state completed, but on_reconnect_succeeded is delayed.
    harness.controller.reconnect_token_shared().mark_completed(token_x);

    // A new disconnect of the same output arrives before the success notification is processed: must start chain Y.
    let result = harness.disconnect(1, 1, "output dropped again").await;
    assert!(result.is_some(), "a disconnect after a completed reconnect must never be lost");
    let fresh = harness.current_reconnect_token();
    assert_ne!(fresh, token_x, "a fresh chain must start after the completed one");
    harness.assert_output_known_disconnected(true);

    // Stale success of X arrives late: must not cancel active chain Y nor clear the recovery marker.
    harness.on_reconnect_succeeded(token_x);
    harness.assert_retry_is_current(fresh);
    harness.assert_output_known_disconnected(true);
}

#[tokio::test]
async fn manual_chain_binds_to_the_output_for_duplicate_coalescing() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token = harness.begin_reconnect_chain();
    harness.bind_reconnect_to_output(harness.controller.generation(), harness.controller.output_epoch());
    harness.assert_retry_is_current(token);

    let duplicate = harness.disconnect_current("output dropped").await;
    assert!(
        duplicate.is_none(),
        "a duplicate disconnect during a pending manual reconnect must be coalesced"
    );
    harness.assert_retry_is_current(token);

    harness.end_reconnect_chain(token);
    let fresh = harness.disconnect_current("output dropped again").await;
    assert!(
        fresh.is_some(),
        "a disconnect after the manual chain ended must start a fresh chain"
    );
    assert_ne!(harness.current_reconnect_token(), token);
}

#[tokio::test]
async fn pause_invalidates_the_reconnect_chain() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token = harness.begin_reconnect_chain();
    harness.assert_retry_is_current(token);

    harness.pause();
    harness.assert_retry_not_current(token);
    harness.assert_shared_reconnect_token(0);
}

#[tokio::test]
async fn stale_chain_cleanup_does_not_end_a_newer_chain() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token_x = harness.begin_reconnect_chain();
    let token_y = harness.begin_reconnect_chain();
    harness.end_reconnect_chain(token_x);

    harness.assert_retry_is_current(token_y);
}

#[tokio::test]
async fn disconnect_after_pause_and_play_starts_a_fresh_chain() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token_x = harness.begin_reconnect_chain();
    harness.bind_reconnect_to_output(harness.controller.generation(), harness.controller.output_epoch());
    harness.pause();
    harness.assert_retry_not_current(token_x);

    harness.play().await.expect("play prepare");
    let result = harness.disconnect_current("output dropped").await;
    assert!(result.is_some(), "a disconnect after Pause->Play must start a fresh chain");
    assert_ne!(harness.current_reconnect_token(), token_x);
}

#[tokio::test]
async fn target_refresh_failure_keeps_the_chain_retryable() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token = harness.begin_reconnect_chain();
    assert!(harness.reconnect_if_current(1, 1, token).await.is_err());
    harness.assert_retry_is_current(token);
    assert!(harness.reconnect_if_current(1, 1, token).await.is_err());
    harness.assert_retry_is_current(token);
}

#[tokio::test]
async fn retry_attempts_do_not_mint_a_new_chain_token() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token = harness.begin_reconnect_chain();
    for _ in 0..3 {
        assert!(harness.reconnect_if_current(1, 1, token).await.is_err());
        assert_eq!(harness.current_reconnect_token(), token);
        harness.assert_retry_is_current(token);
    }
}

#[tokio::test]
async fn superseded_chain_never_reconnects() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token_a = harness.begin_reconnect_chain();
    let token_b = harness.begin_reconnect_chain();
    harness.assert_retry_not_current(token_a);
    harness.assert_retry_is_current(token_b);

    harness.assert_reconnect_if_current_dropped(token_a).await;
}

#[tokio::test]
async fn stop_invalidates_the_chain_for_future_retries() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let token = harness.begin_reconnect_chain();
    harness.stop();
    harness.assert_reconnect_if_current_dropped(token).await;
}

#[tokio::test]
async fn stop_clears_the_marker_so_play_does_not_reconnect_the_old_output() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    harness.play().await.expect("play prepare");
    harness.pause();
    assert!(harness.disconnect_current("output dropped while paused").await.is_none());
    harness.assert_output_known_disconnected(true);

    harness.stop();
    harness.assert_output_known_disconnected(false);

    play_and_commit(&mut harness.controller).await;
    assert!(
        harness.resume_reconnect_for_break().await.is_none(),
        "a stopped station must not reconnect the old output"
    );
}

#[tokio::test]
async fn stale_success_does_not_clear_a_new_outputs_marker_after_replacement() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;

    let first = harness.disconnect(1, 1, "drop #1").await;
    assert!(first.is_some(), "disconnect #1 must start a chain");
    let token_x = harness.current_reconnect_token();
    harness.assert_output_known_disconnected(true);

    // Reconnect X completes, but before on_reconnect_succeeded arrives, a Skip replaces the output (gen 1 -> 2).
    harness.controller.reconnect_token_shared().mark_completed(token_x);
    harness.skip().await.unwrap();
    let attempt = harness
        .controller
        .pending_skip()
        .expect("a prepared skip must stay pending until the executor reports");
    harness.commit_skip(attempt, &Ok(())).await;
    harness.assert_generation(2);
    harness.assert_output_known_disconnected(false);

    // The new output (gen 2) drops: starts chain Y and sets its own disconnected marker.
    let second = harness.disconnect(2, 1, "drop #2 of the new output").await;
    assert!(second.is_some(), "the new output's disconnect must start a chain");
    let token_y = harness.current_reconnect_token();
    assert_ne!(token_y, token_x);
    harness.assert_output_known_disconnected(true);

    // Stale success of old output X arrives: must not invalidate chain Y or clear marker of gen 2.
    let before = harness.snapshot();
    harness.on_reconnect_succeeded(token_x);
    harness.assert_snapshot_unchanged(&before, "stale reconnect success for old output");
    harness.assert_retry_is_current(token_y);
}
/// Runs a reconnect-runtime scenario against an isolated, migrated test
/// database with the deterministic managed-mode settings pinned (the
/// migration seeds the singleton settings row). The database lifecycle
/// (create, cleanup on success AND on scenario panic) is handled by the
/// shared runner (`crate::test_db::run_with_test_db`); the scenario is
/// skipped entirely when `DATABASE_URL` is absent.
async fn run_reconnect_test(scenario: impl AsyncFnOnce(&crate::test_db::TestDb) -> ()) {
    crate::test_db::run_with_test_db(async |db| {
        sqlx::query(
            "UPDATE icecast_settings
             SET mode = 'managed', port = 8000, source_password = 'surcast-test',
                 admin_user = 'admin', admin_password = 'surcast-test'
             WHERE id = '00000000-0000-0000-0000-000000000001'",
        )
        .execute(&db.pool)
        .await
        .unwrap_or_else(|error| panic!("failed to configure reconnect test database: {error}"));
        scenario(db).await;
    })
    .await;
}

struct ReconnectRuntimeTest {
    runtime: StationRuntime,
    events: mpsc::UnboundedSender<PipelineEvent>,
    pipeline: Arc<RecordingPipeline>,
}

impl ReconnectRuntimeTest {
    async fn setup(db: &crate::test_db::TestDb) -> Self {
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = ControllerScenario::stopped()
            .with_db(db.pool.clone())
            .with_pipeline(pipeline.clone())
            .with_queue(&["A", "B"])
            .build()
            .await;
        let (runtime, events) = harness.into_runtime();
        Self { runtime, events, pipeline }
    }

    async fn setup_failing_once(db: &crate::test_db::TestDb) -> Self {
        let test = Self::setup(db).await;
        test.pipeline.fail_once(Call::Reconnect);
        test
    }

    async fn play(&self) {
        self.runtime.play().await.unwrap();
    }

    async fn pause(&self) {
        self.runtime.pause().await.unwrap();
    }

    async fn reconnect(&self) -> Result<(), PipelineError> {
        self.runtime.reconnect().await
    }

    async fn disconnect(&self, generation: u64, output_epoch: u64) {
        self.events
            .send(PipelineEvent::SinkDisconnected {
                generation,
                output_epoch,
                message: "output dropped".into(),
            })
            .unwrap();
    }

    async fn wait_reconnect_count(&self, count: usize) {
        testsupport::wait_for("reconnect count to reach target", || self.pipeline.count(Call::Reconnect) >= count).await;
    }

    fn assert_reconnect_count(&self, count: usize) {
        assert_eq!(self.pipeline.count(Call::Reconnect), count);
    }

    async fn assert_reconnect_count_stays(&self, count: usize, duration: Duration) {
        tokio::time::sleep(duration).await;
        assert_eq!(self.pipeline.count(Call::Reconnect), count);
    }

    async fn finish(self) {
        self.runtime.shutdown().await.unwrap();
    }
}

/// Seeds the stations row (plus its owning user) so queue cursor commits
/// are observable in the database; `current` optionally pins the
/// persisted cursor to the given queue item. `songs` are mirrored into
/// the songs/station_queue tables so the commit's reload keeps the
/// in-memory queue in sync with the persisted one.
async fn seed_station(db: &sqlx::PgPool, station_id: Uuid, current: Option<Uuid>, songs: &[SongInfo]) {
    sqlx::query("INSERT INTO users (id, username, password_hash, name) VALUES ($1, 'skip-regression', 'unused', 'skip regression')")
        .bind(station_id)
        .execute(db)
        .await
        .unwrap_or_else(|error| panic!("failed to seed the station user: {error}"));
    sqlx::query(
        "INSERT INTO stations (id, name, created_by, current_queue_item_id, current_queue_cursor_format)
         VALUES ($1, 'skip-regression', $1, $2, 1)",
    )
    .bind(station_id)
    .bind(current)
    .execute(db)
    .await
    .unwrap_or_else(|error| panic!("failed to seed the stations row: {error}"));
    for song in songs {
        sqlx::query(
            "INSERT INTO songs (id, title, artist, duration, file_path, uploaded_by)
             VALUES ($1, $2, 'skip-regression', 1, '/tmp/skip-regression', $3)",
        )
        .bind(song.song_id)
        .bind(&song.title)
        .bind(station_id)
        .execute(db)
        .await
        .unwrap_or_else(|error| panic!("failed to seed the songs row: {error}"));
        sqlx::query("INSERT INTO station_queue (id, station_id, song_id, position) VALUES ($1, $2, $3, $4)")
            .bind(song.queue_item_id)
            .bind(station_id)
            .bind(song.song_id)
            .bind(song.position)
            .execute(db)
            .await
            .unwrap_or_else(|error| panic!("failed to seed the station_queue row: {error}"));
    }
}

/// The persisted station cursor as stored in the database.
async fn persisted_cursor(db: &sqlx::PgPool, station_id: Uuid) -> Option<Uuid> {
    let (current,): (Option<Uuid>,) = sqlx::query_as("SELECT current_queue_item_id FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_one(db)
        .await
        .unwrap();
    current
}

/// Polls the persisted station cursor until it equals `expected`.
/// Deterministic: only cooperative yields, bounded by a timeout.
async fn wait_for_db_cursor(db: &sqlx::PgPool, station_id: Uuid, expected: Option<Uuid>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if persisted_cursor(db, station_id).await == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the persisted station cursor never reached {expected:?}"));
}

/// Plays the station through a gated pipeline: waits for the replace to
/// enter the gate, lets it through, and awaits the play command.
async fn play_through_gate(runtime: &StationRuntime, gate: &testsupport::Gate) {
    let play = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.play().await }
    });
    gate.wait_started().await;
    gate.release();
    play.await.unwrap().unwrap();
}

/// Waits for the next status event and asserts it announces the given
/// track as current.
async fn expect_song_change(status_rx: &mut broadcast::Receiver<StatusEvent>, title: &str) {
    let song_change = status_rx
        .recv()
        .await
        .expect("the successful replacement must publish a SongChange");
    match song_change {
        StatusEvent::SongChange { title: actual, .. } => {
            assert_eq!(actual, title, "the SongChange must announce the actually activated track");
        }
        other => panic!("expected SongChange, got {other:?}"),
    }
}

/// Starts a stopped controller and commits its initial replace with Ok,
/// transitioning it to Playing for unit tests that require a playing station.
async fn play_and_commit(controller: &mut StationController) {
    let prepared = controller.play().await.expect("play_and_commit prepare");
    let PipelineOperation::Replace(plan) = prepared.operation else {
        panic!("play_and_commit expected a Replace operation, got {:?}", prepared.operation);
    };
    assert!(
        matches!(plan.mode, ReplaceMode::InitialReplaceFromStopped),
        "play_and_commit expected InitialReplaceFromStopped mode, got {:?}",
        plan.mode
    );
    let id = prepared
        .play_attempt_id
        .expect("play_and_commit requires a play_attempt_id on the initial replace");
    assert_eq!(
        controller.state,
        PipelineState::Stopped,
        "controller must be Stopped before commit_play"
    );
    assert!(
        controller.commit_play(id, &Ok(())),
        "commit_play must successfully apply for attempt {id}"
    );
    assert_eq!(
        controller.state,
        PipelineState::Playing,
        "controller must be Playing after commit_play"
    );
}

/// Prepares a skip and returns its attempt id, asserting the operation
/// is the expected replacement.
async fn prepare_skip_attempt(controller: &mut StationController) -> u64 {
    let prepared = controller.skip().await.expect("a skip with a successor must prepare a replacement");
    assert!(
        matches!(prepared.operation, PipelineOperation::Replace(_)),
        "a skip with a successor must prepare a replacement"
    );
    controller.pending_skip().expect("the prepared skip must be pending")
}

/// Removes `removed` from the persisted queue and inserts `inserted` in
/// its place (same position): the skip commit's reload then surfaces a
/// successor that the PairPlan never staged. Used by the realign tests.
async fn replace_persisted_successor(db: &sqlx::PgPool, station_id: Uuid, removed: &TrackKey, inserted: &SongInfo) {
    sqlx::query("DELETE FROM station_queue WHERE id = $1")
        .bind(removed.queue_item_id)
        .execute(db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO songs (id, title, artist, duration, file_path, uploaded_by)
         VALUES ($1, $2, 'skip-regression', 1, '/tmp/skip-regression', $3)",
    )
    .bind(inserted.song_id)
    .bind(&inserted.title)
    .bind(station_id)
    .execute(db)
    .await
    .unwrap();
    sqlx::query("INSERT INTO station_queue (id, station_id, song_id, position) VALUES ($1, $2, $3, $4)")
        .bind(inserted.queue_item_id)
        .bind(station_id)
        .bind(inserted.song_id)
        .bind(inserted.position)
        .execute(db)
        .await
        .unwrap();
}

/// Removes `removed` from the persisted queue: a reload fed from that
/// queue then no longer contains the track a physical operation targets.
async fn remove_persisted_song(db: &sqlx::PgPool, removed: &TrackKey) {
    sqlx::query("DELETE FROM station_queue WHERE id = $1")
        .bind(removed.queue_item_id)
        .execute(db)
        .await
        .unwrap();
}

/// Unpacks a skip-commit followup into the realign id and its roll,
/// asserting the followup is the expected realign.
fn expect_realign_followup(followup: SkipFollowup) -> (u64, RollingPlan) {
    match followup {
        SkipFollowup::Realign { id, operation } => {
            let PipelineOperation::Roll(plan) = operation else {
                panic!("a realign followup must be a roll");
            };
            (id, *plan)
        }
        other => panic!("expected a realign followup, got {other:?}"),
    }
}

/// Sends a staged DecodeFailed for `track` (generation 1) and returns
/// the prepared replacement operation — or None when the event was
/// remembered by an in-flight realign (no new operation is minted).
async fn staged_decode_failure(controller: &mut StationController, track: TrackKey, message: &str) -> Option<PreparedOperation> {
    inject_decode_failure(controller, 1, &track, message).await
}

/// Injects a DecodeFailed event for a specified generation and track, returning
/// any minted prepared operation.
async fn inject_decode_failure(
    controller: &mut StationController,
    generation: u64,
    track: &TrackKey,
    message: &str,
) -> Option<PreparedOperation> {
    controller
        .handle_event(PipelineEvent::DecodeFailed {
            generation,
            track: track.clone(),
            message: message.into(),
        })
        .await
        .map(|result| result.expect("a decode failure handling must not error"))
}

/// The post-handover Attach mechanics shared by the handover tests: a
/// playing controller hands over to the staged branch and returns the
/// correlated Attach operation.
async fn prepare_handover_attach(controller: &mut StationController, b_key: TrackKey) -> PreparedOperation {
    controller
        .handle_event(PipelineEvent::Handover {
            generation: 1,
            current: b_key,
        })
        .await
        .expect("a handover of the staged branch must be accepted")
        .expect("the handover must not fail")
}

#[tokio::test]
async fn manual_reconnect_through_the_runtime_reports_success() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup(db).await;
        let result = test.reconnect().await;
        assert!(result.is_ok(), "the manual caller must receive Ok, got {result:?}");
        test.assert_reconnect_count(1);
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn manual_reconnect_through_the_runtime_reports_failure() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup_failing_once(db).await;
        let result = test.reconnect().await;
        match result {
            Err(PipelineError::Pipeline(message)) => {
                assert!(message.contains("injected failure"), "unexpected error: {message}")
            }
            other => panic!("expected the pipeline error, got {other:?}"),
        }
        test.assert_reconnect_count(1);
        test.assert_reconnect_count_stays(1, Duration::from_millis(1500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn pause_then_retry_then_play_does_not_block_future_reconnects() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup_failing_once(db).await;
        test.play().await;
        test.disconnect(1, 1).await;
        test.wait_reconnect_count(1).await;
        test.assert_reconnect_count(1);

        test.pause().await;
        test.play().await;
        test.disconnect(1, 1).await;
        test.wait_reconnect_count(2).await;
        test.assert_reconnect_count(2);
        test.assert_reconnect_count_stays(2, Duration::from_millis(1500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn disconnect_while_paused_is_recovered_by_play() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup(db).await;
        test.play().await;
        test.pause().await;
        test.disconnect(1, 1).await;
        test.assert_reconnect_count_stays(0, Duration::from_millis(200)).await;

        test.play().await;
        test.wait_reconnect_count(1).await;
        test.assert_reconnect_count(1);
        test.assert_reconnect_count_stays(1, Duration::from_millis(1500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn disconnect_before_pause_is_recovered_by_play_without_a_second_event() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup_failing_once(db).await;
        test.play().await;
        test.disconnect(1, 1).await;
        test.wait_reconnect_count(1).await;
        test.assert_reconnect_count(1);

        test.pause().await;
        test.play().await;
        test.wait_reconnect_count(2).await;
        test.assert_reconnect_count(2);
        test.assert_reconnect_count_stays(2, Duration::from_millis(1500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn pause_interrupting_recovery_keeps_the_output_recoverable() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup_failing_once(db).await;
        test.play().await;
        test.pause().await;
        test.disconnect(1, 1).await;
        test.play().await;
        test.wait_reconnect_count(1).await;
        test.assert_reconnect_count(1);

        test.pause().await;
        test.play().await;
        test.wait_reconnect_count(2).await;
        test.assert_reconnect_count(2);
        test.assert_reconnect_count_stays(2, Duration::from_millis(1500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn successful_recovery_clears_the_marker_for_later_cycles() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup(db).await;
        test.play().await;
        test.pause().await;
        test.disconnect(1, 1).await;
        test.play().await;
        test.wait_reconnect_count(1).await;
        test.assert_reconnect_count(1);

        test.pause().await;
        test.play().await;
        test.assert_reconnect_count_stays(1, Duration::from_millis(500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn successful_manual_reconnect_clears_the_marker() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup(db).await;
        test.play().await;
        test.pause().await;
        test.disconnect(1, 1).await;
        test.reconnect().await.unwrap();
        test.assert_reconnect_count(1);

        test.play().await;
        test.assert_reconnect_count_stays(1, Duration::from_millis(500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn failed_manual_reconnect_keeps_the_marker_for_play_recovery() {
    run_reconnect_test(async |db| {
        let test = ReconnectRuntimeTest::setup_failing_once(db).await;
        test.play().await;
        test.pause().await;
        test.disconnect(1, 1).await;
        let result = test.reconnect().await;
        assert!(result.is_err(), "the manual reconnect must report its failure");
        test.assert_reconnect_count(1);

        test.play().await;
        test.wait_reconnect_count(2).await;
        test.assert_reconnect_count(2);
        test.assert_reconnect_count_stays(2, Duration::from_millis(1500)).await;
        test.finish().await;
    })
    .await;
}

#[tokio::test]
async fn replaced_output_invalidates_a_queued_reconnect_before_the_pipeline() {
    run_reconnect_test(async |db| {
        let pipeline = Arc::new(RecordingPipeline::new());
        let mut harness = ControllerScenario::stopped()
            .with_db(db.pool.clone())
            .with_pipeline(pipeline.clone())
            .with_queue(&["A", "B"])
            .build()
            .await;
        let station_id = harness.controller.station_id;
        let a_item_id = harness.song(0).queue_item_id;
        seed_station(&db.pool, station_id, Some(a_item_id), &queued_songs(&["A", "B"])).await;

        play_and_commit(&mut harness.controller).await;
        harness.assert_generation(1);
        harness.assert_output_epoch(1);
        harness.assert_state(PipelineState::Playing);

        let driver = harness.controller.driver();
        let (urgent_tx, urgent) = mpsc::unbounded_channel::<crate::streamer::runtime::ExecutorTask>();
        let (regular_tx, regular) = mpsc::unbounded_channel::<crate::streamer::runtime::ExecutorTask>();
        let executor = tokio::spawn(crate::streamer::runtime::run_executor(urgent, regular, driver));

        // Gate blocks the executor on an in-flight replace so the subsequent reconnect is queued.
        let gate = Gate::new();
        pipeline.set_replace_gate(Some(gate.clone()));

        let dummy_plan = PairPlan {
            mode: ReplaceMode::InitialReplaceFromStopped,
            generation: 99,
            output_epoch: 99,
            current: StationController::track(queued_song("in-flight", 99)),
            next: None,
        };
        crate::streamer::runtime::ExecutorTask::Operation(crate::streamer::runtime::PendingPipelineAction::operation(
            PipelineOperation::Replace(Box::new(dummy_plan)),
            None,
        ))
        .submit(&urgent_tx);
        gate.wait_started().await;

        // Disconnect occurs while playing: prepares reconnect and enqueues it behind the gated operation.
        let op = harness
            .disconnect(1, 1, "output dropped")
            .await
            .expect("a disconnect while playing must start a chain")
            .expect("the reconnect target must build against the test database");
        let token_x = harness.current_reconnect_token();
        assert!(harness.reconnect_retry_is_current(token_x));
        let PipelineOperation::Reconnect(target) = op.operation else {
            panic!("expected a reconnect operation, got {op:?}");
        };
        let (commands_tx, _commands_rx) = mpsc::channel(32);
        crate::streamer::runtime::ExecutorTask::Operation(crate::streamer::runtime::PendingPipelineAction::reconnect(
            target,
            commands_tx,
            1,
            1,
            0,
            token_x,
            harness.controller.reconnect_token_shared(),
            None,
            true,
        ))
        .submit(&urgent_tx);

        // While reconnect X is queued behind the gate, a Skip replaces the output (advancing generation to 2).
        harness.skip().await.unwrap();
        let attempt = harness
            .controller
            .pending_skip()
            .expect("a prepared skip must stay pending until the executor reports");
        harness.commit_skip(attempt, &Ok(())).await;
        harness.assert_generation(2);
        assert!(
            !harness.reconnect_retry_is_current(token_x),
            "an output replacement must invalidate the reconnect chain of the old output"
        );
        assert_eq!(
            harness.controller.reconnect_token_shared().token(),
            0,
            "the shared executor state must be invalidated by the replacement"
        );

        // Releasing the gate lets the executor reach reconnect X, which must be dropped before touching the pipeline.
        gate.release();
        drop(urgent_tx);
        drop(regular_tx);
        executor.await.unwrap();
        assert_eq!(
            pipeline.count(Call::Reconnect),
            0,
            "a stale queued reconnect of a replaced output must never call pipeline.reconnect()"
        );
    })
    .await;
}

/// Regression: a failed manual Skip must not desynchronize the
/// controller/queue/DB from the still-playing pipeline. The skip commit
/// (queue cursor, generation, SongChange) happens only after the pipeline
/// replacement succeeded; a failed Replace leaves everything on the old
/// track/generation, the old EOS stays valid and drives the natural
/// progression, and later Skips still work.
#[tokio::test]
async fn failed_skip_keeps_queue_db_and_generation_on_the_old_track() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, events) = harness.into_runtime();

        runtime.play().await.unwrap();
        assert_eq!(pipeline.count(Call::Replace), 1);
        assert_eq!(
            persisted_cursor(&db.pool, station_id).await,
            Some(songs[0].queue_item_id),
            "play must leave the persisted cursor on A"
        );

        // The first Replace during the manual skip fails.
        pipeline.fail_once(Call::Replace);
        let result = runtime.skip().await;
        assert!(result.is_err(), "a failed replacement must fail the skip, got {result:?}");

        // Split-brain guard: nothing was committed to B — the persisted
        // cursor still points at A, no SongChange claims B, and the
        // controller stayed on generation 1 (proven by the EOS below).
        assert_eq!(pipeline.count(Call::Replace), 2, "the failed skip must still reach the pipeline");
        assert_eq!(
            persisted_cursor(&db.pool, station_id).await,
            Some(songs[0].queue_item_id),
            "the persisted cursor must stay on A after a failed skip"
        );
        assert!(
            matches!(status_rx.try_recv(), Err(TryRecvError::Empty)),
            "no SongChange may claim B after a failed skip"
        );

        // The EOS from the still-playing A/1 is NOT stale after the
        // failed skip: it still drives the progression to B.
        events
            .send(PipelineEvent::CurrentEos {
                generation: 1,
                current: StationController::track(songs[0].clone()).key,
            })
            .unwrap();
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        assert_eq!(pipeline.count(Call::Replace), 3, "the EOS retry must replace A with B");
        expect_song_change(&mut status_rx, "B").await;

        // A manual skip still works after the failed attempt: no stale
        // generation/identity blocks it, and the success is committed
        // exactly once (the cursor lands on C, not beyond).
        runtime.skip().await.unwrap();
        wait_for_db_cursor(&db.pool, station_id, Some(songs[2].queue_item_id)).await;
        assert_eq!(pipeline.count(Call::Replace), 4);
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// The commit must never race ahead of the pipeline: while the skip's
/// replacement is physically in flight, the persisted cursor and the
/// notifications must still describe the old track.
#[tokio::test]
async fn skip_commits_only_after_the_pipeline_replacement_finished() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B"]);
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, _events) = harness.into_runtime();
        let gate = pipeline.replace_gate().expect("gated pipeline");

        // Play: its replace is the only one that may consume the gate
        // permit — wait for it to enter, then let it through.
        play_through_gate(&runtime, &gate).await;

        // The skip's replacement blocks inside the gate: the permit is
        // gone, so `count == 2` means the replace entered the gate and
        // cannot have finished.
        let skip = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.skip().await }
        });
        testsupport::wait_for("the skip replacement to reach the pipeline", || pipeline.count(Call::Replace) == 2).await;

        // The pipeline replacement is in flight: nothing may be committed
        // yet, and the caller must not have been answered.
        assert_eq!(
            persisted_cursor(&db.pool, station_id).await,
            Some(songs[0].queue_item_id),
            "the cursor must not move before the pipeline finished the replacement"
        );
        assert!(
            matches!(status_rx.try_recv(), Err(TryRecvError::Empty)),
            "no SongChange may be published while the replacement is in flight"
        );
        assert!(!skip.is_finished(), "the manual caller must not be answered before the commit ran");

        gate.release();
        skip.await.unwrap().expect("the skip must succeed once the replacement finished");
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        expect_song_change(&mut status_rx, "B").await;
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// A completion for an attempt that is no longer current must leave the
/// newer pending attempt fully intact: the correlation happens BEFORE
/// any state is consumed, so a stale completion can never commit (or
/// destroy) a superseded attempt.
#[tokio::test]
async fn stale_skip_completion_leaves_the_newer_pending_attempt_intact() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    harness.assert_generation(1);

    let operation = harness.skip().await.expect("a skip with a successor must prepare a replacement");
    assert!(matches!(operation.operation, PipelineOperation::Replace(_)));
    let attempt = harness.controller.pending_skip().expect("the prepared skip must be pending");

    // A foreign completion (attempt - 1) must not consume the pending state, commit, or touch generation.
    let before = harness.snapshot();
    let (applied, followup) = harness.commit_skip(attempt.wrapping_sub(1), &Ok(())).await;
    assert!(!applied, "a stale completion must not apply");
    assert!(
        matches!(followup, SkipFollowup::None),
        "a stale completion must not produce follow-up work"
    );
    harness.assert_snapshot_unchanged(&before, "stale skip completion");

    // The real completion still commits exactly once.
    let (applied, followup) = harness.commit_skip(attempt, &Ok(())).await;
    assert!(applied, "the current completion must apply");
    assert!(
        matches!(followup, SkipFollowup::None),
        "the queue successor still matches the staged next"
    );
    harness.assert_generation(2);
    harness.assert_pending_skip_id(None);
}

/// The runtime loop must stay responsive while a skip replacement is in
/// flight: a second manual skip is answered immediately (single-flight),
/// and the first skip still completes and commits normally afterwards.
#[tokio::test]
async fn skip_while_a_replacement_is_in_flight_is_refused_without_blocking_the_loop() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B"]);
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, _events) = harness.into_runtime();
        let gate = pipeline.replace_gate().expect("gated pipeline");

        play_through_gate(&runtime, &gate).await;

        // The skip's replacement blocks inside the gate.
        let skip = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.skip().await }
        });
        gate.wait_started().await;
        assert_eq!(pipeline.count(Call::Replace), 2, "the skip must reach the pipeline");

        // The loop processed the second skip WHILE the first replacement
        // is still in flight: refused immediately, not queued behind the
        // physical pipeline, and the first skip keeps awaiting its own
        // completion.
        let second = runtime.skip().await;
        assert!(
            second.is_err(),
            "a second skip while one is in flight must be refused, got {second:?}"
        );
        assert!(!skip.is_finished(), "the first skip must still be awaiting its replacement");

        gate.release();
        skip.await
            .unwrap()
            .expect("the first skip must succeed once the replacement finished");
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        expect_song_change(&mut status_rx, "B").await;
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// End to end: an EOS-triggered skip whose replace fails once retries
/// through the runtime loop and commits the successor — the retry
/// replace is submitted with its own completion, so the second outcome
/// is committed exactly once.
#[tokio::test]
async fn failed_eos_skip_retries_through_the_runtime_loop() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, events) = harness.into_runtime();

        runtime.play().await.unwrap();
        assert_eq!(pipeline.count(Call::Replace), 1);

        // The EOS-driven replacement fails once; the terminal condition
        // is re-resolved and the retry succeeds.
        pipeline.fail_nth(Call::Replace, 1);
        events
            .send(PipelineEvent::CurrentEos {
                generation: 1,
                current: StationController::track(songs[0].clone()).key,
            })
            .unwrap();
        testsupport::wait_for("the EOS retry to reach the pipeline", || pipeline.count(Call::Replace) == 3).await;
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        expect_song_change(&mut status_rx, "B").await;
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// The skip commit must keep `planned_next` aligned with the branch the
/// pipeline actually staged, even when the commit's refill/reload
/// changes the queue successor: the new successor is synchronized into
/// the pipeline EXPLICITLY (an align-next roll targeting the staged
/// branch), and a subsequent handover to the realigned branch commits
/// cleanly while a handover to the old staged branch stays stale.
#[tokio::test]
async fn skip_realigns_a_changed_queue_successor_instead_of_claiming_it() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, _events) = harness.into_runtime();
        let gate = pipeline.replace_gate().expect("gated pipeline");
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);
        let d_key = StationController::track(d.clone()).key;

        play_through_gate(&runtime, &gate).await;

        // The skip prepares with C staged — but while its replacement is
        // in flight the persisted queue changes: C is removed and D
        // takes its place, so the commit's reload must surface a
        // successor that was never staged in the PairPlan.
        let skip = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.skip().await }
        });
        gate.wait_started().await;
        assert_eq!(pipeline.count(Call::Replace), 2, "the skip must reach the pipeline");
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;

        gate.release();
        skip.await.unwrap().expect("the skip must succeed once the replacement finished");
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        expect_song_change(&mut status_rx, "B").await;

        // The successor changed under the staged plan: the controller
        // must not merely write D into its bookkeeping — the pipeline
        // gets an explicit roll replacing the STAGED branch (C) with D.
        testsupport::wait_for("the realign roll to reach the pipeline", || pipeline.count(Call::Roll) == 1).await;
        let roll = pipeline.rolls().into_iter().next().expect("one realign roll");
        match roll.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, c_key, "the roll must target the branch the PairPlan staged");
                assert_eq!(
                    replacement.expect("D must be staged").track.key,
                    d_key,
                    "the roll must stage the post-commit successor"
                );
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(roll.generation, 2, "the roll must run under the new identity");

        // The handover semantics around the realign (stale handover of
        // the replaced branch, valid handover of the realigned one,
        // late completions, failed-roll behavior) are covered
        // deterministically at controller level by
        // `failed_realign_roll_keeps_the_staged_branch_claim_and_accepts_its_handover`,
        // `late_realign_completion_is_inert_after_a_newer_handover` and
        // `reload_while_a_skip_is_in_flight_defers_alignment_to_the_commit`.
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// The realign roll scheduled by a skip commit is itself two-phase: the
/// queue successor changed under the staged plan, but `planned_next`
/// keeps describing the STAGED branch (what the pipeline physically
/// holds) until the roll SUCCEEDED. A failed roll must not leave D in
/// the bookkeeping — and a handover of the still-staged C stays valid.
#[tokio::test]
async fn failed_realign_roll_keeps_the_staged_branch_claim_and_accepts_its_handover() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, events) = harness.into_runtime();
        let gate = pipeline.replace_gate().expect("gated pipeline");
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);
        let d_key = StationController::track(d.clone()).key;

        play_through_gate(&runtime, &gate).await;

        let skip = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.skip().await }
        });
        gate.wait_started().await;
        assert_eq!(pipeline.count(Call::Replace), 2, "the skip must reach the pipeline");
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
        // The realign roll the commit schedules will FAIL.
        pipeline.fail_nth(Call::Roll, 0);

        gate.release();
        skip.await
            .unwrap()
            .expect("the skip itself must succeed even when the realign fails");
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        expect_song_change(&mut status_rx, "B").await;
        testsupport::wait_for("the realign roll to run and fail", || pipeline.count(Call::Roll) == 1).await;

        // The failed roll must not have claimed D: the controller still
        // describes the staged C, so a physically valid handover of C is
        // ACCEPTED (it schedules the attach of the queue successor) —
        // never dropped as a stale realignment.
        events
            .send(PipelineEvent::Handover {
                generation: 2,
                current: c_key,
            })
            .unwrap();
        testsupport::wait_for("the accepted handover to attach the queue successor", || {
            pipeline.count(Call::Roll) == 2
        })
        .await;
        match pipeline.rolls()[1].change.clone() {
            RollingChange::Attach(next) => {
                assert_eq!(
                    next.track.key, d_key,
                    "the accepted handover must attach the successor of the handover target"
                );
            }
            other => panic!("expected an Attach after the accepted handover, got {other:?}"),
        }
        // The handover committed the REAL logical identity of the
        // physically playing C — removed from the queue though it is:
        // the persisted cursor follows C and the SongChange claims C.
        wait_for_db_cursor(&db.pool, station_id, Some(songs[2].queue_item_id)).await;
        expect_song_change(&mut status_rx, "C").await;

        // The realigned branch (D) commits cleanly once handed over.
        events
            .send(PipelineEvent::Handover {
                generation: 2,
                current: d_key,
            })
            .unwrap();
        wait_for_db_cursor(&db.pool, station_id, Some(d.queue_item_id)).await;
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// An unrelated operation submitted while a skip is pending must never
/// carry the skip's attempt id: its completion cannot commit the skip,
/// answer the manual caller, or advance anything. A decode-failure
/// roll of the staged branch is a real existing path that runs through
/// the event arm exactly while a skip may be in flight.
#[tokio::test]
async fn unrelated_event_operation_is_not_bound_to_the_pending_skip() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "X"]).await;
    let b_key = harness.track_key(1);

    let attempt = harness.prepare_skip_attempt().await;
    harness.assert_generation(1);

    // A DecodeFailed of the staged branch produces a correlated roll —
    // explicitly bound to its own realign record, NOT to the pending skip attempt.
    let prepared = harness
        .handle_event(PipelineEvent::DecodeFailed {
            generation: 1,
            track: b_key,
            message: "decoder exposed no usable branch".into(),
        })
        .await
        .expect("a staged-branch decode failure must produce a replacement")
        .expect("the replacement must not fail");
    assert!(matches!(prepared.operation, PipelineOperation::Roll(_)));
    assert!(
        prepared.attempt_id.is_none(),
        "an unrelated roll must not be bound to the pending skip attempt"
    );
    assert!(
        prepared.realign_id.is_some(),
        "the decode-failure roll must be bound to its own realign record"
    );
    harness.assert_pending_skip_id(Some(attempt));
    harness.assert_generation(1);

    // The skip's own completion still commits exactly once.
    let (applied, followup) = harness.commit_skip(attempt, &Ok(())).await;
    assert!(applied);
    assert!(
        matches!(followup, SkipFollowup::None),
        "the queue successor still matches the staged next"
    );
    harness.assert_generation(2);
    harness.assert_pending_skip_id(None);
}
/// End to end: an unrelated roll that FAILS while a skip is in flight
/// must not fail the skip. The skip's replacement was submitted with a
/// completion bound to its own attempt; the decode-failure roll is bound
/// to its own realign record, so its failure changes nothing about the
/// skip's commit or the manual caller's answer.
#[tokio::test]
async fn an_unrelated_failed_roll_does_not_fail_the_in_flight_skip() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "X"]);
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, events) = harness.into_runtime();
        let gate = pipeline.replace_gate().expect("gated pipeline");

        play_through_gate(&runtime, &gate).await;

        let skip = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.skip().await }
        });
        gate.wait_started().await;
        assert_eq!(pipeline.count(Call::Replace), 2, "the skip must reach the pipeline");
        pipeline.fail_once(Call::Roll);
        events
            .send(PipelineEvent::DecodeFailed {
                generation: 1,
                track: StationController::track(songs[1].clone()).key,
                message: "decoder exposed no usable branch".into(),
            })
            .unwrap();
        assert!(!skip.is_finished(), "the skip must keep awaiting its own completion");

        gate.release();
        skip.await
            .unwrap()
            .expect("the skip must succeed: an unrelated roll failure must not fail it");
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        expect_song_change(&mut status_rx, "B").await;
        assert_eq!(pipeline.count(Call::Roll), 1, "the unrelated roll ran once and failed");
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// A Reload that removes the pending skip TARGET while the Replace is
/// physically in flight must not turn the commit into a harmless no-op:
/// the physical Replace adopted B, so the commit represents B as the
/// logical current (a phantom while the reloaded queue no longer
/// contains it — the documented convention for a current that vanished
/// while playing) and realigns the staged branch toward the newest
/// queue. After the release the pipeline current, the controller
/// current, the queue current and the persisted cursor all describe B;
/// the manual skip caller receives the reconciled success. There is
/// never a committed generation that logically points at A/D while the
/// pipeline plays B.
#[tokio::test]
async fn reload_removing_the_pending_skip_target_reconciles_the_physical_current() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let mut status_rx = harness.controller.status_tx.subscribe();
        let (runtime, _events) = harness.into_runtime();
        let gate = pipeline.replace_gate().expect("gated pipeline");
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);
        let d_key = StationController::track(d.clone()).key;

        play_through_gate(&runtime, &gate).await;

        let skip = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.skip().await }
        });
        gate.wait_started().await;
        assert_eq!(pipeline.count(Call::Replace), 2, "the skip must reach the pipeline");

        // While the Replace is physically in flight the queue changes to
        // no longer contain B itself (A -> D): the persisted queue AND
        // the reload agree.
        remove_persisted_song(&db.pool, &StationController::track(songs[1].clone()).key).await;
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
        runtime
            .reload(vec![songs[0].clone(), d.clone()], true)
            .await
            .expect("the reload must apply");
        assert_eq!(pipeline.count(Call::Roll), 0, "the reload must not schedule a roll");

        gate.release();
        skip.await
            .unwrap()
            .expect("the manual skip caller must receive the reconciled success");
        // The commit reconciled: the pipeline plays B, and the logical
        // current represents B (phantom) while the realign roll
        // synchronizes the newest successor D into the pipeline.
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        expect_song_change(&mut status_rx, "B").await;
        testsupport::wait_for("the realign roll to reach the pipeline", || pipeline.count(Call::Roll) == 1).await;
        match pipeline.rolls()[0].change.clone() {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, c_key, "the roll must replace the staged C");
                assert_eq!(
                    replacement.expect("D must be staged").track.key,
                    d_key,
                    "the roll must align to the newest successor"
                );
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(
            pipeline.rolls()[0].current.queue_item_id,
            songs[1].queue_item_id,
            "the roll anchors on the physically current B"
        );
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// A Reload that changes the desired successor while a realign roll is
/// in flight marks the alignment dirty: the completion re-reads the
/// LATEST queue and prepares another correlated realign toward it.
/// planned_next stays on the branch the pipeline physically holds after
/// the first roll (D) — no optimistic bookkeeping toward E.
#[tokio::test]
async fn reload_during_an_in_flight_realign_reconciles_the_newest_successor_after_success() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
        let station_id = controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);
        let d_key = StationController::track(d.clone()).key;
        let e = queued_song("E", 2);
        let e_key = StationController::track(e.clone()).key;

        play_and_commit(&mut controller).await;
        let attempt = prepare_skip_attempt(&mut controller).await;
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
        controller
            .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
            .await
            .unwrap();

        // The commit schedules R1 = C -> D; planned_next stays C.
        let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
        assert!(applied);
        let (r1, roll) = expect_realign_followup(followup);
        assert_eq!(roll.generation, 2);
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&c_key),
            "planned_next must keep describing the staged C while R1 is in flight"
        );

        // While R1 is in flight the queue's successor moves D -> E: the
        // reload applies but schedules nothing itself and does not touch
        // planned_next — the completion must reconcile.
        assert!(
            controller
                .reload(vec![songs[0].clone(), songs[1].clone(), e.clone()], true)
                .await
                .unwrap()
                .is_none(),
            "a reload during an in-flight realign must not schedule a roll itself"
        );
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&c_key),
            "the reload must not optimistically touch planned_next"
        );

        // R1 succeeds: D is the branch the pipeline physically holds
        // now, so planned_next advances to D — NOT to the newest E.
        // Because the alignment is dirty, a follow-up realign D -> E is
        // prepared with its own correlation.
        let (r2, prepared) = controller
            .commit_realign(r1, &Ok(()))
            .expect("the dirty reload must produce a follow-up realign");
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&d_key),
            "planned_next advances to the branch the pipeline physically holds after R1, not to E"
        );
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("a realign followup must be a roll");
        };
        match plan.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, d_key, "R2 must replace the branch R1 adopted");
                assert_eq!(replacement.expect("E must be staged").track.key, e_key);
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r2), "R2 must be registered and correlated");

        // R2 succeeds: planned_next advances to E, the newest successor.
        assert!(
            controller.commit_realign(r2, &Ok(())).is_none(),
            "no further follow-up once the newest successor is aligned"
        );
        assert_eq!(controller.planned_next().as_ref(), Some(&e_key));
    })
    .await;
}

/// The same reload during an in-flight realign, but the first roll
/// FAILS: planned_next keeps the staged C (the physical truth), and the
/// newest queue intent is not forgotten — the controller prepares the
/// explicit recovery realign C -> E.
#[tokio::test]
async fn reload_during_an_in_flight_realign_reconciles_after_failure() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
        let station_id = controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);
        let e = queued_song("E", 2);
        let e_key = StationController::track(e.clone()).key;

        play_and_commit(&mut controller).await;
        let attempt = prepare_skip_attempt(&mut controller).await;
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
        controller
            .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
            .await
            .unwrap();
        let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
        assert!(applied);
        let (r1, _roll) = expect_realign_followup(followup);
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));

        // The queue's successor moves D -> E while R1 is in flight.
        assert!(controller
            .reload(vec![songs[0].clone(), songs[1].clone(), e.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R1 fails: planned_next stays C — the pipeline still stages it
        // — and the newest queue intent (E) is recovered explicitly with
        // a follow-up realign C -> E.
        let (r2, prepared) = controller
            .commit_realign(r1, &Err(PipelineError::Pipeline("boom".into())))
            .expect("the dirty reload must produce the explicit recovery realign");
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&c_key),
            "planned_next must keep the staged C after the failed roll"
        );
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("a realign followup must be a roll");
        };
        match plan.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, c_key, "the recovery must replace the still-staged C");
                assert_eq!(replacement.expect("E must be staged").track.key, e_key);
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r2));

        // The recovery succeeds: planned_next advances to E.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&e_key));
    })
    .await;
}

/// A physically valid handover of a track that was REMOVED from the
/// queue (the failed-realign scenario) must commit the real logical
/// identity: queue current becomes C (phantom), the persisted cursor
/// follows C, the SongChange claims C and the generation stays the
/// committed one — never a controller that logically points at B while
/// the pipeline plays C.
#[tokio::test]
async fn handover_of_a_removed_track_commits_the_phantom_identity() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
        let station_id = controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);
        let d_key = StationController::track(d.clone()).key;
        let mut status_rx = controller.status_tx.subscribe();

        play_and_commit(&mut controller).await;
        let attempt = prepare_skip_attempt(&mut controller).await;
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
        controller
            .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
            .await
            .unwrap();
        let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
        assert!(applied);
        let (id, _roll) = expect_realign_followup(followup);

        // The realign fails: the pipeline still stages C, and the
        // controller still claims it.
        assert!(controller
            .commit_realign(id, &Err(PipelineError::Pipeline("boom".into())))
            .is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));
        assert_eq!(
            controller.queue.current_song_info().expect("a current must exist").queue_item_id,
            songs[1].queue_item_id
        );
        // Drain the commit's own SongChange (B) before the handover.
        expect_song_change(&mut status_rx, "B").await;

        // The pipeline physically hands over to C — which the queue no
        // longer contains. The handover must COMMIT the real identity:
        // current = C, cursor = C, SongChange = C, generation = 2.
        let handover = controller
            .handle_event(PipelineEvent::Handover {
                generation: 2,
                current: c_key.clone(),
            })
            .await
            .expect("a handover of the still-staged branch must be accepted")
            .expect("the handover must not fail");
        assert!(handover.attempt_id.is_none(), "a handover is never a skip operation");
        assert_eq!(
            controller
                .queue
                .current_song_info()
                .expect("a committed handover has a current")
                .queue_item_id,
            c_key.queue_item_id,
            "the queue current must represent the physically playing C even though it was removed from the queue"
        );
        assert_eq!(
            controller.planned_next(),
            None,
            "the accepted handover's attach is two-phase: nothing is claimed until the roll succeeds"
        );
        let attach_id = handover.realign_id.expect("the handover's attach must be a correlated realign");
        assert!(controller.commit_realign(attach_id, &Ok(())).is_none());
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&d_key),
            "the successful attach claims the queue successor"
        );
        assert_eq!(controller.generation, 2, "the generation must stay the committed one");
        wait_for_db_cursor(&db.pool, station_id, Some(songs[2].queue_item_id)).await;
        match status_rx.recv().await.expect("the handover must publish a SongChange") {
            StatusEvent::SongChange { title, .. } => {
                assert_eq!(title, "C", "the SongChange must claim C, not another track");
            }
            other => panic!("expected SongChange, got {other:?}"),
        }

        // The realigned branch commits cleanly once handed over.
        let _ = controller
            .handle_event(PipelineEvent::Handover {
                generation: 2,
                current: d_key,
            })
            .await;
        wait_for_db_cursor(&db.pool, station_id, Some(d.queue_item_id)).await;
        assert_eq!(
            controller
                .queue
                .current_song_info()
                .expect("a committed handover has a current")
                .queue_item_id,
            d.queue_item_id
        );
    })
    .await;
}

/// A dirty Reload that exhausts the queue while a realign is in flight
/// must still reconcile after the FIRST roll succeeds: the pipeline
/// physically stages D, so planned_next advances to D — and the
/// follow-up realign D -> None makes the pipeline DROP D. `None` is an
/// explicit desired physical state, never an automatic "no work".
#[tokio::test]
async fn dirty_reload_to_an_exhausted_queue_drops_the_staged_branch_after_success() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
        let station_id = controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);
        let d_key = StationController::track(d.clone()).key;

        play_and_commit(&mut controller).await;
        let attempt = prepare_skip_attempt(&mut controller).await;
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
        controller
            .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
            .await
            .unwrap();
        let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
        assert!(applied);
        let (r1, _roll) = expect_realign_followup(followup);

        // While R1 (C -> D) is in flight the queue shrinks to B only: the
        // reload applies, schedules nothing, and marks the alignment dirty.
        assert!(
            controller
                .reload(vec![songs[0].clone(), songs[1].clone()], true)
                .await
                .unwrap()
                .is_none(),
            "a reload during an in-flight realign must not schedule a roll itself"
        );

        // R1 succeeds: the pipeline stages D, so planned_next = D — the
        // newest queue wants NOTHING, and the follow-up realign D -> None
        // drops the staged branch.
        let (r2, prepared) = controller
            .commit_realign(r1, &Ok(()))
            .expect("the dirty reload must produce the drop follow-up");
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&d_key),
            "planned_next advances to the physically staged D after R1"
        );
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("a realign followup must be a roll");
        };
        match plan.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, d_key, "R2 must drop the branch R1 adopted");
                assert!(replacement.is_none(), "an exhausted queue must stage nothing");
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r2), "R2 must be registered and correlated");

        // R2 succeeds: the staged claim is dropped.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next(), None, "the exhausted queue stages nothing");
    })
    .await;
}

/// The same exhausted-queue reload, but the FIRST realign FAILS: the
/// pipeline still stages C, so planned_next stays C — and the recovery
/// realign C -> None drops it.
#[tokio::test]
async fn dirty_reload_to_an_exhausted_queue_drops_the_staged_branch_after_failure() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
        let station_id = controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let c_key = StationController::track(songs[2].clone()).key;
        let d = queued_song("D", 2);

        play_and_commit(&mut controller).await;
        let attempt = prepare_skip_attempt(&mut controller).await;
        replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
        controller
            .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
            .await
            .unwrap();
        let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
        assert!(applied);
        let (r1, _roll) = expect_realign_followup(followup);

        // The queue shrinks to B only while R1 is in flight.
        assert!(controller
            .reload(vec![songs[0].clone(), songs[1].clone()], true)
            .await
            .unwrap()
            .is_none());

        // R1 fails: planned_next stays C (the pipeline still stages it),
        // and the recovery realign C -> None is prepared.
        let (r2, prepared) = controller
            .commit_realign(r1, &Err(PipelineError::Pipeline("boom".into())))
            .expect("the dirty reload must produce the drop follow-up");
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&c_key),
            "planned_next must keep the still-staged C after the failed roll"
        );
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("a realign followup must be a roll");
        };
        match plan.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, c_key, "the recovery must drop the still-staged C");
                assert!(replacement.is_none(), "an exhausted queue must stage nothing");
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }

        // The recovery succeeds: the staged claim is dropped.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next(), None);
    })
    .await;
}

/// A failed ordinary reload realign keeps the staged claim: planned_next
/// stays the physically staged branch, so a handover of it remains
/// accepted (it schedules the attach of the queue successor) instead of
/// being dropped as a stale realignment.
#[tokio::test]
async fn failed_reload_realign_keeps_the_staged_claim() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;
    let b_key = harness.track_key(1);
    let x = queued_song("X", 3);
    let x_key = StationController::track(x.clone()).key;

    let prepared = harness
        .reload(vec![harness.song(0), x, harness.song(1)], true)
        .await
        .unwrap()
        .expect("the swap reload must issue a roll");
    let realign_id = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&x_key));

    // The roll fails: the pipeline still stages B, so planned_next stays B.
    assert!(harness
        .commit_realign(realign_id, &Err(PipelineError::Pipeline("boom".into())))
        .is_none());
    harness.assert_planned_next_key(Some(&b_key));

    // A handover of the still-staged B is physically valid and accepted.
    let handover = harness
        .handle_event(PipelineEvent::Handover {
            generation: 1,
            current: b_key,
        })
        .await
        .expect("a handover of the still-staged branch must be accepted")
        .expect("the handover must not fail");
    assert!(handover.attempt_id.is_none());
    let attach_id = harness.assert_rolling_attach(&handover, &x_key);
    harness.assert_no_staged_next();
    assert!(harness.commit_realign(attach_id, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&x_key));
}

/// End-to-end: a failed ordinary reload realign (fault injection) keeps
/// the staged claim, so the pipeline's handover of the staged branch is
/// accepted and commits for real.
#[tokio::test]
async fn failed_reload_realign_keeps_the_handover_valid() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let (runtime, events) = harness.into_runtime();
        let b_key = StationController::track(songs[1].clone()).key;
        let x = queued_song("X", 3);
        let x_key = StationController::track(x.clone()).key;

        runtime.play().await.unwrap();
        // The persisted queue agrees with the reload: C is replaced by X
        // so the handover's commit re-read (refill) keeps X as successor.
        replace_persisted_successor(&db.pool, station_id, &StationController::track(songs[2].clone()).key, &x).await;
        // The reload's realign roll will FAIL.
        pipeline.fail_once(Call::Roll);
        runtime
            .reload(vec![songs[0].clone(), x.clone(), songs[1].clone()], true)
            .await
            .expect("the reload must apply");
        testsupport::wait_for("the failed reload roll to complete", || pipeline.count(Call::Roll) == 1).await;

        // The pipeline hands over to the still-staged B: accepted (the
        // controller kept claiming it) and committed for real.
        events
            .send(PipelineEvent::Handover {
                generation: 1,
                current: b_key,
            })
            .unwrap();
        testsupport::wait_for("the accepted handover to attach the queue successor", || {
            pipeline.count(Call::Roll) == 2
        })
        .await;
        match pipeline.rolls()[1].change.clone() {
            RollingChange::Attach(next) => {
                assert_eq!(
                    next.track.key, x_key,
                    "the accepted handover must attach the successor of the handover target"
                );
            }
            other => panic!("expected an Attach after the accepted handover, got {other:?}"),
        }
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        // The attach is correlated: its completion advanced planned_next
        // to X, so a physically valid handover of X is now accepted and
        // commits for real (a fire-and-forget attach would leave the
        // controller claiming nothing and drop this handover as stale).
        let _ = runtime.status().await.expect("the status probe must answer");
        events
            .send(PipelineEvent::Handover {
                generation: 1,
                current: x_key,
            })
            .unwrap();
        wait_for_db_cursor(&db.pool, station_id, Some(x.queue_item_id)).await;
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// A successful Handover Attach is two-phase: after the handover
/// commits the staged branch, `planned_next` stays None — the pipeline
/// stages nothing — until the correlated Attach roll SUCCEEDED; only
/// then is the queue successor claimed.
#[tokio::test]
async fn handover_attach_is_two_phase_until_the_roll_succeeds() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "X"]).await;
    let b_key = harness.track_key(1);
    let x_key = harness.track_key(2);

    let prepared = prepare_handover_attach(harness.controller_mut(), b_key.clone()).await;
    let PipelineOperation::Roll(plan) = &prepared.operation else {
        panic!("the handover must issue an attach roll");
    };
    assert_eq!(plan.generation, 1);
    assert_eq!(plan.current, b_key);
    let attach_id = harness.assert_rolling_attach(&prepared, &x_key);
    assert!(prepared.attempt_id.is_none());
    harness.assert_current_song("B");
    harness.assert_pending_realign_id(Some(attach_id));
    harness.assert_no_staged_next();
    // The attach roll succeeds: the queue successor is claimed.
    assert!(harness.commit_realign(attach_id, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&x_key));
}

/// A failed Handover Attach claims nothing: `planned_next` stays None,
/// no controller state claims the successor — and a later reload
/// attaches it again through the same correlated mechanism.
#[tokio::test]
async fn failed_handover_attach_claims_nothing_and_a_reload_recovers() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "X"]).await;
    let b_key = harness.track_key(1);
    let x_key = harness.track_key(2);

    let prepared = prepare_handover_attach(harness.controller_mut(), b_key.clone()).await;
    let attach_id = harness.assert_rolling_attach(&prepared, &x_key);

    // The attach fails: nothing is claimed.
    assert!(harness
        .commit_realign(attach_id, &Err(PipelineError::Pipeline("boom".into())))
        .is_none());
    harness.assert_no_staged_next();
    harness.assert_pending_realign_id(None);
    harness.assert_current_song("B");

    // A later reload reconciles: the orphaned successor is attached again with a fresh correlated realign.
    let prepared = harness
        .reload_reordered(&["A", "B", "X"], true)
        .await
        .unwrap()
        .expect("the reload must attach the orphaned successor");
    let recovery_id = harness.assert_rolling_attach(&prepared, &x_key);
    assert_ne!(recovery_id, attach_id, "the recovery is a fresh realign");
    harness.assert_no_staged_next();
    assert!(harness.commit_realign(recovery_id, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&x_key));
}

/// The single-slot serialization rule: a duplicate staged DecodeFailed
/// while a realign is in flight must NOT overwrite the pending record —
/// the in-flight roll already replaces the failed branch, so no second
/// operation is minted — and on SUCCESS the absorbed event is fully
/// satisfied: `commit_realign` returns no follow-up work (it must never
/// reconcile the queue back toward the broken branch, which may still
/// be the queue head).
#[tokio::test]
async fn duplicate_decode_failure_does_not_overwrite_the_in_flight_realign() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    let prepared = harness
        .staged_decode_failure(b_key.clone(), "broken next")
        .await
        .expect("the first decode failure must prepare a roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));
    harness.assert_pending_realign_id(Some(r1));
    harness.assert_planned_next_key(Some(&b_key));

    // The same staged branch fails again before R1 completes: absorbed.
    assert!(harness.staged_decode_failure(b_key.clone(), "broken next again").await.is_none());
    harness.assert_pending_realign_id(Some(r1));
    harness.assert_planned_next_key(Some(&b_key));

    // R1 succeeds: no recovery work.
    assert!(harness.commit_realign(r1, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&c_key));
}

/// A staged DecodeFailed while a RELOAD realign for the same staged
/// branch is in flight is absorbed the same way: the reload roll owns
/// the physical change.
#[tokio::test]
async fn decode_failure_during_a_reload_realign_keeps_the_record() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C", "D"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    // The queue reorders so successor becomes C.
    let prepared = harness
        .reload_reordered(&["A", "C", "B", "D"], true)
        .await
        .unwrap()
        .expect("the reordered reload must issue a roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));
    harness.assert_pending_realign_id(Some(r1));

    // The still-staged B fails to decode: absorbed.
    assert!(harness.staged_decode_failure(b_key.clone(), "broken next").await.is_none());
    harness.assert_pending_realign_id(Some(r1));
    harness.assert_planned_next_key(Some(&b_key));

    // R1 succeeds: successor is claimed.
    assert!(harness.commit_realign(r1, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&c_key));
}

/// A decode-failure intent that arrives while a realign is in flight is
/// not lost: after R1 completes, the follow-up realign is based on the
/// NOW-KNOWN physical state (the branch the roll physically adopted),
/// never on a stale second record.
#[tokio::test]
async fn decode_failure_intent_is_reconciled_after_the_in_flight_realign() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    let prepared = harness
        .staged_decode_failure(b_key.clone(), "broken next")
        .await
        .expect("the first decode failure must prepare a roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    // The queue drops B and offers D while R1 is in flight.
    let d = queued_song("D", 2);
    let d_key = StationController::track(d.clone()).key;
    assert!(harness.reload(vec![harness.song(0), d], true).await.unwrap().is_none());
    assert!(harness.staged_decode_failure(b_key, "broken next again").await.is_none());
    harness.assert_pending_realign_id(Some(r1));

    // R1 succeeds -> follow-up C -> D.
    let (r2, followup) = harness.commit_realign(r1, &Ok(())).expect("dirty realign follow-up");
    harness.assert_planned_next_key(Some(&c_key));
    let r2_id = harness.assert_rolling_replace_next(&followup, &c_key, Some(&d_key));
    assert_eq!(r2, r2_id);
    harness.assert_pending_realign_id(Some(r2));

    // Follow-up succeeds: newest successor D claimed.
    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));
}

/// The absorbed duplicate staged DecodeFailed is NOT lost when the
/// in-flight realign FAILS: the still-staged broken branch is replaced
/// again — a fresh correlated recovery (R2) computed from the now-known
/// physical state, never an optimistic claim, and never a second
/// operation while R1 is unresolved.
#[tokio::test]
async fn duplicate_decode_failure_retries_after_the_first_realign_fails() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    let prepared = harness
        .staged_decode_failure(b_key.clone(), "broken next")
        .await
        .expect("the first decode failure must prepare a roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));
    harness.assert_pending_realign_id(Some(r1));
    harness.assert_planned_next_key(Some(&b_key));

    assert!(harness.staged_decode_failure(b_key.clone(), "broken next again").await.is_none());
    harness.assert_pending_realign_id(Some(r1));

    // R1 FAILS: preserved as fresh recovery R2.
    let (r2, followup) = harness
        .commit_realign(r1, &Err(PipelineError::Pipeline("boom".into())))
        .expect("the failed realign must produce the recovery");
    assert_ne!(r2, r1);
    harness.assert_planned_next_key(Some(&b_key));
    let r2_id = harness.assert_rolling_replace_next(&followup, &b_key, Some(&c_key));
    assert_eq!(r2, r2_id);
    harness.assert_pending_realign_id(Some(r2));

    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&c_key));
}

/// Queue-dirty reconciliation and the preserved decode-failure fact
/// compose into exactly ONE correlated next operation when R1 fails:
/// the recovery targets the physical B (still staged) with the latest
/// queue's successor after the broken branch — never two competing
/// rolls.
#[tokio::test]
async fn dirty_queue_and_duplicate_decode_failure_compose_into_one_recovery() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let b_key = harness.track_key(1);
    let d = queued_song("D", 2);
    let d_key = StationController::track(d.clone()).key;

    let prepared = harness
        .staged_decode_failure(b_key.clone(), "broken next")
        .await
        .expect("the first decode failure must prepare a roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&harness.track_key(2)));
    assert!(harness.staged_decode_failure(b_key.clone(), "broken next again").await.is_none());
    harness.assert_pending_realign_id(Some(r1));

    // Reload changes queue (C -> D) while R1 unresolved.
    assert!(harness
        .reload(vec![harness.song(0), harness.song(1), d], true)
        .await
        .unwrap()
        .is_none());

    // R1 FAILS: exactly ONE recovery roll toward D.
    let (r2, followup) = harness
        .commit_realign(r1, &Err(PipelineError::Pipeline("boom".into())))
        .expect("failed realign recovery");
    assert_ne!(r2, r1);
    harness.assert_planned_next_key(Some(&b_key));
    let r2_id = harness.assert_rolling_replace_next(&followup, &b_key, Some(&d_key));
    assert_eq!(r2, r2_id);
    harness.assert_pending_realign_id(Some(r2));

    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));
}

/// End-to-end: a duplicate staged DecodeFailed is preserved through a
/// failed first replacement roll — the recovery roll is submitted and
/// the controller accepts the resulting branch. Order-insensitive: the
/// duplicate event is absorbed whether it arrives before or after the
/// first roll's completion.
#[tokio::test]
async fn duplicate_decode_failure_recovery_at_runtime() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let (runtime, events) = harness.into_runtime();
        let b_key = StationController::track(songs[1].clone()).key;
        let c_key = StationController::track(songs[2].clone()).key;

        runtime.play().await.unwrap();
        // The FIRST replacement roll (B -> C) will FAIL.
        pipeline.fail_nth(Call::Roll, 0);
        events
            .send(PipelineEvent::DecodeFailed {
                generation: 1,
                track: b_key.clone(),
                message: "decoder exposed no usable branch".into(),
            })
            .unwrap();
        // A duplicate DecodeFailed(B) arrives before the roll completes:
        // absorbed — no second roll, the failure fact is remembered.
        events
            .send(PipelineEvent::DecodeFailed {
                generation: 1,
                track: b_key.clone(),
                message: "decoder exposed no usable branch again".into(),
            })
            .unwrap();
        // The failed replacement and its decode recovery complete (the
        // count may skip 1: the recovery is minted as soon as the failed
        // roll's completion is processed).
        testsupport::wait_for("the failed replacement and its recovery roll to run", || {
            pipeline.count(Call::Roll) == 2
        })
        .await;
        wait_for_db_cursor(&db.pool, station_id, Some(songs[0].queue_item_id)).await;
        let _ = runtime.status().await.expect("the status probe must answer");

        // The recovery roll is the expected replacement of the broken
        // branch.
        match pipeline.rolls()[1].change.clone() {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, b_key, "the recovery must target the still-staged broken branch");
                assert_eq!(replacement.expect("C must be staged").track.key, c_key);
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        // The recovery succeeded: a handover of C is accepted and
        // commits for real.
        wait_for_db_cursor(&db.pool, station_id, Some(songs[0].queue_item_id)).await;
        let _ = runtime.status().await.expect("the status probe must answer");
        events
            .send(PipelineEvent::Handover {
                generation: 1,
                current: c_key,
            })
            .unwrap();
        wait_for_db_cursor(&db.pool, station_id, Some(songs[2].queue_item_id)).await;
        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// A known-broken staged branch (B) remains excluded across follow-up
/// realigns under the same current identity: when R1 (B -> C) succeeds
/// into a queue-dirtied follow-up R2 (C -> D), and a second reload
/// dirties R2 to E while B is still the raw queue head, the follow-up R3
/// must align to E (never re-staging the broken B).
#[tokio::test]
async fn decode_exclusion_survives_a_successful_dirty_follow_up_chain() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let a = harness.song(0);
    let b = harness.song(1);
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d = queued_song("D", 3);
    let d_key = StationController::track(d.clone()).key;
    let e = queued_song("E", 4);
    let e_key = StationController::track(e.clone()).key;

    // R1: staged B fails to decode -> replaces with C.
    let prepared = harness
        .staged_decode_failure(b_key.clone(), "broken next")
        .await
        .expect("the first decode failure must prepare a roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    // While R1 is in flight: reload queue to [A, B, D] (B is still raw head).
    assert!(harness.reload(vec![a.clone(), b.clone(), d], true).await.unwrap().is_none());
    harness.assert_pending_realign_id(Some(r1));

    // R1 succeeds: follow-up R2 is minted toward D (skipping B).
    let (r2, followup) = harness.commit_realign(r1, &Ok(())).expect("dirty realign follow-up R2");
    assert_ne!(r2, r1);
    harness.assert_planned_next_key(Some(&c_key));
    let r2_id = harness.assert_rolling_replace_next(&followup, &c_key, Some(&d_key));
    assert_eq!(r2, r2_id);
    harness.assert_pending_realign_id(Some(r2));

    // While R2 is in flight: reload queue to [A, B, E] (B is still raw head).
    assert!(harness.reload(vec![a, b, e], true).await.unwrap().is_none());

    // R2 succeeds: follow-up R3 MUST be minted toward E (NEVER B!).
    let (r3, followup) = harness.commit_realign(r2, &Ok(())).expect("dirty realign follow-up R3");
    assert_ne!(r3, r2);
    harness.assert_planned_next_key(Some(&d_key));
    let r3_id = harness.assert_rolling_replace_next(&followup, &d_key, Some(&e_key));
    assert_eq!(r3, r3_id);
    harness.assert_pending_realign_id(Some(r3));

    // R3 succeeds: no further follow-up; planned_next becomes E.
    assert!(harness.commit_realign(r3, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&e_key));
}

/// If a dirty follow-up (R2: C -> D) fails after a reload to [A, B, E],
/// the physical branch remains C and the follow-up must align C -> E
/// (never resurrecting broken B).
#[tokio::test]
async fn decode_exclusion_survives_follow_up_failure() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let a = harness.song(0);
    let b = harness.song(1);
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d = queued_song("D", 3);
    let d_key = StationController::track(d.clone()).key;
    let e = queued_song("E", 4);
    let e_key = StationController::track(e.clone()).key;

    let prepared = harness.staged_decode_failure(b_key.clone(), "broken next").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    assert!(harness.reload(vec![a.clone(), b.clone(), d], true).await.unwrap().is_none());
    let (r2, followup) = harness.commit_realign(r1, &Ok(())).expect("R2");
    let r2_id = harness.assert_rolling_replace_next(&followup, &c_key, Some(&d_key));
    assert_eq!(r2, r2_id);

    assert!(harness.reload(vec![a, b, e], true).await.unwrap().is_none());

    // R2 FAILS: physical state is still C. Follow-up must be C -> E (never B).
    let (r3, followup) = harness
        .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
        .expect("failed R2 must produce dirty follow-up");
    assert_ne!(r3, r2);
    harness.assert_planned_next_key(Some(&c_key));
    let r3_id = harness.assert_rolling_replace_next(&followup, &c_key, Some(&e_key));
    assert_eq!(r3, r3_id);
    harness.assert_pending_realign_id(Some(r3));

    assert!(harness.commit_realign(r3, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&e_key));
}

/// An unchanged reload during a decode follow-up must not mark the
/// realign dirty merely because raw peek_next_song() is the broken B:
/// dirty detection compares against the effective desired successor.
#[tokio::test]
async fn unchanged_reload_does_not_spuriously_dirty_a_decode_realign_chain() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let a = harness.song(0);
    let b = harness.song(1);
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d = queued_song("D", 3);
    let d_key = StationController::track(d.clone()).key;

    let prepared = harness.staged_decode_failure(b_key.clone(), "broken next").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    assert!(harness.reload(vec![a.clone(), b.clone(), d.clone()], true).await.unwrap().is_none());
    let (r2, followup) = harness.commit_realign(r1, &Ok(())).expect("R2");
    let r2_id = harness.assert_rolling_replace_next(&followup, &c_key, Some(&d_key));
    assert_eq!(r2, r2_id);

    // An UNCHANGED reload with the same effective queue [A, B, D]: NOT dirty!
    assert!(harness.reload(vec![a, b, d], true).await.unwrap().is_none());

    assert!(
        harness.commit_realign(r2, &Ok(())).is_none(),
        "an unchanged reload must not manufacture spurious follow-up work"
    );
    harness.assert_planned_next_key(Some(&d_key));
}

/// A reload of an unchanged queue during an automatic decode retry must
/// not manufacture a dirty mark and bypass the bounded retry budget.
#[tokio::test]
async fn retry_budget_is_not_bypassed_by_an_unchanged_reload() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let a = harness.song(0);
    let b = harness.song(1);
    let c = harness.song(2);
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    let prepared = harness.staged_decode_failure(b_key.clone(), "broken next").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    // R1 FAILS -> bounded retry R2 (B -> C), budget now 0.
    let (r2, followup) = harness
        .commit_realign(r1, &Err(PipelineError::Pipeline("R1 failed".into())))
        .expect("failed R1 must produce retry R2");
    let r2_id = harness.assert_rolling_replace_next(&followup, &b_key, Some(&c_key));
    assert_eq!(r2, r2_id);

    // An UNCHANGED reload [A, B, C] while R2 is in flight: NOT dirty!
    assert!(harness.reload(vec![a, b, c], true).await.unwrap().is_none());

    // R2 FAILS with exhausted budget: no R3, no hot loop.
    assert!(
        harness
            .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
            .is_none(),
        "exhausted retry budget with no queue change must produce no further roll"
    );
    harness.assert_planned_next_key(Some(&b_key));
}

/// If an explicit reload genuinely changes the effective desired successor
/// (C -> D) while a retry with budget 0 is in flight, the failure of the
/// retry still reconciles toward D as a single correlated queue operation
/// (without re-arming an automatic decode retry budget).
#[tokio::test]
async fn changed_reload_reconciles_after_retry_budget_exhaustion() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let a = harness.song(0);
    let b = harness.song(1);
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d = queued_song("D", 3);
    let d_key = StationController::track(d.clone()).key;

    let prepared = harness.staged_decode_failure(b_key.clone(), "broken next").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    let (r2, followup) = harness
        .commit_realign(r1, &Err(PipelineError::Pipeline("R1 failed".into())))
        .expect("failed R1 must produce retry R2");
    let r2_id = harness.assert_rolling_replace_next(&followup, &b_key, Some(&c_key));
    assert_eq!(r2, r2_id);

    // Genuine queue change [A, B, D] while R2 is in flight: dirty!
    assert!(harness.reload(vec![a, b, d], true).await.unwrap().is_none());

    // R2 FAILS: retry budget exhausted, dirty queue reconciles toward D.
    let (r3, followup) = harness
        .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
        .expect("dirty queue change must reconcile toward D");
    assert_ne!(r3, r2);
    harness.assert_planned_next_key(Some(&b_key));
    let r3_id = harness.assert_rolling_replace_next(&followup, &b_key, Some(&d_key));
    assert_eq!(r3, r3_id);
    harness.assert_pending_realign_id(Some(r3));

    assert!(
        harness
            .commit_realign(r3, &Err(PipelineError::Pipeline("R3 failed".into())))
            .is_none(),
        "exhausted queue follow-up must not loop"
    );
    harness.assert_planned_next_key(Some(&b_key));
}

/// Problem 1 regression: when R1 (B -> C) succeeds with no dirty
/// follow-up, pending_realign becomes None. A subsequent unchanged
/// Reload(A -> B -> C) across the idle gap must still know B is broken
/// under current A, choosing C (no roll, planned_next stays C).
#[tokio::test]
async fn decode_exclusion_survives_idle_gap_after_roll_success() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    let prepared = harness.staged_decode_failure(b_key, "broken next").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &harness.track_key(1), Some(&c_key));

    // R1 succeeds: pending_realign is cleared; planned_next advances to C.
    assert!(harness.commit_realign(r1, &Ok(())).is_none());
    harness.assert_pending_realign_id(None);
    harness.assert_planned_next_key(Some(&c_key));

    // Unchanged reload across the idle gap: effective successor is C -> no roll prepared!
    let result = harness.reload_reordered(&["A", "B", "C"], true).await.unwrap();
    assert!(result.is_none(), "unchanged reload after idle gap must not prepare a roll");
    harness.assert_pending_realign_id(None);
    harness.assert_planned_next_key(Some(&c_key));
}

/// Problem 1 regression: after an idle gap following R1 success, a
/// changed reload to [A, B, D] must still skip the broken B (which is
/// raw queue head) and prepare ReplaceNext(C -> D), never C -> B.
#[tokio::test]
async fn changed_reload_after_idle_gap_still_skips_excluded_branch() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    let prepared = harness.staged_decode_failure(b_key, "broken next").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &harness.track_key(1), Some(&c_key));

    assert!(harness.commit_realign(r1, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&c_key));

    // Reload to [A, B, D] across the idle gap:
    let d = queued_song("D", 3);
    let d_key = StationController::track(d.clone()).key;
    let prepared = harness
        .reload(vec![harness.song(0), harness.song(1), d], true)
        .await
        .unwrap()
        .expect("changed reload must prepare a roll");
    let r2 = harness.assert_rolling_replace_next(&prepared, &c_key, Some(&d_key));

    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));
}

/// Problem 2 regression: multiple staged branches can fail decoding
/// under the same current playback identity. When B fails (replaced with
/// C) and then C also fails, both B and C are excluded, so the second
/// replacement must choose D (never C -> B).
#[tokio::test]
async fn consecutive_decode_failures_skip_all_broken_branches() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C", "D"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d_key = harness.track_key(3);

    let prepared = harness.staged_decode_failure(b_key, "broken next").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &harness.track_key(1), Some(&c_key));

    // R1 (B -> C) succeeds: physical staged is now C.
    assert!(harness.commit_realign(r1, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&c_key));

    // Now C also fails decoding!
    let prepared = harness.staged_decode_failure(c_key.clone(), "C failed too").await.expect("roll");
    let r2 = harness.assert_rolling_replace_next(&prepared, &c_key, Some(&d_key));

    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));
}

/// Problem 2 regression: after both B and C have failed and D is
/// physically staged, a reload to [A, B, C, E] must skip both B and C
/// and choose E (preparing ReplaceNext(D -> E), never D -> B or D -> C).
#[tokio::test]
async fn multiple_excluded_branches_survive_reload() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C", "D"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d_key = harness.track_key(3);

    let prepared = harness.staged_decode_failure(b_key, "broken B").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &harness.track_key(1), Some(&c_key));
    assert!(harness.commit_realign(r1, &Ok(())).is_none());

    let prepared = harness.staged_decode_failure(c_key.clone(), "broken C").await.expect("roll");
    let r2 = harness.assert_rolling_replace_next(&prepared, &c_key, Some(&d_key));
    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));

    // Reload to [A, B, C, E]:
    let e = queued_song("E", 4);
    let e_key = StationController::track(e.clone()).key;
    let prepared = harness
        .reload(vec![harness.song(0), harness.song(1), harness.song(2), e], true)
        .await
        .unwrap()
        .expect("reload must prepare a roll toward E");
    let r3 = harness.assert_rolling_replace_next(&prepared, &d_key, Some(&e_key));

    assert!(harness.commit_realign(r3, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&e_key));
}

/// When B is already excluded and R2 (C -> E) is in flight, a DecodeFailed(C)
/// arrives while R2 is unresolved: R2 retains ownership (no second roll),
/// both B and C are remembered in exclusions, and expected C is marked
/// broken. On R2 failure, bounded recovery replaces C toward the next
/// non-excluded successor (skipping both B and C).
#[tokio::test]
async fn second_broken_branch_while_realign_is_in_flight() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C", "D"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d_key = harness.track_key(3);

    let prepared = harness.staged_decode_failure(b_key, "broken B").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &harness.track_key(1), Some(&c_key));
    assert!(harness.commit_realign(r1, &Ok(())).is_none());

    // Reload to [A, B, D] so R2 (C -> D) is in flight.
    let prepared = harness.reload_reordered(&["A", "B", "D"], true).await.unwrap().expect("C -> D");
    let r2 = harness.assert_rolling_replace_next(&prepared, &c_key, Some(&d_key));

    // While R2 (C -> D) is in flight, C emits DecodeFailed: absorbed.
    assert!(harness
        .staged_decode_failure(c_key.clone(), "C failed during realign")
        .await
        .is_none());
    harness.assert_pending_realign_id(Some(r2));

    // R2 FAILS: bounded retry from physical C toward D, skipping B and C.
    let (r3, followup) = harness
        .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
        .expect("failed roll on broken expected must produce retry roll");
    assert_ne!(r3, r2);
    harness.assert_planned_next_key(Some(&c_key));
    let r3_id = harness.assert_rolling_replace_next(&followup, &c_key, Some(&d_key));
    assert_eq!(r3, r3_id);

    assert!(harness.commit_realign(r3, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));
}

/// A transition to a new current identity (Handover) clears the decode
/// exclusions of the old current: a track excluded under current A is
/// eligible again as a successor under current C.
#[tokio::test]
async fn exclusions_clear_on_new_current_identity_after_handover() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let a = harness.song(0);
    let b = harness.song(1);
    let c = harness.song(2);
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);

    let prepared = harness.staged_decode_failure(b_key.clone(), "broken B").await.expect("roll");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));
    assert!(harness.commit_realign(r1, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&c_key));

    // Reload so B is placed after C: [A, C, B].
    assert!(harness.reload(vec![a, c, b], true).await.unwrap().is_none());

    // Handover to C occurs: Attach for B prepared under current C (exclusions from A cleared).
    let prepared = prepare_handover_attach(harness.controller_mut(), c_key.clone()).await;
    let r2 = harness.assert_rolling_attach(&prepared, &b_key);

    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&b_key));
}

/// When a physical ReplaceNext roll succeeds and the pipeline hands over
/// to the desired branch BEFORE its RealignResult completion is
/// processed, the controller must accept the Handover, commit the desired
/// song as current, supersede the in-flight realign, and ensure late
/// completions for that realign are inert.
#[tokio::test]
async fn replacenext_desired_hands_over_before_completion() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;
    let a = harness.song(0);
    let b_key = harness.track_key(1);
    let c = queued_song("C", 2);
    let c_key = StationController::track(c.clone()).key;
    let d = queued_song("D", 3);
    let d_key = StationController::track(d.clone()).key;

    // Reload to [A, C, D] prepares ReplaceNext(B -> C):
    let prepared = harness
        .reload(vec![a, c, d], true)
        .await
        .unwrap()
        .expect("reload must prepare B -> C");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));
    harness.assert_planned_next_key(Some(&b_key));
    harness.assert_pending_realign_id(Some(r1));

    // Deliver Handover(C) BEFORE commit_realign(r1, ...):
    let handover_op = harness
        .handle_event(PipelineEvent::Handover {
            generation: 1,
            current: c_key.clone(),
        })
        .await
        .expect("Handover of pending desired branch must be accepted")
        .expect("the handover must not fail");

    harness.assert_current_song_key(&c_key);
    harness.assert_no_staged_next();

    let r2 = harness.assert_rolling_attach(&handover_op, &d_key);
    assert_ne!(r2, r1);

    // Late R1 completions (Ok and Err) must be completely inert:
    assert!(harness.commit_realign(r1, &Ok(())).is_none(), "late R1 Ok must be inert");
    assert!(
        harness
            .commit_realign(r1, &Err(PipelineError::Pipeline("late err".into())))
            .is_none(),
        "late R1 Err must be inert"
    );
    harness.assert_pending_realign_id(Some(r2));

    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));
}

/// When an in-flight ReplaceNext becomes dirty via a Reload, but the
/// physically desired branch hands over before completion, the old dirty
/// alignment intent is discarded (it belonged to the old current identity)
/// and the new current identity derives its successor fresh.
#[tokio::test]
async fn dirty_replacenext_desired_hands_over_before_completion() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;
    let a = harness.song(0);
    let b_key = harness.track_key(1);
    let c = queued_song("C", 2);
    let c_key = StationController::track(c.clone()).key;
    let d = queued_song("D", 3);
    let e = queued_song("E", 4);
    let e_key = StationController::track(e.clone()).key;

    let prepared = harness
        .reload(vec![a.clone(), c.clone(), d.clone()], true)
        .await
        .unwrap()
        .expect("reload prepare");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    // Reload while R1 in flight: [A, E, D] -> dirty.
    assert!(harness.reload(vec![a, e, d], true).await.unwrap().is_none());

    // Handover(C) arrives before committing R1:
    let handover_op = harness
        .handle_event(PipelineEvent::Handover {
            generation: 1,
            current: c_key.clone(),
        })
        .await
        .expect("Handover of C must be accepted")
        .expect("must not fail");

    harness.assert_current_song_key(&c_key);
    harness.assert_no_staged_next();

    let r2 = harness.assert_rolling_attach(&handover_op, &e_key);
    assert_ne!(r2, r1);

    assert!(harness.commit_realign(r1, &Ok(())).is_none(), "late R1 Ok must be inert");
    assert!(
        harness
            .commit_realign(r1, &Err(PipelineError::Pipeline("late err".into())))
            .is_none(),
        "late R1 Err must be inert"
    );
    harness.assert_pending_realign_id(Some(r2));

    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&e_key));
}

/// When a post-Handover Attach physically succeeds and hands over before
/// its RealignResult is processed, the Handover must be accepted, the
/// previous attach realign superseded, and late completions made inert.
#[tokio::test]
async fn post_handover_attach_desired_hands_over_before_completion() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C", "D"]).await;
    let b_key = harness.track_key(1);
    let c_key = harness.track_key(2);
    let d_key = harness.track_key(3);

    // Handover(B) arrives -> current B, staged None, returns Attach(C) with R1:
    let handover_op = harness
        .handle_event(PipelineEvent::Handover {
            generation: 1,
            current: b_key.clone(),
        })
        .await
        .unwrap()
        .unwrap();
    let r1 = harness.assert_rolling_attach(&handover_op, &c_key);
    harness.assert_no_staged_next();
    harness.assert_pending_realign_id(Some(r1));

    // Deliver Handover(C) BEFORE commit_realign(r1, Ok):
    let handover_c = harness
        .handle_event(PipelineEvent::Handover {
            generation: 1,
            current: c_key.clone(),
        })
        .await
        .expect("Handover of C must be accepted")
        .expect("must not fail");

    harness.assert_current_song_key(&c_key);
    assert_ne!(harness.controller.pending_realign(), Some(r1));
    let r2 = harness.assert_rolling_attach(&handover_c, &d_key);
    assert_ne!(r2, r1);

    assert!(harness.commit_realign(r1, &Ok(())).is_none());
    harness.assert_pending_realign_id(Some(r2));
    assert!(harness.commit_realign(r2, &Ok(())).is_none());
    harness.assert_planned_next_key(Some(&d_key));
}

/// A Handover for an unrelated track or a stale generation must be
/// rejected without mutating current, planned_next, or pending realign state.
#[tokio::test]
async fn invalid_or_stale_handover_is_rejected() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;
    let a = harness.song(0);
    let a_key = harness.track_key(0);
    let b_key = harness.track_key(1);
    let c = queued_song("C", 2);
    let c_key = StationController::track(c.clone()).key;
    let z = queued_song("Z", 99);
    let z_key = StationController::track(z).key;

    let prepared = harness.reload(vec![a, c], true).await.unwrap().expect("reload prepare");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    // Unrelated track Z:
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::Handover {
                generation: 1,
                current: z_key,
            },
            "unrelated handover",
        )
        .await;

    // Stale generation:
    harness
        .assert_stale_event_is_inert(
            PipelineEvent::Handover {
                generation: 99,
                current: b_key.clone(),
            },
            "stale generation handover",
        )
        .await;

    harness.assert_current_song_key(&a_key);
    harness.assert_planned_next_key(Some(&b_key));
    harness.assert_pending_realign_id(Some(r1));
}

/// When a realign is in flight (e.g. ReplaceNext B -> C), but the old
/// expected branch B hands over (e.g. roll failed or old branch won the
/// race), B must be accepted as current and supersede R1.
#[tokio::test]
async fn old_expected_branch_handover_is_accepted_while_realign_unresolved() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B"]).await;
    let a = harness.song(0);
    let b_key = harness.track_key(1);
    let c = queued_song("C", 2);
    let c_key = StationController::track(c.clone()).key;
    let d = queued_song("D", 3);

    let prepared = harness.reload(vec![a, c, d], true).await.unwrap().expect("reload prepare");
    let r1 = harness.assert_rolling_replace_next(&prepared, &b_key, Some(&c_key));

    // Old expected branch B hands over:
    let _ = harness
        .handle_event(PipelineEvent::Handover {
            generation: 1,
            current: b_key.clone(),
        })
        .await
        .expect("old expected branch B must be accepted")
        .expect("must not fail");

    harness.assert_current_song_key(&b_key);
    assert_ne!(harness.controller.pending_realign(), Some(r1));
    assert!(harness.commit_realign(r1, &Ok(())).is_none());
}

/// Runtime ordering regression: at runtime, when ReplaceNext(B -> C)
/// is executed, Handover(C) is delivered to the runtime before the
/// RealignResult(Ok) command is processed. The runtime accepts Handover(C),
/// updates DB cursor to C, attaches D, and late RealignResult is inert.
#[tokio::test]
async fn replacenext_desired_hands_over_at_runtime_before_realign_completion() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B"]);
        let c = queued_song("C", 1);
        let d = queued_song("D", 2);
        let roll_gate = testsupport::Gate::new();
        let pipeline = Arc::new(RecordingPipeline::with_roll_gate(roll_gate.clone()));
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let (runtime, events) = harness.into_runtime();
        let b_key = StationController::track(songs[1].clone()).key;
        let c_key = StationController::track(c.clone()).key;
        let d_key = StationController::track(d.clone()).key;

        runtime.play().await.unwrap();

        // Replace B with C in persisted DB queue and insert D:
        replace_persisted_successor(&db.pool, station_id, &b_key, &c).await;
        sqlx::query(
            "INSERT INTO songs (id, title, artist, duration, file_path, uploaded_by)
             VALUES ($1, $2, 'skip-regression', 1, '/tmp/skip-regression', $3)",
        )
        .bind(d.song_id)
        .bind(&d.title)
        .bind(station_id)
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO station_queue (id, station_id, song_id, position) VALUES ($1, $2, $3, $4)")
            .bind(d.queue_item_id)
            .bind(station_id)
            .bind(d.song_id)
            .bind(d.position)
            .execute(&db.pool)
            .await
            .unwrap();
        let _ = runtime.status().await.expect("the status probe must answer");
        runtime
            .reload(vec![songs[0].clone(), c.clone(), d.clone()], true)
            .await
            .expect("reload must apply");

        // R1 enters RecordingPipeline::roll() and is HELD inside the gate:
        roll_gate.wait_started().await;
        assert_eq!(pipeline.count(Call::Roll), 1);
        match pipeline.rolls()[0].change.clone() {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, b_key);
                assert_eq!(replacement.expect("C is desired").track.key, c_key);
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }

        // While R1 is still held (roll() has NOT returned and RealignResult
        // physically cannot have been produced), deliver Handover(C):
        events
            .send(PipelineEvent::Handover {
                generation: 1,
                current: c_key.clone(),
            })
            .unwrap();

        // Handover(C) commits while R1 is still held: DB cursor advances to C:
        wait_for_db_cursor(&db.pool, station_id, Some(c.queue_item_id)).await;

        // Now release R1 so its completion can be delivered and the
        // post-handover Attach(D) can execute:
        roll_gate.release();
        // Release permit for the post-handover Attach roll:
        roll_gate.release();

        // Post-handover Attach for D runs:
        testsupport::wait_for("the post-handover attach roll to run", || pipeline.count(Call::Roll) == 2).await;
        match pipeline.rolls()[1].change.clone() {
            RollingChange::Attach(next) => {
                assert_eq!(next.track.key, d_key);
            }
            other => panic!("expected Attach, got {other:?}"),
        }

        runtime.shutdown().await.unwrap();
    })
    .await;
}

/// End-to-end: a failed post-handover Attach claims nothing at runtime
/// (the handover still commits; the successor is not claimed), and a
/// later reload re-attaches the NEWEST successor with a fresh
/// correlated roll. The reload's successor differs from the failed
/// attach's target, so the test is order-insensitive: whether the
/// failed attach's completion is processed before or after the reload,
/// the recovery roll is issued exactly once.
#[tokio::test]
async fn failed_handover_attach_at_runtime_is_recovered_by_a_reload() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "X"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone());
        let station_id = harness.controller.station_id;
        seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
        let (runtime, events) = harness.into_runtime();
        let b_key = StationController::track(songs[1].clone()).key;
        let x_key = StationController::track(songs[2].clone()).key;
        let y = queued_song("Y", 2);
        let y_key = StationController::track(y.clone()).key;

        runtime.play().await.unwrap();
        // The handover's attach roll (X) will FAIL.
        pipeline.fail_once(Call::Roll);
        events
            .send(PipelineEvent::Handover {
                generation: 1,
                current: b_key,
            })
            .unwrap();
        testsupport::wait_for("the handover's attach roll to run and fail", || pipeline.count(Call::Roll) == 1).await;
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        match pipeline.rolls()[0].change.clone() {
            RollingChange::Attach(next) => {
                assert_eq!(next.track.key, x_key, "the handover must attach the queue successor");
            }
            other => panic!("expected an Attach, got {other:?}"),
        }
        // The failed attach claimed nothing: the cursor is exactly the
        // handover's commit, and X is not staged.

        // The queue moves on to Y (persisted for the final handover) and
        // a reload reconciles the orphaned successor: whether the failed
        // attach's completion or the reload is processed first, the
        // recovery roll targets the NEWEST successor.
        replace_persisted_successor(&db.pool, station_id, &x_key, &y).await;
        let _ = runtime.status().await.expect("the status probe must answer");
        runtime
            .reload(vec![songs[0].clone(), songs[1].clone(), y.clone()], true)
            .await
            .expect("the recovery reload must apply");
        testsupport::wait_for("the recovery attach to reach the pipeline", || pipeline.count(Call::Roll) == 2).await;
        match pipeline.rolls()[1].change.clone() {
            RollingChange::Attach(next) => {
                assert_eq!(next.track.key, y_key, "the recovery must attach the newest queue successor");
            }
            other => panic!("expected an Attach, got {other:?}"),
        }
        // The recovery attach succeeded: a handover of Y is accepted and
        // commits for real.
        wait_for_db_cursor(&db.pool, station_id, Some(songs[1].queue_item_id)).await;
        let _ = runtime.status().await.expect("the status probe must answer");
        events
            .send(PipelineEvent::Handover {
                generation: 1,
                current: y_key,
            })
            .unwrap();
        wait_for_db_cursor(&db.pool, station_id, Some(y.queue_item_id)).await;
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn manual_pause_ends_the_auto_idle_state() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

    let operation = controller.skip().await.unwrap();
    assert!(matches!(operation.operation, PipelineOperation::Stop));
    assert!(controller.idle());

    controller.queue.reload_songs(vec![fresh], false);
    let (_operation, attempt_a) = controller
        .resume_from_idle()
        .await
        .expect("an idle station must resume once the queue fills");
    assert!(controller.idle());

    let operation = controller.pause();
    assert!(matches!(operation, PipelineOperation::SetPlaying(false)));
    assert_eq!(controller.state, PipelineState::Paused);
    assert!(!controller.idle(), "a manual pause must end the auto-idle state");

    controller.on_resume_result(attempt_a, Ok(()));
    assert_eq!(controller.state, PipelineState::Paused);
    assert!(
        controller.resume_from_idle().await.is_none(),
        "a paused station must never auto-resume"
    );
    assert!(controller.resume_from_idle().await.is_none());
    assert!(controller.resume_from_idle().await.is_none());
}

#[tokio::test]
async fn paused_station_never_auto_resumes_on_later_ticks() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    let harness = Harness::with_pipeline(pipeline.clone(), vec![song.clone()]);
    let queue = harness.controller.queue.clone();
    let (runtime, events) = harness.into_runtime();
    let playing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    let gate = pipeline.replace_gate().expect("gated pipeline");
    gate.wait_started().await;
    gate.release();
    playing.await.unwrap().unwrap();
    events
        .send(PipelineEvent::CurrentEos {
            generation: 1,
            current: StationController::track(song).key,
        })
        .unwrap();
    testsupport::wait_for("the exhausted station to stop", || {
        pipeline.count(Call::Replace) >= 1 && pipeline.calls().contains(&Call::Stop)
    })
    .await;
    assert_eq!(pipeline.count(Call::Replace), 1);

    queue.reload_songs(vec![fresh], false);
    testsupport::wait_for("the resume replace to reach the pipeline", || pipeline.count(Call::Replace) >= 2).await;

    // The user pauses while the resume is in flight; the pause command
    // is queued before the release, so the controller processes it (and
    // ends the auto-idle state) before the stale completion arrives.
    let pausing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.pause().await })
    };
    // The resume replace is blocked inside the gate; arm the release for
    // exactly this operation (a release can never be lost, but this keeps
    // the pause strictly queued ahead of the release).
    gate.wait_started().await;
    gate.release();
    pausing.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        pipeline.count(Call::Replace),
        2,
        "idle ticks after a manual pause must not start another resume"
    );
    assert_eq!(pipeline.count(Call::SetPlaying), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_runtime_auto_starts_when_content_arrives_without_an_api_command() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let pipeline = Arc::new(RecordingPipeline::new());
    let harness = Harness::with_pipeline(pipeline.clone(), vec![song.clone()]);
    let queue = harness.controller.queue.clone();
    let (runtime, events) = harness.into_runtime();
    runtime.play().await.unwrap();
    events
        .send(PipelineEvent::CurrentEos {
            generation: 1,
            current: StationController::track(song).key,
        })
        .unwrap();
    // With no database reachable the exhaustion refill fails and is
    // retried (bounded, ~750ms of backoff) before the controller stops.
    testsupport::wait_for("the exhausted station to stop", || pipeline.count(Call::Stop) > 0).await;
    assert_eq!(pipeline.count(Call::Replace), 1);

    // Content arrives in the queue (AutoDJ / schedule fill writing rows),
    // with NO API command: only the runtime's idle tick polls. The next
    // tick must replace the plan and start playback.
    queue.reload_songs(vec![fresh], false);
    tokio::time::timeout(Duration::from_secs(4), async {
        while pipeline.count(Call::Replace) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the idle runtime must start playback once the queue fills");
    assert_eq!(pipeline.count(Call::Replace), 2);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_reports_a_real_stopped_pipeline_as_ok() {
    let song = queued_song("A", 0);
    let (mut controller, _) = Harness::playing(vec![song]).await.into_parts();
    controller.stop();
    let status = controller.status().await.expect("a working pipeline must report status");
    let StatusEvent::State { playing, elapsed, .. } = status else {
        panic!("status must be a state event");
    };
    assert!(!playing, "a stopped pipeline reports playing=false");
    assert_eq!(elapsed, 0);
}

#[tokio::test]
async fn status_propagates_a_snapshot_pipeline_error() {
    let pipeline = Arc::new(RecordingPipeline::new());
    pipeline.fail(Call::Snapshot);
    let harness = Harness::with_pipeline(pipeline, Vec::new());
    let controller = harness.controller;
    // A failed Snapshot must surface as a pipeline error — never as a
    // legal `Stopped` status that monitoring would mistake for a healthy
    // stopped station.
    let result = controller.status().await;
    let Err(PipelineError::Pipeline(message)) = result else {
        panic!("a failed snapshot must propagate as a pipeline error, got {result:?}");
    };
    assert!(message.contains("injected failure"), "unexpected error message: {message}");
}

#[tokio::test]
async fn shutdown_runs_through_the_executor_and_discards_pending_operations() {
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    let harness = Harness::with_pipeline(pipeline.clone(), queued_songs(&["A", "B"]));
    let (runtime, _events) = harness.into_runtime();
    let playing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    let gate = pipeline.replace_gate().expect("gated pipeline");
    // The first replace blocks the executor; a pause and the shutdown
    // queue behind it. Pause is a quick synchronous command (no DB), so
    // the runtime finishes submitting both while the executor is still
    // blocked inside the first replace.
    gate.wait_started().await;
    let pausing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.pause().await })
    };
    let shutting_down = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown().await })
    };
    // Let the runtime drain its command queue (play's refill/push spend
    // ~40ms on the unreachable DB before pause + shutdown are submitted).
    // The executor is still blocked inside the first replace, so nothing
    // else can run in the meantime.
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Release the first replace: the executor must pick the shutdown
    // barrier next (urgent lane), discard the pending pause, stop, and go
    // terminal — nothing may run after the stop.
    gate.release();
    let play_result = tokio::time::timeout(Duration::from_secs(5), playing)
        .await
        .expect("play must complete")
        .unwrap();
    assert!(play_result.is_ok(), "play must succeed: {play_result:?}");
    let pause_result = tokio::time::timeout(Duration::from_secs(5), pausing)
        .await
        .expect("pause must complete")
        .unwrap();
    assert!(
        pause_result.is_err(),
        "a pending pause must be refused once the shutdown barrier lands (calls: {:?})",
        pipeline.calls()
    );
    let shutdown_result = tokio::time::timeout(Duration::from_secs(5), shutting_down)
        .await
        .expect("shutdown must complete")
        .unwrap();
    assert!(shutdown_result.is_ok(), "shutdown must succeed: {shutdown_result:?}");
    assert_eq!(pipeline.calls(), [Call::Replace, Call::Stop]);
    assert!(runtime.play().await.is_err());
}

#[tokio::test]
async fn idle_runtime_queues_only_one_resume_replace_while_one_is_in_flight() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    let harness = Harness::with_pipeline(pipeline.clone(), vec![song.clone()]);
    let queue = harness.controller.queue.clone();
    let (runtime, events) = harness.into_runtime();
    let playing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    let gate = pipeline.replace_gate().expect("gated pipeline");
    // The initial replace blocks inside the pipeline; release it so the
    // exhaustion stop below can run through the free executor.
    gate.wait_started().await;
    gate.release();
    playing.await.unwrap().unwrap();
    events
        .send(PipelineEvent::CurrentEos {
            generation: 1,
            current: StationController::track(song).key,
        })
        .unwrap();
    testsupport::wait_for("the exhausted station to stop", || {
        pipeline.count(Call::Replace) >= 1 && pipeline.calls().contains(&Call::Stop)
    })
    .await;
    assert_eq!(pipeline.count(Call::Replace), 1);
    assert_eq!(pipeline.count(Call::Stop), 1);

    // Content arrives; the first idle tick starts a resume replace that
    // blocks inside the pipeline.
    queue.reload_songs(vec![fresh], false);
    testsupport::wait_for("the first resume replace to reach the pipeline", || {
        pipeline.count(Call::Replace) >= 2
    })
    .await;

    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        pipeline.count(Call::Replace),
        2,
        "idle ticks must not queue a second resume while one is in flight"
    );

    gate.wait_started().await;
    gate.release();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(pipeline.count(Call::Replace), 2);
    assert_eq!(pipeline.count(Call::Stop), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_runtime_retries_a_failed_auto_resume_on_the_next_tick() {
    let song = queued_song("A", 0);
    let fresh = queued_song("B", 1);
    let pipeline = Arc::new(RecordingPipeline::new());
    // The second replace (the first automatic resume) fails; the third
    // succeeds.
    pipeline.fail_nth(Call::Replace, 1);
    let harness = Harness::with_pipeline(pipeline.clone(), vec![song.clone()]);
    let queue = harness.controller.queue.clone();
    let (runtime, events) = harness.into_runtime();
    runtime.play().await.unwrap();
    events
        .send(PipelineEvent::CurrentEos {
            generation: 1,
            current: StationController::track(song).key,
        })
        .unwrap();
    // The queue exhausts (DB unreachable, bounded fill retries ~750ms):
    // the station becomes idle.
    testsupport::wait_for("the exhausted station to stop", || pipeline.count(Call::Stop) > 0).await;
    assert_eq!(pipeline.count(Call::Replace), 1);

    queue.reload_songs(vec![fresh], false);
    tokio::time::timeout(Duration::from_secs(4), async {
        while pipeline.count(Call::Replace) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first auto-resume attempt must run");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(pipeline.count(Call::Replace), 2, "a failed resume must not be retried eagerly");

    tokio::time::timeout(Duration::from_secs(4), async {
        while pipeline.count(Call::Replace) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the idle tick must retry the failed resume");
    assert_eq!(pipeline.count(Call::Replace), 3);
    assert_eq!(pipeline.count(Call::Stop), 1);
    runtime.shutdown().await.unwrap();
}

/// Fault-injection regression: when a manual `Play` from `Stopped` fails
/// its initial Replace in the pipeline executor, the runtime reports `Err`,
/// the controller logically remains `Stopped` (never falsely committed to
/// `Playing`), and a subsequent `Play` retries `InitialReplaceFromStopped`
/// (second Replace) rather than issuing a no-op `SetPlaying(true)`.
#[tokio::test]
async fn failed_initial_play_leaves_runtime_stopped_and_retries_initial_replace() {
    let songs = queued_songs(&["A", "B"]);
    let pipeline = Arc::new(RecordingPipeline::new());
    // First Replace attempt fails in the pipeline:
    pipeline.fail_nth(Call::Replace, 0);

    let harness = Harness::with_pipeline(pipeline.clone(), songs);
    let (runtime, _events) = harness.into_runtime();

    // 1. Initial play from Stopped fails:
    let first_play = runtime.play().await;
    assert!(first_play.is_err(), "first play must return an error from the failed Replace");

    // 2. Controller logically remains Stopped:
    let status = runtime.status().await.expect("status probe must succeed");
    assert!(
        matches!(status, StatusEvent::State { playing: false, .. }),
        "controller must report stopped/not playing after failed initial play"
    );
    assert_eq!(pipeline.count(Call::Replace), 1, "exactly one Replace was attempted");
    assert_eq!(pipeline.count(Call::SetPlaying), 0, "no SetPlaying was called");

    // 3. Second play from Stopped succeeds:
    let second_play = runtime.play().await;
    assert!(second_play.is_ok(), "second play must succeed: {second_play:?}");

    // 4. Exactly 2 Replaces were executed (the second play issued InitialReplaceFromStopped):
    assert_eq!(
        pipeline.count(Call::Replace),
        2,
        "second play must issue a second Replace (not SetPlaying)"
    );
    assert_eq!(
        pipeline.count(Call::SetPlaying),
        0,
        "SetPlaying must not be called when starting from Stopped"
    );

    // 5. Controller is now Playing:
    let status = runtime.status().await.expect("status probe must succeed");
    assert!(
        matches!(status, StatusEvent::State { playing: true, .. }),
        "controller must report playing after successful second play"
    );

    runtime.shutdown().await.unwrap();
}

/// Unit test: manual play from Stopped produces an initial play attempt,
/// keeps Stopped until committed, rolls back on error, and stale completions
/// cannot touch the controller state.
#[tokio::test]
async fn initial_play_attempt_correlation_and_error_rollback() {
    let mut harness = ControllerScenario::stopped().with_queue(&["A", "B"]).build().await;
    harness.assert_state(PipelineState::Stopped);

    // 1. Prepare initial play:
    let prepared = harness.play().await.expect("initial play prepare");
    let PipelineOperation::Replace(plan) = prepared.operation else {
        panic!("initial play from stopped must produce a Replace");
    };
    assert!(matches!(plan.mode, ReplaceMode::InitialReplaceFromStopped));
    let attempt_1 = prepared.play_attempt_id.expect("attempt id must be present");
    assert_eq!(harness.controller.pending_play(), Some(attempt_1));
    harness.assert_state(PipelineState::Stopped);

    // 2. Commit error for attempt 1:
    let applied = harness.commit_play(attempt_1, &Err(PipelineError::Pipeline("pipeline failed".into())));
    assert!(applied);
    harness.assert_state(PipelineState::Stopped);
    assert_eq!(harness.controller.pending_play(), None);

    // 3. Second play prepares attempt 2:
    let prepared_2 = harness.play().await.expect("second play prepare");
    let attempt_2 = prepared_2.play_attempt_id.expect("second attempt id");
    assert_ne!(attempt_2, attempt_1);
    assert_eq!(harness.controller.pending_play(), Some(attempt_2));
    harness.assert_state(PipelineState::Stopped);

    // 4. Stale completion for attempt 1 is inert:
    assert!(!harness.commit_play(attempt_1, &Ok(())));
    assert_eq!(harness.controller.pending_play(), Some(attempt_2));
    harness.assert_state(PipelineState::Stopped);

    // 5. Success for attempt 2 commits Playing:
    assert!(harness.commit_play(attempt_2, &Ok(())));
    harness.assert_state(PipelineState::Playing);
    assert_eq!(harness.controller.pending_play(), None);

    // 6. Play from Playing returns SetPlaying(true) without play_attempt_id:
    let prepared_3 = harness.play().await.expect("third play prepare");
    assert!(matches!(prepared_3.operation, PipelineOperation::SetPlaying(true)));
    assert_eq!(prepared_3.play_attempt_id, None);
    harness.assert_state(PipelineState::Playing);
}

/// A manual pause or stop while an initial play replace is in flight clears
/// the pending attempt so the delayed executor completion cannot overwrite
/// the user's decision.
#[tokio::test]
async fn stale_initial_play_completion_does_not_override_manual_pause_or_stop() {
    let mut harness = ControllerScenario::stopped().with_queue(&["A", "B"]).build().await;

    // Prepare play:
    let prepared = harness.play().await.expect("play prepare");
    let attempt = prepared.play_attempt_id.expect("attempt id");
    harness.assert_pending_play_id(Some(attempt));

    // User pauses before replace finishes:
    harness.pause();
    harness.assert_state(PipelineState::Paused);
    harness.assert_pending_play_id(None);

    // Delayed success of initial play arrives:
    let before_paused = harness.snapshot();
    assert!(!harness.commit_play(attempt, &Ok(())));
    harness.assert_snapshot_unchanged(&before_paused, "delayed initial play completion after pause");

    // User stops:
    harness.stop();
    harness.assert_state(PipelineState::Stopped);
    let before_stopped = harness.snapshot();
    assert!(!harness.commit_play(attempt, &Ok(())));
    harness.assert_snapshot_unchanged(&before_stopped, "delayed initial play completion after stop");
}

struct BlockedSkipFromStopped {
    runtime: StationRuntime,
    pipeline: Arc<RecordingPipeline>,
    gate: Arc<Gate>,
    skipping: oneshot::Receiver<Result<(), PipelineError>>,
    _events: mpsc::UnboundedSender<PipelineEvent>,
}

async fn start_blocked_skip_from_stopped(songs: Vec<SongInfo>) -> BlockedSkipFromStopped {
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    let harness = Harness::with_pipeline(pipeline.clone(), songs);
    let gate = pipeline.replace_gate().expect("gated pipeline");
    let (runtime, _events) = harness.into_runtime();
    let skipping = runtime
        .submit_and_wait_admitted(StationCommand::Skip)
        .await
        .expect("skip admission must succeed");
    gate.wait_started().await;
    assert_eq!(pipeline.count(Call::Replace), 1);
    BlockedSkipFromStopped {
        runtime,
        pipeline,
        gate,
        skipping,
        _events,
    }
}

struct BlockedSkipResolvingFailedPlay {
    runtime: StationRuntime,
    pipeline: Arc<RecordingPipeline>,
    gate: Arc<Gate>,
    playing: tokio::task::JoinHandle<Result<(), PipelineError>>,
    skipping: oneshot::Receiver<Result<(), PipelineError>>,
    _events: mpsc::UnboundedSender<PipelineEvent>,
}

async fn start_blocked_skip_resolving_failed_play(songs: Vec<SongInfo>, fail_skip: bool) -> BlockedSkipResolvingFailedPlay {
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    // First Replace (P1) will fail in pipeline:
    pipeline.fail_nth(Call::Replace, 0);
    if fail_skip {
        pipeline.fail_nth(Call::Replace, 1);
    }

    let harness = Harness::with_pipeline(pipeline.clone(), songs);
    let gate = pipeline.replace_gate().expect("gated pipeline");
    let (runtime, _events) = harness.into_runtime();

    let playing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    gate.wait_started().await;
    assert_eq!(pipeline.count(Call::Replace), 1);

    // Skip is admitted and prepared against in-flight Play P1:
    let skipping = runtime
        .submit_and_wait_admitted(StationCommand::Skip)
        .await
        .expect("skip admission");

    BlockedSkipResolvingFailedPlay {
        runtime,
        pipeline,
        gate,
        playing,
        skipping,
        _events,
    }
}

struct BlockedOutOfOrderSkipResolvingPlay {
    runtime: StationRuntime,
    pipeline: Arc<RecordingPipeline>,
    gate: Arc<Gate>,
    play_result_gate: Arc<Gate>,
    playing: tokio::task::JoinHandle<Result<(), PipelineError>>,
    skipping: oneshot::Receiver<Result<(), PipelineError>>,
    _events: mpsc::UnboundedSender<PipelineEvent>,
}

async fn start_blocked_out_of_order_skip_resolving_play(songs: Vec<SongInfo>, fail_play: bool) -> BlockedOutOfOrderSkipResolvingPlay {
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    if fail_play {
        pipeline.fail_nth(Call::Replace, 0);
    }

    let harness = Harness::with_pipeline(pipeline.clone(), songs);
    let gate = pipeline.replace_gate().expect("gated pipeline");
    let (runtime, _events) = harness.into_runtime();

    let play_result_gate = Gate::new();
    runtime.set_play_result_gate(play_result_gate.clone());

    let playing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    gate.wait_started().await;
    assert_eq!(pipeline.count(Call::Replace), 1);

    let skipping = runtime
        .submit_and_wait_admitted(StationCommand::Skip)
        .await
        .expect("skip admission");

    BlockedOutOfOrderSkipResolvingPlay {
        runtime,
        pipeline,
        gate,
        play_result_gate,
        playing,
        skipping,
        _events,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockedPlayFollowup {
    Pause,
    Skip { fail_skip: bool },
    SkipThenPause,
    OverlappingPlay,
    SkipThenOverlappingPlay,
}

#[derive(Clone, Debug)]
struct BlockedInitialPlayCase<'a> {
    name: &'static str,
    queue: &'a [&'a str],
    followup: BlockedPlayFollowup,
    expected_playing: bool,
    expected_title: &'a str,
    expected_replaces: usize,
    expected_pipeline_state: Option<PipelineState>,
    expected_set_playing: Option<usize>,
}

async fn run_blocked_initial_play_case(case: BlockedInitialPlayCase<'_>) {
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    if let BlockedPlayFollowup::Skip { fail_skip: true } = case.followup {
        pipeline.fail_nth(Call::Replace, 1);
    }
    let harness = Harness::with_pipeline(pipeline.clone(), queued_songs(case.queue));
    let gate = pipeline.replace_gate().expect("gated pipeline");
    let (runtime, _events) = harness.into_runtime();

    let playing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    gate.wait_started().await;
    assert_eq!(pipeline.count(Call::Replace), 1, "{}: initial Replace in gate", case.name);

    match case.followup {
        BlockedPlayFollowup::Pause => {
            let mut pausing = runtime
                .submit_and_wait_admitted(StationCommand::Pause)
                .await
                .expect("pause admission");
            assert!(matches!(pausing.try_recv(), Err(OneshotTryRecvError::Empty)));
            gate.release();
            assert!(playing.await.unwrap().is_ok(), "{}: play ok", case.name);
            assert!(pausing.await.unwrap().is_ok(), "{}: pause ok", case.name);
        }
        BlockedPlayFollowup::Skip { fail_skip } => {
            let mut skipping = runtime
                .submit_and_wait_admitted(StationCommand::Skip)
                .await
                .expect("skip admission");
            assert!(matches!(skipping.try_recv(), Err(OneshotTryRecvError::Empty)));
            gate.release();
            gate.wait_started().await;
            assert_eq!(pipeline.count(Call::Replace), 2, "{}: skip replace in gate", case.name);
            assert!(playing.await.unwrap().is_ok(), "{}: play ok", case.name);
            gate.release();
            let skip_res = skipping.await.unwrap();
            if fail_skip {
                assert!(skip_res.is_err(), "{}: skip failed as injected", case.name);
            } else {
                assert!(skip_res.is_ok(), "{}: skip ok", case.name);
            }
        }
        BlockedPlayFollowup::SkipThenPause => {
            let mut skipping = runtime
                .submit_and_wait_admitted(StationCommand::Skip)
                .await
                .expect("skip admission");
            assert!(matches!(skipping.try_recv(), Err(OneshotTryRecvError::Empty)));
            let mut pausing = runtime
                .submit_and_wait_admitted(StationCommand::Pause)
                .await
                .expect("pause admission");
            assert!(matches!(pausing.try_recv(), Err(OneshotTryRecvError::Empty)));
            gate.release();
            gate.wait_started().await;
            assert_eq!(pipeline.count(Call::Replace), 2, "{}: skip replace in gate", case.name);
            gate.release();
            assert!(playing.await.unwrap().is_ok(), "{}: play ok", case.name);
            assert!(skipping.await.unwrap().is_ok(), "{}: skip ok", case.name);
            assert!(pausing.await.unwrap().is_ok(), "{}: pause ok", case.name);
        }
        BlockedPlayFollowup::OverlappingPlay => {
            let mut second_play = runtime
                .submit_and_wait_admitted(StationCommand::Play)
                .await
                .expect("second play admission");
            let second_result = second_play.try_recv().expect("second play answered immediately");
            assert!(second_result.is_err(), "{}: second play refused", case.name);
            assert_eq!(pipeline.count(Call::Replace), 1, "{}: no second replace", case.name);
            gate.release();
            assert!(playing.await.unwrap().is_ok(), "{}: first play ok", case.name);
        }
        BlockedPlayFollowup::SkipThenOverlappingPlay => {
            let mut skipping = runtime
                .submit_and_wait_admitted(StationCommand::Skip)
                .await
                .expect("skip admission");
            assert!(matches!(skipping.try_recv(), Err(OneshotTryRecvError::Empty)));
            let mut overlapping_play = runtime
                .submit_and_wait_admitted(StationCommand::Play)
                .await
                .expect("overlapping play admission");
            let play_err = overlapping_play.try_recv().expect("overlapping play answered immediately");
            assert!(play_err.is_err(), "{}: overlapping play refused", case.name);
            gate.release();
            gate.wait_started().await;
            assert_eq!(pipeline.count(Call::Replace), 2, "{}: skip replace in gate", case.name);
            gate.release();
            assert!(playing.await.unwrap().is_ok(), "{}: play ok", case.name);
            assert!(skipping.await.unwrap().is_ok(), "{}: skip ok", case.name);
        }
    }

    let status = runtime.status().await.expect("status probe");
    match status {
        StatusEvent::State { playing, title, .. } => {
            assert_eq!(playing, case.expected_playing, "{}: playing mismatch", case.name);
            assert_eq!(title, case.expected_title, "{}: title mismatch", case.name);
        }
        other => panic!("{}: expected State event, got {other:?}", case.name),
    }
    assert_eq!(
        pipeline.count(Call::Replace),
        case.expected_replaces,
        "{}: replace count mismatch",
        case.name
    );
    if let Some(expected_state) = case.expected_pipeline_state {
        assert_eq!(
            pipeline.snapshot_state(),
            expected_state,
            "{}: physical pipeline state mismatch",
            case.name
        );
    }
    if let Some(expected_set_playing) = case.expected_set_playing {
        assert_eq!(
            pipeline.count(Call::SetPlaying),
            expected_set_playing,
            "{}: SetPlaying count mismatch",
            case.name
        );
    }
    runtime.shutdown().await.unwrap();
}

async fn run_blocked_skip_resolving_failed_play(fail_skip: bool, name: &'static str) {
    let test = start_blocked_skip_resolving_failed_play(queued_songs(&["A", "B", "C"]), fail_skip).await;

    // 1. Release gate for P1 (P1 fails as injected):
    test.gate.release();
    let play_1_res = test.playing.await.unwrap();
    assert!(play_1_res.is_err(), "{}: first play must fail as injected", name);

    // Skip Replace starts and hits gate:
    test.gate.wait_started().await;
    assert_eq!(test.pipeline.count(Call::Replace), 2);

    // 2. User calls Play P2 while Skip is in flight -> refused:
    let mut second_play = test
        .runtime
        .submit_and_wait_admitted(StationCommand::Play)
        .await
        .expect("second play admission");
    let play_2_res = second_play.try_recv().expect("second play answered immediately");
    assert!(play_2_res.is_err(), "{}: play while skip is in flight must be refused", name);
    assert_eq!(test.pipeline.count(Call::Replace), 2, "{}: no third replace was queued", name);

    // 3. Release gate for Skip Replace:
    test.gate.release();
    let skip_res = test.skipping.await.unwrap();
    if fail_skip {
        assert!(skip_res.is_err(), "{}: skip must fail as injected", name);
    } else {
        assert!(skip_res.is_ok(), "{}: skip must succeed: {skip_res:?}", name);
    }

    // 4. Station is Stopped:
    assert_eq!(
        test.pipeline.snapshot_state(),
        PipelineState::Stopped,
        "{}: physical pipeline must remain Stopped",
        name
    );
    let status = test.runtime.status().await.expect("status probe");
    let expected_inter_title = if fail_skip { "A" } else { "B" };
    match status {
        StatusEvent::State { playing, title, .. } => {
            assert!(!playing, "{}: station must remain Stopped", name);
            assert_eq!(title, expected_inter_title, "{}: intermediate track title", name);
        }
        other => panic!("expected State event, got {other:?}"),
    }
    assert_eq!(test.pipeline.count(Call::Replace), 2);

    // 5. Subsequent Play P3 succeeds:
    let third_playing = {
        let runtime = test.runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    test.gate.wait_started().await;
    assert_eq!(test.pipeline.count(Call::Replace), 3);
    test.gate.release();

    let third_play = third_playing.await.unwrap();
    assert!(third_play.is_ok(), "{}: third play must succeed: {third_play:?}", name);
    assert_eq!(
        test.pipeline.snapshot_state(),
        PipelineState::Playing,
        "{}: physical pipeline must transition to Playing",
        name
    );
    let status = test.runtime.status().await.expect("status probe");
    match status {
        StatusEvent::State { playing, title, .. } => {
            assert!(playing, "{}: station must be Playing after retry", name);
            assert_eq!(title, expected_inter_title, "{}: final track title", name);
        }
        other => panic!("expected State event, got {other:?}"),
    }
    assert_eq!(test.pipeline.count(Call::Replace), 3);

    test.runtime.shutdown().await.unwrap();
}

async fn run_blocked_out_of_order_skip_resolving_play(fail_play: bool, name: &'static str) {
    let test = start_blocked_out_of_order_skip_resolving_play(queued_songs(&["A", "B", "C"]), fail_play).await;

    // 1. Release gate for P1 (P1 completes in pipeline and forwarder enters play_result_gate):
    test.gate.release();
    test.play_result_gate.wait_started().await;

    // 2. Skip Replace (for B) starts and hits gate:
    test.gate.wait_started().await;
    assert_eq!(test.pipeline.count(Call::Replace), 2);

    // 3. Release gate for Skip Replace (S succeeds in pipeline):
    test.gate.release();
    let skip_res = test.skipping.await.unwrap();
    assert!(skip_res.is_ok(), "{}: skip must succeed: {skip_res:?}", name);

    if fail_play {
        // 4. SkipResult was processed while PlayResult(Err) is still held:
        assert_eq!(test.pipeline.snapshot_state(), PipelineState::Stopped, "{}: pipeline Stopped", name);
        let status = test.runtime.status().await.expect("status probe");
        match status {
            StatusEvent::State { playing, title, .. } => {
                assert!(!playing, "{}: controller must be Stopped while PlayResult is withheld", name);
                assert_eq!(title, "B", "{}: track must advance to B", name);
            }
            other => panic!("expected State event, got {other:?}"),
        }

        // 5. Release play_result_gate: PlayResult(Err) is delivered to the controller:
        test.play_result_gate.release();
        let play_res = test.playing.await.unwrap();
        assert!(play_res.is_err(), "{}: play must fail as injected: {play_res:?}", name);
        assert_eq!(test.pipeline.snapshot_state(), PipelineState::Stopped);
        let status = test.runtime.status().await.expect("status probe");
        match status {
            StatusEvent::State { playing, title, .. } => {
                assert!(!playing, "{}: station must remain Stopped after delayed PlayResult(Err)", name);
                assert_eq!(title, "B", "{}: station must stay on track B", name);
            }
            other => panic!("expected State event, got {other:?}"),
        }
        assert_eq!(test.pipeline.count(Call::Replace), 2);

        // Subsequent Play P2 succeeds on track B via InitialReplaceFromStopped(B):
        test.runtime.clear_play_result_gate();
        let second_playing = {
            let runtime = test.runtime.clone();
            tokio::spawn(async move { runtime.play().await })
        };
        test.gate.wait_started().await;
        assert_eq!(test.pipeline.count(Call::Replace), 3);
        test.gate.release();

        let second_play = second_playing.await.unwrap();
        assert!(second_play.is_ok(), "{}: second play must succeed: {second_play:?}", name);
        assert_eq!(test.pipeline.snapshot_state(), PipelineState::Playing);
        let status = test.runtime.status().await.expect("status probe");
        match status {
            StatusEvent::State { playing, title, .. } => {
                assert!(playing, "{}: station must now be Playing", name);
                assert_eq!(title, "B", "{}: station must play track B", name);
            }
            other => panic!("expected State event, got {other:?}"),
        }
        assert_eq!(test.pipeline.count(Call::Replace), 3);
    } else {
        // 4. Now release play_result_gate: PlayResult(Ok) is delivered to the controller:
        test.play_result_gate.release();
        let play_res = test.playing.await.unwrap();
        assert!(play_res.is_ok(), "{}: play must succeed: {play_res:?}", name);
        assert_eq!(test.pipeline.snapshot_state(), PipelineState::Playing);
        let status = test.runtime.status().await.expect("status probe");
        match status {
            StatusEvent::State { playing, title, .. } => {
                assert!(playing, "{}: station must be Playing after both Play and Skip succeeded", name);
                assert_eq!(title, "B", "{}: station must play track B", name);
            }
            other => panic!("expected State event, got {other:?}"),
        }
        assert_eq!(test.pipeline.count(Call::Replace), 2);
    }
    test.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_play_superseded_by_pause_keeps_station_paused_after_replace_finishes() {
    run_blocked_initial_play_case(BlockedInitialPlayCase {
        name: "runtime play superseded by pause keeps station paused after replace finishes",
        queue: &["A", "B"],
        followup: BlockedPlayFollowup::Pause,
        expected_playing: false,
        expected_title: "A",
        expected_replaces: 1,
        expected_pipeline_state: None,
        expected_set_playing: Some(1),
    })
    .await;
}

#[tokio::test]
async fn initial_play_in_flight_followed_by_skip_commits_playing_successor() {
    run_blocked_initial_play_case(BlockedInitialPlayCase {
        name: "initial play in flight followed by skip commits playing successor",
        queue: &["A", "B", "C"],
        followup: BlockedPlayFollowup::Skip { fail_skip: false },
        expected_playing: true,
        expected_title: "B",
        expected_replaces: 2,
        expected_pipeline_state: None,
        expected_set_playing: None,
    })
    .await;
}

#[tokio::test]
async fn initial_play_in_flight_followed_by_failed_skip_keeps_station_playing_initial_track() {
    run_blocked_initial_play_case(BlockedInitialPlayCase {
        name: "initial play in flight followed by failed skip keeps station playing initial track",
        queue: &["A", "B", "C"],
        followup: BlockedPlayFollowup::Skip { fail_skip: true },
        expected_playing: true,
        expected_title: "A",
        expected_replaces: 2,
        expected_pipeline_state: None,
        expected_set_playing: None,
    })
    .await;
}

#[tokio::test]
async fn initial_play_in_flight_followed_by_skip_then_pause_remains_paused() {
    run_blocked_initial_play_case(BlockedInitialPlayCase {
        name: "initial play in flight followed by skip then pause remains paused",
        queue: &["A", "B", "C"],
        followup: BlockedPlayFollowup::SkipThenPause,
        expected_playing: false,
        expected_title: "B",
        expected_replaces: 2,
        expected_pipeline_state: None,
        expected_set_playing: None,
    })
    .await;
}

/// Unit test: manual play from Stopped interleaved with a skip attempt
/// commits both operations coherently without dropping state.
#[tokio::test]
async fn initial_play_attempt_interleaved_with_skip_commits_coherently() {
    let mut harness = ControllerScenario::stopped().with_queue(&["A", "B", "C"]).build().await;
    let b_key = harness.track_key(1);

    // 1. Prepare initial play:
    let prepared_play = harness.play().await.expect("play prepare");
    let play_id = prepared_play.play_attempt_id.expect("play attempt id");
    harness.assert_state(PipelineState::Stopped);
    harness.assert_pending_play_id(Some(play_id));

    // 2. Prepare skip while play is in flight:
    let prepared_skip = harness.skip().await.expect("skip must prepare");
    let skip_id = prepared_skip.attempt_id.expect("skip attempt id");
    harness.assert_pending_skip_id(Some(skip_id));
    harness.assert_pending_play_id(Some(play_id));

    // 3. Play commit succeeds:
    assert!(harness.commit_play(play_id, &Ok(())));
    harness.assert_state(PipelineState::Playing);

    // 4. Skip commit succeeds:
    let (applied, _followup) = harness.commit_skip(skip_id, &Ok(())).await;
    assert!(applied);
    harness.assert_state(PipelineState::Playing);
    harness.assert_generation(2);
    harness.assert_current_song_key(&b_key);
}

#[tokio::test]
async fn skip_while_paused_preserves_paused_state_and_advances_track() {
    let mut harness = ControllerHarness::playing_queue(&["A", "B", "C"]).await;
    let a_key = harness.track_key(0);
    let b_key = harness.track_key(1);
    harness.assert_state(PipelineState::Playing);
    harness.assert_current_song_key(&a_key);

    // 1. User pauses the station:
    harness.pause();
    harness.assert_state(PipelineState::Paused);

    // 2. User invokes skip while paused:
    let prepared = harness.skip().await.expect("skip must prepare successfully");
    let skip_id = prepared.attempt_id.expect("attempt id");
    harness.assert_state(PipelineState::Paused);

    // 3. Skip completes successfully:
    let (applied, _) = harness.commit_skip(skip_id, &Ok(())).await;
    assert!(applied);
    harness.assert_state(PipelineState::Paused);
    harness.assert_current_song_key(&b_key);
}

#[tokio::test]
async fn runtime_skip_while_paused_preserves_paused_state() {
    run_reconnect_test(async |db| {
        let songs = queued_songs(&["A", "B", "C"]);
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), songs);
        let (runtime, _events) = harness.into_runtime();

        runtime.play().await.expect("play must succeed");
        runtime.pause().await.expect("pause must succeed");

        let status = runtime.status().await.expect("status probe");
        match status {
            StatusEvent::State { playing, title, .. } => {
                assert!(!playing, "station must be paused initially");
                assert_eq!(title, "A", "station must have track A");
            }
            other => panic!("expected State event, got {other:?}"),
        }

        runtime.skip().await.expect("skip while paused must succeed");

        let status = runtime.status().await.expect("status probe");
        match status {
            StatusEvent::State { playing, title, .. } => {
                assert!(!playing, "station must remain paused after skip");
                assert_eq!(title, "B", "station must advance to track B");
            }
            other => panic!("expected State event, got {other:?}"),
        }

        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn initial_play_in_flight_followed_by_skip_then_stop_remains_stopped() {
    let mut harness = ControllerScenario::stopped().with_queue(&["A", "B", "C"]).build().await;
    let b_key = harness.track_key(1);

    // 1. Prepare initial Play (attempt 1):
    let play_prep = harness.play().await.expect("play prepare");
    let play_id = play_prep.play_attempt_id.expect("play attempt id");
    harness.assert_pending_play_id(Some(play_id));

    // 2. Prepare Skip while Play is in flight:
    let skip_prep = harness.skip().await.expect("skip prepare");
    let skip_id = skip_prep.attempt_id.expect("skip attempt id");
    harness.assert_pending_skip_id(Some(skip_id));

    // 3. User stops the station (newer decision):
    harness.stop();
    harness.assert_state(PipelineState::Stopped);
    harness.assert_pending_play_id(None);

    // 4. Delayed completions arrive for Play and Skip:
    assert!(!harness.commit_play(play_id, &Ok(())));
    harness.assert_state(PipelineState::Stopped);

    let (applied, _) = harness.commit_skip(skip_id, &Ok(())).await;
    assert!(applied);
    harness.assert_state(PipelineState::Stopped);
    harness.assert_current_song_key(&b_key);
}

/// Unit test: when an initial Play is already pending, a second Play attempt
/// is refused with an error and does not overwrite pending_play.
#[tokio::test]
async fn controller_refuses_overlapping_play_while_initial_replace_is_pending() {
    let mut harness = ControllerScenario::stopped().with_queue(&["A", "B"]).build().await;
    harness.assert_state(PipelineState::Stopped);

    // 1. Prepare initial play (P1):
    let prepared_1 = harness.play().await.expect("first play prepare");
    let attempt_1 = prepared_1.play_attempt_id.expect("attempt id");
    assert_eq!(harness.controller.pending_play(), Some(attempt_1));
    harness.assert_state(PipelineState::Stopped);

    // 2. Second play while P1 is pending is refused:
    let error = harness.play().await.unwrap_err();
    assert!(matches!(error, PipelineError::Pipeline(_)));
    // P1 ownership is intact:
    assert_eq!(harness.controller.pending_play(), Some(attempt_1));
    harness.assert_state(PipelineState::Stopped);

    // 3. P1 commits cleanly:
    assert!(harness.commit_play(attempt_1, &Ok(())));
    harness.assert_state(PipelineState::Playing);
    assert_eq!(harness.controller.pending_play(), None);
}

/// Runtime concurrency (Test A): when initial Play P1 is blocked in flight,
/// a second Play P2 is refused by the command loop and does not create a
/// second Replace operation.
#[tokio::test]
async fn overlapping_play_while_initial_replace_is_blocked_is_refused_and_creates_no_second_replace() {
    run_blocked_initial_play_case(BlockedInitialPlayCase {
        name: "overlapping play while initial replace is blocked is refused and creates no second replace",
        queue: &["A", "B"],
        followup: BlockedPlayFollowup::OverlappingPlay,
        expected_playing: true,
        expected_title: "A",
        expected_replaces: 1,
        expected_pipeline_state: None,
        expected_set_playing: None,
    })
    .await;
}

/// Unit test: when a Skip from Stopped is pending, a Play attempt is refused
/// with an error and does not prepare an initial Replace on the stale queue cursor.
#[tokio::test]
async fn controller_refuses_play_from_stopped_while_skip_is_pending() {
    let mut harness = ControllerScenario::stopped().with_queue(&["A", "B", "C"]).build().await;
    let b_key = harness.track_key(1);
    harness.assert_state(PipelineState::Stopped);

    // 1. Prepare Skip S:
    let prepared_skip = harness.skip().await.expect("skip prepare");
    let skip_id = prepared_skip.attempt_id.expect("skip attempt id");
    assert_eq!(harness.controller.pending_skip(), Some(skip_id));
    harness.assert_state(PipelineState::Stopped);

    // 2. Play while Skip is pending is refused:
    let error = harness.play().await.unwrap_err();
    assert!(matches!(error, PipelineError::Pipeline(_)));
    assert_eq!(harness.controller.pending_skip(), Some(skip_id));
    harness.assert_state(PipelineState::Stopped);

    // 3. Skip commits cleanly:
    let (applied, _) = harness.commit_skip(skip_id, &Ok(())).await;
    assert!(applied);
    harness.assert_state(PipelineState::Stopped);
    assert_eq!(harness.controller.pending_skip(), None);
    harness.assert_current_song_key(&b_key);

    // 4. Now Play can be prepared on track B:
    let prepared_play = harness.play().await.expect("play prepare");
    let play_id = prepared_play.play_attempt_id.expect("play attempt id");
    let PipelineOperation::Replace(plan) = prepared_play.operation else {
        panic!("expected Replace operation");
    };
    assert_eq!(plan.current.key, b_key);
    assert!(harness.commit_play(play_id, &Ok(())));
    harness.assert_state(PipelineState::Playing);
}
/// Runtime concurrency: when a Skip from Stopped is in flight (blocked at gate),
/// a subsequent Play is refused immediately by the command loop and does not
/// queue a stale Replace for track A. When Skip finishes, station is Stopped on B,
/// and a subsequent Play prepares and executes InitialReplace for track B.
#[tokio::test]
async fn play_while_skip_from_stopped_is_pending_is_refused_and_does_not_prepare_stale_initial_replace() {
    let test = start_blocked_skip_from_stopped(queued_songs(&["A", "B", "C"])).await;

    // 1. Play while Skip is blocked in gate:
    let mut play_cmd = test
        .runtime
        .submit_and_wait_admitted(StationCommand::Play)
        .await
        .expect("play admission must succeed");

    // Play was refused immediately:
    let play_result = play_cmd.try_recv().expect("play must be answered immediately");
    assert!(play_result.is_err(), "play while skip is pending must fail: {play_result:?}");
    assert_eq!(test.pipeline.count(Call::Replace), 1, "no second replace was queued");

    // 2. Release gate for Skip:
    test.gate.release();

    let skip_result = test.skipping.await.unwrap();
    assert!(skip_result.is_ok(), "skip must succeed: {skip_result:?}");

    // 3. Station is Stopped on track B:
    assert_eq!(
        test.pipeline.snapshot_state(),
        PipelineState::Stopped,
        "physical pipeline must remain Stopped after skip from stopped"
    );
    let status = test.runtime.status().await.expect("status probe must succeed");
    match status {
        StatusEvent::State { playing, title, .. } => {
            assert!(!playing, "station must remain Stopped after skip from stopped");
            assert_eq!(title, "B", "station must have advanced to track B");
        }
        other => panic!("expected State event, got {other:?}"),
    }
    assert_eq!(test.pipeline.count(Call::Replace), 1);

    // 4. Now Play succeeds on track B:
    let playing = {
        let runtime = test.runtime.clone();
        tokio::spawn(async move { runtime.play().await })
    };
    test.gate.wait_started().await;
    assert_eq!(test.pipeline.count(Call::Replace), 2);
    test.gate.release();

    let play_result = playing.await.unwrap();
    assert!(play_result.is_ok(), "play after skip resolved must succeed: {play_result:?}");
    assert_eq!(
        test.pipeline.snapshot_state(),
        PipelineState::Playing,
        "physical pipeline must be Playing after initial replace on track B"
    );
    let status = test.runtime.status().await.expect("status probe must succeed");
    match status {
        StatusEvent::State { playing, title, .. } => {
            assert!(playing, "station must now be Playing");
            assert_eq!(title, "B", "station must play track B");
        }
        other => panic!("expected State event, got {other:?}"),
    }
    assert_eq!(test.pipeline.count(Call::Replace), 2);

    test.runtime.shutdown().await.unwrap();
}

/// Runtime concurrency: Initial Play P1 fails in pipeline, but Skip S was
/// prepared while P1 was in flight. While S is blocked in flight, a new Play P2
/// is refused. When S succeeds, it advances the track to B while preserving Stopped.
/// A subsequent Play P3 then cleanly starts playback on track B.
#[tokio::test]
async fn play_while_skip_resolving_failed_play_is_in_flight_is_refused_and_skip_commits_stopped() {
    run_blocked_skip_resolving_failed_play(
        false,
        "play while skip resolving failed play is in flight is refused and skip commits stopped",
    )
    .await;
}

#[tokio::test]
async fn play_while_skip_resolving_failed_play_is_in_flight_is_refused_and_failed_skip_allows_subsequent_play() {
    run_blocked_skip_resolving_failed_play(
        true,
        "play while skip resolving failed play is in flight is refused and failed skip allows subsequent play",
    )
    .await;
}

#[tokio::test]
async fn play_while_skip_resolving_failed_play_with_out_of_order_skip_result_first_commits_stopped() {
    run_blocked_out_of_order_skip_resolving_play(
        true,
        "play while skip resolving failed play with out of order skip result first commits stopped",
    )
    .await;
}

#[tokio::test]
async fn play_while_skip_resolving_successful_play_with_out_of_order_skip_result_first_commits_playing() {
    run_blocked_out_of_order_skip_resolving_play(
        false,
        "play while skip resolving successful play with out of order skip result first commits playing",
    )
    .await;
}

#[tokio::test]
async fn play_in_flight_followed_by_skip_then_overlapping_play_is_refused_and_commits_skip_successor() {
    run_blocked_initial_play_case(BlockedInitialPlayCase {
        name: "play in flight followed by skip then overlapping play is refused and commits skip successor",
        queue: &["A", "B", "C"],
        followup: BlockedPlayFollowup::SkipThenOverlappingPlay,
        expected_playing: true,
        expected_title: "B",
        expected_replaces: 2,
        expected_pipeline_state: Some(PipelineState::Playing),
        expected_set_playing: None,
    })
    .await;
}

#[tokio::test]
async fn skip_from_stopped_without_pending_play_preserves_stopped_state() {
    let songs = queued_songs(&["A", "B", "C"]);
    let (mut controller, _) = Harness::stopped(songs.clone()).into_parts();
    assert_eq!(controller.state, PipelineState::Stopped);

    let prepared = controller.skip().await.expect("skip prepare");
    let skip_id = prepared.attempt_id.expect("skip attempt id");
    assert_eq!(controller.state, PipelineState::Stopped);

    let (applied, _) = controller.commit_skip(skip_id, &Ok(())).await;
    assert!(applied);
    assert_eq!(
        controller.state,
        PipelineState::Stopped,
        "skip on stopped station without pending play must remain Stopped"
    );
    assert_eq!(
        controller.queue.current_song_info().as_ref().map(StationController::key_of),
        Some(StationController::key_of(&songs[1]))
    );
}

#[tokio::test]
async fn multiple_decode_failures_before_skip_commit_are_all_preserved_and_handled() {
    run_pending_skip_failure_case(PendingSkipFailureCase {
        name: "multiple decode failures before skip commit are all preserved and handled",
        initial_queue: &["A", "B", "C", "D"],
        failed_current: true,
        failed_staged_next: true,
        interruption: SkipInterruption::None,
        expected_state_after_skip: PipelineState::Playing,
        expected_generation_after_skip: 2,
        expected_has_deferred_terminal: true,
        expected_has_decode_exclusions: true,
        expected_realign_roll: None,
        expected_immediate_recovery_target: Some("D"),
        resume_recovery_target: None,
        expected_final_cursor: "D",
        expected_final_state: PipelineState::Playing,
        expected_final_generation: 3,
        subsequent_play_after_stop: false,
    })
    .await;
}

#[tokio::test]
async fn pause_during_failed_pending_skip_preserves_paused_without_autoplay() {
    run_pending_skip_failure_case(PendingSkipFailureCase {
        name: "pause during failed pending skip preserves paused without autoplay",
        initial_queue: &["A", "B", "C"],
        failed_current: true,
        failed_staged_next: false,
        interruption: SkipInterruption::Pause,
        expected_state_after_skip: PipelineState::Paused,
        expected_generation_after_skip: 2,
        expected_has_deferred_terminal: true,
        expected_has_decode_exclusions: false,
        expected_realign_roll: None,
        expected_immediate_recovery_target: None,
        resume_recovery_target: None,
        expected_final_cursor: "B",
        expected_final_state: PipelineState::Paused,
        expected_final_generation: 2,
        subsequent_play_after_stop: false,
    })
    .await;
}

#[tokio::test]
async fn pause_during_failed_pending_skip_with_multiple_failures_preserves_paused_and_realigns_staged_next() {
    run_pending_skip_failure_case(PendingSkipFailureCase {
        name: "pause during failed pending skip with multiple failures preserves paused and realigns staged next",
        initial_queue: &["A", "B", "C", "D"],
        failed_current: true,
        failed_staged_next: true,
        interruption: SkipInterruption::Pause,
        expected_state_after_skip: PipelineState::Paused,
        expected_generation_after_skip: 2,
        expected_has_deferred_terminal: true,
        expected_has_decode_exclusions: true,
        expected_realign_roll: Some(("C", "D")),
        expected_immediate_recovery_target: None,
        resume_recovery_target: None,
        expected_final_cursor: "B",
        expected_final_state: PipelineState::Paused,
        expected_final_generation: 2,
        subsequent_play_after_stop: false,
    })
    .await;
}

#[tokio::test]
async fn stop_during_failed_pending_skip_preserves_stopped_without_autoplay() {
    run_pending_skip_failure_case(PendingSkipFailureCase {
        name: "stop during failed pending skip preserves stopped without autoplay",
        initial_queue: &["A", "B", "C"],
        failed_current: true,
        failed_staged_next: false,
        interruption: SkipInterruption::Stop,
        expected_state_after_skip: PipelineState::Stopped,
        expected_generation_after_skip: 2,
        expected_has_deferred_terminal: false,
        expected_has_decode_exclusions: false,
        expected_realign_roll: None,
        expected_immediate_recovery_target: None,
        resume_recovery_target: None,
        expected_final_cursor: "B",
        expected_final_state: PipelineState::Stopped,
        expected_final_generation: 2,
        subsequent_play_after_stop: false,
    })
    .await;
}

#[tokio::test]
async fn stop_during_failed_pending_skip_with_multiple_failures_preserves_stopped_and_creates_no_realign() {
    run_pending_skip_failure_case(PendingSkipFailureCase {
        name: "stop during failed pending skip with multiple failures preserves stopped and creates no realign",
        initial_queue: &["A", "B", "C", "D"],
        failed_current: true,
        failed_staged_next: true,
        interruption: SkipInterruption::Stop,
        expected_state_after_skip: PipelineState::Stopped,
        expected_generation_after_skip: 2,
        expected_has_deferred_terminal: false,
        expected_has_decode_exclusions: false,
        expected_realign_roll: None,
        expected_immediate_recovery_target: None,
        resume_recovery_target: None,
        expected_final_cursor: "B",
        expected_final_state: PipelineState::Playing,
        expected_final_generation: 3,
        subsequent_play_after_stop: true,
    })
    .await;
}

#[tokio::test]
async fn pause_during_failed_current_skip_preserves_failure_and_resume_recovers_to_successor() {
    run_pending_skip_failure_case(PendingSkipFailureCase {
        name: "pause during failed current skip preserves failure and resume recovers to successor",
        initial_queue: &["A", "B", "C"],
        failed_current: true,
        failed_staged_next: false,
        interruption: SkipInterruption::Pause,
        expected_state_after_skip: PipelineState::Paused,
        expected_generation_after_skip: 2,
        expected_has_deferred_terminal: true,
        expected_has_decode_exclusions: false,
        expected_realign_roll: None,
        expected_immediate_recovery_target: None,
        resume_recovery_target: Some("C"),
        expected_final_cursor: "C",
        expected_final_state: PipelineState::Playing,
        expected_final_generation: 3,
        subsequent_play_after_stop: false,
    })
    .await;
}

#[tokio::test]
async fn pause_during_multiple_failed_skip_preserves_failures_and_resume_recovers_to_unbroken_successor() {
    run_pending_skip_failure_case(PendingSkipFailureCase {
        name: "pause during multiple failed skip preserves failures and resume recovers to unbroken successor",
        initial_queue: &["A", "B", "C", "D"],
        failed_current: true,
        failed_staged_next: true,
        interruption: SkipInterruption::Pause,
        expected_state_after_skip: PipelineState::Paused,
        expected_generation_after_skip: 2,
        expected_has_deferred_terminal: true,
        expected_has_decode_exclusions: true,
        expected_realign_roll: Some(("C", "D")),
        expected_immediate_recovery_target: None,
        resume_recovery_target: Some("D"),
        expected_final_cursor: "D",
        expected_final_state: PipelineState::Playing,
        expected_final_generation: 3,
        subsequent_play_after_stop: false,
    })
    .await;
}

struct PausedFailedSkipFixture {
    runtime: StationRuntime,
    /// Retained sender guard: keeping this alive prevents the runtime loop from shutting down on EOF.
    _event_sender_guard: mpsc::UnboundedSender<PipelineEvent>,
    pipeline: Arc<RecordingPipeline>,
    status_rx: broadcast::Receiver<StatusEvent>,
    station_id: Uuid,
    songs: Vec<SongInfo>,
}
async fn setup_paused_failed_skip(
    db: &sqlx::PgPool,
    track_names: &[&'static str],
    staged_also_fails: bool,
    roll_gate: Option<Arc<testsupport::Gate>>,
) -> PausedFailedSkipFixture {
    let songs = queued_songs(track_names);
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    if let Some(ref rg) = roll_gate {
        pipeline.set_roll_gate(Some(rg.clone()));
    }
    let harness = Harness::with_db(db.clone(), pipeline.clone(), songs.clone());
    let station_id = harness.controller.station_id;
    seed_station(db, station_id, Some(songs[0].queue_item_id), &songs).await;
    let status_rx = harness.controller.status_tx.subscribe();
    let (runtime, events) = harness.into_runtime();
    let gate = pipeline.replace_gate().expect("gated pipeline");
    let b_key = StationController::track(songs[1].clone()).key;

    play_through_gate(&runtime, &gate).await;

    let skip_rx = runtime.begin_command(StationCommand::Skip).await.expect("skip command begin");
    gate.wait_started().await;
    assert_eq!(pipeline.count(Call::Replace), 2);

    let notify = Arc::new(tokio::sync::Notify::new());
    events
        .send(PipelineEvent::DecodeFailed {
            generation: 2,
            track: b_key.clone(),
            message: "B failed".into(),
        })
        .unwrap();
    let c_key = if staged_also_fails && songs.len() > 2 {
        let key = StationController::track(songs[2].clone()).key;
        events
            .send(PipelineEvent::DecodeFailed {
                generation: 2,
                track: key.clone(),
                message: "C failed".into(),
            })
            .unwrap();
        Some(key)
    } else {
        None
    };
    events.send(PipelineEvent::TestBarrier(notify.clone())).unwrap();
    notify.notified().await;

    // Deterministically assert that the event loop processed the failures:
    let snapshot_before_pause = runtime.test_snapshot().await.unwrap();
    assert_eq!(snapshot_before_pause.generation, 1);
    assert!(snapshot_before_pause.pending_skip.is_some());
    assert_eq!(snapshot_before_pause.pending_skip_failures, Some((Some(b_key), c_key)));

    // Admit and execute controller.pause() before releasing the Replace gate:
    let pause_rx = runtime
        .submit_and_wait_admitted(StationCommand::Pause)
        .await
        .expect("pause command admitted");

    let snapshot_paused = runtime.test_snapshot().await.unwrap();
    assert_eq!(snapshot_paused.state, PipelineState::Paused);
    gate.release();
    pause_rx.await.unwrap().unwrap();
    skip_rx.await.unwrap().expect("the initial skip must succeed in pipeline");
    pipeline.set_replace_gate(None);
    wait_for_db_cursor(db, station_id, Some(songs[1].queue_item_id)).await;

    PausedFailedSkipFixture {
        runtime,
        _event_sender_guard: events,
        pipeline,
        status_rx,
        station_id,
        songs,
    }
}

/// End to end through StationRuntime: when a station is paused during a failed skip,
/// resuming playback via runtime.play() prepares and executes a recovery Replace from
/// the failed track B to successor C, returning Ok only after the recovery successfully
/// committed in the pipeline.
#[tokio::test]
async fn pause_during_failed_skip_resume_recovers_at_runtime() {
    run_reconnect_test(async |db| {
        let fixture = setup_paused_failed_skip(&db.pool, &["A", "B", "C"], false, None).await;

        let initial_snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(initial_snapshot.state, PipelineState::Paused);
        let mut status_rx = fixture.status_rx.resubscribe();

        // Resume playback via runtime.play():
        // Must prepare and execute recovery Replace B -> C, answering the caller only
        // after the physical Replace and logical commit succeed.
        fixture.runtime.play().await.unwrap();

        testsupport::wait_for("recovery replace to reach pipeline", || fixture.pipeline.count(Call::Replace) == 3).await;
        wait_for_db_cursor(&db.pool, fixture.station_id, Some(fixture.songs[2].queue_item_id)).await;
        expect_song_change(&mut status_rx, "C").await;
        let final_snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(final_snapshot.state, PipelineState::Playing);
        assert_eq!(final_snapshot.generation, 3);
        assert_eq!(final_snapshot.pending_skip, None);

        fixture.runtime.shutdown().await.unwrap();
    })
    .await;
}

/// End to end through StationRuntime: when a station is paused during a failed skip
/// and the subsequent resume recovery Replace fails in the pipeline, runtime.play()
/// MUST return Err to the caller and the controller state MUST roll back to Paused.
#[tokio::test]
async fn pause_during_failed_skip_resume_recovery_failure_returns_err_at_runtime() {
    run_reconnect_test(async |db| {
        let fixture = setup_paused_failed_skip(&db.pool, &["A", "B", "C"], false, None).await;
        let c_key = StationController::track(fixture.songs[2].clone()).key;

        // Fail the recovery Replace (the 3rd Replace operation on the pipeline):
        fixture.pipeline.fail_nth(Call::Replace, 2);

        let play_res = fixture.runtime.play().await;
        assert!(
            play_res.is_err(),
            "runtime.play() must return Err when recovery Replace fails, got {play_res:?}"
        );
        assert_eq!(fixture.pipeline.count(Call::Replace), 3);

        // Verify full coherent rollback to Paused state on B:
        let snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 2, "failed recovery must not advance generation");
        assert_eq!(snapshot.state, PipelineState::Paused, "controller state must roll back to Paused");
        assert_eq!(snapshot.pending_skip, None, "pending skip must be cleaned up");
        assert_eq!(
            snapshot.planned_next.as_ref(),
            Some(&c_key),
            "planned_next must still describe the physical staged branch C"
        );
        assert_eq!(snapshot.pending_realign, None, "pending realign must not be orphaned");

        assert_eq!(
            persisted_cursor(&db.pool, fixture.station_id).await,
            Some(fixture.songs[1].queue_item_id),
            "cursor must remain on B after recovery failure"
        );

        // A subsequent manual skip succeeds cleanly to C:
        fixture.runtime.skip().await.unwrap();
        testsupport::wait_for("manual skip to reach pipeline", || fixture.pipeline.count(Call::Replace) == 4).await;
        wait_for_db_cursor(&db.pool, fixture.station_id, Some(fixture.songs[2].queue_item_id)).await;
        let after_skip = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(after_skip.generation, 3);
        assert_eq!(after_skip.state, PipelineState::Paused);

        fixture.runtime.shutdown().await.unwrap();
    })
    .await;
}

/// End to end through StationRuntime: when both current B and staged next C fail during skip,
/// a realign roll C -> D is scheduled while Paused. While Roll C -> D is HELD IN FLIGHT on
/// roll_gate, runtime.play() is invoked. The recovery Replace directly targets unbroken
/// successor D and is submitted behind Roll in the sequential lane. When roll_gate is
/// released, both operations complete in order and playback cleanly transitions to D.
#[tokio::test]
async fn pause_with_staged_failure_and_realign_play_recovers_to_valid_successor_at_runtime() {
    run_reconnect_test(async |db| {
        let roll_gate = testsupport::Gate::new();
        let fixture = setup_paused_failed_skip(&db.pool, &["A", "B", "C", "D"], true, Some(roll_gate.clone())).await;
        let c_key = StationController::track(fixture.songs[2].clone()).key;
        // Wait for realign roll C -> D to enter and block in roll_gate:
        roll_gate.wait_started().await;
        assert_eq!(fixture.pipeline.count(Call::Roll), 1);

        // In-flight assertion while Roll is held on the gate:
        let in_flight = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(in_flight.state, PipelineState::Paused);
        assert!(in_flight.pending_realign.is_some(), "realign roll must be pending in flight");
        assert_eq!(
            in_flight.planned_next.as_ref(),
            Some(&c_key),
            "planned_next must still describe the physical staged branch C while Roll is held"
        );

        let d_key = StationController::track(fixture.songs[3].clone()).key;
        let mut status_rx = fixture.status_rx.resubscribe();

        // Spawn Play while Roll is STILL held on the gate:
        let play_task = tokio::spawn({
            let runtime = fixture.runtime.clone();
            async move { runtime.play().await }
        });

        // Prove deterministically via command barrier that the controller loop
        // admitted and processed the Play command (preparing recovery Replace to D):
        fixture.runtime.barrier().await.unwrap();

        // In-flight assertion before Roll gate is released:
        let mid_snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(mid_snapshot.state, PipelineState::Paused);
        assert!(mid_snapshot.pending_realign.is_some(), "realign roll must still be in flight");
        assert!(mid_snapshot.pending_skip.is_some(), "recovery skip must be pending");
        assert_eq!(
            mid_snapshot.pending_skip_target.as_ref(),
            Some(&d_key),
            "pending skip target must be unbroken successor D"
        );
        assert_eq!(
            mid_snapshot.planned_next.as_ref(),
            Some(&c_key),
            "planned_next must still describe the physical staged branch C while Roll is held"
        );

        // Now release the Roll gate:
        roll_gate.release();

        // Play command finishes after the recovery Replace completes:
        play_task.await.unwrap().unwrap();

        testsupport::wait_for("recovery replace to reach pipeline", || fixture.pipeline.count(Call::Replace) == 3).await;
        wait_for_db_cursor(&db.pool, fixture.station_id, Some(fixture.songs[3].queue_item_id)).await;
        expect_song_change(&mut status_rx, "D").await;

        let snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot.state, PipelineState::Playing);
        assert_eq!(snapshot.generation, 3);
        assert_eq!(snapshot.pending_skip, None);
        assert_eq!(snapshot.pending_realign, None);

        assert_eq!(fixture.pipeline.count(Call::Roll), 1, "exactly one realign roll was submitted");
        assert_eq!(
            fixture.pipeline.count(Call::Replace),
            3,
            "exactly 3 replace operations were submitted"
        );

        fixture.runtime.shutdown().await.unwrap();
    })
    .await;
}

struct GatedFailingManualSkipFixture {
    runtime: StationRuntime,
    pipeline: Arc<RecordingPipeline>,
    gate: Arc<testsupport::Gate>,
    skip_rx: oneshot::Receiver<Result<(), PipelineError>>,
    station_id: Uuid,
    songs: Vec<SongInfo>,
    _event_sender_guard: mpsc::UnboundedSender<PipelineEvent>,
}

async fn start_gated_failing_manual_skip(db: &sqlx::PgPool, track_names: &[&'static str]) -> GatedFailingManualSkipFixture {
    let songs = queued_songs(track_names);
    let pipeline = Arc::new(RecordingPipeline::with_gates());
    let harness = Harness::with_db(db.clone(), pipeline.clone(), songs.clone());
    let station_id = harness.controller.station_id;
    seed_station(db, station_id, Some(songs[0].queue_item_id), &songs).await;
    let (runtime, event_sender_guard) = harness.into_runtime();
    let gate = pipeline.replace_gate().expect("gated pipeline");

    play_through_gate(&runtime, &gate).await;

    // In-flight Replace B (2nd Replace operation) will fail:
    pipeline.fail_nth(Call::Replace, 1);

    // Start manual skip from Playing A to B, which blocks on replace_gate:
    let skip_rx = runtime.begin_command(StationCommand::Skip).await.expect("skip command begin");
    gate.wait_started().await;
    assert_eq!(pipeline.count(Call::Replace), 2);

    GatedFailingManualSkipFixture {
        runtime,
        pipeline,
        gate,
        skip_rx,
        station_id,
        songs,
        _event_sender_guard: event_sender_guard,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GatedSkipInterruption {
    Pause,
    Stop,
}

struct GatedManualSkipInterruptionCase {
    name: &'static str,
    queue: &'static [&'static str],
    interruption: GatedSkipInterruption,
    expected_state: PipelineState,
}

async fn run_gated_manual_skip_interruption_case(db: &sqlx::PgPool, case: GatedManualSkipInterruptionCase) {
    let fixture = start_gated_failing_manual_skip(db, case.queue).await;
    let in_flight = fixture.runtime.test_snapshot().await.unwrap();
    assert_eq!(in_flight.state, PipelineState::Playing, "{}: fixture must begin Playing", case.name);
    assert!(
        in_flight.pending_skip.is_some(),
        "{}: manual skip must be pending before interruption",
        case.name
    );
    match case.interruption {
        GatedSkipInterruption::Pause => {
            let pause_rx = fixture
                .runtime
                .submit_and_wait_admitted(StationCommand::Pause)
                .await
                .expect("pause command admitted");
            let snapshot_paused = fixture.runtime.test_snapshot().await.unwrap();
            assert_eq!(
                snapshot_paused.state,
                PipelineState::Paused,
                "{}: snapshot state after pause",
                case.name
            );
            fixture.gate.release();
            let skip_res = fixture.skip_rx.await.unwrap();
            assert!(skip_res.is_err(), "{}: manual skip must return Err", case.name);
            pause_rx.await.unwrap().unwrap();
        }
        GatedSkipInterruption::Stop => {
            let stop_rx = fixture
                .runtime
                .submit_and_wait_admitted(StationCommand::Stop)
                .await
                .expect("stop command admitted");
            let snapshot_stopped = fixture.runtime.test_snapshot().await.unwrap();
            assert_eq!(
                snapshot_stopped.state,
                PipelineState::Stopped,
                "{}: snapshot state after stop",
                case.name
            );
            fixture.gate.release();
            let skip_res = fixture.skip_rx.await.unwrap();
            assert!(skip_res.is_err(), "{}: manual skip must return Err", case.name);
            stop_rx.await.unwrap().unwrap();
        }
    }
    let final_snapshot = fixture.runtime.test_snapshot().await.unwrap();
    assert_eq!(final_snapshot.state, case.expected_state, "{}: final state", case.name);
    assert_eq!(final_snapshot.generation, 1, "{}: final generation", case.name);
    assert_eq!(final_snapshot.pending_skip, None, "{}: pending_skip None", case.name);
    assert_eq!(
        persisted_cursor(db, fixture.station_id).await,
        Some(fixture.songs[0].queue_item_id),
        "{}: cursor remains on initial song",
        case.name
    );
    if case.expected_state == PipelineState::Stopped {
        assert_eq!(final_snapshot.planned_next, None, "{}: planned_next None on stop", case.name);
        assert_eq!(final_snapshot.pending_realign, None, "{}: pending_realign None on stop", case.name);
        assert!(
            final_snapshot.deferred_terminal.is_none(),
            "{}: deferred_terminal None on stop",
            case.name
        );
        assert!(fixture.pipeline.count(Call::Stop) > 0, "{}: physical pipeline stopped", case.name);
    }
    assert_eq!(
        fixture.pipeline.snapshot_state(),
        case.expected_state,
        "{}: pipeline snapshot state",
        case.name
    );
    fixture.runtime.shutdown().await.unwrap();
}

/// End to end through StationRuntime: a late SkipResult(Err) after Pause
/// must keep the station in Paused and not revert it to Playing.
#[tokio::test]
async fn manual_skip_failure_after_pause_preserves_paused() {
    run_reconnect_test(async |db| {
        run_gated_manual_skip_interruption_case(
            &db.pool,
            GatedManualSkipInterruptionCase {
                name: "manual skip failure after pause preserves paused",
                queue: &["A", "B", "C"],
                interruption: GatedSkipInterruption::Pause,
                expected_state: PipelineState::Paused,
            },
        )
        .await;
    })
    .await;
}

/// End to end through StationRuntime: a late SkipResult(Err) after Stop
/// must keep the station in Stopped and not revert it to Playing.
#[tokio::test]
async fn manual_skip_failure_after_stop_preserves_stopped() {
    run_reconnect_test(async |db| {
        run_gated_manual_skip_interruption_case(
            &db.pool,
            GatedManualSkipInterruptionCase {
                name: "manual skip failure after stop preserves stopped",
                queue: &["A", "B", "C"],
                interruption: GatedSkipInterruption::Stop,
                expected_state: PipelineState::Stopped,
            },
        )
        .await;
    })
    .await;
}

/// End to end through StationRuntime: a late resume recovery Replace failure after Stop
/// preserves Stopped and leaves no orphaned state or ghost recovery.
#[tokio::test]
async fn resume_recovery_failure_after_stop_preserves_stopped() {
    run_reconnect_test(async |db| {
        let fixture = setup_paused_failed_skip(&db.pool, &["A", "B", "C"], false, None).await;

        // Set up a replace gate so recovery Replace to C will block:
        let gate = testsupport::Gate::new();
        fixture.pipeline.set_replace_gate(Some(gate.clone()));
        // Fail the recovery Replace (3rd Replace operation):
        fixture.pipeline.fail_nth(Call::Replace, 2);

        // Start resume recovery via Play:
        let play_rx = fixture
            .runtime
            .begin_command(StationCommand::Play)
            .await
            .expect("play command begin");
        gate.wait_started().await;
        assert_eq!(fixture.pipeline.count(Call::Replace), 3);

        // While recovery Replace is held on gate, submit Stop:
        let stop_rx = fixture
            .runtime
            .submit_and_wait_admitted(StationCommand::Stop)
            .await
            .expect("stop command admitted");

        let snapshot_stopped = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot_stopped.state, PipelineState::Stopped);
        gate.release();
        let play_res = play_rx.await.unwrap();
        assert!(play_res.is_err(), "play must return Err");
        stop_rx.await.unwrap().unwrap();

        // Final state: controller Stopped, physical pipeline Stopped, no orphaned state
        let final_snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(final_snapshot.state, PipelineState::Stopped);
        assert_eq!(final_snapshot.pending_skip, None);
        assert_eq!(final_snapshot.pending_realign, None);
        assert_eq!(final_snapshot.planned_next, None);
        assert!(final_snapshot.deferred_terminal.is_none());
        assert!(fixture.pipeline.count(Call::Stop) > 0);
        assert_eq!(fixture.pipeline.snapshot_state(), PipelineState::Stopped);

        fixture.runtime.shutdown().await.unwrap();
    })
    .await;
}

/// End to end through StationRuntime: when a station has a deferred terminal track B in Paused
/// and a manual skip B -> C is in flight on replace_gate, a concurrent/interleaved Play command
/// is refused with Err and does NOT wipe deferred_terminal nor issue SetPlaying(true) on broken B.
/// When the in-flight manual skip fails, the deferred terminal is still intact for B, and a later
/// successful manual skip advances to C and clears the terminal record.
#[tokio::test]
async fn play_during_pending_terminal_skip_does_not_lose_deferred_terminal_or_resume_broken_track() {
    run_reconnect_test(async |db| {
        let fixture = setup_paused_failed_skip(&db.pool, &["A", "B", "C"], false, None).await;
        let b_key = StationController::track(fixture.songs[1].clone()).key;
        let c_key = StationController::track(fixture.songs[2].clone()).key;

        // Set up a replace gate on the pipeline so the manual skip will block in flight:
        let gate = testsupport::Gate::new();
        fixture.pipeline.set_replace_gate(Some(gate.clone()));
        // In-flight manual Replace (3rd Replace operation on the pipeline) will fail:
        fixture.pipeline.fail_nth(Call::Replace, 2);

        // Start manual skip B -> C:
        let skip_rx = fixture
            .runtime
            .begin_command(StationCommand::Skip)
            .await
            .expect("skip command begin");
        gate.wait_started().await;
        assert_eq!(fixture.pipeline.count(Call::Replace), 3);

        // Snapshot while manual Replace B -> C is held on the gate:
        let in_flight = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(in_flight.state, PipelineState::Paused);
        assert!(in_flight.pending_skip.is_some());
        assert_eq!(in_flight.pending_skip_target.as_ref(), Some(&c_key));
        assert_eq!(
            in_flight.deferred_terminal,
            Some((2, b_key.clone(), 1)),
            "deferred terminal for broken track B must be present before Play"
        );

        // While manual Replace is still held on the gate, invoke Play:
        let play_res = fixture.runtime.play().await;
        assert!(
            play_res.is_err(),
            "play must be rejected with Err while skip is in flight for terminal track, got {play_res:?}"
        );

        // Snapshot immediately after rejected Play:
        let snapshot_after_play = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot_after_play.state, PipelineState::Paused);
        assert_eq!(
            snapshot_after_play.deferred_terminal,
            Some((2, b_key.clone(), 1)),
            "deferred terminal must NOT be cleared by rejected Play"
        );
        assert_eq!(
            fixture.pipeline.count(Call::SetPlaying),
            1,
            "no SetPlaying(true) must be issued for broken track B"
        );

        // Now release the Replace gate:
        gate.release();

        // The manual skip returns Err:
        let skip_res = skip_rx.await.unwrap();
        assert!(skip_res.is_err(), "manual skip must return Err");

        // Final state after failed skip:
        let final_snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(final_snapshot.state, PipelineState::Paused);
        assert_eq!(final_snapshot.generation, 2);
        assert_eq!(final_snapshot.pending_skip, None);
        assert_eq!(
            final_snapshot.deferred_terminal,
            Some((2, b_key, 1)),
            "deferred terminal must remain intact after failed manual skip"
        );
        assert_eq!(fixture.pipeline.count(Call::SetPlaying), 1, "no ghost SetPlaying(true) was issued");
        assert_eq!(fixture.pipeline.count(Call::Roll), 0, "no ghost Roll was issued");

        // A subsequent successful manual skip cleanly advances to C:
        fixture.pipeline.set_replace_gate(None);
        fixture.runtime.skip().await.unwrap();
        testsupport::wait_for("manual skip to reach pipeline", || fixture.pipeline.count(Call::Replace) == 4).await;
        wait_for_db_cursor(&db.pool, fixture.station_id, Some(fixture.songs[2].queue_item_id)).await;
        let after_skip = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(after_skip.generation, 3);
        assert_eq!(after_skip.state, PipelineState::Paused);
        assert!(
            after_skip.deferred_terminal.is_none(),
            "deferred terminal is cleared only after successful identity change"
        );

        fixture.runtime.shutdown().await.unwrap();
    })
    .await;
}

/// End to end through StationRuntime: when a resume recovery Replace fails, a second
/// Play attempt on the exhausted retry budget returns Err without attempting to issue
/// SetPlaying(true) on the broken track, and a subsequent manual skip succeeds to C.
#[tokio::test]
async fn second_play_after_failed_recovery_policy() {
    run_reconnect_test(async |db| {
        let fixture = setup_paused_failed_skip(&db.pool, &["A", "B", "C"], false, None).await;
        let b_key = StationController::track(fixture.songs[1].clone()).key;

        // First recovery Replace to C fails:
        fixture.pipeline.fail_nth(Call::Replace, 2);

        let first_play = fixture.runtime.play().await;
        assert!(first_play.is_err(), "first play must return Err on failed recovery replace");

        let snapshot_after_first = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot_after_first.state, PipelineState::Paused);
        assert_eq!(snapshot_after_first.generation, 2);
        assert_eq!(snapshot_after_first.pending_skip, None);
        assert_eq!(
            snapshot_after_first.deferred_terminal,
            Some((2, b_key.clone(), 0)),
            "deferred terminal must be preserved with retries_left = 0"
        );

        // Second play attempt on exhausted retry budget:
        let second_play = fixture.runtime.play().await;
        assert!(
            second_play.is_err(),
            "second play must return Err when terminal retry budget is exhausted"
        );

        let snapshot_after_second = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot_after_second.state, PipelineState::Paused);
        assert_eq!(snapshot_after_second.generation, 2);
        assert_eq!(snapshot_after_second.pending_skip, None);
        assert_eq!(
            snapshot_after_second.deferred_terminal,
            Some((2, b_key, 0)),
            "deferred terminal must still be preserved without dropping info"
        );
        assert_eq!(
            fixture.pipeline.count(Call::SetPlaying),
            1,
            "no SetPlaying(true) must be issued for broken current track B"
        );

        // A subsequent manual skip succeeds cleanly to C:
        fixture.runtime.skip().await.unwrap();
        testsupport::wait_for("manual skip to reach pipeline", || fixture.pipeline.count(Call::Replace) == 4).await;
        wait_for_db_cursor(&db.pool, fixture.station_id, Some(fixture.songs[2].queue_item_id)).await;
        let after_skip = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(after_skip.generation, 3);
        assert_eq!(after_skip.state, PipelineState::Paused);
        assert!(after_skip.deferred_terminal.is_none());

        fixture.runtime.shutdown().await.unwrap();
    })
    .await;
}
