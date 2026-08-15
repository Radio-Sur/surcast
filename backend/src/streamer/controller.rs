use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::{broadcast, mpsc};

use super::driver::{PipelineDriver, PipelineOperation, PipelineOperationResult};
use super::pipeline::{
    IcecastTarget, PairPlan, PipelineError, PipelineEvent, PipelineSnapshot, PipelineState, PipelineTrack, PlannedNext,
    PlaybackPipelineFactory, ReplaceMode, RollingChange, RollingPlan, StationPlaybackConfig, TrackKey, TrackMetadata,
};
use super::{QueueManager, SongInfo, StatusEvent};
use crate::stations::repository;

pub(crate) struct StationController {
    queue: Arc<QueueManager>,
    db: PgPool,
    station_id: uuid::Uuid,
    playback: StationPlaybackConfig,
    driver: PipelineDriver,
    target: IcecastTarget,
    state: PipelineState,
    status_tx: broadcast::Sender<StatusEvent>,
    queue_tx: broadcast::Sender<String>,
    generation: u64,
    output_epoch: u64,
    planned_next: Option<(TrackKey, super::queue_state::QueueAnchor)>,
    /// The queue drained and the station stopped waiting for new content
    /// (AutoDJ / schedule fill), as opposed to a manual stop. Only this
    /// state may auto-resume playback once the queue fills again.
    idle: bool,
    /// Monotonic id of the single in-flight automatic idle resume, if any.
    /// Guards the idle ticker against queueing several resume replaces and
    /// lets a stale completion (a superseding user decision arrived first)
    /// be ignored instead of overwriting it.
    pending_resume: Option<u64>,
    resume_attempt_seq: u64,
    /// Token of the currently active reconnect retry chain. Retry timers
    /// carry the token of the chain that scheduled them and only act while
    /// their token still matches — a newer reconnect attempt, a stop, or a
    /// shutdown invalidates older timers without touching generation /
    /// output_epoch.
    active_reconnect_retry: Option<u64>,
    reconnect_retry_seq: u64,
    /// The (generation, output_epoch) the active automatic reconnect chain
    /// belongs to. A duplicate `SinkDisconnected` for the same output while
    /// a chain is active is ignored: it must not mint a second chain (which
    /// would reset the exponential backoff and enqueue redundant
    /// reconnects).
    active_reconnect_output: Option<(u64, u64)>,
    /// Shared reconnect chain state, readable by the pipeline executor: a
    /// queued reconnect operation checks the token right before touching the
    /// pipeline and skips itself when its chain was superseded (or
    /// invalidated by a stop) after being enqueued. The `completed` flag is
    /// set by the executor the moment a reconnect succeeds — before the
    /// `ReconnectFinished` command reaches the runtime — so a disconnect
    /// landing in that window is treated as a fresh event, never coalesced
    /// into the finished chain.
    reconnect_token_shared: std::sync::Arc<ReconnectShared>,
    /// (generation, output_epoch) of an output we KNOW is disconnected: set
    /// on every `SinkDisconnected` for the current output, independent of
    /// the station state, and kept until a reconnect for exactly that output
    /// succeeds (or the output identity changes / a manual stop). It is
    /// factual state, not a pending command — Pause invalidates the retry
    /// chain but preserves this marker, so the next Play can always recover
    /// a broken output without needing a second disconnect event.
    known_disconnected_output: Option<(u64, u64)>,
}

/// Shared reconnect chain state between the controller and the pipeline
/// executor.
#[derive(Default)]
pub(crate) struct ReconnectShared {
    token: std::sync::atomic::AtomicU64,
    completed_token: std::sync::atomic::AtomicU64,
}

impl ReconnectShared {
    /// Marks `token` as the active chain and clears the completion marker.
    pub(crate) fn set_token(&self, token: u64) {
        self.token.store(token, std::sync::atomic::Ordering::Release);
        self.completed_token.store(0, std::sync::atomic::Ordering::Release);
    }

    /// Records that `token`'s reconnect finished (success or one-shot
    /// failure). A pure store: the identity travels in the value, so an old
    /// in-flight operation finishing late can never mark a NEWER chain as
    /// completed — `completed_token` simply holds the old token.
    pub(crate) fn mark_completed(&self, token: u64) {
        self.completed_token.store(token, std::sync::atomic::Ordering::Release);
    }

    /// Whether the CURRENT chain is finished: the executor completed exactly
    /// this chain's reconnect.
    pub(crate) fn is_current_completed(&self) -> bool {
        let current = self.token.load(std::sync::atomic::Ordering::Acquire);
        current != 0 && self.completed_token.load(std::sync::atomic::Ordering::Acquire) == current
    }

    /// Clears both the token and the completion marker (stop/shutdown/chain
    /// end).
    pub(crate) fn invalidate(&self) {
        self.token.store(0, std::sync::atomic::Ordering::Release);
        self.completed_token.store(0, std::sync::atomic::Ordering::Release);
    }

    /// The active chain token as observed by the executor.
    pub(crate) fn token(&self) -> u64 {
        self.token.load(std::sync::atomic::Ordering::Acquire)
    }
}
impl StationController {
    pub(crate) async fn new(
        queue: Arc<QueueManager>,
        db: PgPool,
        mount: &str,
        prebuffer_bytes: i32,
        factory: Arc<dyn PlaybackPipelineFactory>,
        status_tx: broadcast::Sender<StatusEvent>,
        queue_tx: broadcast::Sender<String>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<PipelineEvent>), PipelineError> {
        let station_id = queue.station_id();
        let settings = repository::find_playback_settings(&db, station_id)
            .await
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let playback = match settings {
            Some(settings) => StationPlaybackConfig::from_persisted(
                &settings.transition_mode,
                settings.default_fade_ms,
                settings.autocue_fade_max_ms,
                prebuffer_bytes,
            )?,
            None => StationPlaybackConfig::from_persisted("off", 0, 0, prebuffer_bytes)?,
        };
        let (endpoint, password) = crate::icecast::models::get_connection_config(&db)
            .await
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let target = IcecastTarget::parse(&endpoint, password, mount, mount.trim_matches('/').to_owned())?;
        let instance = factory.create(playback.pipeline_config(target.clone())).await?;
        Ok((
            Self {
                queue,
                db,
                station_id,
                playback,
                driver: PipelineDriver::spawn(instance.pipeline),
                target,
                state: PipelineState::Stopped,
                status_tx,
                queue_tx,
                generation: 0,
                output_epoch: 0,
                planned_next: None,
                idle: false,
                pending_resume: None,
                resume_attempt_seq: 0,
                active_reconnect_retry: None,
                reconnect_retry_seq: 0,
                active_reconnect_output: None,
                reconnect_token_shared: std::sync::Arc::default(),
                known_disconnected_output: None,
            },
            instance.events,
        ))
    }

    pub(crate) async fn handle_event(&mut self, event: PipelineEvent) -> Option<Result<PipelineOperation, PipelineError>> {
        match event {
            PipelineEvent::DecodeFailed {
                generation,
                track,
                message,
            } => {
                tracing::warn!(station_id = %self.station_id, generation, %message, "GStreamer decoder exposed no usable audio branch");
                let current = self.queue.current_song_info();
                if let Some(operation) = self.resolve_current_terminal(generation, &track).await {
                    return Some(operation);
                }
                if generation == self.generation && self.planned_next.as_ref().is_some_and(|(key, _)| key == &track) {
                    let replacement = current.as_ref().and_then(|current| {
                        self.queue.successor_after(&track).map(|successor| {
                            let track = Self::track(successor);
                            let current = Self::track(current.clone());
                            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current, Some(&track));
                            PlannedNext { track, transition }
                        })
                    });
                    self.planned_next = replacement
                        .as_ref()
                        .map(|next| (next.track.key.clone(), self.queue.anchor_after_current()));
                    if let Some(current) = current {
                        return Some(Ok(PipelineOperation::Roll(Box::new(RollingPlan {
                            generation,
                            current: Self::track(current).key,
                            change: RollingChange::ReplaceNext {
                                expected_next: track,
                                replacement,
                            },
                        }))));
                    }
                }
            }
            PipelineEvent::CurrentEos {
                generation,
                current: track,
            } => {
                if let Some(operation) = self.resolve_current_terminal(generation, &track).await {
                    return Some(operation);
                }
            }
            PipelineEvent::Handover {
                generation,
                current: track,
            } => {
                let current = self.queue.current_song_info().map(|song| song.queue_item_id);
                if generation == self.generation && current != Some(track.queue_item_id) {
                    if !self.planned_next.as_ref().is_some_and(|(key, _)| key == &track) {
                        // The staged next was replaced (queue realignment) and the
                        // pipeline handed over to the old plan; the queue state must
                        // not consume a track that will never play.
                        tracing::warn!(station_id = %self.station_id, queue_item_id = %track.queue_item_id, "ignoring stale handover after queue realignment");
                        return None;
                    }
                    let anchor = self
                        .planned_next
                        .take()
                        .filter(|(key, _)| key == &track)
                        .map_or_else(|| self.queue.anchor_after_current(), |(_, anchor)| anchor);
                    self.queue.commit_current(&track, anchor).await;
                    self.publish_song_change();
                    self.push_queue_update().await;

                    if let (Some(current), Some(next)) = (self.queue.current_song_info(), self.queue.peek_next_song()) {
                        let current = Self::track(current);
                        let next = Self::track(next);
                        let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current, Some(&next));
                        let next_anchor = self.queue.anchor_after_current();
                        self.planned_next = Some((next.key.clone(), next_anchor));
                        return Some(Ok(PipelineOperation::Roll(Box::new(RollingPlan {
                            generation: self.generation,
                            current: track,
                            change: RollingChange::Attach(PlannedNext { track: next, transition }),
                        }))));
                    }
                }
            }
            PipelineEvent::SinkDisconnected {
                generation,
                output_epoch,
                message,
            } => {
                tracing::error!(station_id = %self.station_id, generation, output_epoch, %message, "GStreamer output disconnected");
                return self.reconnect_for_output(generation, output_epoch).await;
            }
        }
        None
    }

    async fn resolve_current_terminal(&mut self, generation: u64, track: &TrackKey) -> Option<Result<PipelineOperation, PipelineError>> {
        let current = self.queue.current_song_info().map(|song| song.queue_item_id);
        if generation == self.generation && current == Some(track.queue_item_id) {
            Some(self.skip().await)
        } else {
            None
        }
    }

    fn track(song: SongInfo) -> PipelineTrack {
        PipelineTrack {
            key: TrackKey {
                queue_item_id: song.queue_item_id,
                song_id: song.song_id,
            },
            metadata: TrackMetadata {
                title: song.title,
                artist: song.artist,
            },
            path: PathBuf::from(song.file_path),
            cue_in: Duration::from_secs_f64(song.cue_in.max(0.0)),
            cue_out: Duration::from_secs_f64(song.cue_out.max(0.0)),
            cross_start_next: Duration::from_secs_f64(song.cross_start_next.max(0.0)),
            analyzed: song.analyzed,
        }
    }

    fn replace_current(&mut self, mode: ReplaceMode) -> PipelineOperation {
        let Some(current) = self.queue.current_song_info() else {
            self.state = PipelineState::Stopped;
            return PipelineOperation::Stop;
        };
        let current_track = Self::track(current);
        let anchor = self.queue.anchor_after_current();
        let next = self.queue.peek_next_song().map(Self::track);
        self.planned_next = next.as_ref().map(|track| (track.key.clone(), anchor));
        let next = next.map(|track| {
            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, Some(&track));
            PlannedNext { track, transition }
        });
        self.generation += 1;
        if matches!(mode, ReplaceMode::InitialReplaceFromStopped) {
            self.output_epoch = self.output_epoch.wrapping_add(1).max(1);
        }
        // A new output identity makes the reconnect chain of the OLD output
        // stale immediately: the shared token flips to 0, so a reconnect
        // that was already queued for the old identity is dropped by the
        // executor's pre-pipeline token guard (a stale reconnect must never
        // touch the new pipeline), a retry timer of the old chain is
        // rejected by `reconnect_if_current`, and a late ReconnectFinished
        // cannot influence any chain of the new output. Every identity
        // change (skip, play from Stopped, idle resume) funnels through
        // here; `stop_after_current` invalidates on its own path. The
        // known-disconnected marker is handled right below — the chain
        // invalidation never touches it.
        self.invalidate_reconnect_chain();
        // A new output identity: a known-disconnected marker for an older
        // output must never trigger a reconnect of the new one.
        if self.known_disconnected_output != Some((self.generation, self.output_epoch)) {
            self.known_disconnected_output = None;
        }
        PipelineOperation::Replace(Box::new(PairPlan {
            mode,
            generation: self.generation,
            output_epoch: self.output_epoch,
            current: current_track,
            next,
        }))
    }

    pub(crate) async fn play(&mut self) -> PipelineOperation {
        // A manual play is a newer playback decision: any in-flight
        // automatic resume must not be able to overwrite it later.
        self.pending_resume = None;
        if self.state == PipelineState::Stopped {
            if self.queue.current_song_info().is_none() {
                // An empty queue is not immediately terminal: Auto DJ /
                // schedule fill may have rows to add. Give it a chance and
                // reload from the DB before falling back to Stopped; only a
                // failing fill is retried, bounded, so a transient DB error
                // cannot leave the station dead.
                let mut attempts = 0u32;
                loop {
                    let ran = self.queue.refill().await;
                    self.queue.reload_from_db().await;
                    if self.queue.current_song_info().is_some() || ran || attempts >= 2 {
                        break;
                    }
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(250 * u64::from(attempts))).await;
                }
                if self.queue.current_song_info().is_none() {
                    self.idle = true;
                    return PipelineOperation::Stop;
                }
                self.push_queue_update().await;
            } else {
                // The queue may hold fewer upcoming songs than the configured
                // AutoDJ songs_ahead minimum (e.g. songs were removed while
                // the station was stopped). Top the database queue up before
                // starting; the fill is count-based and no-ops when nothing
                // is missing. The in-memory copy is not reloaded here — the
                // first commit (handover or skip) reloads it, and the panel
                // queue view reads the database directly.
                self.queue.refill().await;
                self.push_queue_update().await;
            }
            let operation = self.replace_current(ReplaceMode::InitialReplaceFromStopped);
            self.idle = false;
            self.state = PipelineState::Playing;
            operation
        } else {
            self.idle = false;
            self.state = PipelineState::Playing;
            PipelineOperation::SetPlaying(true)
        }
    }

    pub(crate) fn pause(&mut self) -> PipelineOperation {
        // A manual pause is a newer playback decision: it ends the auto-idle
        // state (no future idle tick may start playback), invalidates any
        // in-flight automatic resume, and a stale completion must not flip
        // the station back to Playing. It also ends the reconnect retry
        // chain: retries are only eligible while Playing (Model B), so a
        // paused station must not keep an active token that can never fire —
        // a later Play plus a fresh disconnect starts a new chain.
        self.idle = false;
        self.pending_resume = None;
        self.invalidate_reconnect_chain();
        self.state = PipelineState::Paused;
        PipelineOperation::SetPlaying(false)
    }

    pub(crate) fn stop(&mut self) -> PipelineOperation {
        // A manual stop is both a newer playback decision (invalidate any
        // in-flight automatic resume) and the end of any reconnect retry
        // chain (a fired timer must never wake a stopped station).
        self.idle = false;
        self.pending_resume = None;
        self.invalidate_reconnect_chain();
        // A manual stop cancels recovery completely: a later Play must not
        // restore an output that was broken before the stop.
        self.known_disconnected_output = None;
        self.state = PipelineState::Stopped;
        PipelineOperation::Stop
    }

    pub(crate) fn idle(&self) -> bool {
        self.idle
    }

    /// Periodic auto-resume hook for a station that stopped because its
    /// queue drained (idle), never after a manual stop. Asks AutoDJ /
    /// schedule fill for new content and, once the queue holds a current
    /// song again, starts playback exactly like an initial play(). Returns
    /// no operation while the queue stays empty or while an automatic
    /// resume is already in flight, so the runtime keeps polling on the
    /// next tick without touching the stopped pipeline.
    ///
    /// The controller state is NOT advanced here: the caller (runtime)
    /// submits the replace to the pipeline executor and reports the outcome
    /// back through `on_resume_result`. A failed replace must leave the
    /// station idle/retryable — marking it `Playing` before the pipeline
    /// actually started would suppress every later automatic retry. The
    /// returned attempt id correlates the completion with this exact
    /// attempt; a newer user decision (play/pause/stop) invalidates it.
    pub(crate) async fn resume_from_idle(&mut self) -> Option<(PipelineOperation, u64)> {
        if !self.idle || self.pending_resume.is_some() {
            return None;
        }
        self.queue.refill().await;
        self.queue.reload_from_db().await;
        self.queue.current_song_info()?;
        self.push_queue_update().await;
        self.resume_attempt_seq = self.resume_attempt_seq.wrapping_add(1).max(1);
        let attempt_id = self.resume_attempt_seq;
        self.pending_resume = Some(attempt_id);
        Some((self.replace_current(ReplaceMode::InitialReplaceFromStopped), attempt_id))
    }

    /// Outcome of an automatic idle resume, reported by the pipeline
    /// executor after the replace ran. Only the outcome of the current
    /// attempt is applied: a stale completion (a newer user decision or a
    /// newer resume attempt superseded it) must never touch `idle`, `state`,
    /// or the user's decision. Success moves the controller to `Playing`; a
    /// failure keeps it idle so the next tick retries instead of leaving the
    /// radio dead.
    pub(crate) fn on_resume_result(&mut self, attempt_id: u64, result: Result<(), PipelineError>) {
        if self.pending_resume != Some(attempt_id) {
            return;
        }
        self.pending_resume = None;
        match result {
            Ok(()) => {
                self.idle = false;
                self.state = PipelineState::Playing;
            }
            Err(error) => {
                tracing::warn!(station_id = %self.station_id, %error, "automatic idle resume failed; retrying on the next tick");
                self.idle = true;
                self.state = PipelineState::Stopped;
            }
        }
    }

    /// Starts a new reconnect retry chain and returns its token. Every
    /// reconnect attempt (event-driven or manual) supersedes whatever chain
    /// was active before: older retry timers carry a different token and are
    /// ignored when they fire, and queued reconnect operations observe the
    /// new token through the shared atomic.
    pub(crate) fn begin_reconnect_chain(&mut self) -> u64 {
        self.reconnect_retry_seq = self.reconnect_retry_seq.wrapping_add(1).max(1);
        let token = self.reconnect_retry_seq;
        self.active_reconnect_retry = Some(token);
        self.reconnect_token_shared.set_token(token);
        token
    }

    /// Whether a retry timer with `token` still belongs to the active
    /// reconnect retry chain.
    pub(crate) fn reconnect_retry_is_current(&self, token: u64) -> bool {
        self.active_reconnect_retry == Some(token)
    }

    /// Shared reconnect chain state, handed to queued reconnect operations
    /// so they can skip themselves when their chain was superseded before
    /// the pipeline ran.
    pub(crate) fn reconnect_token_shared(&self) -> std::sync::Arc<ReconnectShared> {
        self.reconnect_token_shared.clone()
    }

    /// Ends the reconnect chain lifecycle after a reconnect that actually
    /// SUCCEEDED: the bookkeeping is cleared AND the known-disconnected
    /// marker is removed — but ONLY when this chain is still the one
    /// responsible for the currently known-disconnected output.
    ///
    /// The token guard is the whole point: the executor marks a chain
    /// completed before `ReconnectFinished` reaches the runtime, so a newer
    /// disconnect of the SAME output (same generation/output_epoch) can
    /// start a fresh chain Y in that window. A delayed success of the older
    /// chain X must neither end Y nor clear the marker that disconnect #2
    /// set — the success of X describes a connection state BEFORE the
    /// second drop. `generation`/`output_epoch` alone cannot distinguish
    /// the two: both disconnects concern the exact same output identity,
    /// only the chain token / event order differ.
    ///
    /// Marker removal is therefore tied to the OUTPUT the chain was bound
    /// to (`active_reconnect_output`), captured before the bookkeeping is
    /// cleared — never to the bare current identity.
    pub(crate) fn on_reconnect_succeeded(&mut self, token: u64) {
        if self.active_reconnect_retry != Some(token) {
            return;
        }

        let connected_output = self.active_reconnect_output;

        self.active_reconnect_retry = None;
        self.active_reconnect_output = None;
        self.reconnect_token_shared.invalidate();

        if self.known_disconnected_output == connected_output {
            self.known_disconnected_output = None;
        }
    }

    /// Ends the reconnect chain lifecycle: called when the chain finished —
    /// automatic reconnect success, manual one-shot success/failure
    /// (`ReconnectFinished`), or a retry that became ineligible (paused /
    /// stopped / stale output). A later disconnect for the same output is a
    /// fresh event and must start a new chain instead of being coalesced
    /// into the finished one. The token guard keeps an older chain's
    /// cleanup from ending a newer chain.
    pub(crate) fn end_reconnect_chain(&mut self, token: u64) {
        if self.active_reconnect_retry == Some(token) {
            self.active_reconnect_retry = None;
            self.active_reconnect_output = None;
            self.reconnect_token_shared.invalidate();
        }
    }

    /// Invalidates the reconnect retry chain entirely (stop, shutdown): no
    /// timer may fire a reconnect for a stopped station and no queued
    /// reconnect operation may touch the pipeline afterwards.
    fn invalidate_reconnect_chain(&mut self) {
        self.active_reconnect_retry = None;
        self.active_reconnect_output = None;
        self.reconnect_token_shared.invalidate();
    }

    /// Token of the chain that the most recent `reconnect()` started; the
    /// runtime attaches it to the reconnect pipeline action so its retry
    /// timers can be correlated and invalidated.
    pub(crate) fn current_reconnect_token(&self) -> u64 {
        self.active_reconnect_retry.unwrap_or(0)
    }

    /// Binds the active reconnect chain to the current output so duplicate
    /// disconnects during a manual attempt are coalesced exactly like during
    /// an automatic one. Called by the runtime after a manual reconnect
    /// built its target.
    pub(crate) fn bind_reconnect_to_output(&mut self, generation: u64, output_epoch: u64) {
        if self.active_reconnect_retry.is_some() {
            self.active_reconnect_output = Some((generation, output_epoch));
        }
    }

    /// Starts a reconnect chain for the current output when it is known to
    /// be disconnected — called by the runtime right after the resume
    /// `SetPlaying(true)` was queued, because the pipeline does not restore
    /// the connection on its own. The disconnected marker is factual state
    /// and is NOT consumed here: it stays until a reconnect for exactly this
    /// output succeeds (or the output is replaced / a manual stop clears
    /// it), so an interrupted recovery (e.g. another Pause) can always be
    /// retried by the next Play. Returns no operation when the output is not
    /// known-disconnected or an active chain already covers it.
    pub(crate) async fn resume_reconnect_for_break(&mut self) -> Option<Result<PipelineOperation, PipelineError>> {
        if self.known_disconnected_output != Some((self.generation, self.output_epoch)) {
            return None;
        }
        if self.active_reconnect_retry.is_some()
            && self.active_reconnect_output == Some((self.generation, self.output_epoch))
            && !self.reconnect_token_shared.is_current_completed()
        {
            return None;
        }
        self.begin_reconnect_chain();
        self.active_reconnect_output = Some((self.generation, self.output_epoch));
        Some(self.build_reconnect_target().await)
    }

    /// Whether the CURRENT output is known to be disconnected and awaiting
    /// recovery.
    pub(crate) fn is_output_known_disconnected(&self) -> bool {
        self.known_disconnected_output == Some((self.generation, self.output_epoch))
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn output_epoch(&self) -> u64 {
        self.output_epoch
    }

    pub(crate) async fn reconnect(&mut self) -> Result<PipelineOperation, PipelineError> {
        // One-shot manual reconnect. The target is built FIRST, without
        // touching the chain state: a failed refresh leaves the automatic
        // chain (its queued operation and its timers) completely untouched.
        // Superseding only happens once the target exists — cancellation
        // observed by the executor is one-way and cannot be rolled back.
        let operation = self.build_reconnect_target().await?;
        self.begin_reconnect_chain();
        Ok(operation)
    }

    /// Event-driven reconnect for a dropped output. A duplicate disconnect
    /// for an output that already has an active automatic chain is ignored:
    /// the existing chain keeps running (same token, same attempt counter,
    /// same backoff) instead of minting a second chain. A disconnect that
    /// arrives AFTER the executor marked the chain completed (reconnect
    /// succeeded, `ReconnectFinished` not processed yet) is a fresh event.
    pub(crate) async fn reconnect_for_output(
        &mut self,
        generation: u64,
        output_epoch: u64,
    ) -> Option<Result<PipelineOperation, PipelineError>> {
        // "Current output identity" is independent of the playback state:
        // output_is_current() additionally requires Playing, which would
        // wrongly discard a disconnect received while paused.
        if generation != self.generation || output_epoch != self.output_epoch {
            return None;
        }
        // The current output is (still) disconnected — factual state,
        // recorded before any per-state decision below.
        self.known_disconnected_output = Some((generation, output_epoch));
        if self.state != PipelineState::Playing {
            // Not Playing: no reconnect now; the marker survives for the next Play.
            return None;
        }
        let chain_active = self.active_reconnect_retry.is_some() && self.active_reconnect_output == Some((generation, output_epoch));
        if chain_active && !self.reconnect_token_shared.is_current_completed() {
            tracing::debug!(station_id = %self.station_id, generation, output_epoch, "ignoring duplicate output disconnect; reconnect chain already active");
            return None;
        }
        self.begin_reconnect_chain();
        self.active_reconnect_output = Some((generation, output_epoch));
        Some(self.build_reconnect_target().await)
    }

    /// Builds a reconnect operation for the CURRENT chain without touching
    /// the chain token: a retry attempt must never supersede its own chain.
    async fn build_reconnect_target(&mut self) -> Result<PipelineOperation, PipelineError> {
        let (endpoint, password) = crate::icecast::models::get_connection_config(&self.db)
            .await
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        self.target = IcecastTarget::parse(&endpoint, password, &self.target.mount, self.target.stream_name.clone())?;
        Ok(PipelineOperation::Reconnect(self.target.clone()))
    }
    fn output_is_current(&self, generation: u64, output_epoch: u64) -> bool {
        generation == self.generation && output_epoch == self.output_epoch && self.state == PipelineState::Playing
    }

    /// Prepares the next attempt of an EXISTING reconnect retry chain. The
    /// token stays the same across all attempts of the chain; only a
    /// superseding reconnect mints a new one. A target-refresh error
    /// surfaces as `Err` and is retryable by the runtime — it never
    /// invalidates the chain.
    pub(crate) async fn reconnect_if_current(
        &mut self,
        generation: u64,
        output_epoch: u64,
        token: u64,
    ) -> Result<Option<PipelineOperation>, PipelineError> {
        if !self.reconnect_retry_is_current(token) || !self.output_is_current(generation, output_epoch) {
            // Stale chain (superseded or invalidated by a stop) or stale
            // output: the retry is dropped without touching the pipeline.
            return Ok(None);
        }
        self.build_reconnect_target().await.map(Some)
    }

    async fn stop_after_current(&mut self) -> PipelineOperation {
        self.queue.finish_current().await;
        self.generation += 1;
        // The queue drained on its own: this is an idle wait for new content,
        // not a manual stop, so the runtime may auto-resume later.
        self.idle = true;
        // The station is not broadcasting anymore: a reconnect retry timer
        // must not fire a reconnect for a stopped output.
        self.invalidate_reconnect_chain();
        self.state = PipelineState::Stopped;
        // No StatusEvent is pushed anywhere else on this path; without this
        // the panel live feed keeps the last playing state forever and shows
        // an exhausted station as still broadcasting.
        let _ = self.status_tx.send(StatusEvent::State {
            playing: false,
            song_index: self.queue.current_song_index(),
            total: self.queue.song_count(),
            elapsed: 0,
            title: String::new(),
            artist: String::new(),
            duration: 0,
        });
        PipelineOperation::Stop
    }

    pub(crate) async fn skip(&mut self) -> Result<PipelineOperation, PipelineError> {
        // A manual skip is a newer playback decision: a stale automatic
        // resume completion must not overwrite it later.
        self.pending_resume = None;
        let Some(current) = self.queue.current_song_info() else {
            return Ok(self.stop_after_current().await);
        };
        let current_key = TrackKey {
            queue_item_id: current.queue_item_id,
            song_id: current.song_id,
        };
        let mut next = self.queue.successor_after(&current_key);
        if next.is_none() {
            // The in-memory queue can lag behind the database: Auto DJ refills
            // (triggered manually or by a schedule) insert rows without the
            // live streamer reloading. Retry once against the DB before
            // treating the queue as exhausted.
            self.queue.reload_from_db().await;
            next = self.queue.successor_after(&current_key);
        }
        if next.is_none() {
            // The queue is exhausted in the DB too. Give Auto DJ / schedule
            // fill a chance to add successors before stopping. A clean no-op
            // (Auto DJ disabled, nothing to pick) stops immediately; only a
            // failing fill is retried, a bounded number of times, so a
            // transient DB error cannot kill the radio for good.
            let mut attempts = 0u32;
            loop {
                let ran = self.queue.refill().await;
                self.queue.reload_from_db().await;
                next = self.queue.successor_after(&current_key);
                if next.is_some() || ran || attempts >= 2 {
                    break;
                }
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempts))).await;
            }
        }
        let Some(next) = next else {
            return Ok(self.stop_after_current().await);
        };
        let next_key = TrackKey {
            queue_item_id: next.queue_item_id,
            song_id: next.song_id,
        };
        let expected_generation = self.generation;
        let anchor = self.queue.anchor_after_current();
        let _ = self.queue.commit_current(&next_key, anchor).await;
        let operation = self.replace_current(ReplaceMode::ActiveReplace {
            expected_generation,
            expected_current: current_key,
        });
        self.publish_song_change();
        self.push_queue_update().await;
        Ok(operation)
    }

    pub(crate) async fn reload(&mut self, songs: Vec<SongInfo>, align_next: bool) -> Result<Option<PipelineOperation>, PipelineError> {
        let was_stopped = matches!(self.state, PipelineState::Stopped);
        let retain_missing_current = !was_stopped;
        self.queue.reload_songs(songs, retain_missing_current);
        if was_stopped {
            // The station was started while its database queue was still
            // empty, leaving an idle streamer behind. Once songs arrive
            // (manual add, Auto DJ refill, schedule) playback must begin;
            // play() stays a no-op while the queue remains empty.
            return Ok(if self.queue.current_song_info().is_some() {
                Some(self.play().await)
            } else {
                None
            });
        }
        if !align_next || !matches!(self.state, PipelineState::Playing | PipelineState::Paused) {
            return Ok(None);
        }
        let Some((staged_key, _)) = self.planned_next.clone() else {
            return Ok(None);
        };
        let Some(current) = self.queue.current_song_info() else {
            return Ok(None);
        };
        let next = self.queue.peek_next_song();
        let next_key = next.as_ref().map(|song| TrackKey {
            queue_item_id: song.queue_item_id,
            song_id: song.song_id,
        });
        if next_key.as_ref() == Some(&staged_key) {
            return Ok(None);
        }
        let current_track = Self::track(current);
        let anchor = self.queue.anchor_after_current();
        let replacement = next.map(|song| {
            let track = Self::track(song);
            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, Some(&track));
            PlannedNext { track, transition }
        });
        self.planned_next = next_key.map(|key| (key, anchor));
        tracing::info!(station_id = %self.station_id, "realigning staged next after queue change");
        Ok(Some(PipelineOperation::Roll(Box::new(RollingPlan {
            generation: self.generation,
            current: current_track.key,
            change: RollingChange::ReplaceNext {
                expected_next: staged_key,
                replacement,
            },
        }))))
    }

    pub(crate) fn update_config(&mut self, playback: StationPlaybackConfig) -> Option<PipelineOperation> {
        let output_changed = self.playback.output != playback.output;
        self.playback = playback;
        output_changed.then_some(PipelineOperation::ApplyOutput(self.playback.output))
    }

    pub(crate) fn driver(&self) -> PipelineDriver {
        self.driver.clone()
    }

    pub(crate) async fn push_queue_update(&self) {
        let message = self.queue.queue_json().await;
        let _ = self.queue_tx.send(message);
    }

    pub(crate) async fn trim_played_items(&self) {
        self.queue.trim_played_items().await;
    }

    fn publish_song_change(&self) {
        let idx = self.queue.current_song_index();
        if let Some(song) = self.queue.current_song_info() {
            let _ = self.status_tx.send(StatusEvent::SongChange {
                song_index: idx,
                total: self.queue.song_count(),
                elapsed: 0,
                title: song.title,
                artist: song.artist,
                duration: song.duration,
            });
        }
    }

    pub(crate) async fn status(&self) -> Result<StatusEvent, PipelineError> {
        // A failed or missing snapshot is a pipeline fault, never a legal
        // `Stopped`: the API and monitoring must be able to tell an unhealthy
        // streamer apart from a station that is intentionally stopped.
        let PipelineSnapshot { state, elapsed } = match self.driver.execute(PipelineOperation::Snapshot).await? {
            PipelineOperationResult::Snapshot(snapshot) => snapshot,
            PipelineOperationResult::Unit => {
                return Err(PipelineError::Pipeline(
                    "pipeline driver returned no snapshot for a status request".into(),
                ));
            }
        };
        let idx = self.queue.current_song_index();
        let song = self.queue.current_song_info();
        Ok(StatusEvent::State {
            playing: state == PipelineState::Playing,
            song_index: idx,
            total: self.queue.song_count(),
            elapsed: elapsed.as_secs(),
            title: song.as_ref().map_or_else(String::new, |song| song.title.clone()),
            artist: song.as_ref().map_or_else(String::new, |song| song.artist.clone()),
            duration: song.map_or(0, |song| song.duration),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::db;
    use std::str::FromStr;
    use std::sync::Arc;

    use super::*;
    use crate::streamer::driver::{PipelineDriver, PipelineOperation};
    use crate::streamer::runtime::StationRuntime;
    use crate::streamer::testsupport::{self, queued_song, queued_songs, Call, RecordingPipeline};
    use tokio::sync::{broadcast, mpsc};
    use uuid::Uuid;

    /// Builds a real `StationController` around the shared recording
    /// pipeline, hiding the recurring broadcast channels, queue manager,
    /// station id, playback config, driver, target, and reconnect/resume
    /// defaults. Tests reach the controller through `harness.controller`
    /// (fields stay private to the controller module) and read pipeline
    /// effects through `harness.pipeline`.
    struct Harness {
        controller: StationController,
        pipeline: Arc<RecordingPipeline>,
    }

    impl Harness {
        fn new(db: PgPool, pipeline: Arc<RecordingPipeline>, songs: Vec<SongInfo>) -> Self {
            let (status_tx, _) = broadcast::channel(1);
            let (queue_tx, _) = broadcast::channel(1);
            let station_id = Uuid::new_v4();
            let queue = Arc::new(QueueManager::new(db.clone(), station_id, String::new(), songs, 0));
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
                active_reconnect_retry: None,
                reconnect_retry_seq: 0,
                active_reconnect_output: None,
                reconnect_token_shared: std::sync::Arc::default(),
                known_disconnected_output: None,
            };
            Self { controller, pipeline }
        }

        /// A stopped controller with the given queue over the intentionally
        /// unavailable database.
        fn stopped(songs: Vec<SongInfo>) -> Self {
            Self::new(testsupport::unavailable_db(), Arc::new(RecordingPipeline::new()), songs)
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

        /// A playing controller: `play()` has run and reported a Replace.
        async fn playing(songs: Vec<SongInfo>) -> Self {
            let mut harness = Self::stopped(songs);
            assert!(matches!(harness.controller.play().await, PipelineOperation::Replace(_)));
            assert_eq!(harness.controller.state, PipelineState::Playing);
            harness
        }

        /// Splits the harness back into its controller and pipeline for
        /// tests that drive the controller directly.
        fn into_parts(self) -> (StationController, Arc<RecordingPipeline>) {
            (self.controller, self.pipeline)
        }
    }

    #[tokio::test]
    async fn stale_events_do_not_replace_or_reconnect() {
        let song = queued_song("current", 0);
        let pipeline = Arc::new(RecordingPipeline::new());
        let mut harness = Harness::with_pipeline(pipeline.clone(), vec![song.clone()]);
        harness.controller.generation = 1;
        let _ = harness
            .controller
            .handle_event(PipelineEvent::DecodeFailed {
                generation: 0,
                track: StationController::track(song).key,
                message: "stale".into(),
            })
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
        // No database is reachable here, so the AutoDJ refill attempt inside
        // play() cannot produce songs and the controller must stay Stopped
        // (the E2E suite covers the case where the refill succeeds).
        let harness = Harness::stopped(Vec::new());
        let mut controller = harness.controller;
        let pipeline = harness.pipeline;

        assert!(matches!(controller.play().await, PipelineOperation::Stop));
        assert_eq!(controller.state, PipelineState::Stopped);
        assert_eq!(pipeline.count(Call::Replace), 0);
    }

    #[tokio::test]
    async fn next_decode_failure_replaces_only_the_failed_terminal_branch() {
        let current = queued_song("", 0);
        let failed = queued_song("", 1);
        let successor = queued_song("", 2);
        let harness = Harness::stopped(vec![current.clone(), failed.clone(), successor.clone()]);
        let mut controller = harness.controller;
        let failed_key = StationController::track(failed).key;
        controller.state = PipelineState::Playing;
        controller.generation = 1;
        controller.output_epoch = 1;
        controller.planned_next = Some((failed_key.clone(), controller.queue.anchor_after_current()));

        let operation = controller
            .handle_event(PipelineEvent::DecodeFailed {
                generation: 1,
                track: failed_key.clone(),
                message: "broken next".into(),
            })
            .await
            .unwrap()
            .unwrap();
        let PipelineOperation::Roll(plan) = operation else {
            panic!("next failure must issue a rolling replacement");
        };
        assert_eq!(plan.current.queue_item_id, current.queue_item_id);
        let RollingChange::ReplaceNext {
            expected_next,
            replacement: Some(replacement),
        } = plan.change
        else {
            panic!("next failure must replace its terminal branch");
        };
        assert_eq!(expected_next, failed_key);
        assert_eq!(replacement.track.key.queue_item_id, successor.queue_item_id);
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
        let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
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
        let a = queued_song("A", 0);
        let mut harness = Harness::stopped(Vec::new());
        assert!(matches!(harness.controller.play().await, PipelineOperation::Stop));
        assert_eq!(harness.controller.state, PipelineState::Stopped);
        let pipeline = harness.pipeline.clone();

        let operation = harness.controller.reload(vec![a], false).await.unwrap();
        let Some(PipelineOperation::Replace(plan)) = operation else {
            panic!("reload into a stopped controller with songs must issue a replace");
        };
        assert!(matches!(plan.mode, ReplaceMode::InitialReplaceFromStopped));
        assert_eq!(harness.controller.state, PipelineState::Playing);
        assert_eq!(
            pipeline.count(Call::Replace),
            0,
            "replace is executed by the runtime, not the controller"
        );

        let mut harness = Harness::stopped(Vec::new());
        assert!(matches!(harness.controller.play().await, PipelineOperation::Stop));
        let operation = harness.controller.reload(vec![], false).await.unwrap();
        assert!(operation.is_none(), "an empty reload must not start anything");
        assert_eq!(harness.controller.state, PipelineState::Stopped);
    }

    #[tokio::test]
    async fn reload_realigns_staged_next_to_reordered_head() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let x = queued_song("X", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, b.queue_item_id);

        let operation = controller
            .reload(vec![a.clone(), x.clone(), b.clone(), c.clone()], true)
            .await
            .unwrap();
        let Some(PipelineOperation::Roll(plan)) = operation else {
            panic!("reorder reload must issue a rolling replacement");
        };
        assert_eq!(plan.current.queue_item_id, a.queue_item_id);
        let RollingChange::ReplaceNext {
            expected_next,
            replacement,
        } = plan.change
        else {
            panic!("reorder reload must use ReplaceNext");
        };
        assert_eq!(expected_next.queue_item_id, b.queue_item_id);
        let replacement = replacement.expect("replacement must be staged");
        assert_eq!(replacement.track.key.queue_item_id, x.queue_item_id);
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, x.queue_item_id);
    }

    #[tokio::test]
    async fn reload_without_align_keeps_the_staged_next() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let x = queued_song("X", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone()]).await.into_parts();
        let operation = controller.reload(vec![a, x, b.clone()], false).await.unwrap();
        assert!(operation.is_none(), "non-aligning reload must not touch the pipeline");
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, b.queue_item_id);
    }

    #[tokio::test]
    async fn reload_with_unchanged_head_does_not_roll() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let x = queued_song("X", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let operation = controller.reload(vec![a, b.clone(), c.clone(), x], true).await.unwrap();
        assert!(operation.is_none(), "append-only reload must not roll");
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, b.queue_item_id);
    }

    #[tokio::test]
    async fn reload_exhausting_queue_drops_the_staged_next() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone()]).await.into_parts();
        let operation = controller.reload(vec![a.clone()], true).await.unwrap();
        let Some(PipelineOperation::Roll(plan)) = operation else {
            panic!("exhausting reload must issue a roll");
        };
        let RollingChange::ReplaceNext {
            expected_next,
            replacement,
        } = plan.change
        else {
            panic!("exhausting reload must use ReplaceNext");
        };
        assert_eq!(expected_next.queue_item_id, b.queue_item_id);
        assert!(replacement.is_none(), "no successor may be staged after exhaustion");
        assert!(controller.planned_next.is_none());
    }

    #[tokio::test]
    async fn stale_handover_after_realignment_is_ignored() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let x = queued_song("X", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        controller.reload(vec![a.clone(), x.clone(), b.clone()], true).await.unwrap();
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, x.queue_item_id);

        // The pipeline handed over to the OLD staged next (B) right after the swap:
        // the queue must not consume B because it will never play.
        let operation = controller
            .handle_event(PipelineEvent::Handover {
                generation: 1,
                current: b_key,
            })
            .await;
        assert!(operation.is_none(), "stale handover must be ignored");
        assert_eq!(controller.queue.current_song_info().unwrap().queue_item_id, a.queue_item_id);
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, x.queue_item_id);
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
        assert!(matches!(operation, PipelineOperation::Stop));
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
        assert!(matches!(operation, PipelineOperation::Stop));
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
        assert!(matches!(operation, PipelineOperation::Stop));
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
        let song = queued_song("A", 0);
        let fresh = queued_song("B", 1);
        let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

        let operation = controller.skip().await.unwrap();
        assert!(matches!(operation, PipelineOperation::Stop));
        assert!(controller.idle());

        controller.queue.reload_songs(vec![fresh], false);
        let (_operation, attempt_id) = controller
            .resume_from_idle()
            .await
            .expect("an idle station must resume once the queue fills");
        assert!(controller.idle());

        let operation = controller.play().await;
        assert!(matches!(operation, PipelineOperation::Replace(_)));
        assert_eq!(controller.state, PipelineState::Playing);
        assert!(!controller.idle());

        controller.on_resume_result(attempt_id, Err(PipelineError::Pipeline("boom: stale resume failed".into())));
        assert_eq!(controller.state, PipelineState::Playing);
        assert!(!controller.idle());
    }

    #[tokio::test]
    async fn stale_successful_resume_does_not_override_a_manual_pause() {
        let song = queued_song("A", 0);
        let fresh = queued_song("B", 1);
        let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

        let operation = controller.skip().await.unwrap();
        assert!(matches!(operation, PipelineOperation::Stop));
        assert!(controller.idle());

        controller.queue.reload_songs(vec![fresh], false);
        let (_operation, attempt_id) = controller
            .resume_from_idle()
            .await
            .expect("an idle station must resume once the queue fills");

        let operation = controller.pause();
        assert!(matches!(operation, PipelineOperation::SetPlaying(false)));
        assert_eq!(controller.state, PipelineState::Paused);

        controller.on_resume_result(attempt_id, Ok(()));
        assert_eq!(controller.state, PipelineState::Paused);
    }

    #[tokio::test]
    async fn reconnect_retry_token_is_invalidated_by_a_newer_chain_and_stop() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token_a = controller.begin_reconnect_chain();
        assert!(controller.reconnect_retry_is_current(token_a));

        let token_b = controller.begin_reconnect_chain();
        assert!(
            !controller.reconnect_retry_is_current(token_a),
            "a superseded chain must be invalidated"
        );
        assert!(controller.reconnect_retry_is_current(token_b));

        controller.stop();
        assert!(
            !controller.reconnect_retry_is_current(token_b),
            "a stop must invalidate the retry chain"
        );
    }

    #[tokio::test]
    async fn duplicate_disconnects_for_the_same_output_start_a_single_chain() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();
        assert_eq!(controller.state, PipelineState::Playing);

        // The later two disconnects are duplicates: a new chain would reset
        // the backoff and enqueue redundant reconnects.
        let first = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped".into(),
            })
            .await;
        assert!(first.is_some(), "the first disconnect must produce a reconnect attempt");
        let duplicate_a = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped".into(),
            })
            .await;
        let duplicate_b = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped".into(),
            })
            .await;
        assert!(
            duplicate_a.is_none(),
            "a duplicate disconnect must be coalesced into the existing chain"
        );
        assert!(
            duplicate_b.is_none(),
            "a duplicate disconnect must be coalesced into the existing chain"
        );

        assert!(controller.reconnect_retry_is_current(1));
        assert_eq!(controller.current_reconnect_token(), 1);
    }

    #[tokio::test]
    async fn a_new_disconnect_after_a_successful_chain_starts_a_fresh_one() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token = controller.begin_reconnect_chain();
        controller.end_reconnect_chain(token);
        assert!(!controller.reconnect_retry_is_current(token));

        let result = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped again".into(),
            })
            .await;
        assert!(result.is_some(), "a disconnect after a finished chain must start a new chain");
        assert_ne!(controller.current_reconnect_token(), token, "the new chain must mint a fresh token");
    }

    #[tokio::test]
    async fn failed_manual_reconnect_leaves_the_automatic_chain_untouched() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token_x = controller.begin_reconnect_chain();
        assert!(controller.reconnect_retry_is_current(token_x));

        // A manual reconnect is requested, but building its target fails
        // (unreachable DB). The chain state must be completely untouched —
        // the manual attempt never superseded anything, so the executor
        // still sees token X as current and the queued automatic operation
        // and any retry timers keep working.
        let result = controller.reconnect().await;
        assert!(result.is_err(), "the manual reconnect must surface its refresh error");
        assert!(controller.reconnect_retry_is_current(token_x));
        assert_eq!(
            controller.reconnect_token_shared().token(),
            token_x,
            "the shared executor state must not have observed a supersession"
        );
        assert!(!controller.reconnect_token_shared().is_current_completed());
    }

    #[tokio::test]
    async fn disconnect_after_completed_reconnect_starts_a_fresh_chain_before_reconnect_succeeded() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        // Chain X runs; the pipeline reconnect succeeds. The executor marks
        // the chain completed in the shared state, but ReconnectFinished
        // has NOT reached the runtime yet (simulated window).
        let token_x = controller.begin_reconnect_chain();
        controller.bind_reconnect_to_output(controller.generation(), controller.output_epoch());
        controller.reconnect_token_shared().mark_completed(token_x);

        // A disconnect for the same output arrives in that window: it must
        // NOT be coalesced into the finished chain — a fresh chain starts
        // and sets the known-disconnected marker for the SAME output.
        let result = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped again".into(),
            })
            .await;
        assert!(result.is_some(), "a disconnect after a completed reconnect must never be lost");
        let fresh = controller.current_reconnect_token();
        assert_ne!(fresh, token_x, "a fresh chain must start after the completed one");
        assert!(
            controller.is_output_known_disconnected(),
            "disconnect #2 must have set the marker for the current output"
        );

        // The delayed ReconnectFinished(X, succeeded: true) arrives through
        // the REAL success-completion path. X is stale (Y is active): it
        // must neither end the new chain nor clear the marker that
        // disconnect #2 set.
        controller.on_reconnect_succeeded(token_x);
        assert!(
            controller.reconnect_retry_is_current(fresh),
            "stale success must not end the newer chain"
        );
        assert!(
            controller.is_output_known_disconnected(),
            "stale success must not clear the marker belonging to the newer disconnect"
        );
    }

    #[tokio::test]
    async fn manual_chain_binds_to_the_output_for_duplicate_coalescing() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token = controller.begin_reconnect_chain();
        controller.bind_reconnect_to_output(controller.generation(), controller.output_epoch());
        assert!(controller.reconnect_retry_is_current(token));

        let duplicate = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: controller.generation(),
                output_epoch: controller.output_epoch(),
                message: "output dropped".into(),
            })
            .await;
        assert!(
            duplicate.is_none(),
            "a duplicate disconnect during a pending manual reconnect must be coalesced"
        );
        assert!(controller.reconnect_retry_is_current(token));

        controller.end_reconnect_chain(token);
        let fresh = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: controller.generation(),
                output_epoch: controller.output_epoch(),
                message: "output dropped again".into(),
            })
            .await;
        assert!(
            fresh.is_some(),
            "a disconnect after the manual chain ended must start a fresh chain"
        );
        assert_ne!(controller.current_reconnect_token(), token);
    }

    /// An isolated, migrated test database for the runtime reconnect tests:
    /// a fresh `*_test_<uuid>` database is created per test through the real
    /// `sqlx` connection options (no hand-rolled URL parsing — sslmode,
    /// IPv6, and query parameters are preserved), migrated with
    /// `db::run_migrations` (no schema copy), and dropped again by
    /// `cleanup()` after the pool is closed. Returns `None` only when
    /// `DATABASE_URL` is absent (the tests then skip); any configured
    /// connection, setup, or migration failure is a test failure, never a
    /// silent skip.
    async fn reconnect_test_db() -> Option<ReconnectTestDb> {
        let database_url = std::env::var("DATABASE_URL").ok()?;
        let options =
            sqlx::postgres::PgConnectOptions::from_str(&database_url).unwrap_or_else(|error| panic!("invalid DATABASE_URL: {error}"));
        let base_db = options.get_database().map(str::to_owned).unwrap_or_else(|| "postgres".to_owned());
        let db_name = format!("{}_test_{}", base_db, Uuid::new_v4().to_string().replace('-', ""));

        // Admin connection to the maintenance database for CREATE/DROP.
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(options.clone().database("postgres"))
            .await
            .unwrap_or_else(|error| panic!("failed to connect for reconnect test database setup: {error}"));
        sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
            .execute(&admin_pool)
            .await
            .unwrap_or_else(|error| panic!("failed to create reconnect test database '{db_name}': {error}"));

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(options.database(&db_name))
            .await
            .unwrap_or_else(|error| panic!("failed to connect to reconnect test database '{db_name}': {error}"));
        db::run_migrations(&pool).await;
        // The migration seeds the singleton settings row; pin a
        // deterministic managed-mode config for this isolated database.
        sqlx::query(
            "UPDATE icecast_settings
             SET mode = 'managed', port = 8000, source_password = 'surcast-test',
                 admin_user = 'admin', admin_password = 'surcast-test'
             WHERE id = '00000000-0000-0000-0000-000000000001'",
        )
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("failed to configure reconnect test database: {error}"));

        Some(ReconnectTestDb { pool, admin_pool, db_name })
    }

    /// Owns an isolated reconnect test database; `cleanup()` closes the pool
    /// first and then drops the database, so no connections are held during
    /// the DROP.
    struct ReconnectTestDb {
        pool: PgPool,
        admin_pool: PgPool,
        db_name: String,
    }

    impl ReconnectTestDb {
        async fn cleanup(self) {
            self.pool.close().await;
            // `DROP DATABASE` fails while ANY backend of the database is
            // still alive. A queue load executed mid-test (skip →
            // commit_current → reload_from_db) can leave an idle connection
            // whose pool handle was closed but whose backend outlives the
            // close; terminate any straggler backend before dropping.
            let _ = sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()")
                .bind(&self.db_name)
                .execute(&self.admin_pool)
                .await;
            sqlx::query(&format!("DROP DATABASE IF EXISTS \"{}\"", self.db_name))
                .execute(&self.admin_pool)
                .await
                .unwrap_or_else(|error| panic!("failed to drop reconnect test database '{}': {error}", self.db_name));
            self.admin_pool.close().await;
        }
    }

    /// Test A: a manual reconnect through the full `StationRuntime` path —
    /// `StationCommand::Reconnect(response)` hands the response to the
    /// reconnect-aware action and the caller receives the real pipeline
    /// result.
    #[tokio::test]
    async fn manual_reconnect_through_the_runtime_reports_success() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, _events) = harness.into_runtime();
        let result = runtime.reconnect().await;
        assert!(result.is_ok(), "the manual caller must receive Ok, got {result:?}");
        assert_eq!(pipeline.count(Call::Reconnect), 1);
        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    /// Test B: a failed manual reconnect through the full runtime path
    /// delivers the actual PipelineError (never a cancelled channel), runs
    /// exactly once, and stays one-shot.
    #[tokio::test]
    async fn manual_reconnect_through_the_runtime_reports_failure() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        pipeline.fail_once(Call::Reconnect);
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, _events) = harness.into_runtime();
        let result = runtime.reconnect().await;
        match result {
            Err(PipelineError::Pipeline(message)) => assert!(message.contains("injected failure"), "unexpected error: {message}"),
            other => panic!("expected the pipeline error, got {other:?}"),
        }
        assert_eq!(pipeline.count(Call::Reconnect), 1, "the manual reconnect must run exactly once");
        // One-shot: no automatic retry timer fires within the first backoff
        // window.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(pipeline.count(Call::Reconnect), 1, "a manual reconnect must stay one-shot");
        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    #[tokio::test]
    async fn pause_invalidates_the_reconnect_chain() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token = controller.begin_reconnect_chain();
        assert!(controller.reconnect_retry_is_current(token));

        // A manual pause ends the chain: retries are only eligible while
        // Playing (Model B).
        controller.pause();
        assert!(
            !controller.reconnect_retry_is_current(token),
            "a manual pause must end the reconnect chain"
        );
        assert_eq!(
            controller.reconnect_token_shared().token(),
            0,
            "the shared state must be invalidated too"
        );
    }

    #[tokio::test]
    async fn stale_chain_cleanup_does_not_end_a_newer_chain() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token_x = controller.begin_reconnect_chain();
        let token_y = controller.begin_reconnect_chain();
        controller.end_reconnect_chain(token_x);

        assert!(
            controller.reconnect_retry_is_current(token_y),
            "ending a stale chain must not end the newer chain"
        );
    }

    /// The orphaned-chain regression: Pause during backoff, the retry timer
    /// firing while paused, then Play, then a fresh disconnect — the
    /// disconnect must NOT be coalesced into the dead chain; a real pipeline
    /// reconnect must run.
    #[tokio::test]
    async fn pause_then_retry_then_play_does_not_block_future_reconnects() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        pipeline.fail_once(Call::Reconnect);
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, events) = harness.into_runtime();
        runtime.play().await.unwrap();

        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped".into(),
            })
            .unwrap();
        testsupport::wait_for("the first reconnect attempt to run", || pipeline.count(Call::Reconnect) > 0).await;

        // The user pauses while the backoff timer is pending. The pause must
        // end the chain immediately: if it stayed active, a disconnect right
        // after Play (BEFORE the timer fires) would be coalesced into the
        // dead chain and the reconnect lost forever.
        runtime.pause().await.unwrap();
        runtime.play().await.unwrap();
        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped again".into(),
            })
            .unwrap();
        testsupport::wait_for(
            "a real reconnect after Pause→Play instead of one coalesced into the dead chain",
            || pipeline.count(Call::Reconnect) >= 2,
        )
        .await;

        // Chain X's stale retry timer fires later: it must not run a third
        // reconnect.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(pipeline.count(Call::Reconnect), 2);

        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    #[tokio::test]
    async fn disconnect_after_pause_and_play_starts_a_fresh_chain() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token_x = controller.begin_reconnect_chain();
        controller.bind_reconnect_to_output(controller.generation(), controller.output_epoch());

        // Pause ends the chain (Model B): the dead chain must never coalesce
        // a later disconnect.
        controller.pause();
        assert!(!controller.reconnect_retry_is_current(token_x));

        controller.play().await;
        let result = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: controller.generation(),
                output_epoch: controller.output_epoch(),
                message: "output dropped".into(),
            })
            .await;
        assert!(result.is_some(), "a disconnect after Pause→Play must start a fresh chain");
        assert_ne!(controller.current_reconnect_token(), token_x);
    }

    /// The pipeline does not restore a connection that broke while paused:
    /// a `SinkDisconnected` during `Paused` must be remembered, and the next
    /// `Play` must queue a reconnect after resuming — with NO second
    /// disconnect event injected.
    #[tokio::test]
    async fn disconnect_while_paused_is_recovered_by_play() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, events) = harness.into_runtime();
        runtime.play().await.unwrap();

        // The output breaks while paused: no immediate action, but the
        // disconnect must be remembered.
        runtime.pause().await.unwrap();
        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped while paused".into(),
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            pipeline.count(Call::Reconnect),
            0,
            "a disconnect while paused must not run a reconnect yet"
        );

        runtime.play().await.unwrap();
        testsupport::wait_for("Play to restore an output that broke while paused", || {
            pipeline.count(Call::Reconnect) > 0
        })
        .await;
        assert_eq!(pipeline.count(Call::Reconnect), 1);

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(pipeline.count(Call::Reconnect), 1);

        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    /// Test 1: a disconnect that happened BEFORE the pause is recovered by
    /// Play — no second `SinkDisconnected` event is injected.
    #[tokio::test]
    async fn disconnect_before_pause_is_recovered_by_play_without_a_second_event() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        pipeline.fail_once(Call::Reconnect);
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, events) = harness.into_runtime();

        runtime.play().await.unwrap();
        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped".into(),
            })
            .unwrap();
        testsupport::wait_for("the first reconnect attempt to run", || pipeline.count(Call::Reconnect) > 0).await;

        // Pause: the retry chain is invalidated, but the knowledge that the
        // output is disconnected must survive.
        runtime.pause().await.unwrap();

        runtime.play().await.unwrap();
        testsupport::wait_for("Play to recover an output that was already disconnected before the pause", || {
            pipeline.count(Call::Reconnect) >= 2
        })
        .await;
        assert_eq!(pipeline.count(Call::Reconnect), 2);

        // The stale timer from chain X fires later and does nothing.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(pipeline.count(Call::Reconnect), 2);

        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    /// Test 2: a Pause interrupting an in-flight recovery does not lose the
    /// disconnected knowledge — the next Play retries the recovery.
    #[tokio::test]
    async fn pause_interrupting_recovery_keeps_the_output_recoverable() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        pipeline.fail_once(Call::Reconnect);
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, events) = harness.into_runtime();

        runtime.play().await.unwrap();
        runtime.pause().await.unwrap();
        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped while paused".into(),
            })
            .unwrap();

        runtime.play().await.unwrap();
        testsupport::wait_for("the first recovery attempt to run", || pipeline.count(Call::Reconnect) > 0).await;

        // Pause again before recovery succeeded; the marker must survive.
        runtime.pause().await.unwrap();

        runtime.play().await.unwrap();
        testsupport::wait_for("a second Play to retry the interrupted recovery", || {
            pipeline.count(Call::Reconnect) >= 2
        })
        .await;
        assert_eq!(pipeline.count(Call::Reconnect), 2);

        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    /// Test 3: a successful recovery clears the marker — a later Pause/Play
    /// cycle must not run a redundant reconnect.
    #[tokio::test]
    async fn successful_recovery_clears_the_marker_for_later_cycles() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, events) = harness.into_runtime();

        runtime.play().await.unwrap();
        runtime.pause().await.unwrap();
        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped while paused".into(),
            })
            .unwrap();
        runtime.play().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while pipeline.count(Call::Reconnect) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the recovery must run");
        assert_eq!(pipeline.count(Call::Reconnect), 1);

        runtime.pause().await.unwrap();
        runtime.play().await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            pipeline.count(Call::Reconnect),
            1,
            "no redundant reconnect after a successful recovery"
        );

        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    /// Test 4: a successful manual reconnect while paused clears the marker
    /// — the later Play does not reconnect again.
    #[tokio::test]
    async fn successful_manual_reconnect_clears_the_marker() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, events) = harness.into_runtime();

        runtime.play().await.unwrap();
        runtime.pause().await.unwrap();
        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped while paused".into(),
            })
            .unwrap();
        runtime.reconnect().await.unwrap();
        assert_eq!(pipeline.count(Call::Reconnect), 1);

        runtime.play().await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            pipeline.count(Call::Reconnect),
            1,
            "no redundant reconnect after a successful manual reconnect"
        );

        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    /// Test 4b: a FAILED manual reconnect while paused keeps the marker —
    /// the later Play can still recover.
    #[tokio::test]
    async fn failed_manual_reconnect_keeps_the_marker_for_play_recovery() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        pipeline.fail_once(Call::Reconnect);
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let (runtime, events) = harness.into_runtime();

        runtime.play().await.unwrap();
        runtime.pause().await.unwrap();
        events
            .send(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped while paused".into(),
            })
            .unwrap();
        let result = runtime.reconnect().await;
        assert!(result.is_err(), "the manual reconnect must report its failure");
        assert_eq!(pipeline.count(Call::Reconnect), 1);

        runtime.play().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while pipeline.count(Call::Reconnect) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Play must recover the output after a failed manual reconnect");
        assert_eq!(pipeline.count(Call::Reconnect), 2);

        runtime.shutdown().await.unwrap();
        db.cleanup().await;
    }

    /// Functional regression: a delayed success of an OLD chain (completion
    /// of X arriving after a SECOND disconnect of the same output started
    /// chain Y) must not erase the newer disconnect. Pause invalidates Y,
    /// and the next Play still knows the output is disconnected — it runs
    /// the recovery WITHOUT a third `SinkDisconnected` event.
    #[tokio::test]
    async fn stale_success_before_pause_does_not_block_play_recovery() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let mut controller = harness.controller;

        assert!(matches!(controller.play().await, PipelineOperation::Replace(_)));
        let first = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "drop #1".into(),
            })
            .await;
        assert!(first.is_some(), "disconnect #1 must start a chain");
        let token_x = controller.current_reconnect_token();
        assert!(controller.is_output_known_disconnected());

        // X succeeds in the pipeline; the executor marks it completed, but
        // ReconnectFinished(X) has not reached the runtime yet (the race
        // window).
        controller.reconnect_token_shared().mark_completed(token_x);

        // Disconnect #2 for the SAME output in that window: a fresh chain Y
        // starts (a completed chain is never coalesced) and the marker is
        // re-set for the same identity.
        let second = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "drop #2".into(),
            })
            .await;
        assert!(second.is_some(), "disconnect #2 must never be lost");
        let token_y = controller.current_reconnect_token();
        assert_ne!(token_y, token_x, "disconnect #2 must start a fresh chain");

        // The delayed success of X arrives NOW (stale token): it must leave
        // chain Y and the marker of disconnect #2 untouched.
        controller.on_reconnect_succeeded(token_x);
        assert!(
            controller.reconnect_retry_is_current(token_y),
            "stale success must not end the newer chain"
        );
        assert!(
            controller.is_output_known_disconnected(),
            "stale success must not clear the marker of the newer disconnect"
        );

        // Pause invalidates Y (Model B); the factual marker survives.
        controller.pause();
        assert!(!controller.reconnect_retry_is_current(token_y));
        assert!(controller.is_output_known_disconnected());

        // Play — still no third SinkDisconnected: the surviving marker
        // drives the recovery.
        assert!(matches!(controller.play().await, PipelineOperation::SetPlaying(_)));
        assert!(
            controller.resume_reconnect_for_break().await.is_some(),
            "Play must recover the output after the stale success + Pause sequence"
        );

        db.cleanup().await;
    }

    #[tokio::test]
    async fn stop_clears_the_marker_so_play_does_not_reconnect_the_old_output() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        controller.play().await;
        controller.pause();
        let result = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: controller.generation(),
                output_epoch: controller.output_epoch(),
                message: "output dropped while paused".into(),
            })
            .await;
        assert!(result.is_none(), "no reconnect while paused");
        assert!(controller.is_output_known_disconnected());

        controller.stop();
        assert!(!controller.is_output_known_disconnected());

        controller.play().await;
        assert!(
            controller.resume_reconnect_for_break().await.is_none(),
            "a stopped station must not reconnect the old output"
        );
    }

    /// Output-replacement variant: an old reconnect chain X (bound to the
    /// replaced output) succeeding late must not clear the marker that a
    /// disconnect of the NEW output set. The token guard is what protects
    /// this — the identities differ, but the old marker was already removed
    /// by the replacement, so the stale success must not touch the new one.
    #[tokio::test]
    async fn stale_success_does_not_clear_a_new_outputs_marker_after_replacement() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let first = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "drop #1".into(),
            })
            .await;
        assert!(first.is_some(), "disconnect #1 must start a chain");
        let token_x = controller.current_reconnect_token();
        assert!(controller.is_output_known_disconnected());

        // X succeeds in the pipeline, but ReconnectFinished(X) is delayed;
        // meanwhile skip replaces the output and a new disconnect creates
        // chain Y. X's late success must neither end Y nor clear the new
        // output's marker.
        controller.reconnect_token_shared().mark_completed(token_x);

        controller.skip().await.unwrap();
        assert_eq!(controller.generation(), 2);
        assert!(
            !controller.is_output_known_disconnected(),
            "a replaced output must not keep a known-disconnected marker"
        );

        let second = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 2,
                output_epoch: 1,
                message: "drop #2 of the new output".into(),
            })
            .await;
        assert!(second.is_some(), "the new output's disconnect must start a chain");
        let token_y = controller.current_reconnect_token();
        assert_ne!(token_y, token_x);
        assert!(controller.is_output_known_disconnected());

        controller.on_reconnect_succeeded(token_x);
        assert!(
            controller.reconnect_retry_is_current(token_y),
            "stale success must not end the newer chain"
        );
        assert!(
            controller.is_output_known_disconnected(),
            "stale success must not clear the marker of the new output"
        );
    }

    /// Mandatory regression: a reconnect that was QUEUED for output
    /// (generation 1) must never touch the pipeline once the output
    /// identity was replaced (skip → generation 2) — even though its chain
    /// token X was still current when it was enqueued. The executor is
    /// physically blocked inside a previous `replace`, so the stale
    /// reconnect is guaranteed to still sit in the queue when the
    /// replacement happens; after the executor is released it must be
    /// dropped by the pre-pipeline token guard (the replacement
    /// invalidated the shared token), with `reconnect_count == 0` as the
    /// observable pipeline effect.
    #[tokio::test]
    async fn replaced_output_invalidates_a_queued_reconnect_before_the_pipeline() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let mut controller = harness.controller;
        let driver = controller.driver();
        let (urgent_tx, urgent) = mpsc::unbounded_channel::<crate::streamer::runtime::ExecutorTask>();
        let (regular_tx, regular) = mpsc::unbounded_channel::<crate::streamer::runtime::ExecutorTask>();
        let executor = tokio::spawn(crate::streamer::runtime::run_executor(urgent, regular, driver));

        // Play queues a replace that BLOCKS inside the pipeline: the
        // sequential executor is now held and cannot run anything else.
        let operation = controller.play().await;
        crate::streamer::runtime::ExecutorTask::Operation(crate::streamer::runtime::PendingPipelineAction::operation(operation, None))
            .submit(&urgent_tx);
        let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
        gate.wait_started().await;

        // The output disconnects while the executor is blocked: chain X is
        // minted and its reconnect operation is queued BEHIND the replace,
        // still carrying token X and identity (1, 1).
        let op = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped".into(),
            })
            .await
            .expect("a disconnect while playing must start a chain")
            .expect("the reconnect target must build against the test database");
        let token_x = controller.current_reconnect_token();
        assert!(controller.reconnect_retry_is_current(token_x));
        let PipelineOperation::Reconnect(target) = op else {
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
            controller.reconnect_token_shared(),
            None,
            true,
        ))
        .submit(&urgent_tx);

        // The user skips while the reconnect is still queued: the output
        // identity is replaced (generation 2) and the reconnect chain of
        // the old identity is invalidated — the shared token flips to 0.
        controller.skip().await.unwrap();
        assert_eq!(controller.generation(), 2);
        assert!(
            !controller.reconnect_retry_is_current(token_x),
            "an output replacement must invalidate the reconnect chain of the old output"
        );
        assert_eq!(
            controller.reconnect_token_shared().token(),
            0,
            "the shared executor state must be invalidated by the replacement"
        );

        // Release the executor: the replace finishes, then the stale queued
        // reconnect of the old identity reaches the pre-pipeline token
        // guard and is dropped — the pipeline is never touched.
        gate.release();
        drop(urgent_tx);
        drop(regular_tx);
        executor.await.unwrap();
        assert_eq!(
            pipeline.count(Call::Reconnect),
            0,
            "a stale queued reconnect of a replaced output must never call pipeline.reconnect()"
        );
        assert_eq!(pipeline.count(Call::Replace), 1);

        db.cleanup().await;
    }

    /// Controller-level companion: the bookkeeping half of the invariant —
    /// after a skip the old chain's token is no longer current and the
    /// shared executor state no longer matches it.
    #[tokio::test]
    async fn skip_invalidates_the_reconnect_chain_of_the_old_output() {
        let Some(db) = reconnect_test_db().await else { return };
        let pipeline = Arc::new(RecordingPipeline::new());
        let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
        let mut controller = harness.controller;

        assert!(matches!(controller.play().await, PipelineOperation::Replace(_)));
        let disconnect = controller
            .handle_event(PipelineEvent::SinkDisconnected {
                generation: 1,
                output_epoch: 1,
                message: "output dropped".into(),
            })
            .await
            .expect("a disconnect while playing must start a chain")
            .expect("the disconnect handling must not fail");
        let PipelineOperation::Reconnect(_) = disconnect else {
            panic!("a disconnect while playing must produce a reconnect");
        };
        let token_x = controller.current_reconnect_token();
        assert!(controller.reconnect_retry_is_current(token_x));

        controller.skip().await.unwrap();
        assert_eq!(controller.generation(), 2);
        assert!(
            !controller.reconnect_retry_is_current(token_x),
            "the old chain token must no longer be current after a skip"
        );
        assert_eq!(
            controller.reconnect_token_shared().token(),
            0,
            "the shared token must be invalidated so a queued reconnect of the old output is dropped"
        );
        assert!(
            !controller.is_output_known_disconnected(),
            "the old output's marker must not survive the replacement"
        );

        // A retry timer of the old chain is rejected too (the runtime path
        // `reconnect_if_current` now fails on the token AND the identity).
        let retry = controller
            .reconnect_if_current(1, 1, token_x)
            .await
            .expect("a stale retry is dropped, never an error");
        assert!(retry.is_none(), "a retry of the old identity must be dropped");

        db.cleanup().await;
    }

    #[tokio::test]
    async fn manual_pause_ends_the_auto_idle_state() {
        let song = queued_song("A", 0);
        let fresh = queued_song("B", 1);
        let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

        let operation = controller.skip().await.unwrap();
        assert!(matches!(operation, PipelineOperation::Stop));
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
        let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
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
    async fn target_refresh_failure_keeps_the_chain_retryable() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token = controller.begin_reconnect_chain();
        let result = controller.reconnect_if_current(1, 1, token).await;
        assert!(result.is_err(), "the config refresh must surface its error");

        assert!(
            controller.reconnect_retry_is_current(token),
            "a refresh failure must not invalidate the chain"
        );
        let result = controller.reconnect_if_current(1, 1, token).await;
        assert!(result.is_err());
        assert!(controller.reconnect_retry_is_current(token));
    }

    #[tokio::test]
    async fn retry_attempts_do_not_mint_a_new_chain_token() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token = controller.begin_reconnect_chain();
        // Each failed attempt would schedule the next retry: the token must
        // stay identical — a retry never supersedes its own chain.
        for _ in 0..3 {
            let result = controller.reconnect_if_current(1, 1, token).await;
            assert!(result.is_err());
            assert_eq!(
                controller.current_reconnect_token(),
                token,
                "a retry attempt must reuse the chain token"
            );
            assert!(controller.reconnect_retry_is_current(token));
        }
    }

    #[tokio::test]
    async fn superseded_chain_never_reconnects() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        // Chain A fails and schedules a timer; a newer chain B supersedes
        // it. When A's timer fires, the retry must be dropped — the check
        // happens before any target refresh or pipeline call.
        let token_a = controller.begin_reconnect_chain();
        let token_b = controller.begin_reconnect_chain();
        assert!(!controller.reconnect_retry_is_current(token_a));
        assert!(controller.reconnect_retry_is_current(token_b));

        let result = controller.reconnect_if_current(1, 1, token_a).await;
        assert!(matches!(result, Ok(None)), "a superseded chain must never reconnect");
    }

    #[tokio::test]
    async fn stop_invalidates_the_chain_for_future_retries() {
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B"])).await.into_parts();

        let token = controller.begin_reconnect_chain();
        controller.stop();
        let result = controller.reconnect_if_current(1, 1, token).await;
        assert!(matches!(result, Ok(None)), "a stopped station must never reconnect");
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
        let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
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
        let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
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
}
