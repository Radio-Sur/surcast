use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::{broadcast, mpsc, oneshot};

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
    /// The exact branch the physical pipeline stages (the PairPlan's next),
    /// with its full song metadata: a handover commits this branch even when
    /// a reload removed it from the queue, so the controller needs the song
    /// itself (title/artist/position), not only its key. `planned_next`
    /// always describes physical reality, never a desired future state —
    /// it only advances once the pipeline actually adopted the branch.
    planned_next: Option<(SongInfo, super::queue_state::QueueAnchor)>,
    /// True when the station is stopped because its queue was empty
    /// (AutoDJ / schedule fill), as opposed to a manual stop. Only this
    /// state may auto-resume playback once the queue fills again.
    idle: bool,
    /// Monotonic id of the single in-flight automatic idle resume, if any.
    /// Guards the idle ticker against queueing several resume replaces and
    /// lets a stale completion (a superseding user decision arrived first)
    /// be ignored instead of overwriting it.
    pending_resume: Option<u64>,
    resume_attempt_seq: u64,
    /// Monotonic id of the single in-flight manual initial play replace, if
    /// any. Guards against stale completions overwriting a newer user
    /// decision and defers committing `Playing` until the physical replace
    /// succeeded.
    pending_play: Option<u64>,
    play_attempt_seq: u64,
    /// Attempt id of the most recent failed initial play replace, if any.
    /// Cleared on successful or failed resolution or when an ordinary skip occurs.
    last_failed_play: Option<u64>,
    /// Attempt id of the most recent successful initial play replace, if any.
    /// Retained so that a subsequent skip commit can recognize that the initial
    /// play succeeded when `commit_play` was processed before `commit_skip`.
    resolved_play_success: Option<u64>,
    /// Attempt id of an in-flight initial play whose track was superseded by a
    /// successful skip commit before `commit_play` arrived. When `commit_play`
    /// eventually runs, this informs it that the skip already advanced
    /// the track, so an error must not record `last_failed_play` on the new track.
    pending_play_resolved_by_skip: Option<u64>,
    /// The prepared-but-uncommitted skip (manual or terminal-driven): the
    /// queue/DB cursor, generation bump, planned-next update and
    /// notifications are applied only after the pipeline replacement
    /// succeeded (`commit_skip`). A failed Replace must leave the
    /// controller and the queue logically on the old track/generation while
    /// the pipeline keeps playing it — committing first would desynchronize
    /// them (split-brain), and terminal events guarded by the old identity
    /// would then be rejected as stale. The command loop is single-threaded
    /// and `skip()` refuses a second attempt while one is pending, so at
    /// most one attempt is ever in flight; the attempt id still correlates
    /// the executor completion with the exact attempt, so a stale/foreign
    /// completion can never commit a superseded attempt.
    pending_skip: Option<PendingSkip>,
    skip_attempt_seq: u64,
    /// The in-flight realign roll of the most recent skip commit, if any.
    /// At most one realign is ever outstanding: a newer skip commit mints
    /// a fresh record (its id supersedes the older one, whose completion
    /// then finds no matching id), and a handover clears it. `planned_next`
    /// is only advanced by `commit_realign` after the roll succeeded.
    pending_realign: Option<PendingRealign>,
    realign_seq: u64,
    /// A terminal event (`CurrentEos` / current-track `DecodeFailed`) that
    /// arrived while a skip was already in flight — or that triggered the
    /// in-flight skip itself. It is deferred, never consumed: if the
    /// pending skip fails without advancing the identity, the terminal
    /// condition is re-resolved (`retry_deferred_terminal`), bounded so a
    /// persistently failing pipeline cannot hot-loop the executor.
    deferred_terminal: Option<DeferredTerminal>,
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
    /// The staged branches known to have failed decoding under the current
    /// playback identity `(generation, current)`. Staged-next selection
    /// skips all tracks in this collection across reloads and follow-up
    /// realigns, even across idle gaps when no roll is in flight. A
    /// transition to a new current identity (handover, skip commit, stop,
    /// new playback) clears this collection.
    decode_exclusions: Option<DecodeExclusions>,
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
/// A pipeline operation plus the correlation its preparation declared: the
/// skip attempt id (produced by the skip preparation itself, so a completion
/// is bound to exactly the attempt that created the operation — never
/// inferred from a global pending state at submission time) and/or the id of
/// the two-phase realign record the operation was minted against (any Roll
/// that changes the staged next: skip-commit realigns, ordinary reload
/// realigns, staged-next decode-failure replacements, post-handover
/// Attaches). `realign_id` makes the runtime submit the operation with a
/// `RealignResult` completion so `planned_next` only advances after the
/// roll physically succeeded.
#[derive(Debug)]
pub(crate) struct PreparedOperation {
    pub(crate) operation: super::driver::PipelineOperation,
    pub(crate) attempt_id: Option<u64>,
    pub(crate) realign_id: Option<u64>,
    pub(crate) play_attempt_id: Option<u64>,
}

/// What a skip commit produced beyond the commit itself: the runtime
/// submits follow-up work with exactly the correlation this type describes.
#[derive(Debug)]
pub(crate) enum SkipFollowup {
    /// No further pipeline work.
    None,
    /// A realign roll whose outcome must be correlated (`RealignResult`):
    /// `planned_next` is only advanced once the roll succeeded while the
    /// identity it was built for still holds.
    Realign {
        id: u64,
        operation: super::driver::PipelineOperation,
    },
    /// A fresh skip attempt (terminal retry) or any other operation the
    /// commit produced (e.g. a Stop for an exhausted queue), with the
    /// attempt id when it carries a skip.
    Operation(PreparedOperation),
}

/// The state a prepared skip needs to commit once its pipeline replacement
/// succeeded: the successor that becomes current, the anchor marking the
/// old current as consumed, and the exact next branch the PairPlan staged
/// (`planned_next` must keep describing that branch after the commit, whose
/// refill/reload can change the queue successor under the staged plan).
struct PendingSkip {
    attempt_id: u64,
    target_generation: u64,
    /// The song the physical Replace will adopt — its full metadata lets the
    /// commit represent it as the logical current even when a reload removed
    /// it from the queue while the Replace was in flight.
    next_song: SongInfo,
    anchor: super::queue_state::QueueAnchor,
    /// The branch the PairPlan stages after the new current (full metadata
    /// for the same reason: `planned_next` must keep describing it while its
    /// realign roll is in flight).
    staged_next: Option<SongInfo>,
    /// The manual caller's response, answered by the runtime only after the
    /// commit (or abandon) ran — never before the pipeline replaced.
    response: Option<oneshot::Sender<Result<(), PipelineError>>>,
    /// The attempt id of the in-flight initial play (`pending_play`) this
    /// skip was prepared against, if any. The final playback state after a
    /// successful skip commit depends on the actual physical outcome of this
    /// play attempt:
    /// - P1 Ok + S Ok -> `Playing`
    /// - P1 Err + S Ok -> `Stopped` (the skip advanced the track while the
    ///   pipeline stayed stopped; a subsequent `play()` starts playback on
    ///   the new track).
    ///
    /// If a newer `pause()` or `stop()` intervened, this field is cleared
    /// to None so the late skip completion preserves `Paused`/`Stopped`.
    resolving_play_attempt: Option<u64>,
    failed_current: Option<TrackKey>,
    failed_staged_next: Option<TrackKey>,
    is_play_resume: bool,
}

/// A deferred terminal event: generation/track of the ended track and how
/// many re-resolutions are still allowed after a failed skip.
struct DeferredTerminal {
    generation: u64,
    track: TrackKey,
    retries_left: u8,
}

/// An in-flight realign roll scheduled for a staged-next physical change
/// (skip-commit realigns, ordinary reload realigns, staged-next
/// decode-failure replacements, post-handover Attaches, dirty follow-ups).
/// The single slot enforces the serialization rule: at one
/// generation/current identity there is at most one unresolved realign —
/// `prepare_realign` absorbs same-identity second intents (marking the
/// record `dirty` when they bring a newer successor) and only a physically
/// newer identity (handover, skip commit) supersedes the record.
/// `planned_next` keeps describing the STAGED branch until the roll
/// succeeded while the identity it was built for still holds — only
/// `commit_realign` may advance it to `desired`, exactly once. A handover
/// of the staged branch (or a newer skip commit) supersedes the realign:
/// its late completion finds no matching pending record (or a failed
/// identity check) and has zero effect on the newer state.
/// Tracks the staged branches that have failed decoding under the current
/// playback identity `(generation, current)`. Staged-next selection skips
/// all known-broken branches for this identity across reloads and follow-up
/// realigns, even across idle gaps when no roll is in flight. A transition
/// to a new current identity (handover, skip commit, stop, new playback)
/// clears this collection.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodeExclusions {
    generation: u64,
    current: TrackKey,
    tracks: Vec<TrackKey>,
}

impl DecodeExclusions {
    fn new(generation: u64, current: TrackKey) -> Self {
        Self {
            generation,
            current,
            tracks: Vec::new(),
        }
    }

    fn matches(&self, generation: u64, current: &TrackKey) -> bool {
        self.generation == generation && &self.current == current
    }

    fn add(&mut self, track: TrackKey) {
        if !self.tracks.contains(&track) {
            self.tracks.push(track);
        }
    }

    fn contains(&self, track: &TrackKey) -> bool {
        self.tracks.contains(track)
    }

    fn is_empty(&self) -> bool {
        self.tracks.is_empty()
    }
}

struct PendingRealign {
    id: u64,
    /// Controller generation the roll was built for; the completion only
    /// claims `desired` while the generation still matches.
    generation: u64,
    /// The queue current the roll anchors on; a handover (or a newer
    /// commit) moving the cursor invalidates the claim.
    current: TrackKey,
    /// The staged branch the roll replaces — the controller must still
    /// claim exactly this branch for the completion to apply.
    expected_next: Option<TrackKey>,
    /// The branch to claim once the roll succeeded (None = drop the staged
    /// next because the queue is exhausted). Full song metadata: the queue
    /// may change again while the roll is in flight, and the claimed branch
    /// must stay representable even if it is then removed from the queue.
    desired: Option<SongInfo>,
    /// A queue reload while the roll was in flight changed the successor
    /// away from `desired`: the completion must re-read the latest queue and
    /// prepare another correlated realign toward it (the reload's alignment
    /// intent must not be forgotten).
    dirty: bool,
    /// `expected_next` itself is the known-broken physical branch: if this
    /// roll FAILS, the still-staged broken branch must be replaced again
    /// (bounded by `decode_retries_left`). When this roll SUCCEEDS, the
    /// physical branch is no longer broken (`expected_is_broken` becomes
    /// false on follow-up realigns).
    expected_is_broken: bool,
    /// Bounds the controller-initiated retries for a decode-failure fact:
    /// each retry decrements it and every fresh `DecodeFailed` event
    /// re-arms it, so a persistently failing roll cannot hot-loop the
    /// executor.
    decode_retries_left: u8,
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
            },
            instance.events,
        ))
    }
}
#[cfg(test)]
pub(crate) fn test_controller(pipeline: Arc<dyn super::pipeline::PlaybackPipeline>, songs: Vec<SongInfo>) -> StationController {
    use super::testsupport;
    let (status_tx, _) = broadcast::channel(1);
    let (queue_tx, _) = broadcast::channel(1);
    let station_id = uuid::Uuid::new_v4();
    let queue = Arc::new(QueueManager::new(
        testsupport::unavailable_db(),
        station_id,
        String::new(),
        songs,
        0,
    ));
    StationController {
        queue,
        db: testsupport::unavailable_db(),
        station_id,
        playback: testsupport::playback_config(),
        driver: PipelineDriver::spawn(pipeline),
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
    }
}

impl StationController {
    pub(crate) async fn handle_event(&mut self, event: PipelineEvent) -> Option<Result<PreparedOperation, PipelineError>> {
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
                if generation == self.generation && self.planned_next.as_ref().is_some_and(|(song, _)| Self::key_of(song) == track) {
                    let current_track = current.as_ref().map(|current| Self::track(current.clone()));
                    // Staged-DecodeFailed semantics: record the broken branch
                    // under the current identity so every queue alignment under
                    // this identity skips it, and select the next unconsumed
                    // playable song.
                    self.record_decode_exclusion(track.clone());
                    let replacement_song = self.effective_successor();
                    let replacement = replacement_song.clone().map(|successor| {
                        let track = Self::track(successor);
                        let current = current_track.clone().expect("a successor only exists while a current does");
                        let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current, Some(&track));
                        PlannedNext { track, transition }
                    });
                    if let Some(current) = current {
                        // Two-phase like every staged-next change:
                        // `planned_next` keeps describing the failed branch
                        // the pipeline still stages until the replacement
                        // roll SUCCEEDED (`commit_realign` advances it to
                        // the replacement, or drops it when the queue is
                        // exhausted). The record is armed as a decode
                        // failure (`expected_is_broken` + one retry): a
                        // duplicate DecodeFailed of the same branch while
                        // the roll is in flight is remembered, never
                        // discarded — the pipeline emits DecodeFailed once
                        // per branch attach, so a FAILED replacement roll
                        // would otherwise lose the failure fact forever.
                        let prepared = self.prepare_realign(
                            Self::key_of(&current),
                            Some(track.clone()),
                            replacement_song.clone(),
                            RollingChange::ReplaceNext {
                                expected_next: track,
                                replacement,
                            },
                            "decode-failure replacement",
                            true,
                            1,
                        )?;
                        return Some(Ok(prepared));
                    }
                }
                if let Some(pending) = self.pending_skip.as_mut() {
                    if generation == pending.target_generation {
                        if Self::key_of(&pending.next_song) == track {
                            pending.failed_current = Some(track);
                            return None;
                        }
                        if pending.staged_next.as_ref().is_some_and(|song| Self::key_of(song) == track) {
                            pending.failed_staged_next = Some(track);
                            return None;
                        }
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
                    let handover_target = if self.planned_next.as_ref().is_some_and(|(song, _)| Self::key_of(song) == track) {
                        // Case 1: The physically staged branch became current.
                        let (song, anchor) = self.planned_next.take().expect("guarded above");
                        Some((song, anchor))
                    } else if self.pending_realign.as_ref().is_some_and(|pending| {
                        pending.generation == self.generation
                            && self.queue.current_song_info().map(|song| song.queue_item_id) == Some(pending.current.queue_item_id)
                            && self.planned_next.as_ref().map(|(song, _)| Self::key_of(song)) == pending.expected_next
                            && pending.desired.as_ref().map(Self::key_of) == Some(track.clone())
                    }) {
                        // Case 2: The desired branch of an unresolved realign
                        // was physically adopted and handed over before the
                        // RealignResult completion was processed. Handover is
                        // authoritative physical evidence that the roll
                        // succeeded.
                        let pending = self.pending_realign.as_ref().expect("guarded above");
                        let song = pending.desired.clone().expect("guarded above");
                        let anchor = self.queue.anchor_after_current();
                        self.planned_next = None;
                        Some((song, anchor))
                    } else {
                        None
                    };

                    let Some((song, anchor)) = handover_target else {
                        // The staged next was replaced (queue realignment) and the
                        // pipeline handed over to the old plan; or an unrelated/stale
                        // track handed over. The queue state must not consume a track
                        // that will never play.
                        tracing::warn!(station_id = %self.station_id, queue_item_id = %track.queue_item_id, "ignoring stale handover after queue realignment");
                        return None;
                    };

                    // The staged/desired branch became current: an in-flight realign
                    // of that branch is superseded — the pipeline no longer
                    // stages it, so its completion must not claim anything
                    // for the newer state. The id-correlation and the
                    // identity checks in `commit_realign` are the backstop;
                    // clearing the record makes the supersession explicit.
                    // The current identity moved to the new track: old decode
                    // exclusions are cleared for the new current.
                    self.pending_realign = None;
                    self.decode_exclusions = None;
                    self.queue.commit_current(&song, anchor).await;
                    self.publish_song_change();
                    self.push_queue_update().await;

                    if let (Some(current), Some(next_song)) = (self.queue.current_song_info(), self.effective_successor()) {
                        let current_track = Self::track(current);
                        let next = Self::track(next_song.clone());
                        let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, Some(&next));
                        // The Attach is a staged-next physical operation
                        // like any other: two-phase. `planned_next` STAYS
                        // None — the pipeline stages nothing after the
                        // handover — until the attach roll SUCCEEDED
                        // (`commit_realign` advances it to the queue
                        // successor). A failed attach claims nothing, so a
                        // later reload/alignment may attach the successor
                        // again.
                        return self
                            .prepare_realign(
                                current_track.key,
                                None,
                                Some(next_song),
                                RollingChange::Attach(PlannedNext { track: next, transition }),
                                "handover attach",
                                false,
                                0,
                            )
                            .map(Ok);
                    }
                }
            }
            PipelineEvent::FatalPipeline { pipeline_epoch, message } => {
                let is_current_epoch = pipeline_epoch == self.output_epoch;
                let is_pending_start = self.pending_play.is_some() || self.pending_resume.is_some();
                if is_current_epoch && (self.state != PipelineState::Stopped || is_pending_start) {
                    tracing::error!(
                        station_id = %self.station_id,
                        pipeline_epoch,
                        %message,
                        "fatal GStreamer pipeline failure; stopping station"
                    );
                    let operation = self.stop();
                    return Some(Ok(PreparedOperation {
                        operation,
                        attempt_id: None,
                        realign_id: None,
                        play_attempt_id: None,
                    }));
                } else {
                    tracing::debug!(
                        station_id = %self.station_id,
                        pipeline_epoch,
                        current_epoch = self.output_epoch,
                        state = ?self.state,
                        %message,
                        "stale fatal pipeline event ignored"
                    );
                }
            }
            PipelineEvent::SinkDisconnected {
                generation,
                output_epoch,
                message,
            } => {
                tracing::error!(station_id = %self.station_id, generation, output_epoch, %message, "GStreamer output disconnected");
                return self.reconnect_for_output(generation, output_epoch).await.map(|result| {
                    result.map(|operation| PreparedOperation {
                        operation,
                        attempt_id: None,
                        realign_id: None,
                        play_attempt_id: None,
                    })
                });
            }
            #[cfg(test)]
            PipelineEvent::TestBarrier(_) => return None,
        }
        None
    }

    async fn resolve_current_terminal(&mut self, generation: u64, track: &TrackKey) -> Option<Result<PreparedOperation, PipelineError>> {
        let current = self.queue.current_song_info().map(|song| song.queue_item_id);
        if generation == self.generation && current == Some(track.queue_item_id) {
            if self.pending_skip.is_some() {
                // A skip for this identity is already in flight: the terminal
                // event is deferred until that skip resolves. Dropping it
                // here would strand the station at end-of-track if the
                // in-flight skip fails — the deferred event is what re-triggers
                // the progression then.
                self.deferred_terminal = Some(DeferredTerminal {
                    generation,
                    track: track.clone(),
                    retries_left: 1,
                });
                return None;
            }
            // The event-driven skip gets the same two-phase treatment as a
            // manual one; the terminal condition is remembered so a failed
            // replacement retries it (bounded) instead of stranding the
            // station on the ended track.
            self.deferred_terminal = Some(DeferredTerminal {
                generation,
                track: track.clone(),
                retries_left: 1,
            });
            Some(self.skip().await)
        } else {
            None
        }
    }

    fn key_of(song: &SongInfo) -> TrackKey {
        TrackKey {
            queue_item_id: song.queue_item_id,
            song_id: song.song_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn track(song: SongInfo) -> PipelineTrack {
        Self::track_inner(song)
    }

    #[cfg(not(test))]
    fn track(song: SongInfo) -> PipelineTrack {
        Self::track_inner(song)
    }

    fn track_inner(song: SongInfo) -> PipelineTrack {
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
        let next_song = self.queue.peek_next_song();
        self.planned_next = next_song.as_ref().map(|song| (song.clone(), anchor));
        let next = next_song.map(|song| {
            let track = Self::track(song);
            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, Some(&track));
            PlannedNext { track, transition }
        });
        self.generation += 1;
        self.decode_exclusions = None;
        self.pending_realign = None;
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

    pub(crate) async fn play(&mut self) -> Result<PreparedOperation, PipelineError> {
        // A manual play is a newer playback decision: any in-flight
        // automatic resume must not be able to overwrite it later.
        self.pending_resume = None;
        if self.state == PipelineState::Stopped {
            if self.pending_play.is_some() {
                return Err(PipelineError::Pipeline("a play is already in progress".into()));
            }
            if self.pending_skip.is_some() {
                return Err(PipelineError::Pipeline("a skip is already in progress".into()));
            }
            self.last_failed_play = None;
            self.resolved_play_success = None;
            self.pending_play_resolved_by_skip = None;
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
                    return Ok(PreparedOperation {
                        operation: PipelineOperation::Stop,
                        attempt_id: None,
                        realign_id: None,
                        play_attempt_id: None,
                    });
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
            self.play_attempt_seq = self.play_attempt_seq.wrapping_add(1).max(1);
            let attempt_id = self.play_attempt_seq;
            self.pending_play = Some(attempt_id);
            Ok(PreparedOperation {
                operation,
                attempt_id: None,
                realign_id: None,
                play_attempt_id: Some(attempt_id),
            })
        } else {
            self.idle = false;
            if let Some(prepared) = self.retry_deferred_terminal().await? {
                if let Some(pending) = self.pending_skip.as_mut() {
                    pending.is_play_resume = true;
                }
                return Ok(prepared);
            }
            self.state = PipelineState::Playing;
            Ok(PreparedOperation {
                operation: PipelineOperation::SetPlaying(true),
                attempt_id: None,
                realign_id: None,
                play_attempt_id: None,
            })
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
        self.pending_play = None;
        self.last_failed_play = None;
        self.resolved_play_success = None;
        self.pending_play_resolved_by_skip = None;
        if let Some(pending) = self.pending_skip.as_mut() {
            pending.resolving_play_attempt = None;
            pending.is_play_resume = false;
        }
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
        self.pending_play = None;
        self.last_failed_play = None;
        self.resolved_play_success = None;
        self.pending_play_resolved_by_skip = None;
        if let Some(pending) = self.pending_skip.as_mut() {
            pending.resolving_play_attempt = None;
            pending.is_play_resume = false;
        }
        self.invalidate_reconnect_chain();
        // A stop clears known_disconnected_output so later playback does not attempt to restore an output that was broken before the stop.
        self.known_disconnected_output = None;
        self.decode_exclusions = None;
        self.deferred_terminal = None;
        self.pending_realign = None;
        self.planned_next = None;
        self.state = PipelineState::Stopped;
        PipelineOperation::Stop
    }
    pub(crate) fn idle(&self) -> bool {
        self.idle
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> PipelineState {
        self.state
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
        self.pending_play = None;
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
        self.pending_resume = None;
        self.pending_play = None;
        self.last_failed_play = None;
        self.resolved_play_success = None;
        self.pending_play_resolved_by_skip = None;
        if let Some(pending) = self.pending_skip.as_mut() {
            pending.resolving_play_attempt = None;
        }
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

    pub(crate) async fn skip(&mut self) -> Result<PreparedOperation, PipelineError> {
        // A manual skip is a newer playback decision: a stale automatic
        // resume completion must not overwrite it later.
        self.pending_resume = None;
        // Skips are single-flight: while one replacement is in flight the
        // next skip is refused rather than prepared against pre-commit state
        // — its plan would be stale the moment the first replace adopts the
        // new identity, so the executor would reject it and the commit would
        // never happen. The loop stays responsive (the refusal is immediate),
        // and the manual caller is answered without touching the pipeline.
        if self.pending_skip.is_some() {
            return Err(PipelineError::Pipeline("a skip is already in progress".into()));
        }
        let Some(current) = self.queue.current_song_info() else {
            return Ok(PreparedOperation {
                operation: self.stop_after_current().await,
                attempt_id: None,
                realign_id: None,
                play_attempt_id: None,
            });
        };
        let mut next = self.effective_successor();
        if next.is_none() {
            // The in-memory queue can lag behind the database: Auto DJ refills
            // (triggered manually or by a schedule) insert rows without the
            // live streamer reloading. Retry once against the DB before
            // treating the queue as exhausted.
            self.queue.reload_from_db().await;
            next = self.effective_successor();
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
                next = self.effective_successor();
                if next.is_some() || ran || attempts >= 2 {
                    break;
                }
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempts))).await;
            }
        }
        let Some(next) = next else {
            // Exhausted queue: a plain stop, no skip attempt is prepared —
            // the operation carries no attempt id and the runtime submits
            // it without a skip completion (answering the caller directly).
            return Ok(PreparedOperation {
                operation: self.stop_after_current().await,
                attempt_id: None,
                realign_id: None,
                play_attempt_id: None,
            });
        };
        let next_key = TrackKey {
            queue_item_id: next.queue_item_id,
            song_id: next.song_id,
        };
        // Two-phase skip: prepare the replacement WITHOUT committing the
        // logical state. The queue/DB cursor, the generation, `planned_next`
        // and the SongChange are only advanced once the pipeline replacement
        // succeeded (`commit_skip`): a failed Replace must leave the station
        // on the old track/generation while the pipeline keeps playing it —
        // committing first would desynchronize the controller from the
        // actually active pipeline, and terminal events guarded by the old
        // identity would then be rejected as stale. The generation the plan
        // carries is reserved (`self.generation + 1`) without being committed
        // into controller state; only `commit_skip` adopts it.
        let anchor = self.queue.anchor_after_current();
        let next_track = Self::track(next.clone());
        let staged_song = self.queue.successor_after(&next_key);
        let next_staged = staged_song.clone().map(|successor| {
            let track = Self::track(successor);
            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &next_track, Some(&track));
            PlannedNext { track, transition }
        });
        self.skip_attempt_seq = self.skip_attempt_seq.wrapping_add(1).max(1);
        let attempt_id = self.skip_attempt_seq;
        let resolving_play_attempt = self.pending_play;
        let target_generation = self.generation + 1;
        self.pending_skip = Some(PendingSkip {
            attempt_id,
            target_generation,
            next_song: next,
            anchor,
            staged_next: staged_song,
            response: None,
            resolving_play_attempt,
            failed_current: None,
            failed_staged_next: None,
            is_play_resume: false,
        });
        // An in-flight realign roll of an earlier commit stays in place: its
        // roll was already submitted to the sequential executor, and its
        // completion claims the replacement only while the identity it was
        // built for still holds (generation/current/planned-next checks in
        // `commit_realign`). This new replace supersedes it logically — the
        // pipeline will rebuild the staged branch — and the late completion
        // is dropped by those checks, never by guessing.
        Ok(PreparedOperation {
            operation: PipelineOperation::Replace(Box::new(PairPlan {
                mode: ReplaceMode::ActiveReplace {
                    expected_generation: self.generation,
                    expected_current: Self::key_of(&current),
                },
                generation: self.generation + 1,
                output_epoch: self.output_epoch,
                current: next_track,
                next: next_staged,
            })),
            attempt_id: Some(attempt_id),
            realign_id: None,
            play_attempt_id: None,
        })
    }

    /// The attempt id of the skip whose pipeline replacement is currently
    /// in flight, if any (test visibility; the runtime no longer needs it —
    /// completions are bound explicitly at the skip preparation site).
    #[cfg(test)]
    pub(crate) fn pending_skip(&self) -> Option<u64> {
        self.pending_skip.as_ref().map(|pending| pending.attempt_id)
    }

    #[cfg(test)]
    pub(crate) fn pending_skip_failures(&self) -> Option<(Option<TrackKey>, Option<TrackKey>)> {
        self.pending_skip
            .as_ref()
            .map(|p| (p.failed_current.clone(), p.failed_staged_next.clone()))
    }

    /// The branch the controller currently believes the pipeline stages
    /// next. Tests assert the two-phase invariant around realign rolls.
    #[cfg(test)]
    pub(crate) fn planned_next(&self) -> Option<TrackKey> {
        self.planned_next.as_ref().map(|(song, _)| Self::key_of(song))
    }

    /// The id of the in-flight realign roll of the most recent skip commit,
    /// if any (test visibility).
    #[cfg(test)]
    pub(crate) fn pending_realign(&self) -> Option<u64> {
        self.pending_realign.as_ref().map(|pending| pending.id)
    }
    #[cfg(test)]
    pub(crate) fn pending_skip_target(&self) -> Option<TrackKey> {
        self.pending_skip.as_ref().map(|p| Self::key_of(&p.next_song))
    }
    #[cfg(test)]
    pub(crate) fn has_deferred_terminal(&self) -> bool {
        self.deferred_terminal.is_some()
    }
    #[cfg(test)]
    pub(crate) fn deferred_terminal_info(&self) -> Option<(u64, TrackKey, u8)> {
        self.deferred_terminal
            .as_ref()
            .map(|d| (d.generation, d.track.clone(), d.retries_left))
    }

    #[cfg(test)]
    pub(crate) fn has_decode_exclusions(&self) -> bool {
        self.decode_exclusions.as_ref().is_some_and(|e| !e.is_empty())
    }

    /// Holds the manual caller's response while the prepared skip is in
    /// flight; the runtime answers it only after the commit (or abandon)
    /// ran.
    pub(crate) fn attach_skip_response(&mut self, attempt_id: u64, response: oneshot::Sender<Result<(), PipelineError>>) {
        if let Some(pending) = self.pending_skip.as_mut() {
            if pending.attempt_id == attempt_id {
                pending.response = Some(response);
            }
        }
    }

    /// Takes the manual caller's response iff `attempt_id` is still the
    /// pending attempt — a stale completion must never answer (or steal the
    /// response of) a newer request.
    pub(crate) fn take_skip_response(&mut self, attempt_id: u64) -> Option<oneshot::Sender<Result<(), PipelineError>>> {
        let pending = self.pending_skip.as_mut()?;
        if pending.attempt_id != attempt_id {
            return None;
        }
        pending.response.take()
    }

    /// Commits the prepared initial play after the pipeline executor ran its
    /// replacement. Only the outcome of the CURRENT attempt is applied. On
    /// success the controller state moves to `Playing` (unless a newer decision
    /// changed it); on failure the controller remains `Stopped` so a subsequent
    /// play attempt can retry `InitialReplaceFromStopped`.
    pub(crate) fn commit_play(&mut self, attempt_id: u64, result: &Result<(), PipelineError>) -> bool {
        if self.pending_play != Some(attempt_id) {
            return false;
        }
        self.pending_play = None;
        let was_resolved_by_skip = self.pending_play_resolved_by_skip == Some(attempt_id);
        self.pending_play_resolved_by_skip = None;
        match result {
            Ok(()) => {
                self.idle = false;
                self.last_failed_play = None;
                self.resolved_play_success = if was_resolved_by_skip { None } else { Some(attempt_id) };
                self.state = PipelineState::Playing;
            }
            Err(error) => {
                tracing::warn!(station_id = %self.station_id, %error, "initial play replace failed; controller remains stopped");
                self.idle = false;
                self.resolved_play_success = None;
                if !was_resolved_by_skip {
                    self.last_failed_play = Some(attempt_id);
                } else {
                    self.last_failed_play = None;
                }
                self.state = PipelineState::Stopped;
            }
        }
        true
    }

    /// The attempt id of the initial play whose pipeline replacement is
    /// currently in flight, if any (test visibility).
    #[cfg(test)]
    pub(crate) fn pending_play(&self) -> Option<u64> {
        self.pending_play
    }

    /// Commits (or abandons) the prepared skip after the pipeline executor
    /// ran its replacement. Only the outcome of the CURRENT attempt is
    /// applied — the correlation happens BEFORE any state is consumed, so a
    /// completion for an attempt a newer decision superseded leaves the
    /// pending attempt fully intact. On success the queue/DB cursor, the
    /// generation, `planned_next` and the SongChange are advanced exactly
    /// once, and the returned operation (if any) explicitly re-synchronizes
    /// the pipeline with a queue successor that the commit's refill/reload
    /// changed; on failure everything stays on the old track/generation so
    /// the still-playing pipeline and the controller remain coherent, and a
    /// deferred terminal event is re-resolved (bounded). Returns whether
    /// the attempt was applied and the follow-up pipeline operation.
    pub(crate) async fn commit_skip(&mut self, attempt_id: u64, result: &Result<(), PipelineError>) -> (bool, SkipFollowup) {
        // Correlation before the take: a stale completion must not destroy
        // the newer pending attempt.
        let Some(pending) = self.pending_skip.as_ref() else {
            return (false, SkipFollowup::None);
        };
        if pending.attempt_id != attempt_id {
            return (false, SkipFollowup::None);
        }
        let pending = self.pending_skip.take().expect("attempt id matched above");
        match result {
            Ok(()) => {
                // The physical Replace succeeded: the pipeline plays
                // `pending.next_song`. `commit_current` ALWAYS advances the
                // in-memory current to that song (the logical current must
                // represent what the pipeline actually plays) — the outcome
                // only says how the database side went:
                // - Applied: cursor (and refill) persisted;
                // - Deferred: persistence failed, the dirty cursor retries
                //   it on the next queue reload;
                // - Missing: a reload removed the song from the queue while
                //   the Replace was in flight; the current is represented as
                //   a phantom (the documented convention for a current that
                //   vanished while playing) until a handover commits a queue
                //   member.
                // None of the three is a "no-op" — the pipeline adopted the
                // song, so claiming anything else would split the controller
                // from the physical pipeline.
                let outcome = self.queue.commit_current(&pending.next_song, pending.anchor).await;
                let outcome_successor = match &outcome {
                    super::queue_manager::CommitOutcome::Applied { successor }
                    | super::queue_manager::CommitOutcome::Deferred { successor }
                    | super::queue_manager::CommitOutcome::Missing { successor } => {
                        if matches!(outcome, super::queue_manager::CommitOutcome::Deferred { .. }) {
                            tracing::warn!(station_id = %self.station_id, "skip commit deferred the queue cursor persistence; it retries on the next queue reload");
                        } else if matches!(outcome, super::queue_manager::CommitOutcome::Missing { .. }) {
                            tracing::warn!(station_id = %self.station_id, queue_item_id = %pending.next_song.queue_item_id, "the skip target vanished from the queue while the replace was in flight; representing it as the logical current until a handover commits a queue member");
                        }
                        successor.clone()
                    }
                };
                // Identity-change bookkeeping formerly done by
                // `replace_current` at command time — now only after the
                // pipeline actually adopted the new track.
                self.generation += 1;
                self.decode_exclusions = None;
                self.pending_realign = None;
                self.idle = false;
                let is_stopped = self.state == PipelineState::Stopped;
                if !is_stopped {
                    if let Some(ref failed_next) = pending.failed_staged_next {
                        self.record_decode_exclusion(failed_next.clone());
                    }
                }
                if pending.is_play_resume && !is_stopped {
                    self.state = PipelineState::Playing;
                }
                if let Some(play_id) = pending.resolving_play_attempt {
                    if self.resolved_play_success == Some(play_id) {
                        self.resolved_play_success = None;
                        self.pending_play_resolved_by_skip = None;
                        self.state = PipelineState::Playing;
                    } else if self.last_failed_play == Some(play_id) {
                        self.last_failed_play = None;
                        self.pending_play_resolved_by_skip = None;
                    } else if self.pending_play == Some(play_id) {
                        // Play has not completed yet: outcome is unknown!
                        // Defer setting playback state until commit_play arrives;
                        // station remains Stopped on the new track.
                        self.pending_play_resolved_by_skip = Some(play_id);
                    }
                } else {
                    self.last_failed_play = None;
                    self.resolved_play_success = None;
                    self.pending_play_resolved_by_skip = None;
                }
                self.invalidate_reconnect_chain();
                if self.known_disconnected_output != Some((self.generation, self.output_epoch)) {
                    self.known_disconnected_output = None;
                }

                let desired_successor = if !is_stopped && pending.failed_staged_next.is_some() {
                    self.effective_successor()
                } else {
                    outcome_successor
                };

                let anchor = self.queue.anchor_after_current();

                // The identity moved on: any deferred terminal event of the
                // old identity is stale.
                self.deferred_terminal = None;
                if !is_stopped {
                    if let Some(failed_track) = pending.failed_current {
                        self.deferred_terminal = Some(DeferredTerminal {
                            generation: self.generation,
                            track: failed_track,
                            retries_left: 1,
                        });
                        if self.state == PipelineState::Playing {
                            self.publish_song_change();
                            self.push_queue_update().await;
                            if let Ok(prepared) = self.skip().await {
                                return (true, SkipFollowup::Operation(prepared));
                            }
                        }
                    }
                }

                // If no immediate recovery skip replaced the current track,
                // synchronize any queue change or staged-next decode failure
                // into the physical pipeline via a correlated realign roll,
                // but ONLY while the pipeline is active (Playing or Paused).
                // A stopped station must never issue new pipeline operations
                // or create a pending_realign.
                let realign = if matches!(self.state, PipelineState::Playing | PipelineState::Paused)
                    && desired_successor.as_ref().map(Self::key_of) != pending.staged_next.as_ref().map(Self::key_of)
                {
                    let current_track = Self::track(self.queue.current_song_info().expect("a committed skip has a current track"));
                    let replacement = desired_successor.clone().map(|song| {
                        let track = Self::track(song);
                        let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, Some(&track));
                        PlannedNext { track, transition }
                    });
                    let change = match &pending.staged_next {
                        Some(staged) => RollingChange::ReplaceNext {
                            expected_next: Self::key_of(staged),
                            replacement,
                        },
                        None => RollingChange::Attach(replacement.expect("a realign without a staged next needs a successor")),
                    };
                    let is_decode_failure = pending.failed_staged_next.is_some();
                    tracing::info!(station_id = %self.station_id, "realigning staged next after the skip commit's queue change");
                    self.prepare_realign(
                        current_track.key,
                        pending.staged_next.as_ref().map(Self::key_of),
                        desired_successor.clone(),
                        change,
                        if is_decode_failure {
                            "decode-failure replacement"
                        } else {
                            "skip-commit realign"
                        },
                        is_decode_failure,
                        if is_decode_failure { 1 } else { 0 },
                    )
                    .map(|prepared| (prepared.realign_id.expect("a realign always carries its id"), prepared.operation))
                } else {
                    None
                };

                match &realign {
                    Some(_) => {
                        self.planned_next = pending.staged_next.clone().map(|song| (song, anchor));
                    }
                    None => {
                        if self.state == PipelineState::Stopped {
                            self.planned_next = None;
                        } else {
                            self.planned_next = desired_successor.clone().map(|song| (song, anchor));
                        }
                    }
                }

                self.publish_song_change();
                self.push_queue_update().await;

                let followup = realign
                    .map(|(id, operation)| SkipFollowup::Realign { id, operation })
                    .unwrap_or(SkipFollowup::None);
                (true, followup)
            }
            Err(error) => {
                tracing::warn!(station_id = %self.station_id, %error, "skip replacement failed; keeping the current track");
                if let Some(play_id) = pending.resolving_play_attempt {
                    if self.last_failed_play == Some(play_id) {
                        self.last_failed_play = None;
                    }
                } else {
                    self.last_failed_play = None;
                    self.resolved_play_success = None;
                }
                let followup = if self.state == PipelineState::Playing {
                    match self.retry_deferred_terminal().await {
                        Ok(Some(prepared)) => SkipFollowup::Operation(prepared),
                        _ => SkipFollowup::None,
                    }
                } else {
                    SkipFollowup::None
                };
                (true, followup)
            }
        }
    }
    async fn retry_deferred_terminal(&mut self) -> Result<Option<PreparedOperation>, PipelineError> {
        let Some(deferred) = self.deferred_terminal.as_ref() else {
            return Ok(None);
        };
        let generation = deferred.generation;
        let track = deferred.track.clone();
        let current = self.queue.current_song_info().map(|song| song.queue_item_id);
        if generation != self.generation || current != Some(track.queue_item_id) {
            // Truly stale: the logical identity physically changed
            self.deferred_terminal = None;
            return Ok(None);
        }
        if self.pending_skip.is_some() {
            // Identity still matches the terminal condition, but a skip / recovery
            // is already in flight. Do NOT clear deferred_terminal; do NOT permit
            // resuming broken playback with SetPlaying(true).
            return Err(PipelineError::Pipeline(
                "play is refused while skip is in flight for terminal track; wait for skip to commit".into(),
            ));
        }
        if deferred.retries_left == 0 {
            tracing::warn!(station_id = %self.station_id, "terminal retry budget exhausted; waiting for the next event or a manual skip");
            return Err(PipelineError::Pipeline(
                "terminal retry budget exhausted; manual skip required".into(),
            ));
        }
        let retries_left = deferred.retries_left - 1;
        self.deferred_terminal = Some(DeferredTerminal {
            generation,
            track,
            retries_left,
        });
        match self.skip().await {
            Ok(prepared) => Ok(Some(prepared)),
            Err(error) => {
                tracing::warn!(station_id = %self.station_id, %error, "terminal retry refused");
                Err(error)
            }
        }
    }

    /// Commits (or abandons) the outcome of a realign roll scheduled by a
    /// skip commit. The correlation happens BEFORE any state is consumed: a
    /// completion for a realign a newer one superseded leaves the newer
    /// record untouched. On success `planned_next` is advanced to the
    /// replacement branch — exactly once — but ONLY while the identity the
    /// roll was built for still describes the controller (same generation,
    /// same queue current) AND the controller still claims the exact staged
    /// branch the roll replaced. A handover of the staged branch (which
    /// clears the pending record and moves the cursor) or a newer skip
    /// commit (new generation / new staged branch) therefore makes a late
    /// completion a no-op: it must never overwrite the newer state. On
    /// failure nothing is claimed: the staged branch stays physically in
    /// the pipeline and `planned_next` keeps describing it. When the
    /// alignment was dirtied by a reload, returns the follow-up realign
    /// (id + operation) that reconciles the branch the pipeline physically
    /// holds after this result with the newest queue successor; `planned_next`
    /// keeps describing that physical branch until the follow-up succeeds.
    pub(crate) fn commit_realign(&mut self, id: u64, result: &Result<(), PipelineError>) -> Option<(u64, PreparedOperation)> {
        // Correlation before the take: a stale completion must not destroy
        // the newer pending realign.
        let pending = self.pending_realign.as_ref()?;
        if pending.id != id {
            return None;
        }
        let pending = self.pending_realign.take().expect("realign id matched above");
        // The completion only applies while the identity the roll was built
        // for still holds. A STALE completion (a newer handover or skip
        // commit moved the generation/current/planned-next) has no authority
        // over the physical state: it must not claim anything AND it must
        // not create follow-up work — its expected_next/desired no longer
        // describe what the pipeline holds.
        let identity_current = self.generation == pending.generation
            && self.queue.current_song_info().map(|song| song.queue_item_id) == Some(pending.current.queue_item_id)
            && self.planned_next.as_ref().map(|(song, _)| Self::key_of(song)) == pending.expected_next;
        if !identity_current {
            tracing::warn!(station_id = %self.station_id, "ignoring stale realign completion");
            return None;
        }
        // The identity still holds, so the physical state after this result
        // is known: the roll either adopted the desired branch (success) or
        // the pipeline still stages the expected branch (failure). Only the
        // former mutates `planned_next`.
        let physical_next = match result {
            Ok(()) => {
                let anchor = self.queue.anchor_after_current();
                self.planned_next = pending.desired.clone().map(|song| (song, anchor));
                pending.desired.as_ref().map(Self::key_of)
            }
            Err(error) => {
                tracing::warn!(station_id = %self.station_id, %error, "realign roll failed; keeping the staged next");
                pending.expected_next.clone()
            }
        };
        // A staged-DecodeFailed fact: the expected branch is KNOWN BROKEN
        // and the pipeline emits DecodeFailed once per branch attach — a
        // failed replacement roll does NOT re-emit it. If this roll FAILED
        // and the expected branch itself is the broken one, the
        // still-staged broken branch must be replaced again. The retry is
        // computed from the now-known physical state and the latest queue
        // (skipping the broken branch via `effective_successor`), and it is
        if result.is_err() && pending.expected_is_broken && pending.decode_retries_left > 0 {
            let broken = pending
                .expected_next
                .clone()
                .expect("a decode-failure realign always replaces a staged branch");
            let successor = self.effective_successor();
            let replacement = successor.clone().map(|song| {
                let track = Self::track(song);
                let current = Self::track(self.queue.current_song_info().expect("a decode retry needs the current song"));
                let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current, Some(&track));
                PlannedNext { track, transition }
            });
            let prepared = self
                .prepare_realign(
                    pending.current.clone(),
                    Some(broken.clone()),
                    successor,
                    RollingChange::ReplaceNext {
                        expected_next: broken,
                        replacement,
                    },
                    "decode-failure retry",
                    true,
                    pending.decode_retries_left.saturating_sub(1),
                )
                .expect("the completion consumed the record; the retry slot is free");
            let id = prepared.realign_id.expect("a realign always carries its id");
            return Some((id, prepared));
        }
        // A queue reload while the roll was in flight changed the desired
        // successor (the alignment-dirty mark): re-read the LATEST queue and
        // prepare another correlated realign toward it — the reload's
        // alignment intent must not be forgotten. `planned_next` keeps
        // describing the branch the pipeline physically holds after this
        // result until the follow-up roll succeeds.
        if !pending.dirty {
            return None;
        }
        self.prepare_realign_followup(&pending, physical_next)
    }

    /// The single-slot serialization rule for staged-next physical
    /// operations: at one generation/current identity there is at most ONE
    /// unresolved realign. This is the only place that mints realign ids,
    /// registers the `PendingRealign` record and correlates the prepared
    /// roll — and it enforces the rule: when the slot already holds a
    /// realign of the SAME identity (same generation, current and expected
    /// staged branch), the in-flight roll already owns the physical change
    /// — a second roll would race it, and a second record would orphan the
    /// first roll's completion — so the intent is NOT forgotten but
    /// remembered for the appropriate outcome. A staged-DecodeFailed intent
    /// arms the record (`expected_is_broken` plus a retry budget): if the
    /// in-flight roll later FAILS, the still-staged broken branch is
    /// replaced again. A record of a DIFFERENT identity is stale by
    /// definition (a skip commit or handover moved the identity): the newer
    /// intent supersedes it. Returns `None` when the intent was absorbed
    /// (remembered) by an in-flight realign.
    fn prepare_realign(
        &mut self,
        current: TrackKey,
        expected_next: Option<TrackKey>,
        desired: Option<SongInfo>,
        change: RollingChange,
        context: &'static str,
        expected_is_broken: bool,
        decode_retries_left: u8,
    ) -> Option<PreparedOperation> {
        if let Some(realign) = &mut self.pending_realign {
            let same_identity =
                realign.generation == self.generation && realign.current == current && realign.expected_next == expected_next;
            if same_identity {
                if expected_is_broken {
                    // A staged DecodeFailed fact for the same branch is never
                    // discarded: the in-flight roll already replaces the
                    // branch, but if that roll FAILS the still-staged broken
                    // branch must be replaced again — the pipeline emits
                    // DecodeFailed once per branch attach and will not
                    // re-emit it. Remember the fact (and re-arm its retry
                    // budget) on the record; the completion decides.
                    realign.expected_is_broken = true;
                    realign.decode_retries_left = realign.decode_retries_left.max(1);
                }
                return None;
            }
        }
        self.realign_seq = self.realign_seq.wrapping_add(1).max(1);
        let id = self.realign_seq;
        let generation = self.generation;
        self.pending_realign = Some(PendingRealign {
            id,
            generation,
            current: current.clone(),
            expected_next,
            desired,
            dirty: false,
            expected_is_broken,
            decode_retries_left: if expected_is_broken { decode_retries_left } else { 0 },
        });
        tracing::info!(station_id = %self.station_id, %context, "preparing a correlated realign");
        Some(PreparedOperation {
            operation: PipelineOperation::Roll(Box::new(RollingPlan {
                generation,
                current,
                change,
            })),
            attempt_id: None,
            realign_id: Some(id),
            play_attempt_id: None,
        })
    }

    fn record_decode_exclusion(&mut self, track: TrackKey) {
        if let Some(current) = self.queue.current_song_info() {
            let current_key = Self::key_of(&current);
            let generation = self.generation;
            let entry = self
                .decode_exclusions
                .get_or_insert_with(|| DecodeExclusions::new(generation, current_key.clone()));
            if !entry.matches(generation, &current_key) {
                *entry = DecodeExclusions::new(generation, current_key);
            }
            entry.add(track);
        }
    }

    /// The queue's effective desired next staged branch for the current
    /// playback identity: skips all unconsumed queue items that are known to
    /// have failed decoding under `(generation, current)`. If all remaining
    /// items are excluded (or the queue is exhausted), returns `None`.
    fn effective_successor(&self) -> Option<SongInfo> {
        let current = self.queue.current_song_info()?;
        let current_key = Self::key_of(&current);
        let excluded = self.decode_exclusions.as_ref().filter(|e| e.matches(self.generation, &current_key));
        let Some(excluded) = excluded else {
            return self.queue.peek_next_song();
        };
        if excluded.is_empty() {
            return self.queue.peek_next_song();
        }

        let mut candidate = self.queue.peek_next_song();
        while let Some(song) = candidate {
            let key = Self::key_of(&song);
            if !excluded.contains(&key) {
                return Some(song);
            }
            candidate = self.queue.successor_after(&key);
        }
        None
    }

    /// Mint the next correlated realign toward the latest queue successor
    /// after a realign completion whose alignment was dirtied by a reload.
    /// `physical_next` is the branch the pipeline physically holds after
    /// the completed roll (the desired one on success, the staged one on
    /// failure) — the four meaningful combinations with the latest queue
    /// successor are: replace staged with successor, drop the staged branch
    /// (successor exhausted), attach the successor (nothing staged), or do
    /// nothing (they already match). `None` is an explicit desired physical
    /// state, never an automatic "no work".
    fn prepare_realign_followup(&mut self, pending: &PendingRealign, physical_next: Option<TrackKey>) -> Option<(u64, PreparedOperation)> {
        let successor = self.effective_successor();
        if successor.as_ref().map(Self::key_of) == physical_next {
            // The latest queue already wants the branch the pipeline holds;
            // nothing to align.
            return None;
        }
        let desired = successor.clone();
        let replacement = successor.map(|song| {
            let track = Self::track(song);
            let current = Self::track(self.queue.current_song_info().expect("a realign follow-up needs the current song"));
            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current, Some(&track));
            PlannedNext { track, transition }
        });
        let change = match &physical_next {
            Some(expected) => RollingChange::ReplaceNext {
                expected_next: expected.clone(),
                // `None` here is the explicit "drop the staged branch" — the
                // queue is exhausted and the pipeline must stop staging it.
                replacement,
            },
            None => {
                // Nothing is staged; the only meaningful operation is an
                // attach of the newest successor. Both None -> no work (the
                // equality check above already returned).
                let replacement = replacement?;
                RollingChange::Attach(replacement)
            }
        };
        let prepared = self.prepare_realign(
            pending.current.clone(),
            physical_next.clone(),
            desired,
            change,
            "realign follow-up",
            false,
            0,
        )?;
        let id = prepared.realign_id.expect("a realign always carries its id");
        Some((id, prepared))
    }

    pub(crate) async fn reload(&mut self, songs: Vec<SongInfo>, align_next: bool) -> Result<Option<PreparedOperation>, PipelineError> {
        let was_stopped = matches!(self.state, PipelineState::Stopped);
        let retain_missing_current = !was_stopped;
        self.queue.reload_songs(songs, retain_missing_current);
        if was_stopped {
            // The station was started while its database queue was still
            // empty, leaving an idle streamer behind. Once songs arrive
            // (manual add, Auto DJ refill, schedule) playback must begin;
            // play() stays a no-op while the queue remains empty.
            return Ok(if self.queue.current_song_info().is_some() {
                Some(self.play().await?)
            } else {
                None
            });
        }
        if !align_next || !matches!(self.state, PipelineState::Playing | PipelineState::Paused) {
            return Ok(None);
        }
        // While a skip (or its realign roll) is in flight, the alignment is
        // owned by the skip commit: it re-reads the queue at commit time and
        // synchronizes the successor into the pipeline explicitly (the
        // realign roll, whose outcome is correlated). A roll prepared now
        // would anchor on the PRE-commit identity — the pipeline already
        // adopted the new generation, so it could only fail as StalePlan —
        // and its optimistic `planned_next` write would race the commit's
        // own realign decision. The reload still applies the new queue; the
        // commit's successor check observes it. For an in-flight realign
        // (whose commit already happened) the reload's new successor is
        // remembered on the pending record (`dirty`): the completion then
        // reconciles toward the latest queue instead of forgetting it.
        if self.pending_skip.is_some() || self.pending_realign.is_some() {
            let successor = self.effective_successor().map(|song| Self::key_of(&song));
            if let Some(realign) = &mut self.pending_realign {
                if successor != realign.desired.as_ref().map(Self::key_of) {
                    tracing::info!(station_id = %self.station_id, "queue changed while a realign is in flight; the realign completion will reconcile the successor");
                    realign.dirty = true;
                }
            }
            return Ok(None);
        }
        let Some(current) = self.queue.current_song_info() else {
            return Ok(None);
        };
        let current_track = Self::track(current.clone());
        let next = self.effective_successor();
        let next_key = next.as_ref().map(Self::key_of);
        let Some((staged_song, _)) = self.planned_next.clone() else {
            // Nothing is staged — e.g. a failed handover Attach left the
            // pipeline without a staged next — so the queue successor is
            // attached with the same correlated two-phase realign: a
            // failure claims nothing and a later reload may attach again.
            let Some(next_song) = next.clone() else {
                return Ok(None);
            };
            let track = Self::track(next_song);
            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, Some(&track));
            return Ok(self.prepare_realign(
                current_track.key,
                None,
                next.clone(),
                RollingChange::Attach(PlannedNext { track, transition }),
                "reload attach",
                false,
                0,
            ));
        };
        let staged_key = Self::key_of(&staged_song);
        if next_key.as_ref() == Some(&staged_key) {
            return Ok(None);
        }
        let replacement = next.clone().map(|song| {
            let track = Self::track(song);
            let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, Some(&track));
            PlannedNext { track, transition }
        });
        // Ordinary queue realignment is two-phase exactly like the skip
        // commit's: `planned_next` keeps describing the branch the pipeline
        // physically stages until the roll SUCCEEDED (`prepare_realign`
        // registers the correlated record; `commit_realign` advances it).
        // Claiming the new successor at submission time would describe a
        // pipeline that still stages the old branch — a roll failure (or a
        // stale-plan rejection) would then leave the controller split from
        // the pipeline and a physically valid handover of the still-staged
        // branch would be dropped as stale.
        Ok(self.prepare_realign(
            current_track.key,
            Some(staged_key.clone()),
            next.clone(),
            RollingChange::ReplaceNext {
                expected_next: staged_key,
                replacement,
            },
            "queue realignment",
            false,
            0,
        ))
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
mod tests;
