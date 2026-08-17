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
                    })
                });
            }
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
        self.decode_exclusions = None;
        self.pending_realign = None;
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
            });
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
            // Exhausted queue: a plain stop, no skip attempt is prepared —
            // the operation carries no attempt id and the runtime submits
            // it without a skip completion (answering the caller directly).
            return Ok(PreparedOperation {
                operation: self.stop_after_current().await,
                attempt_id: None,
                realign_id: None,
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
        self.pending_skip = Some(PendingSkip {
            attempt_id,
            next_song: next,
            anchor,
            staged_next: staged_song,
            response: None,
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
                    expected_current: current_key,
                },
                generation: self.generation + 1,
                output_epoch: self.output_epoch,
                current: next_track,
                next: next_staged,
            })),
            attempt_id: Some(attempt_id),
            realign_id: None,
        })
    }

    /// The attempt id of the skip whose pipeline replacement is currently
    /// in flight, if any (test visibility; the runtime no longer needs it —
    /// completions are bound explicitly at the skip preparation site).
    #[cfg(test)]
    pub(crate) fn pending_skip(&self) -> Option<u64> {
        self.pending_skip.as_ref().map(|pending| pending.attempt_id)
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
                match &outcome {
                    super::queue_manager::CommitOutcome::Applied { .. } => {}
                    super::queue_manager::CommitOutcome::Deferred { .. } => {
                        tracing::warn!(station_id = %self.station_id, "skip commit deferred the queue cursor persistence; it retries on the next queue reload");
                    }
                    super::queue_manager::CommitOutcome::Missing { .. } => {
                        tracing::warn!(station_id = %self.station_id, queue_item_id = %pending.next_song.queue_item_id, "the skip target vanished from the queue while the replace was in flight; representing it as the logical current until a handover commits a queue member");
                    }
                }
                let successor = match &outcome {
                    super::queue_manager::CommitOutcome::Applied { successor }
                    | super::queue_manager::CommitOutcome::Deferred { successor }
                    | super::queue_manager::CommitOutcome::Missing { successor } => successor.clone(),
                };
                // Identity-change bookkeeping formerly done by
                // `replace_current` at command time — now only after the
                // pipeline actually adopted the new track.
                self.generation += 1;
                self.decode_exclusions = None;
                self.pending_realign = None;
                self.invalidate_reconnect_chain();
                if self.known_disconnected_output != Some((self.generation, self.output_epoch)) {
                    self.known_disconnected_output = None;
                }
                // The pipeline adopted exactly the next branch the PairPlan
                // staged; `planned_next` must describe that branch, never a
                // successor the commit's refill/reload invented. When the
                // commit really advanced the queue and the successor
                // changed, the new successor is synchronized into the
                // physical pipeline explicitly (the align-next roll) — and
                // `planned_next` keeps describing the STAGED branch until
                // that roll SUCCEEDED (`commit_realign`). Claiming the new
                // successor at submission time would describe a pipeline
                // that still stages the old branch: a roll failure would
                // leave the controller and the pipeline split, and a
                // physically valid handover of the still-staged branch
                // would be rejected.
                let anchor = self.queue.anchor_after_current();
                let realign = if successor.as_ref().map(Self::key_of) != pending.staged_next.as_ref().map(Self::key_of) {
                    let current_track = Self::track(self.queue.current_song_info().expect("a committed skip has a current track"));
                    let replacement = successor.clone().map(|song| {
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
                    // The commit moved to a physically newer identity: any
                    // in-flight realign of the older identity (e.g. a
                    // decode-failure replacement prepared before the skip)
                    // is superseded, and the newest successor is
                    // synchronized with a fresh correlated roll.
                    tracing::info!(station_id = %self.station_id, "realigning staged next after the skip commit's queue change");
                    self.prepare_realign(
                        current_track.key,
                        pending.staged_next.as_ref().map(Self::key_of),
                        successor.clone(),
                        change,
                        "skip-commit realign",
                        false,
                        0,
                    )
                    .map(|prepared| (prepared.realign_id.expect("a realign always carries its id"), prepared.operation))
                } else {
                    None
                };
                // With a realign in flight `planned_next` becomes the staged
                // branch (the physical truth the pipeline just adopted) —
                // `commit_realign` advances it to the replacement exactly
                // once, on roll success. Without a realign, the successor
                // the queue offers is the truth the pipeline holds.
                match &realign {
                    Some(_) => {
                        self.planned_next = pending.staged_next.clone().map(|song| (song, anchor));
                    }
                    None => {
                        self.planned_next = successor.clone().map(|song| (song, anchor));
                    }
                }
                self.publish_song_change();
                self.push_queue_update().await;
                // The identity moved on: any deferred terminal event of the
                // old identity is stale.
                self.deferred_terminal = None;
                let followup = realign
                    .map(|(id, operation)| SkipFollowup::Realign { id, operation })
                    .unwrap_or(SkipFollowup::None);
                (true, followup)
            }
            Err(error) => {
                tracing::warn!(station_id = %self.station_id, %error, "skip replacement failed; keeping the current track");
                let followup = match self.retry_deferred_terminal().await {
                    Some(prepared) => SkipFollowup::Operation(prepared),
                    None => SkipFollowup::None,
                };
                (true, followup)
            }
        }
    }

    /// After a failed event-driven skip the terminal condition still holds
    /// (the failed replacement never advanced the identity): re-prepare the
    /// skip, bounded to one retry per terminal event so a persistently
    /// failing pipeline cannot hot-loop the executor. Manual skip failures
    /// carry no deferred terminal and never retry here.
    async fn retry_deferred_terminal(&mut self) -> Option<PreparedOperation> {
        let deferred = self.deferred_terminal.take()?;
        if deferred.retries_left == 0 {
            tracing::warn!(station_id = %self.station_id, "terminal retry budget exhausted; waiting for the next event or a manual skip");
            return None;
        }
        let generation = deferred.generation;
        let track = deferred.track.clone();
        let current = self.queue.current_song_info().map(|song| song.queue_item_id);
        if generation == self.generation && current == Some(track.queue_item_id) && self.pending_skip.is_none() {
            self.deferred_terminal = Some(DeferredTerminal {
                generation,
                track,
                retries_left: deferred.retries_left - 1,
            });
            return match self.skip().await {
                Ok(prepared) => Some(prepared),
                Err(error) => {
                    tracing::warn!(station_id = %self.station_id, %error, "terminal retry refused");
                    None
                }
            };
        }
        None
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
                Some(PreparedOperation {
                    operation: self.play().await,
                    attempt_id: None,
                    realign_id: None,
                })
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
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::streamer::driver::{PipelineDriver, PipelineOperation};
    use crate::streamer::runtime::StationRuntime;
    use crate::streamer::testsupport::{self, queued_song, queued_songs, Call, RecordingPipeline};
    use tokio::sync::broadcast::error::TryRecvError;
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
        let failed_key = StationController::track(failed.clone()).key;
        controller.state = PipelineState::Playing;
        controller.generation = 1;
        controller.output_epoch = 1;
        controller.planned_next = Some((failed.clone(), controller.queue.anchor_after_current()));

        let operation = controller
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
        assert_eq!(
            controller.planned_next.as_ref().unwrap().0.queue_item_id,
            failed.queue_item_id,
            "planned_next keeps the failed staged branch until the replacement roll succeeded"
        );

        // The replacement roll succeeds: planned_next advances to the
        // successor; a failed roll would keep the failed branch.
        assert!(controller.commit_realign(realign_id, &Ok(())).is_none());
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, successor.queue_item_id);
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
        let Some(prepared) = operation else {
            panic!("reload into a stopped controller with songs must issue a replace");
        };
        let PipelineOperation::Replace(plan) = prepared.operation else {
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

        // Ordinary reload realignment is two-phase: the reload prepares the
        // roll (with a correlated completion) but `planned_next` keeps the
        // staged branch until the roll physically succeeded.
        let operation = controller
            .reload(vec![a.clone(), x.clone(), b.clone(), c.clone()], true)
            .await
            .unwrap();
        let Some(prepared) = operation else {
            panic!("reorder reload must issue a rolling replacement");
        };
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("reorder reload must issue a rolling replacement");
        };
        let realign_id = prepared.realign_id.expect("the reload roll must be correlated");
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
        assert_eq!(
            controller.planned_next.as_ref().unwrap().0.queue_item_id,
            b.queue_item_id,
            "planned_next must keep the physically staged branch until the roll succeeded"
        );

        // The roll succeeds: planned_next advances to the new successor.
        assert!(
            controller.commit_realign(realign_id, &Ok(())).is_none(),
            "no follow-up without a dirty reload"
        );
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
        let Some(prepared) = operation else {
            panic!("exhausting reload must issue a roll");
        };
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("exhausting reload must issue a roll");
        };
        let realign_id = prepared.realign_id.expect("the drop roll must be correlated");
        let RollingChange::ReplaceNext {
            expected_next,
            replacement,
        } = plan.change
        else {
            panic!("exhausting reload must use ReplaceNext");
        };
        assert_eq!(expected_next.queue_item_id, b.queue_item_id);
        assert!(replacement.is_none(), "no successor may be staged after exhaustion");
        assert_eq!(
            controller.planned_next.as_ref().unwrap().0.queue_item_id,
            b.queue_item_id,
            "planned_next keeps the staged branch until the drop roll succeeded"
        );

        // The drop roll succeeds: the staged claim is dropped.
        assert!(controller.commit_realign(realign_id, &Ok(())).is_none());
        assert!(controller.planned_next.is_none());
    }

    #[tokio::test]
    async fn stale_handover_after_realignment_is_ignored() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let x = queued_song("X", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let prepared = controller
            .reload(vec![a.clone(), x.clone(), b.clone()], true)
            .await
            .unwrap()
            .expect("the swap reload must issue a roll");
        let realign_id = prepared.realign_id.expect("the reload roll must be correlated");
        // The roll succeeded: the staged claim moved to X.
        assert!(controller.commit_realign(realign_id, &Ok(())).is_none());
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
        let song = queued_song("A", 0);
        let fresh = queued_song("B", 1);
        let (mut controller, _) = Harness::playing(vec![song.clone()]).await.into_parts();

        let operation = controller.skip().await.unwrap();
        assert!(matches!(operation.operation, PipelineOperation::Stop));
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
        assert!(matches!(operation.operation, PipelineOperation::Stop));
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

    /// Unpacks a prepared post-handover Attach operation, asserting that it is a
    /// Roll with an Attach of `expected_track`, and returning its correlated realign id.
    fn expect_attach(prepared: PreparedOperation, expected_track: &TrackKey) -> u64 {
        let id = prepared.realign_id.expect("an Attach operation must carry a realign id");
        match prepared.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::Attach(planned) => {
                    assert_eq!(&planned.track.key, expected_track, "the Attach must target the expected successor");
                }
                other => panic!("expected RollingChange::Attach, got {other:?}"),
            },
            other => panic!("expected PipelineOperation::Roll, got {other:?}"),
        }
        id
    }

    /// Sends a staged DecodeFailed for `track` (generation 1) and returns
    /// the prepared replacement operation — or None when the event was
    /// remembered by an in-flight realign (no new operation is minted).
    async fn staged_decode_failure(controller: &mut StationController, track: TrackKey, message: &str) -> Option<PreparedOperation> {
        controller
            .handle_event(PipelineEvent::DecodeFailed {
                generation: 1,
                track,
                message: message.into(),
            })
            .await
            .map(|result| result.expect("a staged decode failure must not error"))
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

    /// Starts a playing controller over `songs` where songs[1] (B) fails
    /// decoding, returning the controller, track keys for B and C, and the
    /// correlated realign id R1.
    async fn prepare_broken_b_playing(songs: &[SongInfo]) -> (StationController, TrackKey, TrackKey, u64) {
        let (mut controller, _) = Harness::playing(songs.to_vec()).await.into_parts();
        let b_key = StationController::track(songs[1].clone()).key;
        let c_key = StationController::track(songs[2].clone()).key;
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");
        (controller, b_key, c_key, r1)
    }

    /// Test A: a manual reconnect through the full `StationRuntime` path —
    /// `StationCommand::Reconnect(response)` hands the response to the
    /// reconnect-aware action and the caller receives the real pipeline
    /// result.
    #[tokio::test]
    async fn manual_reconnect_through_the_runtime_reports_success() {
        run_reconnect_test(async |db| {
            let pipeline = Arc::new(RecordingPipeline::new());
            let harness = Harness::with_db(db.pool.clone(), pipeline.clone(), queued_songs(&["A", "B"]));
            let (runtime, _events) = harness.into_runtime();
            let result = runtime.reconnect().await;
            assert!(result.is_ok(), "the manual caller must receive Ok, got {result:?}");
            assert_eq!(pipeline.count(Call::Reconnect), 1);
            runtime.shutdown().await.unwrap();
        })
        .await;
    }

    /// Test B: a failed manual reconnect through the full runtime path
    /// delivers the actual PipelineError (never a cancelled channel), runs
    /// exactly once, and stays one-shot.
    #[tokio::test]
    async fn manual_reconnect_through_the_runtime_reports_failure() {
        run_reconnect_test(async |db| {
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
            tokio::time::sleep(Duration::from_millis(1500)).await;
            assert_eq!(pipeline.count(Call::Reconnect), 1, "a manual reconnect must stay one-shot");
            runtime.shutdown().await.unwrap();
        })
        .await;
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
        run_reconnect_test(async |db| {
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
        })
        .await;
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
        run_reconnect_test(async |db| {
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
        })
        .await;
    }

    /// Test 1: a disconnect that happened BEFORE the pause is recovered by
    /// Play — no second `SinkDisconnected` event is injected.
    #[tokio::test]
    async fn disconnect_before_pause_is_recovered_by_play_without_a_second_event() {
        run_reconnect_test(async |db| {
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
        })
        .await;
    }

    /// Test 2: a Pause interrupting an in-flight recovery does not lose the
    /// disconnected knowledge — the next Play retries the recovery.
    #[tokio::test]
    async fn pause_interrupting_recovery_keeps_the_output_recoverable() {
        run_reconnect_test(async |db| {
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
        })
        .await;
    }

    /// Test 3: a successful recovery clears the marker — a later Pause/Play
    /// cycle must not run a redundant reconnect.
    #[tokio::test]
    async fn successful_recovery_clears_the_marker_for_later_cycles() {
        run_reconnect_test(async |db| {
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
        })
        .await;
    }

    /// Test 4: a successful manual reconnect while paused clears the marker
    /// — the later Play does not reconnect again.
    #[tokio::test]
    async fn successful_manual_reconnect_clears_the_marker() {
        run_reconnect_test(async |db| {
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
        })
        .await;
    }

    /// Test 4b: a FAILED manual reconnect while paused keeps the marker —
    /// the later Play can still recover.
    #[tokio::test]
    async fn failed_manual_reconnect_keeps_the_marker_for_play_recovery() {
        run_reconnect_test(async |db| {
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
        })
        .await;
    }

    /// Functional regression: a delayed success of an OLD chain (completion
    /// of X arriving after a SECOND disconnect of the same output started
    /// chain Y) must not erase the newer disconnect. Pause invalidates Y,
    /// and the next Play still knows the output is disconnected — it runs
    /// the recovery WITHOUT a third `SinkDisconnected` event.
    #[tokio::test]
    async fn stale_success_before_pause_does_not_block_play_recovery() {
        run_reconnect_test(async |db| {
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
        })
        .await;
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

        // The skip is two-phase: the identity change commits once the
        // replacement succeeded.
        controller.skip().await.unwrap();
        let attempt = controller
            .pending_skip()
            .expect("a prepared skip must stay pending until the executor reports");
        controller.commit_skip(attempt, &Ok(())).await;
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
        run_reconnect_test(async |db| {
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
                controller.reconnect_token_shared(),
                None,
                true,
            ))
            .submit(&urgent_tx);

            // The user skips while the reconnect is still queued: after the
            // replacement succeeds, the output identity is replaced
            // (generation 2) and the reconnect chain of the old identity is
            // invalidated — the shared token flips to 0. The skip is
            // two-phase, so the bookkeeping follows the committed success.
            controller.skip().await.unwrap();
            let attempt = controller
                .pending_skip()
                .expect("a prepared skip must stay pending until the executor reports");
            controller.commit_skip(attempt, &Ok(())).await;
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
        })
        .await;
    }

    /// Controller-level companion: the bookkeeping half of the invariant —
    /// after a skip the old chain's token is no longer current and the
    /// shared executor state no longer matches it.
    #[tokio::test]
    async fn skip_invalidates_the_reconnect_chain_of_the_old_output() {
        run_reconnect_test(async |db| {
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
            let PipelineOperation::Reconnect(_) = disconnect.operation else {
                panic!("a disconnect while playing must produce a reconnect");
            };
            let token_x = controller.current_reconnect_token();
            assert!(controller.reconnect_retry_is_current(token_x));

            controller.skip().await.unwrap();
            // The skip is two-phase: the identity-change bookkeeping is
            // applied only once the pipeline replacement succeeded.
            assert_eq!(controller.generation(), 1, "a prepared skip must not commit the generation yet");
            let attempt = controller
                .pending_skip()
                .expect("a prepared skip must stay pending until the executor reports");
            controller.commit_skip(attempt, &Ok(())).await;
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

            // Current = A, generation 1; the pipeline plays A/1.
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
            let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");

            // Play: its replace is the only one that may consume the gate
            // permit — wait for it to enter, then let it through.
            play_through_gate(&runtime, gate).await;

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
        let (mut controller, _) = Harness::playing(queued_songs(&["A", "B", "C"])).await.into_parts();
        assert_eq!(controller.generation(), 1);

        let operation = controller.skip().await.expect("a skip with a successor must prepare a replacement");
        assert!(matches!(operation.operation, PipelineOperation::Replace(_)));
        let attempt = controller.pending_skip().expect("the prepared skip must be pending");

        // A foreign completion (attempt 0 never existed) must not consume
        // the pending state, must not commit, and must not touch the
        // generation.
        let (applied, followup) = controller.commit_skip(attempt.wrapping_sub(1), &Ok(())).await;
        assert!(!applied, "a stale completion must not apply");
        assert!(
            matches!(followup, SkipFollowup::None),
            "a stale completion must not produce follow-up work"
        );
        assert_eq!(
            controller.pending_skip(),
            Some(attempt),
            "the pending attempt must survive a stale completion"
        );
        assert_eq!(controller.generation(), 1, "a stale completion must not commit");

        // The real completion still commits exactly once.
        let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
        assert!(applied, "the current completion must apply");
        assert!(
            matches!(followup, SkipFollowup::None),
            "the queue successor still matches the staged next"
        );
        assert_eq!(controller.generation(), 2);
        assert_eq!(controller.pending_skip(), None, "the commit must consume the pending attempt");
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
            let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");

            play_through_gate(&runtime, gate).await;

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

    /// A terminal event arriving while a skip is already in flight must not
    /// be lost: it is deferred, and when the in-flight skip FAILS (without
    /// advancing the identity) the deferred terminal re-triggers the
    /// progression with a fresh attempt.
    #[tokio::test]
    async fn terminal_event_during_an_in_flight_skip_is_deferred_and_retried_after_the_skip_fails() {
        run_reconnect_test(async |db| {
            let songs = queued_songs(&["A", "B"]);
            let pipeline = Arc::new(RecordingPipeline::new());
            let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
            let station_id = controller.station_id;
            seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;

            controller.play().await;
            assert_eq!(controller.generation(), 1);
            let operation = controller.skip().await.expect("a skip with a successor must prepare a replacement");
            assert!(matches!(operation.operation, PipelineOperation::Replace(_)));
            let attempt = controller.pending_skip().expect("the prepared skip must be pending");

            // The EOS for the still-playing A/1 arrives while the skip is in
            // flight: deferred, not re-skipped.
            let event = controller
                .handle_event(PipelineEvent::CurrentEos {
                    generation: 1,
                    current: StationController::track(songs[0].clone()).key,
                })
                .await;
            assert!(event.is_none(), "the terminal event must be deferred while a skip is in flight");

            // The in-flight skip fails: the deferred terminal re-triggers the
            // progression as a fresh attempt, and nothing was committed.
            let (applied, followup) = controller
                .commit_skip(attempt, &Err(PipelineError::Pipeline("boom: injected failure".into())))
                .await;
            assert!(applied, "the failed attempt must resolve");
            match followup {
                SkipFollowup::Operation(prepared) => {
                    assert!(matches!(prepared.operation, PipelineOperation::Replace(_)));
                }
                other => panic!("expected a retry Replace followup, got {other:?}"),
            }
            let retry = controller.pending_skip().expect("the retry must be pending");
            assert_ne!(retry, attempt, "every skip attempt must carry a fresh id");
            assert_eq!(controller.generation(), 1, "the failed skip must not commit");

            // The retry succeeds and commits B exactly once.
            let (applied, followup) = controller.commit_skip(retry, &Ok(())).await;
            assert!(applied);
            assert!(
                matches!(followup, SkipFollowup::None),
                "the queue successor still matches the staged next"
            );
            assert_eq!(controller.generation(), 2);
            assert_eq!(controller.pending_skip(), None);
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
            let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
            let c_key = StationController::track(songs[2].clone()).key;
            let d = queued_song("D", 2);
            let d_key = StationController::track(d.clone()).key;

            play_through_gate(&runtime, gate).await;

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
            let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
            let c_key = StationController::track(songs[2].clone()).key;
            let d = queued_song("D", 2);
            let d_key = StationController::track(d.clone()).key;

            play_through_gate(&runtime, gate).await;

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

    /// While a realign roll is in flight the controller keeps claiming the
    /// STAGED branch; a handover of that branch supersedes the realign
    /// (the pipeline no longer stages it), and the late realign completion
    /// — success or failure — must have zero effect on the newer state.
    #[tokio::test]
    async fn late_realign_completion_is_inert_after_a_newer_handover() {
        run_reconnect_test(async |db| {
            let songs = queued_songs(&["A", "B", "C"]);
            let pipeline = Arc::new(RecordingPipeline::new());
            let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
            let station_id = controller.station_id;
            seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
            let c_key = StationController::track(songs[2].clone()).key;
            let d = queued_song("D", 2);
            let d_key = StationController::track(d.clone()).key;

            controller.play().await;
            let attempt = prepare_skip_attempt(&mut controller).await;
            replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;

            // Commit schedules the realign C -> D; planned_next STAYS C —
            // the roll's outcome is not claimed before it succeeded.
            let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
            assert!(applied);
            let (id, roll) = match followup {
                SkipFollowup::Realign { id, operation } => {
                    let PipelineOperation::Roll(plan) = operation else {
                        panic!("a realign followup must be a roll");
                    };
                    (id, plan)
                }
                other => panic!("expected a realign followup, got {other:?}"),
            };
            match roll.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, c_key, "the roll must target the staged branch");
                    assert_eq!(replacement.expect("D must be staged").track.key, d_key);
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            }
            assert_eq!(roll.generation, 2, "the roll must run under the new identity");
            assert_eq!(
                controller.planned_next().as_ref(),
                Some(&c_key),
                "planned_next must keep describing the staged C while the roll is in flight"
            );
            assert_eq!(
                controller.pending_realign(),
                Some(id),
                "the commit must register the in-flight realign"
            );

            // The pipeline hands over to the still-staged C before the roll
            // completes: the handover is physically valid and commits; the
            // realign of the now-current branch is superseded explicitly.
            let handover = controller
                .handle_event(PipelineEvent::Handover {
                    generation: 2,
                    current: c_key,
                })
                .await
                .expect("a handover of the still-staged branch must be accepted")
                .expect("the handover must not fail");
            assert!(handover.attempt_id.is_none(), "a handover is never a skip operation");
            // The handover superseded the old realign AND minted a fresh
            // correlated Attach of the queue successor D: planned_next stays
            // None — the pipeline stages nothing yet — until that attach
            // roll succeeds.
            let attach_id = handover.realign_id.expect("the handover's attach must be a correlated realign");
            assert_ne!(attach_id, id, "the attach is a fresh realign, not the superseded one");
            assert_eq!(
                controller.planned_next(),
                None,
                "planned_next must stay None while the handover's attach is in flight"
            );

            // The late realign completion (Ok or Err) must not overwrite the
            // newer state.
            assert!(
                controller.commit_realign(id, &Ok(())).is_none(),
                "a superseded realign completion must not apply"
            );
            assert_eq!(controller.planned_next(), None, "the late completion must not touch planned_next");
            assert!(
                controller
                    .commit_realign(id, &Err(PipelineError::Pipeline("boom".into())))
                    .is_none(),
                "a superseded failure must not apply either"
            );
            assert_eq!(controller.planned_next(), None);
            // The handover's own attach succeeds: the queue successor is
            // claimed exactly once.
            assert!(controller.commit_realign(attach_id, &Ok(())).is_none());
            assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
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
        let songs = queued_songs(&["A", "B", "X"]);
        let (mut controller, _) = Harness::playing(songs.clone()).await.into_parts();
        let b_key = StationController::track(songs[1].clone()).key;

        let attempt = prepare_skip_attempt(&mut controller).await;
        assert_eq!(controller.generation(), 1);

        // A DecodeFailed of the staged branch produces a correlated roll —
        // explicitly bound to its own realign record, NOT to the pending skip attempt.
        let prepared = controller
            .handle_event(PipelineEvent::DecodeFailed {
                generation: 1,
                track: b_key.clone(),
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
        // The unrelated operation changed nothing about the skip.
        assert_eq!(controller.pending_skip(), Some(attempt), "the pending skip must survive");
        assert_eq!(controller.generation(), 1, "the unrelated operation must not commit anything");

        // The skip's own completion still commits exactly once.
        let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
        assert!(applied);
        assert!(
            matches!(followup, SkipFollowup::None),
            "the queue successor still matches the staged next"
        );
        assert_eq!(controller.generation(), 2);
        assert_eq!(controller.pending_skip(), None);
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
            let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");

            play_through_gate(&runtime, gate).await;

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

    /// A Reload processed while a skip replacement is in flight applies the
    /// new queue but defers alignment to the skip commit: no roll is
    /// prepared against the pre-commit identity, and the commit's own
    /// realign decision observes the reloaded queue. The staged claim is
    /// only advanced once the realign roll succeeded.
    #[tokio::test]
    async fn reload_while_a_skip_is_in_flight_defers_alignment_to_the_commit() {
        run_reconnect_test(async |db| {
            let songs = queued_songs(&["A", "B", "C"]);
            let pipeline = Arc::new(RecordingPipeline::new());
            let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
            let station_id = controller.station_id;
            seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
            let c_key = StationController::track(songs[2].clone()).key;
            let d = queued_song("D", 2);
            let d_key = StationController::track(d.clone()).key;

            controller.play().await;
            let attempt = prepare_skip_attempt(&mut controller).await;
            replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
            let b_key = StationController::track(songs[1].clone()).key;

            // The newer command (reload of the changed queue) is processed
            // while the skip is still in flight: it is applied (no error)
            // but emits NO roll — alignment is owned by the skip commit.
            // planned_next still describes what the pipeline physically
            // stages: B, the play's staged branch (the skip replacement has
            // not run yet).
            let operation = controller
                .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
                .await
                .unwrap();
            assert!(
                operation.is_none(),
                "a reload during an in-flight skip must not schedule a stale roll"
            );
            assert_eq!(
                controller.planned_next().as_ref(),
                Some(&b_key),
                "planned_next still describes the branch the pipeline actually stages"
            );

            // The commit observes the reloaded queue and schedules the
            // realign; planned_next stays C until it succeeded.
            let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
            assert!(applied);
            let id = match followup {
                SkipFollowup::Realign { id, operation } => {
                    let PipelineOperation::Roll(plan) = operation else {
                        panic!("a realign followup must be a roll");
                    };
                    match plan.change {
                        RollingChange::ReplaceNext {
                            expected_next,
                            replacement,
                        } => {
                            assert_eq!(&expected_next, &c_key);
                            assert_eq!(replacement.expect("D must be staged").track.key, d_key);
                        }
                        other => panic!("expected ReplaceNext, got {other:?}"),
                    }
                    id
                }
                other => panic!("expected a realign followup, got {other:?}"),
            };
            assert_eq!(
                controller.planned_next().as_ref(),
                Some(&c_key),
                "planned_next must not claim D before the roll succeeded"
            );

            // The roll succeeds: planned_next advances to D exactly once.
            assert!(
                controller.commit_realign(id, &Ok(())).is_none(),
                "the current realign completion must apply without follow-up"
            );
            assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
            assert!(
                controller.commit_realign(id, &Ok(())).is_none(),
                "a second completion of the same realign must not apply"
            );
            assert_eq!(controller.planned_next().as_ref(), Some(&d_key));

            // After the successful realign a handover of the OLD staged
            // branch is stale (the pipeline stages D now) and must be
            // dropped; a handover of the realigned branch commits cleanly.
            assert!(
                controller
                    .handle_event(PipelineEvent::Handover {
                        generation: 2,
                        current: c_key,
                    })
                    .await
                    .is_none(),
                "a handover of the replaced staged branch must be dropped after the realign"
            );
            assert_eq!(
                persisted_cursor(&db.pool, station_id).await,
                Some(songs[1].queue_item_id),
                "the stale handover must not move the cursor"
            );
            // The handover commits D and may return None when the queue is
            // exhausted (no attach follows) — the committed cursor is the
            // proof of acceptance.
            let _ = controller
                .handle_event(PipelineEvent::Handover {
                    generation: 2,
                    current: d_key,
                })
                .await;
            wait_for_db_cursor(&db.pool, station_id, Some(d.queue_item_id)).await;
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
            let gate = pipeline.replace_gate.as_ref().expect("gated pipeline");
            let c_key = StationController::track(songs[2].clone()).key;
            let d = queued_song("D", 2);
            let d_key = StationController::track(d.clone()).key;

            play_through_gate(&runtime, gate).await;

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

    /// The controller-level identity assertions behind the runtime scenario:
    /// after the physical Replace succeeded while B was removed from the
    /// queue, the controller/queue current IS B (phantom), planned_next
    /// stays on the staged C until the realign succeeds, and the handover
    /// of the realigned branch commits for real.
    #[tokio::test]
    async fn commit_skip_with_a_removed_target_keeps_the_logical_identity_on_the_pipeline() {
        run_reconnect_test(async |db| {
            let songs = queued_songs(&["A", "B", "C"]);
            let pipeline = Arc::new(RecordingPipeline::new());
            let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
            let station_id = controller.station_id;
            seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
            let b_key = StationController::track(songs[1].clone()).key;
            let c_key = StationController::track(songs[2].clone()).key;
            let d = queued_song("D", 2);
            let d_key = StationController::track(d.clone()).key;

            controller.play().await;
            let attempt = prepare_skip_attempt(&mut controller).await;

            // The queue no longer contains B while the Replace is in flight.
            remove_persisted_song(&db.pool, &b_key).await;
            replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
            assert!(
                controller.reload(vec![songs[0].clone(), d.clone()], true).await.unwrap().is_none(),
                "a reload during an in-flight skip must not schedule a stale roll"
            );

            let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
            assert!(applied, "the physical Replace succeeded; the commit must apply");
            // The non-negotiable identity invariant: controller current ==
            // queue current == the physically adopted B.
            assert_eq!(
                controller
                    .queue
                    .current_song_info()
                    .expect("a committed skip has a current")
                    .queue_item_id,
                songs[1].queue_item_id,
                "the controller current must be the physically adopted B, never a queue head it did not play"
            );
            assert_eq!(
                controller.planned_next().as_ref(),
                Some(&c_key),
                "planned_next keeps describing the staged C until the realign succeeds"
            );
            let (id, roll) = expect_realign_followup(followup);
            match roll.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, c_key);
                    assert_eq!(replacement.expect("D must be staged").track.key, d_key);
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            }
            assert_eq!(roll.generation, 2, "the roll must run under the new identity");
            assert_eq!(
                roll.current.queue_item_id, b_key.queue_item_id,
                "the roll anchors on the physical current B"
            );

            // The realign succeeds: planned_next advances to D, and the
            // handover of the realigned branch commits D — superseding the
            // phantom B for good.
            assert!(
                controller.commit_realign(id, &Ok(())).is_none(),
                "no follow-up realign without a dirty reload"
            );
            assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
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

            controller.play().await;
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

            controller.play().await;
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

            controller.play().await;
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

            controller.play().await;
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

            controller.play().await;
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

    /// A STALE dirty realign completion must create NO work: after a newer
    /// handover superseded the realign (and its record), the late completion
    /// must not register a new realign, return a follow-up operation, or
    /// touch the newer planned_next — the stale record has no authority over
    /// the physical state.
    #[tokio::test]
    async fn stale_dirty_realign_completion_creates_no_follow_up_work() {
        run_reconnect_test(async |db| {
            let songs = queued_songs(&["A", "B", "C"]);
            let pipeline = Arc::new(RecordingPipeline::new());
            let (mut controller, _) = Harness::with_db(db.pool.clone(), pipeline.clone(), songs.clone()).into_parts();
            let station_id = controller.station_id;
            seed_station(&db.pool, station_id, Some(songs[0].queue_item_id), &songs).await;
            let c_key = StationController::track(songs[2].clone()).key;
            let d = queued_song("D", 2);
            let e = queued_song("E", 2);

            controller.play().await;
            let attempt = prepare_skip_attempt(&mut controller).await;
            replace_persisted_successor(&db.pool, station_id, &c_key, &d).await;
            controller
                .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
                .await
                .unwrap();
            let (applied, followup) = controller.commit_skip(attempt, &Ok(())).await;
            assert!(applied);
            let (r1, _roll) = expect_realign_followup(followup);

            // R1 becomes dirty: the queue's successor moves D -> E.
            assert!(controller
                .reload(vec![songs[0].clone(), songs[1].clone(), e.clone()], true)
                .await
                .unwrap()
                .is_none());

            // A newer physical transition supersedes R1: the pipeline hands
            // over to the still-staged C, clearing the old realign record
            // and minting a FRESH correlated Attach of the newest successor
            // E — the handover's own staged-next operation.
            let prepared = controller
                .handle_event(PipelineEvent::Handover {
                    generation: 2,
                    current: c_key,
                })
                .await
                .expect("the handover of the still-staged branch must be accepted")
                .expect("the handover must not fail");
            let attach_id = prepared.realign_id.expect("the handover's attach must be a correlated realign");
            assert_ne!(attach_id, r1, "the attach is a fresh realign, not the superseded one");

            // The late R1 completion — success or failure — must be fully
            // inert: no new realign, no follow-up operation, no planned_next
            // change (the handover's newer attach owns the state).
            assert!(
                controller.commit_realign(r1, &Ok(())).is_none(),
                "a stale dirty completion must not create follow-up work"
            );
            assert_eq!(
                controller.pending_realign(),
                Some(attach_id),
                "the stale completion must not register or touch the newer realign"
            );
            let handover_claim = controller.planned_next();
            assert!(
                controller
                    .commit_realign(r1, &Err(PipelineError::Pipeline("boom".into())))
                    .is_none(),
                "a stale dirty failure must not create follow-up work either"
            );
            assert_eq!(
                controller.planned_next(),
                handover_claim,
                "the stale completions must not touch the newer planned_next"
            );
        })
        .await;
    }

    /// A failed ordinary reload realign keeps the staged claim: planned_next
    /// stays the physically staged branch, so a handover of it remains
    /// accepted (it schedules the attach of the queue successor) instead of
    /// being dropped as a stale realignment.
    #[tokio::test]
    async fn failed_reload_realign_keeps_the_staged_claim() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let x = queued_song("X", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let x_key = StationController::track(x.clone()).key;

        let prepared = controller
            .reload(vec![a.clone(), x.clone(), b.clone()], true)
            .await
            .unwrap()
            .expect("the swap reload must issue a roll");
        let realign_id = prepared.realign_id.expect("the reload roll must be correlated");

        // The roll fails: the pipeline still stages B, so planned_next stays
        // B — never an optimistic X.
        assert!(controller
            .commit_realign(realign_id, &Err(PipelineError::Pipeline("boom".into())))
            .is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));

        // A handover of the still-staged B is physically valid and accepted.
        let handover = controller
            .handle_event(PipelineEvent::Handover {
                generation: 1,
                current: b_key,
            })
            .await
            .expect("a handover of the still-staged branch must be accepted")
            .expect("the handover must not fail");
        assert!(handover.attempt_id.is_none());
        // The accepted handover's Attach of X is two-phase: planned_next
        // stays None — the pipeline stages nothing yet — until the attach
        // roll succeeds.
        let attach_id = handover.realign_id.expect("the handover's attach must be a correlated realign");
        assert_eq!(controller.planned_next(), None, "the attach must not be claimed optimistically");
        assert!(controller.commit_realign(attach_id, &Ok(())).is_none());
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&x_key),
            "the successful attach claims the queue successor"
        );
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
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let x = queued_song("X", 2);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), x.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let x_key = StationController::track(x.clone()).key;

        let prepared = prepare_handover_attach(&mut controller, b_key.clone()).await;
        let attach_id = prepared.realign_id.expect("the handover's attach must be a correlated realign");
        assert!(prepared.attempt_id.is_none(), "a handover is never a skip operation");
        assert_eq!(
            controller
                .queue
                .current_song_info()
                .expect("a committed handover has a current")
                .queue_item_id,
            b.queue_item_id
        );
        assert_eq!(
            controller.pending_realign(),
            Some(attach_id),
            "the handover must register the attach realign"
        );
        assert_eq!(
            controller.planned_next(),
            None,
            "planned_next must stay None while the attach is in flight"
        );
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("the handover must issue an attach roll");
        };
        assert_eq!(plan.generation, 1);
        assert_eq!(plan.current, b_key);
        match plan.change {
            RollingChange::Attach(next) => {
                assert_eq!(next.track.key, x_key, "the attach must target the queue successor");
            }
            other => panic!("expected an Attach, got {other:?}"),
        }

        // The attach roll succeeds: the queue successor is claimed.
        assert!(controller.commit_realign(attach_id, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&x_key));
    }

    /// A failed Handover Attach claims nothing: `planned_next` stays None,
    /// no controller state claims the successor — and a later reload
    /// attaches it again through the same correlated mechanism.
    #[tokio::test]
    async fn failed_handover_attach_claims_nothing_and_a_reload_recovers() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let x = queued_song("X", 2);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), x.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let x_key = StationController::track(x.clone()).key;

        let prepared = prepare_handover_attach(&mut controller, b_key.clone()).await;
        let attach_id = prepared.realign_id.expect("the handover's attach must be a correlated realign");

        // The attach fails: nothing is claimed.
        assert!(controller
            .commit_realign(attach_id, &Err(PipelineError::Pipeline("boom".into())))
            .is_none());
        assert_eq!(controller.planned_next(), None, "a failed attach must claim nothing");
        assert_eq!(controller.pending_realign(), None);
        assert_eq!(
            controller
                .queue
                .current_song_info()
                .expect("a committed handover has a current")
                .queue_item_id,
            b.queue_item_id
        );

        // A later reload reconciles: the orphaned successor is attached
        // again with a fresh correlated realign (still two-phase).
        let prepared = controller
            .reload(vec![a.clone(), b.clone(), x.clone()], true)
            .await
            .unwrap()
            .expect("the reload must attach the orphaned successor");
        let recovery_id = prepared.realign_id.expect("the recovery attach must be correlated");
        assert_ne!(recovery_id, attach_id, "the recovery is a fresh realign");
        let PipelineOperation::Roll(plan) = prepared.operation else {
            panic!("the reload must issue an attach roll");
        };
        match plan.change {
            RollingChange::Attach(next) => {
                assert_eq!(next.track.key, x_key, "the recovery must attach the queue successor");
            }
            other => panic!("expected an Attach, got {other:?}"),
        }
        assert_eq!(controller.planned_next(), None, "the recovery attach is also two-phase");
        assert!(controller.commit_realign(recovery_id, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&x_key));
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
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let c_key = StationController::track(c.clone()).key;

        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");
        assert_eq!(controller.pending_realign(), Some(r1));
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));

        // The same staged branch fails again before R1 completes: absorbed —
        // no second operation, no second record, R1 keeps ownership.
        assert!(
            staged_decode_failure(&mut controller, b_key.clone(), "broken next again")
                .await
                .is_none(),
            "a duplicate decode failure must not mint a second operation"
        );
        assert_eq!(controller.pending_realign(), Some(r1), "R1 must keep ownership");
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));

        // R1 succeeds: no recovery work — the absorbed event is satisfied
        // (the broken branch is gone) — and the successor is claimed
        // exactly once.
        assert!(
            controller.commit_realign(r1, &Ok(())).is_none(),
            "a satisfied decode failure must not produce follow-up work"
        );
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));
    }

    /// A staged DecodeFailed while a RELOAD realign for the same staged
    /// branch is in flight is absorbed the same way: the reload roll owns
    /// the physical change.
    #[tokio::test]
    async fn decode_failure_during_a_reload_realign_keeps_the_record() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let d = queued_song("D", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone(), d.clone()])
            .await
            .into_parts();
        let b_key = StationController::track(b.clone()).key;
        let c_key = StationController::track(c.clone()).key;

        // The queue reorders so the successor becomes C: the reload realigns
        // the staged B -> C.
        let prepared = controller
            .reload(vec![a.clone(), c.clone(), b.clone(), d.clone()], true)
            .await
            .unwrap()
            .expect("the reordered reload must issue a roll");
        let r1 = prepared.realign_id.expect("the reload roll must be correlated");
        assert_eq!(controller.pending_realign(), Some(r1));

        // The still-staged B fails to decode while the reload roll is in
        // flight: absorbed — no second roll, R1 keeps ownership (and the
        // decode fact is remembered on the record).
        assert!(staged_decode_failure(&mut controller, b_key.clone(), "broken next").await.is_none());
        assert_eq!(controller.pending_realign(), Some(r1), "R1 must keep ownership");
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));

        // R1 succeeds: the reordered successor is claimed.
        assert!(controller.commit_realign(r1, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));
    }

    /// A decode-failure intent that arrives while a realign is in flight is
    /// not lost: after R1 completes, the follow-up realign is based on the
    /// NOW-KNOWN physical state (the branch the roll physically adopted),
    /// never on a stale second record.
    #[tokio::test]
    async fn decode_failure_intent_is_reconciled_after_the_in_flight_realign() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let d = queued_song("D", 2);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let c_key = StationController::track(c.clone()).key;
        let d_key = StationController::track(d.clone()).key;

        // The staged B fails: R1 replaces it with C.
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");

        // The queue drops B and offers D while R1 is in flight, and a
        // duplicate DecodeFailed(B) arrives too: both intents are absorbed —
        // R1 keeps ownership, and the dirty mark reconciles the newest
        // successor after the completion.
        assert!(controller.reload(vec![a.clone(), d.clone()], true).await.unwrap().is_none());
        assert!(staged_decode_failure(&mut controller, b_key, "broken next again").await.is_none());
        assert_eq!(controller.pending_realign(), Some(r1));

        // R1 succeeds: the pipeline physically stages C, so planned_next
        // advances to C — and the follow-up realign replaces C with the
        // newest successor D.
        let (r2, followup) = controller
            .commit_realign(r1, &Ok(()))
            .expect("the dirty realign must produce the follow-up");
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&c_key),
            "planned_next must describe the physically staged C, not D"
        );
        let PipelineOperation::Roll(plan) = followup.operation else {
            panic!("the follow-up must be a roll");
        };
        match plan.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, c_key, "the follow-up must target the physically staged branch");
                assert_eq!(replacement.expect("D must be staged").track.key, d_key);
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r2));

        // The follow-up succeeds: the newest successor is claimed.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
    }

    /// The absorbed duplicate staged DecodeFailed is NOT lost when the
    /// in-flight realign FAILS: the still-staged broken branch is replaced
    /// again — a fresh correlated recovery (R2) computed from the now-known
    /// physical state, never an optimistic claim, and never a second
    /// operation while R1 is unresolved.
    #[tokio::test]
    async fn duplicate_decode_failure_retries_after_the_first_realign_fails() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let c_key = StationController::track(c.clone()).key;

        // First DecodeFailed(B): R1 replaces the staged B with C.
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");
        assert_eq!(controller.pending_realign(), Some(r1));
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));

        // A duplicate DecodeFailed(B) before R1 completes: no second roll,
        // R1 keeps ownership — but the failure fact is remembered.
        assert!(
            staged_decode_failure(&mut controller, b_key.clone(), "broken next again")
                .await
                .is_none(),
            "a duplicate decode failure must not mint a second operation"
        );
        assert_eq!(controller.pending_realign(), Some(r1), "R1 must keep ownership");

        // R1 FAILS: the pipeline still stages the broken B — the absorbed
        // event is preserved as a fresh correlated recovery R2.
        let (r2, followup) = controller
            .commit_realign(r1, &Err(PipelineError::Pipeline("boom".into())))
            .expect("the failed realign must produce the recovery");
        assert_ne!(r2, r1, "the recovery is a fresh realign");
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&b_key),
            "planned_next must keep describing the still-staged broken B until R2 succeeds"
        );
        let PipelineOperation::Roll(plan) = followup.operation else {
            panic!("the recovery must be a roll");
        };
        match plan.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, b_key, "the recovery must target the still-staged broken branch");
                assert_eq!(replacement.expect("C must be staged").track.key, c_key);
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(
            controller.pending_realign(),
            Some(r2),
            "the recovery must be registered and correlated"
        );

        // R2 succeeds: the replacement is claimed exactly once.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));
    }

    /// Queue-dirty reconciliation and the preserved decode-failure fact
    /// compose into exactly ONE correlated next operation when R1 fails:
    /// the recovery targets the physical B (still staged) with the latest
    /// queue's successor after the broken branch — never two competing
    /// rolls.
    #[tokio::test]
    async fn dirty_queue_and_duplicate_decode_failure_compose_into_one_recovery() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let d = queued_song("D", 2);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let d_key = StationController::track(d.clone()).key;

        // R1 replaces the staged broken B with C; a duplicate DecodeFailed
        // arrives too (remembered on the record).
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");
        assert!(staged_decode_failure(&mut controller, b_key.clone(), "broken next again")
            .await
            .is_none());
        assert_eq!(controller.pending_realign(), Some(r1));

        // A reload changes the queue (C -> D) while R1 is unresolved: the
        // alignment is marked dirty on the record.
        assert!(controller
            .reload(vec![a.clone(), b.clone(), d.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R1 FAILS: exactly ONE next operation — the decode recovery toward
        // the latest queue's successor after the broken B (D).
        let (r2, followup) = controller
            .commit_realign(r1, &Err(PipelineError::Pipeline("boom".into())))
            .expect("the failed realign must produce exactly one recovery");
        assert_ne!(r2, r1, "the recovery is a fresh realign");
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));
        let PipelineOperation::Roll(plan) = followup.operation else {
            panic!("the recovery must be a roll");
        };
        match plan.change {
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => {
                assert_eq!(expected_next, b_key, "the recovery must target the still-staged broken branch");
                assert_eq!(
                    replacement.expect("D must be staged").track.key,
                    d_key,
                    "the recovery must follow the latest queue"
                );
            }
            other => panic!("expected ReplaceNext, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r2), "exactly one correlated record");

        // R2 succeeds: the newest successor is claimed.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
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
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let d = queued_song("D", 3);
        let e = queued_song("E", 4);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let c_key = StationController::track(c.clone()).key;
        let d_key = StationController::track(d.clone()).key;
        let e_key = StationController::track(e.clone()).key;

        // R1: staged B fails to decode -> replaces with C.
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");

        // While R1 is in flight: reload queue to [A, B, D] (B is still raw head).
        // R1 becomes dirty toward D.
        assert!(controller
            .reload(vec![a.clone(), b.clone(), d.clone()], true)
            .await
            .unwrap()
            .is_none());
        assert_eq!(controller.pending_realign(), Some(r1));

        // R1 succeeds: physical state is now C, planned_next becomes C.
        // Follow-up R2 is minted toward D (skipping B).
        let (r2, followup) = controller
            .commit_realign(r1, &Ok(()))
            .expect("the dirty realign must produce follow-up R2");
        assert_ne!(r2, r1);
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));
        match followup.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, c_key);
                    assert_eq!(replacement.expect("D must be desired").track.key, d_key);
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r2));

        // While R2 is in flight: reload queue to [A, B, E] (B is still raw head).
        // R2 becomes dirty toward E.
        assert!(controller
            .reload(vec![a.clone(), b.clone(), e.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R2 succeeds: physical state is now D, planned_next becomes D.
        // Follow-up R3 MUST be minted toward E (NEVER B!).
        let (r3, followup) = controller
            .commit_realign(r2, &Ok(()))
            .expect("the dirty realign must produce follow-up R3");
        assert_ne!(r3, r2);
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&d_key),
            "planned_next must describe physically staged D before R3 succeeds"
        );
        match followup.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, d_key, "R3 must replace physically staged D");
                    assert_eq!(
                        replacement.expect("E must be desired").track.key,
                        e_key,
                        "R3 must select E, never resurrecting broken B"
                    );
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r3));

        // R3 succeeds: no further follow-up; planned_next becomes E.
        assert!(controller.commit_realign(r3, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&e_key));
    }

    /// If a dirty follow-up (R2: C -> D) fails after a reload to [A, B, E],
    /// the physical branch remains C and the follow-up must align C -> E
    /// (never resurrecting broken B).
    #[tokio::test]
    async fn decode_exclusion_survives_follow_up_failure() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let d = queued_song("D", 3);
        let e = queued_song("E", 4);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let c_key = StationController::track(c.clone()).key;
        let e_key = StationController::track(e.clone()).key;

        // R1: staged B fails to decode -> replaces with C.
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");

        // Reload to [A, B, D] while R1 is in flight.
        assert!(controller
            .reload(vec![a.clone(), b.clone(), d.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R1 succeeds -> R2 (C -> D).
        let (r2, _) = controller
            .commit_realign(r1, &Ok(()))
            .expect("R1 success must produce follow-up R2");

        // While R2 is in flight: reload to [A, B, E].
        assert!(controller
            .reload(vec![a.clone(), b.clone(), e.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R2 FAILS: physical state is still C. Follow-up must be C -> E.
        let (r3, followup) = controller
            .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
            .expect("failed R2 must produce dirty follow-up");
        assert_ne!(r3, r2);
        assert_eq!(
            controller.planned_next().as_ref(),
            Some(&c_key),
            "planned_next must keep describing physically staged C"
        );
        match followup.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, c_key, "follow-up must replace still-staged C");
                    assert_eq!(
                        replacement.expect("E must be desired").track.key,
                        e_key,
                        "follow-up must target E, never broken B"
                    );
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r3));

        // Follow-up succeeds: planned_next becomes E.
        assert!(controller.commit_realign(r3, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&e_key));
    }

    /// An unchanged reload during a decode follow-up must not mark the
    /// realign dirty merely because raw peek_next_song() is the broken B:
    /// dirty detection compares against the effective desired successor.
    #[tokio::test]
    async fn unchanged_reload_does_not_spuriously_dirty_a_decode_realign_chain() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let d = queued_song("D", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let d_key = StationController::track(d.clone()).key;

        // R1: staged B fails to decode -> replaces with C.
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");

        // Reload to [A, B, D] while R1 is in flight.
        assert!(controller
            .reload(vec![a.clone(), b.clone(), d.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R1 succeeds -> R2 (C -> D), desired is D, excluded_broken is B.
        let (r2, _) = controller
            .commit_realign(r1, &Ok(()))
            .expect("R1 success must produce follow-up R2");

        // An UNCHANGED reload with the same effective queue [A, B, D]:
        // effective desired is D == realign.desired (D) -> NOT dirty!
        assert!(controller
            .reload(vec![a.clone(), b.clone(), d.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R2 succeeds: must NOT manufacture an unwanted R3!
        assert!(
            controller.commit_realign(r2, &Ok(())).is_none(),
            "an unchanged reload must not manufacture spurious follow-up work"
        );
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
    }

    /// A reload of an unchanged queue during an automatic decode retry must
    /// not manufacture a dirty mark and bypass the bounded retry budget.
    #[tokio::test]
    async fn retry_budget_is_not_bypassed_by_an_unchanged_reload() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;

        // R1: staged B fails to decode -> replaces with C.
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");

        // R1 FAILS -> bounded retry R2 (B -> C), budget now 0.
        let (r2, _) = controller
            .commit_realign(r1, &Err(PipelineError::Pipeline("R1 failed".into())))
            .expect("failed R1 must produce retry R2");

        // An UNCHANGED reload [A, B, C] while R2 is in flight:
        // effective desired is C == realign.desired (C) -> NOT dirty!
        assert!(controller
            .reload(vec![a.clone(), b.clone(), c.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R2 FAILS with exhausted budget: no R3, no hot loop, planned_next
        // remains physical B.
        assert!(
            controller
                .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
                .is_none(),
            "exhausted retry budget with no queue change must produce no further roll"
        );
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));
    }

    /// If an explicit reload genuinely changes the effective desired successor
    /// (C -> D) while a retry with budget 0 is in flight, the failure of the
    /// retry still reconciles toward D as a single correlated queue operation
    /// (without re-arming an automatic decode retry budget).
    #[tokio::test]
    async fn changed_reload_reconciles_after_retry_budget_exhaustion() {
        let a = queued_song("A", 0);
        let b = queued_song("B", 1);
        let c = queued_song("C", 2);
        let d = queued_song("D", 3);
        let (mut controller, _) = Harness::playing(vec![a.clone(), b.clone(), c.clone()]).await.into_parts();
        let b_key = StationController::track(b.clone()).key;
        let d_key = StationController::track(d.clone()).key;

        // R1: staged B fails to decode -> replaces with C.
        let prepared = staged_decode_failure(&mut controller, b_key.clone(), "broken next")
            .await
            .expect("the first decode failure must prepare a roll");
        let r1 = prepared.realign_id.expect("the replacement must be correlated");

        // R1 FAILS -> bounded retry R2 (B -> C), budget now 0.
        let (r2, _) = controller
            .commit_realign(r1, &Err(PipelineError::Pipeline("R1 failed".into())))
            .expect("failed R1 must produce retry R2");

        // A GENUINE queue change [A, B, D] while R2 is in flight:
        // effective desired becomes D != realign.desired (C) -> dirty!
        assert!(controller
            .reload(vec![a.clone(), b.clone(), d.clone()], true)
            .await
            .unwrap()
            .is_none());

        // R2 FAILS: retry budget is exhausted, but dirty reconciliation
        // produces exactly ONE roll toward D (skipping broken B).
        let (r3, followup) = controller
            .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
            .expect("dirty queue change must reconcile toward D");
        assert_ne!(r3, r2);
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));
        match followup.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, b_key, "must replace still-staged B");
                    assert_eq!(
                        replacement.expect("D must be desired").track.key,
                        d_key,
                        "must select newest successor D"
                    );
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }
        assert_eq!(controller.pending_realign(), Some(r3));

        // R3 fails: because R3 was a queue-driven follow-up (not armed with decode
        // retry budget), it produces NO further automatic roll.
        assert!(
            controller
                .commit_realign(r3, &Err(PipelineError::Pipeline("R3 failed".into())))
                .is_none(),
            "exhausted queue follow-up must not loop"
        );
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));
    }

    /// Problem 1 regression: when R1 (B -> C) succeeds with no dirty
    /// follow-up, pending_realign becomes None. A subsequent unchanged
    /// Reload(A -> B -> C) across the idle gap must still know B is broken
    /// under current A, choosing C (no roll, planned_next stays C).
    #[tokio::test]
    async fn decode_exclusion_survives_idle_gap_after_roll_success() {
        let songs = queued_songs(&["A", "B", "C"]);
        let (mut controller, _b_key, c_key, r1) = prepare_broken_b_playing(&songs).await;

        // R1 succeeds: pending_realign is cleared; planned_next advances to C.
        assert!(controller.commit_realign(r1, &Ok(())).is_none());
        assert_eq!(controller.pending_realign(), None);
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));

        // Unchanged reload across the idle gap: effective successor is C,
        // which already matches the staged C -> no roll prepared!
        let result = controller.reload(songs.clone(), true).await.unwrap();
        assert!(result.is_none(), "unchanged reload after idle gap must not prepare a roll");
        assert_eq!(controller.pending_realign(), None);
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));
    }

    /// Problem 1 regression: after an idle gap following R1 success, a
    /// changed reload to [A, B, D] must still skip the broken B (which is
    /// raw queue head) and prepare ReplaceNext(C -> D), never C -> B.
    #[tokio::test]
    async fn changed_reload_after_idle_gap_still_skips_excluded_branch() {
        let songs = queued_songs(&["A", "B", "C"]);
        let (mut controller, _b_key, c_key, r1) = prepare_broken_b_playing(&songs).await;

        // R1 succeeds -> idle gap with staged C.
        assert!(controller.commit_realign(r1, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));

        // Reload to [A, B, D] across the idle gap:
        let d = queued_song("D", 3);
        let d_key = StationController::track(d.clone()).key;
        let prepared = controller
            .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
            .await
            .unwrap()
            .expect("changed reload must prepare a roll");
        let r2 = prepared.realign_id.expect("the roll must be correlated");

        // The roll must replace staged C with D, NEVER B!
        match prepared.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, c_key);
                    assert_eq!(replacement.expect("D must be desired").track.key, d_key);
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }

        // R2 succeeds -> planned_next becomes D.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
    }

    /// Problem 2 regression: multiple staged branches can fail decoding
    /// under the same current playback identity. When B fails (replaced with
    /// C) and then C also fails, both B and C are excluded, so the second
    /// replacement must choose D (never C -> B).
    #[tokio::test]
    async fn consecutive_decode_failures_skip_all_broken_branches() {
        let songs = queued_songs(&["A", "B", "C", "D"]);
        let (mut controller, _b_key, c_key, r1) = prepare_broken_b_playing(&songs).await;
        let d_key = StationController::track(songs[3].clone()).key;

        // R1 (B -> C) succeeds: physical staged is now C.
        assert!(controller.commit_realign(r1, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));

        // Now C also fails decoding!
        let prepared = staged_decode_failure(&mut controller, c_key.clone(), "C failed too")
            .await
            .expect("second decode failure must prepare a roll");
        let r2 = prepared.realign_id.expect("second replacement must be correlated");

        // The second roll must replace C with D (skipping BOTH B and C)!
        match prepared.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, c_key);
                    assert_eq!(
                        replacement.expect("D must be desired").track.key,
                        d_key,
                        "replacement must be D, skipping both B and C"
                    );
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }

        // R2 succeeds: planned_next becomes D.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
    }

    /// Problem 2 regression: after both B and C have failed and D is
    /// physically staged, a reload to [A, B, C, E] must skip both B and C
    /// and choose E (preparing ReplaceNext(D -> E), never D -> B or D -> C).
    #[tokio::test]
    async fn multiple_excluded_branches_survive_reload() {
        let songs = queued_songs(&["A", "B", "C", "D"]);
        let (mut controller, _b_key, c_key, r1) = prepare_broken_b_playing(&songs).await;
        let d_key = StationController::track(songs[3].clone()).key;

        // R1 succeeds -> staged C.
        assert!(controller.commit_realign(r1, &Ok(())).is_none());

        // C fails -> R2 (C -> D).
        let prepared = staged_decode_failure(&mut controller, c_key.clone(), "C broken")
            .await
            .expect("C failure must prepare roll");
        let r2 = prepared.realign_id.unwrap();
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));

        // Reload to [A, B, C, E]:
        let e = queued_song("E", 4);
        let e_key = StationController::track(e.clone()).key;
        let prepared = controller
            .reload(vec![songs[0].clone(), songs[1].clone(), songs[2].clone(), e.clone()], true)
            .await
            .unwrap()
            .expect("reload must prepare a roll toward E");
        let r3 = prepared.realign_id.unwrap();

        match prepared.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, d_key);
                    assert_eq!(
                        replacement.expect("E must be desired").track.key,
                        e_key,
                        "must choose E, skipping both B and C"
                    );
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }

        assert!(controller.commit_realign(r3, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&e_key));
    }

    /// When B is already excluded and R2 (C -> E) is in flight, a DecodeFailed(C)
    /// arrives while R2 is unresolved: R2 retains ownership (no second roll),
    /// both B and C are remembered in exclusions, and expected C is marked
    /// broken. On R2 failure, bounded recovery replaces C toward the next
    /// non-excluded successor (skipping both B and C).
    #[tokio::test]
    async fn second_broken_branch_while_realign_is_in_flight() {
        let songs = queued_songs(&["A", "B", "C", "D"]);
        let (mut controller, _b_key, c_key, r1) = prepare_broken_b_playing(&songs).await;

        // R1 succeeds -> staged C.
        assert!(controller.commit_realign(r1, &Ok(())).is_none());

        // Reload to [A, B, D] so R2 (C -> D) is in flight.
        let d = queued_song("D", 3);
        let d_key = StationController::track(d.clone()).key;
        let prepared = controller
            .reload(vec![songs[0].clone(), songs[1].clone(), d.clone()], true)
            .await
            .unwrap()
            .expect("reload must prepare C -> D");
        let r2 = prepared.realign_id.unwrap();

        // While R2 (C -> D) is in flight, C emits DecodeFailed:
        assert!(
            staged_decode_failure(&mut controller, c_key.clone(), "C failed during realign")
                .await
                .is_none(),
            "DecodeFailed for expected_next while realign in flight must be absorbed"
        );
        assert_eq!(controller.pending_realign(), Some(r2));

        // R2 FAILS: C is physically broken, so commit_realign triggers a bounded
        // retry from physical C toward the effective successor (D), skipping B and C.
        let (r3, followup) = controller
            .commit_realign(r2, &Err(PipelineError::Pipeline("R2 failed".into())))
            .expect("failed roll on broken expected must produce retry roll");
        assert_ne!(r3, r2);
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));

        match followup.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::ReplaceNext {
                    expected_next,
                    replacement,
                } => {
                    assert_eq!(expected_next, c_key, "retry must replace still-staged C");
                    assert_eq!(
                        replacement.expect("D must be desired").track.key,
                        d_key,
                        "retry must target D, skipping both B and C"
                    );
                }
                other => panic!("expected ReplaceNext, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }
        assert!(controller.commit_realign(r3, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
    }

    /// A transition to a new current identity (Handover) clears the decode
    /// exclusions of the old current: a track excluded under current A is
    /// eligible again as a successor under current C.
    #[tokio::test]
    async fn exclusions_clear_on_new_current_identity_after_handover() {
        let songs = queued_songs(&["A", "B", "C"]);
        let (mut controller, b_key, c_key, r1) = prepare_broken_b_playing(&songs).await;

        // R1 (B -> C) succeeds: physical staged is now C, B is excluded under current A.
        assert!(controller.commit_realign(r1, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&c_key));

        // The queue is reloaded so B is placed after C: [A, C, B].
        assert!(controller
            .reload(vec![songs[0].clone(), songs[2].clone(), songs[1].clone()], true)
            .await
            .unwrap()
            .is_none());

        // Handover to C occurs: C becomes current, identity changes to (generation 1, current C).
        // The handover Attach is prepared for the next song in queue (B).
        let prepared = prepare_handover_attach(&mut controller, c_key.clone()).await;
        let r2 = prepared.realign_id.expect("attach must be correlated");

        // The Attach for the new current C must select B (exclusions from A were cleared!).
        match prepared.operation {
            PipelineOperation::Roll(plan) => match plan.change {
                RollingChange::Attach(planned) => {
                    assert_eq!(
                        planned.track.key, b_key,
                        "under current C, B is no longer excluded and must be attached"
                    );
                }
                other => panic!("expected Attach, got {other:?}"),
            },
            other => panic!("expected Roll, got {other:?}"),
        }

        // Attach succeeds -> planned_next becomes B under current C.
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));
    }

    /// When a physical ReplaceNext roll succeeds and the pipeline hands over
    /// to the desired branch BEFORE its RealignResult completion is
    /// processed, the controller must accept the Handover, commit the desired
    /// song as current, supersede the in-flight realign, and ensure late
    /// completions for that realign are inert.
    #[tokio::test]
    async fn replacenext_desired_hands_over_before_completion() {
        let songs = queued_songs(&["A", "B", "C", "D"]);
        let (mut controller, _) = Harness::playing(vec![songs[0].clone(), songs[1].clone()]).await.into_parts();
        let b_key = StationController::track(songs[1].clone()).key;
        let c_key = StationController::track(songs[2].clone()).key;
        let d_key = StationController::track(songs[3].clone()).key;

        // Reload to [A, C, D] prepares ReplaceNext(B -> C):
        let prepared = controller
            .reload(vec![songs[0].clone(), songs[2].clone(), songs[3].clone()], true)
            .await
            .unwrap()
            .expect("reload must prepare B -> C");
        let r1 = prepared.realign_id.expect("realign id must be present");
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));
        assert_eq!(controller.pending_realign(), Some(r1));

        // Deliver Handover(C) BEFORE commit_realign(r1, ...):
        let handover_op = controller
            .handle_event(PipelineEvent::Handover {
                generation: 1,
                current: c_key.clone(),
            })
            .await
            .expect("Handover of pending desired branch must be accepted")
            .expect("the handover must not fail");

        // Handover is accepted and C is the logical current:
        assert_eq!(
            controller.queue.current_song_info().as_ref().map(StationController::key_of),
            Some(c_key)
        );
        // planned_next is None while the post-handover attach is unresolved:
        assert_eq!(controller.planned_next(), None);

        // Returned work is the post-handover Attach for D with a fresh realign id:
        let r2 = expect_attach(handover_op, &d_key);
        assert_ne!(r2, r1);

        // Late R1 completions (Ok and Err) must be completely inert and must not touch R2:
        assert!(controller.commit_realign(r1, &Ok(())).is_none(), "late R1 Ok must be inert");
        assert!(
            controller
                .commit_realign(r1, &Err(PipelineError::Pipeline("late err".into())))
                .is_none(),
            "late R1 Err must be inert"
        );
        assert_eq!(
            controller.pending_realign(),
            Some(r2),
            "late R1 must not destroy post-handover Attach record R2"
        );

        // Completing the post-handover Attach advances planned_next to D:
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
    }

    /// When an in-flight ReplaceNext becomes dirty via a Reload, but the
    /// physically desired branch hands over before completion, the old dirty
    /// alignment intent is discarded (it belonged to the old current identity)
    /// and the new current identity derives its successor fresh.
    #[tokio::test]
    async fn dirty_replacenext_desired_hands_over_before_completion() {
        let songs = queued_songs(&["A", "B", "C", "D", "E"]);
        let (mut controller, _) = Harness::playing(vec![songs[0].clone(), songs[1].clone()]).await.into_parts();
        let c_key = StationController::track(songs[2].clone()).key;
        let e_key = StationController::track(songs[4].clone()).key;

        // R1: B -> C prepared via reload [A, C, D]:
        let prepared = controller
            .reload(vec![songs[0].clone(), songs[2].clone(), songs[3].clone()], true)
            .await
            .unwrap()
            .expect("reload prepare");
        let r1 = prepared.realign_id.unwrap();

        // Reload while R1 is in flight: change queue to [A, E, D] -> R1 becomes dirty!
        assert!(controller
            .reload(vec![songs[0].clone(), songs[4].clone(), songs[3].clone()], true)
            .await
            .unwrap()
            .is_none());

        // Before committing R1, Handover(C) arrives:
        let handover_op = controller
            .handle_event(PipelineEvent::Handover {
                generation: 1,
                current: c_key.clone(),
            })
            .await
            .expect("Handover of C must be accepted")
            .expect("must not fail");

        // C becomes logical current:
        assert_eq!(
            controller.queue.current_song_info().as_ref().map(StationController::key_of),
            Some(c_key)
        );
        assert_eq!(controller.planned_next(), None);

        // The returned operation is an Attach of the new current C's successor (E):
        let r2 = expect_attach(handover_op, &e_key);
        assert_ne!(r2, r1);

        // Late R1 completion (Ok and Err) is inert:
        assert!(controller.commit_realign(r1, &Ok(())).is_none(), "late R1 Ok must be inert");
        assert!(
            controller
                .commit_realign(r1, &Err(PipelineError::Pipeline("late err".into())))
                .is_none(),
            "late R1 Err must be inert"
        );
        assert_eq!(controller.pending_realign(), Some(r2));

        // Completing R2 advances planned_next to E:
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&e_key));
    }

    /// When a post-Handover Attach physically succeeds and hands over before
    /// its RealignResult is processed, the Handover must be accepted, the
    /// previous attach realign superseded, and late completions made inert.
    #[tokio::test]
    async fn post_handover_attach_desired_hands_over_before_completion() {
        let songs = queued_songs(&["A", "B", "C", "D"]);
        let (mut controller, _) = Harness::playing(vec![songs[0].clone(), songs[1].clone(), songs[2].clone(), songs[3].clone()])
            .await
            .into_parts();
        let b_key = StationController::track(songs[1].clone()).key;
        let c_key = StationController::track(songs[2].clone()).key;
        let d_key = StationController::track(songs[3].clone()).key;

        // Handover(B) arrives -> current becomes B, planned_next becomes None, returns Attach(C) with realign id R1:
        let handover_op = controller
            .handle_event(PipelineEvent::Handover {
                generation: 1,
                current: b_key.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let r1 = expect_attach(handover_op, &c_key);
        assert_eq!(controller.planned_next(), None);
        assert_eq!(controller.pending_realign(), Some(r1));

        // Deliver Handover(C) BEFORE commit_realign(r1, Ok):
        let handover_c = controller
            .handle_event(PipelineEvent::Handover {
                generation: 1,
                current: c_key.clone(),
            })
            .await
            .expect("Handover of C (pending desired of Attach) must be accepted")
            .expect("must not fail");

        // C is accepted as current:
        assert_eq!(
            controller.queue.current_song_info().as_ref().map(StationController::key_of),
            Some(c_key)
        );
        // R1 is superseded:
        assert_ne!(controller.pending_realign(), Some(r1));
        // Attach for D is minted under current C:
        let r2 = expect_attach(handover_c, &d_key);
        assert_ne!(r2, r1);

        // Late R1 completion is inert and does not touch R2:
        assert!(controller.commit_realign(r1, &Ok(())).is_none());
        assert_eq!(controller.pending_realign(), Some(r2));
        assert!(controller.commit_realign(r2, &Ok(())).is_none());
        assert_eq!(controller.planned_next().as_ref(), Some(&d_key));
    }

    /// A Handover for an unrelated track or a stale generation must be
    /// rejected without mutating current, planned_next, or pending realign state.
    #[tokio::test]
    async fn invalid_or_stale_handover_is_rejected() {
        let songs = queued_songs(&["A", "B", "C"]);
        let (mut controller, _) = Harness::playing(vec![songs[0].clone(), songs[1].clone()]).await.into_parts();
        let a_key = StationController::track(songs[0].clone()).key;
        let b_key = StationController::track(songs[1].clone()).key;
        let z = queued_song("Z", 99);
        let z_key = StationController::track(z).key;

        // R1: B -> C
        let prepared = controller
            .reload(vec![songs[0].clone(), songs[2].clone()], true)
            .await
            .unwrap()
            .expect("reload prepare");
        let r1 = prepared.realign_id.unwrap();

        // Unrelated track Z:
        assert!(
            controller
                .handle_event(PipelineEvent::Handover {
                    generation: 1,
                    current: z_key.clone(),
                })
                .await
                .is_none(),
            "unrelated handover must be rejected"
        );

        // Stale generation:
        assert!(
            controller
                .handle_event(PipelineEvent::Handover {
                    generation: 99,
                    current: b_key.clone(),
                })
                .await
                .is_none(),
            "stale generation handover must be rejected"
        );

        // State is completely untouched:
        assert_eq!(
            controller.queue.current_song_info().as_ref().map(StationController::key_of),
            Some(a_key)
        );
        assert_eq!(controller.planned_next().as_ref(), Some(&b_key));
        assert_eq!(controller.pending_realign(), Some(r1));
    }

    /// When a realign is in flight (e.g. ReplaceNext B -> C), but the old
    /// expected branch B hands over (e.g. roll failed or old branch won the
    /// race), B must be accepted as current and supersede R1.
    #[tokio::test]
    async fn old_expected_branch_handover_is_accepted_while_realign_unresolved() {
        let songs = queued_songs(&["A", "B", "C", "D"]);
        let (mut controller, _) = Harness::playing(vec![songs[0].clone(), songs[1].clone()]).await.into_parts();
        let b_key = StationController::track(songs[1].clone()).key;

        // R1: B -> C
        let prepared = controller
            .reload(vec![songs[0].clone(), songs[2].clone(), songs[3].clone()], true)
            .await
            .unwrap()
            .expect("reload prepare");
        let r1 = prepared.realign_id.unwrap();

        // Old expected branch B hands over:
        let _ = controller
            .handle_event(PipelineEvent::Handover {
                generation: 1,
                current: b_key.clone(),
            })
            .await
            .expect("old expected branch B must be accepted")
            .expect("must not fail");

        // B becomes current, R1 is superseded:
        assert_eq!(
            controller.queue.current_song_info().as_ref().map(StationController::key_of),
            Some(b_key)
        );
        assert_ne!(controller.pending_realign(), Some(r1));
        assert!(controller.commit_realign(r1, &Ok(())).is_none());
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
