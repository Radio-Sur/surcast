use axum::extract::{Path, State};
use axum::Json;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, watch, Mutex as TokioMutex, Notify, OwnedMutexGuard};
use uuid::Uuid;

use crate::api::StreamersMap;
use crate::config::Config;
use crate::errors::AppError;
use crate::stations::models::Station;
use crate::stations::repository;
use crate::streamer::gstreamer::GStreamerPipelineFactory;
use crate::streamer::pipeline::{PipelineError, StationPlaybackConfig};
use crate::streamer::{SongInfo, StationStreamer};

/// Serializes control-plane lifecycle transitions (Play / Stop / Restart /
/// Delete) per station. Transitions are rare and scoped to one station, so
/// one async mutex per station is enough: station A's pipeline
/// initialization never blocks station B, and a transition never holds a
/// `std` mutex across an `.await`.
///
/// The registry holds a `Weak` handle per station and prunes entries whose
/// mutex no longer has any holder or waiter, so finished transitions for
/// unused UUIDs (including raw-UUID 404s) never grow the map permanently.
/// `Weak::upgrade` is atomic: two concurrent callers for the same station
/// always observe the same mutex, and an entry can never be dropped while a
/// guard or a waiter keeps its `Arc` alive.
///
/// Every transition must hold the guard for its whole duration and must NOT
/// re-enter it (no recursive locking): transitions compose via the `_locked`
/// helpers, which assume the guard is already held.
/// A station-scoped change notification for observers that have no runtime
/// to listen to (no-runtime WebSocket subscribers). Events identify the
/// station, so a change to station B never wakes subscribers of station A
/// into a DB read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StationEvent {
    /// A lifecycle transition of the station finished (desired state may
    /// have changed, or the station may have been deleted).
    Lifecycle { station_id: Uuid },
    /// The station's queue changed through a central queue sync.
    Queue { station_id: Uuid },
}

pub struct StationLifecycleLocks {
    locks: TokioMutex<HashMap<Uuid, Weak<TokioMutex<()>>>>,
    hooks: LifecycleTestHooks,
    /// Station-scoped change notifications (see [`StationEvent`]). A
    /// broadcast channel is used instead of a single shared watch value so
    /// that two rapid changes of different stations cannot collapse into
    /// one wakeup; receivers filter by `station_id` without touching the DB.
    notifications: broadcast::Sender<StationEvent>,
}

impl Default for StationLifecycleLocks {
    fn default() -> Self {
        let (notifications, _) = broadcast::channel(64);
        Self {
            locks: TokioMutex::new(HashMap::new()),
            hooks: LifecycleTestHooks::default(),
            notifications,
        }
    }
}

impl StationLifecycleLocks {
    /// Notifies no-runtime subscribers that the station's lifecycle state
    /// may have changed (persisted desired state or station deletion).
    pub(crate) fn notify_lifecycle_changed(&self, station_id: Uuid) {
        let _ = self.notifications.send(StationEvent::Lifecycle { station_id });
    }

    /// Notifies no-runtime subscribers that the station's queue changed
    /// through the central queue sync (no runtime exists to broadcast it).
    pub(crate) fn notify_queue_changed(&self, station_id: Uuid) {
        let _ = self.notifications.send(StationEvent::Queue { station_id });
    }

    /// Receives station change notifications. `Lagged` means events were
    /// dropped; the receiver must do a full re-check of its own station.
    pub(crate) fn subscribe_notifications(&self) -> broadcast::Receiver<StationEvent> {
        self.notifications.subscribe()
    }

    /// Acquires the per-station lifecycle lock. The returned owned guard
    /// lives as long as the transition; dropping it releases the station.
    /// The guard owns the `Arc` of the per-station `TokioMutex` itself, so
    /// it can be moved into a detached task (terminal shutdown cleanup)
    /// without keeping the whole lock registry alive.
    pub async fn lock(&self, station_id: Uuid) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.locks.lock().await;
            // Prune entries nobody holds or waits on anymore: a strong count
            // of zero means the last guard dropped and no waiter keeps the
            // `Arc` alive, so that mutex can never be acquired again.
            locks.retain(|_, weak| weak.strong_count() > 0);
            match locks.get(&station_id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(TokioMutex::new(()));
                    locks.insert(station_id, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        match lock.clone().try_lock_owned() {
            Ok(guard) => guard,
            Err(_) => {
                // Test support: the station mutex is genuinely held by
                // another transition — report the contention, then wait on
                // the real mutex (no hook blocks the transition here).
                self.hooks.lock_contended.bump();
                lock.lock_owned().await
            }
        }
    }

    /// Test support: lifecycle test hooks scoped to this instance (no-op
    /// unless armed). Each backend session owns its locks, so interleaving
    /// a concurrency test can never leak into another session's restore.
    pub fn test_hooks(&self) -> &LifecycleTestHooks {
        &self.hooks
    }
}

/// Test support: deterministic interleaving points for lifecycle
/// transitions, scoped to one [`StationLifecycleLocks`] instance (one
/// backend session). Arming a hook in a concurrency test can never affect
/// a different session's startup restore or a later test. Hooks are no-ops
/// unless armed.
#[derive(Default)]
pub struct LifecycleTestHooks {
    /// Reports that a transition found the per-station mutex held by
    /// another transition (it then waits on the real mutex, unhooked).
    pub lock_contended: ContendSignal,
    /// Parked inside a start transition, after persistence, before the
    /// runtime is created (the lock is held).
    pub before_runtime_create: LifecycleHook,
    /// Parked inside a stop transition, before persistence/teardown (the
    /// lock is held).
    pub before_stop: LifecycleHook,
}

/// Monotonic counter of observed contention on a station mutex (test
/// support). A watcher created BEFORE the concurrent command resolves when
/// the counter passes its recorded value: deterministic, and — unlike a
/// `Notify` permit — no stale signal can leak between scenarios.
pub struct ContendSignal {
    sender: watch::Sender<u64>,
    _keep_alive: watch::Receiver<u64>,
}

impl Default for ContendSignal {
    fn default() -> Self {
        let (sender, keep_alive) = watch::channel(0);
        Self {
            sender,
            _keep_alive: keep_alive,
        }
    }
}

impl ContendSignal {
    /// Records one observed contention event (test support).
    fn bump(&self) {
        let _ = self.sender.send_if_modified(|count| {
            *count += 1;
            true
        });
    }

    /// Test support: a watcher that resolves once contention is observed
    /// after this call. Create it BEFORE spawning the concurrent command.
    pub fn contend_watcher(&self) -> ContendWatcher {
        let mut receiver = self.sender.subscribe();
        let before = *receiver.borrow_and_update();
        ContendWatcher { receiver, before }
    }
}

/// Test support: the waiting half of [`ContendSignal`].
pub struct ContendWatcher {
    receiver: watch::Receiver<u64>,
    before: u64,
}

impl ContendWatcher {
    /// Waits (bounded) until a transition observed the station mutex busy.
    pub async fn wait(&mut self, what: &str) -> Result<(), Box<dyn std::error::Error>> {
        match tokio::time::timeout(Duration::from_secs(10), self.receiver.wait_for(|count| *count > self.before)).await {
            Err(_) => Err(Box::new(std::io::Error::other(format!("timed out waiting for {what}")))),
            Ok(Err(_)) => Err(Box::new(std::io::Error::other(
                // The sender is gone: a closed channel is NOT an observed
                // contention signal and must never pass the watcher.
                "contention signal closed before contention was observed",
            ))),
            Ok(Ok(_)) => Ok(()),
        }
    }
}

/// State of one lifecycle hook; transitions only park while `Armed`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HookState {
    /// The hook does not intercept transitions.
    Disarmed,
    /// A transition reaching the hook parks until `Released`/`Disarmed`.
    Armed,
    /// A parked transition may continue (single-shot release).
    Released,
}

/// One deterministic interleaving point (test support).
///
/// `wait` parks the transition on a `watch` channel instead of a bare
/// `Notify`: `disarm` publishes `Disarmed`, which wakes every parked
/// transition and lets it finish — failure-safe cleanup with no stale
/// permits (a later test that re-arms starts from a clean state).
pub struct LifecycleHook {
    state: watch::Sender<HookState>,
    /// Keeps the channel open: `watch::Sender::send` drops the value and
    /// returns `Err` when every receiver is gone, which would make the
    /// hooks permanently no-ops.
    _keep_alive: watch::Receiver<HookState>,
    entered: Notify,
}

impl Default for LifecycleHook {
    fn default() -> Self {
        let (state, keep_alive) = watch::channel(HookState::Disarmed);
        Self {
            state,
            _keep_alive: keep_alive,
            entered: Notify::new(),
        }
    }
}

impl LifecycleHook {
    /// Signals the test that the transition reached this hook, then waits
    /// until the hook leaves `Armed` (release or disarm). Only armed hooks
    /// park: a session spawn (startup restore) that passes the same code
    /// path while a test is not interleaving commands must never block.
    async fn wait(&self) {
        if *self.state.borrow() != HookState::Armed {
            return;
        }
        self.entered.notify_one();
        let mut state = self.state.subscribe();
        // The sender outlives every transition (the session owns it), so
        // `wait_for` can only fail if the session was dropped — which must
        // also unblock the transition.
        let _ = state.wait_for(|s| *s != HookState::Armed).await;
    }

    /// Arms the hook for the duration of one concurrency test.
    pub fn arm(&self) {
        let _ = self.state.send(HookState::Armed);
    }

    /// Releases a transition parked at this hook.
    pub fn release(&self) {
        let _ = self.state.send(HookState::Released);
    }

    /// Disarms the hook and wakes every transition parked at it, so
    /// failure-safe cleanup cannot leave a request hung.
    pub fn disarm(&self) {
        let _ = self.state.send(HookState::Disarmed);
    }

    /// Waits until a transition is parked at this hook.
    pub fn entered(&self) -> &Notify {
        &self.entered
    }
}

pub(crate) async fn resolve_station_id(db: &PgPool, id_or_slug: &str) -> Result<Uuid, AppError> {
    if let Ok(uuid) = Uuid::parse_str(id_or_slug) {
        return Ok(uuid);
    }
    repository::resolve_station_id_from_slug(db, id_or_slug).await
}

async fn get_or_create_streamer(
    db: &PgPool,
    streamers: &StreamersMap,
    upload_dir: &str,
    station_id: Uuid,
    station_name: &str,
    songs: Vec<SongInfo>,
    prebuffer_bytes: i32,
) -> Result<Arc<StationStreamer>, AppError> {
    if let Some(existing) = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    } {
        if !songs.is_empty() {
            // A station that was idle (or restarted) keeps the queue stored in
            // the database; reload it so playback resumes with the current rows.
            existing.reload_songs(songs, false).await.map_err(|error| {
                tracing::error!(station_id = %station_id, %error, "stream queue reload failed");
                AppError::Internal("Stream queue reload failed".into())
            })?;
        }
        return Ok(existing);
    }
    let streamer = StationStreamer::new(
        songs,
        station_name,
        station_id,
        db.clone(),
        prebuffer_bytes,
        upload_dir,
        Arc::new(GStreamerPipelineFactory::default()),
    )
    .await
    .map_err(|error| {
        tracing::error!(station_id = %station_id, error = %error, "GStreamer pipeline initialization failed");
        AppError::Internal("Stream initialization failed".into())
    })?;
    let winner = {
        let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(station_id).or_insert_with(|| streamer.clone()).clone()
    };
    Ok(winner)
}

/// Reloads a station's runtime songs after a queue mutation and broadcasts
/// the fresh queue. The central sync point for every external/control-plane
/// queue mutation (REST handlers, schedule fill): each such path ends here.
/// Runtime-internal mutations (the streamer's own `QueueRepository`
/// consume/trim) are NOT routed through this helper — they reload and
/// broadcast inside the runtime, where no-runtime observers do not exist
/// because the path only runs while the runtime is alive. When the station
/// has NO runtime (a stopped station's queue changed), no runtime is
/// created — no-runtime observers are notified instead and fetch a read-only
/// DB snapshot themselves.
///
/// The helper runs under the SAME per-station lifecycle lock as Play/Stop/
/// Restart: a queue mutation cannot "miss" an in-flight runtime creation.
/// Without the lock, the sync could see "no runtime" while Play holds the
/// lock and is about to insert a runtime built from a stale queue read —
/// the mutation would then end up in the DB only, and nobody would ever
/// reload the runtime. Under the lock the two orderings are both correct:
/// Play first → the sync reloads the fresh runtime it inserted; sync first
/// → Play's runtime creation reads the queue rows the mutation already
/// persisted.
pub(crate) async fn sync_streamer_songs(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &StationLifecycleLocks,
    upload_dir: &str,
    station_id: Uuid,
    align_next: bool,
) -> Result<(), AppError> {
    let _guard = lifecycle.lock(station_id).await;

    let has_runtime = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).is_some()
    };
    if !has_runtime {
        lifecycle.notify_queue_changed(station_id);
        return Ok(());
    }

    let rows = repository::find_station_song_info(db, station_id).await?;
    let songs = rows
        .into_iter()
        .map(|r| SongInfo {
            file_path: crate::songs::handlers::resolve_audio_path(upload_dir, &r.0),
            title: r.1,
            artist: r.2,
            duration: r.3,
            queue_item_id: r.4,
            song_id: r.5,
            position: r.6,
            cue_in: r.7,
            cue_out: r.8,
            cross_start_next: r.9,
            analyzed: r.10,
        })
        .collect();

    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        streamer
            .reload_songs(songs, align_next)
            .await
            .map_err(|_| AppError::Internal("Stream reload failed".into()))?;
    }
    if let Some(streamer) = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    } {
        streamer.trim_played_items().await.map_err(|error| {
            tracing::error!(station_id = %station_id, %error, "stream queue trim failed");
            AppError::Internal("Stream queue sync failed".into())
        })?;
        streamer.push_queue_update().await.map_err(|error| {
            tracing::error!(station_id = %station_id, %error, "stream queue push failed");
            AppError::Internal("Stream queue sync failed".into())
        })?;
    }
    Ok(())
}

/// Fan-out: runs [`sync_streamer_songs`] for every station affected by one
/// structural queue mutation (e.g. a global song delete). The caller passes
/// a deduplicated list, so each station is synced exactly once — a runtime
/// is reloaded once, a runtime-less station gets one station-scoped queue
/// notification.
///
/// Attempts ALL stations even when one fails: the DB mutation is already
/// done, so one broken runtime must never deprive the remaining affected
/// stations of their reload / queue notification. The first error is
/// remembered and returned after the loop.
pub(crate) async fn sync_station_queues(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &StationLifecycleLocks,
    upload_dir: &str,
    station_ids: impl IntoIterator<Item = Uuid>,
    align_next: bool,
) -> Result<(), AppError> {
    let mut first_error: Option<AppError> = None;
    for station_id in station_ids {
        if let Err(error) = sync_streamer_songs(db, streamers, lifecycle, upload_dir, station_id, align_next).await {
            tracing::error!(station_id = %station_id, %error, "station queue sync failed; continuing with remaining stations");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(crate) async fn sync_streamer_playback_config(streamers: &StreamersMap, station: &Station) -> Result<(), AppError> {
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|error| error.into_inner());
        map.get(&station.id).cloned()
    };
    let Some(streamer) = streamer else {
        return Ok(());
    };

    let config = StationPlaybackConfig::from_persisted(
        &station.transition_mode,
        station.default_fade_ms,
        station.autocue_fade_max_ms,
        station.prebuffer_bytes,
    )
    .map_err(|error| {
        tracing::error!(station_id = %station.id, %error, "invalid persisted playback configuration");
        AppError::Internal("Stream configuration failed".into())
    })?;
    streamer.update_config(config).await.map_err(|error| {
        tracing::error!(station_id = %station.id, %error, "stream configuration update failed");
        AppError::Internal("Stream configuration failed".into())
    })
}

pub(crate) async fn get_or_create_streamer_for_station(
    db: &PgPool,
    streamers: &StreamersMap,
    upload_dir: &str,
    station_id: Uuid,
) -> Result<Arc<StationStreamer>, AppError> {
    let station = repository::find_station_by_id(db, station_id)
        .await?
        .ok_or_else(|| AppError::NotFound("Station not found".into()))?;

    let rows = repository::find_station_song_info(db, station_id).await?;
    let songs: Vec<SongInfo> = rows
        .into_iter()
        .map(|r| SongInfo {
            file_path: crate::songs::handlers::resolve_audio_path(upload_dir, &r.0),
            title: r.1,
            artist: r.2,
            duration: r.3,
            queue_item_id: r.4,
            song_id: r.5,
            position: r.6,
            cue_in: r.7,
            cue_out: r.8,
            cross_start_next: r.9,
            analyzed: r.10,
        })
        .collect();

    if let Some(existing) = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    } {
        if !songs.is_empty() {
            // Starting a station must resume from the queue stored in the
            // database even when an (idle) streamer already exists.
            existing.reload_songs(songs, false).await.map_err(|error| {
                tracing::error!(station_id = %station_id, %error, "stream queue reload failed");
                AppError::Internal("Stream queue reload failed".into())
            })?;
        }
        return Ok(existing);
    }

    if songs.is_empty() {
        tracing::info!("Station queue is empty, creating idle streamer for {station_id}");
    }
    let mount = station.mount();

    get_or_create_streamer(db, streamers, upload_dir, station_id, &mount, songs, station.prebuffer_bytes).await
}

/// Control-plane Play shared by REST and WebSocket: persists the user's
/// desired state (`started`) and starts the runtime, serialized with every
/// other lifecycle transition of the same station. `is_started` keeps the
/// user's intent even when the pipeline fails to start (e.g. Icecast is
/// down) — a transient failure must not turn the decision into `stopped`.
///
/// Cancellation boundary: the ENTIRE mutating phase runs in the detached
/// [`run_committed_start`] task. The caller owns only the guard
/// acquisition and the JoinHandle await; from the first
/// `set_station_started(true)` await on, ownership (guard, DB pool,
/// streamers, lifecycle, upload_dir) belongs to the task, so caller
/// cancellation can never suppress the lifecycle notification or the
/// runtime-start attempt after the persistence committed. Dropping the
/// JoinHandle does not cancel the task.
pub(crate) async fn start_station(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &Arc<StationLifecycleLocks>,
    upload_dir: &str,
    station_id: Uuid,
) -> Result<Arc<StationStreamer>, AppError> {
    let guard = lifecycle.lock(station_id).await;
    let db = db.clone();
    let streamers = Arc::clone(streamers);
    let lifecycle = Arc::clone(lifecycle);
    let upload_dir = upload_dir.to_owned();
    let operation = tokio::spawn(run_committed_start(db, streamers, lifecycle, upload_dir, station_id, guard));
    operation.await.map_err(|error| {
        tracing::error!(station_id = %station_id, %error, "committed play task failed");
        AppError::Internal("Stream start task failed".into())
    })?
}

/// The full user-facing start, run as ONE cancellation-independent
/// operation: persists `is_started = true`, notifies the lifecycle
/// observers and starts the runtime. The caller moves the guard, the DB
/// pool, the streamer map, the lifecycle Arc and `upload_dir` into this
/// task BEFORE the first mutating DB await, so caller cancellation can
/// never interrupt the mutation or suppress the notification that follows
/// a committed persistence. A persistence error aborts BEFORE the runtime
/// start (nothing was started, nothing to roll back); a runtime-start
/// failure keeps the desired `started` state (the user's intent is not
/// rolled back) and still returns the technical error. The guard is
/// released when the task finishes either way. Errors are logged with the
/// station id because the caller may no longer exist to observe them.
async fn run_committed_start(
    db: PgPool,
    streamers: StreamersMap,
    lifecycle: Arc<StationLifecycleLocks>,
    upload_dir: String,
    station_id: Uuid,
    guard: OwnedMutexGuard<()>,
) -> Result<Arc<StationStreamer>, AppError> {
    let result = (async {
        repository::set_station_started(&db, station_id, true).await?;
        lifecycle.notify_lifecycle_changed(station_id);
        start_station_runtime(&db, &streamers, &lifecycle, &upload_dir, station_id).await
    })
    .await;
    if let Err(error) = &result {
        tracing::error!(station_id = %station_id, %error, "committed start failed");
    }
    let _guard = guard;
    result
}

/// Starts the runtime (get/create + `play`) without touching persistence.
/// Assumes the per-station lifecycle lock is already held; used by the
/// user-facing transitions and by the startup restore, which must NOT
/// re-persist `is_started` or bump `updated_at` — restore re-creates the
/// persisted intent, it does not write it again.
async fn start_station_runtime(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &StationLifecycleLocks,
    upload_dir: &str,
    station_id: Uuid,
) -> Result<Arc<StationStreamer>, AppError> {
    lifecycle.hooks.before_runtime_create.wait().await;
    let streamer = get_or_create_streamer_for_station(db, streamers, upload_dir, station_id).await?;
    streamer.play().await.map_err(|error| {
        tracing::error!(station_id = %station_id, %error, "stream playback failed");
        AppError::Internal("Stream playback failed".into())
    })?;
    Ok(streamer)
}

/// Stops the station's runtime and removes it from the map. `Shutdown` is
/// terminal: the runtime executor exits whether the pipeline stop succeeded
/// or not (the command loop breaks after queueing the barrier, the pipeline
/// executor returns after running it), so the streamer is never reusable
/// after the attempt. The dead runtime must therefore leave the map on BOTH
/// outcomes — a failed stop still propagates its error to the caller, it
/// just must not leave a dead entry that would be reused by get/create and
/// would poison every later Play/Restart. The removal is conditional (only
/// if the map still points at the same `Arc`), so a concurrent replacement
/// cannot be undone. The map guard is never held across the await.
///
/// Cancellation boundary — exactly two phases:
/// - Phase A (caller future): [`StationStreamer::begin_shutdown`] sends
///   the terminal command. Until the bounded command channel admits it, the
///   operation is cancellable together with the caller: no cleanup task
///   exists, the lifecycle guard drops with the caller future, the runtime
///   stays mapped and remains usable. An explicit send failure means the
///   runtime's command receiver is already gone (the loop exited) — that
///   streamer is dead and is removed here so it cannot poison a later
///   Play/Restart, and the stop error is reported.
/// - Phase B (detached task): immediately after the successful send — with
///   NO await in between — the completion receiver, the streamer Arc and
///   the lifecycle guard move into a cancellation-independent task. The
///   task awaits the pipeline stop result, removes exactly that Arc from
///   the map on Ok and Err, and holds the guard until the very end, so a
///   new Play cannot slip past a runtime that is still technically
///   stopping. If the caller died, the task still finishes the cleanup.
///
/// The guard is returned on success so the caller (Restart/Delete) keeps
/// one serialization scope across the whole transition; `Err` means the
/// stop failed (the terminal runtime was still removed from the map).
/// Assumes the per-station lifecycle lock is held by the caller and never
/// touches persistence — the desired state is owned by the caller (Stop
/// persists `false`, Delete removes the row, Restart keeps `started`). Removes `streamer` from the map if — and only if — the map still points
/// at exactly that `Arc`: a concurrent replacement can never be undone.
/// The map guard is never held across an await (there is none).
fn remove_streamer_if_same(streamers: &StreamersMap, station_id: Uuid, streamer: &Arc<StationStreamer>) {
    let mut map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(current) = map.get(&station_id) {
        if Arc::ptr_eq(current, streamer) {
            map.remove(&station_id);
        }
    }
}

/// Finishes an ALREADY ADMITTED shutdown: awaits the pipeline stop result,
/// then removes exactly `streamer` from the map on every outcome (a dead
/// runtime must never stay mapped). Must run in a cancellation-independent
/// context — from here the terminal cleanup may not depend on the request
/// caller's lifetime. `RecvError` is unreachable in practice (the executor
/// always answers the barrier), but the runtime is terminal either way, so
/// it is treated as a failed stop — the removal still runs.
async fn finish_admitted_shutdown(
    streamers: &StreamersMap,
    station_id: Uuid,
    streamer: &Arc<StationStreamer>,
    receiver: oneshot::Receiver<Result<(), PipelineError>>,
) -> Result<(), AppError> {
    let stop_result = match receiver.await {
        Ok(Err(error)) => {
            // The low-level helper owns the source cause: the pipeline
            // layer never logs a failed stop (the executor only forwards
            // the result), so this is the only place the GStreamer reason
            // survives. The caller-facing error stays generic.
            tracing::error!(station_id = %station_id, %error, "stream pipeline stop failed");
            Err(AppError::Internal("Stream stop failed".into()))
        }
        Ok(Ok(())) => Ok(()),
        Err(_) => {
            tracing::error!(station_id = %station_id, "stream stop completion channel closed");
            Err(AppError::Internal("Stream stop failed".into()))
        }
    };
    remove_streamer_if_same(streamers, station_id, streamer);
    stop_result
}

/// Runs a terminal shutdown to completion in a context that does not
/// depend on the request caller: sends the `Shutdown` command (waiting as
/// long as needed for the bounded channel to admit it), then completes the
/// pipeline stop and the map cleanup. An explicit send failure means the
/// command receiver is gone — the runtime is dead and is removed, never
/// left mapped; the technical error is returned either way.
async fn run_terminal_shutdown(streamers: &StreamersMap, station_id: Uuid, streamer: Arc<StationStreamer>) -> Result<(), AppError> {
    match streamer.begin_shutdown().await {
        Ok(receiver) => finish_admitted_shutdown(streamers, station_id, &streamer, receiver).await,
        Err(error) => {
            // Explicit send failure, NOT caller cancellation: the command
            // receiver is gone, so the runtime is already dead. The source
            // cause is logged here; the caller-facing error stays generic.
            tracing::error!(station_id = %station_id, %error, "stream stop command send failed");
            remove_streamer_if_same(streamers, station_id, &streamer);
            Err(AppError::Internal("Stream stop failed".into()))
        }
    }
}

/// The full user-facing Stop, run as ONE cancellation-independent
/// operation. `stop_station` moves the lifecycle guard, the DB pool and
/// the streamer map into this task BEFORE the first mutating DB await, so
/// no caller-owned future ever sits between "the UPDATE started" and
/// "ownership of the Stop is detached". If the caller dies at any point —
/// even before persistence starts — this task still persists
/// `is_started = false`, shuts the runtime down and removes it from the
/// map. A persistence error aborts the stop BEFORE the technical
/// shutdown and leaves the runtime mapped (nothing to roll back: a write
/// whose result was never observed must not be "undone"); the guard is
/// released when the task finishes either way. Errors are logged with the
/// station id because the caller may no longer exist to observe them.
async fn run_committed_stop(
    db: PgPool,
    streamers: StreamersMap,
    lifecycle: Arc<StationLifecycleLocks>,
    station_id: Uuid,
    guard: OwnedMutexGuard<()>,
) -> Result<(), AppError> {
    let result: Result<(), AppError> = (async {
        // Test hook: inside the Stop transition, before persistence; the
        // station lock is held by THIS task.
        lifecycle.hooks.before_stop.wait().await;
        repository::set_station_started(&db, station_id, false).await?;
        lifecycle.notify_lifecycle_changed(station_id);
        let streamer = {
            let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
            map.get(&station_id).cloned()
        };
        let Some(streamer) = streamer else {
            return Ok(());
        };
        run_terminal_shutdown(&streamers, station_id, streamer).await
    })
    .await;
    if let Err(error) = &result {
        tracing::error!(station_id = %station_id, %error, "committed stop failed");
    }
    let _guard = guard;
    result
}

/// Outcome of [`begin_runtime_shutdown_locked`]: either the station has
/// no runtime at all (nothing was stopped, the guard comes back unused)
/// or the terminal `Shutdown` command was admitted — the runtime is
/// terminal from that moment, so the caller MUST transfer the whole
/// remaining transition to a cancellation-independent task immediately.
enum RuntimeShutdown {
    NoRuntime {
        guard: OwnedMutexGuard<()>,
    },
    Admitted {
        guard: OwnedMutexGuard<()>,
        streamer: Arc<StationStreamer>,
        receiver: oneshot::Receiver<Result<(), PipelineError>>,
    },
}

/// Phase A of a terminal shutdown, still in the caller future: locates
/// the station's runtime and sends the `Shutdown` command. Until the
/// bounded command channel admits it, the operation is cancellable
/// together with the caller — no cleanup task exists, the lifecycle guard
/// drops with the caller future, and the runtime stays mapped and
/// remains usable. An explicit send failure (NOT caller cancellation)
/// means the runtime's command receiver is already gone — that streamer
/// is dead and is removed here so it cannot poison a later Play/Restart,
/// and the source error is logged with the station id. (Cancellation
/// never reaches that arm: the future is dropped mid-send and this
/// function simply does not return.)
///
/// On success the returned [`RuntimeShutdown::Admitted`] carries
/// everything the caller needs to move — with NO await in between — into
/// the committed continuation (stop cleanup, then the rest of the
/// transition). Assumes the per-station lifecycle lock is held by the
/// caller; never touches persistence — the desired state is owned by the
/// caller (Stop persists `false`, Delete removes the row, Restart keeps
/// `started`).
async fn begin_runtime_shutdown_locked(
    streamers: &StreamersMap,
    station_id: Uuid,
    guard: OwnedMutexGuard<()>,
) -> Result<RuntimeShutdown, AppError> {
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    let Some(streamer) = streamer else {
        return Ok(RuntimeShutdown::NoRuntime { guard });
    };
    let receiver = match streamer.begin_shutdown().await {
        Ok(receiver) => receiver,
        Err(error) => {
            tracing::error!(station_id = %station_id, %error, "stream stop command send failed");
            remove_streamer_if_same(streamers, station_id, &streamer);
            return Err(AppError::Internal("Stream stop failed".into()));
        }
    };
    Ok(RuntimeShutdown::Admitted { guard, streamer, receiver })
}

/// Control-plane Stop: persists `is_started = false` and shuts the runtime
/// down if one exists, serialized with every other lifecycle transition of
/// the same station. Idempotent: works without a live runtime (e.g. a
/// station persisted as started that has no map entry yet), so persistence
/// never depends on the runtime's existence. A Stop that runs after a
/// concurrent Play always ends the station, because both commands serialize
/// on the same per-station lock.
///
/// Cancellation boundary: the ENTIRE mutating phase runs in the detached
/// [`run_committed_stop`] task. The caller owns only the guard acquisition
/// and the JoinHandle await; from the first `set_station_started(false)`
/// await on, ownership (guard, DB pool, streamers, lifecycle) belongs to
/// the task, so caller cancellation can never interrupt persistence or the
/// technical shutdown mid-flight. Dropping the JoinHandle does not cancel
/// the task.
pub(crate) async fn stop_station(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &Arc<StationLifecycleLocks>,
    station_id: Uuid,
) -> Result<(), AppError> {
    let guard = lifecycle.lock(station_id).await;
    let db = db.clone();
    let streamers = Arc::clone(streamers);
    let lifecycle = Arc::clone(lifecycle);
    let operation = tokio::spawn(run_committed_stop(db, streamers, lifecycle, station_id, guard));
    operation.await.map_err(|error| {
        tracing::error!(station_id = %station_id, %error, "committed stop task failed");
        AppError::Internal("Stream stop task failed".into())
    })?
}

/// Control-plane Restart: stops the old runtime and starts a fresh one
/// under ONE per-station lifecycle lock, keeping the user's intent
/// `started` (restart of a stopped station implies the user wants it
/// running). The lock is acquired exactly once; the terminal shutdown is
/// the cancellation boundary:
///
/// - With a live runtime, BEFORE the old `Shutdown` command is admitted
///   the restart is cancellable together with the caller (no committed
///   intent exists yet): a dropped caller leaves the old runtime
///   untouched, the guard drops and no stale Shutdown can stop the
///   station later.
/// - The moment the command is admitted the old runtime is terminal, so
///   the ENTIRE remaining restart — stop cleanup, persist
///   `is_started = true`, lifecycle notification, fresh runtime start —
///   moves into a cancellation-independent task with NO await in between.
///   Caller cancellation cannot leave "old runtime gone, fresh runtime
///   never attempted".
/// - A restart without a runtime has no admission boundary: it is
///   semantically a committed start and uses the same mechanism as Play.
pub(crate) async fn restart_station(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &Arc<StationLifecycleLocks>,
    upload_dir: &str,
    station_id: Uuid,
) -> Result<Arc<StationStreamer>, AppError> {
    let guard = lifecycle.lock(station_id).await;
    match begin_runtime_shutdown_locked(streamers, station_id, guard).await? {
        RuntimeShutdown::NoRuntime { guard } => {
            // No admission boundary: committed start, spawned BEFORE the
            // first mutating DB await.
            let db = db.clone();
            let streamers = Arc::clone(streamers);
            let lifecycle = Arc::clone(lifecycle);
            let upload_dir = upload_dir.to_owned();
            let operation = tokio::spawn(run_committed_start(db, streamers, lifecycle, upload_dir, station_id, guard));
            operation.await.map_err(|error| {
                tracing::error!(station_id = %station_id, %error, "committed start task failed");
                AppError::Internal("Stream start task failed".into())
            })?
        }
        RuntimeShutdown::Admitted { guard, streamer, receiver } => {
            // Terminal boundary crossed: no await between the successful
            // send above and the transfer into the committed continuation.
            let db = db.clone();
            let streamers = Arc::clone(streamers);
            let lifecycle = Arc::clone(lifecycle);
            let upload_dir = upload_dir.to_owned();
            let operation = tokio::spawn(async move {
                let result = finish_admitted_shutdown(&streamers, station_id, &streamer, receiver).await;
                if let Err(error) = result {
                    tracing::error!(station_id = %station_id, %error, "committed restart: old stream stop failed");
                    let _guard = guard;
                    return Err(error);
                }
                run_committed_start(db, streamers, lifecycle, upload_dir, station_id, guard).await
            });
            operation.await.map_err(|error| {
                tracing::error!(station_id = %station_id, %error, "committed restart task failed");
                AppError::Internal("Stream restart task failed".into())
            })?
        }
    }
}

/// Control-plane Delete: a lifecycle transition like every other command.
/// Under the same per-station lock used by Play / Stop / Restart it stops
/// and removes the runtime first, then deletes the station row — a
/// concurrent Play/Restart cannot create a runtime between the two steps,
/// so no orphan runtime can outlive the station's deletion. Persisting
/// `is_started = false` right before removing the row would be pointless.
///
/// Cancellation boundary: with a live runtime the terminal `Shutdown`
/// command admission is the split — before it the delete is cancellable
/// together with the caller (the station is untouched); after it the
/// ENTIRE remaining delete (stop cleanup, row delete, lifecycle
/// notification) runs in a cancellation-independent task, so caller
/// cancellation cannot leave "runtime gone, station row still there".
/// Without a runtime the row delete itself is the first irreversible side
/// effect, so the committed operation is spawned BEFORE the mutating DB
/// await.
pub(crate) async fn delete_station_lifecycle(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &Arc<StationLifecycleLocks>,
    station_id: Uuid,
) -> Result<(), AppError> {
    let guard = lifecycle.lock(station_id).await;
    match begin_runtime_shutdown_locked(streamers, station_id, guard).await? {
        RuntimeShutdown::NoRuntime { guard } => {
            // No admission boundary: the row delete is the first
            // irreversible side effect, so the operation is committed
            // BEFORE the mutating DB await.
            let db = db.clone();
            let lifecycle = Arc::clone(lifecycle);
            let operation = tokio::spawn(run_committed_delete(db, lifecycle, station_id, guard));
            operation.await.map_err(|error| {
                tracing::error!(station_id = %station_id, %error, "committed delete task failed");
                AppError::Internal("Stream delete task failed".into())
            })?
        }
        RuntimeShutdown::Admitted { guard, streamer, receiver } => {
            // Terminal boundary crossed: no await between the successful
            // send above and the transfer into the committed continuation.
            let db = db.clone();
            let streamers = Arc::clone(streamers);
            let lifecycle = Arc::clone(lifecycle);
            let operation = tokio::spawn(async move {
                let result = finish_admitted_shutdown(&streamers, station_id, &streamer, receiver).await;
                if let Err(error) = result {
                    tracing::error!(station_id = %station_id, %error, "committed delete: old stream stop failed");
                    let _guard = guard;
                    return Err(error);
                }
                run_committed_delete(db, lifecycle, station_id, guard).await
            });
            operation.await.map_err(|error| {
                tracing::error!(station_id = %station_id, %error, "committed delete task failed");
                AppError::Internal("Stream delete task failed".into())
            })?
        }
    }
}

/// Committed station-row deletion: `delete_station` + the lifecycle
/// notification, run as ONE cancellation-independent operation. The
/// caller moves the guard, the DB pool and the lifecycle Arc into this
/// task BEFORE the mutating DB await, so caller cancellation cannot
/// suppress the notification that tells no-runtime forwarders the station
/// is gone. The guard is released when the task finishes either way;
/// errors are logged with the station id because the caller may no longer
/// exist to observe them.
async fn run_committed_delete(
    db: PgPool,
    lifecycle: Arc<StationLifecycleLocks>,
    station_id: Uuid,
    guard: OwnedMutexGuard<()>,
) -> Result<(), AppError> {
    let result = (async {
        repository::delete_station(&db, station_id).await?;
        lifecycle.notify_lifecycle_changed(station_id);
        Ok(())
    })
    .await;
    if let Err(error) = &result {
        tracing::error!(station_id = %station_id, %error, "committed delete failed");
    }
    let _guard = guard;
    result
}

/// Startup restore: every station persisted as `started` is started again,
/// without re-persisting the intent (no `UPDATE`, no `updated_at` bump).
/// One failing station must never block the rest of the boot.
pub async fn restore_started_stations(db: &PgPool, streamers: &StreamersMap, lifecycle: &StationLifecycleLocks, upload_dir: &str) {
    let stations = match repository::find_started_stations(db).await {
        Ok(stations) => stations,
        Err(error) => {
            tracing::error!(%error, "startup restore: failed to load started stations");
            return;
        }
    };
    for station in stations {
        restore_station_if_still_started(db, streamers, lifecycle, upload_dir, station.id).await;
    }
}

/// Restores one station: acquires the per-station lock, re-checks the
/// CURRENT desired state from the database and only starts the runtime if
/// the station still exists and is still started. The snapshot taken by
/// [`restore_started_stations`] before the lock may be stale — a concurrent
/// Stop/Delete could have run while this restore was waiting for the lock —
/// so the re-check under the lock is what prevents `is_started = false`
/// with a live runtime. Nothing is re-persisted.
async fn restore_station_if_still_started(
    db: &PgPool,
    streamers: &StreamersMap,
    lifecycle: &StationLifecycleLocks,
    upload_dir: &str,
    station_id: Uuid,
) {
    let _guard = lifecycle.lock(station_id).await;
    let still_started = match repository::find_station_started(db, station_id).await {
        Ok(Some(true)) => true,
        Ok(_) => false,
        Err(error) => {
            tracing::error!(station_id = %station_id, %error, "startup restore: failed to re-check station state");
            false
        }
    };
    if !still_started {
        return;
    }
    if let Err(error) = start_station_runtime(db, streamers, lifecycle, upload_dir, station_id).await {
        tracing::error!(station_id = %station_id, %error, "startup restore: failed to start station");
    }
}

pub async fn stream_skip(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        streamer.skip().await.map_err(|_| AppError::Internal("Stream skip failed".into()))?;
        Ok(Json(serde_json::json!({ "ok": true, "song_index": streamer.current_song_index() })))
    } else {
        Err(AppError::BadRequest("No active stream".into()))
    }
}

pub async fn stream_play(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    State(config): State<Config>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    start_station(&db, &streamers, &lifecycle, &config.upload_dir, station_id).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn stream_pause(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        streamer
            .pause()
            .await
            .map_err(|_| AppError::Internal("Stream pause failed".into()))?;
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err(AppError::BadRequest("No active stream".into()))
    }
}

pub async fn stream_stop(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    stop_station(&db, &streamers, &lifecycle, station_id).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn stream_restart(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    State(config): State<Config>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    // One lock for the whole transition: stop the old runtime, then start
    // the fresh one explicitly — get/create alone never plays anymore.
    restart_station(&db, &streamers, &lifecycle, &config.upload_dir, station_id).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn stream_status(
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    Path(station_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let station_id = resolve_station_id(&db, &station_id).await?;
    let streamer = {
        let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&station_id).cloned()
    };
    if let Some(streamer) = streamer {
        let status = streamer.status_json().await.map_err(|error| {
            tracing::error!(station_id = %station_id, %error, "stream status failed");
            AppError::Internal("Stream status failed".into())
        })?;
        Ok(Json(status))
    } else {
        Ok(Json(serde_json::json!({
            "playing": false, "song_index": 0, "total": 0, "elapsed": 0, "title": "", "artist": "", "duration": 0,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamer::pipeline::{PipelineConfig, PipelineError, PipelineEvent, PipelineInstance, PlaybackPipelineFactory};
    use crate::streamer::testsupport::{Call, Gate, RecordingPipeline};
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    /// Runs a lifecycle scenario against an isolated, migrated test
    /// database; the shared runner guarantees the database is dropped on
    /// success AND on scenario panic (see `crate::test_db::run_with_test_db`).
    /// The database is skipped entirely when `DATABASE_URL` is absent.
    use crate::test_db::run_with_test_db;

    /// The registry must not keep every UUID it ever saw: after a sequence
    /// of finished transitions on unique stations, only the last entry may
    /// survive (pruned by the next `lock()`).
    #[tokio::test]
    async fn lifecycle_registry_prunes_unused_entries() {
        let lifecycle = Arc::new(StationLifecycleLocks::default());
        for _ in 0..100 {
            let guard = lifecycle.lock(Uuid::new_v4()).await;
            drop(guard);
        }
        let entries = lifecycle.locks.lock().await.len();
        assert_eq!(entries, 1, "finished transitions must not grow the registry");
        // The surviving entry is the last one; a further lock+drop on a
        // fresh UUID replaces it instead of appending.
        let guard = lifecycle.lock(Uuid::new_v4()).await;
        drop(guard);
        assert_eq!(lifecycle.locks.lock().await.len(), 1);
    }

    /// The second caller for the same station must hit the SAME station
    /// mutex: it reports contention (the mutex is held), stays blocked, and
    /// keeps the registry entry alive; after the holder and waiter finish,
    /// the dead entry is pruned by the next lock.
    #[tokio::test]
    async fn concurrent_callers_share_one_mutex_per_station() {
        let lifecycle = Arc::new(StationLifecycleLocks::default());
        let station_id = Uuid::new_v4();
        let first = lifecycle.lock(station_id).await;

        let mut contended = lifecycle.test_hooks().lock_contended.contend_watcher();
        let second = tokio::spawn({
            let lifecycle = Arc::clone(&lifecycle);
            async move { lifecycle.lock(station_id).await }
        });
        // The second caller reached the station mutex and found it held.
        contended
            .wait("second caller to contend on the station mutex")
            .await
            .expect("contention must be observed");
        // A waiter keeps the registry entry alive while blocked.
        assert_eq!(lifecycle.locks.lock().await.len(), 1, "a waiting caller must keep the entry alive");

        // Once the holder releases, the waiter acquires the same lock.
        drop(first);
        let second = second.await.expect("second lock task");
        drop(second);

        // Both guards dropped, nobody waits: the next lock prunes the entry.
        let _ = lifecycle.lock(Uuid::new_v4()).await;
        assert_eq!(lifecycle.locks.lock().await.len(), 1);
    }

    /// Test-only factory: hands the shared [`RecordingPipeline`] (with its
    /// failure injection) to `StationStreamer::new`, so a real streamer can
    /// be built over the fake pipeline without GStreamer or Icecast.
    ///
    /// The runtime's command loop exits when its event channel closes; the
    /// production GStreamer pipeline owns that sender, so the test factory
    /// must keep the senders alive too — otherwise the loop (and with it
    /// the pipeline executor) dies before the test's assertions run.
    struct RecordingPipelineFactory {
        pipeline: Arc<RecordingPipeline>,
        event_senders: Arc<std::sync::Mutex<Vec<mpsc::UnboundedSender<PipelineEvent>>>>,
    }

    #[async_trait]
    impl PlaybackPipelineFactory for RecordingPipelineFactory {
        async fn create(&self, _config: PipelineConfig) -> Result<PipelineInstance, PipelineError> {
            let (events_tx, events_rx) = mpsc::unbounded_channel();
            self.event_senders.lock().unwrap_or_else(|e| e.into_inner()).push(events_tx);
            Ok(PipelineInstance {
                pipeline: self.pipeline.clone(),
                events: events_rx,
            })
        }
    }

    /// Builds a real `StationStreamer` over the shared recording pipeline
    /// (failure injection via `fail_once`), without GStreamer or Icecast.
    /// Returns the streamer plus the factory's event-sender keep-alive
    /// (see [`RecordingPipelineFactory`]).
    async fn recording_streamer(
        pool: &PgPool,
        name: &str,
        station_id: Uuid,
        pipeline: Arc<RecordingPipeline>,
    ) -> (
        Arc<StationStreamer>,
        Arc<std::sync::Mutex<Vec<mpsc::UnboundedSender<PipelineEvent>>>>,
    ) {
        let event_senders: Arc<std::sync::Mutex<Vec<mpsc::UnboundedSender<PipelineEvent>>>> = Default::default();
        let streamer = StationStreamer::new(
            Vec::new(),
            name,
            station_id,
            pool.clone(),
            1024,
            "/tmp",
            Arc::new(RecordingPipelineFactory {
                pipeline,
                event_senders: Arc::clone(&event_senders),
            }),
        )
        .await
        .expect("streamer must build over the recording pipeline");
        (streamer, event_senders)
    }

    /// `Shutdown` is terminal even when the pipeline stop fails (the
    /// executor exits either way), so the dead runtime must leave the map
    /// on BOTH outcomes: the failed stop error is propagated, but the
    /// streamer is not reusable and must not linger as a dead map entry
    /// that get/create would reuse and every later Play would poison.
    #[tokio::test]
    async fn failed_stop_removes_terminal_runtime_from_map() {
        run_with_test_db(async |pool| {
            let map: StreamersMap = Default::default();
            let lifecycle = Arc::new(StationLifecycleLocks::default());
            let station_id = Uuid::new_v4();

            // Injected pipeline stop failure: the committed finish returns
            // Err, the terminal streamer is removed, and a repeat begin
            // without a runtime is a no-op.
            let failing = Arc::new(RecordingPipeline::new());
            let (failing_streamer, _failing_keepalive) = recording_streamer(pool, "stop-failure", station_id, Arc::clone(&failing)).await;
            {
                let mut map = map.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(station_id, failing_streamer);
            }
            failing.fail_once(Call::Stop);
            let guard = lifecycle.lock(station_id).await;
            let shutdown = begin_runtime_shutdown_locked(&map, station_id, guard)
                .await
                .expect("admission must succeed; the failure happens in the pipeline stop");
            let RuntimeShutdown::Admitted {
                guard: _,
                streamer,
                receiver,
            } = shutdown
            else {
                panic!("a mapped runtime must produce an admitted shutdown");
            };
            assert!(
                finish_admitted_shutdown(&map, station_id, &streamer, receiver).await.is_err(),
                "a failed shutdown must surface as an error"
            );
            assert!(
                !map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "a terminal runtime must not stay in the map (no dead entry blocking get/create)"
            );
            let guard = lifecycle.lock(station_id).await;
            assert!(
                matches!(
                    begin_runtime_shutdown_locked(&map, station_id, guard).await,
                    Ok(RuntimeShutdown::NoRuntime { .. })
                ),
                "stopping without a runtime must be a no-op"
            );

            // A fresh runtime for the SAME station is fully operational: its
            // executor is alive, so play() succeeds — the dead streamer's
            // executor would answer "station runtime stopped" instead.
            let healthy_pipeline = Arc::new(RecordingPipeline::new());
            let (healthy, _healthy_keepalive) = recording_streamer(pool, "stop-ok", station_id, Arc::clone(&healthy_pipeline)).await;
            healthy.play().await.expect("a fresh runtime must be fully operational");
            {
                let mut map = map.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(station_id, healthy.clone());
            }
            let guard = lifecycle.lock(station_id).await;
            let shutdown = begin_runtime_shutdown_locked(&map, station_id, guard)
                .await
                .expect("admission must succeed");
            let RuntimeShutdown::Admitted {
                guard: _,
                streamer,
                receiver,
            } = shutdown
            else {
                panic!("a mapped runtime must produce an admitted shutdown");
            };
            assert!(
                finish_admitted_shutdown(&map, station_id, &streamer, receiver).await.is_ok(),
                "successful shutdown must succeed; calls: {:?}",
                healthy_pipeline.calls()
            );
            assert!(
                !map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "a successful shutdown must remove the runtime from the map"
            );
        })
        .await;
    }

    /// Explicit send failure (NOT caller cancellation): the command loop
    /// is already gone, so the streamer is dead and must leave the map —
    /// a dead entry would poison every later Play/Restart with "station
    /// runtime stopped". The stop is still reported as an error.
    #[tokio::test]
    async fn failed_send_removes_a_runtime_whose_command_loop_is_gone() {
        run_with_test_db(async |pool| {
            let map: StreamersMap = Default::default();
            let lifecycle = Arc::new(StationLifecycleLocks::default());
            let station_id = Uuid::new_v4();

            // Shut the runtime down WITHOUT the map helper, so the dead
            // streamer stays mapped — the situation a failed/cancelled stop
            // used to leave behind before the cleanup fix.
            let pipeline = Arc::new(RecordingPipeline::new());
            let (streamer, _keepalive) = recording_streamer(pool, "dead-send", station_id, Arc::clone(&pipeline)).await;
            streamer.shutdown().await;
            {
                let mut map = map.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(station_id, streamer);
            }

            // begin_shutdown fails on the closed command channel: the begin
            // helper must report the stop error (logging the source cause) AND
            // remove the dead Arc.
            let guard = lifecycle.lock(station_id).await;
            assert!(
                begin_runtime_shutdown_locked(&map, station_id, guard).await.is_err(),
                "a stop against a dead command loop must surface as an error"
            );
            assert!(
                !map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "the dead runtime must not stay in the map after a failed send"
            );

            // A fresh runtime for the same station is fully operational — the
            // dead entry never poisoned a later Play.
            let fresh = Arc::new(RecordingPipeline::new());
            let (replacement, _keepalive) = recording_streamer(pool, "dead-send-fresh", station_id, Arc::clone(&fresh)).await;
            replacement
                .play()
                .await
                .expect("a fresh runtime must start after the dead one was removed");
            {
                let mut map = map.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(station_id, replacement);
            }
            let guard = lifecycle.lock(station_id).await;
            let shutdown = begin_runtime_shutdown_locked(&map, station_id, guard)
                .await
                .expect("admission must succeed");
            let RuntimeShutdown::Admitted {
                guard: _,
                streamer,
                receiver,
            } = shutdown
            else {
                panic!("a mapped runtime must produce an admitted shutdown");
            };
            finish_admitted_shutdown(&map, station_id, &streamer, receiver)
                .await
                .expect("the replacement runtime must shut down cleanly");
            assert!(
                !map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "the replacement runtime must leave the map"
            );
        })
        .await;
    }

    /// A broken runtime must not prevent the fan-out from syncing the
    /// remaining stations: the DB mutation is already done, so every
    /// affected station gets its reload/notification attempt even when an
    /// earlier one fails; the first error is still reported.
    #[tokio::test]
    async fn sync_station_queues_continues_after_a_failing_station() {
        run_with_test_db(async |pool| {
            let map: StreamersMap = Default::default();
            let lifecycle = Arc::new(StationLifecycleLocks::default());
            let station_a = Uuid::new_v4();
            let station_b = Uuid::new_v4();

            // Station A has a runtime whose command loop is DEAD (a shutdown
            // already ran — the same failure mode a cancelled stop leaves
            // behind), so its reload fails on send; station B has no runtime
            // and must still receive its station-scoped queue notification.
            let broken = Arc::new(RecordingPipeline::new());
            let (broken_streamer, _broken_keepalive) = recording_streamer(pool, "fan-a", station_a, Arc::clone(&broken)).await;
            broken_streamer.shutdown().await;
            {
                let mut map = map.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(station_a, broken_streamer);
            }
            assert!(
                map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_a),
                "precondition: the dead runtime must still be mapped"
            );

            let mut notifications = lifecycle.subscribe_notifications();
            let result = sync_station_queues(pool, &map, &lifecycle, "/tmp", [station_a, station_b], true).await;
            assert!(result.is_err(), "the failing station's error must be reported");
            let event = tokio::time::timeout(Duration::from_secs(5), notifications.recv())
                .await
                .expect("station B must be notified despite station A's failure")
                .expect("notification channel must stay open");
            match event {
                StationEvent::Queue { station_id } => assert_eq!(station_id, station_b),
                other => panic!("expected a queue notification for station B, got {other:?}"),
            }
        })
        .await;
    }

    /// Cancelling the Stop caller mid-shutdown must not break terminal
    /// cleanup: once Shutdown is accepted (the pipeline stop entered the
    /// gate), the detached cleanup task owns the lifecycle guard, keeps
    /// station serialization, and removes the terminal runtime — even
    /// though the caller future died at `receiver.await`. A fresh runtime
    /// for the same station works afterwards.
    #[tokio::test]
    async fn cancelled_stop_still_removes_terminal_runtime_and_serializes() {
        run_with_test_db(async |pool| {
            let map: StreamersMap = Default::default();
            let lifecycle = Arc::new(StationLifecycleLocks::default());
            let station_id = Uuid::new_v4();
            let gate = Gate::new();
            let pipeline = Arc::new(RecordingPipeline::with_stop_gate(Arc::clone(&gate)));
            let (streamer, _keepalive) = recording_streamer(pool, "cancel-stop", station_id, Arc::clone(&pipeline)).await;
            {
                let mut map = map.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(station_id, streamer);
            }

            // Stop transition; the pipeline stop parks in the gate — at that
            // point Shutdown is accepted and the committed finish task owns
            // the guard (exactly the production tail of Restart/Delete).
            let stop_task = tokio::spawn({
                let map = Arc::clone(&map);
                let lifecycle = Arc::clone(&lifecycle);
                async move {
                    let guard = lifecycle.lock(station_id).await;
                    match begin_runtime_shutdown_locked(&map, station_id, guard).await? {
                        RuntimeShutdown::Admitted { guard, streamer, receiver } => {
                            // No await between admission and the transfer: the
                            // guard moves into the detached finish BEFORE the
                            // stop-result await.
                            let (stop_result, _guard) = tokio::spawn(async move {
                                let stop_result = finish_admitted_shutdown(&map, station_id, &streamer, receiver).await;
                                (stop_result, guard)
                            })
                            .await
                            .map_err(|_| AppError::Internal("Stream stop task failed".into()))?;
                            stop_result
                        }
                        RuntimeShutdown::NoRuntime { .. } => Ok(()),
                    }
                }
            });
            gate.wait_started().await;

            // The caller dies mid-shutdown (abort) — cleanup must continue.
            stop_task.abort();

            // A new transition must NOT pass while the shutdown is in progress:
            // it contends on the station lock held by the cleanup task.
            let mut contended = lifecycle.test_hooks().lock_contended.contend_watcher();
            let next_transition = tokio::spawn({
                let lifecycle = Arc::clone(&lifecycle);
                async move {
                    let _guard = lifecycle.lock(station_id).await;
                }
            });
            contended
                .wait("the next transition to contend on the station lock")
                .await
                .expect("station serialization must survive caller cancellation");

            // Let the shutdown finish: cleanup removes the terminal runtime,
            // then releases the lock and the next transition proceeds.
            gate.release();
            next_transition
                .await
                .expect("the next transition must proceed once the shutdown finished");
            assert!(
                !map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "the terminal runtime must be removed even though the caller was cancelled"
            );

            // A fresh runtime for the same station is fully operational.
            let fresh_pipeline = Arc::new(RecordingPipeline::new());
            let (fresh, _fresh_keepalive) = recording_streamer(pool, "cancel-stop-fresh", station_id, Arc::clone(&fresh_pipeline)).await;
            fresh.play().await.expect("a fresh runtime must work after the cancelled shutdown");
        })
        .await;
    }

    /// The committed Delete operation owns the row delete + the lifecycle
    /// notification: the operation task is spawned BEFORE the mutating DB
    /// await, so killing the request caller cannot suppress the
    /// notification that tells no-runtime forwarders the station is gone.
    #[tokio::test]
    async fn committed_delete_removes_row_and_notifies_after_caller_death() {
        run_with_test_db(async |pool| {
            let map: StreamersMap = Default::default();
            let lifecycle = Arc::new(StationLifecycleLocks::default());
            let station_id = Uuid::new_v4();

            // A stopped station with zero runtime.
            let user_id = Uuid::new_v4();
            let username = format!("committed-delete-{user_id}");
            sqlx::query("INSERT INTO users (id, username, password_hash, name) VALUES ($1, $2, 'x', $3)")
                .bind(user_id)
                .bind(&username)
                .bind(&username)
                .execute(&pool.pool)
                .await
                .expect("user insert");
            sqlx::query("INSERT INTO stations (id, name, created_by) VALUES ($1, 'committed-delete', $2)")
                .bind(station_id)
                .bind(user_id)
                .execute(&pool.pool)
                .await
                .expect("station insert");

            // The committed operation holds the guard; a subscriber listens
            // for the lifecycle notification.
            let mut notifications = lifecycle.subscribe_notifications();
            let guard = lifecycle.lock(station_id).await;
            let operation = tokio::spawn(run_committed_delete(pool.pool.clone(), Arc::clone(&lifecycle), station_id, guard));
            let caller = tokio::spawn(async move { operation.await });

            // The request caller dies mid-operation (cancellation during the
            // DB await is covered by the spawn-before-mutation structure) —
            // the operation task itself must continue.
            caller.abort();
            assert!(caller.await.is_err(), "the Delete caller must be cancelled");

            let event = tokio::time::timeout(Duration::from_secs(5), notifications.recv())
                .await
                .expect("the committed delete must notify after the row delete")
                .expect("notification channel must stay open");
            match event {
                StationEvent::Lifecycle { station_id: notified } => assert_eq!(notified, station_id),
                other => panic!("expected a lifecycle notification, got {other:?}"),
            }
            assert!(
                repository::find_station_by_id(pool, station_id)
                    .await
                    .expect("station lookup")
                    .is_none(),
                "the station row must be gone"
            );
            assert!(
                !map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "no runtime may appear"
            );

            // The operation released the guard when it finished: the next
            // transition must pass without contention.
            let _next = lifecycle.lock(station_id).await;
        })
        .await;
    }

    /// Full-Stop cancellation regression: the caller owns ONLY the guard
    /// acquisition and the JoinHandle await — the whole mutating phase
    /// (persist `false` → technical shutdown → map cleanup) runs in the
    /// detached stop operation. The test parks `before_stop` (inside the
    /// operation, with the guard, BEFORE the DB write), aborts the Stop
    /// caller, and proves the operation still persists the intent, stops
    /// the runtime, cleans the map and only then releases the station
    /// serialization.
    #[tokio::test]
    async fn stop_survives_caller_cancellation_before_persistence() {
        run_with_test_db(async |pool| {
            let map: StreamersMap = Default::default();
            let lifecycle = Arc::new(StationLifecycleLocks::default());
            let station_id = Uuid::new_v4();

            // A station persisted as started with a live runtime.
            let user_id = Uuid::new_v4();
            let username = format!("committed-stop-{user_id}");
            sqlx::query("INSERT INTO users (id, username, password_hash, name) VALUES ($1, $2, 'x', $3)")
                .bind(user_id)
                .bind(&username)
                .bind(&username)
                .execute(&pool.pool)
                .await
                .expect("user insert");
            sqlx::query("INSERT INTO stations (id, name, created_by) VALUES ($1, 'committed-stop', $2)")
                .bind(station_id)
                .bind(user_id)
                .execute(&pool.pool)
                .await
                .expect("station insert");
            repository::set_station_started(pool, station_id, true)
                .await
                .expect("started persist");
            let pipeline = Arc::new(RecordingPipeline::new());
            let (streamer, _keepalive) = recording_streamer(pool, "committed-stop", station_id, Arc::clone(&pipeline)).await;
            streamer.play().await.expect("the runtime must be live");
            {
                let mut map = map.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(station_id, streamer);
            }

            // Park the stop operation BEFORE the DB write: it holds the
            // station guard, but `is_started=false` has not been persisted yet.
            let hooks = lifecycle.test_hooks();
            hooks.before_stop.arm();
            let stop_task = tokio::spawn({
                let pool = pool.pool.clone();
                let map = Arc::clone(&map);
                let lifecycle = Arc::clone(&lifecycle);
                async move { stop_station(&pool, &map, &lifecycle, station_id).await }
            });
            tokio::time::timeout(Duration::from_secs(5), hooks.before_stop.entered().notified())
                .await
                .expect("the Stop must reach the before_stop hook");

            // The mutation has NOT started: desired state is still `true` and
            // the runtime is still mapped.
            assert_eq!(
                repository::find_station_started(pool, station_id).await.expect("started readback"),
                Some(true),
                "is_started must still be true: the DB write has not started"
            );
            assert!(
                map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "the runtime must still be mapped"
            );

            // The request caller dies BEFORE the mutation. The detached stop
            // operation must keep the guard and finish persist + shutdown.
            stop_task.abort();
            assert!(stop_task.await.is_err(), "the Stop caller must be cancelled");

            // A new transition must NOT pass: it contends on the station lock
            // held by the detached stop operation.
            let mut contended = hooks.lock_contended.contend_watcher();
            let next_transition = tokio::spawn({
                let lifecycle = Arc::clone(&lifecycle);
                async move {
                    let _guard = lifecycle.lock(station_id).await;
                }
            });
            contended
                .wait("the next transition to contend behind the committed stop")
                .await
                .expect("station serialization must survive the Stop caller's death");

            // Release: the operation persists false, admits the Shutdown (the
            // command channel is empty, so admission is immediate), stops the
            // pipeline and cleans the map; only then does the next transition
            // proceed.
            hooks.before_stop.release();
            next_transition
                .await
                .expect("the next transition must proceed once the committed stop finished");
            assert!(
                !map.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&station_id),
                "the committed stop must remove the runtime even though the caller was cancelled before persistence"
            );
            assert!(
                pipeline.calls().contains(&Call::Stop),
                "the runtime must have been stopped by the operation"
            );
            assert_eq!(
                repository::find_station_started(pool, station_id).await.expect("started readback"),
                Some(false),
                "final state: is_started=false"
            );
        })
        .await;
    }
}
