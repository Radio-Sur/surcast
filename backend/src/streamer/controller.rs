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
                if self.output_is_current(generation, output_epoch) {
                    return Some(self.reconnect().await);
                }
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
        PipelineOperation::Replace(Box::new(PairPlan {
            mode,
            generation: self.generation,
            output_epoch: self.output_epoch,
            current: current_track,
            next,
        }))
    }

    pub(crate) async fn play(&mut self) -> PipelineOperation {
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
        self.state = PipelineState::Paused;
        PipelineOperation::SetPlaying(false)
    }

    pub(crate) fn stop(&mut self) -> PipelineOperation {
        self.idle = false;
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
    /// no operation while the queue stays empty, so the runtime keeps
    /// polling on the next tick without touching the stopped pipeline.
    pub(crate) async fn resume_from_idle(&mut self) -> Option<PipelineOperation> {
        if !self.idle {
            return None;
        }
        self.queue.refill().await;
        self.queue.reload_from_db().await;
        self.queue.current_song_info()?;
        self.idle = false;
        self.push_queue_update().await;
        let operation = self.replace_current(ReplaceMode::InitialReplaceFromStopped);
        self.state = PipelineState::Playing;
        Some(operation)
    }

    pub(crate) async fn reconnect(&mut self) -> Result<PipelineOperation, PipelineError> {
        let (endpoint, password) = crate::icecast::models::get_connection_config(&self.db)
            .await
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        self.target = IcecastTarget::parse(&endpoint, password, &self.target.mount, self.target.stream_name.clone())?;
        Ok(PipelineOperation::Reconnect(self.target.clone()))
    }
    fn output_is_current(&self, generation: u64, output_epoch: u64) -> bool {
        generation == self.generation && output_epoch == self.output_epoch && self.state == PipelineState::Playing
    }

    pub(crate) async fn reconnect_if_current(
        &mut self,
        generation: u64,
        output_epoch: u64,
    ) -> Result<Option<PipelineOperation>, PipelineError> {
        if self.output_is_current(generation, output_epoch) {
            self.reconnect().await.map(Some)
        } else {
            Ok(None)
        }
    }

    async fn stop_after_current(&mut self) -> PipelineOperation {
        self.queue.finish_current().await;
        self.generation += 1;
        // The queue drained on its own: this is an idle wait for new content,
        // not a manual stop, so the runtime may auto-resume later.
        self.idle = true;
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

    pub(crate) async fn status(&self) -> StatusEvent {
        let PipelineSnapshot { state, elapsed } = match self.driver.execute(PipelineOperation::Snapshot).await {
            Ok(PipelineOperationResult::Snapshot(snapshot)) => snapshot,
            Ok(PipelineOperationResult::Unit) | Err(_) => PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            },
        };
        let idx = self.queue.current_song_index();
        let song = self.queue.current_song_info();
        StatusEvent::State {
            playing: state == PipelineState::Playing,
            song_index: idx,
            total: self.queue.song_count(),
            elapsed: elapsed.as_secs(),
            title: song.as_ref().map_or_else(String::new, |song| song.title.clone()),
            artist: song.as_ref().map_or_else(String::new, |song| song.artist.clone()),
            duration: song.map_or(0, |song| song.duration),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::streamer::pipeline::OutputConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::super::pipeline::PlaybackPipeline;

    use async_trait::async_trait;
    use tokio::sync::{broadcast, mpsc, Mutex, Notify};
    use uuid::Uuid;

    use crate::streamer::runtime::StationRuntime;

    use super::*;
    fn unavailable_db() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(10))
            .connect_lazy("postgres://surcast:surcast@127.0.0.1:1/surcast")
            .unwrap()
    }

    struct FakePipeline {
        replacements: AtomicUsize,
        state_changes: AtomicUsize,
        stops: AtomicUsize,
    }

    #[async_trait]
    impl PlaybackPipeline for FakePipeline {
        async fn replace(&self, _: PairPlan) -> Result<(), PipelineError> {
            self.replacements.fetch_add(1, Ordering::Release);
            Ok(())
        }
        async fn roll(&self, _: super::super::pipeline::RollingPlan) -> Result<(), PipelineError> {
            Ok(())
        }
        async fn apply_output(&self, _: OutputConfig) -> Result<(), PipelineError> {
            Ok(())
        }

        async fn set_playing(&self, _: bool) -> Result<(), PipelineError> {
            self.state_changes.fetch_add(1, Ordering::Release);
            Ok(())
        }

        async fn reconnect(&self, _: IcecastTarget) -> Result<(), PipelineError> {
            Ok(())
        }

        async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
            Ok(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            })
        }

        async fn stop(&self) -> Result<(), PipelineError> {
            self.stops.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    struct BlockingPipeline {
        replace_started: Notify,
        release_replace: Notify,
        calls: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl PlaybackPipeline for BlockingPipeline {
        async fn replace(&self, _: PairPlan) -> Result<(), PipelineError> {
            self.calls.lock().await.push("replace");
            self.replace_started.notify_one();
            self.release_replace.notified().await;
            Ok(())
        }
        async fn roll(&self, _: super::super::pipeline::RollingPlan) -> Result<(), PipelineError> {
            self.calls.lock().await.push("roll");
            Ok(())
        }

        async fn apply_output(&self, _: OutputConfig) -> Result<(), PipelineError> {
            self.calls.lock().await.push("output");
            Ok(())
        }

        async fn set_playing(&self, _: bool) -> Result<(), PipelineError> {
            self.calls.lock().await.push("set_playing");
            Ok(())
        }

        async fn reconnect(&self, _: IcecastTarget) -> Result<(), PipelineError> {
            Ok(())
        }

        async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
            Ok(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            })
        }

        async fn stop(&self) -> Result<(), PipelineError> {
            self.calls.lock().await.push("stop");
            Ok(())
        }
    }

    #[tokio::test]
    async fn stale_events_do_not_replace_or_reconnect() {
        let song = SongInfo {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
            title: "current".into(),
            artist: String::new(),
            duration: 1,
            file_path: String::new(),
            position: 0,
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            analyzed: false,
        };
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let db = unavailable_db();
        let station_id = Uuid::new_v4();
        let queue = Arc::new(QueueManager::new(db.clone(), station_id, String::new(), vec![song.clone()], 0));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let mut controller = StationController {
            queue,
            db: unavailable_db(),
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 1,
            output_epoch: 0,
            planned_next: None,
            idle: false,
        };
        let _ = controller
            .handle_event(PipelineEvent::DecodeFailed {
                generation: 0,
                track: StationController::track(song).key,
                message: "stale".into(),
            })
            .await;
        assert_eq!(pipeline.replacements.load(Ordering::Acquire), 0);
        controller.state = PipelineState::Playing;
        controller.output_epoch = 3;
        assert!(controller.output_is_current(1, 3));
        assert!(!controller.output_is_current(0, 3));
        controller.state = PipelineState::Paused;
        assert!(!controller.output_is_current(1, 3));
        controller.stop();
        assert!(!controller.output_is_current(1, 3));
    }

    #[tokio::test]
    async fn play_with_an_empty_queue_keeps_the_controller_stopped() {
        // No database is reachable here, so the AutoDJ refill attempt inside
        // play() cannot produce songs and the controller must stay Stopped
        // (the E2E suite covers the case where the refill succeeds).
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let db = unavailable_db();
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let mut controller = StationController {
            queue: Arc::new(QueueManager::new(db, Uuid::new_v4(), String::new(), Vec::new(), 0)),
            db: unavailable_db(),
            station_id: Uuid::new_v4(),
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 0,
            output_epoch: 0,
            planned_next: None,
            idle: false,
        };

        assert!(matches!(controller.play().await, PipelineOperation::Stop));
        assert_eq!(controller.state, PipelineState::Stopped);
        assert_eq!(pipeline.replacements.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn next_decode_failure_replaces_only_the_failed_terminal_branch() {
        let song = |position| SongInfo {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
            title: String::new(),
            artist: String::new(),
            duration: 1,
            file_path: String::new(),
            position,
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            analyzed: false,
        };
        let current = song(0);
        let failed = song(1);
        let successor = song(2);
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let db = unavailable_db();
        let station_id = Uuid::new_v4();
        let queue = Arc::new(QueueManager::new(
            db,
            station_id,
            String::new(),
            vec![current.clone(), failed.clone(), successor.clone()],
            0,
        ));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let failed_key = StationController::track(failed).key;
        let mut controller = StationController {
            queue: queue.clone(),
            db: unavailable_db(),
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Playing,
            status_tx,
            queue_tx,
            generation: 1,
            output_epoch: 1,
            planned_next: Some((failed_key.clone(), queue.anchor_after_current())),
            idle: false,
        };

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
        let song = SongInfo {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
            title: "current".into(),
            artist: String::new(),
            duration: 1,
            file_path: String::new(),
            position: 0,
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            analyzed: false,
        };
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let db = unavailable_db();
        let station_id = Uuid::new_v4();
        let queue = Arc::new(QueueManager::new(db.clone(), station_id, String::new(), vec![song.clone()], 0));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let controller = StationController {
            queue,
            db: unavailable_db(),
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 1,
            output_epoch: 0,
            planned_next: None,
            idle: false,
        };
        let (events, receiver) = mpsc::unbounded_channel();
        let runtime = StationRuntime::spawn(controller, receiver);
        runtime.pause().await.unwrap();
        events
            .send(PipelineEvent::CurrentEos {
                generation: 1,
                current: StationController::track(song).key,
            })
            .unwrap();
        // With no database reachable the exhaustion refill fails and is
        // retried (bounded, ~750ms of backoff) before the controller stops.
        tokio::time::timeout(Duration::from_secs(2), async {
            while pipeline.stops.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pipeline.state_changes.load(Ordering::Acquire), 1);
        assert_eq!(pipeline.stops.load(Ordering::Acquire), 1);
        assert_eq!(pipeline.replacements.load(Ordering::Acquire), 0);
        runtime.shutdown().await.unwrap();
        assert!(runtime.play().await.is_err());
    }
    #[tokio::test]
    async fn stale_and_duplicate_events_do_not_supersede_the_current_plan() {
        let song = SongInfo {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
            title: "current".into(),
            artist: String::new(),
            duration: 1,
            file_path: String::new(),
            position: 0,
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            analyzed: false,
        };
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let db = unavailable_db();
        let station_id = Uuid::new_v4();
        let current = StationController::track(song.clone()).key;
        let queue = Arc::new(QueueManager::new(db, station_id, String::new(), vec![song], 0));
        let pipeline = Arc::new(BlockingPipeline {
            replace_started: Notify::new(),
            release_replace: Notify::new(),
            calls: Mutex::new(Vec::new()),
        });
        let controller = StationController {
            queue,
            db: unavailable_db(),
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 0,
            output_epoch: 0,
            planned_next: None,
            idle: false,
        };
        let (events, receiver) = mpsc::unbounded_channel();
        let runtime = StationRuntime::spawn(controller, receiver);
        let playing = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.play().await })
        };
        pipeline.replace_started.notified().await;
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

        pipeline.release_replace.notify_one();
        playing.await.unwrap().unwrap();
        // play() tops up a below-minimum queue through the database, so the
        // stop after the terminal EOS waits on real DB roundtrips; the exact
        // call sequence is asserted below, this is only a wait-for-it window.
        tokio::time::timeout(Duration::from_secs(5), async {
            while pipeline.calls.lock().await.len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut config = StationPlaybackConfig::from_persisted("crossfade", 1000, 1000, 4096).unwrap();
        config.output.prebuffer_bytes = 8192;
        runtime.update_config(config).await.unwrap();
        runtime.shutdown().await.unwrap();
        assert_eq!(*pipeline.calls.lock().await, ["replace", "stop", "output", "stop"]);
    }

    fn song_at(position: i32, title: &str) -> SongInfo {
        SongInfo {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
            title: title.into(),
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

    async fn playing_controller(songs: Vec<SongInfo>) -> (StationController, Arc<FakePipeline>) {
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let db = unavailable_db();
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let mut controller = StationController {
            queue: Arc::new(QueueManager::new(db, Uuid::new_v4(), String::new(), songs, 0)),
            db: unavailable_db(),
            station_id: Uuid::new_v4(),
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 0,
            output_epoch: 0,
            planned_next: None,
            idle: false,
        };
        assert!(matches!(controller.play().await, PipelineOperation::Replace(_)));
        assert_eq!(controller.state, PipelineState::Playing);
        (controller, pipeline)
    }

    #[tokio::test]
    async fn reload_into_a_stopped_controller_starts_playback_once_songs_arrive() {
        // Start was pressed with an empty queue: the controller sits Stopped
        // with nothing to play. The first reload that brings songs must kick
        // off an InitialReplaceFromStopped, not stay idle.
        let a = song_at(0, "A");
        let (mut controller, pipeline) = {
            let (status_tx, _) = broadcast::channel(1);
            let (queue_tx, _) = broadcast::channel(1);
            let db = unavailable_db();
            let pipeline = Arc::new(FakePipeline {
                replacements: AtomicUsize::new(0),
                state_changes: AtomicUsize::new(0),
                stops: AtomicUsize::new(0),
            });
            let mut controller = StationController {
                queue: Arc::new(QueueManager::new(db, Uuid::new_v4(), String::new(), Vec::new(), 0)),
                db: unavailable_db(),
                station_id: Uuid::new_v4(),
                playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
                driver: PipelineDriver::spawn(pipeline.clone()),
                target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
                state: PipelineState::Stopped,
                status_tx,
                queue_tx,
                generation: 0,
                output_epoch: 0,
                planned_next: None,
                idle: false,
            };
            // Play with an empty queue is a no-op that leaves the controller stopped.
            assert!(matches!(controller.play().await, PipelineOperation::Stop));
            assert_eq!(controller.state, PipelineState::Stopped);
            (controller, pipeline)
        };

        let operation = controller.reload(vec![a], false).await.unwrap();
        let Some(PipelineOperation::Replace(plan)) = operation else {
            panic!("reload into a stopped controller with songs must issue a replace");
        };
        assert!(matches!(plan.mode, ReplaceMode::InitialReplaceFromStopped));
        assert_eq!(controller.state, PipelineState::Playing);
        assert_eq!(
            pipeline.replacements.load(Ordering::Acquire),
            0,
            "replace is executed by the runtime, not the controller"
        );

        // A reload that still arrives empty must keep the controller stopped.
        let (mut controller, _) = {
            let (status_tx, _) = broadcast::channel(1);
            let (queue_tx, _) = broadcast::channel(1);
            let db = unavailable_db();
            let pipeline = Arc::new(FakePipeline {
                replacements: AtomicUsize::new(0),
                state_changes: AtomicUsize::new(0),
                stops: AtomicUsize::new(0),
            });
            let mut controller = StationController {
                queue: Arc::new(QueueManager::new(db, Uuid::new_v4(), String::new(), Vec::new(), 0)),
                db: unavailable_db(),
                station_id: Uuid::new_v4(),
                playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
                driver: PipelineDriver::spawn(pipeline.clone()),
                target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
                state: PipelineState::Stopped,
                status_tx,
                queue_tx,
                generation: 0,
                output_epoch: 0,
                planned_next: None,
                idle: false,
            };
            assert!(matches!(controller.play().await, PipelineOperation::Stop));
            (controller, pipeline)
        };
        let operation = controller.reload(vec![], false).await.unwrap();
        assert!(operation.is_none(), "an empty reload must not start anything");
        assert_eq!(controller.state, PipelineState::Stopped);
    }

    #[tokio::test]
    async fn reload_realigns_staged_next_to_reordered_head() {
        let a = song_at(0, "A");
        let b = song_at(1, "B");
        let c = song_at(2, "C");
        let x = song_at(3, "X");
        let (mut controller, _) = playing_controller(vec![a.clone(), b.clone(), c.clone()]).await;
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, b.queue_item_id);

        // A reorder moved X to the top: the staged next (B) must be replaced by X
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
        let a = song_at(0, "A");
        let b = song_at(1, "B");
        let x = song_at(3, "X");
        let (mut controller, _) = playing_controller(vec![a.clone(), b.clone()]).await;
        let operation = controller.reload(vec![a, x, b.clone()], false).await.unwrap();
        assert!(operation.is_none(), "non-aligning reload must not touch the pipeline");
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, b.queue_item_id);
    }

    #[tokio::test]
    async fn reload_with_unchanged_head_does_not_roll() {
        let a = song_at(0, "A");
        let b = song_at(1, "B");
        let c = song_at(2, "C");
        let x = song_at(3, "X");
        let (mut controller, _) = playing_controller(vec![a.clone(), b.clone(), c.clone()]).await;
        // Append-only change (e.g. a manual add): the head stays B, no swap needed
        let operation = controller.reload(vec![a, b.clone(), c.clone(), x], true).await.unwrap();
        assert!(operation.is_none(), "append-only reload must not roll");
        assert_eq!(controller.planned_next.as_ref().unwrap().0.queue_item_id, b.queue_item_id);
    }

    #[tokio::test]
    async fn reload_exhausting_queue_drops_the_staged_next() {
        let a = song_at(0, "A");
        let b = song_at(1, "B");
        let (mut controller, _) = playing_controller(vec![a.clone(), b.clone()]).await;
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
        let a = song_at(0, "A");
        let b = song_at(1, "B");
        let x = song_at(3, "X");
        let (mut controller, _) = playing_controller(vec![a.clone(), b.clone()]).await;
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
        let song = song_at(0, "A");
        let fresh = song_at(1, "B");
        let (mut controller, _pipeline) = playing_controller(vec![song.clone()]).await;

        // The last track ends: skip() exhausts the queue (the unreachable DB
        // makes the fill retries fail) and the controller becomes idle —
        // stopped, but marked for auto-resume, unlike a manual stop.
        let operation = controller.skip().await.unwrap();
        assert!(matches!(operation, PipelineOperation::Stop));
        assert_eq!(controller.state, PipelineState::Stopped);
        assert!(controller.idle());

        // AutoDJ / schedule fill inserts a row. No API command arrives; the
        // periodic idle tick must start playback on its own.
        controller.queue.reload_songs(vec![fresh], false);
        let operation = controller
            .resume_from_idle()
            .await
            .expect("an idle station must resume once the queue fills");
        let PipelineOperation::Replace(plan) = operation else {
            panic!("idle resume must issue an initial replace");
        };
        assert!(matches!(plan.mode, ReplaceMode::InitialReplaceFromStopped));
        assert_eq!(controller.state, PipelineState::Playing);
        assert!(!controller.idle());

        // A manual stop must never be auto-resumed, even with songs queued.
        controller.stop();
        assert!(!controller.idle());
        assert!(controller.resume_from_idle().await.is_none());
        assert_eq!(controller.state, PipelineState::Stopped);
    }

    #[tokio::test]
    async fn idle_runtime_auto_starts_when_content_arrives_without_an_api_command() {
        let song = song_at(0, "A");
        let fresh = song_at(1, "B");
        let (status_tx, _) = broadcast::channel(1);
        let (queue_tx, _) = broadcast::channel(1);
        let db = unavailable_db();
        let station_id = Uuid::new_v4();
        let queue = Arc::new(QueueManager::new(db, station_id, String::new(), vec![song.clone()], 0));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let controller = StationController {
            queue: queue.clone(),
            db: unavailable_db(),
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 0,
            output_epoch: 0,
            planned_next: None,
            idle: false,
        };
        let (events, receiver) = mpsc::unbounded_channel();
        let runtime = StationRuntime::spawn(controller, receiver);
        runtime.play().await.unwrap();
        events
            .send(PipelineEvent::CurrentEos {
                generation: 1,
                current: StationController::track(song).key,
            })
            .unwrap();
        // With no database reachable the exhaustion refill fails and is
        // retried (bounded, ~750ms of backoff) before the controller stops.
        tokio::time::timeout(Duration::from_secs(2), async {
            while pipeline.stops.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pipeline.replacements.load(Ordering::Acquire), 1);

        // Content arrives in the queue (AutoDJ / schedule fill writing rows),
        // with NO API command: only the runtime's idle tick polls. The next
        // tick must replace the plan and start playback.
        queue.reload_songs(vec![fresh], false);
        tokio::time::timeout(Duration::from_secs(4), async {
            while pipeline.replacements.load(Ordering::Acquire) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the idle runtime must start playback once the queue fills");
        assert_eq!(pipeline.replacements.load(Ordering::Acquire), 2);
        runtime.shutdown().await.unwrap();
    }
}
