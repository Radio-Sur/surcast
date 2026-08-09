use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::{broadcast, mpsc};

use super::driver::{PipelineDriver, PipelineOperation, PipelineOperationResult};
use super::pipeline::{
    IcecastTarget, PairPlan, PipelineError, PipelineEvent, PipelineSnapshot, PipelineState, PipelineTrack, PlaybackPipelineFactory,
    StationPlaybackConfig, TrackKey,
};
use super::{QueueManager, SongInfo, StatusEvent};
use crate::stations::repository;

pub(crate) struct StationController {
    queue: Arc<QueueManager>,
    station_id: uuid::Uuid,
    playback: StationPlaybackConfig,
    driver: PipelineDriver,
    target: IcecastTarget,
    state: PipelineState,
    status_tx: broadcast::Sender<StatusEvent>,
    queue_tx: broadcast::Sender<String>,
    generation: u64,
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
                station_id,
                queue,
                playback,
                driver: PipelineDriver::spawn(instance.pipeline),
                target,
                state: PipelineState::Stopped,
                status_tx,
                queue_tx,
                generation: 0,
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
                let current = self.queue.current_song_info().map(|song| song.queue_item_id);
                if generation == self.generation && current == Some(track.queue_item_id) {
                    return Some(self.skip().await);
                }
            }
            PipelineEvent::CurrentEos {
                generation,
                current: track,
            } => {
                let current = self.queue.current_song_info().map(|song| song.queue_item_id);
                if generation == self.generation && current == Some(track.queue_item_id) {
                    return Some(self.skip().await);
                }
            }
            PipelineEvent::Handover {
                generation,
                current: track,
            } => {
                let current = self.queue.current_song_info().map(|song| song.queue_item_id);
                if generation == self.generation && current != Some(track.queue_item_id) {
                    self.queue.commit_current(&track).await;
                    self.publish_song_change();
                    self.push_queue_update().await;
                }
            }
            PipelineEvent::SinkDisconnected { generation, message } => {
                tracing::error!(station_id = %self.station_id, generation, %message, "GStreamer output disconnected");
            }
        }
        None
    }

    fn track(song: SongInfo) -> PipelineTrack {
        PipelineTrack {
            key: TrackKey {
                queue_item_id: song.queue_item_id,
                song_id: song.song_id,
                position: song.position,
            },
            path: PathBuf::from(song.file_path),
            cue_in: Duration::from_secs_f64(song.cue_in.max(0.0)),
            cue_out: Duration::from_secs_f64(song.cue_out.max(0.0)),
            cross_start_next: Duration::from_secs_f64(song.cross_start_next.max(0.0)),
            analyzed: song.analyzed,
        }
    }

    fn replace_current(&mut self) -> PipelineOperation {
        let Some(current) = self.queue.current_song_info() else {
            return PipelineOperation::Stop;
        };
        let next = self.queue.peek_next_song();
        let current_track = Self::track(current);
        let next_track = next.map(Self::track);
        let transition = super::pipeline::TransitionPlanner::plan(self.playback.transition, &current_track, next_track.as_ref());
        self.generation += 1;
        PipelineOperation::Replace(Box::new(PairPlan {
            generation: self.generation,
            current: current_track,
            next: next_track,
            transition,
        }))
    }

    pub(crate) fn play(&mut self) -> PipelineOperation {
        if self.state == PipelineState::Stopped {
            self.state = PipelineState::Playing;
            self.replace_current()
        } else {
            self.state = PipelineState::Playing;
            PipelineOperation::SetPlaying(true)
        }
    }

    pub(crate) fn pause(&mut self) -> PipelineOperation {
        self.state = PipelineState::Paused;
        PipelineOperation::SetPlaying(false)
    }

    pub(crate) fn stop(&mut self) -> PipelineOperation {
        self.state = PipelineState::Stopped;
        PipelineOperation::Stop
    }

    pub(crate) fn reconnect(&self) -> PipelineOperation {
        PipelineOperation::Reconnect(self.target.clone())
    }

    fn stop_after_current(&mut self) -> PipelineOperation {
        self.generation += 1;
        self.state = PipelineState::Stopped;
        PipelineOperation::Stop
    }

    pub(crate) async fn skip(&mut self) -> Result<PipelineOperation, PipelineError> {
        let Some(current) = self.queue.current_song_info() else {
            return Ok(self.stop_after_current());
        };
        let current_key = TrackKey {
            queue_item_id: current.queue_item_id,
            song_id: current.song_id,
            position: current.position,
        };
        let Some(next) = self.queue.successor_after(&current_key) else {
            return Ok(self.stop_after_current());
        };
        let next_key = TrackKey {
            queue_item_id: next.queue_item_id,
            song_id: next.song_id,
            position: next.position,
        };
        let _ = self.queue.commit_current(&next_key).await;
        let operation = self.replace_current();
        self.publish_song_change();
        self.push_queue_update().await;
        Ok(operation)
    }

    pub(crate) async fn reload(&self, songs: Vec<SongInfo>) -> Result<(), PipelineError> {
        self.queue.reload_songs(songs);
        Ok(())
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
        if let Some(song) = self.queue.song_info(idx) {
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
        let song = self.queue.song_info(idx);
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
    async fn stale_decode_failure_does_not_replace_the_current_plan() {
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
        let db = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://surcast:surcast@localhost:5433/surcast")
            .unwrap();
        let station_id = Uuid::new_v4();
        let queue = Arc::new(QueueManager::new(db.clone(), station_id, String::new(), vec![song.clone()], 0));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let mut controller = StationController {
            queue,
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 1,
        };
        let _ = controller
            .handle_event(PipelineEvent::DecodeFailed {
                generation: 0,
                track: StationController::track(song).key,
                message: "stale".into(),
            })
            .await;
        assert_eq!(pipeline.replacements.load(Ordering::Acquire), 0);
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
        let db = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://surcast:surcast@localhost:5433/surcast")
            .unwrap();
        let station_id = Uuid::new_v4();
        let queue = Arc::new(QueueManager::new(db.clone(), station_id, String::new(), vec![song.clone()], 0));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let controller = StationController {
            queue,
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 1,
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
        tokio::time::timeout(Duration::from_millis(100), async {
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
        let db = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://surcast:surcast@localhost:5433/surcast")
            .unwrap();
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
            station_id,
            playback: StationPlaybackConfig::from_persisted("off", 0, 0, 0).unwrap(),
            driver: PipelineDriver::spawn(pipeline.clone()),
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            state: PipelineState::Stopped,
            status_tx,
            queue_tx,
            generation: 0,
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
                    position: 0,
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

        tokio::time::timeout(
            Duration::from_millis(100),
            runtime.update_config(StationPlaybackConfig::from_persisted("crossfade", 1000, 1000, 0).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();

        pipeline.release_replace.notify_one();
        playing.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_millis(100), async {
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
}
