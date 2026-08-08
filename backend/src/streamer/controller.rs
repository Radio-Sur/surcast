use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc;

use super::pipeline::{
    IcecastTarget, PairPlan, PipelineConfig, PipelineError, PipelineEvent, PipelineSnapshot, PipelineState, PipelineTrack,
    PlaybackPipeline, PlaybackPipelineFactory, TrackKey,
};
use super::{QueueManager, SongInfo, StatusEvent};

pub(crate) struct StationController {
    queue: Arc<QueueManager>,
    pipeline: Arc<dyn PlaybackPipeline>,
    generation: AtomicU64,
}
impl StationController {
    pub(crate) async fn new(
        queue: Arc<QueueManager>,
        db: PgPool,
        mount: &str,
        prebuffer_bytes: i32,
        factory: Arc<dyn PlaybackPipelineFactory>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<PipelineEvent>), PipelineError> {
        let (endpoint, password) = crate::icecast::models::get_connection_config(&db)
            .await
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let target = IcecastTarget::parse(&endpoint, password, mount, mount.trim_matches('/').to_owned())?;
        let instance = factory
            .create(PipelineConfig {
                target,
                prebuffer_bytes: prebuffer_bytes.max(0) as usize,
                sample_rate: 44_100,
                channels: 2,
                bitrate_kbps: 128,
            })
            .await?;
        Ok((
            Self {
                queue,
                pipeline: instance.pipeline,
                generation: AtomicU64::new(0),
            },
            instance.events,
        ))
    }

    pub(crate) fn start_events(self: Arc<Self>, mut events: mpsc::UnboundedReceiver<PipelineEvent>) {
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    PipelineEvent::DecodeFailed { generation, track, .. }
                    | PipelineEvent::CurrentEos {
                        generation,
                        current: track,
                    } => {
                        let current = self.queue.current_song_info().map(|song| song.queue_item_id);
                        if generation == self.generation.load(Ordering::Acquire) && current == Some(track.queue_item_id) {
                            if let Err(error) = self.skip().await {
                                tracing::error!(station_id = %self.queue.station_id, error = %error, "failed to advance completed or undecodable current track");
                            }
                        }
                    }
                    PipelineEvent::Handover { .. } | PipelineEvent::SinkDisconnected { .. } => {}
                }
            }
        });
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

    async fn replace_current(&self) -> Result<(), PipelineError> {
        let Some(current) = self.queue.current_song_info() else {
            return self.pipeline.stop().await;
        };
        let next = self.queue.peek_next_song();
        let (mode, fade_ms, autocue_cap_ms) = sqlx::query_as::<_, (String, i32, i32)>(
            "SELECT transition_mode, default_fade_ms, autocue_fade_max_ms FROM stations WHERE id = $1",
        )
        .bind(self.queue.station_id)
        .fetch_optional(&self.queue.db)
        .await
        .map_err(|error| PipelineError::Pipeline(error.to_string()))?
        .unwrap_or_else(|| ("off".into(), 0, 0));
        let current_track = Self::track(current);
        let next_track = next.map(Self::track);
        let transition = super::pipeline::transition_plan(
            &mode,
            &current_track,
            next_track.as_ref(),
            Duration::from_millis(fade_ms.max(0) as u64),
            Duration::from_millis(autocue_cap_ms.max(0) as u64),
        );
        self.pipeline
            .replace(PairPlan {
                generation: self.generation.fetch_add(1, Ordering::AcqRel) + 1,
                current: current_track,
                next: next_track,
                transition,
            })
            .await
    }

    pub(crate) async fn play(&self) -> Result<(), PipelineError> {
        let snapshot = self.pipeline.snapshot().await?;
        if snapshot.state == PipelineState::Stopped {
            self.replace_current().await
        } else {
            self.pipeline.set_playing(true).await
        }
    }

    pub(crate) async fn pause(&self) -> Result<(), PipelineError> {
        self.pipeline.set_playing(false).await
    }

    pub(crate) async fn stop(&self) -> Result<(), PipelineError> {
        self.pipeline.stop().await
    }

    pub(crate) async fn skip(&self) -> Result<(), PipelineError> {
        let Some(current) = self.queue.current_song_info() else {
            return self.pipeline.stop().await;
        };
        let current_key = TrackKey {
            queue_item_id: current.queue_item_id,
            song_id: current.song_id,
            position: current.position,
        };
        let Some(next) = self.queue.successor_after(&current_key) else {
            return self.pipeline.stop().await;
        };
        let next_key = TrackKey {
            queue_item_id: next.queue_item_id,
            song_id: next.song_id,
            position: next.position,
        };
        let _ = self.queue.commit_current(&next_key).await;
        self.replace_current().await?;
        self.publish_song_change();
        Ok(())
    }

    pub(crate) async fn reload(&self, songs: Vec<SongInfo>) -> Result<(), PipelineError> {
        self.queue.reload_songs(songs);
        Ok(())
    }

    fn publish_song_change(&self) {
        let idx = self.queue.current_song_index();
        if let Some(song) = self.queue.song_info(idx) {
            self.queue.publish_status(StatusEvent::SongChange {
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
        let PipelineSnapshot { state, elapsed } = self.pipeline.snapshot().await.unwrap_or(PipelineSnapshot {
            state: PipelineState::Stopped,
            elapsed: Duration::ZERO,
        });
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

    pub(crate) async fn status_json(&self) -> serde_json::Value {
        match self.status().await {
            StatusEvent::State {
                playing,
                song_index,
                total,
                elapsed,
                title,
                artist,
                duration,
            } => serde_json::json!({
                "playing": playing, "song_index": song_index, "total": total, "elapsed": elapsed,
                "title": title, "artist": artist, "duration": duration,
            }),
            StatusEvent::SongChange { .. } => unreachable!(),
        }
    }

    pub(crate) async fn is_playing(&self) -> bool {
        self.pipeline
            .snapshot()
            .await
            .map(|snapshot| snapshot.state == PipelineState::Playing)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::sync::{broadcast, mpsc};
    use uuid::Uuid;

    use super::*;

    struct FakePipeline {
        replacements: AtomicUsize,
        stops: AtomicUsize,
    }

    #[async_trait]
    impl PlaybackPipeline for FakePipeline {
        async fn replace(&self, _: PairPlan) -> Result<(), PipelineError> {
            self.replacements.fetch_add(1, Ordering::Release);
            Ok(())
        }

        async fn append(&self, _: PairPlan) -> Result<(), PipelineError> {
            Ok(())
        }

        async fn set_playing(&self, _: bool) -> Result<(), PipelineError> {
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
        let queue = Arc::new(QueueManager::new(
            sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://surcast:surcast@localhost:5433/surcast")
                .unwrap(),
            Uuid::new_v4(),
            String::new(),
            vec![song.clone()],
            0,
            status_tx,
            queue_tx,
        ));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let controller = Arc::new(StationController {
            queue,
            pipeline: pipeline.clone(),
            generation: AtomicU64::new(1),
        });
        let (events, receiver) = mpsc::unbounded_channel();
        controller.clone().start_events(receiver);
        events
            .send(PipelineEvent::DecodeFailed {
                generation: 0,
                track: StationController::track(song).key,
                message: "stale".into(),
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(25)).await;
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
        let queue = Arc::new(QueueManager::new(
            sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://surcast:surcast@localhost:5433/surcast")
                .unwrap(),
            Uuid::new_v4(),
            String::new(),
            vec![song.clone()],
            0,
            status_tx,
            queue_tx,
        ));
        let pipeline = Arc::new(FakePipeline {
            replacements: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        });
        let controller = Arc::new(StationController {
            queue,
            pipeline: pipeline.clone(),
            generation: AtomicU64::new(1),
        });
        let (events, receiver) = mpsc::unbounded_channel();
        controller.clone().start_events(receiver);
        events
            .send(PipelineEvent::CurrentEos {
                generation: 1,
                current: StationController::track(song).key,
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(pipeline.stops.load(Ordering::Acquire), 1);
        assert_eq!(pipeline.replacements.load(Ordering::Acquire), 0);
    }
}
