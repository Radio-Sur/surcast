pub mod controller;
pub mod gstreamer;
pub mod pipeline;
pub mod queue_manager;
pub mod queue_repository;
pub mod queue_state;
pub mod runtime;

use serde::Serialize;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

use self::controller::StationController;
use self::pipeline::{PipelineError, PlaybackPipelineFactory, StationPlaybackConfig};
use self::queue_manager::QueueManager;
use self::runtime::StationRuntime;

#[derive(Clone, Serialize, Debug)]
pub struct SongInfo {
    pub queue_item_id: Uuid,
    pub song_id: Uuid,
    pub title: String,
    pub artist: String,
    pub duration: i32,
    pub file_path: String,
    pub position: i32,
    pub cue_in: f64,
    pub cue_out: f64,
    pub cross_start_next: f64,
    pub analyzed: bool,
}

#[derive(Clone, Serialize, Debug)]
#[serde(tag = "type", content = "data")]
pub enum StatusEvent {
    #[serde(rename = "state")]
    State {
        playing: bool,
        song_index: usize,
        total: usize,
        elapsed: u64,
        title: String,
        artist: String,
        duration: i32,
    },
    #[serde(rename = "song_change")]
    SongChange {
        song_index: usize,
        total: usize,
        elapsed: u64,
        title: String,
        artist: String,
        duration: i32,
    },
}

pub struct StationStreamer {
    runtime: StationRuntime,
    queue: Arc<QueueManager>,
}

impl StationStreamer {
    pub(crate) async fn new(
        songs: Vec<SongInfo>,
        mount: &str,
        station_id: Uuid,
        db: PgPool,
        prebuffer_bytes: i32,
        upload_dir: &str,
        factory: Arc<dyn PlaybackPipelineFactory>,
    ) -> Result<Arc<Self>, PipelineError> {
        let (status_tx, _) = broadcast::channel(64);
        let (queue_tx, _) = broadcast::channel(64);
        let saved_index = sqlx::query_scalar::<_, i32>("SELECT current_song_index FROM stations WHERE id = $1")
            .bind(station_id)
            .fetch_optional(&db)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
            .max(0);
        let initial_idx = songs.iter().position(|song| song.position >= saved_index).unwrap_or(songs.len());
        let queue = Arc::new(QueueManager::new(
            db.clone(),
            station_id,
            upload_dir.into(),
            songs,
            initial_idx,
            status_tx,
            queue_tx,
        ));
        let (controller, events) = StationController::new(queue.clone(), db, mount, prebuffer_bytes, factory).await?;
        let runtime = StationRuntime::spawn(controller, events);
        Ok(Arc::new(Self { runtime, queue }))
    }

    pub(crate) async fn skip(&self) -> Result<(), PipelineError> {
        self.runtime.skip().await
    }
    pub(crate) async fn play(&self) -> Result<(), PipelineError> {
        self.runtime.play().await
    }
    pub(crate) async fn pause(&self) -> Result<(), PipelineError> {
        self.runtime.pause().await
    }
    pub(crate) async fn stop(&self) -> Result<(), PipelineError> {
        self.runtime.stop().await
    }

    pub(crate) async fn reconnect(&self) -> Result<(), PipelineError> {
        self.runtime.reconnect().await
    }

    pub async fn shutdown(&self) {
        let _ = self.stop().await;
    }
    pub(crate) async fn status(&self) -> StatusEvent {
        self.runtime.status().await.unwrap_or_else(|_| {
            let song_index = self.queue.current_song_index();
            let song = self.queue.song_info(song_index);
            StatusEvent::State {
                playing: false,
                song_index,
                total: self.queue.song_count(),
                elapsed: 0,
                title: song.as_ref().map_or_else(String::new, |song| song.title.clone()),
                artist: song.as_ref().map_or_else(String::new, |song| song.artist.clone()),
                duration: song.map_or(0, |song| song.duration),
            }
        })
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
    pub(crate) fn current_song_index(&self) -> usize {
        self.queue.current_song_index()
    }
    pub(crate) fn subscribe_status(&self) -> broadcast::Receiver<StatusEvent> {
        self.queue.subscribe_status()
    }
    pub(crate) fn subscribe_queue(&self) -> broadcast::Receiver<String> {
        self.queue.subscribe_queue()
    }
    pub(crate) async fn push_queue_update(&self) {
        self.runtime.push_queue_update().await;
    }
    pub(crate) async fn trim_played_items(&self) {
        self.runtime.trim_played_items().await;
    }
    pub(crate) async fn reload_songs(&self, songs: Vec<SongInfo>) -> Result<(), PipelineError> {
        self.runtime.reload(songs).await
    }

    pub(crate) async fn update_config(&self, config: StationPlaybackConfig) -> Result<(), PipelineError> {
        self.runtime.update_config(config).await
    }
}
