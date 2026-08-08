pub mod controller;
pub mod gstreamer;
pub mod pipeline;
pub mod queue_manager;

use serde::Serialize;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

use self::controller::StationController;
use self::pipeline::{PipelineError, PlaybackPipelineFactory};
use self::queue_manager::QueueManager;

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
    controller: Arc<StationController>,
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
        let controller = Arc::new(controller);
        controller.clone().start_events(events);
        Ok(Arc::new(Self { controller, queue }))
    }

    pub(crate) async fn skip(&self) -> Result<(), PipelineError> {
        self.controller.skip().await
    }
    pub(crate) async fn play(&self) -> Result<(), PipelineError> {
        self.controller.play().await
    }
    pub(crate) async fn pause(&self) -> Result<(), PipelineError> {
        self.controller.pause().await
    }
    pub(crate) async fn stop(&self) -> Result<(), PipelineError> {
        self.controller.stop().await
    }

    pub async fn shutdown(&self) {
        let _ = self.stop().await;
    }
    pub(crate) async fn status(&self) -> StatusEvent {
        self.controller.status().await
    }
    pub(crate) async fn status_json(&self) -> serde_json::Value {
        self.controller.status_json().await
    }
    pub(crate) async fn is_playing(&self) -> bool {
        self.controller.is_playing().await
    }
    pub(crate) fn current_song_index(&self) -> usize {
        self.queue.current_song_index()
    }
    pub(crate) fn song_count(&self) -> usize {
        self.queue.song_count()
    }
    pub(crate) fn subscribe_status(&self) -> broadcast::Receiver<StatusEvent> {
        self.queue.subscribe_status()
    }
    pub(crate) fn subscribe_queue(&self) -> broadcast::Receiver<String> {
        self.queue.subscribe_queue()
    }
    pub(crate) fn publish_status(&self, event: StatusEvent) {
        self.queue.publish_status(event)
    }
    pub(crate) async fn push_queue_update(&self) {
        self.queue.push_queue_update().await
    }
    pub(crate) async fn trim_played_items(&self) {
        self.queue.trim_played_items().await
    }
    pub(crate) async fn reload_songs(&self, songs: Vec<SongInfo>) -> Result<(), PipelineError> {
        self.controller.reload(songs).await
    }
}
