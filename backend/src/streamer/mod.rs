pub mod backend;
pub mod connection;
pub mod crossfade;
pub mod engine;
pub mod queue_manager;

use serde::Serialize;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use uuid::Uuid;

use self::backend::StreamBackend;
use self::engine::PlaybackEngine;
use self::queue_manager::QueueManager;

#[derive(Clone, Serialize, Debug)]
pub struct SongInfo {
    pub song_id: String,
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

pub struct PlaybackParams {
    pub total_chunks: usize,
    pub pre_idx: usize,
    pub chunk_size: usize,
    pub chunk_duration: Duration,
    pub bitrate: f64,
    pub prebuffer_chunks: usize,
    pub fade_secs: f64,
    pub fade_chunks: usize,
    pub has_fade: bool,
    pub actual_fade: f64,
    pub cur_start: f64,
    pub cur_cut: f64,
    pub cur_end: f64,
    pub next_start: f64,
    pub next: Option<SongInfo>,
}

pub struct StationStreamer {
    pub engine: Arc<PlaybackEngine>,
    pub queue: Arc<QueueManager>,
}

impl StationStreamer {
    pub async fn new(
        songs: Vec<SongInfo>,
        mount: &str,
        station_id: Uuid,
        db: PgPool,
        prebuffer_bytes: i32,
        upload_dir: &str,
        backend: Arc<dyn StreamBackend>,
    ) -> Arc<Self> {
        let (status_tx, _) = broadcast::channel(64);
        let (queue_tx, _) = broadcast::channel(64);
        let total = songs.len();

        let saved_index = sqlx::query_scalar::<_, i32>("SELECT current_song_index FROM stations WHERE id = $1")
            .bind(station_id)
            .fetch_optional(&db)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
            .max(0) as i32;

        let initial_idx = songs.iter().position(|s| s.position >= saved_index).unwrap_or(songs.len());

        let queue = Arc::new(QueueManager::new(
            db.clone(),
            station_id,
            upload_dir.to_string(),
            songs,
            initial_idx,
            status_tx.clone(),
            queue_tx,
        ));

        let engine = Arc::new(PlaybackEngine::new(
            db,
            station_id,
            mount.to_string(),
            prebuffer_bytes,
            queue.clone(),
            backend,
        ));

        let this = Arc::new(Self {
            engine: engine.clone(),
            queue: queue.clone(),
        });

        if status_tx
            .send(StatusEvent::State {
                playing: true,
                song_index: initial_idx,
                total,
                elapsed: 0,
                title: queue.song_info(initial_idx).map_or("".into(), |s| s.title),
                artist: queue.song_info(initial_idx).map_or("".into(), |s| s.artist),
                duration: queue.song_info(initial_idx).map_or(0, |s| s.duration),
            })
            .is_err()
        {
            tracing::debug!("No status listeners for station {}", station_id);
        }

        tokio::spawn(async move {
            engine.run_playback_loop().await;
        });

        this
    }

    pub async fn skip(&self) {
        self.queue.advance_idx(1);
        self.queue.advance_song().await;
        let idx = self.queue.current_song_index();
        if let Some(info) = self.queue.song_info(idx) {
            self.engine.mark_song_started();
            let total = self.queue.song_count();
            if self
                .queue
                .status_tx
                .send(StatusEvent::SongChange {
                    song_index: idx,
                    total,
                    elapsed: 0,
                    title: info.title.clone(),
                    artist: info.artist.clone(),
                    duration: info.duration,
                })
                .is_err()
            {
                tracing::debug!("No status listeners for station {}", info.title);
            }
        }
    }

    pub fn play(&self) {
        self.engine.set_playing(true);
        let total = self.queue.song_count();
        if let Some(info) = self.queue.song_info(self.queue.current_song_index()) {
            let station_id = self.queue.station_id;
            if self
                .queue
                .status_tx
                .send(StatusEvent::State {
                    playing: true,
                    song_index: self.queue.current_song_index(),
                    total,
                    elapsed: self.engine.elapsed_secs(),
                    title: info.title,
                    artist: info.artist,
                    duration: info.duration,
                })
                .is_err()
            {
                tracing::debug!("No status listeners for station {station_id}");
            }
        }
    }

    pub fn pause(&self) {
        self.engine.set_playing(false);
        let total = self.queue.song_count();
        if let Some(info) = self.queue.song_info(self.queue.current_song_index()) {
            let station_id = self.queue.station_id;
            if self
                .queue
                .status_tx
                .send(StatusEvent::State {
                    playing: false,
                    song_index: self.queue.current_song_index(),
                    total,
                    elapsed: self.engine.elapsed_secs(),
                    title: info.title,
                    artist: info.artist,
                    duration: info.duration,
                })
                .is_err()
            {
                tracing::debug!("No status listeners for station {station_id}");
            }
        }
    }

    pub fn stop(&self) {
        self.engine.set_stopped(true);
        self.engine.set_playing(false);
    }

    pub fn status(&self) -> StatusEvent {
        let len = self.queue.song_count();
        let playing = self.engine.is_playing();
        let idx = self.queue.current_song_index();
        StatusEvent::State {
            playing,
            song_index: idx,
            total: len,
            elapsed: self.engine.elapsed_secs(),
            title: self.queue.song_info(idx).map_or("".into(), |s| s.title),
            artist: self.queue.song_info(idx).map_or("".into(), |s| s.artist),
            duration: self.queue.song_info(idx).map_or(0, |s| s.duration),
        }
    }

    pub fn status_json(&self) -> serde_json::Value {
        let playing = self.engine.is_playing();
        let idx = self.queue.current_song_index();
        let info = self.queue.song_info(idx);
        serde_json::json!({
            "playing": playing,
            "song_index": idx,
            "total": self.queue.song_count(),
            "elapsed": self.engine.elapsed_secs(),
            "title": info.as_ref().map_or("", |s| &s.title),
            "artist": info.as_ref().map_or("", |s| &s.artist),
            "duration": info.map_or(0, |s| s.duration),
        })
    }

    pub fn is_playing(&self) -> bool {
        self.engine.is_playing()
    }

    pub fn current_song_index(&self) -> usize {
        self.queue.current_song_index()
    }

    pub fn song_count(&self) -> usize {
        self.queue.song_count()
    }

    pub fn subscribe_status(&self) -> broadcast::Receiver<StatusEvent> {
        self.queue.subscribe_status()
    }

    pub fn subscribe_queue(&self) -> broadcast::Receiver<String> {
        self.queue.subscribe_queue()
    }

    pub fn publish_status(&self, event: StatusEvent) {
        self.queue.publish_status(event);
    }

    pub async fn push_queue_update(&self) {
        self.queue.push_queue_update().await;
    }

    pub async fn trim_played_items(&self) {
        self.queue.trim_played_items().await;
    }

    pub fn reload_songs(&self, new_songs: Vec<SongInfo>) {
        self.queue.reload_songs(new_songs);
    }
}
