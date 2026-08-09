use std::sync::Mutex;

use sqlx::PgPool;
use tokio::sync::broadcast;
use uuid::Uuid;

use super::pipeline::TrackKey;
use super::{queue_repository::QueueRepository, queue_state::QueueState, SongInfo, StatusEvent};


pub(crate) struct QueueManager {
    repository: QueueRepository,
    state: Mutex<QueueState>,
    status_tx: broadcast::Sender<StatusEvent>,
    queue_tx: broadcast::Sender<String>,
}

impl QueueManager {
    pub fn new(
        db: PgPool,
        station_id: Uuid,
        upload_dir: String,
        songs: Vec<SongInfo>,
        initial_idx: usize,
        status_tx: broadcast::Sender<StatusEvent>,
        queue_tx: broadcast::Sender<String>,
    ) -> Self {
        Self {
            repository: QueueRepository::new(db, station_id, upload_dir),
            state: Mutex::new(QueueState::new(songs, initial_idx)),
            status_tx,
            queue_tx,
        }
    }

    pub(crate) fn station_id(&self) -> Uuid {
        self.repository.station_id()
    }

    pub async fn reload_from_db(&self) {
        let (songs, current_index) = self.repository.load().await;
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace_with_current_index(songs, current_index);
    }

    pub fn reload_songs(&self, songs: Vec<SongInfo>) {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).replace(songs);
    }

    pub async fn trim_played_items(&self) {
        self.repository.trim_played_items().await;
    }

    pub fn subscribe_status(&self) -> broadcast::Receiver<StatusEvent> {
        self.status_tx.subscribe()
    }

    pub fn publish_status(&self, event: StatusEvent) {
        if self.status_tx.send(event).is_err() {
            tracing::debug!(station_id = %self.station_id(), "no status listeners");
        }
    }

    pub fn subscribe_queue(&self) -> broadcast::Receiver<String> {
        self.queue_tx.subscribe()
    }

    pub async fn push_queue_update(&self) {
        let message = self.repository.queue_json().await;
        if self.queue_tx.send(message).is_err() {
            tracing::debug!(station_id = %self.station_id(), "no queue listeners");
        }
    }

    pub fn current_song_index(&self) -> usize {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).current_song_index()
    }

    pub fn song_count(&self) -> usize {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).song_count()
    }

    pub fn song_info(&self, index: usize) -> Option<SongInfo> {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).song_info(index)
    }

    pub fn current_song_info(&self) -> Option<SongInfo> {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).current_song_info()
    }

    pub fn peek_next_song(&self) -> Option<SongInfo> {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).peek_next_song()
    }

    pub fn successor_after(&self, key: &TrackKey) -> Option<SongInfo> {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).successor_after(key)
    }

    pub async fn commit_current(&self, key: &TrackKey) -> Option<TrackKey> {
        let (position, upcoming) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.commit_current(key);
            (state.current_position(key.position), state.upcoming_after(key))
        };
        self.repository.persist_current(position).await;
        self.repository.trim_played_items().await;
        self.repository.refill(upcoming).await;
        self.reload_from_db().await;
        self.push_queue_update().await;
        self.successor_after(key).map(track_key)
    }
}

fn track_key(song: SongInfo) -> TrackKey {
    TrackKey {
        queue_item_id: song.queue_item_id,
        song_id: song.song_id,
        position: song.position,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn song(queue_item_id: Uuid, song_id: Uuid, position: i32) -> SongInfo {
        SongInfo {
            queue_item_id,
            song_id,
            title: String::new(),
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

    #[test]
    fn successor_uses_queue_item_identity_for_duplicate_songs() {
        let repeated_song = Uuid::new_v4();
        let first_item = Uuid::new_v4();
        let second_item = Uuid::new_v4();
        let state = QueueState::new(
            vec![
                song(first_item, repeated_song, 1),
                song(second_item, repeated_song, 2),
                song(Uuid::new_v4(), Uuid::new_v4(), 3),
            ],
            0,
        );

        let successor = state
            .successor_after(&TrackKey {
                queue_item_id: first_item,
                song_id: repeated_song,
                position: 1,
            })
            .unwrap();

        assert_eq!(successor.queue_item_id, second_item);
    }

    fn state_commits_current_item_and_returns_the_successor() {
        let first = song(Uuid::new_v4(), Uuid::new_v4(), 1);
        let second = song(Uuid::new_v4(), Uuid::new_v4(), 2);
        let mut state = QueueState::new(vec![first, second.clone()], 0);

        assert_eq!(
            state.commit_current(&TrackKey {
                queue_item_id: second.queue_item_id,
                song_id: second.song_id,
                position: second.position,
            }),
            None
        );
        assert_eq!(state.current_song_info().unwrap().queue_item_id, second.queue_item_id);
    }
}
