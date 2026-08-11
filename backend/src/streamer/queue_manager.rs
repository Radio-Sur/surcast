use std::sync::Mutex;

use sqlx::PgPool;
use uuid::Uuid;

use super::pipeline::TrackKey;
use super::{
    queue_repository::QueueRepository,
    queue_state::{QueueAnchor, QueueCursor, QueueState},
    SongInfo,
};


pub(crate) struct QueueManager {
    repository: QueueRepository,
    state: Mutex<QueueState>,
    dirty_cursor: Mutex<Option<(Option<Uuid>, QueueCursor)>>,
}

impl QueueManager {
    #[cfg(test)]
    pub fn new(db: PgPool, station_id: Uuid, upload_dir: String, songs: Vec<SongInfo>, initial_idx: usize) -> Self {
        Self {
            repository: QueueRepository::new(db, station_id, upload_dir),
            state: Mutex::new(QueueState::new(songs, initial_idx)),
            dirty_cursor: Mutex::new(None),
        }
    }
    pub fn new_with_cursor(db: PgPool, station_id: Uuid, upload_dir: String, songs: Vec<SongInfo>, cursor: QueueCursor) -> Self {
        Self {
            repository: QueueRepository::new(db, station_id, upload_dir),
            state: Mutex::new(QueueState::from_cursor(songs, cursor)),
            dirty_cursor: Mutex::new(None),
        }
    }

    pub(crate) fn station_id(&self) -> Uuid {
        self.repository.station_id()
    }

    async fn retry_dirty_cursor(&self) {
        let dirty = self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner()).clone();
        let Some((previous_current_queue_item_id, cursor)) = dirty else {
            return;
        };
        if self
            .repository
            .persist_cursor_if_current(previous_current_queue_item_id, &cursor)
            .await
            .is_ok()
        {
            let mut dirty = self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner());
            if dirty.as_ref() == Some(&(previous_current_queue_item_id, cursor)) {
                *dirty = None;
            }
        }
    }

    pub async fn reload_from_db(&self) {
        self.retry_dirty_cursor().await;
        let (songs, current_index) = self.repository.load().await;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.current_song_info().is_none() {
            *state = QueueState::new(songs, current_index);
        } else {
            state.replace(songs, true);
        }
    }

    pub fn reload_songs(&self, songs: Vec<SongInfo>, retain_missing_current: bool) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(songs, retain_missing_current);
    }

    pub async fn trim_played_items(&self) {
        self.repository.trim_played_items().await;
    }

    pub async fn queue_json(&self) -> String {
        self.repository.queue_json().await
    }

    pub fn current_song_index(&self) -> usize {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).current_song_index()
    }

    pub fn song_count(&self) -> usize {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).song_count()
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
    pub(crate) fn anchor_after_current(&self) -> QueueAnchor {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).anchor_after_current()
    }

    pub async fn commit_current(&self, key: &TrackKey, anchor: QueueAnchor) -> Option<TrackKey> {
        let (previous_current_queue_item_id, cursor, upcoming) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let previous_current_queue_item_id = state.current_song_info().map(|song| song.queue_item_id);
            let song = state.song_by_queue_item_id(key.queue_item_id)?;
            state.commit_current(song, anchor);
            (previous_current_queue_item_id, state.persistence_cursor(), state.upcoming())
        };
        if let Err(error) = self
            .repository
            .persist_cursor_if_current(previous_current_queue_item_id, &cursor)
            .await
        {
            tracing::warn!(station_id = %self.station_id(), %error, "deferring queue cursor persistence");
            *self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner()) = Some((previous_current_queue_item_id, cursor));
            return self.successor_after(key).map(track_key);
        }
        *self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner()) = None;
        self.repository.trim_played_items().await;
        self.repository.refill(upcoming).await;
        self.reload_from_db().await;
        self.successor_after(key).map(track_key)
    }
}

fn track_key(song: SongInfo) -> TrackKey {
    TrackKey {
        queue_item_id: song.queue_item_id,
        song_id: song.song_id,
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
            })
            .unwrap();

        assert_eq!(successor.queue_item_id, second_item);
    }

    fn state_commits_current_item_and_returns_the_successor() {
        let first = song(Uuid::new_v4(), Uuid::new_v4(), 1);
        let second = song(Uuid::new_v4(), Uuid::new_v4(), 2);
        let mut state = QueueState::new(vec![first, second.clone()], 0);

        let anchor = state.anchor_after_current();
        assert!(state.commit_current(second.clone(), anchor).is_none());
        assert_eq!(state.current_song_info().unwrap().queue_item_id, second.queue_item_id);
    }
}
