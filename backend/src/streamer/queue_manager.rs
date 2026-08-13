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
    refill_attempted_for: Mutex<Option<TrackKey>>,
}

impl QueueManager {
    #[cfg(test)]
    pub fn new(db: PgPool, station_id: Uuid, upload_dir: String, songs: Vec<SongInfo>, initial_idx: usize) -> Self {
        Self {
            repository: QueueRepository::new(db, station_id, upload_dir),
            state: Mutex::new(QueueState::new(songs, initial_idx)),
            dirty_cursor: Mutex::new(None),
            refill_attempted_for: Mutex::new(None),
        }
    }
    pub fn new_with_cursor(db: PgPool, station_id: Uuid, upload_dir: String, songs: Vec<SongInfo>, cursor: QueueCursor) -> Self {
        Self {
            repository: QueueRepository::new(db, station_id, upload_dir),
            state: Mutex::new(QueueState::from_cursor(songs, cursor)),
            dirty_cursor: Mutex::new(None),
            refill_attempted_for: Mutex::new(None),
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

    /// Ask AutoDJ / schedule fill to top the queue up from the locked database
    /// state. Returns `false` only when the fill call itself failed.
    pub(crate) async fn refill(&self) -> bool {
        self.repository.refill().await
    }

    pub async fn reload_from_db(&self) {
        self.retry_dirty_cursor().await;
        let (songs, _current_index) = self.repository.load().await;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.replace(songs, true);
    }

    pub fn reload_songs(&self, songs: Vec<SongInfo>, retain_missing_current: bool) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(songs, retain_missing_current);
    }

    pub async fn finish_current(&self) {
        let Some((previous_current_queue_item_id, cursor)) = ({
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.finish_current()
        }) else {
            return;
        };
        if let Err(error) = self
            .repository
            .persist_cursor_if_current(Some(previous_current_queue_item_id), &cursor)
            .await
        {
            tracing::warn!(station_id = %self.station_id(), %error, "deferring terminal queue cursor persistence");
            *self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner()) = Some((Some(previous_current_queue_item_id), cursor));
        } else {
            *self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner()) = None;
        }
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
        let (previous_current_queue_item_id, cursor) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let previous_current_queue_item_id = state.current_song_info().map(|song| song.queue_item_id);
            let song = state.song_by_queue_item_id(key.queue_item_id)?;
            state.commit_current(song, anchor);
            (previous_current_queue_item_id, state.persistence_cursor())
        };
        let owns_refill = reserve_refill(
            &mut self.refill_attempted_for.lock().unwrap_or_else(|error| error.into_inner()),
            key,
        );
        let result = if owns_refill {
            self.repository
                .commit_cursor_and_refill(previous_current_queue_item_id, &cursor)
                .await
        } else {
            self.repository
                .persist_cursor_if_current(previous_current_queue_item_id, &cursor)
                .await
        };
        if let Err(error) = result {
            if owns_refill {
                release_refill(
                    &mut self.refill_attempted_for.lock().unwrap_or_else(|error| error.into_inner()),
                    key,
                );
            }
            tracing::warn!(station_id = %self.station_id(), %error, "deferring queue cursor persistence");
            *self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner()) = Some((previous_current_queue_item_id, cursor));
            return self.successor_after(key).map(track_key);
        }
        *self.dirty_cursor.lock().unwrap_or_else(|error| error.into_inner()) = None;
        if owns_refill {
            self.reload_from_db().await;
        }
        self.successor_after(key).map(track_key)
    }
}

fn track_key(song: SongInfo) -> TrackKey {
    TrackKey {
        queue_item_id: song.queue_item_id,
        song_id: song.song_id,
    }
}

fn reserve_refill(attempted_for: &mut Option<TrackKey>, target: &TrackKey) -> bool {
    if attempted_for.as_ref() == Some(target) {
        false
    } else {
        *attempted_for = Some(target.clone());
        true
    }
}
fn release_refill(attempted_for: &mut Option<TrackKey>, target: &TrackKey) {
    if attempted_for.as_ref() == Some(target) {
        *attempted_for = None;
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

    #[test]
    fn state_commits_current_item_and_returns_the_successor() {
        let first = song(Uuid::new_v4(), Uuid::new_v4(), 1);
        let second = song(Uuid::new_v4(), Uuid::new_v4(), 2);
        let mut state = QueueState::new(vec![first, second.clone()], 0);

        let anchor = state.anchor_after_current();
        assert!(state.commit_current(second.clone(), anchor).is_none());
        assert_eq!(state.current_song_info().unwrap().queue_item_id, second.queue_item_id);
    }

    #[test]
    fn refill_is_attempted_once_per_pair_target() {
        let target = TrackKey {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
        };
        let replacement = TrackKey {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
        };
        let mut attempted_for = None;

        assert!(reserve_refill(&mut attempted_for, &target));
        assert!(!reserve_refill(&mut attempted_for, &target));
        release_refill(&mut attempted_for, &target);
        assert!(reserve_refill(&mut attempted_for, &target));
        release_refill(&mut attempted_for, &replacement);
        assert!(!reserve_refill(&mut attempted_for, &target));
        assert!(reserve_refill(&mut attempted_for, &replacement));
    }
}
