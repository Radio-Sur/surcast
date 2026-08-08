use sqlx::PgPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::broadcast;
use uuid::Uuid;

use super::SongInfo;
use super::StatusEvent;
use crate::stations::repository;

/// Resolves the "now playing" index after a queue reload. Queue item identity
/// survives duplicate songs and queue reordering; if the active item was
/// removed, resume at the saved queue position or leave the engine at the end.
pub(crate) fn resolve_index(anchor: Option<Uuid>, songs: &[SongInfo], saved_position: i32) -> usize {
    anchor
        .and_then(|queue_item_id| songs.iter().position(|song| song.queue_item_id == queue_item_id))
        .or_else(|| songs.iter().position(|song| song.position >= saved_position))
        .unwrap_or(songs.len())
}

pub(crate) struct QueueManager {
    pub(crate) db: PgPool,
    pub(crate) station_id: Uuid,
    pub(crate) upload_dir: String,
    pub(crate) songs: Mutex<Vec<SongInfo>>,
    pub(crate) current_idx: AtomicUsize,
    pub(crate) status_tx: broadcast::Sender<StatusEvent>,
    pub(crate) queue_tx: broadcast::Sender<String>,
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
            db,
            station_id,
            upload_dir,
            songs: Mutex::new(songs),
            current_idx: AtomicUsize::new(initial_idx),
            status_tx,
            queue_tx,
        }
    }

    pub async fn reload_from_db(&self) {
        let anchor = {
            let songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            let idx = self.current_idx.load(Ordering::Acquire);
            songs.get(idx).map(|song| song.queue_item_id)
        };

        let rows = repository::find_station_song_info(&self.db, self.station_id)
            .await
            .unwrap_or_default();
        let songs: Vec<SongInfo> = rows
            .into_iter()
            .map(
                |(file_path, title, artist, duration, queue_item_id, song_id, position, cue_in, cue_out, cross_start_next, analyzed)| {
                    SongInfo {
                        file_path: crate::songs::handlers::resolve_audio_path(&self.upload_dir, &file_path),
                        title,
                        artist,
                        duration,
                        queue_item_id,
                        song_id,
                        position,
                        cue_in,
                        cue_out,
                        cross_start_next,
                        analyzed,
                    }
                },
            )
            .collect();

        let saved_index = sqlx::query_scalar::<_, i32>("SELECT current_song_index FROM stations WHERE id = $1")
            .bind(self.station_id)
            .fetch_optional(&self.db)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
            .max(0);

        let new_idx = resolve_index(anchor, &songs, saved_index);

        {
            let mut list = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            *list = songs;
        }
        self.current_idx.store(new_idx, Ordering::Release);
    }

    /// Replaces the in-memory song list after queue edits while retaining the
    /// active queue item when it still exists.
    pub fn reload_songs(&self, new_songs: Vec<SongInfo>) {
        let anchor = {
            let songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            let idx = self.current_idx.load(Ordering::Acquire);
            songs.get(idx).map(|song| song.queue_item_id)
        };
        let new_idx = resolve_index(anchor, &new_songs, 0);

        let mut songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
        *songs = new_songs;
        self.current_idx.store(new_idx, Ordering::Release);
    }

    pub async fn trim_played_items(&self) {
        let row: Option<(i32, i32)> = sqlx::query_as("SELECT current_song_index, played_limit FROM stations WHERE id = $1")
            .bind(self.station_id)
            .fetch_optional(&self.db)
            .await
            .ok()
            .flatten();

        let (current_idx, played_limit) = match row {
            Some(r) => r,
            None => return,
        };

        if played_limit <= 0 || current_idx <= 0 {
            return;
        }

        let played_items: Vec<(Uuid, Option<Uuid>)> =
            sqlx::query_as("SELECT id, origin_playlist_id FROM station_queue WHERE station_id = $1 AND position < $2 ORDER BY position")
                .bind(self.station_id)
                .bind(current_idx)
                .fetch_all(&self.db)
                .await
                .unwrap_or_default();

        if played_items.is_empty() {
            return;
        }

        let mut visible = 0i32;
        let mut i = 0;
        while i < played_items.len() {
            visible += 1;
            if let Some(pid) = played_items[i].1 {
                i += 1;
                while i < played_items.len() && played_items[i].1 == Some(pid) {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        if visible <= played_limit {
            return;
        }

        let to_remove = visible - played_limit;
        let mut removed = 0i32;
        let mut delete_ids: Vec<Uuid> = Vec::new();
        let mut i = 0;

        while i < played_items.len() && removed < to_remove {
            let item = &played_items[i];
            delete_ids.push(item.0);
            i += 1;
            removed += 1;

            if let Some(pid) = item.1 {
                while i < played_items.len() && played_items[i].1 == Some(pid) {
                    delete_ids.push(played_items[i].0);
                    i += 1;
                }
            }
        }

        if delete_ids.is_empty() {
            return;
        }

        for id in &delete_ids {
            if let Err(e) = sqlx::query("DELETE FROM station_queue WHERE id = $1 AND station_id = $2")
                .bind(id)
                .bind(self.station_id)
                .execute(&self.db)
                .await
            {
                tracing::warn!("Failed to clean up queue items: {e}");
            }
        }
    }

    pub fn subscribe_status(&self) -> broadcast::Receiver<StatusEvent> {
        self.status_tx.subscribe()
    }

    pub fn publish_status(&self, event: StatusEvent) {
        if self.status_tx.send(event).is_err() {
            tracing::debug!("No status listeners for station {}", self.station_id);
        }
    }

    pub fn subscribe_queue(&self) -> broadcast::Receiver<String> {
        self.queue_tx.subscribe()
    }

    pub async fn push_queue_update(&self) {
        let items: Vec<serde_json::Value> = sqlx::query_as::<
            _,
            (
                Uuid,
                Uuid,
                Uuid,
                i32,
                String,
                String,
                String,
                i32,
                String,
                String,
                Option<Uuid>,
                Option<String>,
                bool,
            ),
        >(
            r#"SELECT sq.id, sq.station_id, sq.song_id, sq.position,
                      s.title, s.artist, s.album, s.duration, s.mime_type, s.cover_path,
                      sq.origin_playlist_id, p.name as playlist_name, sq.is_auto_dj
               FROM station_queue sq
               JOIN songs s ON s.id = sq.song_id
               LEFT JOIN playlists p ON p.id = sq.origin_playlist_id
               WHERE sq.station_id = $1
               ORDER BY sq.position"#,
        )
        .bind(self.station_id)
        .fetch_all(&self.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(
            |(
                id,
                station_id,
                song_id,
                position,
                title,
                artist,
                album,
                duration,
                mime_type,
                cover_path,
                origin_playlist_id,
                playlist_name,
                is_auto_dj,
            )| {
                serde_json::json!({
                    "id": id,
                    "station_id": station_id,
                    "song_id": song_id,
                    "position": position,
                    "title": title,
                    "artist": artist,
                    "album": album,
                    "duration": duration,
                    "has_cover": !cover_path.is_empty(),
                    "mime_type": mime_type,
                    "origin_playlist_id": origin_playlist_id,
                    "playlist_name": playlist_name,
                    "is_auto_dj": is_auto_dj,
                })
            },
        )
        .collect();

        let msg = serde_json::to_string(&items).unwrap_or_else(|_| "[]".into());
        if self.queue_tx.send(msg).is_err() {
            tracing::debug!("No queue listeners for station {}", self.station_id);
        }
    }

    pub fn current_song_index(&self) -> usize {
        let songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
        let len = songs.len();
        if len == 0 {
            return 0;
        }
        self.current_idx.load(Ordering::Acquire).min(len - 1)
    }

    pub fn song_count(&self) -> usize {
        self.songs.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn song_info(&self, idx: usize) -> Option<SongInfo> {
        self.songs.lock().unwrap_or_else(|e| e.into_inner()).get(idx).cloned()
    }

    pub fn current_song_info(&self) -> Option<SongInfo> {
        let idx = self.current_idx.load(Ordering::Acquire);
        self.song_info(idx)
    }

    pub fn peek_next_song(&self) -> Option<SongInfo> {
        let idx = self.current_idx.load(Ordering::Acquire);
        self.song_info(idx + 1)
    }

    fn successor_in(songs: &[SongInfo], key: &super::pipeline::TrackKey) -> Option<SongInfo> {
        let start = songs
            .iter()
            .position(|song| song.queue_item_id == key.queue_item_id)
            .map(|index| index + 1)
            .unwrap_or_else(|| songs.iter().position(|song| song.position > key.position).unwrap_or(songs.len()));
        songs.get(start).cloned()
    }

    pub fn successor_after(&self, key: &super::pipeline::TrackKey) -> Option<SongInfo> {
        let songs = self.songs.lock().unwrap_or_else(|error| error.into_inner());
        Self::successor_in(&songs, key)
    }

    pub async fn commit_current(&self, key: &super::pipeline::TrackKey) -> Option<super::pipeline::TrackKey> {
        let position = {
            let songs = self.songs.lock().unwrap_or_else(|error| error.into_inner());
            let index = songs.iter().position(|song| song.queue_item_id == key.queue_item_id);
            if let Some(index) = index {
                self.current_idx.store(index, Ordering::Release);
            }
            index
                .and_then(|index| songs.get(index).map(|song| song.position))
                .unwrap_or(key.position)
        };

        if let Err(error) = sqlx::query("UPDATE stations SET current_song_index = $1 WHERE id = $2")
            .bind(position)
            .bind(self.station_id)
            .execute(&self.db)
            .await
        {
            tracing::warn!(station_id = %self.station_id, %error, "failed to persist current queue item");
        }
        self.trim_played_items().await;

        let upcoming = {
            let songs = self.songs.lock().unwrap_or_else(|error| error.into_inner());
            let start = songs
                .iter()
                .position(|song| song.queue_item_id == key.queue_item_id)
                .map(|index| index + 1)
                .unwrap_or_else(|| songs.iter().position(|song| song.position > key.position).unwrap_or(songs.len()));
            songs.len().saturating_sub(start) as i64
        };
        if let Err(error) =
            crate::scheduling::service::fill_queue_from_schedule(&self.db, self.station_id, Some(upcoming), &self.upload_dir).await
        {
            tracing::warn!(station_id = %self.station_id, %error, "AutoDJ successor refill error");
        }
        self.reload_from_db().await;
        self.push_queue_update().await;
        self.successor_after(key).map(|song| super::pipeline::TrackKey {
            queue_item_id: song.queue_item_id,
            song_id: song.song_id,
            position: song.position,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamer::pipeline::TrackKey;

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
        let songs = vec![
            song(first_item, repeated_song, 1),
            song(second_item, repeated_song, 2),
            song(Uuid::new_v4(), Uuid::new_v4(), 3),
        ];

        let successor = QueueManager::successor_in(
            &songs,
            &TrackKey {
                queue_item_id: first_item,
                song_id: repeated_song,
                position: 1,
            },
        )
        .unwrap();

        assert_eq!(successor.queue_item_id, second_item);
    }

    #[test]
    fn resolve_index_keeps_active_queue_item_after_reorder() {
        let repeated_song = Uuid::new_v4();
        let active_item = Uuid::new_v4();
        let songs = vec![
            song(Uuid::new_v4(), Uuid::new_v4(), 0),
            song(active_item, repeated_song, 1),
            song(Uuid::new_v4(), repeated_song, 2),
        ];

        assert_eq!(resolve_index(Some(active_item), &songs, 0), 1);
    }
}
