use sqlx::PgPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::broadcast;
use uuid::Uuid;

use super::SongInfo;
use super::StatusEvent;
use crate::stations::repository;

/// Resolves the "now playing" index for a freshly loaded song list. Keeps the
/// pointer anchored on the song that was playing (by `song_id` and its
/// occurrence rank, so duplicated entries resolve uniquely) across queue edits
/// (reorder / insert / remove). When that song is gone, falls back to the first
/// song whose `position` is at or after `saved_position`, then to the end of the
/// list (so the engine idles and refills).
pub(crate) fn resolve_index(anchor: Option<(&str, usize)>, songs: &[SongInfo], saved_position: i32) -> usize {
    if let Some((id, rank)) = anchor {
        if rank > 0 {
            let mut seen = 0usize;
            for (i, s) in songs.iter().enumerate() {
                if s.song_id == id {
                    seen += 1;
                    if seen == rank {
                        return i;
                    }
                }
            }
        }
    }
    songs.iter().position(|s| s.position >= saved_position).unwrap_or(songs.len())
}

pub struct QueueManager {
    pub db: PgPool,
    pub station_id: Uuid,
    pub upload_dir: String,
    pub songs: Mutex<Vec<SongInfo>>,
    pub current_idx: AtomicUsize,
    pub status_tx: broadcast::Sender<StatusEvent>,
    pub queue_tx: broadcast::Sender<String>,
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

    pub async fn advance_song(&self) {
        self.persist_index().await;
        self.trim_played_items().await;
        self.reload_from_db().await;

        let upcoming = {
            let songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            songs.len().saturating_sub(self.current_idx.load(Ordering::Acquire) + 1) as i64
        };

        if let Err(e) =
            crate::scheduling::service::fill_queue_from_schedule(&self.db, self.station_id, Some(upcoming), &self.upload_dir).await
        {
            tracing::warn!(station_id = %self.station_id, error = ?e, "AutoDJ advance error");
        }
        self.reload_from_db().await;
        self.push_queue_update().await;
    }

    pub async fn reload_from_db(&self) {
        let anchor = {
            let songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            let idx = self.current_idx.load(Ordering::Acquire);
            songs.get(idx).map(|s| {
                let rank = songs[..=idx].iter().filter(|x| x.song_id == s.song_id).count();
                (s.song_id.clone(), rank)
            })
        };
        let anchor_ref = anchor.as_ref().map(|(id, rank)| (id.as_str(), *rank));

        let rows = repository::find_station_song_info(&self.db, self.station_id)
            .await
            .unwrap_or_default();
        let songs: Vec<SongInfo> = rows
            .into_iter()
            .map(
                |(file_path, title, artist, duration, song_id, position, cue_in, cue_out, cross_start_next, analyzed)| SongInfo {
                    file_path: crate::songs::handlers::resolve_audio_path(&self.upload_dir, &file_path),
                    title,
                    artist,
                    duration,
                    song_id,
                    position,
                    cue_in,
                    cue_out,
                    cross_start_next,
                    analyzed,
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

        let new_idx = resolve_index(anchor_ref, &songs, saved_index);

        {
            let mut list = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            *list = songs;
        }
        self.current_idx.store(new_idx, Ordering::Release);
    }

    /// Replaces the in-memory song list (used after queue edits) while keeping
    /// the "now playing" pointer anchored on the same song (id + duplicate rank).
    pub fn reload_songs(&self, new_songs: Vec<SongInfo>) {
        let anchor = {
            let songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            let idx = self.current_idx.load(Ordering::Acquire);
            songs.get(idx).map(|s| {
                let rank = songs[..=idx].iter().filter(|x| x.song_id == s.song_id).count();
                (s.song_id.clone(), rank)
            })
        };
        let anchor_ref = anchor.as_ref().map(|(id, rank)| (id.as_str(), *rank));
        let new_idx = resolve_index(anchor_ref, &new_songs, 0);

        let mut songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
        *songs = new_songs;
        self.current_idx.store(new_idx, Ordering::Release);
    }

    async fn persist_index(&self) {
        let saved_index = sqlx::query_scalar::<_, i32>("SELECT current_song_index FROM stations WHERE id = $1")
            .bind(self.station_id)
            .fetch_optional(&self.db)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);

        let idx = self.current_idx.load(Ordering::Acquire);
        let position = {
            let songs = self.songs.lock().unwrap_or_else(|e| e.into_inner());
            songs
                .get(idx)
                .map(|s| s.position)
                .unwrap_or_else(|| songs.last().map(|s| s.position + 1).unwrap_or(saved_index))
        };

        if let Err(e) = sqlx::query("UPDATE stations SET current_song_index = $1 WHERE id = $2")
            .bind(position)
            .bind(self.station_id)
            .execute(&self.db)
            .await
        {
            tracing::warn!("Failed to persist current_song_index: {e}");
        }
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

    pub fn current_idx(&self) -> usize {
        self.current_idx.load(Ordering::Acquire)
    }

    pub fn advance_idx(&self, delta: usize) {
        self.current_idx.fetch_add(delta, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_index;
    use super::SongInfo;

    fn song(id: &str, position: i32) -> SongInfo {
        SongInfo {
            song_id: id.into(),
            title: "t".into(),
            artist: "a".into(),
            duration: 10,
            file_path: "/tmp/x.mp3".into(),
            position,
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            analyzed: false,
        }
    }

    #[test]
    fn test_resolve_index_keeps_same_song_after_reorder() {
        // playing A; user moved C to the top -> [C, A, B]
        let songs = vec![song("C", 0), song("A", 1), song("B", 2)];
        assert_eq!(resolve_index(Some(("A", 1)), &songs, 0), 1);
        assert_eq!(resolve_index(Some(("B", 1)), &songs, 0), 2);
        assert_eq!(resolve_index(Some(("C", 1)), &songs, 0), 0);
    }

    #[test]
    fn test_resolve_index_keeps_pointer_stable_without_edits() {
        let songs = vec![song("A", 0), song("B", 1), song("C", 2)];
        assert_eq!(resolve_index(Some(("A", 1)), &songs, 0), 0);
        assert_eq!(resolve_index(Some(("B", 1)), &songs, 0), 1);
    }

    #[test]
    fn test_resolve_index_missing_anchor_uses_saved_position() {
        // current song removed; fall back to first song at/after saved position
        let songs = vec![song("X", 0), song("Y", 1)];
        assert_eq!(resolve_index(Some(("GONE", 1)), &songs, 0), 0);
        assert_eq!(resolve_index(Some(("GONE", 1)), &songs, 1), 1);
        assert_eq!(resolve_index(Some(("GONE", 1)), &songs, 3), songs.len());
    }

    #[test]
    fn test_resolve_index_no_anchor_uses_saved_position() {
        let songs = vec![song("A", 0), song("B", 1), song("C", 2)];
        assert_eq!(resolve_index(None, &songs, 1), 1);
        assert_eq!(resolve_index(None, &songs, 5), songs.len());
    }

    #[test]
    fn test_resolve_index_empty_list() {
        assert_eq!(resolve_index(Some(("A", 1)), &[], 0), 0);
        assert_eq!(resolve_index(None, &[], 0), 0);
    }

    #[test]
    fn test_resolve_index_duplicate_songs_rank_uniquely() {
        // [A, A, B] — playing the SECOND copy of A must stay on it, not jump to the first
        let songs = vec![song("A", 0), song("A", 1), song("B", 2)];
        assert_eq!(resolve_index(Some(("A", 1)), &songs, 0), 0);
        assert_eq!(resolve_index(Some(("A", 2)), &songs, 1), 1);
        assert_eq!(resolve_index(Some(("B", 1)), &songs, 0), 2);
    }

    #[test]
    fn test_resolve_index_duplicates_survive_reorder() {
        // A (1st copy) playing; reorder brings another song to the front
        let songs = vec![song("A", 0), song("B", 1), song("A", 2), song("C", 3)];
        assert_eq!(resolve_index(Some(("A", 1)), &songs, 0), 0);
        assert_eq!(resolve_index(Some(("A", 2)), &songs, 0), 2);
    }
}
