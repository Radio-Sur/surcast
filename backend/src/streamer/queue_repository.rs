use sqlx::PgPool;
use uuid::Uuid;

use super::SongInfo;
use crate::stations::repository;

pub(crate) struct QueueRepository {
    db: PgPool,
    station_id: Uuid,
    upload_dir: String,
}

impl QueueRepository {
    pub(crate) fn new(db: PgPool, station_id: Uuid, upload_dir: String) -> Self {
        Self {
            db,
            station_id,
            upload_dir,
        }
    }

    pub(crate) fn station_id(&self) -> Uuid {
        self.station_id
    }

    pub(crate) async fn load(&self) -> (Vec<SongInfo>, usize) {
        let rows = repository::find_station_song_info(&self.db, self.station_id)
            .await
            .unwrap_or_default();
        let songs = rows
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
            .collect::<Vec<_>>();
        let saved_index = sqlx::query_scalar::<_, i32>("SELECT current_song_index FROM stations WHERE id = $1")
            .bind(self.station_id)
            .fetch_optional(&self.db)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
            .max(0);
        let current_index = songs.iter().position(|song| song.position >= saved_index).unwrap_or(songs.len());
        (songs, current_index)
    }

    pub(crate) async fn persist_current(&self, position: i32) {
        if let Err(error) = sqlx::query("UPDATE stations SET current_song_index = $1 WHERE id = $2")
            .bind(position)
            .bind(self.station_id)
            .execute(&self.db)
            .await
        {
            tracing::warn!(station_id = %self.station_id, %error, "failed to persist current queue item");
        }
    }

    pub(crate) async fn refill(&self, upcoming: i64) {
        if let Err(error) =
            crate::scheduling::service::fill_queue_from_schedule(&self.db, self.station_id, Some(upcoming), &self.upload_dir).await
        {
            tracing::warn!(station_id = %self.station_id, %error, "AutoDJ successor refill error");
        }
    }

    pub(crate) async fn trim_played_items(&self) {
        let row: Option<(i32, i32)> = sqlx::query_as("SELECT current_song_index, played_limit FROM stations WHERE id = $1")
            .bind(self.station_id)
            .fetch_optional(&self.db)
            .await
            .ok()
            .flatten();
        let Some((current_idx, played_limit)) = row else {
            return;
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
        let mut index = 0;
        while index < played_items.len() {
            visible += 1;
            if let Some(playlist_id) = played_items[index].1 {
                index += 1;
                while index < played_items.len() && played_items[index].1 == Some(playlist_id) {
                    index += 1;
                }
            } else {
                index += 1;
            }
        }
        if visible <= played_limit {
            return;
        }

        let mut removed = 0i32;
        let mut delete_ids = Vec::new();
        let mut index = 0;
        while index < played_items.len() && removed < visible - played_limit {
            let item = &played_items[index];
            delete_ids.push(item.0);
            index += 1;
            removed += 1;
            if let Some(playlist_id) = item.1 {
                while index < played_items.len() && played_items[index].1 == Some(playlist_id) {
                    delete_ids.push(played_items[index].0);
                    index += 1;
                }
            }
        }
        for id in delete_ids {
            if let Err(error) = sqlx::query("DELETE FROM station_queue WHERE id = $1 AND station_id = $2")
                .bind(id)
                .bind(self.station_id)
                .execute(&self.db)
                .await
            {
                tracing::warn!(station_id = %self.station_id, %error, "failed to clean up queue item");
            }
        }
    }

    pub(crate) async fn queue_json(&self) -> String {
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
        serde_json::to_string(&items).unwrap_or_else(|_| "[]".into())
    }
}
