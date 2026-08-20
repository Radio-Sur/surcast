use sqlx::PgPool;
use uuid::Uuid;

use super::{queue_state::QueueCursor, SongInfo};
use crate::stations::repository;

pub struct QueueRepository {
    db: PgPool,
    station_id: Uuid,
    upload_dir: String,
}

impl QueueRepository {
    pub fn new(db: PgPool, station_id: Uuid, upload_dir: String) -> Self {
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

    pub(crate) async fn persist_cursor_if_current(
        &self,
        previous_current_queue_item_id: Option<Uuid>,
        cursor: &QueueCursor,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query(
            "UPDATE stations
             SET current_queue_item_id = $1,
                 consumed_queue_item_ids = $2,
                 current_song_index = $3,
                 current_queue_cursor_format = 1
             WHERE id = $4
               AND (current_queue_cursor_format = 0
                    OR current_queue_item_id IS NOT DISTINCT FROM $5
                    OR current_queue_item_id IS NOT DISTINCT FROM $1)",
        )
        .bind(cursor.current_queue_item_id)
        .bind(&cursor.consumed_queue_item_ids)
        .bind(cursor.legacy_position)
        .bind(self.station_id)
        .bind(previous_current_queue_item_id)
        .execute(&self.db)
        .await?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            // The guard failed: the stored cursor references an id that is
            // gone (queue cleared, song deleted while stopped) or was left
            // stale by an older session, so no previous/current id matches.
            // A single streamer owns this station's cursor (the streamer map
            // is keyed by station), so heal it unconditionally — a frozen
            // cursor would otherwise fail every later persist and suppress
            // the AutoDJ refill until the queue drains.
            sqlx::query(
                "UPDATE stations
                 SET current_queue_item_id = $1,
                     consumed_queue_item_ids = $2,
                     current_song_index = $3,
                     current_queue_cursor_format = 1
                 WHERE id = $4",
            )
            .bind(cursor.current_queue_item_id)
            .bind(&cursor.consumed_queue_item_ids)
            .bind(cursor.legacy_position)
            .bind(self.station_id)
            .execute(&self.db)
            .await?;
            Ok(())
        }
    }

    pub async fn commit_cursor_and_refill(
        &self,
        previous_current_queue_item_id: Option<Uuid>,
        cursor: &QueueCursor,
    ) -> Result<(), sqlx::Error> {
        // Retries wrap the whole atomic unit (advisory lock + cursor persist +
        // trim + refill) in a fresh transaction every attempt. A failed
        // statement inside a PostgreSQL transaction aborts it — retrying on
        // the same handle is not a retry at all. Rolling the transaction back
        // also undoes any partial inserts a failed fill already made, so the
        // next attempt starts from a clean state.
        let mut attempt = 0u32;
        loop {
            match self.commit_cursor_and_refill_once(previous_current_queue_item_id, cursor).await {
                Ok(()) => return Ok(()),
                Err(error) if attempt < 2 => {
                    attempt += 1;
                    tracing::warn!(station_id = %self.station_id, %error, "queue cursor commit failed; retrying on a fresh transaction");
                    tokio::time::sleep(std::time::Duration::from_millis(100 * u64::from(attempt))).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn commit_cursor_and_refill_once(
        &self,
        previous_current_queue_item_id: Option<Uuid>,
        cursor: &QueueCursor,
    ) -> Result<(), sqlx::Error> {
        // The cursor persist and the AutoDJ refill share one transaction and
        // one advisory lock, so a concurrent fill (manual trigger, idle tick)
        // can never count the upcoming window from a stale cursor state:
        // without the lock the persist could land between another fill's
        // count and insert, over- or under-filling the window.
        let mut transaction = self.db.begin().await?;
        let outcome: Result<Vec<Uuid>, sqlx::Error> = async {
            crate::scheduling::service::auto_fill::lock_station_queue(&mut transaction, self.station_id)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
            let result = sqlx::query(
                "UPDATE stations
                 SET current_queue_item_id = $1,
                     consumed_queue_item_ids = $2,
                     current_song_index = $3,
                     current_queue_cursor_format = 1
                 WHERE id = $4
                   AND (current_queue_cursor_format = 0
                        OR current_queue_item_id IS NOT DISTINCT FROM $5
                        OR current_queue_item_id IS NOT DISTINCT FROM $1)",
            )
            .bind(cursor.current_queue_item_id)
            .bind(&cursor.consumed_queue_item_ids)
            .bind(cursor.legacy_position)
            .bind(self.station_id)
            .bind(previous_current_queue_item_id)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() != 1 {
                // The guard failed: the stored cursor references an id that is
                // gone (queue cleared, song deleted while stopped) or was left
                // stale by an older session, so no previous/current id matches.
                // A single streamer owns this station's cursor (the streamer map
                // is keyed by station), so heal it unconditionally — a frozen
                // cursor would otherwise fail every later persist and suppress
                // the AutoDJ refill until the queue drains.
                sqlx::query(
                    "UPDATE stations
                     SET current_queue_item_id = $1,
                         consumed_queue_item_ids = $2,
                         current_song_index = $3,
                         current_queue_cursor_format = 1
                     WHERE id = $4",
                )
                .bind(cursor.current_queue_item_id)
                .bind(&cursor.consumed_queue_item_ids)
                .bind(cursor.legacy_position)
                .bind(self.station_id)
                .execute(&mut *transaction)
                .await?;
            }
            self.trim_played_items_on(&mut transaction).await?;
            // Refill from the locked database state; the in-memory queue can lag
            // rows added or removed by other clients. Runs inside the same
            // transaction so the fill's upcoming count sees the persisted cursor.
            // Newly queued songs are only collected here: their analysis is a
            // side effect that cannot be rolled back, so it must wait for the
            // commit below (a rolled-back attempt must not analyze songs that
            // never made it into the queue, and a retry must not analyze the
            // same song repeatedly).
            crate::scheduling::service::fill_queue_from_schedule_locked(&mut transaction, self.station_id)
                .await
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
        }
        .await;
        match outcome {
            Ok(analyze) => {
                transaction.commit().await?;
                for song_id in analyze {
                    crate::songs::analysis::spawn_analysis(&self.db, song_id, self.station_id, &self.upload_dir);
                }
                Ok(())
            }
            Err(error) => {
                // Best effort: the transaction may already be aborted, in
                // which case the rollback itself fails — dropping the
                // transaction rolls back either way.
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn trim_played_items_on(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
        let row: Option<(i32, Vec<Uuid>)> = sqlx::query_as("SELECT played_limit, consumed_queue_item_ids FROM stations WHERE id = $1")
            .bind(self.station_id)
            .fetch_optional(&mut *connection)
            .await?;
        let Some((played_limit, consumed_queue_item_ids)) = row else {
            return Ok(());
        };
        if played_limit <= 0 || consumed_queue_item_ids.is_empty() {
            return Ok(());
        }

        // Queue positions are mutable: insert, reorder, and playlist removal
        // can all renumber them while a live track is playing. Only durable
        // cursor identities establish that a row has been played.
        let played_items: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
            "SELECT id, origin_playlist_id
                 FROM station_queue
                 WHERE station_id = $1 AND id = ANY($2)
                 ORDER BY position",
        )
        .bind(self.station_id)
        .bind(&consumed_queue_item_ids)
        .fetch_all(&mut *connection)
        .await?;
        if played_items.is_empty() {
            return Ok(());
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
            return Ok(());
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

        // Errors propagate: a failed DELETE (or the cursor UPDATE below)
        // aborts the surrounding transaction, so swallowing them here would
        // leave the caller running further SQL on an aborted transaction.
        // The caller (commit_cursor_and_refill) rolls back and retries the
        // whole atomic unit on a fresh transaction.
        let deleted_ids: Vec<Uuid> = sqlx::query_scalar("DELETE FROM station_queue WHERE station_id = $1 AND id = ANY($2) RETURNING id")
            .bind(self.station_id)
            .bind(&delete_ids)
            .fetch_all(&mut *connection)
            .await?;
        if deleted_ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "UPDATE stations
             SET consumed_queue_item_ids = ARRAY(
                 SELECT queue_item_id
                 FROM unnest(consumed_queue_item_ids) AS queue_item_id
                 WHERE NOT (queue_item_id = ANY($1))
             )
             WHERE id = $2",
        )
        .bind(&deleted_ids)
        .bind(self.station_id)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }

    /// Runs the AutoDJ / schedule fill. Returns `true` when the fill call
    /// completed, `false` when it failed (DB error, lock timeout, ...) — a
    /// failed fill must be retried by the caller, an empty successful fill
    /// means AutoDJ genuinely had nothing to add.
    pub(crate) async fn refill(&self) -> bool {
        match crate::scheduling::service::fill_queue_from_schedule(&self.db, self.station_id, &self.upload_dir).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(station_id = %self.station_id, %error, "AutoDJ successor refill error");
                false
            }
        }
    }

    pub(crate) async fn trim_played_items(&self) {
        let row: Option<(i32, Vec<Uuid>)> = sqlx::query_as("SELECT played_limit, consumed_queue_item_ids FROM stations WHERE id = $1")
            .bind(self.station_id)
            .fetch_optional(&self.db)
            .await
            .ok()
            .flatten();
        let Some((played_limit, consumed_queue_item_ids)) = row else {
            return;
        };
        if played_limit <= 0 || consumed_queue_item_ids.is_empty() {
            return;
        }

        // Queue positions are mutable: insert, reorder, and playlist removal
        // can all renumber them while a live track is playing. Only durable
        // cursor identities establish that a row has been played.
        let played_items: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
            "SELECT id, origin_playlist_id
                 FROM station_queue
                 WHERE station_id = $1 AND id = ANY($2)
                 ORDER BY position",
        )
        .bind(self.station_id)
        .bind(&consumed_queue_item_ids)
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

        let deleted_ids: Vec<Uuid> =
            match sqlx::query_scalar("DELETE FROM station_queue WHERE station_id = $1 AND id = ANY($2) RETURNING id")
                .bind(self.station_id)
                .bind(&delete_ids)
                .fetch_all(&self.db)
                .await
            {
                Ok(ids) => ids,
                Err(error) => {
                    tracing::warn!(station_id = %self.station_id, %error, "failed to clean up queue items");
                    return;
                }
            };
        if deleted_ids.is_empty() {
            return;
        }
        if let Err(error) = sqlx::query(
            "UPDATE stations
             SET consumed_queue_item_ids = ARRAY(
                 SELECT queue_item_id
                 FROM unnest(consumed_queue_item_ids) AS queue_item_id
                 WHERE NOT (queue_item_id = ANY($1))
             )
             WHERE id = $2",
        )
        .bind(&deleted_ids)
        .bind(self.station_id)
        .execute(&self.db)
        .await
        {
            tracing::warn!(station_id = %self.station_id, %error, "failed to discard trimmed queue identities");
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
