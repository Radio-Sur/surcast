use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::{AppError, DbResult};

pub async fn delete_station_queue_by_song(db: &PgPool, station_id: Uuid, song_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_queue WHERE station_id = $1 AND song_id = $2")
        .bind(station_id)
        .bind(song_id)
        .execute(db)
        .await
        .db_error("failed to clear station queue")?;
    Ok(())
}

pub async fn queue_next_position(db: &PgPool, station_id: Uuid) -> Result<i32, AppError> {
    let max_pos: (Option<i32>,) = sqlx::query_as("SELECT MAX(position) FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(db)
        .await
        .db_error("failed to get next queue position")?;
    Ok(max_pos.0.unwrap_or(-1) + 1)
}

pub async fn insert_queue_item(
    db: &PgPool,
    station_id: Uuid,
    song_id: Uuid,
    position: i32,
    origin_playlist_id: Option<Uuid>,
) -> Result<(), AppError> {
    sqlx::query("INSERT INTO station_queue (station_id, song_id, position, origin_playlist_id) VALUES ($1, $2, $3, $4)")
        .bind(station_id)
        .bind(song_id)
        .bind(position)
        .bind(origin_playlist_id)
        .execute(db)
        .await
        .db_error("failed to insert queue items")?;
    Ok(())
}

pub async fn insert_queue_items_batch(
    db: &PgPool,
    station_id: Uuid,
    song_ids: &[Uuid],
    start_position: i32,
    origin_playlist_id: Option<Uuid>,
) -> Result<(), AppError> {
    for (i, &song_id) in song_ids.iter().enumerate() {
        insert_queue_item(db, station_id, song_id, start_position + i as i32, origin_playlist_id).await?;
    }
    Ok(())
}

/// Remove rows that have already been played, so the queue table holds only the
/// current track and upcoming ones. Stale played rows (e.g. from previous adds
/// of the same playlist) would otherwise inflate the "Song X of Y" counters
/// whenever new content is enqueued.
///
/// Trim is identity-based only: exactly the rows named in the durable format-1
/// cursor's `consumed_queue_item_ids`. A positional cutoff such as
/// `position < current_song_index` must never be used here: reorder/insert
/// handlers renumber queue positions from 0 without moving the station cursor,
/// so a stale index would delete unplayed current/upcoming tracks.
pub async fn trim_consumed_queue_items(db: &PgPool, station_id: Uuid) -> Result<(), AppError> {
    sqlx::query(
        "DELETE FROM station_queue sq
         WHERE sq.station_id = $1
           AND EXISTS (
             SELECT 1 FROM stations st
             WHERE st.id = $1
               AND sq.id = ANY(st.consumed_queue_item_ids)
           )",
    )
    .bind(station_id)
    .execute(db)
    .await
    .db_error("failed to trim consumed queue items")?;
    Ok(())
}

/// Re-anchor the legacy `stations.current_song_index` to the current track's
/// new position after an operation that renumbered queue positions (reorder,
/// insert-at-position, playlist removal). Positional consumers — legacy
/// format-0 load, the played-window trim and AutoDJ demand counting — rely on
/// the index matching the current row's position; left stale after a renumber
/// they would treat upcoming rows as played and delete/over-count them.
pub async fn sync_current_song_index_after_renumber(db: &PgPool, station_id: Uuid) -> Result<(), AppError> {
    sqlx::query(
        "UPDATE stations st
         SET current_song_index = sq.position
         FROM station_queue sq
         WHERE st.id = $1
           AND st.current_queue_item_id IS NOT NULL
           AND sq.id = st.current_queue_item_id",
    )
    .bind(station_id)
    .execute(db)
    .await
    .db_error("failed to sync current song index after renumber")?;
    Ok(())
}

pub async fn find_queue_items_all(
    db: &PgPool,
    station_id: Uuid,
) -> Result<
    Vec<(
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
    )>,
    AppError,
> {
    sqlx::query_as::<
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
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to fetch queue items")
}

pub async fn find_queue_items_by_song_ids(
    db: &PgPool,
    station_id: Uuid,
    song_ids: &[Uuid],
) -> Result<
    Vec<(
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
    )>,
    AppError,
> {
    sqlx::query_as::<
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
           WHERE sq.station_id = $1 AND sq.song_id = ANY($2)
           ORDER BY sq.position"#,
    )
    .bind(station_id)
    .bind(song_ids)
    .fetch_all(db)
    .await
    .db_error("failed to fetch queue items with filter")
}

pub async fn delete_queue_by_id(db: &PgPool, item_id: Uuid, station_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_queue WHERE id = $1 AND station_id = $2")
        .bind(item_id)
        .bind(station_id)
        .execute(db)
        .await
        .db_error("failed to check song in library for insert")?;
    Ok(())
}

pub async fn set_queue_position(db: &PgPool, item_id: Uuid, position: i32) -> Result<(), AppError> {
    sqlx::query("UPDATE station_queue SET position = $1 WHERE id = $2")
        .bind(position)
        .bind(item_id)
        .execute(db)
        .await
        .db_error("failed to shift queue positions")?;
    Ok(())
}

pub async fn shift_queue_positions_from(db: &PgPool, station_id: Uuid, from_position: i32) -> Result<(), AppError> {
    sqlx::query("UPDATE station_queue SET position = position + 1 WHERE station_id = $1 AND position >= $2")
        .bind(station_id)
        .bind(from_position)
        .execute(db)
        .await
        .db_error("failed to remove playlist songs from queue")?;
    Ok(())
}

pub async fn insert_queue_item_at(db: &PgPool, station_id: Uuid, song_id: Uuid, position: i32) -> Result<(), AppError> {
    sqlx::query("INSERT INTO station_queue (station_id, song_id, position) VALUES ($1, $2, $3)")
        .bind(station_id)
        .bind(song_id)
        .bind(position)
        .execute(db)
        .await
        .db_error("failed to re-index queue positions")?;
    Ok(())
}

pub async fn delete_queue_by_playlist(db: &PgPool, station_id: Uuid, playlist_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_queue WHERE station_id = $1 AND origin_playlist_id = $2")
        .bind(station_id)
        .bind(playlist_id)
        .execute(db)
        .await
        .db_error("failed to update queue position")?;
    Ok(())
}

pub async fn list_ordered_queue_ids(db: &PgPool, station_id: Uuid) -> Result<Vec<(Uuid,)>, AppError> {
    sqlx::query_as("SELECT id FROM station_queue WHERE station_id = $1 ORDER BY position")
        .bind(station_id)
        .fetch_all(db)
        .await
        .db_error("failed to reorder queue items")
}
