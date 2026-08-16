use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::{AppError, DbResult};
use crate::songs::models::*;

pub async fn find_all_songs(db: &PgPool) -> Result<Vec<Song>, AppError> {
    sqlx::query_as::<_, Song>("SELECT * FROM songs ORDER BY created_at DESC")
        .fetch_all(db)
        .await
        .db_error("failed to list songs")
}

pub async fn find_songs_search(
    db: &PgPool,
    q: Option<&str>,
    artist: Option<&str>,
    album: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<Song>, AppError> {
    sqlx::query_as::<_, Song>(
        r#"SELECT * FROM songs
           WHERE ($1::text IS NULL OR title ILIKE '%' || $1 || '%' OR artist ILIKE '%' || $1 || '%' OR album ILIKE '%' || $1 || '%')
             AND ($2::text IS NULL OR artist ILIKE $2)
             AND ($3::text IS NULL OR album ILIKE $3)
           ORDER BY artist, album, title
           LIMIT $4 OFFSET $5"#,
    )
    .bind(q)
    .bind(artist)
    .bind(album)
    .bind(limit)
    .bind(offset)
    .fetch_all(db)
    .await
    .db_error("failed to search songs")
}

pub async fn count_songs_search(db: &PgPool, q: Option<&str>, artist: Option<&str>, album: Option<&str>) -> Result<i64, AppError> {
    let count: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*) FROM songs
           WHERE ($1::text IS NULL OR title ILIKE '%' || $1 || '%' OR artist ILIKE '%' || $1 || '%' OR album ILIKE '%' || $1 || '%')
             AND ($2::text IS NULL OR artist ILIKE $2)
             AND ($3::text IS NULL OR album ILIKE $3)"#,
    )
    .bind(q)
    .bind(artist)
    .bind(album)
    .fetch_one(db)
    .await
    .db_error("failed to count songs")?;
    Ok(count.0)
}

pub async fn find_artists(db: &PgPool, q: Option<&str>, limit: i64, offset: i64) -> Result<Vec<ArtistEntry>, AppError> {
    sqlx::query_as::<_, (String, i64, i64)>(
        r#"SELECT artist,
                  COUNT(DISTINCT album) as album_count,
                  COUNT(*) as song_count
           FROM songs
           WHERE ($1::text IS NULL OR artist ILIKE '%' || $1 || '%')
           GROUP BY artist
           ORDER BY artist
           LIMIT $2 OFFSET $3"#,
    )
    .bind(q)
    .bind(limit)
    .bind(offset)
    .fetch_all(db)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(name, album_count, song_count)| ArtistEntry {
                name,
                album_count,
                song_count,
            })
            .collect()
    })
    .db_error("failed to list artists")
}

pub async fn count_artists(db: &PgPool, q: Option<&str>) -> Result<i64, AppError> {
    let count: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(DISTINCT artist) FROM songs
           WHERE ($1::text IS NULL OR artist ILIKE '%' || $1 || '%')"#,
    )
    .bind(q)
    .fetch_one(db)
    .await
    .db_error("failed to count artists")?;
    Ok(count.0)
}

pub async fn find_song_ids_by_artist_album(
    db: &PgPool,
    playlist_id: Uuid,
    artist: Option<&str>,
    album: Option<&str>,
) -> Result<Vec<Uuid>, AppError> {
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        r#"SELECT s.id FROM songs s
           WHERE s.id NOT IN (
               SELECT song_id FROM playlist_songs WHERE playlist_id = $1
           )
           AND ($2::text IS NULL OR s.artist ILIKE $2)
           AND ($3::text IS NULL OR s.album ILIKE $3)
           ORDER BY s.artist, s.album, s.title"#,
    )
    .bind(playlist_id)
    .bind(artist)
    .bind(album)
    .fetch_all(db)
    .await
    .db_error("failed to find song IDs by artist/album")?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

pub async fn find_song_by_id(db: &PgPool, id: Uuid) -> Result<Option<Song>, AppError> {
    sqlx::query_as::<_, Song>("SELECT * FROM songs WHERE id = $1")
        .bind(id)
        .fetch_optional(db)
        .await
        .db_error("failed to find song")
}

pub async fn update_song_fields(db: &PgPool, id: Uuid, title: &str, artist: &str, album: &str, duration: i32) -> Result<(), AppError> {
    sqlx::query("UPDATE songs SET title = $1, artist = $2, album = $3, duration = $4, updated_at = NOW() WHERE id = $5")
        .bind(title)
        .bind(artist)
        .bind(album)
        .bind(duration)
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to update song")?;
    Ok(())
}

pub async fn delete_song(db: &PgPool, id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM songs WHERE id = $1")
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to delete song record")?;
    Ok(())
}

/// Atomically deletes one song everywhere: queue rows, station-library rows
/// and the `songs` row itself, and returns the DISTINCT station IDs whose
/// queue rows were actually deleted.
///
/// TOCTOU-safe in two ways:
/// - the affected stations come from `DELETE ... RETURNING station_id`
///   inside the SAME statement that removes the rows — never from a
///   pre-delete SELECT that could be stale;
/// - the parent `songs` row is locked (`FOR UPDATE`) BEFORE the queue
///   delete: a concurrent enqueue's foreign-key check blocks on that lock,
///   so no queue row for this song can appear between the queue delete and
///   the final `songs` delete. Without the lock, `ON DELETE CASCADE` would
///   remove such a late row outside the returned affected set.
pub async fn delete_song_globally(db: &PgPool, song_id: Uuid) -> Result<Vec<Uuid>, AppError> {
    let mut tx = db.begin().await.db_error("failed to begin song delete transaction")?;
    sqlx::query("SELECT id FROM songs WHERE id = $1 FOR UPDATE")
        .bind(song_id)
        .execute(&mut *tx)
        .await
        .db_error("failed to lock song row")?;
    let affected: Vec<Uuid> = sqlx::query_scalar(
        "WITH deleted AS (
             DELETE FROM station_queue WHERE song_id = $1 RETURNING station_id
         )
         SELECT DISTINCT station_id FROM deleted",
    )
    .bind(song_id)
    .fetch_all(&mut *tx)
    .await
    .db_error("failed to clear song from station queues")?;
    sqlx::query("DELETE FROM station_songs WHERE song_id = $1")
        .bind(song_id)
        .execute(&mut *tx)
        .await
        .db_error("failed to unassign song from stations")?;
    sqlx::query("DELETE FROM songs WHERE id = $1")
        .bind(song_id)
        .execute(&mut *tx)
        .await
        .db_error("failed to delete song")?;
    tx.commit().await.db_error("failed to commit song delete")?;
    Ok(affected)
}

/// Batch equivalent of [`delete_song_globally`]: deletes every listed song
/// and returns the DISTINCT station IDs whose queue rows were actually
/// deleted. The parent rows are locked in a stable order (`ORDER BY id`),
/// so two concurrent batch deletes can never deadlock on each other.
pub async fn delete_songs_globally(db: &PgPool, ids: &[Uuid]) -> Result<Vec<Uuid>, AppError> {
    let mut tx = db.begin().await.db_error("failed to begin batch song delete transaction")?;
    sqlx::query("SELECT id FROM songs WHERE id = ANY($1) ORDER BY id FOR UPDATE")
        .bind(ids)
        .execute(&mut *tx)
        .await
        .db_error("failed to lock song rows")?;
    let affected: Vec<Uuid> = sqlx::query_scalar(
        "WITH deleted AS (
             DELETE FROM station_queue WHERE song_id = ANY($1) RETURNING station_id
         )
         SELECT DISTINCT station_id FROM deleted",
    )
    .bind(ids)
    .fetch_all(&mut *tx)
    .await
    .db_error("failed to clear songs from station queues")?;
    sqlx::query("DELETE FROM station_songs WHERE song_id = ANY($1)")
        .bind(ids)
        .execute(&mut *tx)
        .await
        .db_error("failed to batch unassign songs from stations")?;
    sqlx::query("DELETE FROM songs WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut *tx)
        .await
        .db_error("failed to batch delete songs")?;
    tx.commit().await.db_error("failed to commit batch song delete")?;
    Ok(affected)
}

pub async fn find_station_ids_for_song(db: &PgPool, song_id: Uuid) -> Result<Vec<(Uuid,)>, AppError> {
    sqlx::query_as("SELECT station_id FROM station_songs WHERE song_id = $1 ORDER BY position")
        .bind(song_id)
        .fetch_all(db)
        .await
        .db_error("failed to query station IDs for songs")
}

pub struct InsertSongParams {
    pub id: Uuid,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub cover_path: String,
    pub file_path: String,
    pub file_size: i64,
    pub mime_type: String,
    pub duration: i32,
    pub uploaded_by: Uuid,
    pub cue_in: f64,
    pub cue_out: f64,
    pub cross_start_next: f64,
    pub loudness: Option<f32>,
    pub loudness_range: Option<f32>,
    pub true_peak: Option<f32>,
    pub true_peak_db: Option<f32>,
    pub amplify: Option<f32>,
    pub sustained_ending: bool,
    pub longtail: bool,
    pub analyzed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Default for InsertSongParams {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            title: String::new(),
            artist: String::new(),
            album: String::new(),
            cover_path: String::new(),
            file_path: String::new(),
            file_size: 0,
            mime_type: String::new(),
            duration: 0,
            uploaded_by: Uuid::new_v4(),
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            loudness: None,
            loudness_range: None,
            true_peak: None,
            true_peak_db: None,
            amplify: None,
            sustained_ending: false,
            longtail: false,
            analyzed_at: None,
        }
    }
}

pub async fn insert_song_record(db: &PgPool, params: &InsertSongParams) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO songs (id, title, artist, album, cover_path, file_path, file_size, mime_type, duration, uploaded_by, cue_in, cue_out, cross_start_next, loudness, loudness_range, true_peak, true_peak_db, amplify, sustained_ending, longtail, analyzed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)",
    )
    .bind(params.id)
    .bind(&params.title)
    .bind(&params.artist)
    .bind(&params.album)
    .bind(&params.cover_path)
    .bind(&params.file_path)
    .bind(params.file_size)
    .bind(&params.mime_type)
    .bind(params.duration)
    .bind(params.uploaded_by)
    .bind(params.cue_in)
    .bind(params.cue_out)
    .bind(params.cross_start_next)
    .bind(params.loudness)
    .bind(params.loudness_range)
    .bind(params.true_peak)
    .bind(params.true_peak_db)
    .bind(params.amplify)
    .bind(params.sustained_ending)
    .bind(params.longtail)
    .bind(params.analyzed_at)
    .execute(db)
    .await
    .db_error("failed to insert song record")?;
    Ok(())
}

pub async fn update_song_analysis(db: &PgPool, song_id: Uuid, analysis: &crate::songs::analysis::SongAnalysis) -> Result<(), AppError> {
    sqlx::query(
        "UPDATE songs SET duration = COALESCE($1, duration), cue_in = $2, cue_out = $3, cross_start_next = $4, loudness = $5, loudness_range = $6, true_peak = $7, true_peak_db = $8, amplify = $9, sustained_ending = $10, longtail = $11, analyzed_at = NOW() WHERE id = $12",
    )
    .bind(analysis.duration)
    .bind(analysis.cue_in)
    .bind(analysis.cue_out)
    .bind(analysis.cross_start_next)
    .bind(analysis.loudness)
    .bind(analysis.loudness_range)
    .bind(analysis.true_peak)
    .bind(analysis.true_peak_db)
    .bind(analysis.amplify)
    .bind(analysis.sustained_ending)
    .bind(analysis.longtail)
    .bind(song_id)
    .execute(db)
    .await
    .db_error("failed to update song analysis")?;
    Ok(())
}

pub async fn assign_song_to_station(db: &PgPool, station_id: Uuid, song_id: Uuid) -> Result<(), AppError> {
    sqlx::query("INSERT INTO station_songs (station_id, song_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(station_id)
        .bind(song_id)
        .execute(db)
        .await
        .db_error("failed to assign song to station")?;
    Ok(())
}

pub async fn count_songs_by_selectors(
    db: &PgPool,
    artist_names: &[String],
    album_artists: &[String],
    album_names: &[String],
    exclude_ids: &[Uuid],
) -> Result<i64, AppError> {
    let result: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*) FROM (
            SELECT id FROM songs WHERE artist = ANY($1::text[])
            UNION
            SELECT s.id FROM songs s
            JOIN unnest($2::text[], $3::text[]) AS a(artist, album) ON s.artist = a.artist AND s.album = a.album
        ) sub
        WHERE id <> ALL($4::uuid[])"#,
    )
    .bind(artist_names)
    .bind(album_artists)
    .bind(album_names)
    .bind(exclude_ids)
    .fetch_one(db)
    .await
    .db_error("failed to count songs by selectors")?;
    Ok(result.0)
}

pub async fn delete_song_from_station_queue(db: &PgPool, song_id: Uuid, station_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_queue WHERE song_id = $1 AND station_id = $2")
        .bind(song_id)
        .bind(station_id)
        .execute(db)
        .await
        .db_error("failed to remove from station queue")?;
    Ok(())
}

pub async fn delete_song_from_station_songs(db: &PgPool, song_id: Uuid, station_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_songs WHERE song_id = $1 AND station_id = $2")
        .bind(song_id)
        .bind(station_id)
        .execute(db)
        .await
        .db_error("failed to remove from station library")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    /// The destructive batch delete must return exactly the stations whose
    /// queue rows were really removed — DISTINCT, no unrelated stations —
    /// and the matching rows must be gone while unrelated ones survive.
    #[tokio::test]
    async fn batch_delete_returns_distinct_affected_stations() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else { return };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .expect("test database must be reachable");
        crate::db::run_migrations(&pool).await;

        // Unique username per run: a previous run's row must never collide
        // with the fresh user id this test inserts below.
        let user_id = Uuid::new_v4();
        let username = format!("affected-query-test-{user_id}");
        sqlx::query("INSERT INTO users (id, username, password_hash, name) VALUES ($1, $2, 'x', $3)")
            .bind(user_id)
            .bind(&username)
            .bind(&username)
            .execute(&pool)
            .await
            .expect("user insert");

        async fn station_with_song(pool: &PgPool, user_id: Uuid, name: &str, song_ids: &[Uuid]) -> Uuid {
            let station_id = Uuid::new_v4();
            sqlx::query("INSERT INTO stations (id, name, created_by) VALUES ($1, $2, $3)")
                .bind(station_id)
                .bind(name)
                .bind(user_id)
                .execute(pool)
                .await
                .expect("station insert");
            for (i, song_id) in song_ids.iter().enumerate() {
                sqlx::query("INSERT INTO station_queue (station_id, song_id, position) VALUES ($1, $2, $3)")
                    .bind(station_id)
                    .bind(song_id)
                    .bind(i as i32)
                    .execute(pool)
                    .await
                    .expect("queue insert");
            }
            station_id
        }

        async fn song(pool: &PgPool, user_id: Uuid, title: &str) -> Uuid {
            let song_id = Uuid::new_v4();
            sqlx::query("INSERT INTO songs (id, title, file_path, uploaded_by) VALUES ($1, $2, $3, $4)")
                .bind(song_id)
                .bind(title)
                .bind(format!("{title}.wav"))
                .bind(user_id)
                .execute(pool)
                .await
                .expect("song insert");
            song_id
        }

        let song_a = song(&pool, user_id, "del-a").await;
        let song_b = song(&pool, user_id, "del-b").await;
        let song_unrelated = song(&pool, user_id, "keep").await;
        // Station A queues both deleted songs; station B one; station C an
        // unrelated song that must survive.
        let station_a = station_with_song(&pool, user_id, "affected A", &[song_a, song_b]).await;
        let station_b = station_with_song(&pool, user_id, "affected B", &[song_b]).await;
        let station_c = station_with_song(&pool, user_id, "unaffected C", &[song_unrelated]).await;

        let mut affected = delete_songs_globally(&pool, &[song_a, song_b]).await.expect("batch delete");
        affected.sort();
        let mut expected = vec![station_a, station_b];
        expected.sort();
        assert_eq!(affected, expected, "affected stations must be exactly A and B, each once");

        // Only the unrelated row may survive; the deleted songs must have
        // no queue rows left anywhere.
        let deleted_remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM station_queue WHERE song_id = ANY($1)")
            .bind(&[song_a, song_b])
            .fetch_one(&pool)
            .await
            .expect("deleted-songs readback");
        assert_eq!(deleted_remaining, 0, "deleted songs must have no queue rows left");
        let unrelated: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT station_id, song_id FROM station_queue WHERE station_id = $1")
            .bind(station_c)
            .fetch_all(&pool)
            .await
            .expect("unrelated readback");
        assert_eq!(unrelated, vec![(station_c, song_unrelated)], "the unrelated row must survive");

        // Clean up this run's rows (the two deleted songs are already gone;
        // the unrelated one must be removed before the user row).
        sqlx::query("DELETE FROM station_queue WHERE station_id = ANY($1)")
            .bind(&[station_a, station_b, station_c])
            .execute(&pool)
            .await
            .expect("queue cleanup");
        sqlx::query("DELETE FROM stations WHERE id = ANY($1)")
            .bind(&[station_a, station_b, station_c])
            .execute(&pool)
            .await
            .expect("station cleanup");
        sqlx::query("DELETE FROM songs WHERE id = $1")
            .bind(song_unrelated)
            .execute(&pool)
            .await
            .expect("song cleanup");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("user cleanup");
    }
}
