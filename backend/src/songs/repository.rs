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

pub async fn delete_songs_batch(db: &PgPool, ids: &[Uuid]) -> Result<(), AppError> {
    sqlx::query("DELETE FROM songs WHERE id = ANY($1)")
        .bind(ids)
        .execute(db)
        .await
        .db_error("failed to batch delete songs")?;
    Ok(())
}

pub async fn delete_songs_from_all_stations_batch(db: &PgPool, ids: &[Uuid]) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_songs WHERE song_id = ANY($1)")
        .bind(ids)
        .execute(db)
        .await
        .db_error("failed to batch unassign songs from stations")?;
    sqlx::query("DELETE FROM station_queue WHERE song_id = ANY($1)")
        .bind(ids)
        .execute(db)
        .await
        .db_error("failed to batch clear songs from station queues")?;
    Ok(())
}

pub async fn delete_song_from_all_station_songs(db: &PgPool, song_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_songs WHERE song_id = $1")
        .bind(song_id)
        .execute(db)
        .await
        .db_error("failed to unassign song from stations")?;
    Ok(())
}

pub async fn delete_song_from_all_station_queues(db: &PgPool, song_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_queue WHERE song_id = $1")
        .bind(song_id)
        .execute(db)
        .await
        .db_error("failed to clear song from station queues")?;
    Ok(())
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
