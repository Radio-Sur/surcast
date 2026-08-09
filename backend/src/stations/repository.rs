use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::errors::{AppError, DbResult};
use crate::stations::models::*;

pub async fn find_all_stations(db: &PgPool) -> Result<Vec<Station>, AppError> {
    sqlx::query_as::<_, Station>("SELECT * FROM stations ORDER BY created_at DESC")
        .fetch_all(db)
        .await
        .db_error("failed to list stations")
}

pub async fn find_station_by_id(db: &PgPool, id: Uuid) -> Result<Option<Station>, AppError> {
    sqlx::query_as::<_, Station>("SELECT * FROM stations WHERE id = $1")
        .bind(id)
        .fetch_optional(db)
        .await
        .db_error("failed to find station")
}

pub struct PlaybackSettings {
    pub transition_mode: String,
    pub default_fade_ms: i32,
    pub autocue_fade_max_ms: i32,
}

pub async fn find_playback_settings(db: &PgPool, station_id: Uuid) -> Result<Option<PlaybackSettings>, AppError> {
    sqlx::query_as::<_, (String, i32, i32)>("SELECT transition_mode, default_fade_ms, autocue_fade_max_ms FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_optional(db)
        .await
        .db_error("failed to find station playback settings")
        .map(|settings| {
            settings.map(|(transition_mode, default_fade_ms, autocue_fade_max_ms)| PlaybackSettings {
                transition_mode,
                default_fade_ms,
                autocue_fade_max_ms,
            })
        })
}

pub struct CreateStationParams {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub slug: String,
    pub stream_url: Option<String>,
    pub prebuffer_bytes: i32,
    pub played_limit: i32,
    pub default_fade_ms: i32,
    pub transition_mode: String,
    pub autocue_fade_max_ms: i32,
    pub created_by: Uuid,
}

pub async fn insert_station(db: &PgPool, params: &CreateStationParams) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO stations (id, name, description, slug, stream_url, prebuffer_bytes, played_limit, default_fade_ms, transition_mode, autocue_fade_max_ms, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(params.id)
    .bind(&params.name)
    .bind(&params.description)
    .bind(&params.slug)
    .bind(&params.stream_url)
    .bind(params.prebuffer_bytes)
    .bind(params.played_limit)
    .bind(params.default_fade_ms)
    .bind(&params.transition_mode)
    .bind(params.autocue_fade_max_ms)
    .bind(params.created_by)
    .execute(db)
    .await
    .db_error("failed to create station")?;
    Ok(())
}

pub async fn delete_station(db: &PgPool, id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM stations WHERE id = $1")
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to delete station")?;
    Ok(())
}

pub struct UpdateStationParams {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub slug: String,
    pub stream_url: Option<String>,
    pub prebuffer_bytes: i32,
    pub played_limit: i32,
    pub default_fade_ms: i32,
    pub transition_mode: String,
    pub autocue_fade_max_ms: i32,
}

pub async fn update_station_fields(db: &PgPool, params: &UpdateStationParams) -> Result<(), AppError> {
    sqlx::query(
        "UPDATE stations SET name = $1, description = $2, slug = $3, stream_url = $4, prebuffer_bytes = $5, played_limit = $6, default_fade_ms = $7, transition_mode = $8, autocue_fade_max_ms = $9, updated_at = NOW() WHERE id = $10",
    )
    .bind(&params.name)
    .bind(&params.description)
    .bind(&params.slug)
    .bind(&params.stream_url)
    .bind(params.prebuffer_bytes)
    .bind(params.played_limit)
    .bind(params.default_fade_ms)
    .bind(&params.transition_mode)
    .bind(params.autocue_fade_max_ms)
    .bind(params.id)
    .execute(db)
    .await
    .db_error("failed to update station")?;
    Ok(())
}

pub async fn find_station_songs_joined(
    conn: &mut PgConnection,
    station_id: Uuid,
) -> Result<Vec<(Uuid, Uuid, String, String, String, i32, String, String)>, AppError> {
    sqlx::query_as::<_, (Uuid, Uuid, String, String, String, i32, String, String)>(
        r#"SELECT ss.id, ss.song_id, s.title, s.artist, s.album, s.duration, s.mime_type, s.cover_path
           FROM station_songs ss
           JOIN songs s ON s.id = ss.song_id
           WHERE ss.station_id = $1
           ORDER BY s.title"#,
    )
    .bind(station_id)
    .fetch_all(&mut *conn)
    .await
    .db_error("failed to list station library")
}

pub async fn find_station_songs_joined_paginated(
    conn: &mut PgConnection,
    station_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<(Uuid, Uuid, String, String, String, i32, String, String)>, AppError> {
    sqlx::query_as::<_, (Uuid, Uuid, String, String, String, i32, String, String)>(
        r#"SELECT ss.id, ss.song_id, s.title, s.artist, s.album, s.duration, s.mime_type, s.cover_path
           FROM station_songs ss
           JOIN songs s ON s.id = ss.song_id
           WHERE ss.station_id = $1
           ORDER BY s.title
           LIMIT $2 OFFSET $3"#,
    )
    .bind(station_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(&mut *conn)
    .await
    .db_error("failed to list paginated station library")
}

pub async fn count_station_songs(conn: &mut PgConnection, station_id: Uuid) -> Result<i64, AppError> {
    let result: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM station_songs WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(&mut *conn)
        .await
        .db_error("failed to count station library songs")?;
    Ok(result.0)
}

pub async fn find_station_songs_joined_by_ids(
    db: &PgPool,
    station_id: Uuid,
    song_ids: &[Uuid],
) -> Result<Vec<(Uuid, Uuid, String, String, String, i32, String, String)>, AppError> {
    sqlx::query_as::<_, (Uuid, Uuid, String, String, String, i32, String, String)>(
        r#"SELECT ss.id, ss.song_id, s.title, s.artist, s.album, s.duration, s.mime_type, s.cover_path
           FROM station_songs ss
           JOIN songs s ON s.id = ss.song_id
           WHERE ss.station_id = $1 AND ss.song_id = ANY($2)
           ORDER BY s.title"#,
    )
    .bind(station_id)
    .bind(song_ids)
    .fetch_all(db)
    .await
    .db_error("failed to query added songs")
}

pub async fn insert_station_song(conn: &mut PgConnection, station_id: Uuid, song_id: Uuid) -> Result<(), AppError> {
    sqlx::query("INSERT INTO station_songs (station_id, song_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(station_id)
        .bind(song_id)
        .execute(&mut *conn)
        .await
        .db_error("failed to add song to station")?;
    Ok(())
}

pub async fn delete_station_song(db: &PgPool, station_id: Uuid, song_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM station_songs WHERE station_id = $1 AND song_id = $2")
        .bind(station_id)
        .bind(song_id)
        .execute(db)
        .await
        .db_error("failed to remove from station library")?;
    Ok(())
}

pub async fn check_song_in_library(db: &PgPool, station_id: Uuid, song_id: Uuid) -> Result<bool, AppError> {
    let in_library: (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM station_songs WHERE station_id = $1 AND song_id = $2)")
        .bind(station_id)
        .bind(song_id)
        .fetch_one(db)
        .await
        .db_error("failed to verify song in library")?;
    Ok(in_library.0)
}

pub async fn verify_station_exists(db: &PgPool, station_id: Uuid) -> Result<(), AppError> {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM stations WHERE id = $1)")
        .bind(station_id)
        .fetch_one(db)
        .await
        .db_error("failed to verify station existence")?
        .then_some(())
        .ok_or_else(|| AppError::NotFound("Station not found".into()))
}

pub async fn find_station_song_info(
    db: &PgPool,
    station_id: Uuid,
) -> Result<Vec<(String, String, String, i32, Uuid, Uuid, i32, f64, f64, f64, bool)>, AppError> {
    sqlx::query_as::<_, (String, String, String, i32, Uuid, Uuid, i32, f64, f64, f64, bool)>(
        r#"SELECT s.file_path, s.title, s.artist, s.duration, sq.id, s.id, sq.position,
                  s.cue_in, s.cue_out, s.cross_start_next,
                  (s.analyzed_at IS NOT NULL)::bool AS analyzed
           FROM station_queue sq
           JOIN songs s ON s.id = sq.song_id
           WHERE sq.station_id = $1
           ORDER BY sq.position"#,
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to load station songs")
}

pub async fn resolve_station_id_from_slug(db: &PgPool, slug: &str) -> Result<Uuid, AppError> {
    sqlx::query_scalar::<_, Uuid>("SELECT id FROM stations WHERE slug = $1")
        .bind(slug)
        .fetch_optional(db)
        .await
        .db_error("failed to resolve station ID")?
        .ok_or_else(|| AppError::NotFound("Station not found".into()))
}

pub async fn insert_station_songs_by_artist(conn: &mut PgConnection, station_id: Uuid, artist: &str) -> Result<i32, AppError> {
    let result = sqlx::query(
        r#"INSERT INTO station_songs (station_id, song_id)
           SELECT $1, s.id
           FROM songs s
           WHERE s.artist = $2
             AND NOT EXISTS (
               SELECT 1 FROM station_songs ss WHERE ss.station_id = $1 AND ss.song_id = s.id
             )"#,
    )
    .bind(station_id)
    .bind(artist)
    .execute(&mut *conn)
    .await
    .db_error("failed to add artist songs to station")?;
    Ok(result.rows_affected() as i32)
}

pub async fn insert_station_songs_by_album(conn: &mut PgConnection, station_id: Uuid, artist: &str, album: &str) -> Result<i32, AppError> {
    let result = sqlx::query(
        r#"INSERT INTO station_songs (station_id, song_id)
           SELECT $1, s.id
           FROM songs s
           WHERE s.artist = $2 AND s.album = $3
             AND NOT EXISTS (
               SELECT 1 FROM station_songs ss WHERE ss.station_id = $1 AND ss.song_id = s.id
             )"#,
    )
    .bind(station_id)
    .bind(artist)
    .bind(album)
    .execute(&mut *conn)
    .await
    .db_error("failed to add album songs to station")?;
    Ok(result.rows_affected() as i32)
}

pub async fn find_all_station_ids(db: &PgPool) -> Result<Vec<(Uuid,)>, AppError> {
    sqlx::query_as("SELECT id FROM stations")
        .fetch_all(db)
        .await
        .db_error("failed to query all stations")
}
