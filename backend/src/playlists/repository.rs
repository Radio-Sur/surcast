use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::errors::{AppError, DbResult};
use crate::playlists::models::*;

pub async fn find_all_playlists(db: &PgPool) -> Result<Vec<Playlist>, AppError> {
    sqlx::query_as::<_, Playlist>("SELECT * FROM playlists ORDER BY created_at DESC")
        .fetch_all(db)
        .await
        .db_error("failed to list playlists")
}

pub async fn find_playlist_by_id(db: &PgPool, id: Uuid) -> Result<Option<Playlist>, AppError> {
    sqlx::query_as::<_, Playlist>("SELECT * FROM playlists WHERE id = $1")
        .bind(id)
        .fetch_optional(db)
        .await
        .db_error("failed to get playlist")
}

pub async fn find_playlist_by_slug(db: &PgPool, slug: &str) -> Result<Option<Playlist>, AppError> {
    sqlx::query_as::<_, Playlist>("SELECT * FROM playlists WHERE slug = $1")
        .bind(slug)
        .fetch_optional(db)
        .await
        .db_error("failed to get playlist by slug")
}

pub async fn resolve_playlist_id(db: &PgPool, id_or_slug: &str) -> Result<Uuid, AppError> {
    if let Ok(uuid) = Uuid::parse_str(id_or_slug) {
        return Ok(uuid);
    }
    find_playlist_by_slug(db, id_or_slug)
        .await?
        .map(|p| p.id)
        .ok_or_else(|| AppError::NotFound("Playlist not found".into()))
}

pub async fn insert_playlist(db: &PgPool, id: Uuid, name: &str, description: &str, slug: &str, created_by: Uuid) -> Result<(), AppError> {
    sqlx::query("INSERT INTO playlists (id, name, description, slug, created_by) VALUES ($1, $2, $3, $4, $5)")
        .bind(id)
        .bind(name)
        .bind(description)
        .bind(slug)
        .bind(created_by)
        .execute(db)
        .await
        .db_error("failed to create playlist")?;
    Ok(())
}

pub async fn update_playlist_fields(db: &PgPool, id: Uuid, name: &str, description: &str, slug: &str) -> Result<(), AppError> {
    sqlx::query("UPDATE playlists SET name = $1, description = $2, slug = $3, updated_at = NOW() WHERE id = $4")
        .bind(name)
        .bind(description)
        .bind(slug)
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to update playlist")?;
    Ok(())
}

pub async fn delete_playlist(db: &PgPool, id: Uuid) -> Result<u64, AppError> {
    let result = sqlx::query("DELETE FROM playlists WHERE id = $1")
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to delete playlist")?;
    Ok(result.rows_affected())
}

pub async fn playlist_exists(db: &PgPool, playlist_id: Uuid) -> Result<bool, AppError> {
    let exists: (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM playlists WHERE id = $1)")
        .bind(playlist_id)
        .fetch_one(db)
        .await
        .db_error("failed to check playlist exists")?;
    Ok(exists.0)
}

pub async fn compute_max_playlist_position(conn: &mut PgConnection, playlist_id: Uuid) -> Result<i32, AppError> {
    let max_pos: (Option<i32>,) = sqlx::query_as("SELECT MAX(position) FROM playlist_songs WHERE playlist_id = $1")
        .bind(playlist_id)
        .fetch_one(&mut *conn)
        .await
        .db_error("failed to compute max position")?;
    Ok(max_pos.0.unwrap_or(-1) + 1)
}

pub async fn playlist_song_stats(db: &PgPool, playlist_id: Uuid) -> Result<(i64, i64), AppError> {
    sqlx::query_as::<_, (i64, Option<i64>)>(
        "SELECT COUNT(*), COALESCE(SUM(s.duration), 0) FROM playlist_songs ps LEFT JOIN songs s ON s.id = ps.song_id WHERE ps.playlist_id = $1",
    )
    .bind(playlist_id)
    .fetch_one(db)
    .await
    .map(|(count, duration)| (count, duration.unwrap_or(0)))
    .db_error("failed to compute playlist stats")
}

pub async fn find_playlist_song_ids(db: &PgPool, playlist_id: Uuid) -> Result<Vec<Uuid>, AppError> {
    let songs = sqlx::query_as::<_, (Uuid,)>(
        r#"SELECT ps.song_id
           FROM playlist_songs ps
           WHERE ps.playlist_id = $1
           ORDER BY ps.position"#,
    )
    .bind(playlist_id)
    .fetch_all(db)
    .await
    .db_error("failed to query playlist song IDs")?;
    Ok(songs.into_iter().map(|(id,)| id).collect())
}

pub async fn find_playlist_songs_with_details(
    conn: &mut PgConnection,
    playlist_id: Uuid,
) -> Result<Vec<(Uuid, Uuid, Uuid, i32, String, String, String, i32, String)>, AppError> {
    sqlx::query_as::<_, (Uuid, Uuid, Uuid, i32, String, String, String, i32, String)>(
        r#"SELECT ps.id, ps.playlist_id, ps.song_id, ps.position,
                  s.title, s.artist, s.album, s.duration, s.cover_path
           FROM playlist_songs ps
           JOIN songs s ON s.id = ps.song_id
           WHERE ps.playlist_id = $1
           ORDER BY ps.position"#,
    )
    .bind(playlist_id)
    .fetch_all(&mut *conn)
    .await
    .db_error("failed to query playlist songs")
}

pub async fn find_playlist_songs_with_details_paginated(
    conn: &mut PgConnection,
    playlist_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<(Uuid, Uuid, Uuid, i32, String, String, String, i32, String)>, AppError> {
    sqlx::query_as::<_, (Uuid, Uuid, Uuid, i32, String, String, String, i32, String)>(
        r#"SELECT ps.id, ps.playlist_id, ps.song_id, ps.position,
                  s.title, s.artist, s.album, s.duration, s.cover_path
           FROM playlist_songs ps
           JOIN songs s ON s.id = ps.song_id
           WHERE ps.playlist_id = $1
           ORDER BY ps.position
           LIMIT $2 OFFSET $3"#,
    )
    .bind(playlist_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(&mut *conn)
    .await
    .db_error("failed to query paginated playlist songs")
}

pub async fn count_playlist_songs(conn: &mut PgConnection, playlist_id: Uuid) -> Result<i64, AppError> {
    let result: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM playlist_songs WHERE playlist_id = $1")
        .bind(playlist_id)
        .fetch_one(&mut *conn)
        .await
        .db_error("failed to count playlist songs")?;
    Ok(result.0)
}

pub async fn insert_playlist_song(conn: &mut PgConnection, playlist_id: Uuid, song_id: Uuid, position: i32) -> Result<(), AppError> {
    sqlx::query("INSERT INTO playlist_songs (playlist_id, song_id, position) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
        .bind(playlist_id)
        .bind(song_id)
        .bind(position)
        .execute(&mut *conn)
        .await
        .db_error("failed to add song to playlist")?;
    Ok(())
}

pub async fn insert_playlist_songs_by_artist(
    conn: &mut PgConnection,
    playlist_id: Uuid,
    artist: &str,
    start_pos: i32,
) -> Result<i32, AppError> {
    let result = sqlx::query(
        r#"INSERT INTO playlist_songs (playlist_id, song_id, position)
           SELECT $1, s.id, $2 + ROW_NUMBER() OVER (ORDER BY s.album, s.title) - 1
           FROM songs s
           WHERE s.artist = $3
             AND NOT EXISTS (
               SELECT 1 FROM playlist_songs ps WHERE ps.playlist_id = $1 AND ps.song_id = s.id
             )"#,
    )
    .bind(playlist_id)
    .bind(start_pos)
    .bind(artist)
    .execute(&mut *conn)
    .await
    .db_error("failed to add artist songs to playlist")?;
    Ok(result.rows_affected() as i32)
}

pub async fn insert_playlist_songs_by_album(
    conn: &mut PgConnection,
    playlist_id: Uuid,
    artist: &str,
    album: &str,
    start_pos: i32,
) -> Result<i32, AppError> {
    let result = sqlx::query(
        r#"INSERT INTO playlist_songs (playlist_id, song_id, position)
           SELECT $1, s.id, $2 + ROW_NUMBER() OVER (ORDER BY s.title) - 1
           FROM songs s
           WHERE s.artist = $3 AND s.album = $4
             AND NOT EXISTS (
               SELECT 1 FROM playlist_songs ps WHERE ps.playlist_id = $1 AND ps.song_id = s.id
             )"#,
    )
    .bind(playlist_id)
    .bind(start_pos)
    .bind(artist)
    .bind(album)
    .execute(&mut *conn)
    .await
    .db_error("failed to add album songs to playlist")?;
    Ok(result.rows_affected() as i32)
}
pub async fn delete_playlist_song(conn: &mut PgConnection, playlist_id: Uuid, song_id: Uuid) -> Result<(), AppError> {
    sqlx::query("DELETE FROM playlist_songs WHERE playlist_id = $1 AND song_id = $2")
        .bind(playlist_id)
        .bind(song_id)
        .execute(&mut *conn)
        .await
        .db_error("failed to remove song from playlist")?;
    Ok(())
}

pub async fn delete_playlist_songs_batch(conn: &mut PgConnection, playlist_id: Uuid, song_ids: &[Uuid]) -> Result<(), AppError> {
    sqlx::query("DELETE FROM playlist_songs WHERE playlist_id = $1 AND song_id = ANY($2)")
        .bind(playlist_id)
        .bind(song_ids)
        .execute(&mut *conn)
        .await
        .db_error("failed to batch remove songs from playlist")?;
    Ok(())
}

pub async fn reorder_playlist_songs(conn: &mut PgConnection, playlist_id: Uuid, song_ids: &[Uuid]) -> Result<(), AppError> {
    for (i, &song_id) in song_ids.iter().enumerate() {
        sqlx::query("UPDATE playlist_songs SET position = $1 WHERE playlist_id = $2 AND song_id = $3")
            .bind(i as i32)
            .bind(playlist_id)
            .bind(song_id)
            .execute(&mut *conn)
            .await
            .db_error("failed to update song position")?;
    }
    Ok(())
}
