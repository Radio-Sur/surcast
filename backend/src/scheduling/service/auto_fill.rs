use std::collections::HashSet;

use rand::prelude::IndexedRandom;
use rand::RngExt;
use sqlx::{pool::PoolConnection, PgPool, Postgres};
use uuid::Uuid;

use crate::errors::{AppError, DbResult};
use crate::scheduling::models::{AutoDjMode, SourceType};

pub struct AutoFillConfig {
    pub source_type: SourceType,
    pub source_playlist_id: Option<Uuid>,
    pub mode: AutoDjMode,
    pub avoid_repeat: bool,
    pub min_gap: i32,
    pub songs_ahead: i32,
}

async fn lock_station_queue(db: &PgPool, station_id: Uuid) -> Result<PoolConnection<Postgres>, AppError> {
    let mut connection = db.acquire().await.db_error("failed to acquire AutoDJ queue lock")?;
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
        .bind(station_id.to_string())
        .execute(&mut *connection)
        .await
        .db_error("failed to lock AutoDJ queue")?;
    Ok(connection)
}

async fn unlock_station_queue(connection: &mut PoolConnection<Postgres>, station_id: Uuid) {
    if let Err(error) = sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
        .bind(station_id.to_string())
        .execute(&mut **connection)
        .await
    {
        tracing::error!(%error, %station_id, "failed to unlock AutoDJ queue");
    }
}

async fn active_song_ids(db: &PgPool, station_id: Uuid) -> Result<HashSet<Uuid>, AppError> {
    sqlx::query_scalar(
        "SELECT sq.song_id
         FROM station_queue sq
         JOIN stations st ON st.id = sq.station_id
         WHERE sq.station_id = $1
           AND (sq.id IS NOT DISTINCT FROM st.current_queue_item_id
                OR sq.id <> ALL(st.consumed_queue_item_ids))",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to load active queue songs")
    .map(|song_ids: Vec<Uuid>| song_ids.into_iter().collect())
}

pub(crate) async fn fill_from_playlist(
    db: &PgPool,
    station_id: Uuid,
    playlist_id: Uuid,
    songs_ahead: Option<i32>,
    upload_dir: &str,
) -> Result<(), AppError> {
    let mut lock = lock_station_queue(db, station_id).await?;
    let result = fill_from_playlist_locked(db, station_id, playlist_id, songs_ahead, upload_dir).await;
    unlock_station_queue(&mut lock, station_id).await;
    result
}

async fn fill_from_playlist_locked(
    db: &PgPool,
    station_id: Uuid,
    playlist_id: Uuid,
    songs_ahead: Option<i32>,
    upload_dir: &str,
) -> Result<(), AppError> {
    let song_ids: Vec<Uuid> = sqlx::query_scalar("SELECT ps.song_id FROM playlist_songs ps WHERE ps.playlist_id = $1 ORDER BY ps.position")
        .bind(playlist_id)
        .fetch_all(db)
        .await
        .db_error("failed to fetch playlist songs")?;

    if song_ids.is_empty() {
        return Ok(());
    }

    // The playlist fill tops the queue up to the same songs_ahead window as
    // every other fill — it must never dump the whole playlist in one go.
    // The schedule's own setting wins; otherwise the station's AutoDJ
    // minimum applies, with the legacy default of five.
    let target: i64 = match songs_ahead {
        Some(n) => i64::from(n),
        None => sqlx::query_scalar::<_, Option<i32>>("SELECT songs_ahead FROM station_auto_fill WHERE station_id = $1")
            .bind(station_id)
            .fetch_optional(db)
            .await
            .db_error("failed to load auto-fill songs_ahead")?
            .flatten()
            .map(i64::from)
            .unwrap_or(5),
    };

    let upcoming_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM station_queue WHERE station_id = $1 AND position > (SELECT COALESCE(current_song_index, 0) FROM stations WHERE id = $1)"
    )
    .bind(station_id)
    .fetch_one(db)
    .await
    .db_error("failed to count upcoming songs")?;

    if upcoming_count >= target {
        return Ok(());
    }

    // Same rule as the AutoDJ fill: a queue without a current row needs one
    // extra pick, because the first inserted row becomes the current track.
    let queue_empty: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1 FROM station_queue sq
             JOIN stations st ON st.id = sq.station_id
             WHERE sq.station_id = $1
               AND sq.id <> ALL(st.consumed_queue_item_ids)
         )",
    )
    .bind(station_id)
    .fetch_one(db)
    .await
    .db_error("failed to check whether the queue is empty")?;

    let mut need = target - upcoming_count;
    if queue_empty {
        need += 1;
    }

    let next_pos: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(position), -1) + 1 FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(db)
        .await
        .db_error("failed to find next queue position")?;

    let active_song_ids = active_song_ids(db, station_id).await?;
    for (added, song_id) in song_ids.iter().filter(|song_id| !active_song_ids.contains(song_id)).enumerate() {
        if added as i64 >= need {
            break;
        }
        sqlx::query(
            "INSERT INTO station_queue (station_id, song_id, position, origin_playlist_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        )
        .bind(station_id)
        .bind(song_id)
        .bind(next_pos + added as i32)
        .bind(playlist_id)
        .execute(db)
        .await
        .db_error("failed to enqueue playlist song")?;

        crate::songs::analysis::spawn_analysis(db, *song_id, station_id, upload_dir);
    }

    Ok(())
}

async fn pick_from_playlist(db: &PgPool, playlist_id: Uuid) -> Result<Vec<Uuid>, AppError> {
    sqlx::query_scalar("SELECT ps.song_id FROM playlist_songs ps WHERE ps.playlist_id = $1 ORDER BY ps.position")
        .bind(playlist_id)
        .fetch_all(db)
        .await
        .db_error("failed to pick songs from playlist")
}

async fn pick_from_station_library(db: &PgPool, station_id: Uuid) -> Result<Vec<Uuid>, AppError> {
    sqlx::query_scalar("SELECT ss.song_id FROM station_songs ss WHERE ss.station_id = $1")
        .bind(station_id)
        .fetch_all(db)
        .await
        .db_error("failed to pick from station library")
}

async fn pick_from_global_library(db: &PgPool) -> Result<Vec<Uuid>, AppError> {
    sqlx::query_scalar("SELECT id FROM songs")
        .fetch_all(db)
        .await
        .db_error("failed to pick from global library")
}

async fn pick_weighted(db: &PgPool, station_id: Uuid, excluded: &HashSet<Uuid>) -> Result<Vec<Uuid>, AppError> {
    let weighted = sqlx::query_as::<_, (Uuid, i32)>(
        "SELECT safp.playlist_id, safp.weight FROM station_auto_fill_playlists safp WHERE safp.station_id = $1 AND safp.weight > 0",
    )
    .bind(station_id)
    .fetch_all(db)
    .await
    .db_error("failed to load weighted playlists")?;

    let mut eligible = Vec::new();
    let mut repeat = Vec::new();
    for (playlist_id, weight) in weighted {
        let songs = pick_from_playlist(db, playlist_id).await?;
        if songs.is_empty() {
            continue;
        }
        let unique: Vec<_> = songs.iter().copied().filter(|song_id| !excluded.contains(song_id)).collect();
        repeat.push((weight, songs));
        if !unique.is_empty() {
            eligible.push((weight, unique));
        }
    }
    let candidates = if eligible.is_empty() { repeat } else { eligible };
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let total_weight: i32 = candidates.iter().map(|(weight, _)| *weight).sum();
    let mut rng = rand::make_rng::<rand::rngs::StdRng>();
    let pick = rng.random_range(0..total_weight);
    let mut cumulative = 0;
    for (weight, songs) in candidates {
        cumulative += weight;
        if pick < cumulative {
            return Ok(songs);
        }
    }
    unreachable!("positive weights cover the sampled range")
}

async fn apply_mode_to_candidates(db: &PgPool, candidates: &[Uuid], mode: &AutoDjMode, station_id: Uuid) -> Result<Uuid, AppError> {
    if candidates.is_empty() {
        return Err(AppError::BadRequest("No songs available for selection".into()));
    }
    let mut rng = rand::make_rng::<rand::rngs::StdRng>();
    let selected = match mode {
        AutoDjMode::Sequential => {
            sqlx::query_scalar::<_, Option<i32>>(
                "SELECT MAX(sq.position) FROM station_queue sq JOIN station_songs ss ON ss.song_id = sq.song_id WHERE sq.station_id = $1 AND sq.origin_playlist_id IS NOT NULL AND ss.song_id = ANY($2)",
            )
            .bind(station_id)
            .bind(candidates)
            .fetch_optional(db)
            .await
            .db_error("failed to find last sequential position")?
            .flatten()
            .map(|last_pos| {
                let idx = last_pos as usize % candidates.len();
                candidates[idx]
            })
            .unwrap_or_else(|| candidates[0])
        }
        AutoDjMode::Reverse => {
            let last_song = sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT sq.song_id FROM station_queue sq WHERE sq.station_id = $1 AND sq.position = (SELECT MAX(position) FROM station_queue WHERE station_id = $1)",
            )
            .bind(station_id)
            .fetch_optional(db)
            .await
            .db_error("failed to find last played song")?
            .flatten();

            match last_song {
                Some(last_id) => {
                    let idx = candidates.iter().position(|&id| id == last_id).unwrap_or(0);
                    let prev_idx = if idx == 0 { candidates.len() - 1 } else { idx - 1 };
                    candidates[prev_idx]
                }
                None => candidates[candidates.len() - 1],
            }
        }
        AutoDjMode::Random => candidates[rng.random_range(0..candidates.len())],
    };
    Ok(selected)
}

async fn pick_song_for_source(
    db: &PgPool,
    station_id: Uuid,
    source_type: &SourceType,
    source_playlist_id: Option<Uuid>,
    mode: &AutoDjMode,
    avoid_repeat: bool,
    min_gap: i32,
    excluded: &HashSet<Uuid>,
) -> Result<Option<Uuid>, AppError> {
    let song_ids = match source_type {
        SourceType::Playlist => match source_playlist_id {
            Some(pid) => pick_from_playlist(db, pid).await?,
            None => return Ok(None),
        },
        SourceType::StationLibrary => pick_from_station_library(db, station_id).await?,
        SourceType::GlobalLibrary => pick_from_global_library(db).await?,
        SourceType::WeightedPlaylists => pick_weighted(db, station_id, excluded).await?,
    };

    if song_ids.is_empty() {
        tracing::warn!(station_id = %station_id, %source_type, "AutoDJ: no songs in source");
        return Ok(None);
    }
    let unique: Vec<_> = song_ids.iter().copied().filter(|song_id| !excluded.contains(song_id)).collect();
    let candidates = if unique.is_empty() { &song_ids } else { &unique };
    let selected = apply_mode_to_candidates(db, candidates, mode, station_id).await?;

    let unique_artist_count: i64 = if avoid_repeat {
        sqlx::query_scalar("SELECT COUNT(DISTINCT artist) FROM songs WHERE id = ANY($1) AND artist != ''")
            .bind(&song_ids)
            .fetch_one(db)
            .await
            .db_error("failed to count unique artists")?
    } else {
        0
    };

    if avoid_repeat && min_gap > 0 && unique_artist_count > 1 {
        let recent_artists: Vec<String> = sqlx::query_scalar(
            "SELECT s.artist FROM station_queue sq JOIN songs s ON s.id = sq.song_id WHERE sq.station_id = $1 GROUP BY s.artist ORDER BY MAX(sq.position) DESC LIMIT $2",
        )
        .bind(station_id)
        .bind(min_gap)
        .fetch_all(db)
        .await
        .db_error("failed to query recent artists")?;

        let selected_artist: Option<String> = sqlx::query_scalar("SELECT artist FROM songs WHERE id = $1")
            .bind(selected)
            .fetch_optional(db)
            .await
            .db_error("failed to find selected artist")?;

        if let Some(ref artist) = selected_artist {
            if !artist.is_empty() && recent_artists.contains(artist) {
                let recent_ids: Vec<Uuid> = sqlx::query_scalar(
                    "SELECT sq.song_id FROM station_queue sq WHERE sq.station_id = $1 ORDER BY sq.position DESC LIMIT $2",
                )
                .bind(station_id)
                .bind(min_gap)
                .fetch_all(db)
                .await
                .db_error("failed to find recent queue entries")?;

                let mut safe = Vec::new();
                let mut rng = rand::make_rng::<rand::rngs::StdRng>();
                for id in candidates {
                    if recent_ids.contains(id) {
                        continue;
                    }
                    if avoid_repeat {
                        let artist_name: Option<String> = sqlx::query_scalar("SELECT artist FROM songs WHERE id = $1")
                            .bind(id)
                            .fetch_optional(db)
                            .await
                            .db_error("failed to check candidate artist")?;
                        if let Some(ref a) = artist_name {
                            if recent_artists.contains(a) {
                                continue;
                            }
                        }
                    }
                    safe.push(*id);
                }

                if !safe.is_empty() {
                    return Ok(Some(*safe.choose(&mut rng).unwrap_or(&selected)));
                }

                return Ok(Some(selected));
            }
        }
    }

    Ok(Some(selected))
}

async fn pick_and_insert_song(
    db: &PgPool,
    station_id: Uuid,
    config: &AutoFillConfig,
    upload_dir: &str,
    excluded: &mut HashSet<Uuid>,
) -> Result<bool, AppError> {
    let song_id = match pick_song_for_source(
        db,
        station_id,
        &config.source_type,
        config.source_playlist_id,
        &config.mode,
        config.avoid_repeat,
        config.min_gap,
        excluded,
    )
    .await
    {
        Ok(sid) => sid,
        Err(e) => {
            tracing::warn!(station_id = %station_id, error = ?e, "AutoDJ: pick_song error");
            return Ok(false);
        }
    };

    if let Some(sid) = song_id {
        let next_pos: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(position), -1) + 1 FROM station_queue WHERE station_id = $1")
            .bind(station_id)
            .fetch_one(db)
            .await
            .db_error("failed to query next insert position")?;

        sqlx::query("INSERT INTO station_queue (station_id, song_id, position, is_auto_dj) VALUES ($1, $2, $3, true)")
            .bind(station_id)
            .bind(sid)
            .bind(next_pos)
            .execute(db)
            .await
            .db_error("failed to insert auto-DJ selection")?;

        crate::songs::analysis::spawn_analysis(db, sid, station_id, upload_dir);
        excluded.insert(sid);

        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) async fn fill_from_auto_dj_source(
    db: &PgPool,
    station_id: Uuid,
    config: &AutoFillConfig,
    upcoming_count: Option<i64>,
    upload_dir: &str,
) -> Result<(), AppError> {
    let mut lock = lock_station_queue(db, station_id).await?;
    let result = fill_from_auto_dj_source_locked(db, station_id, config, upcoming_count, upload_dir).await;
    unlock_station_queue(&mut lock, station_id).await;
    result
}

async fn fill_from_auto_dj_source_locked(
    db: &PgPool,
    station_id: Uuid,
    config: &AutoFillConfig,
    upcoming_count: Option<i64>,
    upload_dir: &str,
) -> Result<(), AppError> {
    let target = config.songs_ahead as i64;

    let upcoming_count = match upcoming_count {
        Some(count) => count,
        None => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM station_queue WHERE station_id = $1 AND position > (SELECT COALESCE(current_song_index, 0) FROM stations WHERE id = $1)"
            )
            .bind(station_id)
            .fetch_one(db)
            .await
            .db_error("failed to count upcoming for auto-fill")?
        }
    };

    if upcoming_count >= target {
        return Ok(());
    }

    // A queue without a current row needs one extra pick: the first inserted
    // row lands on the position the current track will occupy, so the
    // upcoming window needs `songs_ahead + 1` entries in total. The queue is
    // "functionally empty" when no unconsumed row remains — consumed rows can
    // still sit in the table (they are trimmed only after `played_limit`
    // plays), so a plain table-emptiness check would miss the exhausted case
    // and seed one short.
    let queue_empty: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1 FROM station_queue sq
             JOIN stations st ON st.id = sq.station_id
             WHERE sq.station_id = $1
               AND sq.id <> ALL(st.consumed_queue_item_ids)
         )",
    )
    .bind(station_id)
    .fetch_one(db)
    .await
    .db_error("failed to check whether the queue is empty")?;

    let mut need = target - upcoming_count;
    if queue_empty {
        need += 1;
    }
    tracing::debug!(station_id = %station_id, target, upcoming_count, queue_empty, need, "AutoDJ fill demand");
    let mut excluded = active_song_ids(db, station_id).await?;
    let mut added = 0i32;
    let mut attempts = 0;

    while added < need as i32 && attempts < need * 5 {
        attempts += 1;
        if pick_and_insert_song(db, station_id, config, upload_dir, &mut excluded).await? {
            added += 1;
        }
    }

    Ok(())
}

pub(crate) async fn fill_from_auto_config(
    db: &PgPool,
    station_id: Uuid,
    upcoming_count: Option<i64>,
    upload_dir: &str,
) -> Result<(), AppError> {
    let config = sqlx::query_as::<_, (bool, AutoDjMode, SourceType, Option<Uuid>, bool, i32, i32)>(
        "SELECT enabled, mode, source_type, source_playlist_id, avoid_artist_repeat, min_song_gap, songs_ahead FROM station_auto_fill WHERE station_id = $1",
    )
    .bind(station_id)
    .fetch_optional(db)
    .await
    .db_error("failed to load auto-fill config")?;

    let (enabled, mode, source_type, source_playlist_id, avoid_repeat, min_gap, songs_ahead) = match config {
        Some(c) => c,
        None => {
            tracing::warn!(station_id = %station_id, "AutoDJ: no config row found");
            return Ok(());
        }
    };

    if !enabled {
        tracing::warn!(station_id = %station_id, "AutoDJ: disabled");
        return Ok(());
    }

    let auto_config = AutoFillConfig {
        source_type,
        source_playlist_id,
        mode,
        avoid_repeat,
        min_gap,
        songs_ahead,
    };

    fill_from_auto_dj_source(db, station_id, &auto_config, upcoming_count, upload_dir).await
}
