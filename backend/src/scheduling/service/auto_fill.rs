use std::collections::HashSet;

use rand::prelude::IndexedRandom;
use rand::RngExt;
use sqlx::PgConnection;
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

pub(crate) async fn lock_station_queue(connection: &mut PgConnection, station_id: Uuid) -> Result<(), AppError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(station_id.to_string())
        .execute(connection)
        .await
        .db_error("failed to lock AutoDJ queue")?;
    Ok(())
}

async fn count_upcoming(connection: &mut PgConnection, station_id: Uuid) -> Result<i64, AppError> {
    sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM station_queue sq
         JOIN stations st ON st.id = sq.station_id
         WHERE sq.station_id = $1
           AND sq.id <> ALL(st.consumed_queue_item_ids)
           AND (
               (st.current_queue_item_id IS NULL AND sq.position > st.current_song_index)
               OR (st.current_queue_item_id IS NOT NULL AND sq.id IS DISTINCT FROM st.current_queue_item_id)
           )",
    )
    .bind(station_id)
    .fetch_one(connection)
    .await
    .db_error("failed to count upcoming songs")
}
async fn fill_demand(connection: &mut PgConnection, station_id: Uuid, target: i64) -> Result<i64, AppError> {
    let upcoming = count_upcoming(&mut *connection, station_id).await?;
    if upcoming >= target {
        return Ok(0);
    }

    // Without an unconsumed current row, the first inserted song becomes
    // current and does not count toward the requested upcoming window.
    let queue_empty: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1 FROM station_queue sq
             JOIN stations st ON st.id = sq.station_id
             WHERE sq.station_id = $1
               AND sq.id <> ALL(st.consumed_queue_item_ids)
         )",
    )
    .bind(station_id)
    .fetch_one(&mut *connection)
    .await
    .db_error("failed to check whether the queue is empty")?;

    let needed = target - upcoming + i64::from(queue_empty);
    tracing::debug!(station_id = %station_id, target, upcoming, queue_empty, needed, "AutoDJ fill demand");
    Ok(needed)
}

async fn active_song_ids(connection: &mut PgConnection, station_id: Uuid) -> Result<HashSet<Uuid>, AppError> {
    sqlx::query_scalar(
        "SELECT sq.song_id
         FROM station_queue sq
         JOIN stations st ON st.id = sq.station_id
         WHERE sq.station_id = $1
           AND (sq.id IS NOT DISTINCT FROM st.current_queue_item_id
                OR sq.id <> ALL(st.consumed_queue_item_ids))",
    )
    .bind(station_id)
    .fetch_all(connection)
    .await
    .db_error("failed to load active queue songs")
    .map(|song_ids: Vec<Uuid>| song_ids.into_iter().collect())
}

pub(crate) async fn fill_from_playlist_locked(
    connection: &mut PgConnection,
    station_id: Uuid,
    playlist_id: Uuid,
    songs_ahead: Option<i32>,
    analyze: &mut Vec<Uuid>,
) -> Result<(), AppError> {
    let song_ids: Vec<Uuid> = sqlx::query_scalar("SELECT ps.song_id FROM playlist_songs ps WHERE ps.playlist_id = $1 ORDER BY ps.position")
        .bind(playlist_id)
        .fetch_all(&mut *connection)
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
            .fetch_optional(&mut *connection)
            .await
            .db_error("failed to load auto-fill songs_ahead")?
            .flatten()
            .map(i64::from)
            .unwrap_or(5),
    };

    let need = fill_demand(&mut *connection, station_id, target).await?;
    if need == 0 {
        return Ok(());
    }

    let next_pos: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(position), -1) + 1 FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(&mut *connection)
        .await
        .db_error("failed to find next queue position")?;

    let active_song_ids = active_song_ids(&mut *connection, station_id).await?;
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
        .execute(&mut *connection)
        .await
        .db_error("failed to enqueue playlist song")?;

        analyze.push(*song_id);
    }

    Ok(())
}

async fn pick_from_playlist(connection: &mut PgConnection, playlist_id: Uuid) -> Result<Vec<Uuid>, AppError> {
    sqlx::query_scalar("SELECT ps.song_id FROM playlist_songs ps WHERE ps.playlist_id = $1 ORDER BY ps.position")
        .bind(playlist_id)
        .fetch_all(&mut *connection)
        .await
        .db_error("failed to pick songs from playlist")
}

async fn pick_from_station_library(connection: &mut PgConnection, station_id: Uuid) -> Result<Vec<Uuid>, AppError> {
    sqlx::query_scalar("SELECT ss.song_id FROM station_songs ss WHERE ss.station_id = $1")
        .bind(station_id)
        .fetch_all(&mut *connection)
        .await
        .db_error("failed to pick from station library")
}

async fn pick_from_global_library(connection: &mut PgConnection) -> Result<Vec<Uuid>, AppError> {
    sqlx::query_scalar("SELECT id FROM songs")
        .fetch_all(&mut *connection)
        .await
        .db_error("failed to pick from global library")
}

async fn pick_weighted(connection: &mut PgConnection, station_id: Uuid, excluded: &HashSet<Uuid>) -> Result<Vec<Uuid>, AppError> {
    let weighted = sqlx::query_as::<_, (Uuid, i32)>(
        "SELECT safp.playlist_id, safp.weight FROM station_auto_fill_playlists safp WHERE safp.station_id = $1 AND safp.weight > 0",
    )
    .bind(station_id)
    .fetch_all(&mut *connection)
    .await
    .db_error("failed to load weighted playlists")?;

    let mut eligible = Vec::new();
    let mut repeat = Vec::new();
    for (playlist_id, weight) in weighted {
        let songs = pick_from_playlist(&mut *connection, playlist_id).await?;
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

async fn apply_mode_to_candidates(
    connection: &mut PgConnection,
    candidates: &[Uuid],
    mode: &AutoDjMode,
    station_id: Uuid,
) -> Result<Uuid, AppError> {
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
            .fetch_optional(&mut *connection)
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
            .fetch_optional(&mut *connection)
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
    connection: &mut PgConnection,
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
            Some(pid) => pick_from_playlist(&mut *connection, pid).await?,
            None => return Ok(None),
        },
        SourceType::StationLibrary => pick_from_station_library(&mut *connection, station_id).await?,
        SourceType::GlobalLibrary => pick_from_global_library(&mut *connection).await?,
        SourceType::WeightedPlaylists => pick_weighted(&mut *connection, station_id, excluded).await?,
    };

    if song_ids.is_empty() {
        tracing::warn!(station_id = %station_id, %source_type, "AutoDJ: no songs in source");
        return Ok(None);
    }
    let unique: Vec<_> = song_ids.iter().copied().filter(|song_id| !excluded.contains(song_id)).collect();
    let candidates = if unique.is_empty() { &song_ids } else { &unique };
    let selected = apply_mode_to_candidates(&mut *connection, candidates, mode, station_id).await?;

    let unique_artist_count: i64 = if avoid_repeat {
        sqlx::query_scalar("SELECT COUNT(DISTINCT artist) FROM songs WHERE id = ANY($1) AND artist != ''")
            .bind(&song_ids)
            .fetch_one(&mut *connection)
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
        .fetch_all(&mut *connection)
        .await
        .db_error("failed to query recent artists")?;

        let selected_artist: Option<String> = sqlx::query_scalar("SELECT artist FROM songs WHERE id = $1")
            .bind(selected)
            .fetch_optional(&mut *connection)
            .await
            .db_error("failed to find selected artist")?;

        if let Some(artist) = &selected_artist {
            if !artist.is_empty() && recent_artists.contains(artist) {
                let recent_ids: Vec<Uuid> = sqlx::query_scalar(
                    "SELECT sq.song_id FROM station_queue sq WHERE sq.station_id = $1 ORDER BY sq.position DESC LIMIT $2",
                )
                .bind(station_id)
                .bind(min_gap)
                .fetch_all(&mut *connection)
                .await
                .db_error("failed to find recent queue entries")?;

                let mut safe = Vec::new();
                let mut rng = rand::make_rng::<rand::rngs::StdRng>();
                for id in candidates {
                    if recent_ids.contains(id) {
                        continue;
                    }
                    let artist_name: Option<String> = sqlx::query_scalar("SELECT artist FROM songs WHERE id = $1")
                        .bind(id)
                        .fetch_optional(&mut *connection)
                        .await
                        .db_error("failed to check candidate artist")?;
                    if artist_name.as_ref().is_some_and(|artist| recent_artists.contains(artist)) {
                        continue;
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
    connection: &mut PgConnection,
    station_id: Uuid,
    config: &AutoFillConfig,
    excluded: &mut HashSet<Uuid>,
    analyze: &mut Vec<Uuid>,
) -> Result<bool, AppError> {
    let song_id = match pick_song_for_source(
        &mut *connection,
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
        Err(error) => {
            tracing::warn!(station_id = %station_id, ?error, "AutoDJ: pick_song error");
            return Ok(false);
        }
    };

    if let Some(song_id) = song_id {
        let next_pos: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(position), -1) + 1 FROM station_queue WHERE station_id = $1")
            .bind(station_id)
            .fetch_one(&mut *connection)
            .await
            .db_error("failed to query next insert position")?;

        sqlx::query("INSERT INTO station_queue (station_id, song_id, position, is_auto_dj) VALUES ($1, $2, $3, true)")
            .bind(station_id)
            .bind(song_id)
            .bind(next_pos)
            .execute(&mut *connection)
            .await
            .db_error("failed to insert auto-DJ selection")?;

        analyze.push(song_id);
        excluded.insert(song_id);

        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) async fn fill_from_auto_dj_source_locked(
    connection: &mut PgConnection,
    station_id: Uuid,
    config: &AutoFillConfig,
    analyze: &mut Vec<Uuid>,
) -> Result<(), AppError> {
    let need = fill_demand(&mut *connection, station_id, i64::from(config.songs_ahead)).await?;
    if need == 0 {
        return Ok(());
    }
    let mut excluded = active_song_ids(&mut *connection, station_id).await?;
    let mut added = 0i32;
    let mut attempts = 0;

    while added < need as i32 && attempts < need * 5 {
        attempts += 1;
        if pick_and_insert_song(&mut *connection, station_id, config, &mut excluded, analyze).await? {
            added += 1;
        }
    }

    Ok(())
}

pub(crate) async fn fill_from_auto_config_locked(
    connection: &mut PgConnection,
    station_id: Uuid,
    analyze: &mut Vec<Uuid>,
) -> Result<(), AppError> {
    let config = sqlx::query_as::<_, (bool, AutoDjMode, SourceType, Option<Uuid>, bool, i32, i32)>(
        "SELECT enabled, mode, source_type, source_playlist_id, avoid_artist_repeat, min_song_gap, songs_ahead FROM station_auto_fill WHERE station_id = $1",
    )
    .bind(station_id)
    .fetch_optional(&mut *connection)
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

    fill_from_auto_dj_source_locked(&mut *connection, station_id, &auto_config, analyze).await
}
