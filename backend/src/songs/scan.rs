use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::AppError;
use crate::songs::handlers::is_audio_file;
use crate::songs::import::{import_song_file, sha256_file, ImportOrigin, ImportOutcome, ImportRequest};
use crate::songs::repository;
use crate::util::normalize_lexically;

/// Minimum seconds between two full scans; more frequent calls get a cached
/// `throttled` report instead of touching the disk again.
pub const SCAN_THROTTLE_SECS: i64 = 60;
/// Files modified more recently than this are still being written (e.g. an
/// FTP upload in progress) and are skipped until the next scan.
pub const SCAN_STABLE_SECS: u64 = 30;
/// Caps the one-time hash backfill of pre-existing rows per scan so the
/// first library open on a huge collection stays fast.
pub const BACKFILL_PER_SCAN: i64 = 100;

#[derive(Debug, Default, Serialize)]
pub struct ScanReport {
    pub throttled: bool,
    pub scanned_files: i64,
    pub added: i64,
    pub renamed: i64,
    pub reimported: i64,
    pub marked_missing: i64,
    pub restored: i64,
    pub skipped_duplicates: i64,
    pub backfilled: i64,
}

struct DiskFile {
    relative_path: String,
    size: i64,
    mtime: i64,
}

fn unix_mtime(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Recursively lists audio files under the music directory. Symlinks are
/// never followed; hidden and temporary/partial files are ignored. Paths
/// come back relative to `music_dir` with `/` separators.
async fn list_disk_files(music_dir: &str) -> Result<Vec<DiskFile>, AppError> {
    let base = normalize_lexically(&PathBuf::from(music_dir));
    tokio::fs::create_dir_all(&base)
        .await
        .map_err(|e| AppError::internal(format!("Failed to prepare music directory: {e}")))?;

    let mut out = Vec::new();
    let mut stack = vec![base.clone()];
    while let Some(dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| AppError::internal(format!("Failed to list music directory: {e}")))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AppError::internal(format!("Failed to list music directory: {e}")))?
        {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if file_name.starts_with('.') {
                continue;
            }
            let symlink_meta = tokio::fs::symlink_metadata(entry.path())
                .await
                .map_err(|e| AppError::internal(format!("Failed to stat library file: {e}")))?;
            if symlink_meta.file_type().is_symlink() {
                continue;
            }
            if symlink_meta.is_dir() {
                stack.push(entry.path());
                continue;
            }
            if !symlink_meta.is_file() || !is_audio_file(&file_name) {
                continue;
            }
            let lower = file_name.to_lowercase();
            if lower.ends_with(".part") || lower.ends_with(".tmp") || lower.ends_with(".crdownload") {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&base)
                .map_err(|_| AppError::internal("Library file escaped the music directory".into()))?
                .to_string_lossy()
                .replace('\\', "/");
            out.push(DiskFile {
                relative_path: relative,
                size: symlink_meta.len() as i64,
                mtime: unix_mtime(&symlink_meta),
            });
        }
    }
    Ok(out)
}

/// Lazy library scan. Imports files dropped into the music directory
/// (FTP/manual copies), follows renames/moves by content hash, refreshes
/// changed files in place and flags deleted ones as missing. Never runs
/// autocue analysis — that happens when a song reaches a station queue.
pub async fn scan_library(db: &PgPool, music_dir: &str, lastfm_api_key: Option<&str>) -> Result<ScanReport, AppError> {
    if let Some(last) = repository::get_last_scan_at(db).await? {
        let age = now_unix() - last.timestamp();
        if age < SCAN_THROTTLE_SECS {
            return Ok(ScanReport {
                throttled: true,
                ..Default::default()
            });
        }
    }

    let mut report = ScanReport::default();
    let disk = list_disk_files(music_dir).await?;
    report.scanned_files = disk.len() as i64;

    let known: HashMap<String, repository::LibraryFile> = repository::list_library_files(db)
        .await?
        .into_iter()
        .map(|row| (row.path.clone(), row))
        .collect();
    let mut seen_paths: HashSet<&str> = HashSet::new();
    let stable_before = now_unix() - SCAN_STABLE_SECS as i64;

    for file in &disk {
        seen_paths.insert(file.relative_path.as_str());
        match known.get(&file.relative_path) {
            // Known path, untouched: nothing to do (clears a stale missing
            // flag — the file is provably back).
            Some(row) if row.file_size == file.size && row.mtime == file.mtime => {
                if let Some(song_id) = row.song_id {
                    if let Some(song) = repository::find_song_by_id(db, song_id).await? {
                        if song.missing_since.is_some() {
                            repository::clear_song_missing(db, song_id).await?;
                            report.restored += 1;
                        }
                    }
                }
                repository::upsert_library_file(db, &file.relative_path, row.song_id, row.file_hash.clone(), file.size, file.mtime).await?;
            }
            // Same path, content changed (or first sight of a pre-existing
            // row): re-hash, then re-import in place or fix the tracking row.
            Some(row) => {
                if file.mtime > stable_before {
                    continue;
                }
                let full = crate::songs::import::resolve_music_path(music_dir, &file.relative_path)?;
                let hash = sha256_file(&full).await?;
                if Some(&hash) == row.file_hash.as_ref() {
                    repository::upsert_library_file(db, &file.relative_path, row.song_id, Some(hash), file.size, file.mtime).await?;
                    continue;
                }
                match row.song_id {
                    Some(song_id) if repository::find_song_by_id(db, song_id).await?.is_some() => {
                        refresh_track(db, music_dir, lastfm_api_key, song_id, &file.relative_path, file.size).await?;
                        repository::upsert_library_file(db, &file.relative_path, Some(song_id), Some(hash), file.size, file.mtime).await?;
                        report.reimported += 1;
                    }
                    _ => {
                        import_new_track(db, music_dir, lastfm_api_key, &file.relative_path, &mut report).await?;
                    }
                }
            }
            // Unknown path: brand-new file, or a rename/move/copy. Hash this
            // one file and match by content — the rest of the library is
            // never re-read.
            None => {
                if file.mtime > stable_before {
                    continue;
                }
                let full = crate::songs::import::resolve_music_path(music_dir, &file.relative_path)?;
                let hash = sha256_file(&full).await?;
                match repository::find_song_by_audio_hash(db, &hash).await? {
                    Some(song) if song.file_path != file.relative_path => {
                        // Rename/move when the old path is gone, a copy when
                        // it is still there.
                        if !seen_paths.contains(song.file_path.as_str()) {
                            repository::update_song_path(db, song.id, &file.relative_path, file.size).await?;
                            repository::delete_library_file(db, &song.file_path).await?;
                            repository::upsert_library_file(db, &file.relative_path, Some(song.id), Some(hash), file.size, file.mtime)
                                .await?;
                            report.renamed += 1;
                        } else {
                            repository::upsert_library_file(db, &file.relative_path, Some(song.id), Some(hash), file.size, file.mtime)
                                .await?;
                            report.skipped_duplicates += 1;
                        }
                    }
                    Some(song) => {
                        // Same path re-appeared (e.g. after a deletion flag).
                        repository::upsert_library_file(db, &file.relative_path, Some(song.id), Some(hash), file.size, file.mtime).await?;
                        if song.missing_since.is_some() {
                            repository::clear_song_missing(db, song.id).await?;
                            report.restored += 1;
                        }
                    }
                    None => {
                        import_new_track(db, music_dir, lastfm_api_key, &file.relative_path, &mut report).await?;
                    }
                }
            }
        }
    }

    // Tracked paths with no file on disk that were not consumed as rename
    // sources: deleted. The song row (and its history) stays, flagged.
    for (path, row) in &known {
        if seen_paths.contains(path.as_str()) {
            continue;
        }
        if let Some(song_id) = row.song_id {
            // Only flag when the song's own path is gone (rename targets
            // were already handled above).
            if let Some(song) = repository::find_song_by_id(db, song_id).await? {
                if song.file_path == *path && song.missing_since.is_none() {
                    repository::mark_song_missing(db, song_id).await?;
                    report.marked_missing += 1;
                }
                continue;
            }
        }
        repository::delete_library_file(db, path).await?;
    }

    // One-time backfill of content hashes for rows imported before hashing
    // existed (capped per scan). Needed so future renames match by content.
    report.backfilled = backfill_hashes(db, music_dir).await?;

    repository::set_last_scan_at(db).await?;
    Ok(report)
}

async fn import_new_track(
    db: &PgPool,
    music_dir: &str,
    lastfm_api_key: Option<&str>,
    relative_path: &str,
    report: &mut ScanReport,
) -> Result<(), AppError> {
    let req = ImportRequest {
        relative_path: relative_path.to_string(),
        origin: ImportOrigin::Directory,
        uploaded_by: None,
        allow_duplicate: false,
        title_override: None,
        artist_override: None,
        album_override: None,
        lastfm_api_key: lastfm_api_key.map(str::to_string),
    };
    match import_song_file(db, music_dir, &req).await? {
        ImportOutcome::Created { song_id } => {
            let full = crate::songs::import::resolve_music_path(music_dir, relative_path)?;
            let meta = tokio::fs::metadata(&full)
                .await
                .map_err(|e| AppError::internal(format!("Failed to stat new song: {e}")))?;
            let hash = sha256_file(&full).await?;
            repository::upsert_library_file(db, relative_path, Some(song_id), Some(hash), meta.len() as i64, unix_mtime(&meta)).await?;
            report.added += 1;
        }
        ImportOutcome::Duplicate { duplicate } => {
            let full = crate::songs::import::resolve_music_path(music_dir, relative_path)?;
            let meta = tokio::fs::metadata(&full)
                .await
                .map_err(|e| AppError::internal(format!("Failed to stat new song: {e}")))?;
            let hash = sha256_file(&full).await?;
            repository::upsert_library_file(
                db,
                relative_path,
                Some(duplicate.song_id),
                Some(hash),
                meta.len() as i64,
                unix_mtime(&meta),
            )
            .await?;
            report.skipped_duplicates += 1;
        }
    }
    Ok(())
}

async fn refresh_track(
    db: &PgPool,
    music_dir: &str,
    lastfm_api_key: Option<&str>,
    song_id: Uuid,
    relative_path: &str,
    file_size: i64,
) -> Result<(), AppError> {
    use crate::songs::handlers::mime_from_ext;
    use crate::songs::upload::{enrich_from_external, resolve_song_metadata, sanitize_metadata};

    let full = crate::songs::import::resolve_music_path(music_dir, relative_path)?;
    let hash = sha256_file(&full).await?;
    let discovered = crate::songs::analysis::discover_media(&full).await;
    let file_name = relative_path.rsplit('/').next().unwrap_or(relative_path);
    let resolved = resolve_song_metadata(file_name, None, None, None, &discovered);
    let resolved = if let Some(api_key) = lastfm_api_key {
        enrich_from_external(api_key, &resolved).await
    } else {
        resolved
    };
    let resolved = sanitize_metadata(resolved);
    let has_cover = discovered.cover_data.is_some();

    repository::refresh_song_file(
        db,
        song_id,
        &resolved.title,
        &resolved.artist,
        &resolved.album,
        discovered.duration.unwrap_or(1).max(1),
        file_size,
        mime_from_ext(relative_path),
        Some(hash),
        has_cover,
    )
    .await
}

async fn backfill_hashes(db: &PgPool, music_dir: &str) -> Result<i64, AppError> {
    let rows: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id, file_path FROM songs WHERE audio_hash IS NULL AND missing_since IS NULL LIMIT $1")
            .bind(BACKFILL_PER_SCAN)
            .fetch_all(db)
            .await
            .map_err(|e| AppError::internal(format!("Failed to list songs for hash backfill: {e}")))?;
    let mut done = 0i64;
    for (id, file_path) in rows {
        // Legacy rows either keep a bare UUID filename inside the music dir
        // or (rarely) an absolute path from the old escape hatch.
        let full = if PathBuf::from(&file_path).is_absolute() {
            file_path.clone()
        } else {
            match crate::songs::import::resolve_music_path(music_dir, &file_path) {
                Ok(p) => p,
                Err(_) => continue,
            }
        };
        if !tokio::fs::try_exists(&full).await.unwrap_or(false) {
            continue;
        }
        let Ok(hash) = sha256_file(&full).await else {
            continue;
        };
        sqlx::query("UPDATE songs SET audio_hash = $1 WHERE id = $2 AND audio_hash IS NULL")
            .bind(&hash)
            .bind(id)
            .execute(db)
            .await
            .map_err(|e| AppError::internal(format!("Failed to backfill song hash: {e}")))?;
        done += 1;
    }
    Ok(done)
}
