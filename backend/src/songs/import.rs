use std::path::{Component, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::AppError;
use crate::songs::analysis::discover_media;
use crate::songs::handlers::{is_audio_file, mime_from_ext};
use crate::songs::models::SongOrigin;
use crate::songs::repository;
use crate::songs::upload::{enrich_from_external, resolve_song_metadata, sanitize_metadata};
use crate::util::normalize_lexically;

/// How the song file reached the music directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportOrigin {
    Api,
    Directory,
}

impl ImportOrigin {
    fn song_origin(self) -> SongOrigin {
        match self {
            Self::Api => SongOrigin::Api,
            Self::Directory => SongOrigin::Directory,
        }
    }
}

/// Everything the importer needs. The file must already exist on disk at
/// `music_dir/relative_path` (written by the API upload or dropped in
/// manually, e.g. over FTP). Tags and the filename are never modified.
pub struct ImportRequest {
    pub relative_path: String,
    pub origin: ImportOrigin,
    pub uploaded_by: Option<Uuid>,
    /// When false (default), an already-known audio hash stops the import
    /// with [`ImportOutcome::Duplicate`] instead of inserting a second row.
    pub allow_duplicate: bool,
    pub title_override: Option<String>,
    pub artist_override: Option<String>,
    pub album_override: Option<String>,
    pub lastfm_api_key: Option<String>,
}

/// Info about the already-known song blocking a duplicate import. Shown to
/// the user (panel notification) or returned to API callers so they can
/// confirm with `allow_duplicate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateInfo {
    pub song_id: Uuid,
    pub title: String,
    pub artist: String,
    pub file_path: String,
}

pub enum ImportOutcome {
    Created { song_id: Uuid },
    Duplicate { duplicate: DuplicateInfo },
}

/// Resolves a library-relative path against the music directory. Rejects
/// absolute paths and anything escaping the directory (`..`).
pub fn resolve_music_path(music_dir: &str, relative_path: &str) -> Result<String, AppError> {
    let base = normalize_lexically(&PathBuf::from(music_dir));
    let joined = normalize_lexically(&base.join(relative_path));
    let base_str = base.to_string_lossy();
    let joined_str = joined.to_string_lossy();
    if joined_str == base_str || joined_str.starts_with(&format!("{base_str}/")) {
        Ok(joined_str.into_owned())
    } else {
        Err(AppError::bad_request(format!("Path escapes the music directory: {relative_path}")))
    }
}

/// Sanitizes one path component: no separators, no NUL, never `.`/`..`.
fn sanitize_component(component: &str) -> String {
    let cleaned: String = component.chars().filter(|c| *c != '\0' && *c != '/').collect();
    let cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "untitled".to_string()
    } else if cleaned.len() > 200 {
        cleaned[..200].to_string()
    } else {
        cleaned
    }
}

/// Turns a user-supplied name (multipart filename, ZIP entry) into a safe
/// library-relative path, preserving subfolders. Returns an error for
/// absolute paths and for names that sanitize down to nothing useful.
pub fn sanitize_relative_path(name: &str) -> Result<String, AppError> {
    let path = PathBuf::from(name);
    if path.is_absolute() {
        return Err(AppError::bad_request(format!("Absolute paths are not allowed: {name}")));
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(sanitize_component(&part.to_string_lossy())),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(AppError::bad_request(format!("Invalid path in upload name: {name}")));
            }
        }
    }
    if parts.is_empty() {
        return Err(AppError::bad_request(format!("Invalid upload name: {name}")));
    }
    Ok(parts.join("/"))
}

/// Writes upload bytes into the music directory under the original name
/// (subfolders preserved). On a name collision with *different* content a
/// ` (2)`-style counter is appended; identical content reuses the path so
/// the importer can report the duplicate instead of writing twice.
pub async fn store_upload_bytes(music_dir: &str, original_name: &str, bytes: &[u8]) -> Result<String, AppError> {
    let relative = sanitize_relative_path(original_name)?;
    let new_hash = hex::encode(Sha256::digest(bytes));

    let mut candidate = relative.clone();
    let mut counter = 1u32;
    loop {
        let full = resolve_music_path(music_dir, &candidate)?;
        match tokio::fs::read(&full).await {
            Err(_) => break,
            Ok(existing) => {
                if hex::encode(Sha256::digest(&existing)) == new_hash {
                    return Ok(candidate);
                }
                counter += 1;
                if counter > 10_000 {
                    return Err(AppError::internal("Too many files with the same name".into()));
                }
                candidate = suffixed_name(&relative, counter);
            }
        }
    }

    let full = resolve_music_path(music_dir, &candidate)?;
    if let Some(parent) = PathBuf::from(&full).parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::internal(format!("Failed to prepare music directory: {e}")))?;
    }
    tokio::fs::write(&full, bytes)
        .await
        .map_err(|e| AppError::internal(format!("Failed to save uploaded file: {e}")))?;
    Ok(candidate)
}

fn suffixed_name(relative: &str, counter: u32) -> String {
    match relative.rfind('.') {
        Some(dot) if dot > 0 => format!("{} ({}).{}", &relative[..dot], counter, &relative[dot + 1..]),
        _ => format!("{relative} ({counter})"),
    }
}

/// Streams a file through SHA-256 without loading it into memory.
pub async fn sha256_file(absolute_path: &str) -> Result<String, AppError> {
    let path = absolute_path.to_string();
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let file = std::fs::File::open(&path).map_err(|e| format!("Failed to open file for hashing: {e}"))?;
        let mut reader = std::io::BufReader::new(file);
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf).map_err(|e| format!("Failed to hash file: {e}"))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hex::encode(hasher.finalize()))
    })
    .await
    .map_err(|e| AppError::internal(format!("Hash task failed: {e}")))?
    .map_err(AppError::internal)
}

/// Imports one file that already sits in the music directory. Never touches
/// tags or the filename; never runs autocue analysis (that happens when the
/// song reaches a station queue).
pub async fn import_song_file(db: &PgPool, music_dir: &str, req: &ImportRequest) -> Result<ImportOutcome, AppError> {
    if !is_audio_file(&req.relative_path) {
        return Err(AppError::bad_request(format!("Not an audio file: {}", req.relative_path)));
    }
    let audio_full_path = resolve_music_path(music_dir, &req.relative_path)?;
    let metadata = tokio::fs::metadata(&audio_full_path)
        .await
        .map_err(|e| AppError::not_found(format!("Song file not found on disk: {e}")))?;
    if !metadata.is_file() {
        return Err(AppError::bad_request(format!("Not a file: {}", req.relative_path)));
    }
    let file_size = metadata.len() as i64;

    let audio_hash = sha256_file(&audio_full_path).await?;
    if !req.allow_duplicate {
        if let Some(existing) = repository::find_song_by_audio_hash(db, &audio_hash).await? {
            return Ok(ImportOutcome::Duplicate {
                duplicate: DuplicateInfo {
                    song_id: existing.id,
                    title: existing.title,
                    artist: existing.artist,
                    file_path: existing.file_path,
                },
            });
        }
    }

    let file_name = PathBuf::from(&req.relative_path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| req.relative_path.clone());

    let discovered = discover_media(&audio_full_path).await;
    let resolved = resolve_song_metadata(
        &file_name,
        req.title_override.as_deref(),
        req.artist_override.as_deref(),
        req.album_override.as_deref(),
        &discovered,
    );
    let resolved = if let Some(api_key) = req.lastfm_api_key.as_deref() {
        enrich_from_external(api_key, &resolved).await
    } else {
        resolved
    };
    let resolved = sanitize_metadata(resolved);
    let has_cover = discovered.cover_data.is_some();

    let duration = discovered.duration.unwrap_or(1).max(1);
    let mime = mime_from_ext(&req.relative_path).to_string();
    let song_id = Uuid::new_v4();

    repository::insert_song_record(
        db,
        &repository::InsertSongParams {
            id: song_id,
            title: resolved.title,
            artist: resolved.artist,
            album: resolved.album,
            has_cover,
            file_path: req.relative_path.clone(),
            file_size,
            mime_type: mime,
            duration,
            uploaded_by: req.uploaded_by,
            origin: req.origin.song_origin().as_str().to_string(),
            audio_hash: Some(audio_hash),
            ..Default::default()
        },
    )
    .await?;

    Ok(ImportOutcome::Created { song_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_relative_path_keeps_subfolders() {
        assert_eq!(sanitize_relative_path("rock/ballads/song.mp3").unwrap(), "rock/ballads/song.mp3");
    }

    #[test]
    fn sanitize_relative_path_rejects_escape() {
        assert!(sanitize_relative_path("../evil.mp3").is_err());
        assert!(sanitize_relative_path("/absolute/song.mp3").is_err());
        assert!(sanitize_relative_path("ok/../../evil.mp3").is_err());
    }

    #[test]
    fn sanitize_relative_path_never_empty() {
        assert!(sanitize_relative_path("...").is_err() || !sanitize_relative_path("...").unwrap().is_empty());
        assert!(sanitize_relative_path("").is_err());
    }

    #[test]
    fn resolve_music_path_blocks_escape() {
        assert!(resolve_music_path("/music", "sub/song.mp3").is_ok());
        assert!(resolve_music_path("/music", "../evil.mp3").is_err());
        assert!(resolve_music_path("/music", "/etc/passwd").is_err());
    }

    #[test]
    fn suffixed_name_appends_counter_before_extension() {
        assert_eq!(suffixed_name("song.mp3", 2), "song (2).mp3");
        assert_eq!(suffixed_name("noext", 3), "noext (3)");
    }
}
