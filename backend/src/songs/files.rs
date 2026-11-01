//! Filesystem browser and mutations for the music library.
//!
//! Folders are implicit: there is no folder table, only each file's relative
//! `path`. This module treats the music directory on disk as the source of
//! truth, which makes empty folders visible and lets the panel create, move
//! and delete them. Song rows and `library_files` entries are kept in sync on
//! mutations so the lazy scanner does not mistake them for external changes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::AppError;
use crate::songs::cover;
use crate::songs::handlers::is_audio_file;
use crate::songs::models::Song;
use crate::songs::repository;
use crate::util::normalize_lexically;

const MAX_FOLDER_COVERS: usize = 4;
/// Upper bound on tag reads when a folder's songs all share the same art.
const MAX_COVER_SCAN: usize = 64;

#[derive(Debug, Clone, Copy)]
pub enum FileSort {
    Name,
    Size,
    Modified,
    Type,
}

impl FileSort {
    pub fn parse(value: Option<&str>) -> Self {
        match value.unwrap_or("name") {
            "size" => Self::Size,
            "modified" => Self::Modified,
            "type" => Self::Type,
            _ => Self::Name,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    pub fn parse(value: Option<&str>) -> Self {
        match value.unwrap_or("asc") {
            "desc" => Self::Desc,
            _ => Self::Asc,
        }
    }
}

/// Song metadata attached to a library file.
#[derive(Debug, Serialize)]
pub struct FileSong {
    pub id: Uuid,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i32,
    pub has_cover: bool,
    pub origin: String,
    pub missing_since: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<&Song> for FileSong {
    fn from(song: &Song) -> Self {
        Self {
            id: song.id,
            title: song.title.clone(),
            artist: song.artist.clone(),
            album: song.album.clone(),
            duration: song.duration,
            has_cover: song.has_cover,
            origin: song.origin.clone(),
            missing_since: song.missing_since,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FolderEntry {
    pub name: String,
    pub path: String,
    /// Up to [`MAX_FOLDER_COVERS`] song ids whose embedded art is distinct and
    /// representative of this folder's contents (recursive).
    pub covers: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct FileEntry {
    pub name: String,
    pub path: String,
    pub size: i64,
    pub modified_at: Option<i64>,
    pub is_audio: bool,
    pub song: Option<FileSong>,
}

#[derive(Debug, Serialize)]
pub struct DirectoryListing {
    pub path: String,
    pub parent: Option<String>,
    pub folders: Vec<FolderEntry>,
    pub files: Vec<FileEntry>,
}

/// Validates a client-supplied library-relative path. Rejects absolute paths,
/// `..`, backslashes and NUL. Returns the normalized `a/b/c` form; the root is
/// the empty string.
pub fn validate_relative_path(rel: &str) -> Result<String, AppError> {
    if rel.contains('\0') || rel.contains('\\') {
        return Err(AppError::bad_request("Invalid path".into()));
    }
    if rel.starts_with('/') {
        return Err(AppError::bad_request("Absolute paths are not allowed".into()));
    }
    let mut parts = Vec::new();
    for part in rel.split('/') {
        match part {
            "" | "." => {}
            ".." => return Err(AppError::bad_request("Path escapes the music directory".into())),
            _ => parts.push(part),
        }
    }
    Ok(parts.join("/"))
}

/// Resolves a validated relative path against the music directory. The root
/// (empty path) resolves to the music directory itself.
pub fn resolve_library_path(music_dir: &str, rel: &str) -> Result<PathBuf, AppError> {
    let rel = validate_relative_path(rel)?;
    let base = normalize_lexically(Path::new(music_dir));
    let joined = if rel.is_empty() {
        base.clone()
    } else {
        normalize_lexically(&base.join(&rel))
    };
    if joined == base || joined.starts_with(&base) {
        Ok(joined)
    } else {
        Err(AppError::bad_request("Path escapes the music directory".into()))
    }
}

fn join_relative(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn parent_of(rel: &str) -> Option<String> {
    if rel.is_empty() {
        None
    } else if let Some((parent, _)) = rel.rsplit_once('/') {
        Some(parent.to_string())
    } else {
        Some(String::new())
    }
}

fn unix_mtime(metadata: &std::fs::Metadata) -> Option<i64> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
}

/// Lists the immediate children of `path` (empty = library root). Audio files
/// get their song metadata attached. Symlinks and hidden entries are skipped,
/// matching the scanner.
pub async fn list_directory(
    db: &PgPool,
    music_dir: &str,
    path: &str,
    sort: FileSort,
    order: SortOrder,
) -> Result<DirectoryListing, AppError> {
    let rel = validate_relative_path(path)?;
    let dir = resolve_library_path(music_dir, &rel)?;
    let metadata = tokio::fs::symlink_metadata(&dir)
        .await
        .map_err(|_| AppError::not_found("Folder not found".into()))?;
    if !metadata.is_dir() {
        return Err(AppError::not_found("Folder not found".into()));
    }

    let mut folders = Vec::new();
    let mut files = Vec::new();
    let mut audio_paths = Vec::new();

    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .map_err(|e| AppError::internal(format!("Failed to list folder: {e}")))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::internal(format!("Failed to list folder: {e}")))?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let entry_meta = tokio::fs::symlink_metadata(entry.path())
            .await
            .map_err(|e| AppError::internal(format!("Failed to stat entry: {e}")))?;
        if entry_meta.file_type().is_symlink() {
            continue;
        }
        let entry_path = join_relative(&rel, &name);
        if entry_meta.is_dir() {
            folders.push(FolderEntry {
                name,
                path: entry_path,
                covers: Vec::new(),
            });
            continue;
        }
        if !entry_meta.is_file() {
            continue;
        }
        let is_audio = is_audio_file(&name);
        if is_audio {
            audio_paths.push(entry_path.clone());
        }
        files.push(FileEntry {
            name,
            path: entry_path,
            size: entry_meta.len() as i64,
            modified_at: unix_mtime(&entry_meta),
            is_audio,
            song: None,
        });
    }

    if !audio_paths.is_empty() {
        let songs = repository::find_songs_by_paths(db, &audio_paths).await?;
        let by_path: std::collections::HashMap<&str, &Song> = songs.iter().map(|song| (song.file_path.as_str(), song)).collect();
        for file in &mut files {
            if let Some(song) = by_path.get(file.path.as_str()) {
                file.song = Some(FileSong::from(*song));
            }
        }
    }

    attach_folder_covers(db, music_dir, &rel, &mut folders).await?;

    folders.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()).then_with(|| a.name.cmp(&b.name)));
    files.sort_by(|a, b| compare_files(a, b, sort, order));

    Ok(DirectoryListing {
        path: rel.clone(),
        parent: parent_of(&rel),
        folders,
        files,
    })
}

/// Fills in `covers` for every listed folder, using the cache when the songs
/// below the folder have not changed since it was written.
async fn attach_folder_covers(db: &PgPool, music_dir: &str, rel: &str, folders: &mut [FolderEntry]) -> Result<(), AppError> {
    if folders.is_empty() {
        return Ok(());
    }

    let signatures = repository::folder_cover_signatures(db, rel).await?;
    let signatures: HashMap<String, String> = signatures
        .into_iter()
        .map(|row| {
            (
                join_relative(rel, &row.folder),
                format!("{}:{}", row.song_count, row.max_updated_at.timestamp_millis()),
            )
        })
        .collect();

    let paths: Vec<String> = folders.iter().map(|folder| folder.path.clone()).collect();
    let cached = repository::find_folder_cover_cache(db, &paths).await?;
    let cached: HashMap<String, repository::FolderCoverCacheRow> = cached.into_iter().map(|row| (row.folder_path.clone(), row)).collect();

    for folder in folders.iter_mut() {
        let signature = signatures.get(&folder.path).cloned().unwrap_or_else(|| "0:0".to_string());

        if let Some(row) = cached.get(&folder.path) {
            if row.signature == signature {
                folder.covers = row.covers.clone();
                continue;
            }
        }

        let covers = compute_folder_covers(db, music_dir, &folder.path).await?;
        repository::upsert_folder_cover_cache(db, &folder.path, &signature, &covers).await?;
        folder.covers = covers;
    }

    Ok(())
}

/// Reads embedded art for a folder's songs until [`MAX_FOLDER_COVERS`] distinct
/// images are found. Songs are visited in path order; at most [`MAX_COVER_SCAN`]
/// tags are read so folders with one repeated cover stay cheap.
async fn compute_folder_covers(db: &PgPool, music_dir: &str, folder_path: &str) -> Result<Vec<Uuid>, AppError> {
    let songs = repository::find_cover_songs_under_path(db, folder_path).await?;
    let mut seen: Vec<[u8; 32]> = Vec::new();
    let mut covers = Vec::new();

    for (id, path) in songs.into_iter().take(MAX_COVER_SCAN) {
        let absolute = resolve_library_path(music_dir, &path)?;
        let Some(cover) = cover::read_embedded_cover(absolute.to_string_lossy().into_owned()).await else {
            continue;
        };
        let hash: [u8; 32] = Sha256::digest(&cover.data).into();
        if seen.contains(&hash) {
            continue;
        }
        seen.push(hash);
        covers.push(id);
        if covers.len() >= MAX_FOLDER_COVERS {
            break;
        }
    }

    Ok(covers)
}

fn compare_files(a: &FileEntry, b: &FileEntry, sort: FileSort, order: SortOrder) -> std::cmp::Ordering {
    let ordering = match sort {
        FileSort::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
        FileSort::Size => a.size.cmp(&b.size),
        FileSort::Modified => a.modified_at.unwrap_or(0).cmp(&b.modified_at.unwrap_or(0)),
        FileSort::Type => {
            let a_ext = a.name.rsplit_once('.').map(|(_, ext)| ext.to_lowercase()).unwrap_or_default();
            let b_ext = b.name.rsplit_once('.').map(|(_, ext)| ext.to_lowercase()).unwrap_or_default();
            a_ext.cmp(&b_ext).then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        }
    };
    match order {
        SortOrder::Asc => ordering,
        SortOrder::Desc => ordering.reverse(),
    }
}

/// Every folder under the library (including empty ones), relative paths.
pub async fn list_all_folders(music_dir: &str) -> Result<Vec<String>, AppError> {
    let base = normalize_lexically(Path::new(music_dir));
    if tokio::fs::create_dir_all(&base).await.is_err() {
        return Ok(Vec::new());
    }

    let mut folders = Vec::new();
    let mut stack = vec![(base.clone(), String::new())];
    while let Some((dir, rel)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| AppError::internal(format!("Failed to list folder: {e}")))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AppError::internal(format!("Failed to list folder: {e}")))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let entry_meta = tokio::fs::symlink_metadata(entry.path())
                .await
                .map_err(|e| AppError::internal(format!("Failed to stat entry: {e}")))?;
            if entry_meta.file_type().is_symlink() || !entry_meta.is_dir() {
                continue;
            }
            let child = join_relative(&rel, &name);
            folders.push(child.clone());
            stack.push((entry.path(), child));
        }
    }

    folders.sort_by_cached_key(|folder| folder.to_lowercase());
    Ok(folders)
}

pub async fn create_folder(music_dir: &str, rel: &str) -> Result<String, AppError> {
    let rel = validate_relative_path(rel)?;
    if rel.is_empty() {
        return Err(AppError::bad_request("Folder name is required".into()));
    }
    let path = resolve_library_path(music_dir, &rel)?;
    if tokio::fs::symlink_metadata(&path).await.is_ok() {
        return Err(AppError::conflict("A file or folder with that name already exists".into()));
    }
    tokio::fs::create_dir_all(&path)
        .await
        .map_err(|e| AppError::internal(format!("Failed to create folder: {e}")))?;
    Ok(rel)
}

/// Moves or renames a file or folder, updating every song row and scanner
/// tracking entry under the moved path.
pub async fn move_entry(db: &PgPool, music_dir: &str, from: &str, to: &str) -> Result<(), AppError> {
    let from = validate_relative_path(from)?;
    let to = validate_relative_path(to)?;
    if from.is_empty() || to.is_empty() {
        return Err(AppError::bad_request("Cannot move the library root".into()));
    }
    if from == to {
        return Ok(());
    }
    if to.starts_with(&format!("{from}/")) {
        return Err(AppError::bad_request("Cannot move a folder into itself".into()));
    }

    let source = resolve_library_path(music_dir, &from)?;
    let destination = resolve_library_path(music_dir, &to)?;
    tokio::fs::symlink_metadata(&source)
        .await
        .map_err(|_| AppError::not_found("Source not found".into()))?;
    if tokio::fs::symlink_metadata(&destination).await.is_ok() {
        return Err(AppError::conflict("A file or folder with that name already exists".into()));
    }

    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::internal(format!("Failed to prepare destination: {e}")))?;
    }
    tokio::fs::rename(&source, &destination)
        .await
        .map_err(|e| AppError::internal(format!("Failed to move entry: {e}")))?;

    // Database paths follow the file. A file has no children, so the same
    // prefix-based helpers work for both files and folders.
    repository::move_song_paths(db, &from, &to).await?;
    repository::move_library_file_paths(db, &from, &to).await?;
    Ok(())
}

/// Deletes a single file from disk. Song rows are handled by the caller (they
/// need queue-sync bookkeeping).
pub async fn delete_file_on_disk(music_dir: &str, rel: &str) -> Result<(), AppError> {
    let rel = validate_relative_path(rel)?;
    if rel.is_empty() {
        return Err(AppError::bad_request("Cannot delete the library root".into()));
    }
    let path = resolve_library_path(music_dir, &rel)?;
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|_| AppError::not_found("File not found".into()))?;
    if !metadata.is_file() {
        return Err(AppError::bad_request("Path is not a file".into()));
    }
    tokio::fs::remove_file(&path)
        .await
        .map_err(|e| AppError::internal(format!("Failed to delete file: {e}")))?;
    Ok(())
}

/// Recursively deletes a folder from disk. Song rows are handled by the
/// caller (they need queue-sync bookkeeping).
pub async fn delete_folder_on_disk(music_dir: &str, rel: &str) -> Result<(), AppError> {
    let rel = validate_relative_path(rel)?;
    if rel.is_empty() {
        return Err(AppError::bad_request("Cannot delete the library root".into()));
    }
    let path = resolve_library_path(music_dir, &rel)?;
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|_| AppError::not_found("Folder not found".into()))?;
    if !metadata.is_dir() {
        return Err(AppError::bad_request("Path is not a folder".into()));
    }
    tokio::fs::remove_dir_all(&path)
        .await
        .map_err(|e| AppError::internal(format!("Failed to delete folder: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_path_rejects_escape() {
        assert!(validate_relative_path("../etc").is_err());
        assert!(validate_relative_path("/etc").is_err());
        assert!(validate_relative_path("a/../../b").is_err());
        assert!(validate_relative_path("a\\b").is_err());
    }

    #[test]
    fn relative_path_normalizes() {
        assert_eq!(validate_relative_path("").unwrap(), "");
        assert_eq!(validate_relative_path("rock//2020/").unwrap(), "rock/2020");
        assert_eq!(validate_relative_path("./a/./b").unwrap(), "a/b");
    }

    #[test]
    fn parent_of_root_and_nested() {
        assert_eq!(parent_of(""), None);
        assert_eq!(parent_of("rock"), Some(String::new()));
        assert_eq!(parent_of("rock/2020"), Some("rock".to_string()));
    }
}
