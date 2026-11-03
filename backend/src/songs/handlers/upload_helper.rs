use std::path::PathBuf;

use crate::util::normalize_lexically;

pub(super) fn expand_files(files: Vec<(String, Vec<u8>)>) -> Result<Vec<(String, Vec<u8>)>, crate::errors::AppError> {
    expand_files_bounded(files, 500 * 1024 * 1024, 10_000)
}

fn expand_files_bounded(
    files: Vec<(String, Vec<u8>)>,
    max_bytes: u64,
    max_entries: usize,
) -> Result<Vec<(String, Vec<u8>)>, crate::errors::AppError> {
    use crate::errors::AppError;
    use std::io::Read;
    let limit_error = || AppError::bad_request("Archive exceeds the expanded size or entry limit".into()).with_code("INVALID_ARCHIVE");
    let mut result = Vec::new();
    let mut total = 0;
    let mut entries = 0;
    for (name, bytes) in files {
        if name.to_lowercase().ends_with(".zip") || bytes.starts_with(b"PK\x03\x04") {
            let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
                .map_err(|_| AppError::bad_request("Invalid ZIP archive".into()).with_code("INVALID_ARCHIVE"))?;
            entries += archive.len();
            if entries > max_entries {
                return Err(limit_error());
            }
            for i in 0..archive.len() {
                let entry = archive
                    .by_index(i)
                    .map_err(|_| AppError::bad_request("Invalid ZIP entry".into()).with_code("INVALID_ARCHIVE"))?;
                if entry.is_dir() || !is_audio_file(entry.name()) {
                    continue;
                }
                let name = crate::songs::import::sanitize_relative_path(entry.name())?;
                if entry.size() > max_bytes - total {
                    return Err(limit_error());
                }
                let mut bytes = Vec::new();
                entry
                    .take(max_bytes - total + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|_| AppError::bad_request("Failed to read ZIP entry".into()).with_code("INVALID_ARCHIVE"))?;
                total += bytes.len() as u64;
                if total > max_bytes {
                    return Err(limit_error());
                }
                result.push((name, bytes));
            }
        } else {
            entries += 1;
            total += bytes.len() as u64;
            if entries > max_entries || total > max_bytes {
                return Err(limit_error());
            }
            if is_audio_file(&name) {
                result.push((name, bytes));
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
#[test]
fn archive_limits_apply_to_expanded_bytes_and_entry_count() {
    use std::io::Write;
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for name in ["a.mp3", "b.mp3"] {
        zip.start_file(name, zip::write::SimpleFileOptions::default()).unwrap();
        zip.write_all(&[0; 32]).unwrap();
    }
    let bytes = zip.finish().unwrap().into_inner();
    assert!(expand_files_bounded(vec![("audio.zip".into(), bytes.clone())], 63, 2).is_err());
    assert!(expand_files_bounded(vec![("audio.zip".into(), bytes.clone())], 64, 1).is_err());
    assert_eq!(expand_files_bounded(vec![("audio.zip".into(), bytes)], 64, 2).unwrap().len(), 2);
}

/// Resolves a library-relative path (possibly with subfolders) against the
/// music directory. Absolute paths pass through (legacy escape hatch).
pub(crate) fn resolve_audio_path(music_dir: &str, filename: &str) -> String {
    if filename.starts_with('/') {
        filename.to_string()
    } else {
        let p = PathBuf::from(music_dir).join(filename);
        normalize_lexically(&p).to_string_lossy().into_owned()
    }
}

pub(crate) fn mime_from_ext(filename: &str) -> &'static str {
    match filename.rsplit('.').next().unwrap_or("").to_lowercase().as_str() {
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "flac" => "audio/flac",
        "aac" => "audio/aac",
        "m4a" => "audio/mp4",
        "wma" => "audio/x-ms-wma",
        _ => "application/octet-stream",
    }
}

pub(crate) fn cover_extension_from_mime(mime: &str) -> Option<&'static str> {
    match mime {
        "image/jpeg" | "image/jpg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        "image/avif" => Some("avif"),
        "image/bmp" => Some("bmp"),
        _ => None,
    }
}

pub(crate) fn is_audio_file(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    matches!(ext.as_str(), "mp3" | "wav" | "ogg" | "flac" | "aac" | "m4a" | "wma" | "opus")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mime_from_ext_mp3() {
        assert_eq!(mime_from_ext("song.mp3"), "audio/mpeg");
    }

    #[test]
    fn test_mime_from_ext_wav() {
        assert_eq!(mime_from_ext("song.wav"), "audio/wav");
    }

    #[test]
    fn test_mime_from_ext_flac() {
        assert_eq!(mime_from_ext("song.flac"), "audio/flac");
    }

    #[test]
    fn test_mime_from_ext_ogg() {
        assert_eq!(mime_from_ext("song.ogg"), "audio/ogg");
    }

    #[test]
    fn test_mime_from_ext_unknown() {
        assert_eq!(mime_from_ext("file.txt"), "application/octet-stream");
    }

    #[test]
    fn test_mime_from_ext_no_extension() {
        assert_eq!(mime_from_ext("noext"), "application/octet-stream");
    }

    #[test]
    fn test_is_audio_file_mp3() {
        assert!(is_audio_file("song.mp3"));
    }

    #[test]
    fn test_is_audio_file_wav() {
        assert!(is_audio_file("song.wav"));
    }

    #[test]
    fn test_is_audio_file_txt() {
        assert!(!is_audio_file("song.txt"));
    }

    #[test]
    fn test_is_audio_file_no_ext() {
        assert!(!is_audio_file("noext"));
    }

    #[test]
    fn test_resolve_audio_path_normal() {
        let result = resolve_audio_path("/music", "song.mp3");
        assert_eq!(result, "/music/song.mp3");
    }

    #[test]
    fn test_resolve_audio_path_with_subdir() {
        let result = resolve_audio_path("/music", "sub/song.mp3");
        assert_eq!(result, "/music/sub/song.mp3");
    }

    #[test]
    fn test_resolve_audio_path_absolute() {
        let result = resolve_audio_path("/music", "/absolute/path/song.mp3");
        assert_eq!(result, "/absolute/path/song.mp3");
    }

    #[test]
    fn test_cover_format_preserves_supported_image_type() {
        assert_eq!(cover_extension_from_mime("image/webp"), Some("webp"));
    }

    #[test]
    fn test_cover_format_rejects_raw_samples() {
        assert_eq!(cover_extension_from_mime("video/x-raw"), None);
    }
}
