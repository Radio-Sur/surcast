use std::path::PathBuf;

use uuid::Uuid;

use crate::errors::{AppError, DbResult};
use crate::util::normalize_lexically;

pub(crate) fn resolve_audio_path(upload_dir: &str, filename: &str) -> String {
    if filename.starts_with('/') || filename.contains('/') {
        filename.to_string()
    } else {
        let p = PathBuf::from(upload_dir).join("audio").join(filename);
        normalize_lexically(&p).to_string_lossy().into_owned()
    }
}

pub(crate) fn resolve_cover_path(upload_dir: &str, filename: &str) -> String {
    if filename.is_empty() {
        return String::new();
    }
    if filename.starts_with('/') || filename.contains('/') {
        filename.to_string()
    } else {
        let p = PathBuf::from(upload_dir).join("covers").join(filename);
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

pub(crate) fn cover_mime_from_filename(filename: &str) -> &'static str {
    match filename.rsplit('.').next().unwrap_or("") {
        "png" => "image/png",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "avif" => "image/avif",
        "bmp" => "image/bmp",
        _ => "image/jpeg",
    }
}

pub(crate) fn is_audio_file(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    matches!(ext.as_str(), "mp3" | "wav" | "ogg" | "flac" | "aac" | "m4a" | "wma" | "opus")
}

pub(crate) fn ext_from_filename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or("bin")
}

pub(crate) async fn save_uploaded_file(upload_dir: &str, bytes: &[u8], ext: &str) -> Result<String, AppError> {
    let audio_dir = normalize_lexically(&PathBuf::from(upload_dir).join("audio"));
    tokio::fs::create_dir_all(&audio_dir)
        .await
        .db_error("failed to prepare upload directory")?;

    let filename = format!("{}.{ext}", Uuid::new_v4());
    let path = audio_dir.join(&filename);
    tokio::fs::write(&path, bytes).await.db_error("failed to save uploaded file")?;

    Ok(filename)
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
    fn test_ext_from_filename_mp3() {
        assert_eq!(ext_from_filename("song.mp3"), "mp3");
    }

    #[test]
    fn test_ext_from_filename_flac() {
        assert_eq!(ext_from_filename("song.flac"), "flac");
    }

    #[test]
    fn test_ext_from_filename_no_ext() {
        assert_eq!(ext_from_filename("noext"), "noext");
    }

    #[test]
    fn test_ext_from_filename_hidden() {
        assert_eq!(ext_from_filename(".hidden"), "hidden");
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
        let result = resolve_audio_path("/uploads", "song.mp3");
        assert_eq!(result, "/uploads/audio/song.mp3");
    }

    #[test]
    fn test_resolve_audio_path_with_subdir() {
        let result = resolve_audio_path("/uploads", "sub/song.mp3");
        assert_eq!(result, "sub/song.mp3");
    }

    #[test]
    fn test_resolve_audio_path_absolute() {
        let result = resolve_audio_path("/uploads", "/absolute/path/song.mp3");
        assert_eq!(result, "/absolute/path/song.mp3");
    }

    #[test]
    fn test_resolve_cover_path_normal() {
        let result = resolve_cover_path("/uploads", "cover.jpg");
        assert_eq!(result, "/uploads/covers/cover.jpg");
    }

    #[test]
    fn test_resolve_cover_path_empty() {
        let result = resolve_cover_path("/uploads", "");
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_cover_path_with_subdir() {
        let result = resolve_cover_path("/uploads", "sub/cover.jpg");
        assert_eq!(result, "sub/cover.jpg");
    }

    #[test]
    fn test_cover_format_preserves_supported_image_type() {
        assert_eq!(cover_extension_from_mime("image/webp"), Some("webp"));
        assert_eq!(cover_mime_from_filename("cover.webp"), "image/webp");
    }

    #[test]
    fn test_cover_format_rejects_raw_samples() {
        assert_eq!(cover_extension_from_mime("video/x-raw"), None);
    }

    #[tokio::test]
    async fn test_save_uploaded_file_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let result = save_uploaded_file(path, b"fake content", "mp3").await;
        assert!(result.is_ok());
        let filename = result.unwrap();
        assert!(filename.ends_with(".mp3"));
        let full_path = format!("{path}/audio/{filename}");
        assert!(std::path::Path::new(&full_path).exists());
        let content = tokio::fs::read(&full_path).await.unwrap();
        assert_eq!(content, b"fake content");
    }
}
