use std::path::PathBuf;

use crate::util::normalize_lexically;

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
