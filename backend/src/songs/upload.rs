use crate::errors::AppError;
use crate::songs::analysis::{analyze_audio_with_discovered_duration, discover_media, DiscoveredMedia};
use crate::songs::handlers::{cover_extension_from_mime, ext_from_filename, mime_from_ext, resolve_audio_path, save_uploaded_file};
use crate::songs::repository;
use sqlx::PgPool;
use uuid::Uuid;

pub struct ProcessedSong {
    pub id: Uuid,
}

struct SongMetadata {
    title: String,
    artist: String,
    album: String,
}

fn sanitize_metadata_text(value: String) -> String {
    if !value.contains('\0') {
        return value;
    }
    value
        .split('\0')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" / ")
}

fn sanitize_metadata(mut metadata: SongMetadata) -> SongMetadata {
    metadata.title = sanitize_metadata_text(metadata.title);
    metadata.artist = sanitize_metadata_text(metadata.artist);
    metadata.album = sanitize_metadata_text(metadata.album);
    metadata
}

fn resolve_song_metadata(
    original_name: &str,
    title_override: Option<&str>,
    artist_override: Option<&str>,
    album_override: Option<&str>,
    discovered: &DiscoveredMedia,
) -> SongMetadata {
    let name_without_ext = match original_name.rfind('.') {
        Some(dot) => original_name[..dot].to_string(),
        None => original_name.to_string(),
    };
    let (parsed_artist, parsed_title) = crate::metadata::parse_filename(&name_without_ext);

    let mut title = sanitize_metadata_text(title_override.unwrap_or("").to_string());
    let mut artist = sanitize_metadata_text(artist_override.unwrap_or("").to_string());
    let mut album = sanitize_metadata_text(album_override.unwrap_or("").to_string());

    if title.is_empty() {
        title = sanitize_metadata_text(discovered.title.clone().unwrap_or_default());
    }
    if title.is_empty() {
        title = parsed_title;
    }
    if title.is_empty() {
        title = name_without_ext;
    }

    if artist.is_empty() {
        artist = sanitize_metadata_text(discovered.artist.clone().unwrap_or_default());
    }
    if artist.is_empty() {
        artist = parsed_artist;
    }

    if album.is_empty() {
        album = sanitize_metadata_text(discovered.album.clone().unwrap_or_default());
    }

    sanitize_metadata(SongMetadata { title, artist, album })
}

async fn enrich_from_external(api_key: &str, metadata: &SongMetadata, fetch_cover: bool) -> (SongMetadata, Option<(Vec<u8>, String)>) {
    let mut title = metadata.title.clone();
    let mut artist = metadata.artist.clone();
    let mut album = metadata.album.clone();
    let mut cover = None;

    if !title.is_empty() {
        if let Some(meta) = crate::metadata::enrich(api_key, &title, &artist).await {
            if title.is_empty() {
                title = meta.title;
            }
            if artist.is_empty() {
                artist = meta.artist;
            }
            if album.is_empty() {
                album = meta.album;
            }
            if fetch_cover {
                if let Some(url) = &meta.cover_url {
                    if let Ok(resp) = reqwest::get(url).await {
                        if let Ok(img_bytes) = resp.bytes().await {
                            cover = Some((img_bytes.to_vec(), "image/jpeg".to_string()));
                        }
                    }
                }
            }
        }
    }

    (SongMetadata { title, artist, album }, cover)
}

async fn save_cover(data: &[u8], mime: &str, upload_dir: &str) -> String {
    let Some(ext) = cover_extension_from_mime(mime) else {
        tracing::warn!(%mime, "Unsupported embedded cover format");
        return String::new();
    };
    let covers_dir = format!("{upload_dir}/covers");
    let _ = tokio::fs::create_dir_all(&covers_dir).await;
    let cover_filename = format!("{}.{}", Uuid::new_v4(), ext);
    let cover_file = format!("{covers_dir}/{cover_filename}");
    if let Err(e) = tokio::fs::write(&cover_file, data).await {
        tracing::warn!("Failed to save cover art: {e}");
        String::new()
    } else {
        cover_filename
    }
}

pub async fn process_song_upload(
    db: &PgPool,
    original_name: &str,
    bytes: &[u8],
    upload_dir: &str,
    user_id: Uuid,
    lastfm_api_key: Option<&str>,
    title_override: Option<&str>,
    artist_override: Option<&str>,
    album_override: Option<&str>,
) -> Result<ProcessedSong, AppError> {
    let ext = ext_from_filename(original_name);
    let mime = mime_from_ext(original_name);
    let file_path = save_uploaded_file(upload_dir, bytes, ext).await?;
    let audio_full_path = resolve_audio_path(upload_dir, &file_path);
    let file_size = bytes.len() as i64;

    let discovered = discover_media(&audio_full_path).await;
    let metadata = resolve_song_metadata(original_name, title_override, artist_override, album_override, &discovered);
    let (metadata, external_cover) = if let Some(api_key) = lastfm_api_key {
        enrich_from_external(api_key, &metadata, discovered.cover_data.is_none()).await
    } else {
        (metadata, None)
    };
    let metadata = sanitize_metadata(metadata);

    let cover_path = if let Some((data, mime)) = discovered.cover_data.as_ref().or(external_cover.as_ref()) {
        save_cover(data, mime, upload_dir).await
    } else {
        String::new()
    };

    let mut analysis = analyze_audio_with_discovered_duration(&audio_full_path, discovered.duration).await;
    let duration = analysis.duration.unwrap_or(1).max(1);
    // Fallback: play the whole file.
    if analysis.analyzed_at.is_none() {
        analysis.cue_out = duration as f64;
        analysis.cross_start_next = duration as f64;
    }

    let song_id = Uuid::new_v4();

    repository::insert_song_record(
        db,
        &repository::InsertSongParams {
            id: song_id,
            title: metadata.title.clone(),
            artist: metadata.artist.clone(),
            album: metadata.album.clone(),
            cover_path,
            file_path,
            file_size,
            mime_type: mime.to_string(),
            duration,
            uploaded_by: user_id,
            cue_in: analysis.cue_in,
            cue_out: analysis.cue_out,
            cross_start_next: analysis.cross_start_next,
            loudness: analysis.loudness,
            loudness_range: analysis.loudness_range,
            true_peak: analysis.true_peak,
            true_peak_db: analysis.true_peak_db,
            amplify: analysis.amplify,
            sustained_ending: analysis.sustained_ending,
            longtail: analysis.longtail,
            analyzed_at: analysis.analyzed_at,
        },
    )
    .await?;

    Ok(ProcessedSong { id: song_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_metadata_removes_nul_from_database_fields() {
        let metadata = sanitize_metadata(SongMetadata {
            title: "Song\0".to_string(),
            artist: "Primary\0Guest".to_string(),
            album: "\0Album\0\0".to_string(),
        });

        assert_eq!(metadata.title, "Song");
        assert_eq!(metadata.artist, "Primary / Guest");
        assert_eq!(metadata.album, "Album");
        assert!(!metadata.title.contains('\0'));
        assert!(!metadata.artist.contains('\0'));
        assert!(!metadata.album.contains('\0'));

        let fallback = resolve_song_metadata(
            "Filename Artist - Filename Title.flac",
            Some("\0"),
            Some("\0"),
            Some("\0"),
            &DiscoveredMedia {
                title: Some("\0".to_string()),
                artist: Some("\0".to_string()),
                album: Some("\0".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(fallback.title, "Filename Title");
        assert_eq!(fallback.artist, "Filename Artist");
        assert!(fallback.album.is_empty());
    }

    #[test]
    fn resolve_song_metadata_uses_gstreamer_tags_before_filename() {
        let metadata = resolve_song_metadata(
            "Filename Artist - Filename Title.flac",
            None,
            None,
            None,
            &DiscoveredMedia {
                title: Some("Tagged Title".to_string()),
                artist: Some("Tagged Artist".to_string()),
                album: Some("Tagged Album".to_string()),
                ..Default::default()
            },
        );

        assert_eq!(metadata.title, "Tagged Title");
        assert_eq!(metadata.artist, "Tagged Artist");
        assert_eq!(metadata.album, "Tagged Album");
    }
}
