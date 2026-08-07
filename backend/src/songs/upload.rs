use crate::errors::AppError;
use crate::songs::analysis::analyze_audio;
use crate::songs::handlers::{ext_from_filename, mime_from_ext, resolve_audio_path, save_uploaded_file};
use crate::songs::repository;
use sqlx::PgPool;
use uuid::Uuid;

pub struct ProcessedSong {
    pub id: Uuid,
}

struct Id3Tags {
    title: String,
    artist: String,
    album: String,
    duration: i32,
    cover_data: Option<(Vec<u8>, String)>,
}

struct SongMetadata {
    title: String,
    artist: String,
    album: String,
}

fn extract_id3_tags(audio_full_path: &str) -> Id3Tags {
    let mut title = String::new();
    let mut artist = String::new();
    let mut album = String::new();
    let mut duration = 0i32;
    let mut cover_data = None;

    if let Ok(tag) = id3::Tag::read_from_path(audio_full_path) {
        tracing::debug!(path = %audio_full_path, "Read ID3 tags successfully");
        for frame in tag.frames() {
            match frame.id() {
                "TIT2" => {
                    if let id3::frame::Content::Text(t) = frame.content() {
                        title = t.clone();
                    }
                }
                "TPE1" => {
                    if let id3::frame::Content::Text(a) = frame.content() {
                        artist = a.clone();
                    }
                }
                "TALB" => {
                    if let id3::frame::Content::Text(a) = frame.content() {
                        album = a.clone();
                    }
                }
                "TLEN" => {
                    if let id3::frame::Content::Text(t) = frame.content() {
                        duration = t.parse::<i32>().unwrap_or(0) / 1000;
                    }
                }
                "APIC" => {
                    if let id3::frame::Content::Picture(pic) = frame.content() {
                        if cover_data.is_none() && !pic.data.is_empty() {
                            cover_data = Some((pic.data.clone(), pic.mime_type.clone()));
                        }
                    }
                }
                _ => {}
            }
        }
    } else {
        tracing::debug!(path = %audio_full_path, "No ID3 tags found or failed to read");
    }

    Id3Tags {
        title,
        artist,
        album,
        duration,
        cover_data,
    }
}

fn resolve_song_metadata(
    original_name: &str,
    title_override: Option<&str>,
    artist_override: Option<&str>,
    album_override: Option<&str>,
    id3: &Id3Tags,
) -> SongMetadata {
    let name_without_ext = match original_name.rfind('.') {
        Some(dot) => original_name[..dot].to_string(),
        None => original_name.to_string(),
    };
    let (parsed_artist, parsed_title) = crate::metadata::parse_filename(&name_without_ext);

    let mut title = title_override.unwrap_or("").to_string();
    let mut artist = artist_override.unwrap_or("").to_string();
    let mut album = album_override.unwrap_or("").to_string();

    if title.is_empty() {
        title = id3.title.clone();
    }
    if title.is_empty() {
        title = parsed_title;
    }
    if title.is_empty() {
        title = name_without_ext;
    }

    if artist.is_empty() {
        artist = id3.artist.clone();
    }
    if artist.is_empty() {
        artist = parsed_artist;
    }

    if album.is_empty() {
        album = id3.album.clone();
    }

    SongMetadata { title, artist, album }
}

async fn enrich_from_external(api_key: &str, metadata: &SongMetadata) -> (SongMetadata, Option<(Vec<u8>, String)>) {
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
            if let Some(url) = &meta.cover_url {
                if let Ok(resp) = reqwest::get(url).await {
                    if let Ok(img_bytes) = resp.bytes().await {
                        cover = Some((img_bytes.to_vec(), "image/jpeg".to_string()));
                    }
                }
            }
        }
    }

    (SongMetadata { title, artist, album }, cover)
}

async fn save_cover(data: &[u8], mime: &str, upload_dir: &str) -> String {
    let ext = match mime {
        "image/png" => "png",
        _ => "jpg",
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

    let id3 = extract_id3_tags(&audio_full_path);
    let metadata = resolve_song_metadata(original_name, title_override, artist_override, album_override, &id3);
    let (metadata, cover) = if let Some(api_key) = lastfm_api_key {
        enrich_from_external(api_key, &metadata).await
    } else {
        (metadata, None)
    };

    let cover_path = if let Some((data, mime)) = cover {
        save_cover(&data, &mime, upload_dir).await
    } else if let Some((data, mime)) = &id3.cover_data {
        save_cover(data, mime, upload_dir).await
    } else {
        String::new()
    };

    let duration = if id3.duration > 0 {
        id3.duration
    } else {
        ((file_size as f64) / 16000.0).round() as i32
    }
    .max(1);

    let mut analysis = analyze_audio(&audio_full_path).await;
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
