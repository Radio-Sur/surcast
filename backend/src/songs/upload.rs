use crate::songs::analysis::DiscoveredMedia;

pub(crate) struct SongMetadata {
    pub(crate) title: String,
    pub(crate) artist: String,
    pub(crate) album: String,
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

pub(crate) fn sanitize_metadata(mut metadata: SongMetadata) -> SongMetadata {
    metadata.title = sanitize_metadata_text(metadata.title);
    metadata.artist = sanitize_metadata_text(metadata.artist);
    metadata.album = sanitize_metadata_text(metadata.album);
    metadata
}

pub(crate) fn resolve_song_metadata(
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

/// Fills missing text metadata (title/artist/album) from an external source.
/// Cover art is never fetched: covers always come from the file's own tags.
pub(crate) async fn enrich_from_external(api_key: &str, metadata: &SongMetadata) -> SongMetadata {
    let mut title = metadata.title.clone();
    let mut artist = metadata.artist.clone();
    let mut album = metadata.album.clone();

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
        }
    }

    SongMetadata { title, artist, album }
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
