//! Rich file metadata for the song details dialog.
//!
//! Uses `lofty` to read container properties (duration, bitrate, sample rate,
//! channels, bit depth) and all textual tags. The DB remains the primary
//! source for library fields; this only enriches them.

use std::path::Path;

use serde::Serialize;

use crate::songs::files::resolve_library_path;

#[derive(Debug, Serialize)]
pub struct TagPair {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Default, Serialize)]
pub struct TrackDetails {
    /// Container/file type, e.g. `Mpeg`, `Flac`, `Mp4`.
    pub container: Option<String>,
    pub duration_ms: Option<u64>,
    pub bitrate_kbps: Option<u32>,
    pub sample_rate_hz: Option<u32>,
    pub channels: Option<u8>,
    pub bit_depth: Option<u8>,
    pub has_cover: bool,
    pub tags: Vec<TagPair>,
}

/// Reads metadata for a library file. Returns `None` when the file cannot be
/// parsed; callers then show DB fields only.
pub async fn read_track_details(music_dir: &str, relative_path: &str) -> Option<TrackDetails> {
    let path = resolve_library_path(music_dir, relative_path).ok()?;
    tokio::task::spawn_blocking(move || read_blocking(&path)).await.ok().flatten()
}

fn read_blocking(path: &Path) -> Option<TrackDetails> {
    use lofty::config::ParseOptions;
    use lofty::file::{AudioFile, TaggedFileExt};
    use lofty::probe::Probe;

    // Files with tags but no decodable audio frames (or truncated files) fail
    // the property pass; retry reading tags only so the dialog still shows
    // metadata.
    let tagged = Probe::open(path).ok()?.read().ok().or_else(|| {
        Probe::open(path)
            .ok()?
            .options(ParseOptions::new().read_properties(false))
            .read()
            .ok()
    })?;
    let properties = tagged.properties();
    let tag = tagged.primary_tag().or_else(|| tagged.first_tag());

    let duration_ms = {
        let millis = properties.duration().as_millis() as u64;
        (millis > 0).then_some(millis)
    };

    let mut details = TrackDetails {
        container: Some(format!("{:?}", tagged.file_type())),
        duration_ms,
        bitrate_kbps: properties.audio_bitrate().or_else(|| properties.overall_bitrate()),
        sample_rate_hz: properties.sample_rate(),
        channels: properties.channels(),
        bit_depth: properties.bit_depth(),
        has_cover: tag.map(|tag| tag.picture_count() > 0).unwrap_or(false),
        tags: Vec::new(),
    };

    if let Some(tag) = tag {
        details.tags = tag
            .items()
            .filter_map(|item| {
                let value = match item.value() {
                    lofty::tag::ItemValue::Text(text) | lofty::tag::ItemValue::Locator(text) => text.clone(),
                    lofty::tag::ItemValue::Binary(_) => return None,
                };
                let key = item
                    .key()
                    .map_key(tag.tag_type())
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("{:?}", item.key()));
                Some(TagPair { key, value })
            })
            .collect();
    }

    Some(details)
}
