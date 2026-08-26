use gst_pbutils::prelude::*;
use gstreamer as gst;
use gstreamer_pbutils as gst_pbutils;
use sqlx::PgPool;
use uuid::Uuid;

use crate::songs::handlers::{cover_extension_from_mime, resolve_audio_path};
use crate::songs::repository;

/// Metadata and duration discovered from decoded media by GStreamer.
#[derive(Debug, Default)]
pub(crate) struct DiscoveredMedia {
    pub(crate) duration: Option<i32>,
    pub(crate) title: Option<String>,
    pub(crate) artist: Option<String>,
    pub(crate) album: Option<String>,
    pub(crate) cover_data: Option<(Vec<u8>, String)>,
}

/// Cue/loudness analysis result attached to a song. Metadata discovery is
/// performed separately so upload and background analysis share one decoder-
/// independent fallback duration.
#[derive(Debug, Default)]
pub struct SongAnalysis {
    pub duration: Option<i32>,
    pub cue_in: f64,
    pub cue_out: f64,
    pub cross_start_next: f64,
    pub loudness: Option<f32>,
    pub loudness_range: Option<f32>,
    pub true_peak: Option<f32>,
    pub true_peak_db: Option<f32>,
    pub amplify: Option<f32>,
    pub sustained_ending: bool,
    pub longtail: bool,
    pub analyzed_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn duration_seconds(value: f64) -> Option<i32> {
    if value.is_finite() && value > 0.0 {
        Some(value.round().clamp(1.0, i32::MAX as f64) as i32)
    } else {
        None
    }
}

fn usable_tag_text(value: &str) -> bool {
    value.split('\0').any(|part| !part.trim().is_empty())
}

fn cover_from_sample(sample: &gst::Sample) -> Option<(Vec<u8>, String)> {
    let buffer = sample.buffer()?;
    let mapped = buffer.map_readable().ok()?;
    if mapped.is_empty() {
        return None;
    }
    let mime = sample.caps()?.structure(0)?.name().to_string();
    cover_extension_from_mime(&mime)?;
    Some((mapped.as_slice().to_vec(), mime))
}

fn cover_from_tags(tags: &gst::TagListRef) -> Option<(Vec<u8>, String)> {
    tags.iter_tag::<gst::tags::Image>()
        .find_map(|value| cover_from_sample(&value.get()))
        .or_else(|| {
            tags.iter_tag::<gst::tags::PreviewImage>()
                .find_map(|value| cover_from_sample(&value.get()))
        })
}

fn apply_tags(media: &mut DiscoveredMedia, tags: &gst::TagListRef) {
    if media.title.is_none() {
        media.title = tags
            .iter_tag::<gst::tags::Title>()
            .map(|value| value.get().to_string())
            .find(|value| usable_tag_text(value));
    }
    if media.artist.is_none() {
        media.artist = tags
            .iter_tag::<gst::tags::Artist>()
            .map(|value| value.get().to_string())
            .find(|value| usable_tag_text(value));
    }
    if media.album.is_none() {
        media.album = tags
            .iter_tag::<gst::tags::Album>()
            .map(|value| value.get().to_string())
            .find(|value| usable_tag_text(value));
    }
    if media.cover_data.is_none() {
        media.cover_data = cover_from_tags(tags);
    }
}

fn discover_info(path: &std::path::Path) -> Result<gst_pbutils::DiscovererInfo, String> {
    gst::init().map_err(|error| error.to_string())?;
    let uri = gst::glib::filename_to_uri(path, None).map_err(|error| error.to_string())?;
    let discoverer = gst_pbutils::Discoverer::new(gst::ClockTime::from_seconds(10)).map_err(|error| error.to_string())?;
    discoverer.discover_uri(uri.as_str()).map_err(|error| error.to_string())
}

fn discover_duration(path: &std::path::Path) -> Option<i32> {
    discover_info(path)
        .ok()?
        .duration()
        .and_then(|duration| duration_seconds(duration.seconds_f64()))
}

fn discover_media_blocking(path: &std::path::Path) -> Result<DiscoveredMedia, String> {
    let info = discover_info(path)?;

    let mut media = DiscoveredMedia {
        duration: info.duration().and_then(|duration| duration_seconds(duration.seconds_f64())),
        ..Default::default()
    };
    for container in info.container_streams() {
        if let Some(tags) = container.tags() {
            apply_tags(&mut media, &tags);
        }
    }
    for audio in info.audio_streams() {
        if let Some(tags) = audio.tags() {
            apply_tags(&mut media, &tags);
        }
    }
    Ok(media)
}

pub(crate) async fn discover_media(audio_full_path: &str) -> DiscoveredMedia {
    let path = std::path::PathBuf::from(audio_full_path);
    match tokio::task::spawn_blocking(move || discover_media_blocking(&path)).await {
        Ok(Ok(media)) => media,
        Ok(Err(error)) => {
            tracing::debug!(path = %audio_full_path, %error, "GStreamer media discovery failed");
            DiscoveredMedia::default()
        }
        Err(error) => {
            tracing::warn!(path = %audio_full_path, %error, "GStreamer media discovery task failed");
            DiscoveredMedia::default()
        }
    }
}

fn finite(v: f64) -> Option<f32> {
    if v.is_finite() {
        Some(v as f32)
    } else {
        None
    }
}

#[derive(Clone, Copy)]
enum DurationFallback {
    Discover,
    AlreadyDiscovered(Option<i32>),
}

impl DurationFallback {
    fn known(self) -> Option<i32> {
        match self {
            Self::Discover => None,
            Self::AlreadyDiscovered(duration) => duration,
        }
    }
}

async fn analyze_audio_inner(audio_full_path: &str, duration_fallback: DurationFallback) -> SongAnalysis {
    let path = std::path::PathBuf::from(audio_full_path);
    let opts = autocue_rs::CueOptions::default();

    let result = tokio::task::spawn_blocking(move || {
        let cues = autocue_rs::measure(&path, &opts).and_then(|points| autocue_rs::compute_cues(&points, &opts).map(|result| result.0));
        let duration = cues
            .as_ref()
            .ok()
            .and_then(|cues| duration_seconds(cues.duration))
            .or_else(|| match duration_fallback {
                DurationFallback::Discover => discover_duration(&path),
                DurationFallback::AlreadyDiscovered(duration) => duration,
            });
        (cues, duration)
    })
    .await;

    match result {
        Ok((Ok(cues), duration)) => SongAnalysis {
            duration,
            cue_in: cues.cue_in,
            cue_out: cues.cue_out,
            cross_start_next: cues.cross_start_next,
            loudness: finite(cues.loudness),
            loudness_range: finite(cues.loudness_range),
            true_peak: finite(cues.true_peak),
            true_peak_db: finite(cues.true_peak_db),
            amplify: finite(cues.amplify),
            sustained_ending: cues.sustained_ending,
            longtail: cues.longtail,
            analyzed_at: Some(chrono::Utc::now()),
        },
        Ok((Err(error), duration)) => {
            tracing::warn!(path = %audio_full_path, %error, "audio analysis failed, using defaults");
            SongAnalysis {
                duration,
                ..Default::default()
            }
        }
        Err(error) => {
            tracing::warn!(path = %audio_full_path, %error, "audio analysis task failed, using defaults");
            SongAnalysis {
                duration: duration_fallback.known(),
                ..Default::default()
            }
        }
    }
}

/// Runs autocue-rs cue/loudness analysis off the async runtime and discovers a
/// fallback duration only if cue analysis fails.
pub async fn analyze_audio(audio_full_path: &str) -> SongAnalysis {
    analyze_audio_inner(audio_full_path, DurationFallback::Discover).await
}

pub(crate) async fn analyze_audio_with_discovered_duration(audio_full_path: &str, duration: Option<i32>) -> SongAnalysis {
    analyze_audio_inner(audio_full_path, DurationFallback::AlreadyDiscovered(duration)).await
}

/// Fire-and-forget: analyze a song in the background when it is added to a
/// station's queue. Only runs when the station uses the `autocue` transition
/// mode and the song has no analysis yet. Never fails — all errors are logged
/// and ignored, leaving the song un-analyzed for a later retry.
pub async fn ensure_analyzed(db: PgPool, song_id: Uuid, station_id: Uuid, upload_dir: String) {
    let station = match crate::stations::repository::find_station_by_id(&db, station_id).await {
        Ok(Some(s)) => s,
        _ => {
            tracing::warn!(station_id = %station_id, "queue analysis: station not found");
            return;
        }
    };
    if station.transition_mode != "autocue" {
        return;
    }

    let song = match repository::find_song_by_id(&db, song_id).await {
        Ok(Some(s)) => s,
        _ => {
            tracing::warn!(song_id = %song_id, "queue analysis: song not found");
            return;
        }
    };
    if song.analyzed_at.is_some() {
        return;
    }

    let audio_full_path = resolve_audio_path(&upload_dir, &song.file_path);
    let analysis = analyze_audio(&audio_full_path).await;
    if analysis.analyzed_at.is_none() {
        return;
    }

    if let Err(e) = repository::update_song_analysis(&db, song_id, &analysis).await {
        tracing::warn!(song_id = %song_id, error = ?e, "queue analysis: failed to persist analysis");
    }
}

/// Spawns [`ensure_analyzed`] on the background runtime. This is the fire-and-
/// forget entry point used at every place songs are added to a station queue.
pub fn spawn_analysis(db: &PgPool, song_id: Uuid, station_id: Uuid, upload_dir: &str) {
    tokio::spawn(ensure_analyzed(db.clone(), song_id, station_id, upload_dir.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use id3::TagLike;

    const COVER_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII=";

    fn cover_bytes() -> Vec<u8> {
        base64::engine::general_purpose::STANDARD.decode(COVER_BASE64).unwrap()
    }

    #[test]
    fn tag_text_requires_content_after_sanitization() {
        assert!(!usable_tag_text("\0 \0"));
        assert!(usable_tag_text("Artist\0Guest"));
    }

    fn write_tagged_mp3(path: &std::path::Path) {
        gst::init().unwrap();
        let pipeline = gst::parse::launch(
            "audiotestsrc num-buffers=128 ! audioconvert ! lamemp3enc target=bitrate cbr=true bitrate=128 ! mpegaudioparse ! filesink name=output",
        )
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap();
        pipeline.by_name("output").unwrap().set_property("location", path.to_str().unwrap());
        pipeline.set_state(gst::State::Playing).unwrap();
        let message = pipeline
            .bus()
            .unwrap()
            .timed_pop_filtered(gst::ClockTime::from_seconds(10), &[gst::MessageType::Eos, gst::MessageType::Error])
            .expect("MP3 encoder pipeline must reach EOS");
        pipeline.set_state(gst::State::Null).unwrap();
        assert_eq!(message.type_(), gst::MessageType::Eos, "{message:?}");

        let mut tag = id3::Tag::new();
        tag.set_title("GStreamer Title");
        tag.set_artist("GStreamer Artist");
        tag.set_album("GStreamer Album");
        tag.add_frame(id3::frame::Picture {
            mime_type: "image/png".to_string(),
            picture_type: id3::frame::PictureType::CoverFront,
            description: String::new(),
            data: cover_bytes(),
        });
        tag.write_to_path(path, id3::Version::Id3v24).unwrap();
    }

    fn write_wav(path: &std::path::Path, duration_seconds: u32) {
        let sample_rate = 44_100u32;
        let samples = sample_rate * duration_seconds;
        let data_len = samples * 2;
        let mut wav = Vec::with_capacity((44 + data_len) as usize);
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data_len).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&sample_rate.to_le_bytes());
        wav.extend_from_slice(&(sample_rate * 2).to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_len.to_le_bytes());
        for index in 0..samples {
            let sample = if index % 32 < 16 { 8_000i16 } else { -8_000i16 };
            wav.extend_from_slice(&sample.to_le_bytes());
        }
        std::fs::write(path, wav).unwrap();
    }

    #[tokio::test]
    async fn analyze_audio_reports_decoded_duration() {
        let file = tempfile::Builder::new().suffix(".wav").tempfile().unwrap();
        write_wav(file.path(), 2);

        let analysis = analyze_audio(file.path().to_str().unwrap()).await;
        assert_eq!(analysis.duration, Some(2));
    }

    #[tokio::test]
    async fn discover_media_reads_text_and_cover_tags() {
        let file = tempfile::Builder::new().suffix(".mp3").tempfile().unwrap();
        write_tagged_mp3(file.path());

        let media = discover_media(file.path().to_str().unwrap()).await;

        assert_eq!(media.title.as_deref(), Some("GStreamer Title"));
        assert_eq!(media.artist.as_deref(), Some("GStreamer Artist"));
        assert_eq!(media.album.as_deref(), Some("GStreamer Album"));
        assert_eq!(media.cover_data, Some((cover_bytes(), "image/png".to_string())));
    }
}
