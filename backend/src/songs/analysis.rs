use gstreamer as gst;
use gstreamer_pbutils as gst_pbutils;
use sqlx::PgPool;
use uuid::Uuid;

use crate::songs::handlers::resolve_audio_path;
use crate::songs::repository;

/// Cue/loudness analysis result attached to a song. Duration discovery uses
/// GStreamer when cue analysis is unavailable; cue and loudness data then
/// fall back to defaults.
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

fn discover_duration(path: &std::path::Path) -> Option<i32> {
    gst::init().ok()?;
    let uri = gst::glib::filename_to_uri(path, None).ok()?;
    let discoverer = gst_pbutils::Discoverer::new(gst::ClockTime::from_seconds(10)).ok()?;
    let info = discoverer.discover_uri(uri.as_str()).ok()?;
    info.duration().and_then(|duration| duration_seconds(duration.seconds_f64()))
}

fn finite(v: f64) -> Option<f32> {
    if v.is_finite() {
        Some(v as f32)
    } else {
        None
    }
}

/// Runs duration discovery and autocue-rs cue/loudness analysis off the async
/// runtime. Failures are returned as defaults; `analyzed_at` remains `None`
/// when cue analysis did not complete.
pub async fn analyze_audio(audio_full_path: &str) -> SongAnalysis {
    let path = std::path::PathBuf::from(audio_full_path);
    let opts = autocue_rs::CueOptions::default();

    let result = tokio::task::spawn_blocking(move || {
        let cues = autocue_rs::measure(&path, &opts).and_then(|points| autocue_rs::compute_cues(&points, &opts).map(|result| result.0));
        let duration = cues
            .as_ref()
            .ok()
            .and_then(|cues| duration_seconds(cues.duration))
            .or_else(|| discover_duration(&path));
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
            SongAnalysis::default()
        }
    }
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
}
