use sqlx::PgPool;
use uuid::Uuid;

use crate::songs::handlers::resolve_audio_path;
use crate::songs::repository;

/// Cue/loudness analysis result attached to a song. Falls back to the
/// defaults (whole file is the cue region, no loudness data) when ffmpeg is
/// unavailable or analysis fails.
#[derive(Debug, Default)]
pub struct SongAnalysis {
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

fn finite(v: f64) -> Option<f32> {
    if v.is_finite() {
        Some(v as f32)
    } else {
        None
    }
}

/// Runs autocue-rs cue/loudness analysis over an audio file.
/// Analysis runs off the async runtime (ffmpeg is a blocking subprocess) and
/// never errors out: on any failure it falls back to [`SongAnalysis::default`]
/// (with `analyzed_at` left `None`, signalling "not analyzed").
pub async fn analyze_audio(audio_full_path: &str) -> SongAnalysis {
    let path = std::path::PathBuf::from(audio_full_path);
    let opts = autocue_rs::CueOptions::default();

    let result = tokio::task::spawn_blocking(move || {
        let points = autocue_rs::measure(&path, &opts)?;
        let (cues, _) = autocue_rs::compute_cues(&points, &opts)?;
        Ok::<_, autocue_rs::Error>(cues)
    })
    .await;

    match result {
        Ok(Ok(cues)) => SongAnalysis {
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
        _ => {
            tracing::warn!(path = %audio_full_path, "audio analysis failed, using defaults");
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
