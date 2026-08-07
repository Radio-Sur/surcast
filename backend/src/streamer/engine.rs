use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

use super::crossfade::{CrossfadeConfig, PlaybackConfig};
use tokio::net::TcpStream;

use super::backend::StreamBackend;
use super::queue_manager::QueueManager;
use super::{PlaybackParams, SongInfo, StatusEvent};

pub struct PlaybackEngine {
    pub db: sqlx::PgPool,
    pub station_id: uuid::Uuid,
    pub mount: String,
    pub prebuffer_bytes: i32,
    pub queue: Arc<QueueManager>,
    pub stream_backend: Arc<dyn StreamBackend>,
    pub playing: AtomicBool,
    pub stopped: AtomicBool,
    pub song_started_at: Mutex<Instant>,
}

impl PlaybackEngine {
    pub fn new(
        db: sqlx::PgPool,
        station_id: uuid::Uuid,
        mount: String,
        prebuffer_bytes: i32,
        queue: Arc<QueueManager>,
        stream_backend: Arc<dyn StreamBackend>,
    ) -> Self {
        Self {
            db,
            station_id,
            mount,
            prebuffer_bytes: prebuffer_bytes.max(1024),
            queue,
            stream_backend,
            playing: AtomicBool::new(true),
            stopped: AtomicBool::new(false),
            song_started_at: Mutex::new(Instant::now()),
        }
    }

    pub fn is_playing(&self) -> bool {
        self.playing.load(Ordering::Acquire)
    }

    pub fn set_playing(&self, playing: bool) {
        self.playing.store(playing, Ordering::Release);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub fn set_stopped(&self, stopped: bool) {
        self.stopped.store(stopped, Ordering::Release);
    }

    pub fn elapsed_secs(&self) -> u64 {
        self.song_started_at.lock().unwrap_or_else(|e| e.into_inner()).elapsed().as_secs()
    }

    pub fn mark_song_started(&self) {
        *self.song_started_at.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }

    async fn connect(&self) -> Result<TcpStream, String> {
        self.stream_backend.connect(&self.mount, &self.db).await
    }

    async fn connect_if_needed(&self, stream: &mut Option<TcpStream>) -> bool {
        if stream.is_some() {
            return true;
        }
        match self.connect().await {
            Ok(s) => {
                *stream = Some(s);
                true
            }
            Err(e) => {
                tracing::warn!(
                    mount = %self.mount,
                    error = %e,
                    "Streamer connect failed, retrying in 2s"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
                false
            }
        }
    }

    pub(super) async fn disconnect(&self, stream: &mut Option<TcpStream>) {
        if let Some(mut s) = stream.take() {
            let _ = tokio::time::timeout(Duration::from_millis(500), s.shutdown()).await;
        }
    }

    pub async fn run_playback_loop(self: Arc<Self>) {
        let mut stream: Option<TcpStream> = None;

        loop {
            if self.handle_idle_state(&mut stream).await.is_err() {
                if self.stopped.load(Ordering::Acquire) {
                    break;
                }
                continue;
            }

            let idx = self.queue.current_idx();
            let info = match self.queue.current_song_info() {
                Some(i) => i,
                None => continue,
            };
            let path = info.file_path.clone();

            let bytes = match self.read_song_file(&path).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            let bytes = Self::skip_id3_header(&bytes).to_vec();

            let next_info = self.queue.peek_next_song();
            let params = self
                .compute_playback_params(&info, next_info.as_ref(), self.prebuffer_bytes, &bytes)
                .await;

            if params.has_fade && params.fade_chunks > 0 {
                let Some(next) = params.next.as_ref() else {
                    break;
                };
                let cf_config = CrossfadeConfig {
                    bitrate: params.bitrate,
                    chunk_size: params.chunk_size,
                    chunk_duration: params.chunk_duration,
                    total_chunks: params.total_chunks,
                    pre_idx: params.pre_idx,
                    prebuffer_chunks: params.prebuffer_chunks,
                    cur_start: params.cur_start,
                    cur_cut: params.cur_cut,
                    cur_end: params.cur_end,
                    next_start: params.next_start,
                    fade_secs: params.fade_secs,
                    actual_fade: params.actual_fade,
                };
                let ok = self.play_crossfade(&mut stream, &info, next, idx, &cf_config).await;
                if ok {
                    self.queue.advance_idx(1);
                    self.queue.advance_song().await;
                }
            } else {
                let pb_config = PlaybackConfig {
                    chunks: bytes.chunks(params.chunk_size).map(|c| c.to_vec()).collect(),
                    chunk_duration: params.chunk_duration,
                    total_chunks: params.total_chunks,
                    pre_idx: params.pre_idx,
                };
                let ok = self.play_normal(&mut stream, &pb_config).await;
                if ok {
                    if let Some(s) = stream.as_mut() {
                        let _ = s.flush().await;
                    }
                    self.queue.advance_idx(1);
                    self.queue.advance_song().await;
                }
            }
        }
    }

    async fn handle_idle_state(&self, stream: &mut Option<TcpStream>) -> Result<(), ()> {
        if self.stopped.load(Ordering::Acquire) {
            self.disconnect(stream).await;
            return Err(());
        }

        let songs_len = self.queue.song_count();
        if songs_len == 0 {
            self.disconnect(stream).await;
            if let Err(e) =
                crate::scheduling::service::fill_queue_from_schedule(&self.db, self.station_id, None, &self.queue.upload_dir).await
            {
                tracing::warn!(
                    station_id = %self.station_id,
                    error = ?e,
                    "AutoDJ idle error"
                );
            }
            self.queue.reload_from_db().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            return Err(());
        }

        let idx = self.queue.current_idx();
        if idx >= songs_len {
            self.disconnect(stream).await;
            if let Err(e) =
                crate::scheduling::service::fill_queue_from_schedule(&self.db, self.station_id, None, &self.queue.upload_dir).await
            {
                tracing::warn!(
                    station_id = %self.station_id,
                    error = ?e,
                    "AutoDJ idle error"
                );
            }
            self.queue.reload_from_db().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            return Err(());
        }

        if !self.playing.load(Ordering::Acquire) {
            self.disconnect(stream).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            return Err(());
        }

        if self.stopped.load(Ordering::Acquire) {
            self.disconnect(stream).await;
            return Err(());
        }

        let info = match self.queue.song_info(idx) {
            Some(i) => i,
            None => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                return Err(());
            }
        };

        if !self.connect_if_needed(stream).await {
            return Err(());
        }

        self.mark_song_started();
        let total = self.queue.song_count();
        if self
            .queue
            .status_tx
            .send(StatusEvent::SongChange {
                song_index: idx,
                total,
                elapsed: 0,
                title: info.title.clone(),
                artist: info.artist.clone(),
                duration: info.duration,
            })
            .is_err()
        {
            tracing::debug!("No status listeners for station {}", self.queue.station_id);
        }

        Ok(())
    }

    async fn compute_playback_params(
        &self,
        info: &SongInfo,
        next_info: Option<&SongInfo>,
        prebuffer_bytes: i32,
        bytes: &[u8],
    ) -> PlaybackParams {
        let pre_idx = self.queue.current_idx();
        let total_bytes = bytes.len();
        let duration_secs = info.duration.max(1) as f64;
        let (bitrate, chunk_size, chunk_duration, prebuffer_chunks) =
            Self::compute_chunk_params(total_bytes, duration_secs, prebuffer_bytes);

        let total_chunks = total_bytes.div_ceil(chunk_size);

        let station_settings: (i32, String, i32) = sqlx::query_as(
            "SELECT COALESCE(default_fade_ms, 0), transition_mode, COALESCE(autocue_fade_max_ms, 5000) FROM stations WHERE id = $1",
        )
        .bind(self.station_id)
        .fetch_optional(&self.db)
        .await
        .ok()
        .flatten()
        .unwrap_or((0, "crossfade".to_string(), 5000));
        let (station_fade_ms, transition_mode, autocue_fade_max_ms) = station_settings;
        let fade_secs = (station_fade_ms.clamp(0, 15000) as f64) / 1000.0;
        let autocue_cap_secs = (autocue_fade_max_ms.clamp(0, 15000) as f64) / 1000.0;

        let transition = compute_transition(&transition_mode, info, next_info, fade_secs, autocue_cap_secs);
        let (has_fade, fade_secs_req, geom) = match transition {
            Transition::Fade(g) => (true, g.fade_secs, g),
            Transition::Normal => (false, 0.0, TransitionGeometry::default()),
        };
        let fade_chunks = if has_fade {
            (fade_secs_req / chunk_duration.as_secs_f64()).ceil() as usize
        } else {
            0
        };
        let fade_chunks = fade_chunks.min(total_chunks);
        let actual_fade = if fade_chunks > 0 {
            fade_chunks as f64 * chunk_duration.as_secs_f64()
        } else {
            0.0
        };

        PlaybackParams {
            total_chunks,
            pre_idx,
            chunk_size,
            chunk_duration,
            bitrate,
            prebuffer_chunks,
            fade_secs: fade_secs_req,
            fade_chunks,
            has_fade,
            actual_fade,
            cur_start: geom.cur_start,
            cur_cut: geom.cur_cut,
            cur_end: geom.cur_end,
            next_start: geom.next_start,
            next: next_info.cloned(),
        }
    }

    async fn read_song_file(&self, path: &str) -> Result<Vec<u8>, ()> {
        match tokio::fs::read(path).await {
            Ok(b) => Ok(b),
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "Streamer failed to read song file");
                self.queue.advance_idx(1);
                self.queue.advance_song().await;
                Err(())
            }
        }
    }

    fn skip_id3_header(data: &[u8]) -> &[u8] {
        let data_start = data
            .windows(3)
            .position(|w| w == b"ID3")
            .and_then(|pos| {
                if pos + 10 <= data.len() {
                    let size = ((data[pos + 6] as usize) << 21)
                        | ((data[pos + 7] as usize) << 14)
                        | ((data[pos + 8] as usize) << 7)
                        | (data[pos + 9] as usize);
                    Some(pos + 10 + size)
                } else {
                    None
                }
            })
            .unwrap_or(0);
        &data[data_start..]
    }

    fn compute_chunk_params(total_bytes: usize, duration_secs: f64, prebuffer_bytes: i32) -> (f64, usize, Duration, usize) {
        let raw_bitrate = total_bytes as f64 / duration_secs;
        let bitrate = if raw_bitrate > 256_000.0 { 16384.0 } else { raw_bitrate };
        let chunk_size: usize = 16384;
        let chunk_duration = if bitrate > 0.0 {
            Duration::from_secs_f64(chunk_size as f64 / bitrate)
        } else {
            Duration::from_millis(500)
        };
        let prebuffer_chunks = (prebuffer_bytes as usize / chunk_size).max(1);
        (bitrate, chunk_size, chunk_duration, prebuffer_chunks)
    }
}

/// Where a crossfade starts and ends, expressed as absolute timestamps within
/// the current and next audio files.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TransitionGeometry {
    pub cur_start: f64,
    pub cur_cut: f64,
    pub cur_end: f64,
    pub next_start: f64,
    pub fade_secs: f64,
}

impl Default for TransitionGeometry {
    fn default() -> Self {
        Self {
            cur_start: 0.0,
            cur_cut: 0.0,
            cur_end: 0.0,
            next_start: 0.0,
            fade_secs: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Transition {
    Fade(TransitionGeometry),
    Normal,
}

/// Duration-based crossfade: overlap the last `fade` seconds of the current
/// song with the first `fade` seconds of the next one.
fn naive_transition(cur_duration: f64, next_duration: f64, fade_secs: f64) -> Transition {
    if fade_secs <= 0.0 {
        return Transition::Normal;
    }
    let fade = fade_secs.min(cur_duration).min(next_duration.max(0.0));
    if fade <= 0.0 {
        return Transition::Normal;
    }
    Transition::Fade(TransitionGeometry {
        cur_start: 0.0,
        cur_cut: cur_duration - fade,
        cur_end: cur_duration,
        next_start: 0.0,
        fade_secs: fade,
    })
}

/// Decides how to transition into the next song based on the station's
/// `transition_mode`.
///
/// * `crossfade` — naive duration-based fade (or no fade when `fade_secs` is 0).
/// * `autocue` — cue-point-driven fade: current song plays from `cue_in` up to
///   `cross_start_next`, then overlaps its tail (`cross_start_next` → `cue_out`)
///   with the head of the next song (starting at its `cue_in`). Falls back to a
///   naive fade when either song is un-analyzed, the tail is too short, or no
///   next song exists.
/// * `off` — no transition.
fn compute_transition(mode: &str, cur: &SongInfo, next: Option<&SongInfo>, fade_secs: f64, autocue_cap_secs: f64) -> Transition {
    let cur_duration = cur.duration.max(1) as f64;
    let next_duration = next.map(|n| n.duration.max(1) as f64).unwrap_or(0.0);
    match mode {
        "off" => Transition::Normal,
        "autocue" => {
            if let Some(nxt) = next {
                if cur.analyzed && nxt.analyzed {
                    let cur_start = cur.cue_in.max(0.0);
                    let cur_cut = cur.cross_start_next;
                    let cur_end = cur.cue_out;
                    let next_start = nxt.cue_in.max(0.0);
                    if cur_cut >= cur_start && cur_end > cur_cut {
                        let tail = cur_end - cur_cut;
                        let next_avail = (next_duration - next_start).max(0.0);
                        let fade = tail.min(next_avail).min(autocue_cap_secs.max(0.0));
                        if fade >= 0.2 {
                            return Transition::Fade(TransitionGeometry {
                                cur_start,
                                cur_cut,
                                cur_end,
                                next_start,
                                fade_secs: fade,
                            });
                        }
                    }
                }
                naive_transition(cur_duration, next_duration, fade_secs)
            } else {
                Transition::Normal
            }
        }
        _ => {
            if next.is_some() {
                naive_transition(cur_duration, next_duration, fade_secs)
            } else {
                Transition::Normal
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_id3_data(version: u8, size: usize) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(b"ID3");
        data.push(version);
        data.push(0x00);
        data.push(0x00);
        data.push(((size >> 21) & 0x7F) as u8);
        data.push(((size >> 14) & 0x7F) as u8);
        data.push(((size >> 7) & 0x7F) as u8);
        data.push((size & 0x7F) as u8);
        data.extend(std::iter::repeat(0x00).take(size));
        data
    }

    #[test]
    fn test_skip_id3_v23_header() {
        let mut data = make_id3_data(3, 100);
        data.extend_from_slice(b"audio data");
        let result = PlaybackEngine::skip_id3_header(&data);
        assert_eq!(result, b"audio data");
    }

    #[test]
    fn test_skip_id3_v24_header() {
        let mut data = make_id3_data(4, 100);
        data.extend_from_slice(b"audio data");
        let result = PlaybackEngine::skip_id3_header(&data);
        assert_eq!(result, b"audio data");
    }

    #[test]
    fn test_skip_id3_no_header() {
        let data = b"just audio data";
        let result = PlaybackEngine::skip_id3_header(data);
        assert_eq!(result, b"just audio data");
    }

    #[test]
    fn test_skip_id3_shorter_than_10() {
        let data = b"ID3xxx";
        let result = PlaybackEngine::skip_id3_header(data);
        assert_eq!(result, b"ID3xxx");
    }

    #[test]
    fn test_skip_id3_empty() {
        let data = b"";
        let result = PlaybackEngine::skip_id3_header(data);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_chunk_params_normal() {
        let (bitrate, chunk_size, chunk_duration, prebuffer_chunks) = PlaybackEngine::compute_chunk_params(44100, 1.0, 16384);
        assert!(bitrate > 0.0);
        assert_eq!(chunk_size, 16384);
        assert_eq!(prebuffer_chunks, 1);
        let expected_dur = Duration::from_secs_f64(16384.0 / 44100.0);
        let tolerance = Duration::from_millis(1);
        assert!((chunk_duration - expected_dur).as_secs_f64().abs() < tolerance.as_secs_f64());
    }

    #[test]
    fn test_compute_chunk_params_high_bitrate() {
        let (bitrate, chunk_size, chunk_duration, prebuffer_chunks) = PlaybackEngine::compute_chunk_params(1_000_000, 1.0, 16384);
        assert_eq!(bitrate, 16384.0);
        assert_eq!(chunk_size, 16384);
        assert_eq!(chunk_duration, Duration::from_secs_f64(1.0));
        assert_eq!(prebuffer_chunks, 1);
    }

    #[test]
    fn test_compute_chunk_params_zero_bytes() {
        let (bitrate, chunk_size, chunk_duration, prebuffer_chunks) = PlaybackEngine::compute_chunk_params(0, 1.0, 0);
        assert_eq!(bitrate, 0.0);
        assert_eq!(chunk_size, 16384);
        assert_eq!(chunk_duration, Duration::from_millis(500));
        assert_eq!(prebuffer_chunks, 1);
    }

    #[test]
    fn test_compute_chunk_params_large_prebuffer() {
        let (_, _, _, prebuffer_chunks) = PlaybackEngine::compute_chunk_params(44100, 1.0, 32768);
        assert_eq!(prebuffer_chunks, 2);
    }

    fn make_song(duration: f64, cue_in: f64, cue_out: f64, cross_start_next: f64, analyzed: bool) -> SongInfo {
        SongInfo {
            song_id: "1".into(),
            title: "t".into(),
            artist: "a".into(),
            duration: duration.round() as i32,
            file_path: "/tmp/x.mp3".into(),
            position: 0,
            cue_in,
            cue_out,
            cross_start_next,
            analyzed,
        }
    }

    fn assert_fade(t: Transition, cur_start: f64, cur_cut: f64, cur_end: f64, next_start: f64, fade: f64) {
        match t {
            Transition::Fade(g) => {
                assert!((g.cur_start - cur_start).abs() < 1e-6);
                assert!((g.cur_cut - cur_cut).abs() < 1e-6);
                assert!((g.cur_end - cur_end).abs() < 1e-6);
                assert!((g.next_start - next_start).abs() < 1e-6);
                assert!((g.fade_secs - fade).abs() < 1e-6);
            }
            Transition::Normal => panic!("expected a fade transition"),
        }
    }

    #[test]
    fn test_crossfade_mode_naive() {
        let cur = make_song(10.0, 0.0, 0.0, 0.0, false);
        let next = make_song(12.0, 0.0, 0.0, 0.0, false);
        let t = compute_transition("crossfade", &cur, Some(&next), 3.0, 5.0);
        assert_fade(t, 0.0, 7.0, 10.0, 0.0, 3.0);
    }

    #[test]
    fn test_crossfade_mode_zero_fade_is_normal() {
        let cur = make_song(10.0, 0.0, 0.0, 0.0, false);
        let next = make_song(12.0, 0.0, 0.0, 0.0, false);
        let t = compute_transition("crossfade", &cur, Some(&next), 0.0, 5.0);
        assert_eq!(t, Transition::Normal);
    }

    #[test]
    fn test_off_mode_is_normal() {
        let cur = make_song(10.0, 0.5, 9.0, 8.0, true);
        let next = make_song(12.0, 0.5, 11.0, 10.0, true);
        let t = compute_transition("off", &cur, Some(&next), 3.0, 5.0);
        assert_eq!(t, Transition::Normal);
    }

    #[test]
    fn test_autocue_analyzed_uses_cue_points() {
        let cur = make_song(20.0, 1.5, 18.0, 14.0, true);
        let next = make_song(20.0, 2.0, 19.0, 17.0, true);
        // tail = 18 - 14 = 4.0, next_avail = 18.0, cap = 5.0 -> fade 4.0
        let t = compute_transition("autocue", &cur, Some(&next), 0.0, 5.0);
        assert_fade(t, 1.5, 14.0, 18.0, 2.0, 4.0);
    }

    #[test]
    fn test_autocue_cap_clamps_tail() {
        let cur = make_song(30.0, 1.0, 28.0, 10.0, true);
        let next = make_song(30.0, 2.0, 29.0, 20.0, true);
        // tail = 18.0, cap = 3.0 -> fade 3.0
        let t = compute_transition("autocue", &cur, Some(&next), 0.0, 3.0);
        assert_fade(t, 1.0, 10.0, 28.0, 2.0, 3.0);
    }

    #[test]
    fn test_autocue_unanalyzed_falls_back_to_naive() {
        let cur = make_song(10.0, 0.0, 0.0, 0.0, false);
        let next = make_song(12.0, 0.0, 0.0, 0.0, false);
        // fade_secs 0 (typical in autocue UI) -> no fade
        let t = compute_transition("autocue", &cur, Some(&next), 0.0, 5.0);
        assert_eq!(t, Transition::Normal);

        // with a nonzero station fade -> naive fade
        let t = compute_transition("autocue", &cur, Some(&next), 2.0, 5.0);
        assert_fade(t, 0.0, 8.0, 10.0, 0.0, 2.0);
    }

    #[test]
    fn test_autocue_zero_tail_is_normal() {
        let cur = make_song(20.0, 1.0, 14.0, 14.0, true);
        let next = make_song(20.0, 2.0, 19.0, 17.0, true);
        // cur_end == cur_cut -> no tail
        let t = compute_transition("autocue", &cur, Some(&next), 0.0, 5.0);
        assert_eq!(t, Transition::Normal);
    }

    #[test]
    fn test_autocue_cut_before_cue_in_is_normal() {
        let cur = make_song(20.0, 3.0, 18.0, 1.0, true);
        let next = make_song(20.0, 2.0, 19.0, 17.0, true);
        let t = compute_transition("autocue", &cur, Some(&next), 0.0, 5.0);
        assert_eq!(t, Transition::Normal);
    }

    #[test]
    fn test_no_next_is_normal() {
        let cur = make_song(10.0, 0.0, 0.0, 0.0, true);
        assert_eq!(compute_transition("crossfade", &cur, None, 3.0, 5.0), Transition::Normal);
        assert_eq!(compute_transition("autocue", &cur, None, 3.0, 5.0), Transition::Normal);
    }
}
