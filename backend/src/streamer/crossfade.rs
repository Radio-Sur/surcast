use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::engine::PlaybackEngine;
use super::SongInfo;
use super::StatusEvent;

#[allow(dead_code)]
pub(super) struct CrossfadeConfig {
    pub bitrate: f64,
    pub chunk_size: usize,
    pub chunk_duration: Duration,
    pub total_chunks: usize,
    pub pre_idx: usize,
    pub prebuffer_chunks: usize,
    pub cur_start: f64,
    pub cur_cut: f64,
    pub cur_end: f64,
    pub next_start: f64,
    pub fade_secs: f64,
    pub actual_fade: f64,
}

pub(super) struct PlaybackConfig {
    pub chunks: Vec<Vec<u8>>,
    pub chunk_duration: Duration,
    pub total_chunks: usize,
    pub pre_idx: usize,
}

impl PlaybackEngine {
    pub(super) async fn play_crossfade(
        &self,
        stream: &mut Option<TcpStream>,
        info: &SongInfo,
        next: &SongInfo,
        idx: usize,
        config: &CrossfadeConfig,
    ) -> bool {
        let cur_start = config.cur_start;
        let cur_cut = config.cur_cut;
        let cur_end = config.cur_end;
        let next_start = config.next_start;
        let actual_fade = config.actual_fade;
        let next_start_plus_fade = next_start + actual_fade;
        let filter = format!(
            "[0:a]asplit=2[normal][tail];\
              [normal]atrim=start={cur_start:.3}:end={cur_cut:.3},asetpts=PTS-STARTPTS[normal_trim];\
              [tail]atrim=start={cur_cut:.3}:end={cur_end:.3},asetpts=PTS-STARTPTS[tail_trim];\
              [1:a]asplit=2[head][rest];\
              [head]atrim=start={next_start:.3}:end={next_start_plus_fade:.3},asetpts=PTS-STARTPTS[head_trim];\
              [rest]atrim=start={next_start_plus_fade:.3},asetpts=PTS-STARTPTS[rest_trim];\
              [tail_trim][head_trim]acrossfade=d={actual_fade}[cross];\
              [normal_trim][cross][rest_trim]concat=n=3:v=0:a=1[out]"
        );

        let mut child = match tokio::process::Command::new("ffmpeg")
            .args([
                "-i",
                &info.file_path,
                "-i",
                &next.file_path,
                "-b:a",
                &format!("{}", (config.bitrate * 8.0) as u32),
                "-filter_complex",
                &filter,
                "-map",
                "[out]",
                "-f",
                "mp3",
                "-",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(_) => {
                tracing::warn!(
                    mount = %self.mount,
                    "ffmpeg not available, skipping crossfade"
                );
                self.queue.advance_idx(1);
                self.queue.advance_song().await;
                return false;
            }
        };

        let mut ok = true;
        if let Some(stdout) = child.stdout.take() {
            let mut reader = tokio::io::BufReader::new(stdout);
            let mut buf = vec![0u8; config.chunk_size];
            let mut chunk_count = 0usize;
            let mut song_change_sent = false;
            let mut paced_start: Option<Instant> = None;
            let midpoint_secs = (config.cur_cut - config.cur_start) + config.actual_fade / 2.0;
            let prebuffer_duration = config.prebuffer_chunks as f64 * config.chunk_duration.as_secs_f64();

            loop {
                if !self.playing.load(Ordering::Acquire) || self.stopped.load(Ordering::Acquire) {
                    break;
                }
                let expected = if song_change_sent { config.pre_idx + 1 } else { config.pre_idx };
                if self.queue.current_idx() != expected {
                    ok = false;
                    break;
                }
                let n = match reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                if let Some(s) = stream.as_mut() {
                    if let Err(e) = s.write_all(&buf[..n]).await {
                        tracing::warn!(
                            mount = %self.mount,
                            error = %e,
                            "Streamer write failed"
                        );
                        self.disconnect(stream).await;
                        break;
                    }
                }
                if chunk_count >= config.prebuffer_chunks {
                    if paced_start.is_none() {
                        paced_start = Some(Instant::now());
                    }
                    tokio::time::sleep(config.chunk_duration).await;
                }
                chunk_count += 1;

                if !song_change_sent {
                    let elapsed = paced_start.map(|s| prebuffer_duration + s.elapsed().as_secs_f64()).unwrap_or(0.0);
                    if elapsed >= midpoint_secs {
                        song_change_sent = true;
                        self.queue.advance_idx(1);
                        self.mark_song_started();
                        if self
                            .queue
                            .status_tx
                            .send(StatusEvent::SongChange {
                                song_index: idx + 1,
                                total: self.queue.song_count(),
                                elapsed: 0,
                                title: next.title.clone(),
                                artist: next.artist.clone(),
                                duration: next.duration,
                            })
                            .is_err()
                        {
                            tracing::debug!("No status listeners for station {}", self.queue.station_id);
                        }
                        let upcoming = self.queue.song_count().saturating_sub(self.queue.current_idx() + 1) as i64;
                        let _ = crate::scheduling::service::fill_queue_from_schedule(
                            &self.db,
                            self.station_id,
                            Some(upcoming),
                            &self.queue.upload_dir,
                        )
                        .await;
                        self.queue.push_queue_update().await;
                    }
                }
            }
        }
        let _ = child.wait().await;
        ok
    }

    pub(super) async fn play_normal(&self, stream: &mut Option<TcpStream>, config: &PlaybackConfig) -> bool {
        for (i, chunk) in config.chunks.iter().enumerate() {
            if !self.playing.load(Ordering::Acquire) || self.stopped.load(Ordering::Acquire) {
                self.disconnect(stream).await;
                return false;
            }
            if self.queue.current_idx() != config.pre_idx {
                return false;
            }
            if let Some(s) = stream.as_mut() {
                if let Err(e) = s.write_all(chunk).await {
                    tracing::warn!(
                        mount = %self.mount,
                        error = %e,
                        "Streamer write failed, reconnecting"
                    );
                    self.disconnect(stream).await;
                    return false;
                }
            }
            if i < config.total_chunks - 1 {
                tokio::time::sleep(config.chunk_duration).await;
            }
        }
        true
    }
}
