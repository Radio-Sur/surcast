use async_trait::async_trait;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TrackKey {
    pub queue_item_id: Uuid,
    pub song_id: Uuid,
    pub position: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct PipelineTrack {
    pub key: TrackKey,
    pub path: PathBuf,
    pub cue_in: Duration,
    pub cue_out: Duration,
    pub cross_start_next: Duration,
    pub analyzed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransitionMode {
    Off,
    Crossfade,
    AutoCue,
}

impl TransitionMode {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "off" => Some(Self::Off),
            "crossfade" => Some(Self::Crossfade),
            "autocue" => Some(Self::AutoCue),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Crossfade => "crossfade",
            Self::AutoCue => "autocue",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TransitionConfig {
    pub mode: TransitionMode,
    pub requested_fade: Duration,
    pub autocue_cap: Duration,
}

pub(crate) struct StationPlaybackConfig {
    pub transition: TransitionConfig,
    pub output: OutputConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutputConfig {
    pub prebuffer_bytes: usize,
    pub sample_rate: u32,
    pub channels: u32,
    pub bitrate_kbps: u32,
}

impl StationPlaybackConfig {
    pub(crate) fn from_persisted(
        transition_mode: &str,
        default_fade_ms: i32,
        autocue_fade_max_ms: i32,
        prebuffer_bytes: i32,
    ) -> Result<Self, PipelineError> {
        let mode =
            TransitionMode::parse(transition_mode).ok_or_else(|| PipelineError::InvalidTransitionMode(transition_mode.to_owned()))?;
        Ok(Self {
            transition: TransitionConfig {
                mode,
                requested_fade: Duration::from_millis(default_fade_ms.max(0) as u64),
                autocue_cap: Duration::from_millis(autocue_fade_max_ms.max(0) as u64),
            },
            output: OutputConfig {
                prebuffer_bytes: prebuffer_bytes.max(0) as usize,
                sample_rate: 44_100,
                channels: 2,
                bitrate_kbps: 128,
            },
        })
    }

    pub(crate) fn pipeline_config(&self, target: IcecastTarget) -> PipelineConfig {
        PipelineConfig {
            target,
            output: self.output,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransitionPlan {
    Cut,
    NaiveCrossfade {
        requested_fade: Duration,
    },
    AutoCueCrossfade {
        current_start: Duration,
        fade_start: Duration,
        current_end: Duration,
        next_start: Duration,
        duration: Duration,
        fallback_fade: Duration,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct PairPlan {
    pub generation: u64,
    pub current: PipelineTrack,
    pub next: Option<PipelineTrack>,
    pub transition: TransitionPlan,
}

#[derive(Clone)]
pub(crate) struct IcecastTarget {
    pub host: String,
    pub port: u16,
    pub mount: String,
    pub password: String,
    pub stream_name: String,
}

impl IcecastTarget {
    pub(crate) fn parse(endpoint: &str, password: String, mount: &str, stream_name: String) -> Result<Self, PipelineError> {
        let endpoint = if endpoint.contains("://") {
            endpoint.to_owned()
        } else {
            format!("http://{endpoint}")
        };
        let url = reqwest::Url::parse(&endpoint).map_err(|error| PipelineError::InvalidTarget(error.to_string()))?;
        if url.scheme() != "http"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(PipelineError::InvalidTarget("expected a bare http host and port".into()));
        }
        let mount = mount
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(crate::util::url_encode)
            .collect::<Vec<_>>()
            .join("/");
        if mount.is_empty() {
            return Err(PipelineError::InvalidTarget("mount must not be empty".into()));
        }
        Ok(Self {
            host: url.host_str().expect("checked above").to_owned(),
            port: url.port_or_known_default().expect("http has a default port"),
            mount: format!("/{mount}"),
            password,
            stream_name,
        })
    }
}

impl fmt::Debug for IcecastTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcecastTarget")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("mount", &self.mount)
            .field("stream_name", &self.stream_name)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for IcecastTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "http://{}:{}{}", self.host, self.port, self.mount)
    }
}

pub(crate) struct PipelineConfig {
    pub target: IcecastTarget,
    pub output: OutputConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PipelineState {
    Playing,
    Paused,
    Stopped,
}

#[derive(Clone, Debug)]
pub(crate) struct PipelineSnapshot {
    pub state: PipelineState,
    pub elapsed: Duration,
}

#[derive(Clone, Debug)]
pub(crate) enum PipelineEvent {
    Handover { generation: u64, current: TrackKey },
    CurrentEos { generation: u64, current: TrackKey },
    DecodeFailed { generation: u64, track: TrackKey, message: String },
    SinkDisconnected { generation: u64, message: String },
}

#[derive(Clone, Debug)]
pub(crate) enum PipelineError {
    MissingElement(&'static str),
    StalePlan,
    InvalidTarget(String),
    InvalidTransitionMode(String),
    Initialization(String),
    Pipeline(String),
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingElement(element) => write!(f, "required GStreamer element is unavailable: {element}"),
            Self::StalePlan => write!(f, "stale playback plan"),
            Self::InvalidTarget(message) => write!(f, "invalid Icecast target: {message}"),
            Self::InvalidTransitionMode(mode) => write!(f, "invalid persisted transition mode: {mode}"),
            Self::Initialization(message) => write!(f, "GStreamer initialization failed: {message}"),
            Self::Pipeline(message) => write!(f, "GStreamer pipeline failure: {message}"),
        }
    }
}

impl std::error::Error for PipelineError {}

#[async_trait]
pub(crate) trait PlaybackPipeline: Send + Sync {
    async fn replace(&self, plan: PairPlan) -> Result<(), PipelineError>;
    async fn apply_output(&self, output: OutputConfig) -> Result<(), PipelineError>;
    async fn set_playing(&self, playing: bool) -> Result<(), PipelineError>;
    async fn reconnect(&self, target: IcecastTarget) -> Result<(), PipelineError>;
    async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError>;
    async fn stop(&self) -> Result<(), PipelineError>;
}

#[async_trait]
pub(crate) trait PlaybackPipelineFactory: Send + Sync {
    async fn create(&self, config: PipelineConfig) -> Result<PipelineInstance, PipelineError>;
}

pub(crate) struct PipelineInstance {
    pub pipeline: Arc<dyn PlaybackPipeline>,
    pub events: mpsc::UnboundedReceiver<PipelineEvent>,
}

pub(crate) struct TransitionPlanner;

impl TransitionPlanner {
    pub(crate) fn plan(config: TransitionConfig, current: &PipelineTrack, next: Option<&PipelineTrack>) -> TransitionPlan {
        let Some(next) = next else {
            return TransitionPlan::Cut;
        };

        if config.mode == TransitionMode::Off {
            return TransitionPlan::Cut;
        }

        if config.mode == TransitionMode::AutoCue && current.analyzed && next.analyzed {
            let tail = current.cue_out.checked_sub(current.cross_start_next);
            if current.cross_start_next >= current.cue_in {
                if let Some(tail) = tail {
                    let duration = tail.min(config.autocue_cap);
                    if duration >= Duration::from_millis(200) {
                        return TransitionPlan::AutoCueCrossfade {
                            current_start: current.cue_in,
                            fade_start: current.cross_start_next,
                            current_end: current.cue_out,
                            next_start: next.cue_in,
                            duration,
                            fallback_fade: config.requested_fade,
                        };
                    }
                }
            }
        }

        if config.requested_fade.is_zero() {
            TransitionPlan::Cut
        } else {
            TransitionPlan::NaiveCrossfade {
                requested_fade: config.requested_fade,
            }
        }
    }
}

pub(crate) fn resolve_transition(
    plan: TransitionPlan,
    current_duration: Option<Duration>,
    next_duration: Option<Duration>,
    current_seekable: bool,
    next_seekable: bool,
) -> TransitionPlan {
    let naive = |requested_fade: Duration| match (current_duration, next_duration) {
        (Some(current), Some(next)) if !requested_fade.is_zero() => {
            let fade = requested_fade.min(current).min(next);
            if fade.is_zero() {
                TransitionPlan::Cut
            } else {
                TransitionPlan::NaiveCrossfade { requested_fade: fade }
            }
        }
        _ => TransitionPlan::Cut,
    };

    match plan {
        TransitionPlan::NaiveCrossfade { requested_fade } => naive(requested_fade),
        TransitionPlan::AutoCueCrossfade {
            current_start,
            fade_start,
            current_end,
            next_start,
            duration,
            fallback_fade,
        } => {
            let Some(current_duration) = current_duration else {
                return TransitionPlan::Cut;
            };
            let Some(next_duration) = next_duration else {
                return TransitionPlan::Cut;
            };
            if !current_seekable || !next_seekable || current_start >= current_duration || next_start >= next_duration {
                return naive(fallback_fade);
            }
            let fade_start = fade_start.clamp(current_start, current_duration);
            let current_end = current_end.clamp(fade_start, current_duration);
            let available_current = current_end.saturating_sub(fade_start);
            let available_next = next_duration.saturating_sub(next_start);
            let duration = duration.min(available_current).min(available_next);
            if duration < Duration::from_millis(200) {
                naive(fallback_fade)
            } else {
                TransitionPlan::AutoCueCrossfade {
                    current_start,
                    fade_start,
                    current_end,
                    next_start,
                    duration,
                    fallback_fade,
                }
            }
        }
        TransitionPlan::Cut => TransitionPlan::Cut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(analyzed: bool, cue_in: u64, cue_out: u64, cross_start_next: u64) -> PipelineTrack {
        PipelineTrack {
            key: TrackKey {
                queue_item_id: Uuid::nil(),
                song_id: Uuid::nil(),
                position: 0,
            },
            path: PathBuf::from("/tmp/test.wav"),
            cue_in: Duration::from_secs(cue_in),
            cue_out: Duration::from_secs(cue_out),
            cross_start_next: Duration::from_secs(cross_start_next),
            analyzed,
        }
    }

    fn config(mode: TransitionMode, requested_fade: Duration, autocue_cap: Duration) -> TransitionConfig {
        TransitionConfig {
            mode,
            requested_fade,
            autocue_cap,
        }
    }

    #[test]
    fn transition_mode_parses_persisted_values() {
        assert_eq!(TransitionMode::parse("off"), Some(TransitionMode::Off));
        assert_eq!(TransitionMode::parse("crossfade"), Some(TransitionMode::Crossfade));
        assert_eq!(TransitionMode::parse("autocue"), Some(TransitionMode::AutoCue));
        assert_eq!(TransitionMode::parse("unknown"), None);
    }

    #[test]
    fn playback_config_rejects_unknown_persisted_transition_modes() {
        assert!(matches!(
            StationPlaybackConfig::from_persisted("unknown", 3_000, 5_000, 16_384),
            Err(PipelineError::InvalidTransitionMode(mode)) if mode == "unknown"
        ));
    }

    #[test]
    fn cut_applies_to_off_zero_fade_or_missing_next() {
        let current = track(false, 0, 0, 0);
        let next = track(false, 0, 0, 0);
        assert_eq!(
            TransitionPlanner::plan(
                config(TransitionMode::Off, Duration::from_secs(1), Duration::from_secs(5)),
                &current,
                Some(&next)
            ),
            TransitionPlan::Cut
        );
        assert_eq!(
            TransitionPlanner::plan(
                config(TransitionMode::Crossfade, Duration::ZERO, Duration::from_secs(5)),
                &current,
                Some(&next)
            ),
            TransitionPlan::Cut
        );
        assert_eq!(
            TransitionPlanner::plan(
                config(TransitionMode::Crossfade, Duration::from_secs(1), Duration::from_secs(5)),
                &current,
                None
            ),
            TransitionPlan::Cut
        );
    }

    #[test]
    fn naive_fade_clamps_to_runtime_durations() {
        let resolved = resolve_transition(
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_secs(10),
            },
            Some(Duration::from_secs(3)),
            Some(Duration::from_secs(2)),
            true,
            true,
        );
        assert_eq!(
            resolved,
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_secs(2)
            }
        );
    }

    #[test]
    fn unknown_duration_degrades_to_cut() {
        let resolved = resolve_transition(
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_secs(2),
            },
            None,
            Some(Duration::from_secs(2)),
            true,
            true,
        );
        assert_eq!(resolved, TransitionPlan::Cut);
    }

    #[test]
    fn autocue_uses_valid_analyzed_geometry() {
        let current = track(true, 1, 18, 14);
        let next = track(true, 2, 19, 17);
        let plan = TransitionPlanner::plan(
            config(TransitionMode::AutoCue, Duration::ZERO, Duration::from_secs(5)),
            &current,
            Some(&next),
        );
        assert_eq!(
            resolve_transition(plan, Some(Duration::from_secs(20)), Some(Duration::from_secs(20)), true, true),
            TransitionPlan::AutoCueCrossfade {
                current_start: Duration::from_secs(1),
                fade_start: Duration::from_secs(14),
                current_end: Duration::from_secs(18),
                next_start: Duration::from_secs(2),
                duration: Duration::from_secs(4),
                fallback_fade: Duration::ZERO,
            }
        );
    }

    #[test]
    fn icecast_target_normalizes_mount_without_leaking_password() {
        let target = IcecastTarget::parse("localhost:8000", "secret".into(), "//daily mix//", "Daily".into()).unwrap();
        assert_eq!(target.host, "localhost");
        assert_eq!(target.port, 8000);
        assert_eq!(target.mount, "/daily%20mix");
        assert!(!format!("{target:?}").contains("secret"));
        assert!(!target.to_string().contains("secret"));
    }

    #[test]
    fn icecast_target_rejects_unsupported_urls() {
        for endpoint in ["https://localhost:8000", "http://user@localhost:8000", "http://localhost:8000/?x=1"] {
            assert!(IcecastTarget::parse(endpoint, "secret".into(), "mount", "name".into()).is_err());
        }
    }

    #[test]
    fn invalid_or_unseekable_autocue_falls_back() {
        let current = track(true, 3, 1, 2);
        let next = track(true, 0, 0, 0);
        assert_eq!(
            TransitionPlanner::plan(
                config(TransitionMode::AutoCue, Duration::from_secs(2), Duration::from_secs(5)),
                &current,
                Some(&next)
            ),
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_secs(2)
            }
        );
        let plan = TransitionPlan::AutoCueCrossfade {
            current_start: Duration::ZERO,
            fade_start: Duration::from_secs(2),
            current_end: Duration::from_secs(3),
            next_start: Duration::ZERO,
            duration: Duration::from_secs(1),
            fallback_fade: Duration::from_secs(2),
        };
        assert_eq!(
            resolve_transition(plan, Some(Duration::from_secs(3)), Some(Duration::from_secs(3)), false, true),
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_secs(2)
            }
        );
    }

    #[test]
    fn short_autocue_tail_falls_back_to_naive() {
        let current = track(true, 0, 10, 10);
        let next = track(true, 0, 10, 0);
        assert_eq!(
            TransitionPlanner::plan(
                config(TransitionMode::AutoCue, Duration::from_secs(2), Duration::from_secs(5)),
                &current,
                Some(&next)
            ),
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_secs(2)
            }
        );
    }
}
