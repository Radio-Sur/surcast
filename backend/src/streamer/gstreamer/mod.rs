mod branch;
mod bus;
mod graph;
mod sink;
mod transition;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::pipeline::{
    resolve_transition, IcecastTarget, OutputConfig, PairPlan, PipelineConfig, PipelineError, PipelineEvent, PipelineInstance,
    PipelineSnapshot, PipelineState, PlaybackPipeline, PlaybackPipelineFactory, ReplaceMode, RollingChange, RollingPlan, TrackKey,
    TransitionPlan,
};
use branch::Branch;

#[derive(Clone)]
pub(crate) struct GStreamerPipelineFactory {
    sink_factory: &'static str,
}

impl Default for GStreamerPipelineFactory {
    fn default() -> Self {
        Self {
            sink_factory: sink::DEFAULT_FACTORY,
        }
    }
}

impl GStreamerPipelineFactory {
    #[cfg(test)]
    fn with_test_sink() -> Self {
        Self { sink_factory: "fakesink" }
    }
}

#[derive(Clone)]
struct ActivePlan {
    generation: u64,
    output_epoch: u64,
    current: TrackKey,
    next: Option<TrackKey>,
    handover_at: Option<gst::ClockTime>,
    started_at: Option<gst::ClockTime>,
    current_epoch: gst::ClockTime,
    last_elapsed: gst::ClockTime,
    handed_over: bool,
}

pub(crate) struct GStreamerPipeline {
    pipeline: gst::Pipeline,
    mixer: gst::Element,
    output_queue: gst::Element,
    output_caps: gst::Element,
    encoder: gst::Element,
    sink: Mutex<gst::Element>,
    clock_gate: gst::Element,
    sink_factory: &'static str,
    branches: Mutex<Vec<Branch>>,
    active: Arc<Mutex<Option<ActivePlan>>>,
    snapshot: Mutex<PipelineSnapshot>,
    events: mpsc::UnboundedSender<PipelineEvent>,
}

impl GStreamerPipeline {
    fn set_state(&self, state: PipelineState) -> Result<(), PipelineError> {
        let target = match state {
            PipelineState::Playing => gst::State::Playing,
            PipelineState::Paused => gst::State::Paused,
            PipelineState::Stopped => gst::State::Null,
        };
        self.pipeline
            .set_state(target)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let (result, current, pending) = self.pipeline.state(gst::ClockTime::from_seconds(5));
        result.map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        if current != target {
            return Err(PipelineError::Pipeline(format!(
                "state transition to {target:?} stalled at {current:?} with {pending:?} pending"
            )));
        }
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|error| error.into_inner());
        snapshot.state = state;
        if state == PipelineState::Stopped {
            snapshot.elapsed = Duration::ZERO;
        }
        Ok(())
    }
}

#[async_trait]
impl PlaybackPipeline for GStreamerPipeline {
    async fn apply_output(&self, output: OutputConfig) -> Result<(), PipelineError> {
        graph::configure_output(&self.output_queue, &self.output_caps, &self.encoder, output);
        Ok(())
    }

    async fn replace(&self, plan: PairPlan) -> Result<(), PipelineError> {
        {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            match (&plan.mode, active.as_ref()) {
                (ReplaceMode::InitialReplaceFromStopped, None) => {}
                (
                    ReplaceMode::ActiveReplace {
                        expected_generation,
                        expected_current,
                    },
                    Some(active),
                ) if active.generation == *expected_generation && active.current == *expected_current => {}
                _ => return Err(PipelineError::StalePlan),
            }
        }
        if self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).state != PipelineState::Stopped {
            self.set_state(PipelineState::Paused)?;
            self.pipeline.send_event(gst::event::FlushStart::new());
            self.pipeline.send_event(gst::event::FlushStop::new(true));
        }
        {
            let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            branch::clear(&self.pipeline, &self.mixer, &mut branches);
        }

        let current = branch::attach(
            &self.pipeline,
            &self.mixer,
            self.events.clone(),
            &plan.current,
            plan.generation,
            1.0,
        )?;
        self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(current);
        self.set_state(PipelineState::Paused)?;
        let current_duration = branch::wait_duration(&self.branches, 0).await;

        let mut scheduled_next = None;
        let mut transition_plan = TransitionPlan::Cut;
        if let (Some(next), Some(_)) = (plan.next.as_ref(), current_duration) {
            let next_branch = branch::attach(&self.pipeline, &self.mixer, self.events.clone(), &next.track, plan.generation, 0.0)?;
            next_branch
                .source
                .state(gst::ClockTime::from_seconds(5))
                .0
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(next_branch);
            scheduled_next = Some(next.track.key.clone());
            transition_plan = next.transition;
        }
        let next_duration = if scheduled_next.is_some() {
            branch::wait_duration(&self.branches, 1).await
        } else {
            None
        };
        let (current_seekable, next_seekable) = {
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            (branch::seekable(&branches[0]), branches.get(1).is_some_and(branch::seekable))
        };
        let transition = resolve_transition(transition_plan, current_duration, next_duration, current_seekable, next_seekable);

        let handover_at = {
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            transition::apply_initial(transition, &branches, current_duration)?.handover
        };

        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = Some(ActivePlan {
            generation: plan.generation,
            output_epoch: plan.output_epoch,
            current: plan.current.key,
            next: scheduled_next,
            handover_at,
            handed_over: false,
            started_at: None,
            current_epoch: gst::ClockTime::ZERO,
            last_elapsed: gst::ClockTime::ZERO,
        });
        self.set_state(PipelineState::Playing)
    }

    async fn roll(&self, plan: RollingPlan) -> Result<(), PipelineError> {
        let (attaching, expected_next, replacement) = match plan.change {
            RollingChange::Attach(next) => (true, None, Some(next)),
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => (false, Some(expected_next), replacement),
        };
        {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let Some(active) = active.as_ref() else {
                return Err(PipelineError::StalePlan);
            };
            if active.generation != plan.generation
                || active.current != plan.current
                || expected_next.as_ref().is_some_and(|key| active.next.as_ref() != Some(key))
                || expected_next.is_none() && active.next.is_some()
            {
                return Err(PipelineError::StalePlan);
            }
        }

        {
            let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            if attaching {
                branch::remove_first(&self.pipeline, &self.mixer, &mut branches);
            } else if expected_next.is_some() {
                branch::truncate(&self.pipeline, &self.mixer, &mut branches, 1);
            }
        }
        let (next_key, handover_at) = if let Some(next) = replacement {
            let branch = branch::attach(&self.pipeline, &self.mixer, self.events.clone(), &next.track, plan.generation, 0.0)?;
            self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(branch);
            let current_duration = branch::wait_duration(&self.branches, 0).await;
            let next_duration = branch::wait_duration(&self.branches, 1).await;
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            let transition = resolve_transition(
                next.transition,
                current_duration,
                next_duration,
                branch::seekable(&branches[0]),
                branch::seekable(&branches[1]),
            );
            let handover_at = transition::apply_rolling(transition, &branches, current_duration)?.handover;
            (Some(next.track.key), handover_at)
        } else {
            (None, None)
        };
        let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
        let Some(active) = active.as_mut() else {
            return Err(PipelineError::StalePlan);
        };
        active.next = next_key;
        active.handover_at = handover_at;
        active.handed_over = false;
        Ok(())
    }

    async fn set_playing(&self, playing: bool) -> Result<(), PipelineError> {
        self.set_state(if playing { PipelineState::Playing } else { PipelineState::Paused })
    }

    async fn reconnect(&self, target: IcecastTarget) -> Result<(), PipelineError> {
        let previous_state = self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).state;
        if previous_state == PipelineState::Stopped {
            let sink = self.sink.lock().unwrap_or_else(|error| error.into_inner());
            if self.sink_factory == sink::DEFAULT_FACTORY {
                sink::configure(&sink, &target);
            }
            return Ok(());
        }

        self.set_state(PipelineState::Paused)?;
        self.pipeline
            .set_state(gst::State::Ready)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let old_sink = self.sink.lock().unwrap_or_else(|error| error.into_inner()).clone();
        let candidate = sink::build(self.sink_factory, &target)?;
        self.pipeline
            .add(&candidate)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;

        self.clock_gate.unlink(&old_sink);
        if let Err(error) = self.clock_gate.link(&candidate) {
            let _ = self.pipeline.remove(&candidate);
            let _ = self.clock_gate.link(&old_sink);
            return Err(PipelineError::Pipeline(error.to_string()));
        }

        self.pipeline
            .set_state(gst::State::Paused)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let (result, current, pending) = self.pipeline.state(gst::ClockTime::from_seconds(5));
        result.map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        if current != gst::State::Paused {
            self.clock_gate.unlink(&candidate);
            let _ = candidate.set_state(gst::State::Null);
            let _ = self.pipeline.remove(&candidate);
            if self.clock_gate.link(&old_sink).is_err() {
                let _ = self.set_state(PipelineState::Stopped);
                *self.active.lock().unwrap_or_else(|poison| poison.into_inner()) = None;
                return Err(PipelineError::Pipeline("sink replacement and rollback failed".into()));
            }
            return Err(PipelineError::Pipeline(format!(
                "reconnect transition to Paused stalled at {current:?} with {pending:?} pending"
            )));
        }
        old_sink
            .set_state(gst::State::Null)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        self.pipeline
            .remove(&old_sink)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = candidate;
        if previous_state == PipelineState::Playing {
            self.set_state(PipelineState::Playing)?;
        }
        Ok(())
    }

    async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|error| error.into_inner());
        if snapshot.state != PipelineState::Stopped {
            if let Some(active) = self.active.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
                snapshot.elapsed = Duration::from_nanos(active.last_elapsed.saturating_sub(active.current_epoch).nseconds());
            }
        }
        Ok(snapshot.clone())
    }

    async fn stop(&self) -> Result<(), PipelineError> {
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = None;
        self.set_state(PipelineState::Stopped)
    }
}

#[async_trait]
impl PlaybackPipelineFactory for GStreamerPipelineFactory {
    async fn create(&self, config: PipelineConfig) -> Result<PipelineInstance, PipelineError> {
        let graph::Backbone {
            pipeline,
            mixer,
            output_queue,
            output_caps,
            encoder,
            sink,
            clock_gate,
        } = graph::build_backbone(&config, self.sink_factory)?;
        let (events, receiver) = mpsc::unbounded_channel();
        let active: Arc<Mutex<Option<ActivePlan>>> = Arc::new(Mutex::new(None));
        bus::install(&pipeline, &clock_gate, active.clone(), events.clone())?;
        Ok(PipelineInstance {
            pipeline: Arc::new(GStreamerPipeline {
                pipeline,
                mixer,
                output_queue,
                output_caps,
                encoder,
                sink: Mutex::new(sink),
                clock_gate,
                sink_factory: self.sink_factory,
                branches: Mutex::new(Vec::new()),
                active,
                snapshot: Mutex::new(PipelineSnapshot {
                    state: PipelineState::Stopped,
                    elapsed: Duration::ZERO,
                }),
                events,
            }),
            events: receiver,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamer::pipeline::{IcecastTarget, OutputConfig, PipelineTrack, PlannedNext, TransitionPlan};

    fn config() -> PipelineConfig {
        PipelineConfig {
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            output: OutputConfig {
                prebuffer_bytes: 16_384,
                sample_rate: 44_100,
                channels: 2,
                bitrate_kbps: 128,
            },
        }
    }
    fn write_wav(file: &std::path::Path, duration: Duration, sample: i16) {
        let frames = (duration.as_secs_f64() * 44_100.0).round() as u32;
        let data_len = frames * 4;
        let mut wav = Vec::with_capacity((44 + data_len) as usize);
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data_len).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&44_100u32.to_le_bytes());
        wav.extend_from_slice(&176_400u32.to_le_bytes());
        wav.extend_from_slice(&4u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_len.to_le_bytes());
        for _ in 0..frames {
            wav.extend_from_slice(&sample.to_le_bytes());
            wav.extend_from_slice(&sample.to_le_bytes());
        }
        std::fs::write(file, wav).unwrap();
    }

    fn track(path: &std::path::Path, _position: i32) -> PipelineTrack {
        PipelineTrack {
            key: TrackKey {
                queue_item_id: uuid::Uuid::new_v4(),
                song_id: uuid::Uuid::new_v4(),
            },
            path: path.to_path_buf(),
            cue_in: Duration::ZERO,
            cue_out: Duration::ZERO,
            cross_start_next: Duration::ZERO,
            analyzed: false,
        }
    }

    fn initial_plan(generation: u64, current: PipelineTrack, next: Option<PipelineTrack>, transition: TransitionPlan) -> PairPlan {
        PairPlan {
            mode: ReplaceMode::InitialReplaceFromStopped,
            generation,
            output_epoch: 1,
            current,
            next: next.map(|track| PlannedNext { track, transition }),
        }
    }

    #[tokio::test]
    async fn creates_clocked_mp3_backbone_with_test_sink() {
        let instance = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        assert_eq!(instance.pipeline.snapshot().await.unwrap().state, PipelineState::Stopped);
        instance.pipeline.set_playing(false).await.unwrap();
        assert_eq!(instance.pipeline.snapshot().await.unwrap().state, PipelineState::Paused);
        instance.pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn decodes_a_wav_branch_without_a_second_playback_backend() {
        use crate::streamer::pipeline::{PipelineTrack, TrackKey};
        use tempfile::NamedTempFile;
        use uuid::Uuid;

        let file = NamedTempFile::new().unwrap();
        let mut wav = Vec::from(b"RIFF".as_slice());
        wav.extend_from_slice(&(36u32 + 8_820).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&44_100u32.to_le_bytes());
        wav.extend_from_slice(&176_400u32.to_le_bytes());
        wav.extend_from_slice(&4u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&8_820u32.to_le_bytes());
        wav.extend(std::iter::repeat_n(0u8, 8_820));
        std::fs::write(file.path(), wav).unwrap();

        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let key = TrackKey {
            queue_item_id: Uuid::new_v4(),
            song_id: Uuid::new_v4(),
        };
        pipeline
            .replace(initial_plan(
                1,
                PipelineTrack {
                    key: key.clone(),
                    path: file.path().to_path_buf(),
                    cue_in: Duration::ZERO,
                    cue_out: Duration::ZERO,
                    cross_start_next: Duration::ZERO,
                    analyzed: false,
                },
                None,
                TransitionPlan::Cut,
            ))
            .await
            .unwrap();
        assert_eq!(pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), events.recv()).await.unwrap(),
            Some(PipelineEvent::CurrentEos { generation: 1, current }) if current == key
        ));
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn schedules_next_branch_and_handover_on_the_clocked_fade_midpoint() {
        let current_file = tempfile::NamedTempFile::new().unwrap();
        let next_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(current_file.path(), Duration::from_secs(1), 8_000);
        write_wav(next_file.path(), Duration::from_secs(1), -8_000);
        let current = track(current_file.path(), 0);
        let next = track(next_file.path(), 1);
        let next_key = next.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let started = std::time::Instant::now();

        pipeline
            .replace(initial_plan(
                7,
                current,
                Some(next),
                TransitionPlan::NaiveCrossfade {
                    requested_fade: Duration::from_millis(400),
                },
            ))
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(3), events.recv()).await.unwrap();
        assert!(
            matches!(event, Some(PipelineEvent::Handover { generation: 7, ref current }) if current == &next_key),
            "{event:?}"
        );
        assert!(started.elapsed() >= Duration::from_millis(500));
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn schedules_each_replacement_on_its_own_running_time() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let third_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(third_file.path(), Duration::from_secs(1), 4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let third = track(third_file.path(), 2);
        let third_key = third.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let plan = |generation, mode, current, next| PairPlan {
            mode,
            generation,
            output_epoch: 1,
            current,
            next: Some(PlannedNext {
                track: next,
                transition: TransitionPlan::NaiveCrossfade {
                    requested_fade: Duration::from_millis(400),
                },
            }),
        };

        pipeline
            .replace(plan(1, ReplaceMode::InitialReplaceFromStopped, first, second.clone()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, .. })) {}
        })
        .await
        .unwrap();

        let started = std::time::Instant::now();
        pipeline
            .replace(plan(
                2,
                ReplaceMode::ActiveReplace {
                    expected_generation: 1,
                    expected_current: second.key.clone(),
                },
                second,
                third,
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(
                events.recv().await,
                Some(PipelineEvent::Handover {
                    generation: 2,
                    ref current,
                }) if current == &third_key
            ) {}
        })
        .await
        .unwrap();
        assert!(pipeline.snapshot().await.unwrap().elapsed < Duration::from_millis(250));
        assert!(started.elapsed() >= Duration::from_millis(500));
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn applies_autocue_seeks_before_the_clocked_handover() {
        let current_file = tempfile::NamedTempFile::new().unwrap();
        let next_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(current_file.path(), Duration::from_millis(1_500), 8_000);
        write_wav(next_file.path(), Duration::from_millis(1_500), -8_000);
        let current = track(current_file.path(), 0);
        let next = track(next_file.path(), 1);
        let next_key = next.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let started = std::time::Instant::now();

        pipeline
            .replace(initial_plan(
                8,
                current,
                Some(next),
                TransitionPlan::AutoCueCrossfade {
                    current_start: Duration::from_millis(200),
                    fade_start: Duration::from_millis(800),
                    current_end: Duration::from_millis(1_200),
                    next_start: Duration::from_millis(100),
                    duration: Duration::from_millis(400),
                    fallback_fade: Duration::from_millis(300),
                },
            ))
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(3), events.recv()).await.unwrap();
        assert!(
            matches!(event, Some(PipelineEvent::Handover { generation: 8, ref current }) if current == &next_key),
            "{event:?}"
        );
        assert!(started.elapsed() >= Duration::from_millis(500));
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn rolling_attach_promotes_handover_and_schedules_the_following_track() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let third_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(third_file.path(), Duration::from_secs(1), 4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let third = track(third_file.path(), 2);
        let second_key = second.key.clone();
        let third_key = third.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        pipeline
            .replace(initial_plan(
                1,
                first,
                Some(second),
                TransitionPlan::NaiveCrossfade {
                    requested_fade: Duration::from_millis(400),
                },
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, ref current }) if current == &second_key) {}
        })
        .await
        .unwrap();

        pipeline
            .roll(RollingPlan {
                generation: 1,
                current: second_key,
                change: RollingChange::Attach(PlannedNext {
                    track: third,
                    transition: TransitionPlan::NaiveCrossfade {
                        requested_fade: Duration::from_millis(400),
                    },
                }),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, ref current }) if current == &third_key) {}
        })
        .await
        .unwrap();
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn rolling_replace_next_rejects_a_stale_terminal_key() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let replacement_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(replacement_file.path(), Duration::from_secs(1), 4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let replacement = track(replacement_file.path(), 2);
        let first_key = first.key.clone();
        let PipelineInstance { pipeline, .. } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        pipeline
            .replace(initial_plan(1, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        let result = pipeline
            .roll(RollingPlan {
                generation: 1,
                current: first_key,
                change: RollingChange::ReplaceNext {
                    expected_next: TrackKey {
                        queue_item_id: uuid::Uuid::new_v4(),
                        song_id: uuid::Uuid::new_v4(),
                    },
                    replacement: Some(PlannedNext {
                        track: replacement,
                        transition: TransitionPlan::Cut,
                    }),
                },
            })
            .await;
        assert!(matches!(result, Err(PipelineError::StalePlan)));
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn replacing_a_branch_releases_its_mixer_request_pad() {
        use crate::streamer::pipeline::{PipelineTrack, TrackKey};
        use tempfile::NamedTempFile;
        use uuid::Uuid;

        let file = NamedTempFile::new().unwrap();
        let wav = b"RIFF$\0\0\0WAVEfmt \x10\0\0\0\x01\0\x02\0\x44\xAC\0\0\x10\xB1\x02\0\x04\0\x10\0data\0\0\0\0";
        std::fs::write(file.path(), wav).unwrap();
        let instance = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let plan = |generation, mode| PairPlan {
            mode,
            generation,
            output_epoch: 1,
            current: PipelineTrack {
                key: TrackKey {
                    queue_item_id: Uuid::new_v4(),
                    song_id: Uuid::new_v4(),
                },
                path: file.path().to_path_buf(),
                cue_in: Duration::ZERO,
                cue_out: Duration::ZERO,
                cross_start_next: Duration::ZERO,
                analyzed: false,
            },
            next: None,
        };
        let first = plan(1, ReplaceMode::InitialReplaceFromStopped);
        let first_key = first.current.key.clone();
        instance.pipeline.replace(first).await.unwrap();
        instance
            .pipeline
            .replace(plan(
                2,
                ReplaceMode::ActiveReplace {
                    expected_generation: 1,
                    expected_current: first_key,
                },
            ))
            .await
            .unwrap();
        instance.pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn rolling_replace_next_swaps_only_the_terminal_branch() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let replacement_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(replacement_file.path(), Duration::from_secs(1), 4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let replacement = track(replacement_file.path(), 2);
        let first_key = first.key.clone();
        let second_key = second.key.clone();
        let replacement_key = replacement.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        pipeline
            .replace(initial_plan(1, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        pipeline
            .roll(RollingPlan {
                generation: 1,
                current: first_key,
                change: RollingChange::ReplaceNext {
                    expected_next: second_key,
                    replacement: Some(PlannedNext {
                        track: replacement,
                        transition: TransitionPlan::Cut,
                    }),
                },
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, ref current }) if current == &replacement_key) {}
        })
        .await
        .unwrap();
        pipeline.stop().await.unwrap();
    }

    #[test]
    fn libav_wma_file_decodes_to_raw_audio() {
        use tempfile::TempDir;

        fn run_to_eos(pipeline: gst::Pipeline) {
            pipeline.set_state(gst::State::Playing).unwrap();
            let message = pipeline
                .bus()
                .unwrap()
                .timed_pop_filtered(gst::ClockTime::from_seconds(10), &[gst::MessageType::Eos, gst::MessageType::Error])
                .expect("pipeline must reach EOS");
            pipeline.set_state(gst::State::Null).unwrap();
            assert_eq!(message.type_(), gst::MessageType::Eos, "{message:?}");
        }

        graph::init().unwrap();
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("fixture.wma");
        let location = path.to_str().unwrap();
        let encoder = gst::parse::launch(&format!(
            "audiotestsrc num-buffers=128 ! audioconvert ! avenc_wmav2 ! asfmux ! filesink location={location}"
        ))
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap();
        run_to_eos(encoder);

        let uri = gst::glib::filename_to_uri(&path, None).unwrap();
        let decoder = gst::parse::launch(&format!(
            "uridecodebin uri={} ! audioconvert ! audio/x-raw,format=F32LE,rate=44100,channels=2 ! fakesink sync=false",
            uri
        ))
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap();
        run_to_eos(decoder);
    }

    #[test]
    fn backbone_encoder_produces_nonempty_mp3() {
        use tempfile::TempDir;

        graph::init().unwrap();
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("output.mp3");
        let location = path.to_str().unwrap();
        let pipeline = gst::parse::launch(&format!(
            "audiotestsrc num-buffers=128 ! audioconvert ! audioresample ! audio/x-raw,format=S16LE,rate=44100,channels=2 ! lamemp3enc target=bitrate cbr=true bitrate=128 ! mpegaudioparse ! filesink location={location}"
        ))
        .unwrap()
        .downcast::<gst::Pipeline>()
        .unwrap();
        pipeline.set_state(gst::State::Playing).unwrap();
        let message = pipeline
            .bus()
            .unwrap()
            .timed_pop_filtered(gst::ClockTime::from_seconds(10), &[gst::MessageType::Eos, gst::MessageType::Error])
            .expect("MP3 encoder pipeline must reach EOS");
        pipeline.set_state(gst::State::Null).unwrap();
        assert_eq!(message.type_(), gst::MessageType::Eos, "{message:?}");
        assert!(!std::fs::read(path).unwrap().is_empty());
    }
}
