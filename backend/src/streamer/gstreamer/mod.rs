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
    resolve_transition, IcecastTarget, PairPlan, PipelineConfig, PipelineError, PipelineEvent, PipelineInstance, PipelineSnapshot,
    PipelineState, PlaybackPipeline, PlaybackPipelineFactory, TrackKey,
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
    current: TrackKey,
    next: Option<TrackKey>,
    handover_at: Option<gst::ClockTime>,
    started_at: Option<gst::ClockTime>,
    last_elapsed: gst::ClockTime,
    handed_over: bool,
}

pub(crate) struct GStreamerPipeline {
    pipeline: gst::Pipeline,
    mixer: gst::Element,
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
    async fn replace(&self, plan: PairPlan) -> Result<(), PipelineError> {
        if self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(|active| active.generation > plan.generation)
        {
            return Err(PipelineError::StalePlan);
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
        if let (Some(next), Some(_)) = (plan.next.as_ref(), current_duration) {
            let next_branch = branch::attach(&self.pipeline, &self.mixer, self.events.clone(), next, plan.generation, 0.0)?;
            next_branch
                .source
                .state(gst::ClockTime::from_seconds(5))
                .0
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(next_branch);
            scheduled_next = Some(next.key.clone());
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
        let transition = resolve_transition(plan.transition, current_duration, next_duration, current_seekable, next_seekable);

        let handover_at = {
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            transition::apply(transition, &branches, current_duration)?
        };

        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = Some(ActivePlan {
            generation: plan.generation,
            current: plan.current.key,
            next: scheduled_next,
            handover_at,
            handed_over: false,
            started_at: None,
            last_elapsed: gst::ClockTime::ZERO,
        });
        self.set_state(PipelineState::Playing)
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
        {
            let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(plan) = active.as_mut() {
                plan.handover_at = plan.handover_at.map(|handover_at| handover_at.saturating_sub(plan.last_elapsed));
                plan.started_at = None;
                plan.last_elapsed = gst::ClockTime::ZERO;
            }
        }
        let position = self.pipeline.query_position::<gst::ClockTime>().unwrap_or(gst::ClockTime::ZERO);
        let transition = |target| -> Result<(), PipelineError> {
            self.pipeline
                .set_state(target)
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            let (result, current, pending) = self.pipeline.state(gst::ClockTime::from_seconds(5));
            result.map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            if current != target {
                return Err(PipelineError::Pipeline(format!(
                    "reconnect transition to {target:?} stalled at {current:?} with {pending:?} pending"
                )));
            }
            Ok(())
        };

        transition(gst::State::Ready)?;
        let old_sink = self.sink.lock().unwrap_or_else(|error| error.into_inner()).clone();
        self.clock_gate.unlink(&old_sink);
        old_sink
            .set_state(gst::State::Null)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        self.pipeline
            .remove(&old_sink)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;

        let new_sink = sink::build(self.sink_factory, &target)?;
        self.pipeline
            .add(&new_sink)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        self.clock_gate
            .link(&new_sink)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = new_sink;
        transition(gst::State::Paused)?;
        {
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(current) = branches.first() {
                branch::seek(current, Duration::from_nanos(position.nseconds()), None)?;
            }
        }

        if previous_state == PipelineState::Playing {
            self.set_state(PipelineState::Playing)?;
        }
        Ok(())
    }

    async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|error| error.into_inner());
        if snapshot.state != PipelineState::Stopped {
            if let Some(position) = self.pipeline.query_position::<gst::ClockTime>() {
                snapshot.elapsed = Duration::from_nanos(position.nseconds());
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
    use crate::streamer::pipeline::{IcecastTarget, PipelineTrack, TransitionPlan};

    fn config() -> PipelineConfig {
        PipelineConfig {
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            prebuffer_bytes: 16_384,
            sample_rate: 44_100,
            channels: 2,
            bitrate_kbps: 128,
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

    fn track(path: &std::path::Path, position: i32) -> PipelineTrack {
        PipelineTrack {
            key: TrackKey {
                queue_item_id: uuid::Uuid::new_v4(),
                song_id: uuid::Uuid::new_v4(),
                position,
            },
            path: path.to_path_buf(),
            cue_in: Duration::ZERO,
            cue_out: Duration::ZERO,
            cross_start_next: Duration::ZERO,
            analyzed: false,
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
        use crate::streamer::pipeline::{PairPlan, PipelineTrack, TrackKey, TransitionPlan};
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
            position: 0,
        };
        pipeline
            .replace(PairPlan {
                generation: 1,
                current: PipelineTrack {
                    key: key.clone(),
                    path: file.path().to_path_buf(),
                    cue_in: Duration::ZERO,
                    cue_out: Duration::ZERO,
                    cross_start_next: Duration::ZERO,
                    analyzed: false,
                },
                next: None,
                transition: TransitionPlan::Cut,
            })
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
            .replace(PairPlan {
                generation: 7,
                current,
                next: Some(next),
                transition: TransitionPlan::NaiveCrossfade {
                    requested_fade: Duration::from_millis(400),
                },
            })
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
        let plan = |generation, current, next| PairPlan {
            generation,
            current,
            next: Some(next),
            transition: TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_millis(400),
            },
        };

        pipeline.replace(plan(1, first, second.clone())).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, .. })) {}
        })
        .await
        .unwrap();

        let started = std::time::Instant::now();
        pipeline.replace(plan(2, second, third)).await.unwrap();
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
            .replace(PairPlan {
                generation: 8,
                current,
                next: Some(next),
                transition: TransitionPlan::AutoCueCrossfade {
                    current_start: Duration::from_millis(200),
                    fade_start: Duration::from_millis(800),
                    current_end: Duration::from_millis(1_200),
                    next_start: Duration::from_millis(100),
                    duration: Duration::from_millis(400),
                    fallback_fade: Duration::from_millis(300),
                },
            })
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
    async fn replacing_a_branch_releases_its_mixer_request_pad() {
        use crate::streamer::pipeline::{PairPlan, PipelineTrack, TrackKey, TransitionPlan};
        use tempfile::NamedTempFile;
        use uuid::Uuid;

        let file = NamedTempFile::new().unwrap();
        let wav = b"RIFF$\0\0\0WAVEfmt \x10\0\0\0\x01\0\x02\0\x44\xAC\0\0\x10\xB1\x02\0\x04\0\x10\0data\0\0\0\0";
        std::fs::write(file.path(), wav).unwrap();
        let instance = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let plan = |generation| PairPlan {
            generation,
            current: PipelineTrack {
                key: TrackKey {
                    queue_item_id: Uuid::new_v4(),
                    song_id: Uuid::new_v4(),
                    position: 0,
                },
                path: file.path().to_path_buf(),
                cue_in: Duration::ZERO,
                cue_out: Duration::ZERO,
                cross_start_next: Duration::ZERO,
                analyzed: false,
            },
            next: None,
            transition: TransitionPlan::Cut,
        };
        instance.pipeline.replace(plan(1)).await.unwrap();
        instance.pipeline.replace(plan(2)).await.unwrap();
        instance.pipeline.stop().await.unwrap();
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
