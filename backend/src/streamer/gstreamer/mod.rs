mod graph;
mod sink;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use gst::prelude::*;
use gst_controller::prelude::*;
use gstreamer as gst;
use gstreamer_controller as gst_controller;
use tokio::sync::mpsc;

use super::pipeline::{
    resolve_transition, IcecastTarget, PairPlan, PipelineConfig, PipelineError, PipelineEvent, PipelineInstance, PipelineSnapshot,
    PipelineState, PipelineTrack, PlaybackPipeline, PlaybackPipelineFactory, TrackKey, TransitionPlan,
};

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
struct Branch {
    elements: Vec<gst::Element>,
    source: gst::Element,
    volume: gst::Element,
    mixer_pad: gst::Pad,
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

    fn clear_branches(&self) {
        let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
        for branch in branches.drain(..) {
            self.mixer.release_request_pad(&branch.mixer_pad);
            for element in branch.elements {
                let _ = element.set_state(gst::State::Null);
                let _ = self.pipeline.remove(&element);
            }
        }
    }

    fn attach_track(&self, track: &PipelineTrack, generation: u64, initial_volume: f64) -> Result<Branch, PipelineError> {
        let uri = gst::glib::filename_to_uri(&track.path, None).map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let source = graph::element("uridecodebin")?;
        source.set_property("uri", uri.as_str());
        let queue = graph::element("queue")?;
        let convert = graph::element("audioconvert")?;
        let resample = graph::element("audioresample")?;
        let capsfilter = graph::element("capsfilter")?;
        capsfilter.set_property(
            "caps",
            gst::Caps::builder("audio/x-raw")
                .field("format", "F32LE")
                .field("rate", 44_100i32)
                .field("channels", 2i32)
                .field("layout", "interleaved")
                .build(),
        );
        let volume = graph::element("volume")?;
        volume.set_property("volume", initial_volume);
        self.pipeline
            .add_many([&source, &queue, &convert, &resample, &capsfilter, &volume])
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        gst::Element::link_many([&queue, &convert, &resample, &capsfilter, &volume])
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let queue_sink = queue
            .static_pad("sink")
            .ok_or_else(|| PipelineError::Pipeline("queue has no sink pad".into()))?;
        let audio_sink = queue_sink.clone();
        source.connect_pad_added(move |_, pad| {
            let is_audio = pad
                .current_caps()
                .or_else(|| Some(pad.query_caps(None)))
                .and_then(|caps| caps.structure(0).map(|structure| structure.name().starts_with("audio/x-raw")))
                .unwrap_or(false);
            if is_audio && !audio_sink.is_linked() {
                let _ = pad.link(&audio_sink);
            }
        });
        let no_audio_sink = queue_sink;
        let events = self.events.clone();
        let key = track.key.clone();
        source.connect_no_more_pads(move |_| {
            if !no_audio_sink.is_linked() {
                let _ = events.send(PipelineEvent::DecodeFailed {
                    generation,
                    track: key.clone(),
                    message: "decoder exposed no audio/x-raw pad".into(),
                });
            }
        });
        let mixer_pad = self
            .mixer
            .request_pad_simple("sink_%u")
            .ok_or_else(|| PipelineError::Pipeline("mixer rejected request pad".into()))?;
        let volume_src = volume
            .static_pad("src")
            .ok_or_else(|| PipelineError::Pipeline("volume has no source pad".into()))?;
        volume_src
            .link(&mixer_pad)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let elements = vec![source.clone(), queue, convert, resample, capsfilter, volume.clone()];
        for element in &elements {
            element
                .sync_state_with_parent()
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        }
        Ok(Branch {
            elements,
            source,
            volume,
            mixer_pad,
        })
    }

    fn duration(branch: &Branch) -> Option<Duration> {
        branch
            .volume
            .query_duration::<gst::ClockTime>()
            .or_else(|| branch.source.query_duration::<gst::ClockTime>())
            .map(|duration| Duration::from_nanos(duration.nseconds()))
    }
    async fn wait_duration(&self, index: usize) -> Option<Duration> {
        for _ in 0..100 {
            let duration = {
                let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
                branches.get(index).and_then(Self::duration)
            };
            if duration.is_some() {
                return duration;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }

    fn seekable(branch: &Branch) -> bool {
        [&branch.volume, &branch.source].into_iter().any(|element| {
            let mut query = gst::query::Seeking::new(gst::Format::Time);
            element.query(&mut query) && query.result().0
        })
    }

    fn seek(branch: &Branch, start: Duration, end: Option<Duration>) -> Result<(), PipelineError> {
        if start.is_zero() && end.is_none() {
            return Ok(());
        }
        branch
            .source
            .seek(
                1.0,
                gst::SeekFlags::FLUSH | gst::SeekFlags::ACCURATE,
                gst::SeekType::Set,
                gst::ClockTime::from_nseconds(start.as_nanos() as u64),
                end.map_or(gst::SeekType::None, |_| gst::SeekType::Set),
                end.map(|end| gst::ClockTime::from_nseconds(end.as_nanos() as u64)),
            )
            .map_err(|error| PipelineError::Pipeline(error.to_string()))
    }

    fn fade(volume: &gst::Element, start: (Duration, f64), end: (Duration, f64)) -> Result<(), PipelineError> {
        let source = gst_controller::InterpolationControlSource::new();
        source.set_mode(gst_controller::InterpolationMode::Linear);
        source.set(gst::ClockTime::from_nseconds(start.0.as_nanos() as u64), start.1);
        source.set(gst::ClockTime::from_nseconds(end.0.as_nanos() as u64), end.1);
        let binding = gst_controller::DirectControlBinding::new(volume, "volume", &source);
        volume
            .add_control_binding(&binding)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))
    }

    fn set_offset(branch: &Branch, offset: Duration) {
        branch.mixer_pad.set_offset(offset.as_nanos().min(i64::MAX as u128) as i64);
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
        self.clear_branches();

        let current = self.attach_track(&plan.current, plan.generation, 1.0)?;
        self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(current);
        self.set_state(PipelineState::Paused)?;
        let current_duration = self.wait_duration(0).await;

        let mut scheduled_next = None;
        if let (Some(next), Some(_)) = (plan.next.as_ref(), current_duration) {
            let next_branch = self.attach_track(next, plan.generation, 0.0)?;
            next_branch
                .source
                .state(gst::ClockTime::from_seconds(5))
                .0
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(next_branch);
            scheduled_next = Some(next.key.clone());
        }
        let next_duration = if scheduled_next.is_some() {
            self.wait_duration(1).await
        } else {
            None
        };
        let (current_seekable, next_seekable) = {
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            (Self::seekable(&branches[0]), branches.get(1).is_some_and(Self::seekable))
        };
        let transition = resolve_transition(plan.transition, current_duration, next_duration, current_seekable, next_seekable);

        let handover_at = {
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            match (transition, branches.get(1), current_duration) {
                (TransitionPlan::Cut, Some(next), Some(current_duration)) => {
                    next.volume.set_property("volume", 1.0f64);
                    Self::set_offset(next, current_duration);
                    Some(gst::ClockTime::from_nseconds(current_duration.as_nanos() as u64))
                }
                (TransitionPlan::NaiveCrossfade { requested_fade }, Some(next), Some(current_duration)) => {
                    let fade_start = current_duration.saturating_sub(requested_fade);
                    Self::fade(&branches[0].volume, (fade_start, 1.0), (current_duration, 0.0))?;
                    Self::fade(&next.volume, (Duration::ZERO, 0.0), (requested_fade, 1.0))?;
                    Self::set_offset(next, fade_start);
                    Some(gst::ClockTime::from_nseconds(
                        fade_start.saturating_add(requested_fade / 2).as_nanos() as u64,
                    ))
                }
                (
                    TransitionPlan::AutoCueCrossfade {
                        current_start,
                        fade_start,
                        current_end,
                        next_start,
                        duration,
                        ..
                    },
                    Some(next),
                    _,
                ) => {
                    Self::seek(&branches[0], current_start, Some(current_end))?;
                    Self::seek(next, next_start, None)?;
                    let local_fade_start = fade_start.saturating_sub(current_start);
                    let local_current_end = current_end.saturating_sub(current_start);
                    Self::fade(&branches[0].volume, (local_fade_start, 1.0), (local_current_end, 0.0))?;
                    Self::fade(&next.volume, (Duration::ZERO, 0.0), (duration, 1.0))?;
                    Self::set_offset(next, local_fade_start);
                    Some(gst::ClockTime::from_nseconds(
                        local_fade_start.saturating_add(duration / 2).as_nanos() as u64,
                    ))
                }
                _ => None,
            }
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
                Self::seek(current, Duration::from_nanos(position.nseconds()), None)?;
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
        let handover_active = active.clone();
        let handover_events = events.clone();
        let clock_gate_src = clock_gate
            .static_pad("src")
            .ok_or_else(|| PipelineError::Pipeline("clock gate has no source pad".into()))?;
        clock_gate_src.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            let Some(gst::PadProbeData::Buffer(buffer)) = &info.data else {
                return gst::PadProbeReturn::Ok;
            };
            let handover = {
                let mut active = handover_active.lock().unwrap_or_else(|error| error.into_inner());
                active.as_mut().and_then(|plan| {
                    let due = buffer.pts().is_some_and(|pts| {
                        let started_at = *plan.started_at.get_or_insert(pts);
                        let elapsed = pts.saturating_sub(started_at);
                        plan.last_elapsed = elapsed;
                        plan.handover_at.is_some_and(|handover_at| elapsed >= handover_at)
                    });
                    if due && !plan.handed_over {
                        plan.handed_over = true;
                        plan.next.take().map(|next| {
                            plan.current = next.clone();
                            (plan.generation, next)
                        })
                    } else {
                        None
                    }
                })
            };
            if let Some((generation, current)) = handover {
                let _ = handover_events.send(PipelineEvent::Handover { generation, current });
            }
            gst::PadProbeReturn::Ok
        });
        let eos_active = active.clone();
        let eos_events = events.clone();
        clock_gate_src.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |_, info| {
            if let Some(gst::PadProbeData::Event(event)) = &info.data {
                if event.type_() == gst::EventType::Eos {
                    if let Some(plan) = eos_active.lock().unwrap_or_else(|error| error.into_inner()).clone() {
                        let _ = eos_events.send(PipelineEvent::CurrentEos {
                            generation: plan.generation,
                            current: plan.current,
                        });
                    }
                }
            }
            gst::PadProbeReturn::Ok
        });
        let bus = pipeline
            .bus()
            .ok_or_else(|| PipelineError::Pipeline("pipeline has no bus".into()))?;
        let bus_events = events.clone();
        let bus_active = active.clone();
        bus.set_sync_handler(move |_, message| {
            if let gst::MessageView::Error(error) = message.view() {
                if let Some(active) = bus_active.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
                    let _ = bus_events.send(PipelineEvent::SinkDisconnected {
                        generation: active.generation,
                        message: error.error().to_string(),
                    });
                }
            }
            gst::BusSyncReply::Pass
        });
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
    use crate::streamer::pipeline::IcecastTarget;

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
