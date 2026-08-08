use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::pipeline::{
    IcecastTarget, PairPlan, PipelineConfig, PipelineError, PipelineEvent, PipelineInstance, PipelineSnapshot, PipelineState,
    PlaybackPipeline, PlaybackPipelineFactory,
};

const REQUIRED_ELEMENTS: &[&str] = &[
    "uridecodebin",
    "queue",
    "audioconvert",
    "audioresample",
    "capsfilter",
    "volume",
    "audiomixer",
    "lamemp3enc",
    "mpegaudioparse",
    "identity",
    "shout2send",
];

static GST_INIT: LazyLock<Result<(), String>> = LazyLock::new(|| gst::init().map_err(|error| error.to_string()));
fn init() -> Result<(), PipelineError> {
    GST_INIT
        .as_ref()
        .map(|_| ())
        .map_err(|message| PipelineError::Initialization(message.clone()))
}
fn element(name: &'static str) -> Result<gst::Element, PipelineError> {
    gst::ElementFactory::make(name)
        .build()
        .map_err(|_| PipelineError::MissingElement(name))
}

#[derive(Clone)]
pub(crate) struct GStreamerPipelineFactory {
    sink_factory: &'static str,
}

impl Default for GStreamerPipelineFactory {
    fn default() -> Self {
        Self {
            sink_factory: "shout2send",
        }
    }
}

impl GStreamerPipelineFactory {
    #[cfg(test)]
    fn with_test_sink() -> Self {
        Self { sink_factory: "fakesink" }
    }

    fn validate(&self) -> Result<(), PipelineError> {
        init()?;
        for name in REQUIRED_ELEMENTS {
            if gst::ElementFactory::find(name).is_none() {
                return Err(PipelineError::MissingElement(name));
            }
        }
        if gst::ElementFactory::find(self.sink_factory).is_none() {
            return Err(PipelineError::MissingElement(self.sink_factory));
        }
        Ok(())
    }

    fn build_backbone(&self, config: &PipelineConfig) -> Result<(gst::Pipeline, gst::Element, gst::Element), PipelineError> {
        self.validate()?;
        let pipeline = gst::Pipeline::new();
        let mixer = element("audiomixer")?;
        mixer.set_property("ignore-inactive-pads", true);
        let queue = element("queue")?;
        let threshold_ns = (config.prebuffer_bytes.max(1024) as u64).saturating_mul(1_000_000_000) / 16_000;
        queue.set_property("min-threshold-time", threshold_ns);
        queue.set_property("max-size-time", threshold_ns.saturating_mul(2).max(5_000_000_000));
        queue.set_property("max-size-bytes", 0u32);
        queue.set_property("max-size-buffers", 0u32);
        let convert = element("audioconvert")?;
        let resample = element("audioresample")?;
        let capsfilter = element("capsfilter")?;
        capsfilter.set_property(
            "caps",
            gst::Caps::builder("audio/x-raw")
                .field("format", "S16LE")
                .field("rate", config.sample_rate as i32)
                .field("channels", config.channels as i32)
                .field("layout", "interleaved")
                .build(),
        );
        let encoder = element("lamemp3enc")?;
        encoder.set_property_from_str("target", "bitrate");
        encoder.set_property("cbr", true);
        encoder.set_property("bitrate", config.bitrate_kbps as i32);
        let parser = element("mpegaudioparse")?;
        let clock_gate = element("identity")?;
        clock_gate.set_property("name", "clock_gate");
        clock_gate.set_property("sync", true);
        let sink = element(self.sink_factory)?;
        if self.sink_factory == "shout2send" {
            configure_sink(&sink, &config.target);
        } else {
            sink.set_property("sync", false);
        }
        pipeline
            .add_many([
                &mixer,
                &queue,
                &convert,
                &resample,
                &capsfilter,
                &encoder,
                &parser,
                &clock_gate,
                &sink,
            ])
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        gst::Element::link_many([
            &mixer,
            &queue,
            &convert,
            &resample,
            &capsfilter,
            &encoder,
            &parser,
            &clock_gate,
            &sink,
        ])
        .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        Ok((pipeline, mixer, sink))
    }
}

fn configure_sink(sink: &gst::Element, target: &IcecastTarget) {
    sink.set_property("ip", target.host.as_str());
    sink.set_property("port", target.port as i32);
    sink.set_property("mount", target.mount.as_str());
    sink.set_property("password", target.password.as_str());
    sink.set_property("streamname", target.stream_name.as_str());
    sink.set_property_from_str("protocol", "http");
    sink.set_property("username", "source");
    sink.set_property("sync", false);
}
pub(crate) struct GStreamerPipeline {
    pipeline: gst::Pipeline,
    mixer: gst::Element,
    sink: Mutex<gst::Element>,
    branches: Mutex<Vec<(Vec<gst::Element>, gst::Pad)>>,
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
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|error| error.into_inner());
        snapshot.state = state;
        if state == PipelineState::Stopped {
            snapshot.elapsed = Duration::ZERO;
        }
        Ok(())
    }

    fn clear_branches(&self) {
        let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
        for (elements, mixer_pad) in branches.drain(..) {
            self.mixer.release_request_pad(&mixer_pad);
            for element in elements {
                let _ = element.set_state(gst::State::Null);
                let _ = self.pipeline.remove(&element);
            }
        }
    }

    fn attach_track(&self, track: &super::pipeline::PipelineTrack) -> Result<(), PipelineError> {
        let uri = gst::glib::filename_to_uri(&track.path, None).map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let source = element("uridecodebin")?;
        source.set_property("uri", uri.as_str());
        let queue = element("queue")?;
        let convert = element("audioconvert")?;
        let resample = element("audioresample")?;
        let capsfilter = element("capsfilter")?;
        capsfilter.set_property(
            "caps",
            gst::Caps::builder("audio/x-raw")
                .field("format", "F32LE")
                .field("rate", 44_100i32)
                .field("channels", 2i32)
                .field("layout", "interleaved")
                .build(),
        );
        let volume = element("volume")?;
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
                    generation: 0,
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
        for branch in [&source, &queue, &convert, &resample, &capsfilter, &volume] {
            branch
                .sync_state_with_parent()
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        }
        self.branches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push((vec![source, queue, convert, resample, capsfilter, volume], mixer_pad));
        Ok(())
    }
}

#[async_trait]
impl PlaybackPipeline for GStreamerPipeline {
    async fn replace(&self, plan: PairPlan) -> Result<(), PipelineError> {
        self.clear_branches();
        self.attach_track(&plan.current)?;
        self.set_state(PipelineState::Playing)
    }

    async fn append(&self, plan: PairPlan) -> Result<(), PipelineError> {
        self.attach_track(&plan.current)
    }

    async fn set_playing(&self, playing: bool) -> Result<(), PipelineError> {
        self.set_state(if playing { PipelineState::Playing } else { PipelineState::Paused })
    }

    async fn reconnect(&self, target: IcecastTarget) -> Result<(), PipelineError> {
        configure_sink(&self.sink.lock().unwrap_or_else(|error| error.into_inner()), &target);
        Ok(())
    }

    async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
        Ok(self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).clone())
    }

    async fn stop(&self) -> Result<(), PipelineError> {
        self.set_state(PipelineState::Stopped)
    }
}

#[async_trait]
impl PlaybackPipelineFactory for GStreamerPipelineFactory {
    async fn create(&self, config: PipelineConfig) -> Result<PipelineInstance, PipelineError> {
        let (pipeline, mixer, sink) = self.build_backbone(&config)?;
        let (events, receiver) = mpsc::unbounded_channel();
        Ok(PipelineInstance {
            pipeline: Arc::new(GStreamerPipeline {
                pipeline,
                mixer,
                sink: Mutex::new(sink),
                branches: Mutex::new(Vec::new()),
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

        let instance = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        instance
            .pipeline
            .replace(PairPlan {
                generation: 1,
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
            })
            .await
            .unwrap();
        assert_eq!(instance.pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        instance.pipeline.stop().await.unwrap();
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

        init().unwrap();
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

        init().unwrap();
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
