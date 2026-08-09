use std::sync::LazyLock;

use gst::prelude::*;
use gstreamer as gst;

use super::super::pipeline::{PipelineConfig, PipelineError};
use super::sink;

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

pub(super) struct Backbone {
    pub(super) pipeline: gst::Pipeline,
    pub(super) mixer: gst::Element,
    pub(super) sink: gst::Element,
    pub(super) clock_gate: gst::Element,
}

pub(super) fn init() -> Result<(), PipelineError> {
    GST_INIT
        .as_ref()
        .map(|_| ())
        .map_err(|message| PipelineError::Initialization(message.clone()))
}

pub(super) fn element(name: &'static str) -> Result<gst::Element, PipelineError> {
    gst::ElementFactory::make(name)
        .build()
        .map_err(|_| PipelineError::MissingElement(name))
}

pub(super) fn build_backbone(config: &PipelineConfig, sink_factory: &'static str) -> Result<Backbone, PipelineError> {
    init()?;
    for name in REQUIRED_ELEMENTS {
        if gst::ElementFactory::find(name).is_none() {
            return Err(PipelineError::MissingElement(name));
        }
    }
    if gst::ElementFactory::find(sink_factory).is_none() {
        return Err(PipelineError::MissingElement(sink_factory));
    }

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
    let sink = sink::build(sink_factory, &config.target)?;

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

    Ok(Backbone {
        pipeline,
        mixer,
        sink,
        clock_gate,
    })
}
