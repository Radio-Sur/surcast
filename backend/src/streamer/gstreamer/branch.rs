use std::sync::Mutex;
use std::time::Duration;

use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::super::pipeline::{PipelineError, PipelineEvent, PipelineTrack};
use super::graph;

pub(super) struct Branch {
    pub(super) elements: Vec<gst::Element>,
    pub(super) source: gst::Element,
    pub(super) volume: gst::Element,
    pub(super) mixer_pad: gst::Pad,
}

pub(super) fn clear(pipeline: &gst::Pipeline, mixer: &gst::Element, branches: &mut Vec<Branch>) {
    for branch in branches.drain(..) {
        mixer.release_request_pad(&branch.mixer_pad);
        for element in branch.elements {
            let _ = element.set_state(gst::State::Null);
            let _ = pipeline.remove(&element);
        }
    }
}

pub(super) fn attach(
    pipeline: &gst::Pipeline,
    mixer: &gst::Element,
    events: mpsc::UnboundedSender<PipelineEvent>,
    track: &PipelineTrack,
    generation: u64,
    initial_volume: f64,
) -> Result<Branch, PipelineError> {
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
    pipeline
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
    let mixer_pad = mixer
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

pub(super) async fn wait_duration(branches: &Mutex<Vec<Branch>>, index: usize) -> Option<Duration> {
    for _ in 0..100 {
        let duration = {
            let branches = branches.lock().unwrap_or_else(|error| error.into_inner());
            branches.get(index).and_then(duration)
        };
        if duration.is_some() {
            return duration;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

pub(super) fn duration(branch: &Branch) -> Option<Duration> {
    branch
        .volume
        .query_duration::<gst::ClockTime>()
        .or_else(|| branch.source.query_duration::<gst::ClockTime>())
        .map(|duration| Duration::from_nanos(duration.nseconds()))
}

pub(super) fn seekable(branch: &Branch) -> bool {
    [&branch.volume, &branch.source].into_iter().any(|element| {
        let mut query = gst::query::Seeking::new(gst::Format::Time);
        element.query(&mut query) && query.result().0
    })
}

pub(super) fn seek(branch: &Branch, start: Duration, end: Option<Duration>) -> Result<(), PipelineError> {
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

pub(super) fn set_offset(branch: &Branch, offset: Duration) {
    branch.mixer_pad.set_offset(offset.as_nanos().min(i64::MAX as u128) as i64);
}
