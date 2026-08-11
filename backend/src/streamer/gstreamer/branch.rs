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
    pub(super) timing_pad: gst::Pad,
    pub(super) mixer_pad: Option<gst::Pad>,
    staging_sink: Option<gst::Element>,
}

pub(super) fn clear(pipeline: &gst::Pipeline, mixer: &gst::Element, branches: &mut Vec<Branch>) {
    for branch in branches.drain(..) {
        discard(pipeline, mixer, branch);
    }
}
pub(super) fn truncate(pipeline: &gst::Pipeline, mixer: &gst::Element, branches: &mut Vec<Branch>, len: usize) {
    while branches.len() > len {
        let branch = branches.pop().expect("length checked");
        discard(pipeline, mixer, branch);
    }
}
pub(super) fn remove_first(pipeline: &gst::Pipeline, mixer: &gst::Element, branches: &mut Vec<Branch>) {
    if !branches.is_empty() {
        discard(pipeline, mixer, branches.remove(0));
    }
}

pub(super) fn discard(pipeline: &gst::Pipeline, mixer: &gst::Element, branch: Branch) {
    if let Some(mixer_pad) = branch.mixer_pad {
        let _ = branch.timing_pad.unlink(&mixer_pad);
        mixer.release_request_pad(&mixer_pad);
    }
    for element in branch.elements {
        let _ = element.set_state(gst::State::Null);
        let _ = pipeline.remove(&element);
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
    let mut branch = attach_inner(pipeline, events, track, generation, initial_volume, None)?;
    link_mixer(mixer, &mut branch)?;
    Ok(branch)
}

pub(super) fn attach_staged(
    pipeline: &gst::Pipeline,
    events: mpsc::UnboundedSender<PipelineEvent>,
    track: &PipelineTrack,
    generation: u64,
    initial_volume: f64,
) -> Result<Branch, PipelineError> {
    let staging_sink = graph::element("fakesink")?;
    staging_sink.set_property("async", false);
    staging_sink.set_property("sync", false);
    attach_inner(pipeline, events, track, generation, initial_volume, Some(staging_sink))
}

fn attach_inner(
    pipeline: &gst::Pipeline,
    events: mpsc::UnboundedSender<PipelineEvent>,
    track: &PipelineTrack,
    generation: u64,
    initial_volume: f64,
    staging_sink: Option<gst::Element>,
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
    if let Some(staging_sink) = staging_sink.as_ref() {
        pipeline
            .add(staging_sink)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        volume
            .link(staging_sink)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    }
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
    let volume_src = volume
        .static_pad("src")
        .ok_or_else(|| PipelineError::Pipeline("volume has no source pad".into()))?;
    let mut elements = vec![source.clone(), queue, convert, resample, capsfilter, volume.clone()];
    if let Some(staging_sink) = staging_sink.as_ref() {
        elements.push(staging_sink.clone());
    }
    for element in &elements {
        element
            .sync_state_with_parent()
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    }
    Ok(Branch {
        elements,
        source,
        volume,
        timing_pad: volume_src,
        mixer_pad: None,
        staging_sink,
    })
}

pub(super) fn activate(pipeline: &gst::Pipeline, mixer: &gst::Element, branch: &mut Branch) -> Result<(), PipelineError> {
    if branch.mixer_pad.is_some() {
        return Ok(());
    }
    let staging_sink = branch
        .staging_sink
        .take()
        .ok_or_else(|| PipelineError::Pipeline("staged branch has no fakesink".into()))?;
    let sink_pad = staging_sink
        .static_pad("sink")
        .ok_or_else(|| PipelineError::Pipeline("fakesink has no sink pad".into()))?;
    branch
        .timing_pad
        .unlink(&sink_pad)
        .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    staging_sink
        .set_state(gst::State::Null)
        .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    pipeline
        .remove(&staging_sink)
        .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    branch.elements.retain(|element| element != &staging_sink);
    link_mixer(mixer, branch)
}

fn link_mixer(mixer: &gst::Element, branch: &mut Branch) -> Result<(), PipelineError> {
    let mixer_pad = mixer
        .request_pad_simple("sink_%u")
        .ok_or_else(|| PipelineError::Pipeline("mixer rejected request pad".into()))?;
    branch
        .timing_pad
        .link(&mixer_pad)
        .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    branch.mixer_pad = Some(mixer_pad);
    Ok(())
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
    branch.timing_pad.set_offset(offset.as_nanos().min(i64::MAX as u128) as i64);
}
