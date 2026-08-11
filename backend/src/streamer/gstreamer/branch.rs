use std::sync::Mutex;
use std::time::Duration;

use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::super::pipeline::{PipelineError, PipelineEvent, PipelineTrack, TrackKey};
use super::graph;

pub(super) struct Branch {
    pub(super) key: TrackKey,
    pub(super) elements: Vec<gst::Element>,
    pub(super) source: gst::Element,
    pub(super) volume: gst::Element,
    pub(super) timing_pad: gst::Pad,
    gate: Option<gst::PadProbeId>,
    media_start: Mutex<Duration>,
    duration: Mutex<Option<Duration>>,
    pub(super) mixer_pad: Option<gst::Pad>,
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
pub(super) fn remove_at(pipeline: &gst::Pipeline, mixer: &gst::Element, branches: &mut Vec<Branch>, index: usize) {
    if index < branches.len() {
        discard(pipeline, mixer, branches.remove(index));
    }
}

pub(super) fn discard(pipeline: &gst::Pipeline, mixer: &gst::Element, mut branch: Branch) {
    if let Some(mixer_pad) = branch.mixer_pad.take() {
        let _ = branch.timing_pad.unlink(&mixer_pad);
        mixer.release_request_pad(&mixer_pad);
    }
    remove_elements(pipeline, branch);
}

fn remove_elements(pipeline: &gst::Pipeline, mut branch: Branch) {
    if let Some(gate) = branch.gate.take() {
        branch.timing_pad.remove_probe(gate);
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
    let mut branch = attach_inner(pipeline, events, track, generation, initial_volume)?;
    if let Err(error) = link_mixer(mixer, &mut branch) {
        discard(pipeline, mixer, branch);
        return Err(error);
    }
    if let Err(error) = sync_with_parent(&branch) {
        discard(pipeline, mixer, branch);
        return Err(error);
    }
    Ok(branch)
}

pub(super) fn attach_paused(
    pipeline: &gst::Pipeline,
    mixer: &gst::Element,
    events: mpsc::UnboundedSender<PipelineEvent>,
    track: &PipelineTrack,
    generation: u64,
    initial_volume: f64,
) -> Result<Branch, PipelineError> {
    let mut branch = attach_inner(pipeline, events, track, generation, initial_volume)?;
    if let Err(error) = link_mixer(mixer, &mut branch) {
        discard(pipeline, mixer, branch);
        return Err(error);
    }
    let Some(gate) = branch
        .timing_pad
        .add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, |_, _| gst::PadProbeReturn::Ok)
    else {
        discard(pipeline, mixer, branch);
        return Err(PipelineError::Pipeline("failed to install branch gate".into()));
    };
    branch.gate = Some(gate);
    if let Err(error) = lock_paused(&branch) {
        discard(pipeline, mixer, branch);
        return Err(error);
    }
    Ok(branch)
}

fn lock_paused(branch: &Branch) -> Result<(), PipelineError> {
    for element in branch.elements.iter().rev() {
        element.set_locked_state(true);
        element
            .set_state(gst::State::Paused)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    }
    Ok(())
}

pub(super) fn prepare_paused(branch: &mut Branch) -> Result<(), PipelineError> {
    for element in branch.elements.iter().rev() {
        element.set_locked_state(false);
        if let Err(error) = element.sync_state_with_parent() {
            let message = error.to_string();
            let _ = lock_paused(branch);
            return Err(PipelineError::Pipeline(message));
        }
    }
    Ok(())
}

pub(super) fn release_paused(branch: &mut Branch) {
    if let Some(gate) = branch.gate.take() {
        branch.timing_pad.remove_probe(gate);
    }
}

pub(super) fn activate_paused(branch: &mut Branch) -> Result<(), PipelineError> {
    prepare_paused(branch)?;
    release_paused(branch);
    Ok(())
}

fn attach_inner(
    pipeline: &gst::Pipeline,
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
    let timing = graph::element("identity")?;
    pipeline
        .add_many([&source, &queue, &convert, &resample, &capsfilter, &volume, &timing])
        .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    gst::Element::link_many([&queue, &convert, &resample, &capsfilter, &volume, &timing])
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
    let timing_pad = timing
        .static_pad("src")
        .ok_or_else(|| PipelineError::Pipeline("timing element has no source pad".into()))?;
    let elements = vec![source.clone(), queue, convert, resample, capsfilter, volume.clone(), timing.clone()];
    Ok(Branch {
        key: track.key.clone(),
        elements,
        source,
        volume,
        timing_pad,
        gate: None,
        media_start: Mutex::new(Duration::ZERO),
        duration: Mutex::new(None),
        mixer_pad: None,
    })
}

fn sync_with_parent(branch: &Branch) -> Result<(), PipelineError> {
    for element in branch.elements.iter().rev() {
        element
            .sync_state_with_parent()
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
    }
    Ok(())
}

fn link_mixer(mixer: &gst::Element, branch: &mut Branch) -> Result<(), PipelineError> {
    let mixer_pad = mixer
        .request_pad_simple("sink_%u")
        .ok_or_else(|| PipelineError::Pipeline("mixer rejected request pad".into()))?;
    if let Err(error) = branch.timing_pad.link(&mixer_pad) {
        mixer.release_request_pad(&mixer_pad);
        return Err(PipelineError::Pipeline(error.to_string()));
    }
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
    let mut cached = branch.duration.lock().unwrap_or_else(|error| error.into_inner());
    if cached.is_none() {
        *cached = branch
            .source
            .query_duration::<gst::ClockTime>()
            .or_else(|| branch.volume.query_duration::<gst::ClockTime>())
            .map(|duration| Duration::from_nanos(duration.nseconds()));
    }
    *cached
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
    seek_to(branch, start, end)?;
    *branch.media_start.lock().unwrap_or_else(|error| error.into_inner()) = start;
    Ok(())
}

fn seek_to(branch: &Branch, start: Duration, end: Option<Duration>) -> Result<(), PipelineError> {
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

pub(super) fn media_start(branch: &Branch) -> Duration {
    *branch.media_start.lock().unwrap_or_else(|error| error.into_inner())
}

pub(super) fn set_offset(branch: &Branch, offset: Duration) {
    if let Some(mixer_pad) = branch.mixer_pad.as_ref() {
        mixer_pad.set_offset(offset.as_nanos().min(i64::MAX as u128) as i64);
    }
}

pub(super) fn offset(branch: &Branch) -> Duration {
    branch
        .mixer_pad
        .as_ref()
        .map_or(Duration::ZERO, |mixer_pad| Duration::from_nanos(mixer_pad.offset().max(0) as u64))
}
