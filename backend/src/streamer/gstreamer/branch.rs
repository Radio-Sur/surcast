use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::super::pipeline::{PipelineError, PipelineEvent, PipelineTrack, TrackKey};
use super::graph;

pub(super) struct Branch {
    pub(super) generation: u64,
    pub(super) key: TrackKey,
    pub(super) elements: Vec<gst::Element>,
    pub(super) source: gst::Element,
    pub(super) volume: gst::Element,
    pub(super) timing_pad: gst::Pad,
    pub(super) gate: Option<gst::PadProbeId>,
    media_start: Mutex<Duration>,
    duration: Mutex<Option<Duration>>,
    pub(super) mixer_pad: Option<gst::Pad>,
}

#[cfg(test)]
impl Branch {
    pub(super) fn for_test(
        generation: u64,
        key: TrackKey,
        elements: Vec<gst::Element>,
        source: gst::Element,
        volume: gst::Element,
        timing_pad: gst::Pad,
    ) -> Self {
        Self {
            generation,
            key,
            elements,
            source,
            volume,
            timing_pad,
            gate: None,
            media_start: Mutex::new(Duration::ZERO),
            duration: Mutex::new(None),
            mixer_pad: None,
        }
    }
}
#[derive(Clone)]
pub(super) struct PreparingBranch {
    pub(super) generation: u64,
    pub(super) key: TrackKey,
    pub(super) elements: Vec<gst::Element>,
    pub(super) failed: Arc<AtomicBool>,
}

#[derive(Clone)]
pub(super) struct RetiringBranch {
    pub(super) retirement_id: u64,
    pub(super) elements: Vec<gst::Element>,
}

pub(super) struct BranchRegistry {
    pub(super) live: Vec<Branch>,
    pub(super) preparing: Vec<PreparingBranch>,
    pub(super) retiring: Vec<RetiringBranch>,
    next_retirement_id: u64,
}

impl BranchRegistry {
    pub(super) fn new() -> Self {
        Self {
            live: Vec::new(),
            preparing: Vec::new(),
            retiring: Vec::new(),
            next_retirement_id: 1,
        }
    }

    pub(super) fn register_preparing(&mut self, branch: &Branch) {
        self.preparing.push(PreparingBranch {
            generation: branch.generation,
            key: branch.key.clone(),
            elements: branch.elements.clone(),
            failed: Arc::new(AtomicBool::new(false)),
        });
    }

    pub(super) fn unregister_preparing(&mut self, key: &TrackKey) {
        self.preparing.retain(|b| b.key != *key);
    }

    pub(super) fn mark_preparing_failed(&mut self, key: &TrackKey) {
        for p in &self.preparing {
            if p.key == *key {
                p.failed.store(true, Ordering::Release);
            }
        }
    }

    pub(super) fn is_preparing_failed(&self, key: &TrackKey) -> bool {
        self.preparing
            .iter()
            .find(|b| b.key == *key)
            .is_some_and(|b| b.failed.load(Ordering::Acquire))
    }

    pub(super) fn alloc_retirement_id(&mut self) -> u64 {
        let id = self.next_retirement_id;
        self.next_retirement_id = self.next_retirement_id.wrapping_add(1);
        id
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
    for element in branch.elements.iter().rev() {
        element.set_locked_state(false);
        let _ = element.set_state(gst::State::Null);
        let _ = pipeline.remove(element);
    }
}
pub(super) fn attach(
    pipeline: &gst::Pipeline,
    mixer: &gst::Element,
    registry: Option<&Mutex<BranchRegistry>>,
    events: mpsc::UnboundedSender<PipelineEvent>,
    track: &PipelineTrack,
    generation: u64,
    initial_volume: f64,
) -> Result<Branch, PipelineError> {
    let mut branch = attach_inner(pipeline, events, track, generation, initial_volume)?;
    if let Some(reg) = registry {
        reg.lock().unwrap_or_else(|error| error.into_inner()).register_preparing(&branch);
    }
    if let Err(error) = link_mixer(mixer, &mut branch) {
        if let Some(reg) = registry {
            reg.lock().unwrap_or_else(|e| e.into_inner()).unregister_preparing(&branch.key);
        }
        discard(pipeline, mixer, branch);
        return Err(error);
    }
    for element in branch.elements.iter().rev() {
        if let Err(error) = element.sync_state_with_parent() {
            if let Some(reg) = registry {
                reg.lock().unwrap_or_else(|e| e.into_inner()).unregister_preparing(&branch.key);
            }
            discard(pipeline, mixer, branch);
            return Err(PipelineError::Pipeline(error.to_string()));
        }
    }
    Ok(branch)
}
pub(super) fn attach_paused(
    pipeline: &gst::Pipeline,
    mixer: &gst::Element,
    registry: Option<&Mutex<BranchRegistry>>,
    events: mpsc::UnboundedSender<PipelineEvent>,
    track: &PipelineTrack,
    generation: u64,
    initial_volume: f64,
) -> Result<Branch, PipelineError> {
    let mut branch = attach_inner(pipeline, events, track, generation, initial_volume)?;
    if let Some(reg) = registry {
        reg.lock().unwrap_or_else(|error| error.into_inner()).register_preparing(&branch);
    }
    if let Err(error) = link_mixer(mixer, &mut branch) {
        if let Some(reg) = registry {
            reg.lock().unwrap_or_else(|e| e.into_inner()).unregister_preparing(&branch.key);
        }
        discard(pipeline, mixer, branch);
        return Err(error);
    }
    let Some(gate) = branch
        .timing_pad
        .add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, |_, _| gst::PadProbeReturn::Ok)
    else {
        if let Some(reg) = registry {
            reg.lock().unwrap_or_else(|e| e.into_inner()).unregister_preparing(&branch.key);
        }
        discard(pipeline, mixer, branch);
        return Err(PipelineError::Pipeline("failed to install branch gate".into()));
    };
    branch.gate = Some(gate);
    if let Err(error) = lock_paused(&branch) {
        if let Some(reg) = registry {
            reg.lock().unwrap_or_else(|e| e.into_inner()).unregister_preparing(&branch.key);
        }
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

/// Unlocks the branch and syncs it to the parent's current state (PAUSED at
/// startup, PLAYING for a mid-stream roll). The mixer pad offset must already
/// be applied before this runs, so the first decoded buffer is scheduled at
/// its handover time instead of crossing immediately.
pub(super) fn prepare(branch: &mut Branch) -> Result<(), PipelineError> {
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

pub(super) struct ReleasePausedAction {
    pub(super) timing_pad: gst::Pad,
    pub(super) gate: gst::PadProbeId,
}

pub(super) fn take_paused_release(branch: &mut Branch) -> Option<ReleasePausedAction> {
    branch.gate.take().map(|gate| ReleasePausedAction {
        timing_pad: branch.timing_pad.clone(),
        gate,
    })
}

pub(super) fn apply_paused_release(action: ReleasePausedAction) {
    action.timing_pad.remove_probe(action.gate);
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
        generation,
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

pub(super) async fn wait_duration(registry: &Mutex<BranchRegistry>, index: usize) -> Option<Duration> {
    for _ in 0..100 {
        let duration = {
            let reg = registry.lock().unwrap_or_else(|error| error.into_inner());
            reg.live.get(index).and_then(duration)
        };
        if duration.is_some() {
            return duration;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

pub(super) async fn wait_branch_duration(branch: &Branch) -> Option<Duration> {
    for _ in 0..100 {
        let dur = duration(branch);
        if dur.is_some() {
            return dur;
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
