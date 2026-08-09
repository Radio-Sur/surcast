use std::time::Duration;

use gst_controller::prelude::*;
use gstreamer as gst;
use gstreamer_controller as gst_controller;

use super::super::pipeline::{PipelineError, TransitionPlan};
use super::branch::{self, Branch};

pub(super) fn apply(
    transition: TransitionPlan,
    branches: &[Branch],
    current_duration: Option<Duration>,
) -> Result<Option<gst::ClockTime>, PipelineError> {
    match (transition, branches.get(1), current_duration) {
        (TransitionPlan::Cut, Some(next), Some(current_duration)) => {
            branch::set_offset(next, current_duration);
            next.volume.set_property("volume", 1.0f64);
            Ok(Some(gst::ClockTime::from_nseconds(current_duration.as_nanos() as u64)))
        }
        (TransitionPlan::NaiveCrossfade { requested_fade }, Some(next), Some(current_duration)) => {
            let fade_start = current_duration.saturating_sub(requested_fade);
            branch::set_offset(next, fade_start);
            fade(&branches[0].volume, (fade_start, 1.0), (current_duration, 0.0))?;
            fade(&next.volume, (Duration::ZERO, 0.0), (requested_fade, 1.0))?;
            Ok(Some(gst::ClockTime::from_nseconds(
                fade_start.saturating_add(requested_fade / 2).as_nanos() as u64,
            )))
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
            branch::seek(&branches[0], current_start, Some(current_end))?;
            branch::seek(next, next_start, None)?;
            let local_fade_start = fade_start.saturating_sub(current_start);
            branch::set_offset(next, local_fade_start);

            // Controlled properties are synchronized using stream time. A seek
            // changes the media position, so controller points must use absolute
            // cue positions rather than local running-time offsets.
            fade(&branches[0].volume, (fade_start, 1.0), (current_end, 0.0))?;
            fade(&next.volume, (next_start, 0.0), (next_start.saturating_add(duration), 1.0))?;
            Ok(Some(gst::ClockTime::from_nseconds(
                local_fade_start.saturating_add(duration / 2).as_nanos() as u64,
            )))
        }
        _ => Ok(None),
    }
}

fn fade(volume: &gst::Element, start: (Duration, f64), end: (Duration, f64)) -> Result<(), PipelineError> {
    let source = gst_controller::InterpolationControlSource::new();
    source.set_mode(gst_controller::InterpolationMode::Linear);
    source.set(gst::ClockTime::from_nseconds(start.0.as_nanos() as u64), start.1);
    source.set(gst::ClockTime::from_nseconds(end.0.as_nanos() as u64), end.1);
    let binding = gst_controller::DirectControlBinding::new_absolute(volume, "volume", &source);
    volume
        .add_control_binding(&binding)
        .map_err(|error| PipelineError::Pipeline(error.to_string()))
}
