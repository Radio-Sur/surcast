use std::time::Duration;

use gst_controller::prelude::*;
use gstreamer as gst;
use gstreamer_controller as gst_controller;

use super::super::pipeline::{PipelineError, TransitionPlan};
use super::branch::{self, Branch};

pub(super) struct TransitionSchedule {
    pub handover: Option<gst::ClockTime>,
}

pub(super) fn apply_initial(
    transition: TransitionPlan,
    branches: &[Branch],
    current_duration: Option<Duration>,
) -> Result<TransitionSchedule, PipelineError> {
    let Some(current) = branches.first() else {
        return Ok(TransitionSchedule { handover: None });
    };
    apply(transition, current, branches.get(1), current_duration, Duration::ZERO, true)
}

pub(super) fn apply_replacement(
    transition: TransitionPlan,
    branches: &[Branch],
    current_duration: Option<Duration>,
    timeline_origin: Duration,
) -> Result<TransitionSchedule, PipelineError> {
    let Some(current) = branches.first() else {
        return Ok(TransitionSchedule { handover: None });
    };
    branch::set_offset(current, timeline_origin);
    rebase(
        apply(transition, current, branches.get(1), current_duration, timeline_origin, true)?,
        timeline_origin,
    )
}

pub(super) fn apply_rolling(
    transition: TransitionPlan,
    current: &Branch,
    next: &Branch,
    current_duration: Option<Duration>,
    timeline_origin: Duration,
    current_elapsed: Duration,
) -> Result<TransitionSchedule, PipelineError> {
    let physical_base = branch::offset(current);
    let logical_base = physical_base.saturating_sub(timeline_origin);
    let current_media_start = branch::media_start(current);
    let transition = match (transition, current_duration) {
        (TransitionPlan::NaiveCrossfade { requested_fade }, Some(duration)) => {
            let requested_fade = requested_fade.min(duration.saturating_sub(current_media_start));
            if requested_fade.is_zero() {
                TransitionPlan::Cut
            } else {
                TransitionPlan::NaiveCrossfade { requested_fade }
            }
        }
        (transition, _) => transition,
    };
    let transition = select_rolling_transition(transition, current_duration, current_media_start, logical_base, current_elapsed)?;
    rebase(
        apply(transition, current, Some(next), current_duration, physical_base, false)?,
        timeline_origin,
    )
}

fn select_rolling_transition(
    transition: TransitionPlan,
    current_duration: Option<Duration>,
    current_media_start: Duration,
    timeline_base: Duration,
    current_elapsed: Duration,
) -> Result<TransitionPlan, PipelineError> {
    if planned_handover(transition, current_duration, current_media_start, timeline_base).is_none_or(|handover| handover > current_elapsed)
    {
        return Ok(transition);
    }
    if planned_handover(TransitionPlan::Cut, current_duration, current_media_start, timeline_base)
        .is_none_or(|current_end| current_end <= current_elapsed)
    {
        return Err(PipelineError::StalePlan);
    }
    Ok(TransitionPlan::Cut)
}

fn planned_handover(
    transition: TransitionPlan,
    current_duration: Option<Duration>,
    current_media_start: Duration,
    timeline_base: Duration,
) -> Option<Duration> {
    match transition {
        TransitionPlan::Cut => current_duration.map(|duration| timeline_base.saturating_add(duration.saturating_sub(current_media_start))),
        TransitionPlan::NaiveCrossfade { requested_fade } => current_duration.map(|duration| {
            timeline_base
                .saturating_add(duration.saturating_sub(current_media_start).saturating_sub(requested_fade))
                .saturating_add(requested_fade / 2)
        }),
        TransitionPlan::AutoCueCrossfade { fade_start, duration, .. } => Some(
            timeline_base
                .saturating_add(fade_start.saturating_sub(current_media_start))
                .saturating_add(duration / 2),
        ),
    }
}

fn rebase(schedule: TransitionSchedule, timeline_origin: Duration) -> Result<TransitionSchedule, PipelineError> {
    let origin = clock_time(timeline_origin);
    Ok(TransitionSchedule {
        handover: schedule.handover.map(|handover| handover.saturating_sub(origin)),
    })
}

fn apply(
    transition: TransitionPlan,
    current: &Branch,
    next: Option<&Branch>,
    current_duration: Option<Duration>,
    timeline_base: Duration,
    seek_current: bool,
) -> Result<TransitionSchedule, PipelineError> {
    match (transition, next, current_duration) {
        (TransitionPlan::Cut, Some(next), Some(current_duration)) => {
            let handover = timeline_base.saturating_add(current_duration.saturating_sub(branch::media_start(current)));
            branch::set_offset(next, handover);
            next.volume.set_property("volume", 1.0f64);
            Ok(TransitionSchedule {
                handover: Some(clock_time(handover)),
            })
        }
        (TransitionPlan::NaiveCrossfade { requested_fade }, Some(next), Some(current_duration)) => {
            let fade_start = current_duration.saturating_sub(requested_fade);
            let local_fade_start = fade_start.saturating_sub(branch::media_start(current));
            branch::set_offset(next, timeline_base.saturating_add(local_fade_start));
            fade(&next.volume, (Duration::ZERO, 0.0), (requested_fade, 1.0))?;
            fade(&current.volume, (fade_start, 1.0), (current_duration, 0.0))?;
            Ok(TransitionSchedule {
                handover: Some(clock_time(
                    timeline_base.saturating_add(local_fade_start).saturating_add(requested_fade / 2),
                )),
            })
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
            if seek_current {
                branch::seek(current, current_start, Some(current_end))?;
            }
            branch::seek(next, next_start, None)?;
            let local_fade_start = fade_start.saturating_sub(branch::media_start(current));
            branch::set_offset(next, timeline_base.saturating_add(local_fade_start));
            fade(&next.volume, (next_start, 0.0), (next_start.saturating_add(duration), 1.0))?;
            fade(&current.volume, (fade_start, 1.0), (current_end, 0.0))?;
            Ok(TransitionSchedule {
                handover: Some(clock_time(
                    timeline_base.saturating_add(local_fade_start).saturating_add(duration / 2),
                )),
            })
        }
        _ => Ok(TransitionSchedule { handover: None }),
    }
}

fn clock_time(duration: Duration) -> gst::ClockTime {
    gst::ClockTime::from_nseconds(duration.as_nanos().min(u64::MAX as u128) as u64)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_crossfade_falls_back_to_the_remaining_track_end() {
        let transition = select_rolling_transition(
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_millis(400),
            },
            Some(Duration::from_secs(2)),
            Duration::from_millis(600),
            Duration::ZERO,
            Duration::from_millis(1300),
        )
        .unwrap();

        assert!(matches!(transition, TransitionPlan::Cut));
    }

    #[test]
    fn rolling_schedule_is_stale_after_the_current_track_end() {
        let result = select_rolling_transition(
            TransitionPlan::NaiveCrossfade {
                requested_fade: Duration::from_millis(400),
            },
            Some(Duration::from_secs(1)),
            Duration::ZERO,
            Duration::from_millis(600),
            Duration::from_millis(1600),
        );
        assert!(matches!(result, Err(PipelineError::StalePlan)));
    }
}
