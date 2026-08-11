mod branch;
mod bus;
mod graph;
mod sink;
mod transition;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::pipeline::{
    resolve_transition, IcecastTarget, OutputConfig, PairPlan, PipelineConfig, PipelineError, PipelineEvent, PipelineInstance,
    PipelineSnapshot, PipelineState, PlaybackPipeline, PlaybackPipelineFactory, ReplaceMode, RollingChange, RollingPlan, TrackKey,
    TransitionPlan,
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
    output_epoch: u64,
    current: TrackKey,
    next: Option<TrackKey>,
    handover_at: Option<gst::ClockTime>,
    started_at: Option<gst::ClockTime>,
    timeline_origin: gst::ClockTime,
    current_epoch: gst::ClockTime,
    last_elapsed: gst::ClockTime,
    handed_over: bool,
}

#[derive(Clone)]
pub(super) struct ReplaceCancellation {
    expected_generation: u64,
    expected_current: TrackKey,
    cancelled: Arc<AtomicBool>,
}

impl ReplaceCancellation {
    pub(super) fn cancel_if_matches(&self, generation: u64, current: &TrackKey) -> bool {
        if self.expected_generation == generation && self.expected_current == *current {
            self.cancelled.store(true, Ordering::Release);
            true
        } else {
            false
        }
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct ReplacementCleanup(Arc<Mutex<Option<ReplaceCancellation>>>);

impl Drop for ReplacementCleanup {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = None;
    }
}

enum SinkSlot {
    Active(gst::Element),
    Replacing { _old_sink: gst::Element, _candidate: gst::Element },
}

pub(crate) struct GStreamerPipeline {
    pipeline: gst::Pipeline,
    mixer: gst::Element,
    output_queue: gst::Element,
    output_caps: gst::Element,
    encoder: gst::Element,
    sink: Mutex<SinkSlot>,
    clock_gate: gst::Element,
    sink_factory: &'static str,
    branches: Mutex<Vec<Branch>>,
    active: Arc<Mutex<Option<ActivePlan>>>,
    replacing: Arc<Mutex<Option<ReplaceCancellation>>>,
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
    fn restore_state(&self, state: PipelineState) -> Result<(), PipelineError> {
        self.set_state(state)
    }
    fn force_stopped(&self) {
        let _ = self.pipeline.set_state(gst::State::Null);
        {
            let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            branch::clear(&self.pipeline, &self.mixer, &mut branches);
        }
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = None;
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|error| error.into_inner());
        snapshot.state = PipelineState::Stopped;
        snapshot.elapsed = Duration::ZERO;
    }

    fn finish_paused_transaction(&self, previous_state: PipelineState, result: Result<(), PipelineError>) -> Result<(), PipelineError> {
        match self.restore_state(previous_state) {
            Ok(()) => result,
            Err(error) => {
                self.force_stopped();
                Err(error)
            }
        }
    }

    fn replace_active(&self, plan: &PairPlan, cancellation: &ReplaceCancellation) -> Result<(), PipelineError> {
        let previous_state = self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).state;
        let mut candidates = Vec::with_capacity(2);
        candidates.push(branch::attach_paused(
            &self.pipeline,
            &self.mixer,
            self.events.clone(),
            &plan.current,
            plan.generation,
            1.0,
        )?);
        if let Some(next) = plan.next.as_ref() {
            match branch::attach_paused(&self.pipeline, &self.mixer, self.events.clone(), &next.track, plan.generation, 0.0) {
                Ok(candidate) => candidates.push(candidate),
                Err(error) => {
                    branch::clear(&self.pipeline, &self.mixer, &mut candidates);
                    return Err(error);
                }
            }
        }
        if let Err(error) = self.set_state(PipelineState::Paused) {
            branch::clear(&self.pipeline, &self.mixer, &mut candidates);
            return Err(error);
        }
        if cancellation.is_cancelled() {
            branch::clear(&self.pipeline, &self.mixer, &mut candidates);
            self.restore_state(previous_state)?;
            return Err(PipelineError::StalePlan);
        }
        let timeline_base = {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let Some(active) = active.as_ref() else {
                branch::clear(&self.pipeline, &self.mixer, &mut candidates);
                self.restore_state(previous_state)?;
                return Err(PipelineError::StalePlan);
            };
            Duration::from_nanos(
                active
                    .started_at
                    .unwrap_or(gst::ClockTime::ZERO)
                    .saturating_add(active.last_elapsed)
                    .nseconds(),
            )
        };

        let current_duration = branch::duration(&candidates[0]);
        if current_duration.is_none() {
            branch::truncate(&self.pipeline, &self.mixer, &mut candidates, 1);
        }
        for candidate in &mut candidates {
            if let Err(error) = branch::prepare(candidate) {
                branch::clear(&self.pipeline, &self.mixer, &mut candidates);
                self.restore_state(previous_state)?;
                return Err(error);
            }
        }
        let scheduled_next = plan
            .next
            .as_ref()
            .filter(|_| candidates.len() > 1)
            .map(|next| next.track.key.clone());
        let transition_plan = plan
            .next
            .as_ref()
            .filter(|_| candidates.len() > 1)
            .map_or(TransitionPlan::Cut, |next| next.transition);
        let next_duration = candidates.get(1).and_then(branch::duration);
        let transition = resolve_transition(
            transition_plan,
            current_duration,
            next_duration,
            branch::seekable(&candidates[0]),
            candidates.get(1).is_some_and(branch::seekable),
        );
        let handover_at = match transition::apply_replacement(transition, &candidates, current_duration, timeline_base) {
            Ok(schedule) => schedule.handover,
            Err(error) => {
                branch::clear(&self.pipeline, &self.mixer, &mut candidates);
                self.restore_state(previous_state)?;
                return Err(error);
            }
        };
        if cancellation.is_cancelled() {
            branch::clear(&self.pipeline, &self.mixer, &mut candidates);
            self.restore_state(previous_state)?;
            return Err(PipelineError::StalePlan);
        }

        {
            let mut live = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            branch::clear(&self.pipeline, &self.mixer, &mut live);
            live.extend(candidates);
        }
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = Some(ActivePlan {
            generation: plan.generation,
            output_epoch: plan.output_epoch,
            current: plan.current.key.clone(),
            next: scheduled_next,
            handover_at,
            handed_over: false,
            timeline_origin: gst::ClockTime::from_nseconds(timeline_base.as_nanos().min(u64::MAX as u128) as u64),
            started_at: None,
            current_epoch: gst::ClockTime::ZERO,
            last_elapsed: gst::ClockTime::ZERO,
        });
        {
            let mut live = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            for candidate in live.iter_mut() {
                branch::release_paused(candidate);
            }
        }
        if let Err(error) = self.restore_state(previous_state) {
            self.force_stopped();
            return Err(error);
        }
        Ok(())
    }
}

#[async_trait]
impl PlaybackPipeline for GStreamerPipeline {
    async fn apply_output(&self, output: OutputConfig) -> Result<(), PipelineError> {
        graph::configure_output(&self.output_queue, &self.output_caps, &self.encoder, output);
        Ok(())
    }

    async fn replace(&self, plan: PairPlan) -> Result<(), PipelineError> {
        {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            match (&plan.mode, active.as_ref()) {
                (ReplaceMode::InitialReplaceFromStopped, None) => {}
                (
                    ReplaceMode::ActiveReplace {
                        expected_generation,
                        expected_current,
                    },
                    Some(active),
                ) if active.generation == *expected_generation && active.current == *expected_current => {}
                _ => return Err(PipelineError::StalePlan),
            }
        }
        if let ReplaceMode::ActiveReplace {
            expected_generation,
            expected_current,
        } = &plan.mode
        {
            let cancellation = ReplaceCancellation {
                expected_generation: *expected_generation,
                expected_current: expected_current.clone(),
                cancelled: Arc::new(AtomicBool::new(false)),
            };
            *self.replacing.lock().unwrap_or_else(|error| error.into_inner()) = Some(cancellation.clone());
            let _cleanup = ReplacementCleanup(self.replacing.clone());
            return self.replace_active(&plan, &cancellation);
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
        let mut transition_plan = TransitionPlan::Cut;
        if let (Some(next), Some(_)) = (plan.next.as_ref(), current_duration) {
            let next_branch = branch::attach_paused(&self.pipeline, &self.mixer, self.events.clone(), &next.track, plan.generation, 0.0)?;
            next_branch
                .source
                .state(gst::ClockTime::from_seconds(5))
                .0
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(next_branch);
            scheduled_next = Some(next.track.key.clone());
            transition_plan = next.transition;
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
        let transition = resolve_transition(transition_plan, current_duration, next_duration, current_seekable, next_seekable);
        let handover_at = {
            let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            let handover = transition::apply_initial(transition, &branches, current_duration)?.handover;
            if branches.len() > 1 {
                branch::activate_paused(&mut branches[1])?;
            }
            handover
        };
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = Some(ActivePlan {
            generation: plan.generation,
            output_epoch: plan.output_epoch,
            current: plan.current.key,
            next: scheduled_next,
            handover_at,
            handed_over: false,
            timeline_origin: gst::ClockTime::ZERO,
            started_at: None,
            current_epoch: gst::ClockTime::ZERO,
            last_elapsed: gst::ClockTime::ZERO,
        });
        self.set_state(PipelineState::Playing)
    }

    async fn roll(&self, plan: RollingPlan) -> Result<(), PipelineError> {
        let (expected_next, replacement) = match plan.change {
            RollingChange::Attach(next) => (None, Some(next)),
            RollingChange::ReplaceNext {
                expected_next,
                replacement,
            } => (Some(expected_next), replacement),
        };
        let matches_plan = |active: &ActivePlan| {
            active.generation == plan.generation
                && active.current == plan.current
                && !expected_next.as_ref().is_some_and(|key| active.next.as_ref() != Some(key))
                && !(expected_next.is_none() && active.next.is_some())
        };
        {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            if !active.as_ref().is_some_and(&matches_plan) {
                return Err(PipelineError::StalePlan);
            }
        }

        let (current_index, obsolete_index, candidate_index) = {
            let branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            let current_index = branches
                .iter()
                .position(|branch| branch.key == plan.current)
                .ok_or(PipelineError::StalePlan)?;
            let obsolete_index = if let Some(expected) = expected_next.as_ref() {
                Some(
                    branches
                        .iter()
                        .position(|branch| branch.key == *expected)
                        .ok_or(PipelineError::StalePlan)?,
                )
            } else {
                branches
                    .iter()
                    .enumerate()
                    .find_map(|(index, _)| (index != current_index).then_some(index))
            };
            (current_index, obsolete_index, branches.len())
        };

        let Some(next) = replacement else {
            // Exhausted queue: drop the staged next without pausing the
            // pipeline. Removing a playing branch is the same teardown every
            // natural handover performs.
            let commit_result = (|| {
                {
                    let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
                    active
                        .as_ref()
                        .filter(|active| matches_plan(active))
                        .ok_or(PipelineError::StalePlan)?;
                }
                {
                    let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
                    if let Some(obsolete_index) = obsolete_index {
                        branch::remove_at(&self.pipeline, &self.mixer, &mut branches, obsolete_index);
                    }
                }
                let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
                let active = active
                    .as_mut()
                    .filter(|active| matches_plan(active))
                    .ok_or(PipelineError::StalePlan)?;
                active.next = None;
                active.handover_at = None;
                active.handed_over = false;
                Ok(())
            })();
            return commit_result;
        };

        let candidate = branch::attach_paused(&self.pipeline, &self.mixer, self.events.clone(), &next.track, plan.generation, 0.0)?;
        self.branches.lock().unwrap_or_else(|error| error.into_inner()).push(candidate);
        let current_duration = branch::wait_duration(&self.branches, current_index).await;
        let next_duration = branch::wait_duration(&self.branches, candidate_index).await;

        // No pipeline pause: the candidate stays locked until its pad offset
        // is applied, so its first buffer cannot cross into the mixer before
        // the transition math schedules the start. The current track's
        // dataflow never stops.
        let commit_result = (|| {
            let (timeline_origin, current_elapsed) = {
                let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
                let active = active
                    .as_ref()
                    .filter(|active| matches_plan(active))
                    .ok_or(PipelineError::StalePlan)?;
                (
                    Duration::from_nanos(active.timeline_origin.nseconds()),
                    Duration::from_nanos(active.last_elapsed.nseconds()),
                )
            };
            let handover_at = {
                let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
                let current = branches.get(current_index).ok_or(PipelineError::StalePlan)?;
                let candidate = branches.get(candidate_index).ok_or(PipelineError::StalePlan)?;
                let transition = resolve_transition(
                    next.transition,
                    current_duration,
                    next_duration,
                    branch::seekable(current),
                    branch::seekable(candidate),
                );
                let handover_at =
                    transition::apply_rolling(transition, current, candidate, current_duration, timeline_origin, current_elapsed)?.handover;
                // Unlock the candidate and sync it to the running pipeline:
                // the first decoded buffer now crosses the mixer pad with the
                // offset already applied and is held until the handover.
                branch::prepare(branches.get_mut(candidate_index).ok_or(PipelineError::StalePlan)?)?;
                handover_at
            };
            {
                let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
                if let Some(obsolete_index) = obsolete_index {
                    branch::remove_at(&self.pipeline, &self.mixer, &mut branches, obsolete_index);
                }
                if let Some(candidate) = branches.last_mut() {
                    branch::release_paused(candidate);
                }
            }
            {
                let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
                let active = active
                    .as_mut()
                    .filter(|active| matches_plan(active))
                    .ok_or(PipelineError::StalePlan)?;
                active.next = Some(next.track.key);
                active.handover_at = handover_at;
                active.handed_over = false;
            }
            Ok(())
        })();
        if commit_result.is_err() {
            let mut branches = self.branches.lock().unwrap_or_else(|error| error.into_inner());
            branch::remove_at(&self.pipeline, &self.mixer, &mut branches, candidate_index);
        }
        commit_result
    }

    async fn set_playing(&self, playing: bool) -> Result<(), PipelineError> {
        self.set_state(if playing { PipelineState::Playing } else { PipelineState::Paused })
    }

    async fn reconnect(&self, target: IcecastTarget) -> Result<(), PipelineError> {
        let previous_state = self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).state;
        if previous_state == PipelineState::Stopped {
            let sink = self.sink.lock().unwrap_or_else(|error| error.into_inner());
            if let SinkSlot::Active(sink) = &*sink {
                if self.sink_factory == sink::DEFAULT_FACTORY {
                    sink::configure(sink, &target);
                }
            }
            return Ok(());
        }

        let expected_state = match previous_state {
            PipelineState::Playing => gst::State::Playing,
            PipelineState::Paused => gst::State::Paused,
            PipelineState::Stopped => unreachable!("stopped reconnect returned above"),
        };
        // A paused pipeline has no active output to preserve. Cycling it through Ready
        // forces a newly inserted sink to complete its later preroll before resume.
        if previous_state == PipelineState::Paused {
            // Reassert Paused before the Ready cycle so every newly inserted child
            // participates in the same state transition when playback resumes.
            self.set_state(PipelineState::Paused)?;
            self.pipeline
                .set_state(gst::State::Ready)
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        }

        let old_sink = match &*self.sink.lock().unwrap_or_else(|error| error.into_inner()) {
            SinkSlot::Active(sink) => sink.clone(),
            SinkSlot::Replacing { .. } => return Err(PipelineError::StalePlan),
        };
        let candidate = sink::build(self.sink_factory, &target)?;
        self.pipeline
            .add(&candidate)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = SinkSlot::Replacing {
            _old_sink: old_sink.clone(),
            _candidate: candidate.clone(),
        };

        self.clock_gate.unlink(&old_sink);
        if let Err(error) = self.clock_gate.link(&candidate) {
            let _ = candidate.set_state(gst::State::Null);
            let _ = self.pipeline.remove(&candidate);
            let _ = self.clock_gate.link(&old_sink);
            *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = SinkSlot::Active(old_sink);
            return Err(PipelineError::Pipeline(error.to_string()));
        }
        self.pipeline
            .set_state(expected_state)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        let (result, current, pending) = self.pipeline.state(gst::ClockTime::from_seconds(5));
        result.map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        if current != expected_state {
            self.clock_gate.unlink(&candidate);
            let _ = candidate.set_state(gst::State::Null);
            let _ = self.pipeline.remove(&candidate);
            if self.clock_gate.link(&old_sink).is_err() {
                let _ = self.set_state(PipelineState::Stopped);
                *self.active.lock().unwrap_or_else(|poison| poison.into_inner()) = None;
                return Err(PipelineError::Pipeline("sink replacement and rollback failed".into()));
            }
            *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = SinkSlot::Active(old_sink);
            return Err(PipelineError::Pipeline(format!(
                "reconnect transition to {expected_state:?} stalled at {current:?} with {pending:?} pending"
            )));
        }
        old_sink
            .set_state(gst::State::Null)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        self.pipeline
            .remove(&old_sink)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = SinkSlot::Active(candidate);
        Ok(())
    }

    async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|error| error.into_inner());
        if snapshot.state != PipelineState::Stopped {
            if let Some(active) = self.active.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
                snapshot.elapsed = Duration::from_nanos(active.last_elapsed.saturating_sub(active.current_epoch).nseconds());
            }
        }
        Ok(snapshot.clone())
    }

    async fn stop(&self) -> Result<(), PipelineError> {
        *self.replacing.lock().unwrap_or_else(|error| error.into_inner()) = None;
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
            output_queue,
            output_caps,
            encoder,
            sink,
            clock_gate,
        } = graph::build_backbone(&config, self.sink_factory)?;
        let (events, receiver) = mpsc::unbounded_channel();
        let active: Arc<Mutex<Option<ActivePlan>>> = Arc::new(Mutex::new(None));
        let replacing: Arc<Mutex<Option<ReplaceCancellation>>> = Arc::new(Mutex::new(None));
        bus::install(&pipeline, &clock_gate, active.clone(), replacing.clone(), events.clone()).expect("bus installed");
        Ok(PipelineInstance {
            pipeline: Arc::new(GStreamerPipeline {
                pipeline,
                mixer,
                output_queue,
                output_caps,
                encoder,
                sink: Mutex::new(SinkSlot::Active(sink)),
                clock_gate,
                sink_factory: self.sink_factory,
                branches: Mutex::new(Vec::new()),
                active,
                replacing,
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
    use crate::streamer::pipeline::{IcecastTarget, OutputConfig, PipelineTrack, PlannedNext, TransitionPlan};

    fn config() -> PipelineConfig {
        PipelineConfig {
            target: IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            output: OutputConfig {
                prebuffer_bytes: 1024,
                sample_rate: 44_100,
                channels: 2,
                bitrate_kbps: 128,
            },
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

    #[test]
    fn old_terminal_cancels_matching_staged_replacement() {
        let current = TrackKey {
            queue_item_id: uuid::Uuid::new_v4(),
            song_id: uuid::Uuid::new_v4(),
        };
        let replacement = ReplaceCancellation {
            expected_generation: 7,
            expected_current: current.clone(),
            cancelled: Arc::new(AtomicBool::new(false)),
        };

        assert!(!replacement.cancel_if_matches(8, &current));
        assert!(replacement.cancel_if_matches(7, &current));
        assert!(replacement.is_cancelled());
    }

    fn track(path: &std::path::Path, _position: i32) -> PipelineTrack {
        PipelineTrack {
            key: TrackKey {
                queue_item_id: uuid::Uuid::new_v4(),
                song_id: uuid::Uuid::new_v4(),
            },
            path: path.to_path_buf(),
            cue_in: Duration::ZERO,
            cue_out: Duration::ZERO,
            cross_start_next: Duration::ZERO,
            analyzed: false,
        }
    }

    fn initial_plan(generation: u64, current: PipelineTrack, next: Option<PipelineTrack>, transition: TransitionPlan) -> PairPlan {
        PairPlan {
            mode: ReplaceMode::InitialReplaceFromStopped,
            generation,
            output_epoch: 1,
            current,
            next: next.map(|track| PlannedNext { track, transition }),
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
    async fn reconnect_preserves_paused_and_playing_state() {
        let target = config().target;
        let file = tempfile::NamedTempFile::new().unwrap();
        write_wav(file.path(), Duration::from_secs(5), 0);
        let instance = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        instance
            .pipeline
            .replace(initial_plan(1, track(file.path(), 0), None, TransitionPlan::Cut))
            .await
            .unwrap();
        instance.pipeline.set_playing(false).await.unwrap();
        instance.pipeline.reconnect(target.clone()).await.unwrap();
        assert_eq!(instance.pipeline.snapshot().await.unwrap().state, PipelineState::Paused);
        instance.pipeline.set_playing(true).await.unwrap();
        assert_eq!(instance.pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        instance.pipeline.stop().await.unwrap();

        let instance = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        instance
            .pipeline
            .replace(initial_plan(1, track(file.path(), 0), None, TransitionPlan::Cut))
            .await
            .unwrap();
        instance.pipeline.reconnect(target).await.unwrap();
        assert_eq!(instance.pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        instance.pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn decodes_a_wav_branch_without_a_second_playback_backend() {
        use crate::streamer::pipeline::{PipelineTrack, TrackKey};
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
        };
        pipeline
            .replace(initial_plan(
                1,
                PipelineTrack {
                    key: key.clone(),
                    path: file.path().to_path_buf(),
                    cue_in: Duration::ZERO,
                    cue_out: Duration::ZERO,
                    cross_start_next: Duration::ZERO,
                    analyzed: false,
                },
                None,
                TransitionPlan::Cut,
            ))
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
            .replace(initial_plan(
                7,
                current,
                Some(next),
                TransitionPlan::NaiveCrossfade {
                    requested_fade: Duration::from_millis(400),
                },
            ))
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
        let fourth_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(third_file.path(), Duration::from_secs(1), 4_000);
        write_wav(fourth_file.path(), Duration::from_secs(1), -4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let third = track(third_file.path(), 2);
        let fourth = track(fourth_file.path(), 3);
        let third_key = third.key.clone();
        let fourth_key = fourth.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let plan = |generation, mode, current, next| PairPlan {
            mode,
            generation,
            output_epoch: 1,
            current,
            next: Some(PlannedNext {
                track: next,
                transition: TransitionPlan::NaiveCrossfade {
                    requested_fade: Duration::from_millis(400),
                },
            }),
        };

        pipeline
            .replace(plan(1, ReplaceMode::InitialReplaceFromStopped, first, second.clone()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, .. })) {}
        })
        .await
        .unwrap();

        let started = std::time::Instant::now();
        pipeline
            .replace(plan(
                2,
                ReplaceMode::ActiveReplace {
                    expected_generation: 1,
                    expected_current: second.key.clone(),
                },
                second,
                third,
            ))
            .await
            .unwrap();
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
        assert!(pipeline.snapshot().await.unwrap().elapsed < Duration::from_millis(250));
        assert!(started.elapsed() >= Duration::from_millis(500));
        let rolling_started = std::time::Instant::now();
        pipeline
            .roll(RollingPlan {
                generation: 2,
                current: third_key,
                change: RollingChange::Attach(PlannedNext {
                    track: fourth,
                    transition: TransitionPlan::NaiveCrossfade {
                        requested_fade: Duration::from_millis(400),
                    },
                }),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(
                events.recv().await,
                Some(PipelineEvent::Handover {
                    generation: 2,
                    ref current,
                }) if current == &fourth_key
            ) {}
        })
        .await
        .unwrap();
        assert!(rolling_started.elapsed() >= Duration::from_millis(500));
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
            .replace(initial_plan(
                8,
                current,
                Some(next),
                TransitionPlan::AutoCueCrossfade {
                    current_start: Duration::from_millis(200),
                    fade_start: Duration::from_millis(800),
                    current_end: Duration::from_millis(1_200),
                    next_start: Duration::from_millis(100),
                    duration: Duration::from_millis(400),
                    fallback_fade: Duration::from_millis(300),
                },
            ))
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
    async fn rolling_attach_promotes_handover_and_schedules_the_following_track() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let third_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(third_file.path(), Duration::from_secs(1), 4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let third = track(third_file.path(), 2);
        let second_key = second.key.clone();
        let third_key = third.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        pipeline
            .replace(initial_plan(
                1,
                first,
                Some(second),
                TransitionPlan::NaiveCrossfade {
                    requested_fade: Duration::from_millis(400),
                },
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, ref current }) if current == &second_key) {}
        })
        .await
        .unwrap();
        let second_started = std::time::Instant::now();

        pipeline
            .roll(RollingPlan {
                generation: 1,
                current: second_key,
                change: RollingChange::Attach(PlannedNext {
                    track: third,
                    transition: TransitionPlan::NaiveCrossfade {
                        requested_fade: Duration::from_millis(400),
                    },
                }),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, ref current }) if current == &third_key) {}
        })
        .await
        .unwrap();
        let second_elapsed = second_started.elapsed();
        assert!(
            second_elapsed >= Duration::from_millis(500),
            "rolling handover fired after {second_elapsed:?}, before the promoted track reached its fade window"
        );
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn rolling_attach_accepts_a_single_current_branch() {
        let current_file = tempfile::NamedTempFile::new().unwrap();
        let next_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(current_file.path(), Duration::from_secs(1), 8_000);
        write_wav(next_file.path(), Duration::from_secs(1), -8_000);
        let current = track(current_file.path(), 0);
        let current_key = current.key.clone();
        let next = track(next_file.path(), 1);
        let next_key = next.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        pipeline.replace(initial_plan(1, current, None, TransitionPlan::Cut)).await.unwrap();
        pipeline
            .roll(RollingPlan {
                generation: 1,
                current: current_key,
                change: RollingChange::Attach(PlannedNext {
                    track: next,
                    transition: TransitionPlan::NaiveCrossfade {
                        requested_fade: Duration::from_millis(400),
                    },
                }),
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, ref current }) if current == &next_key) {}
        })
        .await
        .unwrap();
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn rolling_replace_next_rejects_a_stale_terminal_key() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let replacement_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(replacement_file.path(), Duration::from_secs(1), 4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let replacement = track(replacement_file.path(), 2);
        let first_key = first.key.clone();
        let PipelineInstance { pipeline, .. } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        pipeline
            .replace(initial_plan(1, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        let result = pipeline
            .roll(RollingPlan {
                generation: 1,
                current: first_key,
                change: RollingChange::ReplaceNext {
                    expected_next: TrackKey {
                        queue_item_id: uuid::Uuid::new_v4(),
                        song_id: uuid::Uuid::new_v4(),
                    },
                    replacement: Some(PlannedNext {
                        track: replacement,
                        transition: TransitionPlan::Cut,
                    }),
                },
            })
            .await;
        assert!(matches!(result, Err(PipelineError::StalePlan)));
        pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn replacing_a_branch_releases_its_mixer_request_pad() {
        use crate::streamer::pipeline::{PipelineTrack, TrackKey};
        use tempfile::NamedTempFile;
        use uuid::Uuid;

        let file = NamedTempFile::new().unwrap();
        let wav = b"RIFF$\0\0\0WAVEfmt \x10\0\0\0\x01\0\x02\0\x44\xAC\0\0\x10\xB1\x02\0\x04\0\x10\0data\0\0\0\0";
        std::fs::write(file.path(), wav).unwrap();
        let instance = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();
        let plan = |generation, mode| PairPlan {
            mode,
            generation,
            output_epoch: 1,
            current: PipelineTrack {
                key: TrackKey {
                    queue_item_id: Uuid::new_v4(),
                    song_id: Uuid::new_v4(),
                },
                path: file.path().to_path_buf(),
                cue_in: Duration::ZERO,
                cue_out: Duration::ZERO,
                cross_start_next: Duration::ZERO,
                analyzed: false,
            },
            next: None,
        };
        let first = plan(1, ReplaceMode::InitialReplaceFromStopped);
        let first_key = first.current.key.clone();
        instance.pipeline.replace(first).await.unwrap();
        instance
            .pipeline
            .replace(plan(
                2,
                ReplaceMode::ActiveReplace {
                    expected_generation: 1,
                    expected_current: first_key,
                },
            ))
            .await
            .unwrap();
        instance.pipeline.stop().await.unwrap();
    }

    #[tokio::test]
    async fn rolling_replace_next_swaps_only_the_terminal_branch() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let replacement_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(replacement_file.path(), Duration::from_secs(1), 4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let replacement = track(replacement_file.path(), 2);
        let first_key = first.key.clone();
        let second_key = second.key.clone();
        let replacement_key = replacement.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        pipeline
            .replace(initial_plan(1, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        pipeline
            .roll(RollingPlan {
                generation: 1,
                current: first_key,
                change: RollingChange::ReplaceNext {
                    expected_next: second_key,
                    replacement: Some(PlannedNext {
                        track: replacement,
                        transition: TransitionPlan::Cut,
                    }),
                },
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 1, ref current }) if current == &replacement_key) {}
        })
        .await
        .unwrap();
        pipeline.stop().await.unwrap();
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

    #[tokio::test]
    async fn roll_replace_next_mid_stream_does_not_interrupt_the_output() {
        use std::sync::Arc;
        use std::sync::Mutex as StdMutex;
        use std::time::Instant;

        let current_file = tempfile::NamedTempFile::new().unwrap();
        let next_file = tempfile::NamedTempFile::new().unwrap();
        let replacement_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(current_file.path(), Duration::from_secs(1), 8_000);
        write_wav(next_file.path(), Duration::from_secs(1), -8_000);
        write_wav(replacement_file.path(), Duration::from_secs(1), 4_000);
        let current = track(current_file.path(), 0);
        let next = track(next_file.path(), 1);
        let replacement = track(replacement_file.path(), 2);
        let current_key = current.key.clone();
        let next_key = next.key.clone();

        // Build the concrete pipeline so the test can probe the clock gate pad.
        let graph::Backbone {
            pipeline,
            mixer,
            output_queue,
            output_caps,
            encoder,
            sink,
            clock_gate,
        } = graph::build_backbone(&config(), "fakesink").expect("backbone built");
        let (events, receiver) = mpsc::unbounded_channel();
        let active: Arc<Mutex<Option<ActivePlan>>> = Arc::new(Mutex::new(None));
        let replacing: Arc<Mutex<Option<ReplaceCancellation>>> = Arc::new(Mutex::new(None));
        bus::install(&pipeline, &clock_gate, active.clone(), replacing.clone(), events.clone()).expect("bus installed");
        let pipeline = GStreamerPipeline {
            pipeline,
            mixer,
            output_queue,
            output_caps,
            encoder,
            sink: Mutex::new(SinkSlot::Active(sink)),
            clock_gate,
            sink_factory: "fakesink",
            branches: Mutex::new(Vec::new()),
            active,
            replacing,
            snapshot: Mutex::new(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            }),
            events,
        };
        let _events = receiver;
        // Probe the clock-gated output (post-encoder, steady ~26ms frame
        // cadence): a mid-track stall longer than the ~64ms output prebuffer
        // would show up here as a gap well over one frame period.
        let clock_gate = pipeline.pipeline.by_name("clock_gate").expect("clock gate present");
        let gate_src = clock_gate.static_pad("src").expect("clock gate src pad");
        let arrivals: Arc<StdMutex<Vec<Instant>>> = Arc::new(StdMutex::new(Vec::new()));
        {
            let arrivals = arrivals.clone();
            gate_src
                .add_probe(gst::PadProbeType::BUFFER, move |_, info| {
                    if info.buffer().is_some() {
                        arrivals.lock().unwrap_or_else(|error| error.into_inner()).push(Instant::now());
                    }
                    gst::PadProbeReturn::Ok
                })
                .expect("probe installed");
        }

        // Seconds 0-1: tone A. 300ms in we replace the staged next (B) with
        // the moved track X, exactly like a reorder during playback. The swap
        // pauses the pipeline, so the output prebuffer queue must keep the MP3
        // frames flowing: a gap of more than one frame period (100ms bound,
        // one ~26ms frame nominal) would be audible mid-track.
        pipeline
            .replace(initial_plan(17, current, Some(next), TransitionPlan::Cut))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let roll_started = std::time::Instant::now();
        pipeline
            .roll(RollingPlan {
                generation: 17,
                current: current_key,
                change: RollingChange::ReplaceNext {
                    expected_next: next_key,
                    replacement: Some(PlannedNext {
                        track: replacement,
                        transition: TransitionPlan::Cut,
                    }),
                },
            })
            .await
            .unwrap();
        let roll_elapsed = roll_started.elapsed();
        // A ends at 1s; X plays after. Cover both plus margin.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        pipeline.stop().await.unwrap();

        let arrivals = arrivals.lock().unwrap_or_else(|error| error.into_inner());
        let max_gap = arrivals
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]))
            .max()
            .unwrap_or_default();
        tracing::info!(?roll_elapsed, ?max_gap, samples = arrivals.len(), "mid-stream replace next output gaps");
        // The output runs at a steady ~26ms frame cadence; the swap must not
        // stall it beyond the ~64ms prebuffer absorption (two frame periods of
        // headroom for jitter). Any future regression that pauses the pipeline
        // mid-track (or decodes the replacement slowly) trips this.
        assert!(
            max_gap < Duration::from_millis(50),
            "audible output gap {max_gap:?} during a mid-stream replace (roll took {roll_elapsed:?})"
        );
    }

    #[tokio::test]
    async fn roll_replace_next_mid_rotation_keeps_the_promoted_track_playing() {
        let first_file = tempfile::NamedTempFile::new().unwrap();
        let second_file = tempfile::NamedTempFile::new().unwrap();
        let third_file = tempfile::NamedTempFile::new().unwrap();
        let replacement_file = tempfile::NamedTempFile::new().unwrap();
        write_wav(first_file.path(), Duration::from_secs(1), 8_000);
        write_wav(second_file.path(), Duration::from_secs(1), -8_000);
        write_wav(third_file.path(), Duration::from_secs(1), 4_000);
        write_wav(replacement_file.path(), Duration::from_secs(1), -4_000);
        let first = track(first_file.path(), 0);
        let second = track(second_file.path(), 1);
        let third = track(third_file.path(), 2);
        let replacement = track(replacement_file.path(), 3);
        let second_key = second.key.clone();
        let third_key = third.key.clone();
        let replacement_key = replacement.key.clone();
        let PipelineInstance { pipeline, mut events } = GStreamerPipelineFactory::with_test_sink().create(config()).await.unwrap();

        // A plays, B staged. After the handover B is the current track and the
        // Attach roll prunes A, so B plays from the first mixer slot.
        pipeline
            .replace(initial_plan(23, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(PipelineEvent::Handover { generation: 23, ref current }) if current == &second_key) {}
        })
        .await
        .unwrap();
        pipeline
            .roll(RollingPlan {
                generation: 23,
                current: second_key.clone(),
                change: RollingChange::Attach(PlannedNext {
                    track: third,
                    transition: TransitionPlan::Cut,
                }),
            })
            .await
            .unwrap();

        // Reorder mid-B: move X to the head of the upcoming queue while the
        // promoted second track is the one playing.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let replaced = std::time::Instant::now();
        pipeline
            .roll(RollingPlan {
                generation: 23,
                current: second_key.clone(),
                change: RollingChange::ReplaceNext {
                    expected_next: third_key,
                    replacement: Some(PlannedNext {
                        track: replacement,
                        transition: TransitionPlan::Cut,
                    }),
                },
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(
                events.recv().await,
                Some(PipelineEvent::Handover { generation: 23, ref current }) if current == &replacement_key
            ) {}
        })
        .await
        .unwrap();
        // B is a 1s track; the swap happened 200ms into it. The replacement
        // must start at B's end, not immediately.
        assert!(
            replaced.elapsed() >= Duration::from_millis(500),
            "replacement handover fired too early: {replaced:?}"
        );
        pipeline.stop().await.unwrap();
    }
}
