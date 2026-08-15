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
    TrackMetadata, TransitionPlan,
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
    current_metadata: TrackMetadata,
    next: Option<(TrackKey, TrackMetadata)>,
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

struct OutputReconnectGuard(Arc<AtomicBool>);

impl Drop for OutputReconnectGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
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
    metadata_target: Arc<Mutex<IcecastTarget>>,
    metadata_publisher: Option<sink::MetadataPublisher>,
    clock_gate: gst::Element,
    sink_factory: &'static str,
    branches: Mutex<Vec<Branch>>,
    active: Arc<Mutex<Option<ActivePlan>>>,
    replacing: Arc<Mutex<Option<ReplaceCancellation>>>,
    output_reconnecting: Arc<AtomicBool>,
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
    fn publish_metadata(&self, metadata: &TrackMetadata) {
        let Some(publisher) = &self.metadata_publisher else {
            return;
        };
        let target = self.metadata_target.lock().unwrap_or_else(|error| error.into_inner()).clone();
        publisher.publish(target, metadata.clone());
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
            .map(|next| (next.track.key.clone(), next.track.metadata.clone()));
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
            current_metadata: plan.current.metadata.clone(),
            next: scheduled_next,
            handover_at,
            handed_over: false,
            timeline_origin: gst::ClockTime::from_nseconds(timeline_base.as_nanos().min(u64::MAX as u128) as u64),
            started_at: None,
            current_epoch: gst::ClockTime::ZERO,
            last_elapsed: gst::ClockTime::ZERO,
        });
        self.publish_metadata(&plan.current.metadata);
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
    fn rollback_sink_replacement(
        &self,
        old_sink: &gst::Element,
        candidate: &gst::Element,
        previous_state: PipelineState,
    ) -> Result<(), PipelineError> {
        self.clock_gate.unlink(candidate);
        let mut failures = Vec::new();
        if let Err(error) = candidate.set_state(gst::State::Null) {
            failures.push(format!("could not stop candidate sink: {error}"));
        }
        if let Err(error) = self.pipeline.remove(candidate) {
            failures.push(format!("could not remove candidate sink: {error}"));
        }
        if let Err(error) = self.clock_gate.link(old_sink) {
            failures.push(format!("could not relink previous sink: {error}"));
        }
        if failures.is_empty() {
            if let Err(error) = self.restore_state(previous_state) {
                failures.push(format!("could not restore pipeline state: {error}"));
            }
        }
        if failures.is_empty() {
            *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = SinkSlot::Active(old_sink.clone());
            Ok(())
        } else {
            self.force_stopped();
            *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = SinkSlot::Active(old_sink.clone());
            Err(PipelineError::Pipeline(format!(
                "sink replacement rollback failed: {}",
                failures.join("; ")
            )))
        }
    }
    fn suppress_output_events(&self) -> OutputReconnectGuard {
        self.output_reconnecting.store(true, Ordering::Release);
        OutputReconnectGuard(self.output_reconnecting.clone())
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
            scheduled_next = Some((next.track.key.clone(), next.track.metadata.clone()));
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
        let current_metadata = plan.current.metadata.clone();
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = Some(ActivePlan {
            generation: plan.generation,
            output_epoch: plan.output_epoch,
            current: plan.current.key,
            current_metadata: current_metadata.clone(),
            next: scheduled_next,
            handover_at,
            handed_over: false,
            timeline_origin: gst::ClockTime::ZERO,
            started_at: None,
            current_epoch: gst::ClockTime::ZERO,
            last_elapsed: gst::ClockTime::ZERO,
        });
        self.publish_metadata(&current_metadata);
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
                && !expected_next
                    .as_ref()
                    .is_some_and(|key| active.next.as_ref().map(|(next, _)| next) != Some(key))
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
                active.next = Some((next.track.key, next.track.metadata));
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
        let _reconnect_guard = self.suppress_output_events();
        let previous_state = self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).state;
        if previous_state == PipelineState::Stopped {
            let sink = self.sink.lock().unwrap_or_else(|error| error.into_inner());
            if let SinkSlot::Active(sink) = &*sink {
                if self.sink_factory == sink::DEFAULT_FACTORY {
                    sink::configure(sink, &target);
                }
            }
            *self.metadata_target.lock().unwrap_or_else(|error| error.into_inner()) = target;
            return Ok(());
        }

        let expected_state = match previous_state {
            PipelineState::Playing => gst::State::Playing,
            PipelineState::Paused => gst::State::Paused,
            PipelineState::Stopped => unreachable!("stopped reconnect returned above"),
        };
        // Settings changes quiesce the stream first. `shout2send` applies its
        // endpoint properties only from Null, so fully reset the graph before
        // changing the active sink and then restore Paused for the caller to
        // resume.
        if previous_state == PipelineState::Paused {
            self.pipeline
                .set_state(gst::State::Null)
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            let (result, current, pending) = self.pipeline.state(gst::ClockTime::from_seconds(5));
            result.map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            if current != gst::State::Null {
                return Err(PipelineError::Pipeline(format!(
                    "output reset stalled at {current:?} with {pending:?} pending"
                )));
            }
            {
                let sink = self.sink.lock().unwrap_or_else(|error| error.into_inner());
                let SinkSlot::Active(sink) = &*sink else {
                    return Err(PipelineError::StalePlan);
                };
                if self.sink_factory == sink::DEFAULT_FACTORY {
                    sink::configure(sink, &target);
                }
            }
            *self.metadata_target.lock().unwrap_or_else(|error| error.into_inner()) = target;
            let metadata = self
                .active
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .map(|active| active.current_metadata.clone());
            if let Some(metadata) = metadata {
                self.publish_metadata(&metadata);
            }
            return self.restore_state(PipelineState::Paused);
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

        let replacement = (|| -> Result<(), PipelineError> {
            self.clock_gate.unlink(&old_sink);
            self.clock_gate
                .link(&candidate)
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            candidate
                .sync_state_with_parent()
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            self.pipeline
                .set_state(expected_state)
                .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            let (result, current, pending) = self.pipeline.state(gst::ClockTime::from_seconds(5));
            result.map_err(|error| PipelineError::Pipeline(error.to_string()))?;
            if current != expected_state {
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
            Ok(())
        })();

        if let Err(error) = replacement {
            self.rollback_sink_replacement(&old_sink, &candidate, previous_state)?;
            return Err(error);
        }

        *self.sink.lock().unwrap_or_else(|error| error.into_inner()) = SinkSlot::Active(candidate);
        *self.metadata_target.lock().unwrap_or_else(|error| error.into_inner()) = target;
        let metadata = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(|active| active.current_metadata.clone());
        if let Some(metadata) = metadata {
            self.publish_metadata(&metadata);
        }
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
        if let Some(publisher) = &self.metadata_publisher {
            publisher.clear();
        }
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
        let output_reconnecting = Arc::new(AtomicBool::new(false));
        let metadata_target = Arc::new(Mutex::new(config.target.clone()));
        let metadata_publisher = (self.sink_factory == sink::DEFAULT_FACTORY).then(sink::MetadataPublisher::spawn);
        let sink_slot = Mutex::new(SinkSlot::Active(sink));
        bus::install(
            &pipeline,
            &clock_gate,
            metadata_target.clone(),
            metadata_publisher.clone(),
            active.clone(),
            replacing.clone(),
            output_reconnecting.clone(),
            events.clone(),
        )
        .expect("bus installed");
        Ok(PipelineInstance {
            pipeline: Arc::new(GStreamerPipeline {
                pipeline,
                mixer,
                output_queue,
                output_caps,
                encoder,
                sink: sink_slot,
                metadata_target,
                metadata_publisher,
                clock_gate,
                sink_factory: self.sink_factory,
                branches: Mutex::new(Vec::new()),
                active,
                replacing,
                output_reconnecting,
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
    use crate::streamer::pipeline::{IcecastTarget, PipelineTrack, PlannedNext, TransitionPlan};
    use crate::streamer::testsupport::{self, track, write_wav, HttpStub};
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    /// Owns a real (test-sink) pipeline instance plus its event stream and
    /// keeps the temporary WAV files alive for the lifetime of the test.
    struct GstHarness {
        pipeline: Arc<dyn PlaybackPipeline>,
        events: mpsc::UnboundedReceiver<PipelineEvent>,
        files: Vec<tempfile::NamedTempFile>,
    }

    impl GstHarness {
        async fn new() -> Self {
            let instance = GStreamerPipelineFactory::with_test_sink()
                .create(testsupport::pipeline_config())
                .await
                .unwrap();
            Self {
                pipeline: instance.pipeline,
                events: instance.events,
                files: Vec::new(),
            }
        }

        /// Writes a short tone WAV and keeps the temp file alive.
        fn wav(&mut self, duration: Duration, sample: i16) -> PathBuf {
            let file = tempfile::NamedTempFile::new().unwrap();
            write_wav(file.path(), duration, sample);
            let path = file.path().to_path_buf();
            self.files.push(file);
            path
        }

        /// The next pipeline event within the standard bounded timeout.
        async fn next_event(&mut self) -> PipelineEvent {
            tokio::time::timeout(Duration::from_secs(3), self.events.recv())
                .await
                .expect("pipeline event timeout")
                .expect("event channel closed")
        }

        /// Waits (bounded) for an event matching `predicate`.
        async fn wait_for_event(&mut self, predicate: impl Fn(&PipelineEvent) -> bool) -> PipelineEvent {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(event) = self.events.recv().await {
                        if predicate(&event) {
                            return event;
                        }
                    } else {
                        panic!("event channel closed");
                    }
                }
            })
            .await
            .expect("pipeline event timeout")
        }

        async fn stop(&self) {
            self.pipeline.stop().await.unwrap();
        }
    }

    /// An initial replace from a stopped pipeline (output epoch 1).
    fn initial_plan(generation: u64, current: PipelineTrack, next: Option<PipelineTrack>, transition: TransitionPlan) -> PairPlan {
        PairPlan {
            mode: ReplaceMode::InitialReplaceFromStopped,
            generation,
            output_epoch: 1,
            current,
            next: next.map(|track| PlannedNext { track, transition }),
        }
    }

    fn metadata_song(request: &str) -> String {
        // The raw request ends with headers, not with the query string, so
        // parse the request line and compare the *decoded* song parameter
        // (robust against `+` vs `%20` and header ordering).
        let request_line = request.split("\r\n").next().expect("request line present");
        let path = request_line.split_whitespace().nth(1).expect("request target present");
        let url = reqwest::Url::parse(&format!("http://localhost{path}")).expect("metadata request URL parses");
        url.query_pairs()
            .find(|(key, _)| key == "song")
            .map(|(_, value)| value.into_owned())
            .expect("metadata request carries a song parameter")
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

    #[tokio::test]
    async fn creates_clocked_mp3_backbone_with_test_sink() {
        let harness = GstHarness::new().await;
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Stopped);
        harness.pipeline.set_playing(false).await.unwrap();
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Paused);
        harness.stop().await;
    }

    #[tokio::test]
    async fn reconnect_preserves_paused_and_playing_state() {
        let target = testsupport::target();
        let mut harness = GstHarness::new().await;
        let file = harness.wav(Duration::from_secs(5), 0);
        harness
            .pipeline
            .replace(initial_plan(1, track(&file, 0), None, TransitionPlan::Cut))
            .await
            .unwrap();
        harness.pipeline.set_playing(false).await.unwrap();
        harness.pipeline.reconnect(target.clone()).await.unwrap();
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Paused);
        harness.pipeline.set_playing(true).await.unwrap();
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        harness.stop().await;

        let mut harness = GstHarness::new().await;
        let file = harness.wav(Duration::from_secs(5), 0);
        harness
            .pipeline
            .replace(initial_plan(1, track(&file, 0), None, TransitionPlan::Cut))
            .await
            .unwrap();
        harness.pipeline.reconnect(target).await.unwrap();
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        harness.stop().await;
    }

    #[tokio::test]
    async fn decodes_a_wav_branch_without_a_second_playback_backend() {
        let mut harness = GstHarness::new().await;
        let file = harness.wav(Duration::from_millis(200), 0);
        let key = testsupport::track_key();
        harness
            .pipeline
            .replace(initial_plan(
                1,
                PipelineTrack {
                    key: key.clone(),
                    metadata: crate::streamer::pipeline::TrackMetadata {
                        title: "Test track".into(),
                        artist: "Test artist".into(),
                    },
                    path: file,
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
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        assert!(matches!(
            harness.next_event().await,
            PipelineEvent::CurrentEos { generation: 1, current } if current == key
        ));
        harness.stop().await;
    }

    #[tokio::test]
    async fn schedules_next_branch_and_handover_on_the_clocked_fade_midpoint() {
        let mut harness = GstHarness::new().await;
        let current_file = harness.wav(Duration::from_secs(1), 8_000);
        let next_file = harness.wav(Duration::from_secs(1), -8_000);
        let current = track(&current_file, 0);
        let next = track(&next_file, 1);
        let next_key = next.key.clone();
        let started = std::time::Instant::now();

        harness
            .pipeline
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

        let event = harness.next_event().await;
        assert!(
            matches!(
                event,
                PipelineEvent::Handover { generation: 7, ref current }
                    if current == &next_key
            ),
            "{event:?}"
        );
        assert!(started.elapsed() >= Duration::from_millis(500));
        harness.stop().await;
    }

    #[tokio::test]
    async fn schedules_each_replacement_on_its_own_running_time() {
        let mut harness = GstHarness::new().await;
        let first_file = harness.wav(Duration::from_secs(1), 8_000);
        let second_file = harness.wav(Duration::from_secs(1), -8_000);
        let third_file = harness.wav(Duration::from_secs(1), 4_000);
        let fourth_file = harness.wav(Duration::from_secs(1), -4_000);
        let first = track(&first_file, 0);
        let second = track(&second_file, 1);
        let third = track(&third_file, 2);
        let fourth = track(&fourth_file, 3);
        let third_key = third.key.clone();
        let fourth_key = fourth.key.clone();
        let pipeline = harness.pipeline.clone();
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
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 1, .. }))
            .await;

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
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 2, ref current } if current == &third_key))
            .await;
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
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 2, ref current } if current == &fourth_key))
            .await;
        assert!(rolling_started.elapsed() >= Duration::from_millis(500));
        harness.stop().await;
    }

    #[tokio::test]
    async fn applies_autocue_seeks_before_the_clocked_handover() {
        let mut harness = GstHarness::new().await;
        let current_file = harness.wav(Duration::from_millis(1_500), 8_000);
        let next_file = harness.wav(Duration::from_millis(1_500), -8_000);
        let current = track(&current_file, 0);
        let next = track(&next_file, 1);
        let next_key = next.key.clone();
        let started = std::time::Instant::now();

        harness
            .pipeline
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

        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::Handover { generation: 8, ref current } if current == &next_key),
            "{event:?}"
        );
        assert!(started.elapsed() >= Duration::from_millis(500));
        harness.stop().await;
    }

    #[tokio::test]
    async fn rolling_attach_promotes_handover_and_schedules_the_following_track() {
        let mut harness = GstHarness::new().await;
        let first_file = harness.wav(Duration::from_secs(1), 8_000);
        let second_file = harness.wav(Duration::from_secs(1), -8_000);
        let third_file = harness.wav(Duration::from_secs(1), 4_000);
        let first = track(&first_file, 0);
        let second = track(&second_file, 1);
        let third = track(&third_file, 2);
        let second_key = second.key.clone();
        let third_key = third.key.clone();

        harness
            .pipeline
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
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 1, ref current } if current == &second_key))
            .await;
        let second_started = std::time::Instant::now();

        harness
            .pipeline
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
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 1, ref current } if current == &third_key))
            .await;
        let second_elapsed = second_started.elapsed();
        assert!(
            second_elapsed >= Duration::from_millis(500),
            "rolling handover fired after {second_elapsed:?}, before the promoted track reached its fade window"
        );
        harness.stop().await;
    }

    #[tokio::test]
    async fn rolling_attach_accepts_a_single_current_branch() {
        let mut harness = GstHarness::new().await;
        let current_file = harness.wav(Duration::from_secs(1), 8_000);
        let next_file = harness.wav(Duration::from_secs(1), -8_000);
        let current = track(&current_file, 0);
        let current_key = current.key.clone();
        let next = track(&next_file, 1);
        let next_key = next.key.clone();

        harness
            .pipeline
            .replace(initial_plan(1, current, None, TransitionPlan::Cut))
            .await
            .unwrap();
        harness
            .pipeline
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

        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 1, ref current } if current == &next_key))
            .await;
        harness.stop().await;
    }

    #[tokio::test]
    async fn rolling_replace_next_rejects_a_stale_terminal_key() {
        let mut harness = GstHarness::new().await;
        let first_file = harness.wav(Duration::from_secs(1), 8_000);
        let second_file = harness.wav(Duration::from_secs(1), -8_000);
        let replacement_file = harness.wav(Duration::from_secs(1), 4_000);
        let first = track(&first_file, 0);
        let second = track(&second_file, 1);
        let replacement = track(&replacement_file, 2);
        let first_key = first.key.clone();

        harness
            .pipeline
            .replace(initial_plan(1, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        let result = harness
            .pipeline
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
        harness.stop().await;
    }

    #[tokio::test]
    async fn replacing_a_branch_releases_its_mixer_request_pad() {
        let mut harness = GstHarness::new().await;
        let file = harness.wav(Duration::from_millis(100), 0);
        let instance_pipeline = harness.pipeline.clone();
        let plan = |generation, mode| PairPlan {
            mode,
            generation,
            output_epoch: 1,
            current: PipelineTrack {
                key: testsupport::track_key(),
                metadata: crate::streamer::pipeline::TrackMetadata {
                    title: "Test track".into(),
                    artist: "Test artist".into(),
                },
                path: file.clone(),
                cue_in: Duration::ZERO,
                cue_out: Duration::ZERO,
                cross_start_next: Duration::ZERO,
                analyzed: false,
            },
            next: None,
        };
        let first = plan(1, ReplaceMode::InitialReplaceFromStopped);
        let first_key = first.current.key.clone();
        instance_pipeline.replace(first).await.unwrap();
        instance_pipeline
            .replace(plan(
                2,
                ReplaceMode::ActiveReplace {
                    expected_generation: 1,
                    expected_current: first_key,
                },
            ))
            .await
            .unwrap();
        harness.stop().await;
    }

    #[tokio::test]
    async fn rolling_replace_next_swaps_only_the_terminal_branch() {
        let mut harness = GstHarness::new().await;
        let first_file = harness.wav(Duration::from_secs(1), 8_000);
        let second_file = harness.wav(Duration::from_secs(1), -8_000);
        let replacement_file = harness.wav(Duration::from_secs(1), 4_000);
        let first = track(&first_file, 0);
        let second = track(&second_file, 1);
        let replacement = track(&replacement_file, 2);
        let first_key = first.key.clone();
        let second_key = second.key.clone();
        let replacement_key = replacement.key.clone();

        harness
            .pipeline
            .replace(initial_plan(1, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        harness
            .pipeline
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
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 1, ref current } if current == &replacement_key))
            .await;
        harness.stop().await;
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

        let mut harness = GstHarness::new().await;
        let current_file = harness.wav(Duration::from_secs(1), 8_000);
        let next_file = harness.wav(Duration::from_secs(1), -8_000);
        let replacement_file = harness.wav(Duration::from_secs(1), 4_000);
        let current = track(&current_file, 0);
        let next = track(&next_file, 1);
        let replacement = track(&replacement_file, 2);
        let current_key = current.key.clone();
        let next_key = next.key.clone();
        let replacement_key = replacement.key.clone();
        let mut stub = HttpStub::spawn(&[("200 OK", Duration::ZERO), ("200 OK", Duration::from_millis(300))]).await;
        let metadata_port = stub.port;

        // Build the concrete pipeline so the test can probe the clock gate pad.
        let graph::Backbone {
            pipeline,
            mixer,
            output_queue,
            output_caps,
            encoder,
            sink,
            clock_gate,
        } = graph::build_backbone(&testsupport::pipeline_config(), "fakesink").expect("backbone built");
        let (events, receiver) = mpsc::unbounded_channel();
        let active: Arc<Mutex<Option<ActivePlan>>> = Arc::new(Mutex::new(None));
        let replacing: Arc<Mutex<Option<ReplaceCancellation>>> = Arc::new(Mutex::new(None));
        let output_reconnecting = Arc::new(AtomicBool::new(false));
        let metadata_target = Arc::new(Mutex::new(
            IcecastTarget::parse(&format!("127.0.0.1:{metadata_port}"), "secret".into(), "test", "test".into()).unwrap(),
        ));
        let metadata_publisher = Some(sink::MetadataPublisher::spawn());
        let sink_slot = Mutex::new(SinkSlot::Active(sink));
        bus::install(
            &pipeline,
            &clock_gate,
            metadata_target.clone(),
            metadata_publisher.clone(),
            active.clone(),
            replacing.clone(),
            output_reconnecting.clone(),
            events.clone(),
        )
        .expect("bus installed");
        let pipeline = GStreamerPipeline {
            pipeline,
            mixer,
            output_queue,
            output_caps,
            encoder,
            sink: sink_slot,
            metadata_target,
            metadata_publisher,
            clock_gate,
            sink_factory: "fakesink",
            branches: Mutex::new(Vec::new()),
            active,
            replacing,
            output_reconnecting,
            snapshot: Mutex::new(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            }),
            events,
        };
        let mut pipeline_events = receiver;
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
        tokio::time::timeout(Duration::from_secs(2), async {
            while !matches!(
                pipeline_events.recv().await,
                Some(PipelineEvent::Handover { ref current, .. }) if current == &replacement_key
            ) {}
        })
        .await
        .expect("replacement handover");
        tokio::time::timeout(Duration::from_secs(1), stub.join())
            .await
            .expect("metadata requests");
        let metadata_requests = stub.requests();
        assert_eq!(
            metadata_song(&metadata_requests[0]),
            "Artist 0 - Track 0",
            "unexpected metadata requests: {metadata_requests:#?}"
        );
        assert_eq!(
            metadata_song(&metadata_requests[1]),
            "Artist 2 - Track 2",
            "unexpected metadata requests: {metadata_requests:#?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        pipeline.stop().await.unwrap();

        let arrivals = arrivals.lock().unwrap_or_else(|error| error.into_inner());
        let max_gap = arrivals
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]))
            .max()
            .unwrap_or_default();
        tracing::info!(
            ?roll_elapsed,
            ?max_gap,
            samples = arrivals.len(),
            "mid-stream replace next output gaps"
        );
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
        let mut harness = GstHarness::new().await;
        let first_file = harness.wav(Duration::from_secs(1), 8_000);
        let second_file = harness.wav(Duration::from_secs(1), -8_000);
        let third_file = harness.wav(Duration::from_secs(1), 4_000);
        let replacement_file = harness.wav(Duration::from_secs(1), -4_000);
        let first = track(&first_file, 0);
        let second = track(&second_file, 1);
        let third = track(&third_file, 2);
        let replacement = track(&replacement_file, 3);
        let second_key = second.key.clone();
        let third_key = third.key.clone();
        let replacement_key = replacement.key.clone();

        // A plays, B staged. After the handover B is the current track and the
        // Attach roll prunes A, so B plays from the first mixer slot.
        harness
            .pipeline
            .replace(initial_plan(23, first, Some(second), TransitionPlan::Cut))
            .await
            .unwrap();
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 23, ref current } if current == &second_key))
            .await;
        harness
            .pipeline
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
        harness
            .pipeline
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
        harness
            .wait_for_event(|event| matches!(event, PipelineEvent::Handover { generation: 23, ref current } if current == &replacement_key))
            .await;
        // B is a 1s track; the swap happened 200ms into it. The replacement
        // must start at B's end, not immediately.
        assert!(
            replaced.elapsed() >= Duration::from_millis(500),
            "replacement handover fired too early: {replaced:?}"
        );
        harness.stop().await;
    }
}
