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
    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
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

struct PendingEpochCleanup(Arc<Mutex<Option<u64>>>);

impl Drop for PendingEpochCleanup {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = None;
    }
}

struct OutputReconnectGuard(Arc<Mutex<SinkState>>);

impl Drop for OutputReconnectGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|error| error.into_inner()).reconnecting = false;
    }
}

#[derive(Debug)]
pub(super) enum SinkSlot {
    Active(gst::Element),
    Replacing { old_sink: gst::Element, candidate: gst::Element },
}

#[derive(Debug)]
pub(super) struct SinkState {
    pub(super) slot: SinkSlot,
    pub(super) reconnecting: bool,
}

impl SinkState {
    pub(super) fn new(sink: gst::Element) -> Self {
        Self {
            slot: SinkSlot::Active(sink),
            reconnecting: false,
        }
    }
}

pub(crate) struct GStreamerPipeline {
    pipeline: gst::Pipeline,
    mixer: gst::Element,
    output_queue: gst::Element,
    output_caps: gst::Element,
    encoder: gst::Element,
    sink: Arc<Mutex<SinkState>>,
    metadata_target: Arc<Mutex<IcecastTarget>>,
    metadata_publisher: Option<sink::MetadataPublisher>,
    clock_gate: gst::Element,
    sink_factory: &'static str,
    registry: Arc<Mutex<branch::BranchRegistry>>,
    active: Arc<Mutex<Option<ActivePlan>>>,
    replacing: Arc<Mutex<Option<ReplaceCancellation>>>,
    pending_epoch: Arc<Mutex<Option<u64>>>,
    snapshot: Mutex<PipelineSnapshot>,
    events: mpsc::UnboundedSender<PipelineEvent>,
    #[cfg(test)]
    pre_commit_hook: Arc<Mutex<Option<PreCommitHook>>>,
    #[cfg(test)]
    post_commit_hook: Arc<Mutex<Option<PostCommitHook>>>,
    #[cfg(test)]
    teardown_hook: Arc<Mutex<Option<TeardownHook>>>,
}

struct RetiringGuard {
    registry: Arc<Mutex<branch::BranchRegistry>>,
    retirement_id: u64,
}

impl Drop for RetiringGuard {
    fn drop(&mut self) {
        let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
        reg.retiring.retain(|entry| entry.retirement_id != self.retirement_id);
    }
}
#[cfg(test)]
type PreCommitHook = Box<dyn FnMut() -> Result<(), PipelineError> + Send>;
#[cfg(test)]
type PostCommitHook = Box<dyn FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;
#[cfg(test)]
type TeardownHook = Box<dyn FnMut(&Branch) + Send>;
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
            // Diagnostic for stalled transitions: log bus errors and fakesink state
            // without spamming StateChanged. The bus sync handler already routes
            // DecodeFailed/SinkDisconnected, but a stalled transition without
            // an Error message indicates preroll blocking (e.g. live sink async).
            if let Some(bus) = self.pipeline.bus() {
                while let Some(msg) = bus.pop() {
                    if let gst::MessageView::Error(err) = msg.view() {
                        tracing::error!(
                            "GStreamer bus error during set_state to {:?}: {} (debug: {:?}) from {:?}",
                            target,
                            err.error(),
                            err.debug(),
                            msg.src().map(|s| s.path_string().to_string())
                        );
                    }
                }
            }
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
    fn retire_all_live(&self) -> (RetiringGuard, Vec<Branch>) {
        let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
        let branches = std::mem::take(&mut reg.live);
        let retirement_id = reg.alloc_retirement_id();
        for b in &branches {
            reg.retiring.push(branch::RetiringBranch {
                retirement_id,
                elements: b.elements.clone(),
            });
        }
        (
            RetiringGuard {
                registry: self.registry.clone(),
                retirement_id,
            },
            branches,
        )
    }

    fn retire_live_by_key(&self, key: &TrackKey) -> (RetiringGuard, Option<Branch>) {
        let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
        let branch = reg.live.iter().position(|b| b.key == *key).map(|pos| reg.live.remove(pos));
        let retirement_id = reg.alloc_retirement_id();
        if let Some(b) = &branch {
            reg.retiring.push(branch::RetiringBranch {
                retirement_id,
                elements: b.elements.clone(),
            });
        }
        (
            RetiringGuard {
                registry: self.registry.clone(),
                retirement_id,
            },
            branch,
        )
    }

    fn retire_unattached_candidates(&self, candidates: Vec<Branch>) -> (RetiringGuard, Vec<Branch>) {
        let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
        let retirement_id = reg.alloc_retirement_id();
        for b in &candidates {
            reg.unregister_preparing(&b.key);
            reg.retiring.push(branch::RetiringBranch {
                retirement_id,
                elements: b.elements.clone(),
            });
        }
        (
            RetiringGuard {
                registry: self.registry.clone(),
                retirement_id,
            },
            candidates,
        )
    }

    fn discard_retired(&self, branches: Vec<Branch>, _guard: RetiringGuard) {
        for branch in branches {
            #[cfg(test)]
            if let Some(hook) = &mut *self.teardown_hook.lock().unwrap_or_else(|error| error.into_inner()) {
                hook(&branch);
            }
            branch::discard(&self.pipeline, &self.mixer, branch);
        }
    }
    #[cfg(test)]
    fn set_teardown_hook(&self, hook: impl FnMut(&Branch) + Send + 'static) {
        *self.teardown_hook.lock().unwrap_or_else(|error| error.into_inner()) = Some(Box::new(hook));
    }
    fn force_stopped(&self) {
        let (guard, obsolete) = self.retire_all_live();
        {
            let reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            debug_assert!(reg.preparing.is_empty(), "preparing must be empty when force_stopped is called");
            #[cfg(test)]
            assert!(reg.preparing.is_empty(), "preparing must be empty when force_stopped is called");
        }
        let _ = self.pipeline.set_state(gst::State::Null);
        self.discard_retired(obsolete, guard);
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = None;
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|error| error.into_inner());
        snapshot.state = PipelineState::Stopped;
        snapshot.elapsed = Duration::ZERO;
    }
    fn rollback_initial_replace(&self, candidates: Vec<Branch>) {
        if !candidates.is_empty() {
            let (guard, candidates) = self.retire_unattached_candidates(candidates);
            self.discard_retired(candidates, guard);
        }
        self.force_stopped();
    }
    async fn replace_active(&self, plan: &PairPlan, cancellation: &ReplaceCancellation) -> Result<(), PipelineError> {
        let previous_state = self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).state;
        let mut candidates = vec![branch::attach_paused(
            &self.pipeline,
            &self.mixer,
            Some(&self.registry),
            self.events.clone(),
            &plan.current,
            plan.generation,
            1.0,
        )?];
        if let Some(next) = plan.next.as_ref() {
            match branch::attach_paused(
                &self.pipeline,
                &self.mixer,
                Some(&self.registry),
                self.events.clone(),
                &next.track,
                plan.generation,
                0.0,
            ) {
                Ok(candidate) => candidates.push(candidate),
                Err(error) => {
                    let (guard, candidates) = self.retire_unattached_candidates(candidates);
                    self.discard_retired(candidates, guard);
                    return Err(error);
                }
            }
        }
        if let Err(error) = self.set_state(PipelineState::Paused) {
            let (guard, candidates) = self.retire_unattached_candidates(candidates);
            self.discard_retired(candidates, guard);
            return Err(error);
        }
        let is_failed = |candidates: &[Branch]| {
            let reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            candidates.iter().any(|c| reg.is_preparing_failed(&c.key))
        };
        if cancellation.is_cancelled() || is_failed(&candidates) {
            let (guard, candidates) = self.retire_unattached_candidates(candidates);
            self.discard_retired(candidates, guard);
            self.restore_state(previous_state)?;
            return Err(PipelineError::StalePlan);
        }
        let timeline_base = {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let Some(active) = active.as_ref() else {
                let (guard, candidates) = self.retire_unattached_candidates(candidates);
                self.discard_retired(candidates, guard);
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
        if current_duration.is_none() && candidates.len() > 1 {
            let excess = candidates.split_off(1);
            let (guard, excess) = self.retire_unattached_candidates(excess);
            self.discard_retired(excess, guard);
        }
        for candidate in &mut candidates {
            if let Err(error) = branch::prepare(candidate) {
                let (guard, candidates) = self.retire_unattached_candidates(candidates);
                self.discard_retired(candidates, guard);
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
                let (guard, candidates) = self.retire_unattached_candidates(candidates);
                self.discard_retired(candidates, guard);
                self.restore_state(previous_state)?;
                return Err(error);
            }
        };
        if cancellation.is_cancelled() || is_failed(&candidates) {
            let (guard, candidates) = self.retire_unattached_candidates(candidates);
            self.discard_retired(candidates, guard);
            self.restore_state(previous_state)?;
            return Err(PipelineError::StalePlan);
        }

        #[cfg(test)]
        if let Some(mut hook) = self.pre_commit_hook.lock().unwrap_or_else(|error| error.into_inner()).take() {
            if let Err(error) = hook() {
                let (guard, candidates) = self.retire_unattached_candidates(candidates);
                self.discard_retired(candidates, guard);
                self.restore_state(previous_state)?;
                return Err(error);
            }
        }
        if cancellation.is_cancelled() || is_failed(&candidates) {
            let (guard, candidates) = self.retire_unattached_candidates(candidates);
            self.discard_retired(candidates, guard);
            self.restore_state(previous_state)?;
            return Err(PipelineError::StalePlan);
        }

        let (guard, obsolete, release_actions) = {
            let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let matches = match (&plan.mode, active.as_ref()) {
                (
                    ReplaceMode::ActiveReplace {
                        expected_generation,
                        expected_current,
                    },
                    Some(a),
                ) => a.generation == *expected_generation && a.current == *expected_current && a.output_epoch == plan.output_epoch,
                _ => false,
            };
            let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            let any_failed = candidates.iter().any(|c| reg.is_preparing_failed(&c.key));
            if !matches || cancellation.is_cancelled() || any_failed {
                drop(reg);
                drop(active);
                let (guard, candidates) = self.retire_unattached_candidates(candidates);
                self.discard_retired(candidates, guard);
                self.restore_state(previous_state)?;
                return Err(PipelineError::StalePlan);
            }
            for candidate in &candidates {
                reg.unregister_preparing(&candidate.key);
            }
            let release_actions: Vec<_> = candidates.iter_mut().filter_map(branch::take_paused_release).collect();
            let branches = std::mem::replace(&mut reg.live, candidates);

            let retirement_id = reg.alloc_retirement_id();
            for b in &branches {
                reg.retiring.push(branch::RetiringBranch {
                    retirement_id,
                    elements: b.elements.clone(),
                });
            }

            *active = Some(ActivePlan {
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

            let guard = RetiringGuard {
                registry: self.registry.clone(),
                retirement_id,
            };
            (guard, branches, release_actions)
        };

        for action in release_actions {
            branch::apply_paused_release(action);
        }

        self.discard_retired(obsolete, guard);
        self.publish_metadata(&plan.current.metadata);
        if let Err(error) = self.restore_state(previous_state) {
            self.force_stopped();
            return Err(error);
        }
        #[cfg(test)]
        let hook = self.post_commit_hook.lock().unwrap_or_else(|error| error.into_inner()).take();
        #[cfg(test)]
        if let Some(mut hook) = hook {
            hook().await;
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
            self.sink.lock().unwrap_or_else(|error| error.into_inner()).slot = SinkSlot::Active(old_sink.clone());
            Ok(())
        } else {
            self.force_stopped();
            self.sink.lock().unwrap_or_else(|error| error.into_inner()).slot = SinkSlot::Active(old_sink.clone());
            Err(PipelineError::Pipeline(format!(
                "sink replacement rollback failed: {}",
                failures.join("; ")
            )))
        }
    }
    fn suppress_output_events(&self) -> OutputReconnectGuard {
        self.sink.lock().unwrap_or_else(|error| error.into_inner()).reconnecting = true;
        OutputReconnectGuard(self.sink.clone())
    }

    #[cfg(test)]
    pub(super) fn gst_pipeline(&self) -> &gst::Pipeline {
        &self.pipeline
    }

    #[cfg(test)]
    pub(super) fn current_sink_element(&self) -> gst::Element {
        match &self.sink.lock().unwrap_or_else(|error| error.into_inner()).slot {
            SinkSlot::Active(sink) => sink.clone(),
            SinkSlot::Replacing { candidate, .. } => candidate.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn mixer_element(&self) -> gst::Element {
        self.mixer.clone()
    }

    #[cfg(test)]
    pub(super) fn encoder_element(&self) -> gst::Element {
        self.encoder.clone()
    }

    #[cfg(test)]
    pub(super) fn set_pre_commit_hook(&self, hook: impl FnMut() -> Result<(), PipelineError> + Send + 'static) {
        *self.pre_commit_hook.lock().unwrap_or_else(|error| error.into_inner()) = Some(Box::new(hook));
    }
    #[cfg(test)]
    pub(super) fn set_post_commit_hook<F, Fut>(&self, mut hook: F)
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        *self.post_commit_hook.lock().unwrap_or_else(|error| error.into_inner()) = Some(Box::new(move || Box::pin(hook())));
    }
    async fn replace_initial(&self, plan: PairPlan, cancellation: &ReplaceCancellation) -> Result<(), PipelineError> {
        let (guard, obsolete) = self.retire_all_live();
        self.discard_retired(obsolete, guard);
        let mut candidates = Vec::with_capacity(2);
        let is_failed = |candidates: &[Branch]| {
            let reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            candidates.iter().any(|c| reg.is_preparing_failed(&c.key))
        };

        let current = match branch::attach(
            &self.pipeline,
            &self.mixer,
            Some(&self.registry),
            self.events.clone(),
            &plan.current,
            plan.generation,
            1.0,
        ) {
            Ok(branch) => branch,
            Err(error) => {
                self.rollback_initial_replace(candidates);
                return Err(error);
            }
        };
        candidates.push(current);

        if cancellation.is_cancelled() || is_failed(&candidates) {
            self.rollback_initial_replace(candidates);
            return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
        }

        if let Err(error) = self.set_state(PipelineState::Paused) {
            self.rollback_initial_replace(candidates);
            return Err(error);
        }

        let current_duration = branch::wait_branch_duration(&candidates[0]).await;

        if cancellation.is_cancelled() || is_failed(&candidates) {
            self.rollback_initial_replace(candidates);
            return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
        }

        let mut scheduled_next = None;
        let mut transition_plan = TransitionPlan::Cut;
        if let (Some(next), Some(_)) = (plan.next.as_ref(), current_duration) {
            let next_branch = match branch::attach_paused(
                &self.pipeline,
                &self.mixer,
                Some(&self.registry),
                self.events.clone(),
                &next.track,
                plan.generation,
                0.0,
            ) {
                Ok(branch) => branch,
                Err(error) => {
                    self.rollback_initial_replace(candidates);
                    return Err(error);
                }
            };
            if let Err(error) = next_branch.source.state(gst::ClockTime::from_seconds(5)).0 {
                candidates.push(next_branch);
                self.rollback_initial_replace(candidates);
                return Err(PipelineError::Pipeline(error.to_string()));
            }
            candidates.push(next_branch);

            if cancellation.is_cancelled() || is_failed(&candidates) {
                self.rollback_initial_replace(candidates);
                return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
            }

            scheduled_next = Some((next.track.key.clone(), next.track.metadata.clone()));
            transition_plan = next.transition;
        }

        let next_duration = if scheduled_next.is_some() {
            branch::wait_branch_duration(&candidates[1]).await
        } else {
            None
        };

        if cancellation.is_cancelled() || is_failed(&candidates) {
            self.rollback_initial_replace(candidates);
            return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
        }

        let current_seekable = branch::seekable(&candidates[0]);
        let next_seekable = candidates.get(1).is_some_and(branch::seekable);
        let transition = resolve_transition(transition_plan, current_duration, next_duration, current_seekable, next_seekable);

        let handover_at = match transition::apply_initial(transition, &candidates, current_duration) {
            Ok(applied) => applied.handover,
            Err(error) => {
                self.rollback_initial_replace(candidates);
                return Err(error);
            }
        };

        if candidates.len() > 1 {
            if let Err(error) = branch::prepare(&mut candidates[1]) {
                self.rollback_initial_replace(candidates);
                return Err(error);
            }
        }

        if cancellation.is_cancelled() || is_failed(&candidates) {
            self.rollback_initial_replace(candidates);
            return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
        }

        #[cfg(test)]
        if let Some(mut hook) = self.pre_commit_hook.lock().unwrap_or_else(|error| error.into_inner()).take() {
            if let Err(error) = hook() {
                self.rollback_initial_replace(candidates);
                return Err(error);
            }
            if cancellation.is_cancelled() || is_failed(&candidates) {
                self.rollback_initial_replace(candidates);
                return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
            }
        }

        if let Err(error) = self.set_state(PipelineState::Playing) {
            self.rollback_initial_replace(candidates);
            return Err(error);
        }

        if cancellation.is_cancelled() || is_failed(&candidates) {
            self.rollback_initial_replace(candidates);
            return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
        }

        let release_actions: Vec<_>;
        {
            let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());

            if cancellation.is_cancelled() || candidates.iter().any(|c| reg.is_preparing_failed(&c.key)) {
                drop(reg);
                drop(active);
                self.rollback_initial_replace(candidates);
                return Err(PipelineError::Pipeline("initial replace cancelled / decode failed".into()));
            }

            for c in &candidates {
                reg.unregister_preparing(&c.key);
            }
            release_actions = candidates.iter_mut().filter_map(branch::take_paused_release).collect();
            reg.live = candidates;
            *active = Some(ActivePlan {
                generation: plan.generation,
                output_epoch: plan.output_epoch,
                current: plan.current.key,
                current_metadata: plan.current.metadata.clone(),
                next: scheduled_next,
                handover_at,
                handed_over: false,
                timeline_origin: gst::ClockTime::ZERO,
                started_at: None,
                current_epoch: gst::ClockTime::ZERO,
                last_elapsed: gst::ClockTime::ZERO,
            });
        }

        for action in release_actions {
            branch::apply_paused_release(action);
        }

        self.publish_metadata(&plan.current.metadata);
        #[cfg(test)]
        let hook = self.post_commit_hook.lock().unwrap_or_else(|error| error.into_inner()).take();
        #[cfg(test)]
        if let Some(mut hook) = hook {
            hook().await;
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
                ) if active.generation == *expected_generation
                    && active.current == *expected_current
                    && active.output_epoch == plan.output_epoch => {}
                _ => return Err(PipelineError::StalePlan),
            }
        }
        *self.pending_epoch.lock().unwrap_or_else(|error| error.into_inner()) = Some(plan.output_epoch);
        let _pending_cleanup = PendingEpochCleanup(self.pending_epoch.clone());
        let (expected_generation, expected_current) = match &plan.mode {
            ReplaceMode::ActiveReplace {
                expected_generation,
                expected_current,
            } => (*expected_generation, expected_current.clone()),
            ReplaceMode::InitialReplaceFromStopped => (plan.generation, plan.current.key.clone()),
        };
        let cancellation = ReplaceCancellation {
            expected_generation,
            expected_current,
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        *self.replacing.lock().unwrap_or_else(|error| error.into_inner()) = Some(cancellation.clone());
        let _cleanup = ReplacementCleanup(self.replacing.clone());

        if matches!(plan.mode, ReplaceMode::ActiveReplace { .. }) {
            return self.replace_active(&plan, &cancellation).await;
        }

        self.replace_initial(plan, &cancellation).await
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
        let (current_index, active_epoch) = {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let active_ref = active.as_ref().filter(|a| matches_plan(a)).ok_or(PipelineError::StalePlan)?;
            let active_epoch = active_ref.output_epoch;
            let reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            let current_index = reg
                .live
                .iter()
                .position(|branch| branch.key == plan.current)
                .ok_or(PipelineError::StalePlan)?;
            if let Some(expected) = expected_next.as_ref() {
                if !reg.live.iter().any(|b| b.key == *expected) {
                    return Err(PipelineError::StalePlan);
                }
            }
            (current_index, active_epoch)
        };
        *self.pending_epoch.lock().unwrap_or_else(|error| error.into_inner()) = Some(active_epoch);
        let _pending_cleanup = PendingEpochCleanup(self.pending_epoch.clone());

        let Some(next) = replacement else {
            // Exhausted queue: drop the staged next without pausing the
            // pipeline. Removing a playing branch is the same teardown every
            // natural handover performs.
            let (guard, obsolete) = {
                let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
                let active = active
                    .as_mut()
                    .filter(|active| matches_plan(active))
                    .ok_or(PipelineError::StalePlan)?;
                let (guard, obsolete) = if let Some(expected) = expected_next.as_ref() {
                    self.retire_live_by_key(expected)
                } else {
                    let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
                    let obsolete = reg.live.iter().position(|b| b.key != plan.current).map(|pos| reg.live.remove(pos));
                    let retirement_id = reg.alloc_retirement_id();
                    if let Some(obs) = obsolete.as_ref() {
                        reg.retiring.push(branch::RetiringBranch {
                            retirement_id,
                            elements: obs.elements.clone(),
                        });
                    }
                    (
                        RetiringGuard {
                            registry: self.registry.clone(),
                            retirement_id,
                        },
                        obsolete,
                    )
                };
                active.next = None;
                active.handover_at = None;
                active.handed_over = false;
                (guard, obsolete)
            };
            if let Some(branch) = obsolete {
                self.discard_retired(vec![branch], guard);
            }
            return Ok(());
        };

        let mut candidate = branch::attach_paused(
            &self.pipeline,
            &self.mixer,
            Some(&self.registry),
            self.events.clone(),
            &next.track,
            plan.generation,
            0.0,
        )?;
        let candidate_key = candidate.key.clone();

        let rollback = |candidate: Branch| {
            let (guard, candidate) = self.retire_unattached_candidates(vec![candidate]);
            self.discard_retired(candidate, guard);
        };

        let current_duration = branch::wait_duration(&self.registry, current_index).await;
        let next_duration = branch::wait_branch_duration(&candidate).await;

        let (timeline_origin, current_elapsed) = {
            let active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let Some(active) = active.as_ref().filter(|a| matches_plan(a)) else {
                rollback(candidate);
                return Err(PipelineError::StalePlan);
            };
            (
                Duration::from_nanos(active.timeline_origin.nseconds()),
                Duration::from_nanos(active.last_elapsed.nseconds()),
            )
        };

        let handover_at = {
            let reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            let Some(current) = reg.live.get(current_index) else {
                drop(reg);
                rollback(candidate);
                return Err(PipelineError::StalePlan);
            };
            let transition = resolve_transition(
                next.transition,
                current_duration,
                next_duration,
                branch::seekable(current),
                branch::seekable(&candidate),
            );
            match transition::apply_rolling(transition, current, &candidate, current_duration, timeline_origin, current_elapsed) {
                Ok(schedule) => schedule.handover,
                Err(error) => {
                    drop(reg);
                    rollback(candidate);
                    return Err(error);
                }
            }
        };

        if let Err(error) = branch::prepare(&mut candidate) {
            rollback(candidate);
            return Err(error);
        }

        #[cfg(test)]
        if let Some(mut hook) = self.pre_commit_hook.lock().unwrap_or_else(|error| error.into_inner()).take() {
            if let Err(error) = hook() {
                rollback(candidate);
                return Err(error);
            }
        }

        let (guard, obsolete, release_action) = {
            let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
            let Some(active) = active.as_mut().filter(|a| matches_plan(a)) else {
                drop(active);
                rollback(candidate);
                return Err(PipelineError::StalePlan);
            };

            let mut reg = self.registry.lock().unwrap_or_else(|error| error.into_inner());
            if reg.is_preparing_failed(&candidate_key) {
                drop(reg);
                rollback(candidate);
                return Err(PipelineError::Pipeline("preparing candidate decode failed".into()));
            }

            reg.unregister_preparing(&candidate_key);
            let release_action = branch::take_paused_release(&mut candidate);
            let obsolete_branch = if let Some(expected) = expected_next.as_ref() {
                reg.live.iter().position(|b| b.key == *expected).map(|pos| reg.live.remove(pos))
            } else {
                reg.live.iter().position(|b| b.key != plan.current).map(|pos| reg.live.remove(pos))
            };

            let retirement_id = reg.alloc_retirement_id();
            if let Some(obs) = obsolete_branch.as_ref() {
                reg.retiring.push(branch::RetiringBranch {
                    retirement_id,
                    elements: obs.elements.clone(),
                });
            }

            reg.live.push(candidate);
            active.next = Some((next.track.key, next.track.metadata));
            active.handover_at = handover_at;
            active.handed_over = false;

            let guard = RetiringGuard {
                registry: self.registry.clone(),
                retirement_id,
            };
            (guard, obsolete_branch, release_action)
        };

        if let Some(action) = release_action {
            branch::apply_paused_release(action);
        }

        if let Some(branch) = obsolete {
            self.discard_retired(vec![branch], guard);
        }

        Ok(())
    }

    async fn set_playing(&self, playing: bool) -> Result<(), PipelineError> {
        self.set_state(if playing { PipelineState::Playing } else { PipelineState::Paused })
    }

    async fn reconnect(&self, target: IcecastTarget) -> Result<(), PipelineError> {
        let _reconnect_guard = self.suppress_output_events();
        let previous_state = self.snapshot.lock().unwrap_or_else(|error| error.into_inner()).state;
        if previous_state == PipelineState::Stopped {
            let state = self.sink.lock().unwrap_or_else(|error| error.into_inner());
            if let SinkSlot::Active(sink) = &state.slot {
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
                let state = self.sink.lock().unwrap_or_else(|error| error.into_inner());
                let SinkSlot::Active(sink) = &state.slot else {
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

        let old_sink = match &self.sink.lock().unwrap_or_else(|error| error.into_inner()).slot {
            SinkSlot::Active(sink) => sink.clone(),
            SinkSlot::Replacing { .. } => return Err(PipelineError::StalePlan),
        };
        let candidate = sink::build(self.sink_factory, &target)?;
        self.pipeline
            .add(&candidate)
            .map_err(|error| PipelineError::Pipeline(error.to_string()))?;
        self.sink.lock().unwrap_or_else(|error| error.into_inner()).slot = SinkSlot::Replacing {
            old_sink: old_sink.clone(),
            candidate: candidate.clone(),
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

        self.sink.lock().unwrap_or_else(|error| error.into_inner()).slot = SinkSlot::Active(candidate);
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
        let (guard, obsolete) = self.retire_all_live();
        self.discard_retired(obsolete, guard);
        *self.active.lock().unwrap_or_else(|error| error.into_inner()) = None;
        if let Some(publisher) = &self.metadata_publisher {
            publisher.clear();
        }
        self.set_state(PipelineState::Stopped)
    }
}

impl GStreamerPipelineFactory {
    pub(super) async fn create_pipeline(
        &self,
        config: PipelineConfig,
    ) -> Result<(Arc<GStreamerPipeline>, mpsc::UnboundedReceiver<PipelineEvent>), PipelineError> {
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
        let pending_epoch: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
        let metadata_target = Arc::new(Mutex::new(config.target.clone()));
        let metadata_publisher = (self.sink_factory == sink::DEFAULT_FACTORY).then(sink::MetadataPublisher::spawn);
        let sink_state = Arc::new(Mutex::new(SinkState::new(sink)));
        let registry = Arc::new(Mutex::new(branch::BranchRegistry::new()));
        bus::install(
            &pipeline,
            &clock_gate,
            sink_state.clone(),
            registry.clone(),
            metadata_target.clone(),
            metadata_publisher.clone(),
            active.clone(),
            replacing.clone(),
            pending_epoch.clone(),
            events.clone(),
        )
        .expect("bus installed");
        let pipeline = Arc::new(GStreamerPipeline {
            pipeline,
            mixer,
            output_queue,
            output_caps,
            encoder,
            sink: sink_state,
            metadata_target,
            metadata_publisher,
            clock_gate,
            sink_factory: self.sink_factory,
            registry,
            active,
            replacing,
            pending_epoch,
            snapshot: Mutex::new(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            }),
            events,
            #[cfg(test)]
            pre_commit_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            post_commit_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            teardown_hook: Arc::new(Mutex::new(None)),
        });
        Ok((pipeline, receiver))
    }
}

#[async_trait]
impl PlaybackPipelineFactory for GStreamerPipelineFactory {
    async fn create(&self, config: PipelineConfig) -> Result<PipelineInstance, PipelineError> {
        let (pipeline, events) = self.create_pipeline(config).await?;
        Ok(PipelineInstance { pipeline, events })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamer::controller::StationController;
    use crate::streamer::pipeline::{IcecastTarget, PipelineTrack, PlannedNext, TransitionPlan};
    use crate::streamer::runtime::StationRuntime;
    use crate::streamer::testsupport::{self, track, write_wav, HttpStub};
    use std::path::PathBuf;
    use tokio::sync::mpsc;
    struct GstHarness {
        pipeline: Arc<GStreamerPipeline>,
        events: mpsc::UnboundedReceiver<PipelineEvent>,
        files: Vec<tempfile::NamedTempFile>,
    }

    impl GstHarness {
        async fn new() -> Self {
            let (pipeline, events) = GStreamerPipelineFactory::with_test_sink()
                .create_pipeline(testsupport::pipeline_config())
                .await
                .unwrap();
            Self {
                pipeline,
                events,
                files: Vec::new(),
            }
        }

        async fn start_playing(generation: u64) -> Self {
            let mut harness = Self::new().await;
            let track = harness.track(Duration::from_secs(2), 8_000, 0);
            let plan = initial_plan(generation, track, None, TransitionPlan::Cut);
            harness.pipeline.replace(plan).await.unwrap();
            harness
        }
        fn wav(&mut self, duration: Duration, sample: i16) -> PathBuf {
            let file = tempfile::NamedTempFile::new().unwrap();
            write_wav(file.path(), duration, sample);
            let path = file.path().to_path_buf();
            self.files.push(file);
            path
        }

        /// Writes a tone WAV, keeps it alive, and builds the matching track.
        fn track(&mut self, duration: Duration, sample: i16, position: i32) -> PipelineTrack {
            let file = self.wav(duration, sample);
            track(&file, position)
        }

        /// The next pipeline event within the standard bounded timeout.
        async fn next_event(&mut self) -> PipelineEvent {
            tokio::time::timeout(Duration::from_secs(5), self.events.recv())
                .await
                .expect("pipeline event timeout")
                .expect("event channel closed")
        }

        /// Waits (bounded) for an event matching `predicate`.
        /// Uses 10s timeout because multi-track clocked handovers play real audio for up to 6s.
        async fn wait_for_event(&mut self, predicate: impl Fn(&PipelineEvent) -> bool) -> PipelineEvent {
            tokio::time::timeout(Duration::from_secs(10), async {
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

        fn post_error(&self, src: &impl IsA<gst::Object>, message: &str) {
            let msg = gst::message::Error::builder(gst::StreamError::Failed, message).src(src).build();
            self.pipeline
                .gst_pipeline()
                .bus()
                .expect("pipeline has bus")
                .post(msg)
                .expect("message posted to bus");
        }

        fn try_recv_event(&mut self) -> Option<PipelineEvent> {
            self.events.try_recv().ok()
        }

        fn take_events(&mut self) -> mpsc::UnboundedReceiver<PipelineEvent> {
            let (_tx, rx) = mpsc::unbounded_channel();
            std::mem::replace(&mut self.events, rx)
        }

        async fn stop(&self) {
            self.pipeline.stop().await.unwrap();
        }

        async fn start_with_current(&mut self) -> (PipelineTrack, PipelineTrack) {
            let track_a = self.track(Duration::from_secs(4), 8_000, 0);
            let track_b = self.track(Duration::from_secs(4), -8_000, 1);
            let initial_plan = PairPlan {
                generation: 1,
                output_epoch: 1,
                current: track_a.clone(),
                next: None,
                mode: ReplaceMode::InitialReplaceFromStopped,
            };
            self.pipeline.replace(initial_plan).await.unwrap();
            assert_eq!(self.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
            (track_a, track_b)
        }

        async fn start_with_current_and_next(&mut self) -> (PipelineTrack, PipelineTrack, PipelineTrack) {
            let track_a = self.track(Duration::from_secs(4), 8_000, 0);
            let track_b = self.track(Duration::from_secs(4), -8_000, 1);
            let track_c = self.track(Duration::from_secs(4), 4_000, 2);
            let initial_plan = PairPlan {
                generation: 1,
                output_epoch: 1,
                current: track_a.clone(),
                next: Some(PlannedNext {
                    track: track_b.clone(),
                    transition: TransitionPlan::Cut,
                }),
                mode: ReplaceMode::InitialReplaceFromStopped,
            };
            self.pipeline.replace(initial_plan).await.unwrap();
            assert_eq!(self.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
            (track_a, track_b, track_c)
        }
    }

    #[tokio::test]
    async fn output_sink_error_generates_sink_disconnected() {
        let mut harness = GstHarness::start_playing(1).await;
        let sink = harness.pipeline.current_sink_element();
        harness.post_error(&sink, "icecast connection lost");

        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 1, output_epoch: 1, ref message } if message.contains("icecast connection lost")),
            "expected SinkDisconnected event, got {event:?}"
        );
    }

    #[tokio::test]
    async fn media_branch_decoder_error_emits_decode_failed() {
        let mut harness = GstHarness::start_playing(1).await;
        let (source, track_key) = {
            let reg = harness.pipeline.registry.lock().unwrap();
            (reg.live[0].source.clone(), reg.live[0].key.clone())
        };
        harness.post_error(&source, "corrupt audio payload");

        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::DecodeFailed { generation: 1, ref track, ref message } if track == &track_key && message.contains("corrupt audio payload")),
            "expected DecodeFailed event for media branch error, got {event:?}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn stale_generation_branch_error_preserves_branch_generation() {
        let mut harness = GstHarness::start_playing(1).await;
        let (source, track_key) = {
            let reg = harness.pipeline.registry.lock().unwrap();
            (reg.live[0].source.clone(), reg.live[0].key.clone())
        };
        // Advance active plan to generation 2
        {
            let mut active = harness.pipeline.active.lock().unwrap();
            active.as_mut().unwrap().generation = 2;
        }

        // Post error on generation 1 branch
        harness.post_error(&source, "late error from branch G1");

        // Event MUST preserve generation 1 from the branch
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::DecodeFailed { generation: 1, ref track, ref message } if track == &track_key && message.contains("late error from branch G1")),
            "branch error must preserve branch's generation 1 even when active is generation 2, got {event:?}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn initial_replace_error_before_active_plan_commit_is_not_dropped() {
        let mut harness = GstHarness::new().await;
        let file_a = harness.wav(Duration::from_secs(2), 8_000);
        let file_b = harness.wav(Duration::from_secs(2), -8_000);
        let mut song_a = testsupport::queued_song("song_a", 0);
        song_a.file_path = file_a.to_str().unwrap().to_string();
        let mut song_b = testsupport::queued_song("song_b", 1);
        song_b.file_path = file_b.to_str().unwrap().to_string();
        let track_a = StationController::track(song_a);
        let track_b = StationController::track(song_b);
        let track_key = track_a.key.clone();

        // Setup hook in GStreamerPipeline: right after branches are created
        // and pending_generation is set, but BEFORE ActivePlan is committed,
        // inject an error on branch A's uridecodebin source element!
        let pipeline_for_hook = harness.pipeline.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));
        harness.pipeline.set_pre_commit_hook(move || {
            assert!(
                pipeline_for_hook.active.lock().unwrap().is_none(),
                "active plan must not be committed yet"
            );
            assert_eq!(*pipeline_for_hook.pending_epoch.lock().unwrap(), Some(1));
            let source_a = {
                let reg = pipeline_for_hook.registry.lock().unwrap();
                assert!(!reg.preparing.is_empty(), "branch A must be preparing");
                reg.preparing[0].elements[0].clone()
            };
            let error_msg = gst::message::Error::builder(gst::StreamError::Failed, "early decode failure during initial replace")
                .src(&source_a)
                .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);
            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            Ok(())
        });

        let plan = initial_plan(1, track_a, Some(track_b), TransitionPlan::Cut);
        let result = harness.pipeline.replace(plan).await;
        assert!(result.is_err(), "initial replace must return Err on early decode failure");
        hook_ran_rx.await.expect("pre-commit hook must have run");

        // Bus handler MUST emit DecodeFailed for track A with generation 1 (NOT dropped, NOT FatalPipeline)
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::DecodeFailed { generation: 1, ref track, ref message } if track == &track_key && message.contains("early decode failure")),
            "error before active plan commit must emit DecodeFailed for track A, got {event:?}"
        );
        harness.stop().await;
    }

    #[tokio::test]
    #[ignore]
    async fn backbone_error_during_pending_operation_uses_pipeline_epoch() {
        let mut harness = GstHarness::start_playing(1).await;
        let encoder = harness.pipeline.encoder_element();
        let file_c = harness.wav(Duration::from_secs(2), 8_000);
        let mut song_c = testsupport::queued_song("song_c", 2);
        song_c.file_path = file_c.to_str().unwrap().to_string();
        let track_c = StationController::track(song_c);

        // Active plan is generation 1, output_epoch 1.
        assert_eq!(harness.pipeline.active.lock().unwrap().as_ref().unwrap().generation, 1);
        assert_eq!(harness.pipeline.active.lock().unwrap().as_ref().unwrap().output_epoch, 1);

        // Setup hook: during replace generation 2, before active commit, post error on encoder!
        let pipeline_for_hook = harness.pipeline.clone();
        let encoder_for_hook = encoder.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));
        harness.pipeline.set_pre_commit_hook(move || {
            assert_eq!(*pipeline_for_hook.pending_epoch.lock().unwrap(), Some(1));
            assert_eq!(pipeline_for_hook.active.lock().unwrap().as_ref().unwrap().generation, 1);
            let error_msg = gst::message::Error::builder(gst::StreamError::Failed, "backbone encoder failed during replace G2")
                .src(&encoder_for_hook)
                .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);
            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            Ok(())
        });
        let replace_plan = PairPlan {
            generation: 2,
            output_epoch: 1,
            mode: ReplaceMode::ActiveReplace {
                expected_generation: 1,
                expected_current: harness.pipeline.active.lock().unwrap().as_ref().unwrap().current.clone(),
            },
            current: track_c,
            next: None,
        };
        let _ = harness.pipeline.replace(replace_plan).await;
        hook_ran_rx.await.expect("pre-commit hook must have run");

        // Bus event MUST carry pipeline_epoch 1!
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 1, ref message } if message.contains("backbone encoder failed during replace G2")),
            "fatal backbone error during pending operation G2 must carry pipeline_epoch 1, got {event:?}"
        );
        harness.stop().await;
    }
    #[tokio::test]
    #[ignore]
    async fn stale_replace_plan_does_not_claim_backbone_error_and_preserves_pipeline_epoch() {
        let mut harness = GstHarness::start_playing(1).await;
        let encoder = harness.pipeline.encoder_element();
        let file_c = harness.wav(Duration::from_secs(2), 8_000);
        let mut song_c = testsupport::queued_song("song_c", 2);
        song_c.file_path = file_c.to_str().unwrap().to_string();
        let track_c = StationController::track(song_c);

        // Active plan is generation 1, epoch 1.
        assert_eq!(harness.pipeline.active.lock().unwrap().as_ref().unwrap().generation, 1);
        assert_eq!(harness.pipeline.active.lock().unwrap().as_ref().unwrap().output_epoch, 1);

        // A stale replace plan with wrong expected_generation 99:
        let stale_replace_plan = PairPlan {
            generation: 2,
            output_epoch: 99,
            mode: ReplaceMode::ActiveReplace {
                expected_generation: 99,
                expected_current: harness.pipeline.active.lock().unwrap().as_ref().unwrap().current.clone(),
            },
            current: track_c,
            next: None,
        };
        let result = harness.pipeline.replace(stale_replace_plan).await;
        assert!(matches!(result, Err(PipelineError::StalePlan)));

        // Pending epoch MUST NOT have been published / must be None.
        assert_eq!(*harness.pipeline.pending_epoch.lock().unwrap(), None);

        // Inject backbone error on encoder
        harness.post_error(&encoder, "encoder error during active G1 after rejected stale replace");

        // FatalPipeline MUST carry active pipeline_epoch 1!
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 1, ref message } if message.contains("encoder error during active G1")),
            "fatal backbone error after rejected stale replace must use active pipeline_epoch 1, got {event:?}"
        );
        harness.stop().await;
    }

    #[tokio::test]
    #[ignore]
    async fn stale_roll_plan_does_not_claim_backbone_error_and_preserves_pipeline_epoch() {
        let mut harness = GstHarness::start_playing(1).await;
        let mixer = harness.pipeline.mixer_element();
        let file_c = harness.wav(Duration::from_secs(2), 8_000);
        let mut song_c = testsupport::queued_song("song_c", 2);
        song_c.file_path = file_c.to_str().unwrap().to_string();
        let track_c = StationController::track(song_c);

        // Active plan is generation 1, epoch 1.
        assert_eq!(harness.pipeline.active.lock().unwrap().as_ref().unwrap().generation, 1);
        assert_eq!(harness.pipeline.active.lock().unwrap().as_ref().unwrap().output_epoch, 1);

        // A stale roll plan with mismatched generation 99:
        let stale_roll_plan = RollingPlan {
            generation: 99,
            current: harness.pipeline.active.lock().unwrap().as_ref().unwrap().current.clone(),
            change: RollingChange::Attach(PlannedNext {
                track: track_c,
                transition: TransitionPlan::Cut,
            }),
        };
        let result = harness.pipeline.roll(stale_roll_plan).await;
        assert!(matches!(result, Err(PipelineError::StalePlan)));

        // Pending epoch MUST NOT have been published / must be None.
        assert_eq!(*harness.pipeline.pending_epoch.lock().unwrap(), None);

        // Inject backbone error on mixer
        harness.post_error(&mixer, "mixer error during active G1 after rejected stale roll");

        // FatalPipeline MUST carry active pipeline_epoch 1!
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 1, ref message } if message.contains("mixer error during active G1")),
            "fatal backbone error after rejected stale roll must use active pipeline_epoch 1, got {event:?}"
        );
        harness.stop().await;
    }

    #[tokio::test]
    #[ignore]
    async fn branch_discard_does_not_flush_or_break_surviving_stream() {
        let mut harness = GstHarness::new().await;
        let file_a = harness.wav(Duration::from_secs(3), 8_000);
        let file_b = harness.wav(Duration::from_secs(2), -8_000);
        let file_c = harness.wav(Duration::from_secs(2), 8_000);
        let mut song_a = testsupport::queued_song("song_a", 0);
        song_a.file_path = file_a.to_str().unwrap().to_string();
        let mut song_b = testsupport::queued_song("song_b", 1);
        song_b.file_path = file_b.to_str().unwrap().to_string();
        let mut song_c = testsupport::queued_song("song_c", 2);
        song_c.file_path = file_c.to_str().unwrap().to_string();
        let track_a = StationController::track(song_a);
        let track_b = StationController::track(song_b);
        let track_c = StationController::track(song_c);
        let key_b = track_b.key.clone();

        // 1. Initial replace: track A current, track B next
        harness
            .pipeline
            .replace(initial_plan(1, track_a.clone(), Some(track_b), TransitionPlan::Cut))
            .await
            .unwrap();

        assert_eq!(harness.pipeline.registry.lock().unwrap().live.len(), 2);
        assert_eq!(harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);

        // 2. Roll ReplaceNext: replaces obsolete branch B with branch C (discarding B)
        let roll_plan = RollingPlan {
            generation: 1,
            current: track_a.key.clone(),
            change: RollingChange::ReplaceNext {
                expected_next: key_b,
                replacement: Some(PlannedNext {
                    track: track_c.clone(),
                    transition: TransitionPlan::Cut,
                }),
            },
        };
        harness.pipeline.roll(roll_plan).await.unwrap();

        // 3. Verify surviving branch A and new branch C exist, and obsolete branch B was discarded
        {
            let reg = harness.pipeline.registry.lock().unwrap();
            assert_eq!(reg.live.len(), 2);
            assert_eq!(reg.live[0].key, track_a.key);
            assert_eq!(reg.live[1].key, track_c.key);
        }
        // 4. Verify downstream is NOT in flushing state: buffers continue flowing from clock_gate
        let clock_gate = harness.pipeline.pipeline.by_name("clock_gate").expect("clock gate present");
        let gate_src = clock_gate.static_pad("src").expect("clock gate src pad");
        let (buf_tx, mut buf_rx) = mpsc::unbounded_channel();
        let _probe = gate_src.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            if info.buffer().is_some() {
                let _ = buf_tx.send(());
            }
            gst::PadProbeReturn::Ok
        });

        for _ in 0..5 {
            tokio::time::timeout(Duration::from_secs(5), buf_rx.recv())
                .await
                .expect("audio buffers must continue flowing downstream after branch discard")
                .expect("buffer channel open");
        }

        assert_eq!(harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
        harness.stop().await;
    }

    struct RealRuntimeFixture {
        harness: GstHarness,
        runtime: StationRuntime,
        song_a: crate::streamer::SongInfo,
        song_b: crate::streamer::SongInfo,
    }

    async fn start_real_runtime_with_two_tracks() -> RealRuntimeFixture {
        let mut harness = GstHarness::new().await;
        let file_a = harness.wav(Duration::from_secs(4), 8_000);
        let file_b = harness.wav(Duration::from_secs(4), -8_000);
        let mut song_a = testsupport::queued_song("song_a", 0);
        song_a.file_path = file_a.to_str().unwrap().to_string();
        let mut song_b = testsupport::queued_song("song_b", 1);
        song_b.file_path = file_b.to_str().unwrap().to_string();

        let controller = crate::streamer::controller::test_controller(harness.pipeline.clone(), vec![song_a.clone(), song_b.clone()]);
        let events = harness.take_events();
        let runtime = StationRuntime::spawn(controller, events);

        runtime.play().await.unwrap();

        testsupport::wait_for_timeout(Duration::from_secs(5), "pipeline to start playing track A", || {
            harness.pipeline.snapshot.lock().unwrap().state == PipelineState::Playing
                && !harness.pipeline.registry.lock().unwrap().live.is_empty()
        })
        .await;

        RealRuntimeFixture {
            harness,
            runtime,
            song_a,
            song_b,
        }
    }

    #[tokio::test]
    async fn real_gstreamer_bus_error_recovers_through_runtime_event_loop() {
        let fixture = start_real_runtime_with_two_tracks().await;

        let source_a = {
            let reg = fixture.harness.pipeline.registry.lock().unwrap();
            reg.live[0].source.clone()
        };
        // 2. Post real GST_MESSAGE_ERROR on branch A source element
        fixture.harness.post_error(&source_a, "corrupt payload on playing track A");

        // 3. StationRuntime event loop receives DecodeFailed -> prepares Replace -> driver runs Replace -> commits skip to track B
        testsupport::wait_for_async_timeout(
            Duration::from_secs(5),
            "runtime to recover and switch current track to song B",
            || async {
                matches!(
                    fixture.runtime.status().await,
                    Ok(crate::streamer::StatusEvent::State { ref title, playing: true, .. }) if title == &fixture.song_b.title
                )
            },
        )
        .await;

        let _ = fixture.runtime.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn real_gstreamer_fatal_backbone_error_before_commit_stops_runtime_cleanly() {
        let fixture = start_real_runtime_with_two_tracks().await;

        // 2. Set hook: during Replace G2 (skip to song B), before active commit:
        //    - post fatal error on encoder
        //    - signal hook_entered
        //    - wait for release barrier
        let pipeline_for_hook = fixture.harness.pipeline.clone();
        let encoder = fixture.harness.pipeline.encoder_element();
        let (hook_entered_tx, hook_entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let hook_entered_tx = std::sync::Mutex::new(Some(hook_entered_tx));

        fixture.harness.pipeline.set_pre_commit_hook(move || {
            let error_msg = gst::message::Error::builder(
                gst::StreamError::Failed,
                "fatal encoder crash during accepted replace before commit",
            )
            .src(&encoder)
            .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);
            if let Some(tx) = hook_entered_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            tokio::task::block_in_place(|| {
                let _ = release_rx.recv();
            });
            Ok(())
        });

        // 3. Trigger skip to song B in background task while playing
        let skip_runtime = fixture.runtime.clone();
        let skip_handle = tokio::spawn(async move { skip_runtime.skip().await });
        hook_entered_rx.await.expect("pre-commit hook must have run and paused replace");

        // 4. StationRuntime processes FatalPipeline and transitions controller to Stopped WHILE Replace is still paused in pre-commit
        testsupport::wait_for_async_timeout(
            Duration::from_secs(5),
            "controller to stop after backbone fatal error before replace commit",
            || async { matches!(fixture.runtime.state().await, Ok(crate::streamer::pipeline::PipelineState::Stopped)) },
        )
        .await;

        // 5. Unblock the replace operation so executor can finish draining
        let _ = release_tx.send(());
        let _ = skip_handle.await;

        // 6. Station remains Stopped
        assert!(matches!(
            fixture.runtime.status().await,
            Ok(crate::streamer::StatusEvent::State { playing: false, .. })
        ));

        let _ = fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    #[ignore]
    async fn real_gstreamer_fatal_backbone_error_during_failed_pending_replace_stops_runtime() {
        let fixture = start_real_runtime_with_two_tracks().await;

        // 2. Set hook on pipeline to make replace fail and inject backbone error
        let pipeline_for_hook = fixture.harness.pipeline.clone();
        let encoder = fixture.harness.pipeline.encoder_element();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));
        fixture.harness.pipeline.set_pre_commit_hook(move || {
            let error_msg = gst::message::Error::builder(gst::StreamError::Failed, "fatal encoder crash during failed replace")
                .src(&encoder)
                .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);
            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            Err(PipelineError::Pipeline("injected hardware fault during replace".into()))
        });

        // 3. Trigger skip in background task while playing
        let skip_runtime = fixture.runtime.clone();
        let skip_handle = tokio::spawn(async move { skip_runtime.skip().await });
        hook_ran_rx.await.expect("pre-commit hook must have run");

        // 4. Skip MUST fail with Err because replace operation returned Err
        let skip_result = skip_handle.await.expect("skip task panicked");
        assert!(skip_result.is_err(), "skip must return Err on failed replace operation");

        // 5. StationRuntime processes FatalPipeline and transitions to Stopped
        testsupport::wait_for_async_timeout(Duration::from_secs(5), "station to stop after fatal error", || async {
            matches!(
                fixture.runtime.status().await,
                Ok(crate::streamer::StatusEvent::State { playing: false, .. })
            )
        })
        .await;

        let _ = fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    #[ignore]
    async fn retiring_branch_error_during_discard_is_suppressed_without_deadlock() {
        let mut harness = GstHarness::start_playing(1).await;
        let pipeline = harness.pipeline.clone();

        let (hook_entered_tx, hook_entered_rx) = tokio::sync::oneshot::channel();
        let hook_entered_tx = std::sync::Mutex::new(Some(hook_entered_tx));

        // When discard is called during force_stopped, the hook runs while
        // discard is executing. The hook posts an error on the pipeline bus
        // from the REAL retiring branch source element.
        harness.pipeline.set_teardown_hook(move |branch| {
            let error_msg = gst::message::Error::builder(gst::StreamError::Failed, "simulated corrupt source on retiring branch")
                .src(&branch.source)
                .build();
            let _ = pipeline.pipeline.bus().unwrap().post(error_msg);
            if let Some(tx) = hook_entered_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
        });

        // Trigger teardown
        harness.pipeline.force_stopped();

        hook_entered_rx
            .await
            .expect("teardown hook must have executed during branch discard");

        // Verify:
        // 1. Teardown completes without deadlock
        // 2. Error from retiring branch was suppressed -> NO event emitted
        assert!(
            harness.try_recv_event().is_none(),
            "error from retiring branch during discard must be suppressed and emit no event"
        );
        assert_eq!(harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Stopped);
        assert!(harness.pipeline.registry.lock().unwrap().live.is_empty());
    }

    fn preparing_source(pipeline: &GStreamerPipeline, key: &TrackKey, expected_generation: u64) -> gst::Element {
        let reg = pipeline.registry.lock().unwrap();
        let prep = reg
            .preparing
            .iter()
            .find(|p| p.key == *key)
            .expect("candidate must be in preparing branches");
        assert_eq!(prep.generation, expected_generation);
        prep.elements.first().cloned().expect("preparing branch must have elements")
    }

    #[tokio::test]
    async fn active_replace_candidate_media_error_is_classified_as_decode_failed_and_not_fatal_pipeline() {
        let mut harness = GstHarness::new().await;
        let (track_a, track_b) = harness.start_with_current().await;
        let pipeline_weak = Arc::downgrade(&harness.pipeline);
        let a_key = track_a.key.clone();
        let b_key = track_b.key.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));
        harness.pipeline.set_pre_commit_hook(move || {
            let Some(pipeline) = pipeline_weak.upgrade() else {
                return Err(PipelineError::Pipeline("pipeline dropped".into()));
            };

            // 1. Verify old track A is still active & live
            assert_eq!(
                pipeline.active.lock().unwrap().as_ref().map(|a| &a.current),
                Some(&a_key),
                "old track A must still be active"
            );
            assert!(
                pipeline.registry.lock().unwrap().live.iter().any(|b| b.key == a_key),
                "old track A must still be in live branches"
            );
            // 2. Verify candidate B is registered in preparing
            let b_source = preparing_source(&pipeline, &b_key, 2);

            // 3. Verify candidate B is a child/descendant of gst::Pipeline
            assert!(
                bus::is_element_or_child(b_source.upcast_ref(), pipeline.pipeline.upcast_ref()),
                "candidate B must be a child of gst::Pipeline"
            );

            // 4. Post GST_MESSAGE_ERROR from candidate B source element
            let error_msg = gst::message::Error::builder(gst::StreamError::Decode, "corrupt candidate media payload")
                .src(&b_source)
                .build();
            let _ = pipeline.pipeline.bus().unwrap().post(error_msg);

            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }

            // Hook returns Ok(()) — the decode error itself must drive rollback!
            Ok(())
        });

        let active_replace_plan = PairPlan {
            generation: 2,
            output_epoch: 1,
            current: track_b.clone(),
            next: None,
            mode: ReplaceMode::ActiveReplace {
                expected_generation: 1,
                expected_current: track_a.key.clone(),
            },
        };

        let result = harness.pipeline.replace(active_replace_plan).await;
        assert!(result.is_err(), "active replace must fail when candidate B emits media error");

        hook_ran_rx.await.expect("pre-commit hook must have executed");

        // 5. Verify the error event sent to controller:
        // MUST be DecodeFailed { generation: 2, track: track_b.key }
        // MUST NOT be FatalPipeline or SinkDisconnected
        let event = harness.try_recv_event().expect("an event must be emitted");
        match event {
            PipelineEvent::DecodeFailed { generation, track, .. } => {
                assert_eq!(generation, 2);
                assert_eq!(track, track_b.key);
            }
            other => panic!("expected PipelineEvent::DecodeFailed, got {other:?}"),
        }
        assert!(harness.try_recv_event().is_none(), "no second event must be emitted");

        // 6. Old track A remains playing & intact
        assert_eq!(harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
        assert_eq!(
            harness.pipeline.active.lock().unwrap().as_ref().map(|a| &a.current),
            Some(&track_a.key)
        );
        assert!(harness.pipeline.registry.lock().unwrap().live.iter().any(|b| b.key == track_a.key));
        assert!(harness.pipeline.registry.lock().unwrap().preparing.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn active_replace_pending_next_candidate_error_keeps_valid_current() {
        let mut harness = GstHarness::new().await;
        let (track_a, track_b, track_c) = harness.start_with_current_and_next().await;

        let pipeline_weak = Arc::downgrade(&harness.pipeline);
        let c_key = track_c.key.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));

        harness.pipeline.set_pre_commit_hook(move || {
            let Some(pipeline) = pipeline_weak.upgrade() else {
                return Err(PipelineError::Pipeline("pipeline dropped".into()));
            };

            // Verify candidate C is registered in preparing
            let c_source = preparing_source(&pipeline, &c_key, 2);

            // Verify candidate C is a child of gst::Pipeline
            assert!(
                bus::is_element_or_child(c_source.upcast_ref(), pipeline.pipeline.upcast_ref()),
                "candidate C must be a child of gst::Pipeline"
            );

            // Post GST_MESSAGE_ERROR from candidate C source element
            let error_msg = gst::message::Error::builder(gst::StreamError::Decode, "corrupt next candidate media payload")
                .src(&c_source)
                .build();
            let _ = pipeline.pipeline.bus().unwrap().post(error_msg);

            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }

            // Hook returns Ok(()) — error alone drives rollback!
            Ok(())
        });

        let active_replace_plan = PairPlan {
            generation: 2,
            output_epoch: 1,
            current: track_b.clone(),
            next: Some(PlannedNext {
                track: track_c.clone(),
                transition: TransitionPlan::Cut,
            }),
            mode: ReplaceMode::ActiveReplace {
                expected_generation: 1,
                expected_current: track_a.key.clone(),
            },
        };

        let result = harness.pipeline.replace(active_replace_plan).await;
        assert!(result.is_err(), "active replace must fail when candidate C emits media error");

        hook_ran_rx.await.expect("pre-commit hook must have executed");

        // Verify the error event sent to controller is DecodeFailed for track C
        let event = harness.try_recv_event().expect("an event must be emitted");
        match event {
            PipelineEvent::DecodeFailed { generation, track, .. } => {
                assert_eq!(generation, 2);
                assert_eq!(track, track_c.key);
            }
            other => panic!("expected PipelineEvent::DecodeFailed, got {other:?}"),
        }
        assert!(harness.try_recv_event().is_none(), "no second event must be emitted");

        // Old track A remains playing & intact, preparing is cleaned up
        assert_eq!(harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
        assert_eq!(
            harness.pipeline.active.lock().unwrap().as_ref().map(|a| &a.current),
            Some(&track_a.key)
        );
        assert!(harness.pipeline.registry.lock().unwrap().live.iter().any(|b| b.key == track_a.key));
        assert!(harness.pipeline.registry.lock().unwrap().preparing.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn handover_during_active_replace_rejection_leaves_registry_and_active_coherent() {
        let mut harness = GstHarness::new().await;
        let (track_a, track_b) = harness.start_with_current().await;
        let pipeline_weak = Arc::downgrade(&harness.pipeline);
        let b_key = track_b.key.clone();
        let b_metadata = track_b.metadata.clone();

        // Simulate concurrent Handover occurring right before replace_active commit boundary
        harness.pipeline.set_pre_commit_hook(move || {
            let Some(pipeline) = pipeline_weak.upgrade() else { return Ok(()) };
            let mut active = pipeline.active.lock().unwrap();
            if let Some(a) = active.as_mut() {
                a.generation = 2;
                a.current = b_key.clone();
                a.current_metadata = b_metadata.clone();
                a.next = None;
                a.handed_over = true;
            }
            Ok(())
        });

        let active_replace_plan = PairPlan {
            generation: 2,
            output_epoch: 1,
            current: track_b.clone(),
            next: None,
            mode: ReplaceMode::ActiveReplace {
                expected_generation: 1,
                expected_current: track_a.key.clone(),
            },
        };

        let result = harness.pipeline.replace(active_replace_plan).await;
        assert!(
            matches!(result, Err(PipelineError::StalePlan)),
            "active replace must return StalePlan on concurrent handover"
        );

        // Assert after StalePlan rollback:
        // Candidate B is discarded, preparing is empty, active remains as set by handover
        let reg = harness.pipeline.registry.lock().unwrap();
        assert!(reg.preparing.is_empty(), "preparing must be empty after rollback");
        drop(reg);

        let active = harness.pipeline.active.lock().unwrap();
        assert_eq!(active.as_ref().map(|a| &a.current), Some(&track_b.key));
        assert_eq!(active.as_ref().map(|a| a.generation), Some(2));
    }

    fn capture_discarded_sources(pipeline: &GStreamerPipeline) -> Arc<Mutex<Vec<gst::Element>>> {
        let discarded_sources: Arc<Mutex<Vec<gst::Element>>> = Arc::new(Mutex::new(Vec::new()));
        let captured = discarded_sources.clone();
        pipeline.set_teardown_hook(move |branch| {
            captured.lock().unwrap().push(branch.source.clone());
        });
        discarded_sources
    }

    fn assert_fully_stopped_after_failed_initial(
        pipeline: &GStreamerPipeline,
        discarded_sources: &Arc<Mutex<Vec<gst::Element>>>,
        min_discarded: usize,
    ) {
        assert!(pipeline.active.lock().unwrap().is_none(), "active must be None");
        assert!(pipeline.pending_epoch.lock().unwrap().is_none(), "pending_epoch must be None");
        let reg = pipeline.registry.lock().unwrap();
        assert!(reg.live.is_empty(), "live must be empty");
        assert!(reg.preparing.is_empty(), "preparing must be empty");
        assert!(reg.retiring.is_empty(), "retiring must be empty");
        drop(reg);

        assert_eq!(
            pipeline.snapshot.lock().unwrap().state,
            PipelineState::Stopped,
            "snapshot state must be Stopped"
        );

        let discarded = discarded_sources.lock().unwrap();
        assert!(
            discarded.len() >= min_discarded,
            "expected at least {min_discarded} discarded branches, found {}",
            discarded.len()
        );
        for source in discarded.iter() {
            assert!(
                !bus::is_element_or_child(source.upcast_ref(), pipeline.gst_pipeline().upcast_ref()),
                "discarded source must not remain in gst::Pipeline"
            );
        }
    }

    #[tokio::test]
    async fn initial_replace_current_bus_error_before_commit_rolls_back_to_stopped() {
        let mut harness = GstHarness::new().await;
        let track_a = harness.track(Duration::from_secs(4), 8_000, 0);
        let discarded_sources = capture_discarded_sources(&harness.pipeline);

        let pipeline_for_hook = harness.pipeline.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));

        harness.pipeline.set_pre_commit_hook(move || {
            assert!(
                pipeline_for_hook.active.lock().unwrap().is_none(),
                "active plan must not be committed yet"
            );
            let source_a = {
                let reg = pipeline_for_hook.registry.lock().unwrap();
                let prep = reg.preparing.first().expect("branch A must be in preparing");
                prep.elements.first().cloned().expect("preparing branch must have elements")
            };
            let error_msg = gst::message::Error::builder(gst::StreamError::Decode, "current branch media error before commit")
                .src(&source_a)
                .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);
            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            // Hook returns Ok(())! The bus error alone must cancel/rollback replace_initial.
            Ok(())
        });

        let initial_plan = PairPlan {
            generation: 1,
            output_epoch: 1,
            current: track_a.clone(),
            next: None,
            mode: ReplaceMode::InitialReplaceFromStopped,
        };

        let result = harness.pipeline.replace(initial_plan).await;
        assert!(result.is_err(), "initial replace must fail when current branch fails decoding");
        hook_ran_rx.await.expect("pre-commit hook must have run");

        assert_fully_stopped_after_failed_initial(&harness.pipeline, &discarded_sources, 1);
    }

    #[tokio::test]
    #[ignore]
    async fn initial_replace_next_preparing_bus_error_before_adoption_rolls_back_to_stopped() {
        let mut harness = GstHarness::new().await;
        let track_a = harness.track(Duration::from_secs(4), 8_000, 0);
        let track_b = harness.track(Duration::from_secs(4), -8_000, 1);
        let discarded_sources = capture_discarded_sources(&harness.pipeline);

        let pipeline_for_hook = harness.pipeline.clone();
        let b_key = track_b.key.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));

        harness.pipeline.set_pre_commit_hook(move || {
            assert!(
                pipeline_for_hook.active.lock().unwrap().is_none(),
                "active plan must not be committed yet"
            );
            let source_b = {
                let reg = pipeline_for_hook.registry.lock().unwrap();
                let prep = reg
                    .preparing
                    .iter()
                    .find(|b| b.key == b_key)
                    .expect("branch B must be in preparing");
                prep.elements.first().cloned().expect("preparing branch B must have elements")
            };
            let error_msg = gst::message::Error::builder(gst::StreamError::Decode, "next preparing branch media error")
                .src(&source_b)
                .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);
            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            // Hook returns Ok(())!
            Ok(())
        });

        let initial_plan = PairPlan {
            generation: 1,
            output_epoch: 1,
            current: track_a.clone(),
            next: Some(PlannedNext {
                track: track_b.clone(),
                transition: TransitionPlan::Cut,
            }),
            mode: ReplaceMode::InitialReplaceFromStopped,
        };

        let result = harness.pipeline.replace(initial_plan).await;
        assert!(
            result.is_err(),
            "initial replace must fail when preparing next branch fails decoding"
        );
        hook_ran_rx.await.expect("pre-commit hook must have run");

        assert_fully_stopped_after_failed_initial(&harness.pipeline, &discarded_sources, 2);
    }

    #[tokio::test]
    #[ignore]
    async fn real_gstreamer_initial_play_bus_error_recovers_to_stopped_through_runtime() {
        let mut harness = GstHarness::new().await;
        let file_a = harness.wav(Duration::from_secs(4), 8_000);
        let mut song_a = testsupport::queued_song("song_a", 0);
        song_a.file_path = file_a.to_str().unwrap().to_string();

        let controller = crate::streamer::controller::test_controller(harness.pipeline.clone(), vec![song_a.clone()]);
        let events = harness.take_events();
        let runtime = StationRuntime::spawn(controller, events);

        let pipeline_for_hook = harness.pipeline.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));

        harness.pipeline.set_pre_commit_hook(move || {
            let source_a = {
                let reg = pipeline_for_hook.registry.lock().unwrap();
                let prep = reg.preparing.first().expect("branch A must be in preparing branches");
                prep.elements.first().cloned().expect("preparing branch must have elements")
            };

            let error_msg = gst::message::Error::builder(gst::StreamError::Decode, "corrupt initial media payload")
                .src(&source_a)
                .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);

            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }

            // Hook returns Ok(())! The bus error alone must cause initial play failure.
            Ok(())
        });

        // Trigger runtime.play()
        let play_result = runtime.play().await;
        assert!(play_result.is_err(), "play must return Err on initial replace media error");
        hook_ran_rx.await.expect("pre-commit hook must have run");

        // StationRuntime must NOT report successful Playing of broken track:
        // Controller initial play failure policy keeps station Stopped:
        testsupport::wait_for_timeout(Duration::from_secs(5), "station to remain stopped after initial play error", || {
            harness.pipeline.snapshot.lock().unwrap().state == PipelineState::Stopped
        })
        .await;

        assert_eq!(runtime.state().await.unwrap(), PipelineState::Stopped);
        let status = runtime.status().await.unwrap();
        assert!(
            matches!(status, crate::streamer::StatusEvent::State { playing: false, .. }),
            "station must NOT report playing after initial play error"
        );
        assert!(harness.pipeline.active.lock().unwrap().is_none(), "active plan must be None");
        assert!(
            harness.pipeline.pending_epoch.lock().unwrap().is_none(),
            "pending_epoch must be None"
        );
        {
            let reg = harness.pipeline.registry.lock().unwrap();
            assert!(reg.live.is_empty(), "live branches must be empty");
            assert!(reg.preparing.is_empty(), "preparing branches must be empty");
            assert!(reg.retiring.is_empty(), "retiring branches must be empty");
        }

        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    #[ignore]
    async fn initial_replace_failure_before_next_preparation_rolls_back_to_stopped() {
        let mut harness = GstHarness::new().await;
        let track_a = harness.track(Duration::from_secs(4), 8_000, 0);
        let mut track_b = harness.track(Duration::from_secs(4), -8_000, 1);
        track_b.path = std::path::PathBuf::from("");

        let discarded_sources = capture_discarded_sources(&harness.pipeline);

        let initial_plan = PairPlan {
            generation: 1,
            output_epoch: 1,
            current: track_a.clone(),
            next: Some(PlannedNext {
                track: track_b.clone(),
                transition: TransitionPlan::Cut,
            }),
            mode: ReplaceMode::InitialReplaceFromStopped,
        };

        let result = harness.pipeline.replace(initial_plan).await;
        assert!(result.is_err(), "initial replace must fail when next branch attachment fails");

        assert_fully_stopped_after_failed_initial(&harness.pipeline, &discarded_sources, 1);
    }

    #[tokio::test]
    #[ignore]
    async fn initial_replace_next_branch_state_error_rolls_back_to_stopped_and_cleans_up() {
        let mut harness = GstHarness::new().await;
        let track_a = harness.track(Duration::from_secs(4), 8_000, 0);
        let temp_dir = tempfile::tempdir().unwrap();
        let corrupt_path = temp_dir.path().join("corrupt.wav");
        std::fs::write(&corrupt_path, b"RIFF....WAVEfmt ....data not valid audio payload").unwrap();
        let mut track_b = harness.track(Duration::from_secs(4), -8_000, 1);
        track_b.path = corrupt_path;
        let discarded_sources = capture_discarded_sources(&harness.pipeline);

        let initial_plan = PairPlan {
            generation: 1,
            output_epoch: 1,
            current: track_a.clone(),
            next: Some(PlannedNext {
                track: track_b.clone(),
                transition: TransitionPlan::Cut,
            }),
            mode: ReplaceMode::InitialReplaceFromStopped,
        };

        let result = harness.pipeline.replace(initial_plan).await;
        assert!(result.is_err(), "initial replace must fail on source.state error");

        assert_fully_stopped_after_failed_initial(&harness.pipeline, &discarded_sources, 2);
    }

    #[tokio::test]
    #[ignore]
    async fn initial_replace_pre_commit_failure_rolls_back_both_live_branches_to_stopped() {
        let mut harness = GstHarness::new().await;
        let track_a = harness.track(Duration::from_secs(4), 8_000, 0);
        let track_b = harness.track(Duration::from_secs(4), -8_000, 1);
        let discarded_sources = capture_discarded_sources(&harness.pipeline);

        let pipeline_weak = Arc::downgrade(&harness.pipeline);
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));

        harness.pipeline.set_pre_commit_hook(move || {
            let Some(pipeline) = pipeline_weak.upgrade() else {
                return Err(PipelineError::Pipeline("pipeline dropped".into()));
            };
            let reg = pipeline.registry.lock().unwrap();
            assert_eq!(reg.preparing.len(), 2, "both branches must be in preparing before commit");
            assert!(reg.live.is_empty(), "live must be empty before commit");
            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            Err(PipelineError::Pipeline("forced pre-commit failure in initial replace".into()))
        });

        let initial_plan = PairPlan {
            generation: 1,
            output_epoch: 1,
            current: track_a.clone(),
            next: Some(PlannedNext {
                track: track_b.clone(),
                transition: TransitionPlan::Cut,
            }),
            mode: ReplaceMode::InitialReplaceFromStopped,
        };

        let result = harness.pipeline.replace(initial_plan).await;
        assert!(result.is_err(), "initial replace must fail when pre_commit_hook fails");
        hook_ran_rx.await.expect("pre-commit hook must have executed");

        assert_fully_stopped_after_failed_initial(&harness.pipeline, &discarded_sources, 2);
    }
    #[tokio::test]
    async fn take_paused_release_separates_probe_removal_from_registry_mutation() {
        let mut harness = GstHarness::new().await;
        let track = harness.track(Duration::from_secs(4), 8_000, 0);

        let branch = branch::attach_paused(
            &harness.pipeline.pipeline,
            &harness.pipeline.mixer,
            Some(&harness.pipeline.registry),
            harness.pipeline.events.clone(),
            &track,
            1,
            1.0,
        )
        .unwrap();

        // 1. Under registry lock: extract action without GStreamer side effects
        let action = {
            let mut reg = harness.pipeline.registry.lock().unwrap();
            reg.live.push(branch);
            branch::take_paused_release(&mut reg.live[0]).expect("action must be extracted")
        };

        // 2. Registry lock can be acquired freely by bus sync handler or other threads
        let reg_lock = harness.pipeline.registry.lock();
        assert!(reg_lock.is_ok(), "registry lock must be acquirable before apply");
        drop(reg_lock);

        // 3. Apply release outside locks: removes probe cleanly
        branch::apply_paused_release(action);

        let reg = harness.pipeline.registry.lock().unwrap();
        assert!(reg.live[0].gate.is_none(), "gate probe must be removed");
    }

    struct PostCommitBoundary {
        hook_entered: Arc<tokio::sync::Notify>,
        release_barrier: Arc<tokio::sync::Notify>,
        barrier_notify: Arc<tokio::sync::Notify>,
    }

    impl PostCommitBoundary {
        fn new() -> Self {
            Self {
                hook_entered: Arc::new(tokio::sync::Notify::new()),
                release_barrier: Arc::new(tokio::sync::Notify::new()),
                barrier_notify: Arc::new(tokio::sync::Notify::new()),
            }
        }

        fn hook(&self) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
            (self.hook_entered.clone(), self.release_barrier.clone(), self.barrier_notify.clone())
        }

        async fn wait_entered_and_processed(&self) {
            self.hook_entered.notified().await;
            self.barrier_notify.notified().await;
        }

        fn release(&self) {
            self.release_barrier.notify_one();
        }
    }

    fn post_branch_error(pipeline: &GStreamerPipeline, key: &TrackKey, error_msg: &'static str) {
        let source = {
            let reg = pipeline.registry.lock().unwrap();
            let branch = reg
                .live
                .iter()
                .find(|b| b.key == *key)
                .unwrap_or_else(|| panic!("branch {key:?} must be live"));
            branch.source.clone()
        };
        let msg = gst::message::Error::builder(gst::StreamError::Decode, error_msg)
            .src(&source)
            .build();
        let _ = pipeline.pipeline.bus().unwrap().post(msg);
    }
    struct PostCommitRuntimeFixture {
        harness: GstHarness,
        runtime: StationRuntime,
        songs: Vec<crate::streamer::SongInfo>,
        keys: Vec<TrackKey>,
        boundary: PostCommitBoundary,
    }

    impl PostCommitRuntimeFixture {
        async fn start(track_names: &[&'static str]) -> Self {
            let mut harness = GstHarness::new().await;
            let mut songs = Vec::with_capacity(track_names.len());
            let mut keys = Vec::with_capacity(track_names.len());
            for (idx, name) in track_names.iter().enumerate() {
                let file = harness.wav(Duration::from_secs(4), 8_000);
                let mut song = testsupport::queued_song(name, idx as i32);
                song.file_path = file.to_str().unwrap().to_string();
                keys.push(StationController::track(song.clone()).key);
                songs.push(song);
            }

            let controller = crate::streamer::controller::test_controller(harness.pipeline.clone(), songs.clone());
            let events = harness.take_events();
            let runtime = StationRuntime::spawn(controller, events);

            runtime.play().await.unwrap();

            testsupport::wait_for_timeout(Duration::from_secs(5), "pipeline to start playing track A", || {
                harness.pipeline.snapshot.lock().unwrap().state == PipelineState::Playing
                    && !harness.pipeline.registry.lock().unwrap().live.is_empty()
            })
            .await;

            let boundary = PostCommitBoundary::new();
            Self {
                harness,
                runtime,
                songs,
                keys,
                boundary,
            }
        }

        fn install_post_commit_errors(&self, error_keys: Vec<TrackKey>) {
            let (hook_entered, release_barrier, barrier_notify) = self.boundary.hook();
            let pipeline_for_hook = self.harness.pipeline.clone();
            let expected_current = self.keys[1].clone();
            self.harness.pipeline.set_post_commit_hook(move || {
                let pipeline = pipeline_for_hook.clone();
                let hook_entered = hook_entered.clone();
                let release_barrier = release_barrier.clone();
                let barrier_notify = barrier_notify.clone();
                let expected_current = expected_current.clone();
                let error_keys = error_keys.clone();
                async move {
                    assert_eq!(pipeline.active.lock().unwrap().as_ref().map(|a| a.generation), Some(2));
                    assert_eq!(
                        pipeline.active.lock().unwrap().as_ref().map(|a| &a.current),
                        Some(&expected_current)
                    );

                    for key in &error_keys {
                        post_branch_error(&pipeline, key, "corrupt candidate payload after release");
                    }
                    let _ = pipeline.events.send(PipelineEvent::TestBarrier(barrier_notify));

                    hook_entered.notify_one();
                    release_barrier.notified().await;
                }
            });
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn real_gstreamer_active_replace_post_commit_bus_error_recovers_through_runtime() {
        let fixture = PostCommitRuntimeFixture::start(&["song_a", "song_b", "song_c"]).await;
        let track_b_key = fixture.keys[1].clone();
        let track_c_key = fixture.keys[2].clone();

        fixture.install_post_commit_errors(vec![track_b_key.clone()]);
        let runtime_for_skip = fixture.runtime.clone();
        let skip_handle = tokio::spawn(async move { runtime_for_skip.skip().await });

        fixture.boundary.wait_entered_and_processed().await;

        let snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 1, "controller must still be in generation 1");
        assert_eq!(snapshot.state, PipelineState::Playing, "controller state must be Playing");
        assert!(snapshot.pending_skip.is_some(), "pending skip must be in flight");
        assert_eq!(
            snapshot.pending_skip_failures,
            Some((Some(track_b_key.clone()), None)),
            "failure for track B must be recorded in pending skip"
        );

        fixture.boundary.release();

        let skip_result = skip_handle.await.unwrap();
        assert!(skip_result.is_ok(), "skip returns Ok because physical replace was committed");

        let song_c = fixture.songs[2].clone();
        testsupport::wait_for_async_timeout(
            Duration::from_secs(5),
            "station to recover to track C after B decode failure",
            || async {
                if let Ok(crate::streamer::StatusEvent::State {
                    ref title, playing: true, ..
                }) = fixture.runtime.status().await
                {
                    title == &song_c.title
                } else {
                    false
                }
            },
        )
        .await;
        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| &a.current),
            Some(&track_c_key)
        );
        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| a.generation),
            Some(3)
        );

        let _ = fixture.runtime.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn real_gstreamer_active_replace_post_commit_staged_next_bus_error_recovers_through_runtime() {
        let fixture = PostCommitRuntimeFixture::start(&["song_a", "song_b", "song_c", "song_d"]).await;
        let track_c_key = fixture.keys[2].clone();
        let track_d_key = fixture.keys[3].clone();

        fixture.install_post_commit_errors(vec![track_c_key.clone()]);
        let runtime_for_skip = fixture.runtime.clone();
        let skip_handle = tokio::spawn(async move { runtime_for_skip.skip().await });
        fixture.boundary.wait_entered_and_processed().await;

        let snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 1, "controller must still be in generation 1");
        assert_eq!(snapshot.state, PipelineState::Playing, "controller state must be Playing");
        assert!(snapshot.pending_skip.is_some(), "pending skip must be in flight");
        assert_eq!(
            snapshot.pending_skip_failures,
            Some((None, Some(track_c_key.clone()))),
            "failure for staged next track C must be recorded in pending skip"
        );

        fixture.boundary.release();

        let skip_result = skip_handle.await.unwrap();
        assert!(skip_result.is_ok(), "skip returns Ok because physical replace was committed");

        let song_b = fixture.songs[1].clone();
        testsupport::wait_for_async_timeout(Duration::from_secs(5), "station to play track B", || async {
            if let Ok(crate::streamer::StatusEvent::State {
                ref title, playing: true, ..
            }) = fixture.runtime.status().await
            {
                title == &song_b.title
            } else {
                false
            }
        })
        .await;

        testsupport::wait_for_timeout(Duration::from_secs(5), "pipeline active to stage track D as next", || {
            let active = fixture.harness.pipeline.active.lock().unwrap();
            if let Some(plan) = active.as_ref() {
                plan.generation == 2 && plan.next.as_ref().map(|n| &n.0) == Some(&track_d_key)
            } else {
                false
            }
        })
        .await;

        let _ = fixture.runtime.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn real_gstreamer_active_replace_post_commit_both_current_and_staged_bus_errors_recover_through_runtime() {
        let fixture = PostCommitRuntimeFixture::start(&["song_a", "song_b", "song_c", "song_d"]).await;
        let track_b_key = fixture.keys[1].clone();
        let track_c_key = fixture.keys[2].clone();
        let track_d_key = fixture.keys[3].clone();

        fixture.install_post_commit_errors(vec![track_c_key.clone(), track_b_key.clone()]);

        let runtime_for_skip = fixture.runtime.clone();
        let skip_handle = tokio::spawn(async move { runtime_for_skip.skip().await });

        fixture.boundary.wait_entered_and_processed().await;
        let snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 1, "controller must still be in generation 1");
        assert_eq!(snapshot.state, PipelineState::Playing, "controller state must be Playing");
        assert!(snapshot.pending_skip.is_some(), "pending skip must be in flight");
        assert_eq!(
            snapshot.pending_skip_failures,
            Some((Some(track_b_key.clone()), Some(track_c_key.clone()))),
            "both failures must be recorded in pending skip"
        );

        fixture.boundary.release();

        let skip_result = skip_handle.await.unwrap();
        assert!(
            skip_result.is_ok(),
            "initial skip returns Ok because physical replace was committed"
        );

        // Controller commits G2, excludes C, and immediately performs recovery skip B -> D at G3!
        let song_d = fixture.songs[3].clone();
        testsupport::wait_for_async_timeout(
            Duration::from_secs(5),
            "station to recover directly to track D after B and C decode failures",
            || async {
                if let Ok(crate::streamer::StatusEvent::State {
                    ref title, playing: true, ..
                }) = fixture.runtime.status().await
                {
                    title == &song_d.title
                } else {
                    false
                }
            },
        )
        .await;

        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| &a.current),
            Some(&track_d_key)
        );
        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| a.generation),
            Some(3)
        );

        let final_snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(final_snapshot.generation, 3);
        assert_eq!(
            final_snapshot.pending_skip, None,
            "pending_skip must be cleared after recovery commit"
        );
        assert_eq!(final_snapshot.pending_realign, None, "no orphan pending_realign must remain");
        assert_eq!(final_snapshot.pending_skip_failures, None);

        let _ = fixture.runtime.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn real_gstreamer_active_replace_post_commit_both_current_and_staged_errors_with_stop_remains_stopped() {
        let fixture = PostCommitRuntimeFixture::start(&["song_a", "song_b", "song_c", "song_d"]).await;
        let track_b_key = fixture.keys[1].clone();
        let track_c_key = fixture.keys[2].clone();

        fixture.install_post_commit_errors(vec![track_c_key.clone(), track_b_key.clone()]);

        let runtime_for_skip = fixture.runtime.clone();
        let skip_handle = tokio::spawn(async move { runtime_for_skip.skip().await });

        fixture.boundary.wait_entered_and_processed().await;

        let snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 1, "controller must still be in generation 1");
        assert_eq!(snapshot.state, PipelineState::Playing, "controller state must be Playing");
        assert!(snapshot.pending_skip.is_some(), "pending skip must be in flight");
        assert_eq!(
            snapshot.pending_skip_failures,
            Some((Some(track_b_key.clone()), Some(track_c_key.clone()))),
            "both failures must be recorded in pending skip"
        );

        // Intervene with manual Stop test seam (StationCommand::Stop) while the hook is held!
        // `begin_command(StationCommand::Stop)` enqueues the manual stop, and `barrier()`
        // deterministically waits until the runtime command loop has executed `controller.stop()`.
        let stop_receiver = fixture
            .runtime
            .begin_command(crate::streamer::runtime::StationCommand::Stop)
            .await
            .expect("begin_command for Stop failed");
        fixture.runtime.barrier().await.expect("pre-release barrier failed");

        let stop_snapshot = fixture.runtime.test_snapshot().await.expect("test snapshot failed");
        assert_eq!(stop_snapshot.state, PipelineState::Stopped, "controller must logically be Stopped");
        assert_eq!(stop_snapshot.planned_next, None, "planned_next must be None after manual Stop");

        // Release the hook so the in-flight replace completes its physical step
        fixture.boundary.release();

        // The Stop operation queued to the urgent lane executes in the pipeline driver,
        // and the in-flight skip completes:
        let stop_result = stop_receiver.await.expect("stop command receiver failed");
        assert!(stop_result.is_ok(), "manual stop completes successfully");

        // Completion of `skip_handle` proves that the in-flight replace finished and its
        // `SkipResult` was fully processed by the runtime command loop:
        let skip_result = skip_handle.await.expect("skip task panicked");
        assert!(
            skip_result.is_ok(),
            "in-flight skip committed successfully even with intervening Stop"
        );

        // Command-loop barrier deterministically proves that the runtime loop remains alive
        // and that all prior command-loop effects are visible:
        fixture.runtime.barrier().await.expect("command-loop barrier failed");
        // Verify that runtime remains ALIVE, controller is Stopped, and no ghost realign/roll was emitted:
        let final_snapshot = fixture.runtime.test_snapshot().await.unwrap();
        assert_eq!(final_snapshot.state, PipelineState::Stopped, "controller must remain Stopped");
        assert_eq!(final_snapshot.pending_skip, None, "pending_skip must be cleared after late commit");
        assert_eq!(
            final_snapshot.pending_realign, None,
            "no orphan pending_realign must be created after Stop"
        );
        assert_eq!(final_snapshot.planned_next, None, "planned_next must remain None");
        assert_eq!(final_snapshot.pending_skip_failures, None);
        assert_eq!(
            fixture.harness.pipeline.snapshot.lock().unwrap().state,
            PipelineState::Stopped,
            "physical pipeline must stay Stopped"
        );

        // Finally, clean shutdown of the test fixture:
        let _ = fixture.runtime.shutdown().await;
    }
    #[tokio::test]
    #[ignore]
    async fn real_gstreamer_preparing_candidate_error_during_skip_recovers_through_runtime() {
        let mut harness = GstHarness::new().await;
        let file_a = harness.wav(Duration::from_secs(4), 8_000);
        let file_b = harness.wav(Duration::from_secs(4), -8_000);
        let mut song_a = testsupport::queued_song("song_a", 0);
        song_a.file_path = file_a.to_str().unwrap().to_string();
        let mut song_b = testsupport::queued_song("song_b", 1);
        song_b.file_path = file_b.to_str().unwrap().to_string();

        let controller = crate::streamer::controller::test_controller(harness.pipeline.clone(), vec![song_a.clone()]);
        let events = harness.take_events();
        let runtime = StationRuntime::spawn(controller, events);

        runtime.play().await.unwrap();

        testsupport::wait_for_timeout(Duration::from_secs(5), "pipeline to start playing track A", || {
            harness.pipeline.snapshot.lock().unwrap().state == PipelineState::Playing
                && !harness.pipeline.registry.lock().unwrap().live.is_empty()
        })
        .await;
        // Reload queue with song B (align_next: false) so skip has a candidate target without staging it to live
        runtime.reload(vec![song_a.clone(), song_b.clone()], false).await.unwrap();

        let fixture = RealRuntimeFixture {
            harness,
            runtime,
            song_a,
            song_b,
        };

        // 1. Station is playing track A in epoch 1
        assert_eq!(fixture.runtime.state().await.unwrap(), PipelineState::Playing);
        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| a.output_epoch),
            Some(1)
        );
        // 2. Set hook on pipeline to intercept preparing candidate B during skip's replace_active
        let pipeline_for_hook = fixture.harness.pipeline.clone();
        let (hook_ran_tx, hook_ran_rx) = tokio::sync::oneshot::channel();
        let hook_ran_tx = std::sync::Mutex::new(Some(hook_ran_tx));
        fixture.harness.pipeline.set_pre_commit_hook(move || {
            let b_source = {
                let reg = pipeline_for_hook.registry.lock().unwrap();
                let prep = reg.preparing.first().expect("candidate B must be in preparing branches");
                assert_eq!(prep.generation, 2);
                prep.elements.first().cloned().expect("preparing branch must have elements")
            };

            // Post GST_MESSAGE_ERROR from candidate B source element
            let error_msg = gst::message::Error::builder(gst::StreamError::Decode, "corrupt candidate media payload")
                .src(&b_source)
                .build();
            let _ = pipeline_for_hook.pipeline.bus().unwrap().post(error_msg);

            if let Some(tx) = hook_ran_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }

            // Hook returns Ok(())! The error alone must cancel/rollback the replace.
            Ok(())
        });

        // 3. Trigger skip to song B in background task while playing
        let skip_runtime = fixture.runtime.clone();
        let skip_handle = tokio::spawn(async move { skip_runtime.skip().await });
        hook_ran_rx.await.expect("pre-commit hook must have run");

        // 4. Skip MUST fail with Err because candidate B suffered media failure
        let skip_result = skip_handle.await.expect("skip task panicked");
        assert!(skip_result.is_err(), "skip must return Err on failed candidate replace");

        // 5. StationRuntime keeps Playing track A — it does NOT stop as FatalPipeline!
        testsupport::wait_for_async_timeout(
            Duration::from_secs(5),
            "station to remain Playing track A after candidate media error",
            || async {
                let status = fixture.runtime.status().await;
                matches!(
                    status,
                    Ok(crate::streamer::StatusEvent::State {
                        playing: true,
                        ref title,
                        ..
                    }) if title == &fixture.song_a.title
                )
            },
        )
        .await;

        let track_a_key = TrackKey {
            queue_item_id: fixture.song_a.queue_item_id,
            song_id: fixture.song_a.song_id,
        };
        let track_b_key = TrackKey {
            queue_item_id: fixture.song_b.queue_item_id,
            song_id: fixture.song_b.song_id,
        };

        // Assert pipeline active current is track A
        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| &a.current),
            Some(&track_a_key)
        );

        // Assert registry live contains track A and does NOT contain candidate B
        {
            let reg = fixture.harness.pipeline.registry.lock().unwrap();
            assert!(reg.live.iter().any(|b| b.key == track_a_key), "live must contain track A");
            assert!(!reg.live.iter().any(|b| b.key == track_b_key), "live must NOT contain candidate B");
            assert!(reg.preparing.is_empty(), "preparing must be empty after candidate failure");
            assert!(reg.retiring.is_empty(), "retiring must be empty after teardown");
        }

        let _ = fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    #[ignore]
    async fn forced_roll_commit_failure_rolls_back_candidate_and_preserves_live_and_active_coherence() {
        let mut harness = GstHarness::new().await;
        let (track_a, track_b, track_c) = harness.start_with_current_and_next().await;

        harness
            .pipeline
            .set_pre_commit_hook(move || Err(PipelineError::Pipeline("forced pre-commit hook failure".into())));

        let roll_plan = RollingPlan {
            generation: 1,
            current: track_a.key.clone(),
            change: RollingChange::ReplaceNext {
                expected_next: track_b.key.clone(),
                replacement: Some(PlannedNext {
                    track: track_c.clone(),
                    transition: TransitionPlan::Cut,
                }),
            },
        };

        let result = harness.pipeline.roll(roll_plan).await;
        assert!(result.is_err(), "roll must fail when pre_commit_hook fails");

        // Assert after rollback:
        // 1. live registry matches [A, B] exactly
        let reg = harness.pipeline.registry.lock().unwrap();
        let live_keys: Vec<_> = reg.live.iter().map(|b| b.key.clone()).collect();
        assert_eq!(live_keys, vec![track_a.key.clone(), track_b.key.clone()]);
        assert!(reg.preparing.is_empty(), "preparing must be empty after rollback");
        assert!(reg.retiring.is_empty(), "retiring must be empty after rollback");
        drop(reg);

        // 2. ActivePlan is coherent: current A, next B
        let active = harness.pipeline.active.lock().unwrap();
        assert_eq!(active.as_ref().map(|a| &a.current), Some(&track_a.key));
        assert_eq!(active.as_ref().and_then(|a| a.next.as_ref().map(|(k, _)| k)), Some(&track_b.key));
    }

    #[tokio::test]
    #[ignore]
    async fn handover_during_roll_rejection_leaves_registry_and_active_coherent() {
        let mut harness = GstHarness::new().await;
        let (track_a, track_b, track_c) = harness.start_with_current_and_next().await;

        let pipeline_weak = Arc::downgrade(&harness.pipeline);
        let b_key = track_b.key.clone();
        let b_metadata = track_b.metadata.clone();

        // Simulate concurrent Handover occurring right before roll's commit boundary
        harness.pipeline.set_pre_commit_hook(move || {
            let Some(pipeline) = pipeline_weak.upgrade() else { return Ok(()) };
            let mut active = pipeline.active.lock().unwrap();
            if let Some(a) = active.as_mut() {
                a.current = b_key.clone();
                a.current_metadata = b_metadata.clone();
                a.next = None;
                a.handed_over = true;
            }
            Ok(())
        });

        let roll_plan = RollingPlan {
            generation: 1,
            current: track_a.key.clone(),
            change: RollingChange::ReplaceNext {
                expected_next: track_b.key.clone(),
                replacement: Some(PlannedNext {
                    track: track_c.clone(),
                    transition: TransitionPlan::Cut,
                }),
            },
        };

        let result = harness.pipeline.roll(roll_plan).await;
        assert!(
            matches!(result, Err(PipelineError::StalePlan)),
            "roll must return StalePlan on concurrent handover"
        );

        // Assert after StalePlan rollback:
        // Candidate C is discarded, live registry and active remain coherent, branch B was NOT destroyed
        let reg = harness.pipeline.registry.lock().unwrap();
        assert!(!reg.live.iter().any(|b| b.key == track_c.key), "candidate C must not be in live");
        assert!(
            reg.live.iter().any(|b| b.key == track_b.key),
            "promoted branch B must still be in live"
        );
        assert!(reg.preparing.is_empty(), "preparing must be empty");
    }
    #[tokio::test]
    #[ignore]
    async fn exact_race_error_on_retiring_branch_before_pipeline_remove_is_suppressed() {
        let mut harness = GstHarness::new().await;
        let track_a = harness.track(Duration::from_secs(4), 8_000, 0);
        let track_b = harness.track(Duration::from_secs(4), -8_000, 1);
        let track_c = harness.track(Duration::from_secs(4), 4_000, 2);

        let initial_plan = PairPlan {
            generation: 1,
            output_epoch: 1,
            current: track_a.clone(),
            next: Some(PlannedNext {
                track: track_b.clone(),
                transition: TransitionPlan::Cut,
            }),
            mode: ReplaceMode::InitialReplaceFromStopped,
        };
        harness.pipeline.replace(initial_plan).await.unwrap();
        assert_eq!(harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);

        let pipeline_weak = Arc::downgrade(&harness.pipeline);
        let b_key = track_b.key.clone();
        let (hook_executed_tx, hook_executed_rx) = tokio::sync::oneshot::channel();
        let hook_executed_tx = std::sync::Mutex::new(Some(hook_executed_tx));

        // Hook executes inside discard_retired right after branch B was atomically retired from live,
        // before B elements are set to Null or removed from gst::Pipeline.
        harness.pipeline.set_teardown_hook(move |branch| {
            if branch.key == b_key {
                let Some(pipeline) = pipeline_weak.upgrade() else { return };

                // 1. Assert invariant: B is NOT in live branches
                let (is_live, is_retiring) = {
                    let reg = pipeline.registry.lock().unwrap();
                    let live = reg.live.iter().any(|b| b.key == b_key);
                    let retiring = reg.retiring.iter().any(|rb| rb.elements.iter().any(|e| e == &branch.source));
                    (live, retiring)
                };
                assert!(!is_live, "branch B must no longer be in live branches");
                // 2. Assert invariant: B IS in retiring branches
                assert!(is_retiring, "branch B must be present in retiring branches");
                // 3. Assert invariant: B.source is still a child/descendant of gst::Pipeline
                assert!(
                    bus::is_element_or_child(branch.source.upcast_ref(), pipeline.pipeline.upcast_ref()),
                    "branch B source must still be part of gst::Pipeline"
                );

                // 4. Post GstMessage::Error from branch B source element
                let error_msg = gst::message::Error::builder(
                    gst::StreamError::Failed,
                    "simulated error on retiring branch B before pipeline.remove",
                )
                .src(&branch.source)
                .build();
                let _ = pipeline.pipeline.bus().unwrap().post(error_msg);
                if let Some(tx) = hook_executed_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
            }
        });

        let roll_plan = RollingPlan {
            generation: 1,
            current: track_a.key.clone(),
            change: RollingChange::ReplaceNext {
                expected_next: track_b.key.clone(),
                replacement: Some(PlannedNext {
                    track: track_c.clone(),
                    transition: TransitionPlan::Cut,
                }),
            },
        };

        let result = harness.pipeline.roll(roll_plan).await;
        assert!(result.is_ok(), "roll must succeed despite error on retiring branch B");

        hook_executed_rx
            .await
            .expect("teardown hook must have executed for retiring branch B");

        // Verify:
        // 1. Retiring branch error did NOT emit FatalPipeline, DecodeFailed, or SinkDisconnected
        assert!(
            harness.try_recv_event().is_none(),
            "error from retiring branch during roll must be suppressed and emit no event"
        );
        // 2. Pipeline remains Playing with current track A and next track C
        assert_eq!(harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
        let active = harness.pipeline.active.lock().unwrap();
        assert_eq!(active.as_ref().map(|a| &a.current), Some(&track_a.key));
        assert_eq!(active.as_ref().and_then(|a| a.next.as_ref().map(|(k, _)| k)), Some(&track_c.key));
    }

    #[tokio::test]
    #[ignore]
    async fn active_replace_with_mismatched_output_epoch_is_rejected_as_stale_plan() {
        let mut harness = GstHarness::new().await;
        let track_a = harness.track(Duration::from_secs(4), 8_000, 0);
        let track_b = harness.track(Duration::from_secs(4), -8_000, 1);

        let initial_plan = PairPlan {
            generation: 1,
            output_epoch: 1,
            current: track_a.clone(),
            next: None,
            mode: ReplaceMode::InitialReplaceFromStopped,
        };
        harness.pipeline.replace(initial_plan).await.unwrap();

        assert_eq!(
            harness
                .pipeline
                .active
                .lock()
                .unwrap()
                .as_ref()
                .map(|a| (a.generation, a.output_epoch)),
            Some((1, 1))
        );

        let branches_before: Vec<_> = harness
            .pipeline
            .registry
            .lock()
            .unwrap()
            .live
            .iter()
            .map(|b| (b.generation, b.key.clone()))
            .collect();
        // ActiveReplace plan with generation 2, expected_generation 1 (CORRECT),
        // expected_current track_a (CORRECT), but output_epoch 2 (MISMATCHED - ONLY difference).
        let stale_epoch_plan = PairPlan {
            generation: 2,
            output_epoch: 2,
            current: track_b,
            next: None,
            mode: ReplaceMode::ActiveReplace {
                expected_generation: 1,
                expected_current: track_a.key.clone(),
            },
        };

        let result = harness.pipeline.replace(stale_epoch_plan).await;
        assert!(
            matches!(result, Err(PipelineError::StalePlan)),
            "ActiveReplace with mismatched output_epoch must be rejected as StalePlan, got {result:?}"
        );

        assert_eq!(
            *harness.pipeline.pending_epoch.lock().unwrap(),
            None,
            "pending_epoch must not be published for rejected stale plan"
        );
        assert_eq!(
            harness
                .pipeline
                .active
                .lock()
                .unwrap()
                .as_ref()
                .map(|a| (a.generation, a.output_epoch)),
            Some((1, 1)),
            "active plan must retain original generation and output_epoch"
        );
        let branches_after: Vec<_> = harness
            .pipeline
            .registry
            .lock()
            .unwrap()
            .live
            .iter()
            .map(|b| (b.generation, b.key.clone()))
            .collect();
        assert_eq!(
            branches_before, branches_after,
            "pipeline branches must not be mutated on rejected plan"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn real_stop_play_lifecycle_resets_pipeline_epoch_and_ignores_stale_fatal_error() {
        let fixture = start_real_runtime_with_two_tracks().await;

        // P1: playing in epoch 1
        assert_eq!(fixture.harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| a.output_epoch),
            Some(1)
        );

        // Stop P1 via FatalPipeline { pipeline_epoch: 1 } event which triggers real
        // PipelineOperation::Stop through the executor driver
        fixture
            .harness
            .pipeline
            .events
            .send(PipelineEvent::FatalPipeline {
                pipeline_epoch: 1,
                message: "fatal error stopping P1".into(),
            })
            .expect("events channel must be open");

        testsupport::wait_for_timeout(Duration::from_secs(5), "pipeline to be stopped and cleared", || {
            fixture.harness.pipeline.snapshot.lock().unwrap().state == PipelineState::Stopped
                && fixture.harness.pipeline.active.lock().unwrap().is_none()
                && fixture.harness.pipeline.registry.lock().unwrap().live.is_empty()
        })
        .await;

        // runtime.play() executes real InitialReplaceFromStopped in epoch 2
        fixture.runtime.play().await.unwrap();

        testsupport::wait_for_timeout(Duration::from_secs(5), "pipeline to be playing in epoch 2", || {
            fixture.harness.pipeline.snapshot.lock().unwrap().state == PipelineState::Playing
                && fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| a.output_epoch) == Some(2)
        })
        .await;

        // Deliver stale FatalPipeline event from old epoch 1 followed by deterministic TestBarrier
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        fixture
            .harness
            .pipeline
            .events
            .send(PipelineEvent::FatalPipeline {
                pipeline_epoch: 1,
                message: "stale fatal error from previous pipeline epoch 1".into(),
            })
            .expect("events channel must be open");
        fixture
            .harness
            .pipeline
            .events
            .send(PipelineEvent::TestBarrier(notify.clone()))
            .expect("events channel must be open");

        // Deterministically await event loop processing of the stale FatalPipeline event
        notify.notified().await;

        // P2 must remain Playing and stale event was ignored
        assert_eq!(
            fixture.runtime.state().await.unwrap(),
            PipelineState::Playing,
            "runtime must remain Playing in epoch 2 after stale event"
        );
        assert_eq!(fixture.harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Playing);
        assert_eq!(
            fixture.harness.pipeline.active.lock().unwrap().as_ref().map(|a| a.output_epoch),
            Some(2)
        );

        // Current epoch 2 FatalPipeline event MUST stop the station
        fixture
            .harness
            .pipeline
            .events
            .send(PipelineEvent::FatalPipeline {
                pipeline_epoch: 2,
                message: "fatal error from current pipeline epoch 2".into(),
            })
            .expect("events channel must be open");

        testsupport::wait_for_async_timeout(
            Duration::from_secs(5),
            "runtime to stop after current epoch fatal error",
            || async {
                matches!(
                    fixture.runtime.status().await,
                    Ok(crate::streamer::StatusEvent::State { playing: false, .. })
                )
            },
        )
        .await;

        assert_eq!(fixture.harness.pipeline.snapshot.lock().unwrap().state, PipelineState::Stopped);

        let _ = fixture.runtime.shutdown().await;
    }
    #[tokio::test]
    async fn backbone_encoder_and_mixer_errors_emit_fatal_pipeline() {
        let mut harness = GstHarness::start_playing(1).await;
        let encoder = harness.pipeline.encoder_element();
        harness.post_error(&encoder, "encoder internal failure");

        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 1, ref message } if message.contains("encoder internal failure")),
            "expected FatalPipeline event for encoder error, got {event:?}"
        );

        let mixer = harness.pipeline.mixer_element();
        harness.post_error(&mixer, "mixer buffer drop");
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 1, ref message } if message.contains("mixer buffer drop")),
            "expected FatalPipeline event for mixer error, got {event:?}"
        );
    }

    #[tokio::test]
    async fn error_classification_tracks_current_sink_across_reconnect() {
        let mut harness = GstHarness::start_playing(1).await;
        let old_sink = harness.pipeline.current_sink_element();

        // Reconnect to a new target to replace the sink element
        let target = testsupport::target();
        harness.pipeline.reconnect(target).await.unwrap();

        let new_sink = harness.pipeline.current_sink_element();
        assert_ne!(old_sink, new_sink, "sink element must have been replaced");

        // Old sink error must NOT generate SinkDisconnected
        harness.post_error(&old_sink, "stale sink late error");
        assert!(
            harness.try_recv_event().is_none(),
            "stale sink error must not generate SinkDisconnected"
        );

        // New active sink error MUST generate SinkDisconnected
        harness.post_error(&new_sink, "new sink disconnected");
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 1, output_epoch: 1, ref message } if message.contains("new sink disconnected")),
            "expected SinkDisconnected from active replacement sink, got {event:?}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn old_sink_error_is_suppressed_during_reconnect_and_stale_afterward() {
        let mut harness = GstHarness::start_playing(1).await;
        let old_sink = harness.pipeline.current_sink_element();

        // 1. Enter output reconnect suppression (as occurs during reconnect())
        let guard = harness.pipeline.suppress_output_events();

        // 2. An error arrives from the old sink while reconnect is active
        harness.post_error(&old_sink, "old sink network drop during reconnect");
        assert!(
            harness.try_recv_event().is_none(),
            "error during active reconnect must be suppressed"
        );

        // 3. Reconnect completes successfully, replacing old_sink with new_sink
        drop(guard);
        let target = testsupport::target();
        harness.pipeline.reconnect(target).await.unwrap();

        let new_sink = harness.pipeline.current_sink_element();
        assert_ne!(old_sink, new_sink, "sink must be replaced");

        // 4. Any subsequent error from the old stale sink must be rejected
        harness.post_error(&old_sink, "old sink after reconnect completed");
        assert!(
            harness.try_recv_event().is_none(),
            "stale old sink error must not trigger SinkDisconnected after reconnect"
        );

        // 5. New active sink error MUST generate SinkDisconnected
        harness.post_error(&new_sink, "new active sink dropped");
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 1, output_epoch: 1, ref message } if message.contains("new active sink dropped")),
            "expected SinkDisconnected from new active sink, got {event:?}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn failed_sink_replacement_rollback_restores_old_sink_and_clears_suppression() {
        let mut harness = GstHarness::start_playing(1).await;
        let old_sink = harness.pipeline.current_sink_element();

        // 1. Enter output reconnect suppression via RAII guard (as done in reconnect())
        let guard = harness.pipeline.suppress_output_events();
        assert!(
            harness.pipeline.sink.lock().unwrap().reconnecting,
            "suppression must be active during reconnect"
        );

        // 2. Set up a replacing candidate
        let dummy_candidate = gst::ElementFactory::make("fakesink").name("failing-candidate").build().unwrap();
        harness.pipeline.gst_pipeline().add(&dummy_candidate).unwrap();
        harness.pipeline.clock_gate.unlink(&old_sink);
        harness.pipeline.clock_gate.link(&dummy_candidate).unwrap();
        harness.pipeline.sink.lock().unwrap().slot = SinkSlot::Replacing {
            old_sink: old_sink.clone(),
            candidate: dummy_candidate.clone(),
        };

        // 3. Rollback the replacement to old_sink
        harness
            .pipeline
            .rollback_sink_replacement(&old_sink, &dummy_candidate, PipelineState::Playing)
            .unwrap();
        assert_eq!(harness.pipeline.current_sink_element(), old_sink, "old sink must be restored");

        // 4. Drop the reconnect guard, which must clear suppression
        drop(guard);
        assert!(
            !harness.pipeline.sink.lock().unwrap().reconnecting,
            "suppression must be cleared after guard drop"
        );

        // 5. Candidate error must NOT trigger SinkDisconnected
        harness.post_error(&dummy_candidate, "candidate dropped after rollback");
        assert!(
            harness.try_recv_event().is_none(),
            "failed candidate must not trigger SinkDisconnected"
        );

        // 6. Restored old sink error MUST trigger SinkDisconnected
        harness.post_error(&old_sink, "restored old sink error");
        let event = harness.next_event().await;
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 1, output_epoch: 1, ref message } if message.contains("restored old sink error")),
            "expected SinkDisconnected from restored old sink, got {event:?}"
        );
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
        let current = harness.track(Duration::from_secs(5), 0, 0);
        harness
            .pipeline
            .replace(initial_plan(1, current, None, TransitionPlan::Cut))
            .await
            .unwrap();
        harness.pipeline.set_playing(false).await.unwrap();
        harness.pipeline.reconnect(target.clone()).await.unwrap();
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Paused);
        harness.pipeline.set_playing(true).await.unwrap();
        assert_eq!(harness.pipeline.snapshot().await.unwrap().state, PipelineState::Playing);
        harness.stop().await;

        let mut harness = GstHarness::new().await;
        let current = harness.track(Duration::from_secs(5), 0, 0);
        harness
            .pipeline
            .replace(initial_plan(1, current, None, TransitionPlan::Cut))
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
        let current = harness.track(Duration::from_secs(1), 8_000, 0);
        let next = harness.track(Duration::from_secs(1), -8_000, 1);
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
    #[ignore]
    async fn schedules_each_replacement_on_its_own_running_time() {
        let mut harness = GstHarness::new().await;
        let first = harness.track(Duration::from_secs(1), 8_000, 0);
        let second = harness.track(Duration::from_secs(1), -8_000, 1);
        let third = harness.track(Duration::from_secs(1), 4_000, 2);
        let fourth = harness.track(Duration::from_secs(1), -4_000, 3);
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
    #[ignore]
    async fn applies_autocue_seeks_before_the_clocked_handover() {
        let mut harness = GstHarness::new().await;
        let current = harness.track(Duration::from_millis(1_500), 8_000, 0);
        let next = harness.track(Duration::from_millis(1_500), -8_000, 1);
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
    async fn rolling_replace_next_swaps_only_the_terminal_branch() {
        let mut harness = GstHarness::new().await;
        let first_file = harness.wav(Duration::from_secs(2), 8_000);
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
        let pending_epoch: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
        let metadata_target = Arc::new(Mutex::new(
            IcecastTarget::parse(&format!("127.0.0.1:{metadata_port}"), "secret".into(), "test", "test".into()).unwrap(),
        ));
        let metadata_publisher = Some(sink::MetadataPublisher::spawn());
        let sink_state = Arc::new(Mutex::new(SinkState::new(sink)));
        let registry = Arc::new(Mutex::new(branch::BranchRegistry::new()));
        bus::install(
            &pipeline,
            &clock_gate,
            sink_state.clone(),
            registry.clone(),
            metadata_target.clone(),
            metadata_publisher.clone(),
            active.clone(),
            replacing.clone(),
            pending_epoch.clone(),
            events.clone(),
        )
        .expect("bus installed");
        let pipeline = GStreamerPipeline {
            pipeline,
            mixer,
            output_queue,
            output_caps,
            encoder,
            sink: sink_state,
            metadata_target,
            metadata_publisher,
            clock_gate,
            sink_factory: "fakesink",
            registry,
            active,
            replacing,
            pending_epoch,
            snapshot: Mutex::new(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            }),
            events,
            #[cfg(test)]
            pre_commit_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            post_commit_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            teardown_hook: Arc::new(Mutex::new(None)),
        };
        let mut pipeline_events = receiver;
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
        tokio::time::timeout(Duration::from_secs(3), stub.join())
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
    #[ignore]
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
