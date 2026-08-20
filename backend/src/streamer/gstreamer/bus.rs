use std::sync::{Arc, Mutex};

use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::super::pipeline::{IcecastTarget, PipelineError, PipelineEvent, TrackKey};
use super::branch::{Branch, BranchRegistry};
use super::sink::MetadataPublisher;
use super::{ActivePlan, ReplaceCancellation, SinkSlot, SinkState};

pub(super) fn is_element_or_child(src: &gst::Object, element: &gst::Element) -> bool {
    src == element.upcast_ref::<gst::Object>() || src.has_as_ancestor(element)
}

pub(super) fn is_sink_element(src: Option<&gst::Object>, sink_state: &SinkState) -> bool {
    let Some(src) = src else {
        return false;
    };
    match &sink_state.slot {
        SinkSlot::Active(active) => is_element_or_child(src, active),
        SinkSlot::Replacing { old_sink, candidate } => is_element_or_child(src, old_sink) || is_element_or_child(src, candidate),
    }
}

pub(super) fn find_branch_for_element(src: Option<&gst::Object>, branches: &[Branch]) -> Option<(u64, TrackKey)> {
    let src = src?;
    branches
        .iter()
        .find(|branch| branch.elements.iter().any(|elem| is_element_or_child(src, elem)))
        .map(|branch| (branch.generation, branch.key.clone()))
}

pub(super) fn find_preparing_for_element(
    src: Option<&gst::Object>,
    preparing: &[super::branch::PreparingBranch],
) -> Option<(u64, TrackKey)> {
    let src = src?;
    preparing
        .iter()
        .find(|branch| branch.elements.iter().any(|elem| is_element_or_child(src, elem)))
        .map(|branch| (branch.generation, branch.key.clone()))
}
pub(super) fn handle_error_message(
    src: Option<&gst::Object>,
    error_message: &str,
    pipeline: Option<&gst::Pipeline>,
    sink: &Mutex<SinkState>,
    registry: &Mutex<BranchRegistry>,
    active: &Mutex<Option<ActivePlan>>,
    replacing: &Mutex<Option<ReplaceCancellation>>,
    pending_epoch: &Mutex<Option<u64>>,
    events: &mpsc::UnboundedSender<PipelineEvent>,
) -> bool {
    handle_error_message_inner(
        src,
        error_message,
        pipeline,
        sink,
        registry,
        active,
        replacing,
        pending_epoch,
        events,
        |_| (),
    )
}

pub(super) fn handle_error_message_inner(
    src: Option<&gst::Object>,
    error_message: &str,
    pipeline: Option<&gst::Pipeline>,
    sink: &Mutex<SinkState>,
    registry: &Mutex<BranchRegistry>,
    active: &Mutex<Option<ActivePlan>>,
    replacing: &Mutex<Option<ReplaceCancellation>>,
    pending_epoch: &Mutex<Option<u64>>,
    events: &mpsc::UnboundedSender<PipelineEvent>,
    #[allow(unused_variables)] on_classified: impl FnOnce(&SinkState),
) -> bool {
    let state = sink.lock().unwrap_or_else(|error| error.into_inner());
    if is_sink_element(src, &state) {
        if state.reconnecting {
            tracing::debug!(
                src = ?src.map(|s| s.name().to_string()),
                error = %error_message,
                "sink error suppressed during reconnect"
            );
            return false;
        }

        on_classified(&state);

        if let Some(active) = active.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
            let _ = events.send(PipelineEvent::SinkDisconnected {
                generation: active.generation,
                output_epoch: active.output_epoch,
                message: error_message.to_string(),
            });
            return true;
        }
        return false;
    }
    drop(state);

    let (branch_info, is_retiring) = {
        let mut reg = registry.lock().unwrap_or_else(|error| error.into_inner());
        let mut branch_info = find_branch_for_element(src, &reg.live);
        if branch_info.is_some() {
            if let Some(replacing) = replacing.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
                replacing.cancel();
            }
        } else {
            let prep = find_preparing_for_element(src, &reg.preparing);
            if let Some((_, ref key)) = prep {
                reg.mark_preparing_failed(key);
                if let Some(replacing) = replacing.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
                    replacing.cancel();
                }
            }
            branch_info = prep;
        }
        let is_retiring = src.is_some_and(|src| {
            reg.retiring
                .iter()
                .any(|b| b.elements.iter().any(|elem| is_element_or_child(src, elem)))
        });
        (branch_info, is_retiring)
    };
    if let Some((generation, track)) = branch_info {
        let _ = events.send(PipelineEvent::DecodeFailed {
            generation,
            track,
            message: error_message.to_string(),
        });
        return true;
    }

    if is_retiring {
        tracing::debug!(
            src = ?src.map(|s| s.name().to_string()),
            error = %error_message,
            "error from retiring branch suppressed"
        );
        return true;
    }
    let pending_epoch = *pending_epoch.lock().unwrap_or_else(|error| error.into_inner());
    let active_epoch = active
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .as_ref()
        .map(|a| a.output_epoch);

    let Some(pipeline_epoch) = active_epoch.or(pending_epoch) else {
        tracing::warn!(
            src = ?src.map(|s| s.name().to_string()),
            error = %error_message,
            "pipeline error ignored while inactive"
        );
        return false;
    };

    if pipeline.is_none_or(|p| src.is_some_and(|s| is_element_or_child(s, p.upcast_ref()))) {
        let _ = events.send(PipelineEvent::FatalPipeline {
            pipeline_epoch,
            message: error_message.to_string(),
        });
        true
    } else {
        tracing::warn!(
            src = ?src.map(|s| s.name().to_string()),
            error = %error_message,
            "error from non-pipeline element ignored"
        );
        false
    }
}

pub(super) fn install(
    pipeline: &gst::Pipeline,
    clock_gate: &gst::Element,
    sink: Arc<Mutex<SinkState>>,
    registry: Arc<Mutex<BranchRegistry>>,
    metadata_target: Arc<Mutex<IcecastTarget>>,
    metadata_publisher: Option<MetadataPublisher>,
    active: Arc<Mutex<Option<ActivePlan>>>,
    replacing: Arc<Mutex<Option<ReplaceCancellation>>>,
    pending_epoch: Arc<Mutex<Option<u64>>>,
    events: mpsc::UnboundedSender<PipelineEvent>,
) -> Result<(), PipelineError> {
    let handover_active = active.clone();
    let handover_target = metadata_target;
    let handover_publisher = metadata_publisher;
    let handover_events = events.clone();
    let clock_gate_src = clock_gate
        .static_pad("src")
        .ok_or_else(|| PipelineError::Pipeline("clock gate has no source pad".into()))?;
    clock_gate_src.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
        let Some(gst::PadProbeData::Buffer(buffer)) = &info.data else {
            return gst::PadProbeReturn::Ok;
        };
        let handover = {
            let mut active = handover_active.lock().unwrap_or_else(|error| error.into_inner());
            active.as_mut().and_then(|plan| {
                let due = buffer.pts().is_some_and(|pts| {
                    let started_at = *plan.started_at.get_or_insert(pts);
                    let elapsed = pts.saturating_sub(started_at);
                    plan.last_elapsed = elapsed;
                    plan.handover_at.is_some_and(|handover_at| elapsed >= handover_at)
                });
                if due && !plan.handed_over {
                    plan.handed_over = true;
                    plan.next.take().map(|(next, metadata)| {
                        plan.current = next.clone();
                        plan.current_metadata = metadata.clone();
                        plan.current_epoch = plan.last_elapsed;
                        (plan.generation, next, metadata)
                    })
                } else {
                    None
                }
            })
        };
        if let Some((generation, current, metadata)) = handover {
            if let Some(publisher) = &handover_publisher {
                let target = handover_target.lock().unwrap_or_else(|error| error.into_inner()).clone();
                publisher.publish(target, metadata);
            }
            let _ = handover_events.send(PipelineEvent::Handover { generation, current });
        }
        gst::PadProbeReturn::Ok
    });

    let eos_active = active.clone();
    let eos_replacing = replacing.clone();
    let eos_events = events.clone();
    clock_gate_src.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |_, info| {
        if let Some(gst::PadProbeData::Event(event)) = &info.data {
            if event.type_() == gst::EventType::Eos {
                if let Some(plan) = eos_active.lock().unwrap_or_else(|error| error.into_inner()).clone() {
                    if let Some(replacing) = eos_replacing.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
                        replacing.cancel_if_matches(plan.generation, &plan.current);
                    }
                    let _ = eos_events.send(PipelineEvent::CurrentEos {
                        generation: plan.generation,
                        current: plan.current,
                    });
                }
            }
        }
        gst::PadProbeReturn::Ok
    });

    let bus = pipeline
        .bus()
        .ok_or_else(|| PipelineError::Pipeline("pipeline has no bus".into()))?;
    let bus_active = active.clone();
    let bus_replacing = replacing.clone();
    let bus_registry = registry;
    let bus_pending_epoch = pending_epoch;
    let bus_pipeline = pipeline.clone();
    bus.set_sync_handler(move |_, message| {
        if let gst::MessageView::Error(error) = message.view() {
            handle_error_message(
                message.src(),
                &error.error().to_string(),
                Some(&bus_pipeline),
                &sink,
                &bus_registry,
                &bus_active,
                &bus_replacing,
                &bus_pending_epoch,
                &events,
            );
        }
        gst::BusSyncReply::Pass
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamer::gstreamer::branch::RetiringBranch;
    use crate::streamer::pipeline::{TrackKey, TrackMetadata};

    fn test_active_plan(generation: u64, output_epoch: u64) -> ActivePlan {
        ActivePlan {
            generation,
            output_epoch,
            current: TrackKey {
                queue_item_id: uuid::Uuid::new_v4(),
                song_id: uuid::Uuid::new_v4(),
            },
            current_metadata: TrackMetadata {
                title: "Test".into(),
                artist: "Test".into(),
            },
            timeline_origin: gst::ClockTime::ZERO,
            current_epoch: gst::ClockTime::ZERO,
            started_at: None,
            handover_at: None,
            handed_over: false,
            last_elapsed: gst::ClockTime::ZERO,
            next: None,
        }
    }

    #[test]
    fn sink_element_matches_active_sink_and_its_pads() {
        gst::init().unwrap();
        let sink = gst::ElementFactory::make("fakesink").build().unwrap();
        let other = gst::ElementFactory::make("fakesink").build().unwrap();
        let state = SinkState {
            slot: SinkSlot::Active(sink.clone()),
            reconnecting: false,
        };

        assert!(is_sink_element(Some(sink.upcast_ref()), &state));
        let pad = sink.static_pad("sink").expect("fakesink must expose static sink pad");
        assert!(is_sink_element(Some(pad.upcast_ref()), &state));
        assert!(!is_sink_element(Some(other.upcast_ref()), &state));
        assert!(!is_sink_element(None, &state));
    }

    #[test]
    fn sink_element_matches_replacing_sinks_and_rejects_unrelated() {
        gst::init().unwrap();
        let old_sink = gst::ElementFactory::make("fakesink").build().unwrap();
        let candidate = gst::ElementFactory::make("fakesink").build().unwrap();
        let other = gst::ElementFactory::make("fakesink").build().unwrap();
        let state = SinkState {
            slot: SinkSlot::Replacing {
                old_sink: old_sink.clone(),
                candidate: candidate.clone(),
            },
            reconnecting: false,
        };

        assert!(is_sink_element(Some(old_sink.upcast_ref()), &state));
        assert!(is_sink_element(Some(candidate.upcast_ref()), &state));
        assert!(!is_sink_element(Some(other.upcast_ref()), &state));
        assert!(!is_sink_element(None, &state));
    }

    #[test]
    fn handle_error_message_routes_by_lifecycle_state() {
        gst::init().unwrap();
        let pipeline = gst::Pipeline::new();
        let sink = gst::ElementFactory::make("fakesink").build().unwrap();
        let other = gst::ElementFactory::make("fakesink").build().unwrap();
        let branch_elem = gst::ElementFactory::make("identity").build().unwrap();
        pipeline.add_many([&sink, &other, &branch_elem]).unwrap();
        let branch_key = TrackKey {
            queue_item_id: uuid::Uuid::new_v4(),
            song_id: uuid::Uuid::new_v4(),
        };
        let branch = Branch::for_test(
            3,
            branch_key.clone(),
            vec![branch_elem.clone()],
            branch_elem.clone(),
            branch_elem.clone(),
            branch_elem.static_pad("src").unwrap(),
        );
        let registry = Mutex::new(BranchRegistry::new());
        registry.lock().unwrap().live.push(branch);
        let sink_state = Mutex::new(SinkState {
            slot: SinkSlot::Active(sink.clone()),
            reconnecting: false,
        });
        let active = Mutex::new(Some(test_active_plan(3, 2)));
        let pending_epoch = Mutex::new(None);
        let replacing = Mutex::new(None);
        let (events, mut rx) = mpsc::unbounded_channel();

        // 1. Active sink outside suppression -> returns true and emits SinkDisconnected
        assert!(handle_error_message(
            Some(sink.upcast_ref()),
            "test error",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events,
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 3, output_epoch: 2, ref message } if message == "test error")
        );

        // 2. Media branch element -> returns true and emits DecodeFailed
        assert!(handle_error_message(
            Some(branch_elem.upcast_ref()),
            "decode corrupt",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::DecodeFailed { generation: 3, ref track, ref message } if track == &branch_key && message == "decode corrupt")
        );
        // 3. Backbone element -> returns true and emits FatalPipeline
        assert!(handle_error_message(
            Some(other.upcast_ref()),
            "mixer failed",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events
        ));
        let _event = rx.try_recv().expect("event must be sent");

        // 4. Active sink during reconnect suppression -> returns false (logged as suppressed sink error) and sends no event
        sink_state.lock().unwrap().reconnecting = true;
        assert!(!handle_error_message(
            Some(sink.upcast_ref()),
            "suppressed error",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events
        ));

        // 5. After replacement -> old sink is no longer a sink element; if removed from pipeline, it is ignored
        sink_state.lock().unwrap().slot = SinkSlot::Active(other.clone());
        sink_state.lock().unwrap().reconnecting = false;
        let _ = pipeline.remove(&sink);
        assert!(!handle_error_message(
            Some(sink.upcast_ref()),
            "stale sink error",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events
        ));
        // 6. New active sink emits error -> returns true and sends SinkDisconnected
        assert!(handle_error_message(
            Some(other.upcast_ref()),
            "new sink error",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 3, output_epoch: 2, ref message } if message == "new sink error")
        );

        // 7. Branch error uses branch's own generation even if active generation is different
        *active.lock().unwrap() = Some(test_active_plan(99, 1));
        assert!(handle_error_message(
            Some(branch_elem.upcast_ref()),
            "decode corrupt on branch gen 3",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::DecodeFailed { generation: 3, ref track, ref message } if track == &branch_key && message == "decode corrupt on branch gen 3"),
            "branch error must preserve branch's generation 3, not active generation 99, got {event:?}"
        );

        // 8. Backbone error when active is epoch 2 and pending_epoch is 42 -> active epoch 2 is used
        let mixer_elem = gst::ElementFactory::make("identity").build().unwrap();
        pipeline.add(&mixer_elem).unwrap();
        *active.lock().unwrap() = Some(test_active_plan(3, 2));
        *pending_epoch.lock().unwrap() = Some(42);
        assert!(handle_error_message(
            Some(mixer_elem.upcast_ref()),
            "mixer failed during replace while active",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events,
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 2, ref message } if message == "mixer failed during replace while active"),
            "backbone error during active session must carry active epoch 2, got {event:?}"
        );
        // 9. Backbone error when active is None but pending_epoch is Some(42)
        *active.lock().unwrap() = None;
        *pending_epoch.lock().unwrap() = Some(42);
        assert!(handle_error_message(
            Some(mixer_elem.upcast_ref()),
            "mixer failed during initial replace",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events,
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 42, ref message } if message == "mixer failed during initial replace"),
            "backbone error during pending initial replace must carry pending epoch 42, got {event:?}"
        );
        *pending_epoch.lock().unwrap() = None;
        assert!(!handle_error_message(
            Some(mixer_elem.upcast_ref()),
            "spurious error while inactive",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events
        ));
        assert!(rx.try_recv().is_err(), "inactive error must not emit any event");
        // 11. Backbone error when active is epoch 2 and pending_epoch is None (e.g. stale replace rejected) -> uses active epoch 2
        *active.lock().unwrap() = Some(test_active_plan(3, 2));
        *pending_epoch.lock().unwrap() = None;
        assert!(handle_error_message(
            Some(mixer_elem.upcast_ref()),
            "mixer failed on active epoch 2 without pending operation",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events,
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::FatalPipeline { pipeline_epoch: 2, ref message } if message == "mixer failed on active epoch 2 without pending operation"),
            "backbone error on active epoch 2 without pending operation must carry active epoch 2, got {event:?}"
        );
        // 12. Retiring branch error -> returns true and emits NO event
        let retiring_elem = gst::ElementFactory::make("identity").build().unwrap();
        pipeline.add(&retiring_elem).unwrap();
        registry.lock().unwrap().retiring.push(RetiringBranch {
            retirement_id: 1,
            elements: vec![retiring_elem.clone()],
        });
        assert!(handle_error_message(
            Some(retiring_elem.upcast_ref()),
            "retiring branch error during teardown",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events,
        ));

        // 13. Preparing branch error -> marks preparing failed, cancels replacing, emits DecodeFailed
        let prep_elem = gst::ElementFactory::make("identity").build().unwrap();
        pipeline.add(&prep_elem).unwrap();
        let prep_key = TrackKey {
            queue_item_id: uuid::Uuid::new_v4(),
            song_id: uuid::Uuid::new_v4(),
        };
        let prep_branch = Branch::for_test(
            4,
            prep_key.clone(),
            vec![prep_elem.clone()],
            prep_elem.clone(),
            prep_elem.clone(),
            prep_elem.static_pad("src").unwrap(),
        );
        registry.lock().unwrap().register_preparing(&prep_branch);
        let cancel_token = ReplaceCancellation {
            expected_generation: 3,
            expected_current: branch_key.clone(),
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        *replacing.lock().unwrap() = Some(cancel_token.clone());
        assert!(handle_error_message(
            Some(prep_elem.upcast_ref()),
            "preparing candidate decode corrupt",
            Some(&pipeline),
            &sink_state,
            &registry,
            &active,
            &replacing,
            &pending_epoch,
            &events,
        ));
        assert!(
            registry.lock().unwrap().is_preparing_failed(&prep_key),
            "preparing branch must be marked as failed"
        );
        assert!(cancel_token.is_cancelled(), "replacing token must be cancelled");
        let event = rx.try_recv().expect("DecodeFailed must be emitted");
        assert!(
            matches!(event, PipelineEvent::DecodeFailed { generation: 4, ref track, ref message } if track == &prep_key && message == "preparing candidate decode corrupt")
        );
    }

    #[test]
    fn handle_error_message_decision_and_emission_is_atomic_against_concurrent_reconnect() {
        gst::init().unwrap();
        let old_sink = gst::ElementFactory::make("fakesink").build().unwrap();
        let new_sink = gst::ElementFactory::make("fakesink").build().unwrap();
        let sink_state = Arc::new(Mutex::new(SinkState {
            slot: SinkSlot::Active(old_sink.clone()),
            reconnecting: false,
        }));
        let active = Arc::new(Mutex::new(Some(test_active_plan(1, 1))));
        let (events, mut rx) = mpsc::unbounded_channel();
        let (classified_tx, classified_rx) = std::sync::mpsc::channel();
        let (continue_tx, continue_rx) = std::sync::mpsc::channel();

        let handler_sink = sink_state.clone();
        let handler_active = active.clone();
        let handler_old_sink = old_sink.clone();
        let handler_events = events.clone();

        let pipeline = gst::Pipeline::new();
        pipeline.add(&old_sink).unwrap();
        let handler_pipeline = pipeline.clone();
        let handler_thread = std::thread::spawn(move || {
            let handler_registry = Mutex::new(BranchRegistry::new());
            let handler_replacing = Mutex::new(None);
            let handler_pending_epoch = Mutex::new(None);
            handle_error_message_inner(
                Some(handler_old_sink.upcast_ref()),
                "old sink dropped",
                Some(&handler_pipeline),
                &handler_sink,
                &handler_registry,
                &handler_active,
                &handler_replacing,
                &handler_pending_epoch,
                &handler_events,
                |_state| {
                    classified_tx.send(()).unwrap();
                    // Wait for the concurrent thread to prove it cannot enter SinkState
                    continue_rx.recv().unwrap();
                },
            )
        });

        // 1. Wait until handler thread has classified the error and is paused in critical section
        classified_rx.recv().unwrap();

        // 2. PROOF OF ATOMICITY: The concurrent thread attempts to access/mutate SinkState
        // Because the handler holds the SinkState mutex lock, try_lock MUST fail!
        assert!(
            matches!(sink_state.try_lock(), Err(std::sync::TryLockError::WouldBlock)),
            "concurrent context must be blocked from acquiring SinkState during decision->commit"
        );

        // 3. Signal handler thread to complete commit and drop SinkState lock
        continue_tx.send(()).unwrap();

        let handled = handler_thread.join().unwrap();
        assert!(handled, "error handler should have committed successfully");

        // 4. Verify that SinkDisconnected was emitted for the old sink
        let event = rx.try_recv().expect("SinkDisconnected must be emitted");
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 1, output_epoch: 1, ref message } if message == "old sink dropped")
        );

        {
            let mut state = sink_state.lock().unwrap();
            state.slot = SinkSlot::Active(new_sink.clone());
            state.reconnecting = false;
            let _ = pipeline.remove(&old_sink);
        }
        let check_registry = Mutex::new(BranchRegistry::new());
        let check_replacing = Mutex::new(None);
        let check_pending_epoch = Mutex::new(None);
        assert!(!handle_error_message(
            Some(old_sink.upcast_ref()),
            "old sink second error",
            Some(&pipeline),
            &sink_state,
            &check_registry,
            &active,
            &check_replacing,
            &check_pending_epoch,
            &events,
        ));
    }
}
