use std::sync::{Arc, Mutex};

use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::super::pipeline::{IcecastTarget, PipelineError, PipelineEvent};
use super::{sink::MetadataPublisher, ActivePlan, ReplaceCancellation, SinkSlot, SinkState};

fn is_element_or_child(src: &gst::Object, element: &gst::Element) -> bool {
    src == element.upcast_ref::<gst::Object>() || src.has_as_ancestor(element)
}

pub(super) fn is_sink_element(src: Option<&gst::Object>, sink_state: &SinkState) -> bool {
    let Some(src) = src else {
        return false;
    };
    match &sink_state.slot {
        SinkSlot::Active(sink) => is_element_or_child(src, sink),
        SinkSlot::Replacing { old_sink, candidate } => is_element_or_child(src, candidate) || is_element_or_child(src, old_sink),
    }
}

pub(super) fn handle_error_message(
    src: Option<&gst::Object>,
    error_message: &str,
    sink: &Mutex<SinkState>,
    active: &Mutex<Option<ActivePlan>>,
    events: &mpsc::UnboundedSender<PipelineEvent>,
) -> bool {
    handle_error_message_inner(src, error_message, sink, active, events, |_| ())
}

pub(super) fn handle_error_message_inner(
    src: Option<&gst::Object>,
    error_message: &str,
    sink: &Mutex<SinkState>,
    active: &Mutex<Option<ActivePlan>>,
    events: &mpsc::UnboundedSender<PipelineEvent>,
    #[allow(unused_variables)] on_classified: impl FnOnce(&SinkState),
) -> bool {
    let state = sink.lock().unwrap_or_else(|error| error.into_inner());
    if !is_sink_element(src, &state) {
        tracing::warn!(
            src = ?src.map(|s| s.name().to_string()),
            error = %error_message,
            "non-sink pipeline error ignored for output disconnection"
        );
        return false;
    }

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
    false
}

pub(super) fn install(
    pipeline: &gst::Pipeline,
    clock_gate: &gst::Element,
    sink: Arc<Mutex<SinkState>>,
    metadata_target: Arc<Mutex<IcecastTarget>>,
    metadata_publisher: Option<MetadataPublisher>,
    active: Arc<Mutex<Option<ActivePlan>>>,
    replacing: Arc<Mutex<Option<ReplaceCancellation>>>,
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
    bus.set_sync_handler(move |_, message| {
        if let gst::MessageView::Error(error) = message.view() {
            handle_error_message(message.src(), &error.error().to_string(), &sink, &active, &events);
        }
        gst::BusSyncReply::Pass
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let sink = gst::ElementFactory::make("fakesink").build().unwrap();
        let other = gst::ElementFactory::make("fakesink").build().unwrap();
        let sink_state = Mutex::new(SinkState {
            slot: SinkSlot::Active(sink.clone()),
            reconnecting: false,
        });
        let active = Mutex::new(Some(test_active_plan(3, 2)));
        let (events, mut rx) = mpsc::unbounded_channel();

        // 1. Active sink outside suppression -> returns true and emits SinkDisconnected
        assert!(handle_error_message(
            Some(sink.upcast_ref()),
            "test error",
            &sink_state,
            &active,
            &events
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 3, output_epoch: 2, ref message } if message == "test error")
        );

        // 2. Unrelated element -> returns false (logged as non-sink) and sends no event
        assert!(!handle_error_message(
            Some(other.upcast_ref()),
            "other error",
            &sink_state,
            &active,
            &events
        ));
        assert!(rx.try_recv().is_err());

        // 3. Active sink during reconnect suppression -> returns false (logged as suppressed sink error, NOT non-sink) and sends no event
        sink_state.lock().unwrap().reconnecting = true;
        assert!(!handle_error_message(
            Some(sink.upcast_ref()),
            "suppressed error",
            &sink_state,
            &active,
            &events
        ));
        assert!(rx.try_recv().is_err());

        // 4. After replacement -> old sink is no longer a sink element, returns false
        sink_state.lock().unwrap().slot = SinkSlot::Active(other.clone());
        sink_state.lock().unwrap().reconnecting = false;
        assert!(!handle_error_message(
            Some(sink.upcast_ref()),
            "stale sink error",
            &sink_state,
            &active,
            &events
        ));
        assert!(rx.try_recv().is_err());

        // 5. New active sink emits error -> returns true and sends event
        assert!(handle_error_message(
            Some(other.upcast_ref()),
            "new sink error",
            &sink_state,
            &active,
            &events
        ));
        let event = rx.try_recv().expect("event must be sent");
        assert!(
            matches!(event, PipelineEvent::SinkDisconnected { generation: 3, output_epoch: 2, ref message } if message == "new sink error")
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

        // Deterministic channel coordination between handler thread and concurrent mutator thread
        let (classified_tx, classified_rx) = std::sync::mpsc::channel();
        let (continue_tx, continue_rx) = std::sync::mpsc::channel();

        let handler_sink = sink_state.clone();
        let handler_active = active.clone();
        let handler_old_sink = old_sink.clone();
        let handler_events = events.clone();

        let handler_thread = std::thread::spawn(move || {
            handle_error_message_inner(
                Some(handler_old_sink.upcast_ref()),
                "old sink dropped",
                &handler_sink,
                &handler_active,
                &handler_events,
                |_state| {
                    // We are inside the critical section of SinkState, having classified old_sink as active and !reconnecting
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

        // 5. Now that handler released the lock, concurrent context can acquire SinkState and perform reconnect
        {
            let mut state = sink_state.lock().unwrap();
            state.slot = SinkSlot::Active(new_sink.clone());
            state.reconnecting = false;
        }

        // 6. Stale error from old_sink is now rejected
        assert!(!handle_error_message(
            Some(old_sink.upcast_ref()),
            "old sink second error",
            &sink_state,
            &active,
            &events,
        ));
        assert!(rx.try_recv().is_err(), "stale old sink error must not be emitted");
    }
}
