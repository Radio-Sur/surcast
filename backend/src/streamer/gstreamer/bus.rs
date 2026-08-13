use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::mpsc;

use super::super::pipeline::{PipelineError, PipelineEvent};
use super::{ActivePlan, ReplaceCancellation};

pub(super) fn install(
    pipeline: &gst::Pipeline,
    clock_gate: &gst::Element,
    active: Arc<Mutex<Option<ActivePlan>>>,
    replacing: Arc<Mutex<Option<ReplaceCancellation>>>,
    output_reconnecting: Arc<AtomicBool>,
    events: mpsc::UnboundedSender<PipelineEvent>,
) -> Result<(), PipelineError> {
    let handover_active = active.clone();
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
                    plan.next.take().map(|next| {
                        plan.current = next.clone();
                        plan.current_epoch = plan.last_elapsed;
                        (plan.generation, next)
                    })
                } else {
                    None
                }
            })
        };
        if let Some((generation, current)) = handover {
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
            if !output_reconnecting.load(Ordering::Acquire) {
                if let Some(active) = active.lock().unwrap_or_else(|error| error.into_inner()).as_ref() {
                    let _ = events.send(PipelineEvent::SinkDisconnected {
                        generation: active.generation,
                        output_epoch: active.output_epoch,
                        message: error.error().to_string(),
                    });
                }
            }
        }
        gst::BusSyncReply::Pass
    });
    Ok(())
}
