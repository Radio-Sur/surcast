use gst::prelude::*;
use gstreamer as gst;

use super::super::pipeline::{IcecastTarget, PipelineError};

pub(super) const DEFAULT_FACTORY: &str = "shout2send";

pub(super) fn build(factory: &'static str, target: &IcecastTarget) -> Result<gst::Element, PipelineError> {
    let sink = gst::ElementFactory::make(factory)
        .build()
        .map_err(|_| PipelineError::MissingElement(factory))?;
    if factory == DEFAULT_FACTORY {
        configure(&sink, target);
    } else {
        sink.set_property("sync", false);
    }
    Ok(sink)
}

pub(super) fn configure(sink: &gst::Element, target: &IcecastTarget) {
    sink.set_property("ip", target.host.as_str());
    sink.set_property("port", target.port as i32);
    sink.set_property("mount", target.mount.as_str());
    sink.set_property("password", target.password.as_str());
    sink.set_property("streamname", target.stream_name.as_str());
    sink.set_property_from_str("protocol", "http");
    sink.set_property("username", "source");
    sink.set_property("send-title-info", false);
    sink.set_property("sync", false);
}
