use std::time::Duration;

use gst::prelude::*;
use gstreamer as gst;
use tokio::sync::watch;

use super::super::pipeline::{IcecastTarget, PipelineError, TrackMetadata};

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

#[derive(Clone)]
pub(super) struct MetadataPublisher {
    updates: watch::Sender<Option<MetadataUpdate>>,
}

#[derive(Clone)]
struct MetadataUpdate {
    target: IcecastTarget,
    metadata: TrackMetadata,
}

impl MetadataPublisher {
    pub(super) fn spawn() -> Self {
        let (updates, mut pending) = watch::channel::<Option<MetadataUpdate>>(None);
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("metadata HTTP client configuration is valid");
            while pending.changed().await.is_ok() {
                let Some(mut update) = pending.borrow_and_update().clone() else {
                    continue;
                };
                let mut warned = false;
                loop {
                    let result = {
                        let request = send_metadata(&client, &update.target, &update.metadata);
                        tokio::pin!(request);
                        tokio::select! {
                            biased;
                            changed = pending.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                None
                            },
                            result = &mut request => Some(result),
                        }
                    };
                    match result {
                        None => match pending.borrow_and_update().clone() {
                            Some(newer) => {
                                update = newer;
                                warned = false;
                            }
                            None => break,
                        },
                        Some(Ok(())) => break,
                        Some(Err(error)) => {
                            if !warned {
                                tracing::warn!(%error, "Icecast metadata update failed; retrying latest metadata");
                                warned = true;
                            }
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                    }
                }
            }
        });
        Self { updates }
    }

    pub(super) fn publish(&self, target: IcecastTarget, metadata: TrackMetadata) {
        self.updates.send_replace(Some(MetadataUpdate { target, metadata }));
    }

    pub(super) fn clear(&self) {
        self.updates.send_replace(None);
    }
}

async fn send_metadata(client: &reqwest::Client, target: &IcecastTarget, metadata: &TrackMetadata) -> Result<(), reqwest::Error> {
    let mut url = reqwest::Url::parse("http://localhost/admin/metadata").expect("static metadata URL is valid");
    url.set_host(Some(&target.host)).expect("validated Icecast host remains valid");
    url.set_port(Some(target.port)).expect("HTTP URLs accept a port");
    let song = format!("{} - {}", metadata.artist, metadata.title);
    // GET with query parameters, not POST: Icecast 2.4 rejects POST on
    // /admin/metadata with `400 unknown request` (2.5 accepts both), and
    // libshout itself falls back to GET for servers that do not advertise
    // POST. GET works on every supported Icecast version.
    client
        .get(url)
        .basic_auth("source", Some(&target.password))
        .query(&[("mode", "updinfo"), ("mount", target.mount.as_str()), ("song", song.as_str())])
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        loop {
            let mut chunk = vec![0; 1024];
            let length = stream.read(&mut chunk).await.unwrap();
            assert!(length > 0, "request ended before its form body");
            request.extend_from_slice(&chunk[..length]);
            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length: ")?.parse::<usize>().ok())
                .unwrap_or_default();
            if request.len() >= header_end + 4 + content_length {
                return String::from_utf8(request).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn metadata_update_uses_source_credentials_and_current_artist_title() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            request
        });
        let target = IcecastTarget::parse(&format!("127.0.0.1:{port}"), "secret".into(), "daily mix", "Daily".into()).unwrap();

        send_metadata(
            &reqwest::Client::new(),
            &target,
            &TrackMetadata {
                title: "Current title".into(),
                artist: "Current artist".into(),
            },
        )
        .await
        .unwrap();

        let request = server.await.unwrap();
        assert!(request.starts_with("GET /admin/metadata?mode=updinfo&mount=%2Fdaily%2520mix&song=Current+artist+-+Current+title HTTP/1.1"));
        assert!(!request.contains("content-type: application/x-www-form-urlencoded"));
        assert!(request.contains("\r\nauthorization: Basic c291cmNlOnNlY3JldA==\r\n"));
    }

    #[tokio::test]
    async fn metadata_publisher_retries_until_the_source_mount_is_available() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for status in ["404 Not Found", "200 OK"] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                requests.push(request);
                stream
                    .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes())
                    .await
                    .unwrap();
            }
            requests
        });
        let target = IcecastTarget::parse(&format!("127.0.0.1:{port}"), "secret".into(), "stream", "Stream".into()).unwrap();
        let publisher = MetadataPublisher::spawn();
        publisher.publish(
            target,
            TrackMetadata {
                title: "Current title".into(),
                artist: "Current artist".into(),
            },
        );

        let requests = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("metadata retry")
            .unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests
            .iter()
            .all(|request| request.contains("song=Current+artist+-+Current+title")));
    }

    #[tokio::test]
    async fn clearing_metadata_cancels_an_in_flight_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
        let (respond_tx, respond_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _request = read_request(&mut stream).await;
            request_seen_tx.send(()).unwrap();
            respond_rx.await.unwrap();
            let _ = stream
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
            listener
        });
        let target = IcecastTarget::parse(&format!("127.0.0.1:{port}"), "secret".into(), "stream", "Stream".into()).unwrap();
        let publisher = MetadataPublisher::spawn();
        publisher.publish(
            target,
            TrackMetadata {
                title: "Current title".into(),
                artist: "Current artist".into(),
            },
        );
        request_seen_rx.await.unwrap();

        publisher.clear();
        respond_tx.send(()).unwrap();
        let listener = server.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(400), listener.accept()).await.is_err(),
            "cleared metadata was retried"
        );
    }
}
