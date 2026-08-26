use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::Client;
use sqlx::PgPool;

use crate::errors::AppError;
use crate::icecast::models::{get_settings, IcecastMode};
use crate::listeners::{
    ListenerUpdate, ListenersState, HISTORY_SAMPLE_INTERVAL, LIVE_POLL_INTERVAL, RETENTION_DAYS, STATS_REQUEST_TIMEOUT,
};
use crate::stations::repository::find_all_stations;
use crate::util::url_encode;

/// A single `<source>` entry from Icecast's admin stats XML.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceStats {
    mount: String,
    listeners: i32,
}

/// Parses `/admin/stats.xml` into per-mount listener counts.
fn parse_stats_xml(xml: &str) -> Vec<SourceStats> {
    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut sources = Vec::new();
    let mut current: Option<(String, Option<i32>)> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.local_name().as_ref() == b"source" => {
                let mount = e
                    .attributes()
                    .filter_map(|a| a.ok())
                    .find(|a| a.key.as_ref() == b"mount")
                    .map(|a| String::from_utf8_lossy(&a.value).into_owned())
                    .unwrap_or_default();
                current = Some((mount, None));
            }
            Ok(Event::Start(ref e)) if e.local_name().as_ref().eq_ignore_ascii_case(b"listeners") => {
                if let Some((_, slot)) = current.as_mut() {
                    *slot = reader.read_text(e.name()).ok().and_then(|t| t.trim().parse().ok());
                }
            }
            Ok(Event::End(ref e)) if e.local_name().as_ref() == b"source" => {
                if let Some((mount, listeners)) = current.take() {
                    sources.push(SourceStats {
                        mount,
                        listeners: listeners.unwrap_or(0),
                    });
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                tracing::warn!("Failed to parse Icecast stats XML: {e}");
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    sources
}

fn listener_count_for_mount(by_mount: &HashMap<String, i32>, mount: &str) -> i32 {
    let mount = mount.trim_start_matches('/');
    let encoded = url_encode(mount);
    by_mount.get(mount).or_else(|| by_mount.get(&encoded)).copied().unwrap_or(0)
}

/// Builds the admin stats URL and credentials from Icecast settings.
fn stats_endpoint(settings: &crate::icecast::models::IcecastSettings) -> Option<(String, String, String)> {
    match settings.mode {
        IcecastMode::Managed => Some((
            format!("http://127.0.0.1:{}/admin/stats.xml", settings.port),
            settings.admin_user.clone(),
            settings.admin_password.clone(),
        )),
        IcecastMode::External => {
            let base = settings.external_url.as_deref()?.trim_end_matches('/');
            let password = settings
                .external_admin_pw
                .clone()
                .unwrap_or_else(|| settings.admin_password.clone());
            Some((format!("{base}/admin/stats.xml"), settings.admin_user.clone(), password))
        }
    }
}

/// Fetches live listener counts for every station and optionally persists a
/// historical sample.
async fn poll_once(db: &PgPool, state: &Arc<ListenersState>, client: &Client, persist_sample: bool) -> Result<(), AppError> {
    let settings = get_settings(db).await?;
    let Some((url, user, password)) = stats_endpoint(&settings) else {
        return Ok(());
    };

    let response = client
        .get(&url)
        .basic_auth(user, Some(password))
        .timeout(STATS_REQUEST_TIMEOUT)
        .send()
        .await;

    let body = match response {
        Ok(resp) if resp.status().is_success() => resp.text().await.unwrap_or_default(),
        Ok(resp) => {
            tracing::warn!("Icecast stats returned {}; skipping poll", resp.status());
            return Ok(());
        }
        Err(e) => {
            tracing::warn!("Icecast stats fetch failed: {e}; marking stations offline");
            mark_offline(db, state).await;
            return Ok(());
        }
    };

    let sources = parse_stats_xml(&body);
    let by_mount: HashMap<String, i32> = sources
        .iter()
        .map(|s| (s.mount.trim_start_matches('/').to_string(), s.listeners))
        .collect();

    let stations = find_all_stations(db).await?;
    let now = Utc::now();
    let mut samples = persist_sample.then(|| Vec::with_capacity(stations.len()));

    for station in stations {
        let listeners = listener_count_for_mount(&by_mount, &station.mount());
        if let Some(samples) = samples.as_mut() {
            samples.push((station.id, listeners));
        }
        state
            .publish(ListenerUpdate {
                station_id: station.id,
                listeners,
                updated_at: now,
                online: true,
            })
            .await;
    }

    if let Some(samples) = samples {
        crate::listeners::models::insert_samples(db, &samples, now).await?;
        crate::listeners::models::prune_older_than(db, RETENTION_DAYS).await?;
    }

    Ok(())
}

/// Marks every station offline (used when Icecast is unreachable).
async fn mark_offline(db: &PgPool, state: &Arc<ListenersState>) {
    let stations = match find_all_stations(db).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Failed to list stations while marking offline: {e}");
            return;
        }
    };
    let now = Utc::now();
    for station in stations {
        state
            .publish(ListenerUpdate {
                station_id: station.id,
                listeners: 0,
                updated_at: now,
                online: false,
            })
            .await;
    }
}

pub async fn run(db: PgPool, state: Arc<ListenersState>) {
    let client = Client::new();
    let mut interval = tokio::time::interval(LIVE_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_sample_at = None;

    loop {
        interval.tick().await;
        let persist_sample = last_sample_at.is_none_or(|last: tokio::time::Instant| last.elapsed() >= HISTORY_SAMPLE_INTERVAL);
        if persist_sample {
            last_sample_at = Some(tokio::time::Instant::now());
        }
        if let Err(e) = poll_once(&db, &state, &client, persist_sample).await {
            tracing::error!("Listener poll failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multi_source_stats() {
        let xml = r#"<?xml version="1.0"?>
<icestats>
  <source mount="/rock.mp3">
    <listeners>3</listeners>
  </source>
  <source mount="/jazz.mp3">
    <listeners>0</listeners>
  </source>
  <listeners>3</listeners>
</icestats>"#;
        let sources = parse_stats_xml(xml);
        assert_eq!(sources.len(), 2);
        assert_eq!(
            sources[0],
            SourceStats {
                mount: "/rock.mp3".into(),
                listeners: 3
            }
        );
        assert_eq!(
            sources[1],
            SourceStats {
                mount: "/jazz.mp3".into(),
                listeners: 0
            }
        );
    }

    #[test]
    fn handles_source_without_listeners() {
        let xml = r#"<icestats><source mount="/silent.mp3"></source></icestats>"#;
        let sources = parse_stats_xml(xml);
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0],
            SourceStats {
                mount: "/silent.mp3".into(),
                listeners: 0
            }
        );
    }

    #[test]
    fn handles_invalid_xml_gracefully() {
        assert_eq!(parse_stats_xml("<not-closed"), vec![]);
    }

    #[test]
    fn matches_station_mount_with_or_without_leading_slash() {
        let xml = r#"<icestats><source mount="/main.mp3"><listeners>5</listeners></source></icestats>"#;
        let sources = parse_stats_xml(xml);
        let by_mount: HashMap<String, i32> = sources
            .iter()
            .map(|s| (s.mount.trim_start_matches('/').to_string(), s.listeners))
            .collect();

        assert_eq!(listener_count_for_mount(&by_mount, "main.mp3"), 5);
        assert_eq!(listener_count_for_mount(&by_mount, "/main.mp3"), 5);
    }
}
