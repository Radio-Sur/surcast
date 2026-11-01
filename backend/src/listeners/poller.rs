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
    bytes_read: u64,
}

/// Parses `/admin/stats.xml` into per-mount listener counts.
fn parse_stats_xml(xml: &str) -> Result<Vec<SourceStats>, String> {
    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut sources = Vec::new();
    let mut current: Option<SourceStats> = None;
    let mut depth = 0usize;
    let mut complete = false;

    loop {
        match reader.read_event_into(&mut buf).map_err(|error| error.to_string())? {
            Event::Start(ref e) => {
                if depth == 0 && (complete || e.name().as_ref() != b"icestats") {
                    return Err("Expected Icecast stats XML".into());
                }
                if depth == 1 && e.name().as_ref() == b"source" {
                    let mut mount = None;
                    for attribute in e.attributes() {
                        let attribute = attribute.map_err(|error| error.to_string())?;
                        if attribute.key.as_ref() == b"mount" {
                            mount = Some(attribute.unescape_value().map_err(|error| error.to_string())?.into_owned());
                        }
                    }
                    current = Some(SourceStats {
                        mount: mount.filter(|mount| !mount.is_empty()).ok_or("Missing source mount")?,
                        listeners: 0,
                        bytes_read: 0,
                    });
                }
                if depth == 2 {
                    if let Some(source) = current.as_mut() {
                        match e.name().as_ref() {
                            b"listeners" => {
                                source.listeners = reader
                                    .read_text(e.name())
                                    .map_err(|error| error.to_string())?
                                    .trim()
                                    .parse()
                                    .map_err(|_| "Invalid listener count")?;
                                continue;
                            }
                            b"total_bytes_read" => {
                                source.bytes_read = reader
                                    .read_text(e.name())
                                    .map_err(|error| error.to_string())?
                                    .trim()
                                    .parse()
                                    .map_err(|_| "Invalid source byte count")?;
                                continue;
                            }
                            _ => {}
                        }
                    }
                }
                depth += 1;
            }
            Event::End(ref e) => {
                depth = depth.checked_sub(1).ok_or("Unexpected closing tag")?;
                if depth == 1 && e.name().as_ref() == b"source" {
                    sources.push(current.take().ok_or("Unexpected source end")?);
                }
                if depth == 0 {
                    complete = true;
                }
            }
            Event::Empty(ref e) if depth == 1 && e.name().as_ref() == b"source" => {
                return Err("Incomplete Icecast source stats".into());
            }
            Event::Empty(ref e) if depth == 0 => {
                if complete || e.name().as_ref() != b"icestats" {
                    return Err("Expected Icecast stats XML".into());
                }
                complete = true;
            }
            Event::Text(ref text) if depth == 0 && !text.iter().all(u8::is_ascii_whitespace) => {
                return Err("Unexpected content outside Icecast stats".into())
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if !complete || depth != 0 {
        return Err("Incomplete Icecast stats XML".into());
    }
    Ok(sources)
}

/// Verify source presence and received audio without opening a listener
/// connection (which would inflate listener counts). Never interpret an
/// unavailable, unauthorized or malformed stats response as a stopped source.
pub(crate) async fn verify_output(db: &PgPool, mount: &str) -> Result<(bool, bool), AppError> {
    let settings = get_settings(db).await?;
    let (url, user, password) =
        stats_endpoint(&settings).ok_or_else(|| AppError::icecast("Icecast admin endpoint is not configured".into()))?;
    let failure = || AppError::icecast("Could not verify the station output in Icecast".into()).with_code("STREAM_OUTPUT_UNVERIFIED");
    let response = Client::new()
        .get(url)
        .basic_auth(user, Some(password))
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
        .map_err(|_| failure())?;
    let body = response
        .error_for_status()
        .map_err(|_| failure())?
        .text()
        .await
        .map_err(|_| failure())?;
    let sources = parse_stats_xml(&body).map_err(|error| {
        tracing::warn!(%error, "Invalid Icecast stats while verifying station output");
        failure()
    })?;
    let mount = mount.trim_start_matches('/');
    let encoded = url_encode(mount);
    let source = sources.iter().find(|source| {
        let candidate = source.mount.trim_start_matches('/');
        candidate == mount || candidate == encoded
    });
    Ok((source.is_some(), source.is_some_and(|source| source.bytes_read > 0)))
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
            mark_offline(db, state, persist_sample).await;
            return Ok(());
        }
    };

    let sources = parse_stats_xml(&body).map_err(|error| AppError::icecast(format!("Invalid Icecast stats: {error}")))?;
    let by_mount: HashMap<String, i32> = sources
        .iter()
        .map(|s| (s.mount.trim_start_matches('/').to_string(), s.listeners))
        .collect();

    let stations = find_all_stations(db).await?;
    warn_on_mount_collisions(&stations);
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

/// Two stations sharing one Icecast mount read the same `<source>` count,
/// so N real connections show up as 2N in totals and history. Station
/// create/update rejects such collisions (409); this warns about ones that
/// predate the check so an admin can fix them instead of silently doubling.
fn warn_on_mount_collisions(stations: &[crate::stations::models::Station]) {
    use std::collections::HashMap;
    let mut seen: HashMap<String, &str> = HashMap::new();
    for station in stations {
        let mount = station.mount().trim_start_matches('/').to_string();
        if let Some(first) = seen.insert(mount.clone(), station.name.as_str()) {
            tracing::warn!(
                mount = %mount,
                stations = ?[first, station.name.as_str()],
                "Stations share one Icecast mount; listener counts are duplicated for both"
            );
        }
    }
}

/// Marks every station offline (used when Icecast is unreachable). Live
/// state flips immediately, but zero samples are persisted only on the
/// history cadence (`persist_sample`) so outage rows keep the same 30s
/// spacing as live rows — denser zeros would overweight the outage in
/// bucket averages. Zeros (instead of gaps) keep averages honest.
async fn mark_offline(db: &PgPool, state: &Arc<ListenersState>, persist_sample: bool) {
    let stations = match find_all_stations(db).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Failed to list stations while marking offline: {e}");
            return;
        }
    };
    let now = Utc::now();
    let mut samples = persist_sample.then(|| Vec::with_capacity(stations.len()));
    for station in stations {
        if let Some(samples) = samples.as_mut() {
            samples.push((station.id, 0));
        }
        state
            .publish(ListenerUpdate {
                station_id: station.id,
                listeners: 0,
                updated_at: now,
                online: false,
            })
            .await;
    }
    if let Some(samples) = samples {
        if let Err(e) = crate::listeners::models::insert_samples(db, &samples, now).await {
            tracing::warn!("Failed to persist offline listener samples: {e}");
        }
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
        let sources = parse_stats_xml(xml).unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(
            sources[0],
            SourceStats {
                mount: "/rock.mp3".into(),
                listeners: 3,
                bytes_read: 0,
            }
        );
        assert_eq!(
            sources[1],
            SourceStats {
                mount: "/jazz.mp3".into(),
                listeners: 0,
                bytes_read: 0,
            }
        );
    }

    #[test]
    fn handles_source_without_listeners() {
        let xml = r#"<icestats><source mount="/silent.mp3"></source></icestats>"#;
        let sources = parse_stats_xml(xml).unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0],
            SourceStats {
                mount: "/silent.mp3".into(),
                listeners: 0,
                bytes_read: 0,
            }
        );
    }

    #[test]
    fn handles_invalid_xml_gracefully() {
        for xml in [
            "<not-closed",
            "",
            "<html>Unauthorized</html>",
            "<icestats><source mount=\"/a\"></source>",
            "<icestats></icestats><icestats/>",
        ] {
            assert!(parse_stats_xml(xml).is_err(), "must not confirm a stopped stream from {xml}");
        }
        assert_eq!(parse_stats_xml("<icestats/>"), Ok(vec![]));
    }

    #[test]
    fn source_presence_is_not_proof_that_audio_has_arrived() {
        let sources = parse_stats_xml(
            r#"<icestats>
            <source mount="/starting.mp3"><total_bytes_read>0</total_bytes_read></source>
            <source mount="/live.mp3"><total_bytes_read>16384</total_bytes_read></source>
        </icestats>"#,
        )
        .unwrap();
        assert_eq!(sources[0].bytes_read, 0);
        assert_eq!(sources[1].bytes_read, 16384);
        assert!(parse_stats_xml("<icestats><source/></icestats>").is_err());
    }

    #[test]
    fn matches_station_mount_with_or_without_leading_slash() {
        let xml = r#"<icestats><source mount="/main.mp3"><listeners>5</listeners></source></icestats>"#;
        let sources = parse_stats_xml(xml).unwrap();
        let by_mount: HashMap<String, i32> = sources
            .iter()
            .map(|s| (s.mount.trim_start_matches('/').to_string(), s.listeners))
            .collect();

        assert_eq!(listener_count_for_mount(&by_mount, "main.mp3"), 5);
        assert_eq!(listener_count_for_mount(&by_mount, "/main.mp3"), 5);
    }
}
