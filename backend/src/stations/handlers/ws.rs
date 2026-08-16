use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

use crate::api::StreamersMap;
use crate::config::Config;
use crate::errors::AppError;
use crate::listeners::ListenersState;
use crate::stations::repository;
use crate::streamer::{StationStreamer, StatusEvent};

use super::stream::{resolve_station_id, start_station, StationEvent, StationLifecycleLocks};

#[derive(Deserialize)]
pub struct WsQuery {
    token: Option<String>,
}

/// Messages sent from the client to the server.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Inbound {
    Auth { token: String },
    Subscribe { station_id: String },
    Unsubscribe { station_id: String },
    Skip { station_id: String },
    Play { station_id: String },
    Pause { station_id: String },
}

/// Messages pushed from the server to the client.
#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Outbound {
    AuthOk,
    Error {
        /// Connection-level errors carry no station; per-station errors
        /// carry the station the message refers to, so a multi-station
        /// connection can attribute them.
        #[serde(skip_serializing_if = "Option::is_none")]
        station_id: Option<Uuid>,
        data: String,
    },
    Status {
        station_id: Uuid,
        data: StatusEvent,
    },
    QueueUpdate {
        station_id: Uuid,
        data: serde_json::Value,
    },
    Listeners {
        station_id: Uuid,
        listeners: i32,
        updated_at: DateTime<Utc>,
        online: bool,
    },
}

/// Single global WebSocket endpoint for live station state and listener counts.
pub async fn global_ws(
    ws: WebSocketUpgrade,
    State(db): State<PgPool>,
    State(streamers): State<StreamersMap>,
    State(lifecycle): State<Arc<StationLifecycleLocks>>,
    State(config): State<Config>,
    State(listeners): State<Arc<ListenersState>>,
    Query(query): Query<WsQuery>,
) -> Result<impl IntoResponse, AppError> {
    if let Some(ref token) = query.token {
        crate::auth::middleware::verify_token(token, &config.jwt_secret).map_err(|_| AppError::Unauthorized("Invalid token".into()))?;
    }

    let upload_dir = config.upload_dir.clone();
    let jwt_secret = config.jwt_secret.clone();
    let token_from_query = query.token.clone();

    Ok(ws.on_upgrade(move |socket| {
        handle_connection(
            socket,
            db,
            streamers,
            lifecycle,
            upload_dir,
            jwt_secret,
            token_from_query,
            listeners,
        )
    }))
}

async fn handle_connection(
    socket: WebSocket,
    db: PgPool,
    streamers: StreamersMap,
    lifecycle: Arc<StationLifecycleLocks>,
    upload_dir: String,
    jwt_secret: String,
    token_from_query: Option<String>,
    listeners: Arc<ListenersState>,
) {
    let (mut sender, mut receiver) = socket.split();

    if token_from_query.is_none() {
        match authenticate(&mut sender, &mut receiver, &jwt_secret).await {
            Ok(()) => {}
            Err(msg) => {
                let _ = sender.send(error_msg(&msg)).await;
                return;
            }
        }
    }

    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let mut send_task = tokio::spawn(ws_send_task(sender, out_rx));
    let mut listeners_task = tokio::spawn(forward_listeners(listeners, out_tx.clone()));
    let mut recv_task = tokio::spawn(ws_recv_task(receiver, db, streamers, lifecycle, upload_dir, jwt_secret, out_tx));

    tokio::select! {
        _ = &mut send_task => {},
        _ = &mut listeners_task => {},
        _ = &mut recv_task => {},
    }

    send_task.abort();
    listeners_task.abort();
    recv_task.abort();
    let _ = send_task.await;
}

async fn authenticate(
    sender: &mut SplitSink<WebSocket, Message>,
    receiver: &mut SplitStream<WebSocket>,
    jwt_secret: &str,
) -> Result<(), String> {
    let timeout_dur = Duration::from_secs(10);
    let msg = timeout(timeout_dur, receiver.next())
        .await
        .map_err(|_| "auth timeout".to_string())?
        .ok_or("connection closed".to_string())?
        .map_err(|_| "connection error".to_string())?;

    let Message::Text(text) = msg else {
        return Err("expected auth message".to_string());
    };

    #[derive(Deserialize)]
    struct WsAuth {
        #[serde(rename = "type")]
        msg_type: String,
        token: String,
    }

    let auth: WsAuth = serde_json::from_str(&text).map_err(|_| "invalid auth message".to_string())?;
    if auth.msg_type != "auth" {
        return Err("expected auth message".to_string());
    }
    if crate::auth::middleware::verify_token(&auth.token, jwt_secret).is_err() {
        return Err("unauthorized".to_string());
    }

    sender
        .send(Message::Text(serde_json::json!({"type": "auth_ok"}).to_string().into()))
        .await
        .map_err(|_| "send failed".to_string())
}

fn error_msg(data: &str) -> Message {
    Message::Text(serde_json::json!({"type":"error","data":data}).to_string().into())
}

/// Drains the per-connection outbound queue and forwards it to the socket,
/// interleaving heartbeat pings.
async fn ws_send_task(mut sender: SplitSink<WebSocket, Message>, mut out_rx: mpsc::UnboundedReceiver<Outbound>) {
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if sender.send(Message::Ping(vec![].into())).await.is_err() {
                    return;
                }
            }
            msg = out_rx.recv() => {
                match msg {
                    Some(out) => {
                        let text = serde_json::to_string(&out).unwrap_or_default();
                        if sender.send(Message::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
        }
    }
}

/// Sends the cached listener snapshot, then forwards newer listener-count updates.
///
/// Subscribing before reading the cache closes the reconnect race: updates
/// published during the snapshot read remain queued. Timestamps suppress any
/// queued update already represented by that snapshot.
async fn forward_listeners(listeners: Arc<ListenersState>, out_tx: mpsc::UnboundedSender<Outbound>) {
    let mut rx = listeners.subscribe();
    let snapshot = listeners.live_all().await;
    let mut latest = HashMap::with_capacity(snapshot.len());

    for (station_id, live) in snapshot {
        latest.insert(station_id, live.updated_at);
        if out_tx
            .send(Outbound::Listeners {
                station_id,
                listeners: live.listeners,
                updated_at: live.updated_at,
                online: live.online,
            })
            .is_err()
        {
            return;
        }
    }

    loop {
        match rx.recv().await {
            Ok(update) => {
                if latest
                    .get(&update.station_id)
                    .is_some_and(|updated_at| *updated_at >= update.updated_at)
                {
                    continue;
                }
                latest.insert(update.station_id, update.updated_at);
                if out_tx
                    .send(Outbound::Listeners {
                        station_id: update.station_id,
                        listeners: update.listeners,
                        updated_at: update.updated_at,
                        online: update.online,
                    })
                    .is_err()
                {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Handles client commands: subscriptions and stream control.
async fn ws_recv_task(
    mut receiver: SplitStream<WebSocket>,
    db: PgPool,
    streamers: StreamersMap,
    lifecycle: Arc<StationLifecycleLocks>,
    upload_dir: String,
    jwt_secret: String,
    out_tx: mpsc::UnboundedSender<Outbound>,
) {
    let mut subs: HashMap<Uuid, JoinHandle<()>> = HashMap::new();

    while let Some(Ok(msg)) = receiver.next().await {
        let Message::Text(text) = msg else { continue };

        let cmd: Inbound = match serde_json::from_str(&text) {
            Ok(cmd) => cmd,
            Err(_) => {
                let _ = out_tx.send(Outbound::Error {
                    station_id: None,
                    data: "invalid message".into(),
                });
                continue;
            }
        };

        match cmd {
            Inbound::Auth { token } => {
                if crate::auth::middleware::verify_token(&token, &jwt_secret).is_ok() {
                    let _ = out_tx.send(Outbound::AuthOk);
                } else {
                    let _ = out_tx.send(Outbound::Error {
                        station_id: None,
                        data: "unauthorized".into(),
                    });
                }
            }
            Inbound::Subscribe { station_id } => {
                let station_id = match resolve_station_id(&db, &station_id).await {
                    Ok(id) => id,
                    Err(_) => {
                        let _ = out_tx.send(Outbound::Error {
                            station_id: None,
                            data: "unknown station".into(),
                        });
                        continue;
                    }
                };
                // Validate the station exists. Observation only: subscribing
                // must never create a runtime, start the station or change
                // the desired state.
                match repository::find_station_by_id(&db, station_id).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        let _ = out_tx.send(Outbound::Error {
                            station_id: None,
                            data: "unknown station".into(),
                        });
                        continue;
                    }
                    Err(_) => {
                        let _ = out_tx.send(Outbound::Error {
                            station_id: None,
                            data: "no active stream".into(),
                        });
                        continue;
                    }
                }
                if subs.contains_key(&station_id) {
                    continue;
                }
                // `forward_station` is the SINGLE owner of this client's
                // station snapshot: it sends the initial status/error and
                // the read-only DB queue snapshot (per-client, never a
                // broadcast), and a fresh snapshot on every (re-)attach to
                // a runtime. Nothing is sent here in parallel, so snapshot
                // and live feed cannot race or interleave.
                let handle = tokio::spawn(forward_station(
                    db.clone(),
                    upload_dir.clone(),
                    streamers.clone(),
                    lifecycle.clone(),
                    station_id,
                    out_tx.clone(),
                ));
                subs.insert(station_id, handle);
            }
            Inbound::Unsubscribe { station_id } => {
                if let Ok(id) = resolve_station_id(&db, &station_id).await {
                    if let Some(handle) = subs.remove(&id) {
                        handle.abort();
                    }
                }
            }
            Inbound::Skip { station_id } => {
                if let Ok(id) = resolve_station_id(&db, &station_id).await {
                    if let Some(s) = get_streamer(&streamers, &id) {
                        if let Err(error) = s.skip().await {
                            let _ = out_tx.send(Outbound::Error {
                                station_id: Some(id),
                                data: format!("skip failed: {error}"),
                            });
                        }
                    }
                }
            }
            Inbound::Play { station_id } => {
                if let Ok(id) = resolve_station_id(&db, &station_id).await {
                    if let Err(error) = start_station(&db, &streamers, &lifecycle, &upload_dir, id).await {
                        let _ = out_tx.send(Outbound::Error {
                            station_id: Some(id),
                            data: format!("play failed: {error}"),
                        });
                    }
                }
            }
            Inbound::Pause { station_id } => {
                if let Ok(id) = resolve_station_id(&db, &station_id).await {
                    if let Some(s) = get_streamer(&streamers, &id) {
                        if let Err(error) = s.pause().await {
                            let _ = out_tx.send(Outbound::Error {
                                station_id: Some(id),
                                data: format!("pause failed: {error}"),
                            });
                        }
                    }
                }
            }
        }
    }

    for (_, handle) in subs {
        handle.abort();
    }
}

/// Forwards one station's status + queue to the connection.
///
/// This task is the SINGLE owner of the station snapshot for this client:
/// `ws_recv_task` only verifies the station and spawns it. Every attach to a
/// `StationStreamer` — the first one, or a re-attach after the runtime was
/// replaced by a restart — sends a fresh current Status/Error and a
/// read-only DB queue snapshot directly to this connection's `out_tx`
/// (per-client, never a broadcast), BEFORE live broadcasts are forwarded.
/// A subscriber can therefore never stay on a stale snapshot, no matter
/// when the runtime appears or is replaced.
///
/// `push_queue_update` is never used for snapshots: it would broadcast to
/// every subscriber of the station and is reserved for real queue changes.
async fn forward_station(
    db: PgPool,
    upload_dir: String,
    streamers: StreamersMap,
    lifecycle: Arc<StationLifecycleLocks>,
    station_id: Uuid,
    out_tx: mpsc::UnboundedSender<Outbound>,
) {
    loop {
        // The connection may already be gone (e.g. the receive task ended
        // while this task was between phases): end instead of re-attaching
        // to a runtime nobody can receive from.
        if out_tx.is_closed() {
            return;
        }
        let Some(streamer) = get_streamer(&streamers, &station_id) else {
            // No runtime: one snapshot per observable state change, then
            // wait for a runtime to appear or for a station-scoped
            // notification (Lifecycle or Queue). The DB is read only then,
            // never on the map poll ticks; other stations' notifications
            // are ignored without any read.
            let mut notifications = lifecycle.subscribe_notifications();
            let mut last_is_started = None;
            let mut force_queue = false;
            loop {
                match no_runtime_snapshot(
                    &db,
                    &upload_dir,
                    &streamers,
                    &lifecycle,
                    station_id,
                    &out_tx,
                    last_is_started,
                    force_queue,
                )
                .await
                {
                    NoRuntimeSnapshot::Sent { is_started } => last_is_started = Some(is_started),
                    NoRuntimeSnapshot::RuntimePresent => break,
                    NoRuntimeSnapshot::StationGone => return,
                    NoRuntimeSnapshot::DbError => {}
                    NoRuntimeSnapshot::ConnectionGone => return,
                }
                let mut lifecycle_changed = false;
                let mut queue_changed = false;
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                        _ = out_tx.closed() => return,
                        event = notifications.recv() => match event {
                            Ok(StationEvent::Lifecycle { station_id: id }) if id == station_id => lifecycle_changed = true,
                            Ok(StationEvent::Queue { station_id: id }) if id == station_id => queue_changed = true,
                            // Another station's change: nothing to re-read.
                            Ok(_) => continue,
                            // Events were dropped: re-check everything.
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                lifecycle_changed = true;
                                queue_changed = true;
                            }
                            // The session (lifecycle) is gone: re-check
                            // rather than hang.
                            Err(broadcast::error::RecvError::Closed) => lifecycle_changed = true,
                        },
                    }
                    if get_streamer(&streamers, &station_id).is_some() {
                        break;
                    }
                    if lifecycle_changed || queue_changed {
                        break;
                    }
                }
                if get_streamer(&streamers, &station_id).is_some() {
                    break; // runtime appeared: attach (outer loop)
                }
                // A queue mutation must produce a fresh QueueUpdate even
                // when `is_started` did not change.
                force_queue = queue_changed;
            }
            continue;
        };
        // Attach with a full fresh snapshot, then forward live broadcasts
        // until the runtime is replaced (or gone).
        match attach_snapshot_and_forward(&db, &upload_dir, &streamers, station_id, &out_tx, streamer).await {
            ForwardOutcome::Reattach => continue,
            ForwardOutcome::ConnectionGone => return,
        }
    }
}

/// Outcome of one forwarding phase. The outer loop must distinguish a
/// normal runtime replacement (re-attach with a fresh snapshot) from a
/// dead connection: once the output channel is gone, the task has to end —
/// re-attaching would spin on failed sends forever.
///
/// `ConnectionGone` is EXCLUSIVELY a dead output/WebSocket: `out_tx.closed()`
/// or a failed `out_tx.send`. `RecvError::Closed` on the runtime feeds is
/// unreachable while the attached streamer is alive — the only broadcast
/// senders are owned by the `StationStreamer` Arc this task holds — so it is
/// answered with a defensive re-attach, never `ConnectionGone`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ForwardOutcome {
    /// The runtime was replaced/removed: re-attach with a fresh
    /// Status/Error + DB queue snapshot.
    Reattach,
    /// The connection's output channel is gone: the forwarding task must
    /// end instead of re-attaching.
    ConnectionGone,
}

/// Outcome of one no-runtime snapshot attempt.
enum NoRuntimeSnapshot {
    /// Snapshot sent (or skipped because the state did not change); the
    /// observed `is_started` lets the watcher detect changes on the next
    /// signal.
    Sent { is_started: bool },
    /// A runtime appeared while the snapshot was being prepared: the
    /// caller should attach to it instead.
    RuntimePresent,
    /// The station row is gone: forwarding must end instead of polling
    /// forever. An explicit error was sent.
    StationGone,
    /// The DB read failed: an explicit error was sent and the watcher keeps
    /// listening; `last_is_started` is untouched so the next signal retries.
    DbError,
    /// The connection's output channel is gone: the forwarding task must
    /// end instead of spinning on failed sends.
    ConnectionGone,
}

/// The atomic observation result of one no-runtime snapshot: decided under
/// the per-station lifecycle lock, sent (or interpreted) only after the
/// lock is released.
enum StationObservation {
    /// A runtime is mapped for the station: attach to it.
    RuntimePresent,
    /// The station row is gone.
    StationGone,
    /// The station lookup failed; the message payload for the client.
    LookupFailed(String),
    /// The station exists and is not started: a legal stopped snapshot.
    State { is_started: bool },
}

/// Observation-only snapshot for a station without a runtime.
///
/// The runtime-map check AND the persisted `is_started` read must come
/// from ONE quiescent point relative to this station's lifecycle
/// transitions, so the per-station lifecycle lock is held for both. That
/// is what distinguishes "transition still running" (a runtime may appear
/// right after) from "transition finished without a runtime" — the only
/// case that may produce the no-runtime error. Without the joint read, a
/// Play starting right after the map check could persist `started=true`
/// before the observer reads the DB, yielding a transient
/// "started without runtime" error for a transition that was still
/// running.
///
/// The lock is released before anything else: no `out_tx.send`, no queue
/// JSON fetch, no long handling runs under it (the single `find_station_by_id`
/// under the guard is a plain row read and only blocks transitions of THIS
/// station, which is exactly the serialization we need).
///
/// The persisted desired state decides between a legal stopped status and
/// the explicit no-runtime error, always followed by the read-only DB
/// queue. Status/error is sent only when `is_started` changed; `force_queue`
/// additionally sends a fresh queue snapshot (a queue mutation happened),
/// even when `is_started` did not.
async fn no_runtime_snapshot(
    db: &PgPool,
    upload_dir: &str,
    streamers: &StreamersMap,
    lifecycle: &StationLifecycleLocks,
    station_id: Uuid,
    out_tx: &mpsc::UnboundedSender<Outbound>,
    last_is_started: Option<bool>,
    force_queue: bool,
) -> NoRuntimeSnapshot {
    // Wait for any in-flight transition of THIS station to finish, then
    // decide runtime-map + persisted state atomically under the same lock.
    // No helper that re-enters the lock is called while it is held.
    let observation = {
        let _guard = lifecycle.lock(station_id).await;
        if get_streamer(streamers, &station_id).is_some() {
            StationObservation::RuntimePresent
        } else {
            match repository::find_station_by_id(db, station_id).await {
                Ok(Some(station)) => StationObservation::State {
                    is_started: station.is_started,
                },
                Ok(None) => StationObservation::StationGone,
                Err(error) => {
                    tracing::error!(station_id = %station_id, %error, "station lookup failed");
                    StationObservation::LookupFailed(error.to_string())
                }
            }
        }
    };

    let is_started = match observation {
        StationObservation::RuntimePresent => return NoRuntimeSnapshot::RuntimePresent,
        StationObservation::StationGone => {
            // Deleted while subscribed without a runtime: end forwarding
            // instead of polling forever; the client gets the same
            // existing-protocol error as a Subscribe to an unknown station.
            if out_tx
                .send(Outbound::Error {
                    station_id: Some(station_id),
                    data: "unknown station".into(),
                })
                .is_err()
            {
                return NoRuntimeSnapshot::ConnectionGone;
            }
            return NoRuntimeSnapshot::StationGone;
        }
        StationObservation::LookupFailed(error) => {
            // A DB read failure must not be silently treated as "station
            // does not exist" nor as a legal state; the client gets an
            // explicit error and the watcher retries on the next signal.
            if out_tx
                .send(Outbound::Error {
                    station_id: Some(station_id),
                    data: format!("station lookup failed: {error}"),
                })
                .is_err()
            {
                return NoRuntimeSnapshot::ConnectionGone;
            }
            return NoRuntimeSnapshot::DbError;
        }
        StationObservation::State { is_started } => is_started,
    };

    let is_started_changed = last_is_started != Some(is_started);
    if is_started_changed {
        if is_started {
            // Desired-started but no runtime (e.g. a failed startup
            // restore or a start that produced no runtime): observation
            // stays read-only — no runtime is created here — but the
            // subscriber must not wait in silence, and this must not be
            // presented as a legal stopped state.
            tracing::warn!(station_id = %station_id, "station is started but has no runtime");
            if out_tx
                .send(Outbound::Error {
                    station_id: Some(station_id),
                    data: "station is started but no runtime is available".into(),
                })
                .is_err()
            {
                return NoRuntimeSnapshot::ConnectionGone;
            }
        } else if out_tx
            .send(Outbound::Status {
                station_id,
                data: StatusEvent::State {
                    playing: false,
                    song_index: 0,
                    total: 0,
                    elapsed: 0,
                    title: String::new(),
                    artist: String::new(),
                    duration: 0,
                },
            })
            .is_err()
        {
            return NoRuntimeSnapshot::ConnectionGone;
        }
    }
    if is_started_changed || force_queue {
        if let Some(queue) = db_queue_snapshot(db, upload_dir, station_id).await {
            if out_tx.send(Outbound::QueueUpdate { station_id, data: queue }).is_err() {
                return NoRuntimeSnapshot::ConnectionGone;
            }
        }
    }
    NoRuntimeSnapshot::Sent { is_started }
}

/// Attaches to ONE runtime: broadcast receivers first, then a fresh
/// per-client Status/Error + DB queue snapshot, then live forwarding.
///
/// The receivers are created BEFORE the snapshot is fetched: anything the
/// runtime emits while `status()` or the DB read runs is buffered by them
/// and forwarded afterwards, so no change can slip between the snapshot and
/// the live feed. The periodic map recheck detects runtime replacement and
/// returns [`ForwardOutcome::Reattach`] to the outer loop, which re-attaches
/// with a full fresh snapshot. A failed `out_tx.send` (or an already closed
/// channel) is [`ForwardOutcome::ConnectionGone`]: the connection is dead
/// and re-attaching would spin on failed sends.
async fn attach_snapshot_and_forward(
    db: &PgPool,
    upload_dir: &str,
    streamers: &StreamersMap,
    station_id: Uuid,
    out_tx: &mpsc::UnboundedSender<Outbound>,
    streamer: Arc<StationStreamer>,
) -> ForwardOutcome {
    let mut status_rx = streamer.subscribe_status();
    let mut queue_rx = streamer.subscribe_queue();

    match streamer.status().await {
        Ok(status) => {
            if out_tx.send(Outbound::Status { station_id, data: status }).is_err() {
                return ForwardOutcome::ConnectionGone;
            }
        }
        Err(error) => {
            // Never report a pipeline fault as a legal stopped state; the
            // client gets an explicit error instead.
            tracing::error!(station_id = %station_id, %error, "stream status unavailable on attach");
            if out_tx
                .send(Outbound::Error {
                    station_id: Some(station_id),
                    data: format!("stream status unavailable: {error}"),
                })
                .is_err()
            {
                return ForwardOutcome::ConnectionGone;
            }
        }
    }
    if let Some(queue) = db_queue_snapshot(db, upload_dir, station_id).await {
        if out_tx.send(Outbound::QueueUpdate { station_id, data: queue }).is_err() {
            return ForwardOutcome::ConnectionGone;
        }
    }

    forward_live_broadcasts(
        out_tx,
        station_id,
        &mut status_rx,
        &mut queue_rx,
        || !matches!(get_streamer(streamers, &station_id), Some(current) if Arc::ptr_eq(&current, &streamer)),
    )
    .await
}

/// Forwards live status/queue broadcasts of the attached runtime until it
/// is replaced/removed ([`ForwardOutcome::Reattach`]) or the connection
/// dies ([`ForwardOutcome::ConnectionGone`]).
///
/// `runtime_gone` reports whether the runtime we attached to is no longer
/// the one mapped for the station; it is re-evaluated on the periodic map
/// recheck. `RecvError::Closed` on a feed cannot happen while this runtime
/// is alive (the only senders are owned by the streamer Arc we hold) and is
/// answered with a defensive re-attach, never `ConnectionGone`.
///
/// The select observes `out_tx.closed()` directly: when the client
/// disconnects while the runtime is stable and emits nothing, there is no
/// failed send and no replacement to notice — without this branch the task
/// would stay alive with no receiver indefinitely.
async fn forward_live_broadcasts<F>(
    out_tx: &mpsc::UnboundedSender<Outbound>,
    station_id: Uuid,
    status_rx: &mut broadcast::Receiver<StatusEvent>,
    queue_rx: &mut broadcast::Receiver<String>,
    runtime_gone: F,
) -> ForwardOutcome
where
    F: Fn() -> bool,
{
    let mut recheck = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            event = status_rx.recv() => match event {
                Ok(event) => {
                    if out_tx.send(Outbound::Status { station_id, data: event }).is_err() {
                        return ForwardOutcome::ConnectionGone;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    // Unreachable in production: the only senders are owned
                    // by the StationStreamer Arc this task holds, so the
                    // feed cannot close while the runtime is alive. Defend
                    // cheaply — re-attach, never ConnectionGone (the
                    // connection itself is fine).
                    tracing::warn!(station_id = %station_id, "status broadcast closed while streamer alive");
                    return ForwardOutcome::Reattach;
                }
            },
            msg = queue_rx.recv() => match msg {
                Ok(raw) => {
                    let data = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
                    if out_tx.send(Outbound::QueueUpdate { station_id, data }).is_err() {
                        return ForwardOutcome::ConnectionGone;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    // Same defensive handling as the status feed: unreachable
                    // while the owning streamer is alive; re-attach.
                    tracing::warn!(station_id = %station_id, "queue broadcast closed while streamer alive");
                    return ForwardOutcome::Reattach;
                }
            },
            _ = recheck.tick() => {
                if runtime_gone() {
                    // Runtime replaced (or gone): re-attach with a fresh
                    // snapshot — current status included, not only queue.
                    return ForwardOutcome::Reattach;
                }
            }
            // The connection's output channel is gone (the client
            // disconnected): end the task immediately. A stable runtime
            // emits nothing, so waiting for a failed send would leak the
            // task forever.
            _ = out_tx.closed() => return ForwardOutcome::ConnectionGone,
        }
    }
}

fn get_streamer(streamers: &StreamersMap, station_id: &Uuid) -> Option<Arc<StationStreamer>> {
    let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    map.get(station_id).cloned()
}

/// Read-only queue snapshot straight from the database, using the same queue
/// repository the streamers feed their broadcasts from. Never creates a
/// runtime — this is what Subscribe uses for stations without one.
async fn db_queue_snapshot(db: &PgPool, upload_dir: &str, station_id: Uuid) -> Option<serde_json::Value> {
    let raw = crate::streamer::queue_repository::QueueRepository::new(db.clone(), station_id, upload_dir.to_string())
        .queue_json()
        .await;
    serde_json::from_str(&raw).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn forward_station_ends_when_output_channel_is_closed() {
        // A dead connection must terminate the forwarding task instead of
        // spinning: with the receiver already dropped, `forward_station`
        // must return immediately and never reach the no-runtime loop (DB
        // lookups) nor re-attach. The pool is deliberately lazy with an
        // unreachable address — any attempt to use it would hang the test
        // and trip the timeout.
        let (out_tx, rx) = mpsc::unbounded_channel::<Outbound>();
        drop(rx);
        let db = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://127.0.0.1:1/none")
            .expect("lazy pool must not connect eagerly");
        let streamers = StreamersMap::default();
        let lifecycle = Arc::new(StationLifecycleLocks::default());
        tokio::time::timeout(
            Duration::from_secs(5),
            forward_station(db, "/tmp/surcast-upload".into(), streamers, lifecycle, Uuid::new_v4(), out_tx),
        )
        .await
        .expect("forward_station must terminate with a closed output channel");
    }

    #[tokio::test]
    async fn live_forwarding_ends_when_output_channel_closes_during_active_runtime() {
        // The problematic leak: a subscriber attached to a STABLE runtime
        // that never emits another status/queue event. The client vanishes,
        // so no send can fail and the periodic recheck keeps seeing the
        // same runtime — only the direct `out_tx.closed()` branch can end
        // the task. Without it, the future would sit in the select forever
        // and trip the timeout.
        let (status_tx, status_rx) = broadcast::channel::<StatusEvent>(16);
        let (queue_tx, queue_rx) = broadcast::channel::<String>(16);
        let (out_tx, rx) = mpsc::unbounded_channel::<Outbound>();
        let station_id = Uuid::new_v4();

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let mut status_rx = status_rx;
            let mut queue_rx = queue_rx;
            let task = tokio::spawn(async move {
                forward_live_broadcasts(&out_tx, station_id, &mut status_rx, &mut queue_rx, || {
                    // A stable runtime: the map check would always match.
                    false
                })
                .await
            });
            // The output side of the connection dies while the forwarding
            // future is (about to be) inside the select; the channel close
            // must end it without any runtime event or replacement.
            drop(rx);
            task.await.expect("forwarding future must terminate")
        })
        .await
        .expect("live forwarding must end after the output channel closes");

        assert_eq!(outcome, ForwardOutcome::ConnectionGone);
        // The runtime's broadcast senders are still alive and never fired:
        // nothing else could have ended the loop.
        let _ = status_tx.send(StatusEvent::State {
            playing: false,
            song_index: 0,
            total: 0,
            elapsed: 0,
            title: String::new(),
            artist: String::new(),
            duration: 0,
        });
        let _ = queue_tx.send("[]".into());
    }
}
