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
use crate::listeners::{ListenerUpdate, ListenersState};
use crate::streamer::{StationStreamer, StatusEvent};

use super::stream::{get_or_create_streamer_for_station, resolve_station_id};

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
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Outbound {
    AuthOk,
    Error {
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

    Ok(ws.on_upgrade(move |socket| handle_connection(socket, db, streamers, upload_dir, jwt_secret, token_from_query, listeners)))
}

async fn handle_connection(
    socket: WebSocket,
    db: PgPool,
    streamers: StreamersMap,
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
    let mut listeners_task = tokio::spawn(forward_listeners(listeners.subscribe(), out_tx.clone()));
    let mut recv_task = tokio::spawn(ws_recv_task(receiver, db, streamers, upload_dir, jwt_secret, out_tx));

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
        .send(Message::Text(serde_json::json!({"type": "auth_ok"}).to_string()))
        .await
        .map_err(|_| "send failed".to_string())
}

fn error_msg(data: &str) -> Message {
    Message::Text(serde_json::json!({"type":"error","data":data}).to_string())
}

/// Drains the per-connection outbound queue and forwards it to the socket,
/// interleaving heartbeat pings.
async fn ws_send_task(mut sender: SplitSink<WebSocket, Message>, mut out_rx: mpsc::UnboundedReceiver<Outbound>) {
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if sender.send(Message::Ping(vec![])).await.is_err() {
                    return;
                }
            }
            msg = out_rx.recv() => {
                match msg {
                    Some(out) => {
                        let text = serde_json::to_string(&out).unwrap_or_default();
                        if sender.send(Message::Text(text)).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
        }
    }
}

/// Forwards global listener-count updates to a single connection.
async fn forward_listeners(mut rx: broadcast::Receiver<ListenerUpdate>, out_tx: mpsc::UnboundedSender<Outbound>) {
    loop {
        match rx.recv().await {
            Ok(update) => {
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
                        data: "unauthorized".into(),
                    });
                }
            }
            Inbound::Subscribe { station_id } => {
                let station_id = match resolve_station_id(&db, &station_id).await {
                    Ok(id) => id,
                    Err(_) => {
                        let _ = out_tx.send(Outbound::Error {
                            data: "unknown station".into(),
                        });
                        continue;
                    }
                };
                if subs.contains_key(&station_id) {
                    continue;
                }
                let streamer = match get_or_create_streamer_for_station(&db, &streamers, &upload_dir, station_id).await {
                    Ok(s) => s,
                    Err(_) => {
                        let _ = out_tx.send(Outbound::Error {
                            data: "no active stream".into(),
                        });
                        continue;
                    }
                };
                let handle = tokio::spawn(forward_station(streamer.clone(), station_id, out_tx.clone()));
                subs.insert(station_id, handle);
                let _ = out_tx.send(Outbound::Status {
                    station_id,
                    data: streamer.status(),
                });
                streamer.push_queue_update().await;
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
                        s.skip().await;
                    }
                }
            }
            Inbound::Play { station_id } => {
                if let Ok(id) = resolve_station_id(&db, &station_id).await {
                    if let Some(s) = get_streamer(&streamers, &id) {
                        s.play();
                    }
                }
            }
            Inbound::Pause { station_id } => {
                if let Ok(id) = resolve_station_id(&db, &station_id).await {
                    if let Some(s) = get_streamer(&streamers, &id) {
                        s.pause();
                    }
                }
            }
        }
    }

    for (_, handle) in subs {
        handle.abort();
    }
}

/// Forwards a single station's status + queue broadcasts to the connection.
async fn forward_station(streamer: Arc<StationStreamer>, station_id: Uuid, out_tx: mpsc::UnboundedSender<Outbound>) {
    let mut status_rx = streamer.subscribe_status();
    let mut queue_rx = streamer.subscribe_queue();

    loop {
        tokio::select! {
            event = status_rx.recv() => match event {
                Ok(event) => {
                    if out_tx.send(Outbound::Status { station_id, data: event }).is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            },
            msg = queue_rx.recv() => match msg {
                Ok(raw) => {
                    let data = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
                    if out_tx.send(Outbound::QueueUpdate { station_id, data }).is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}

fn get_streamer(streamers: &StreamersMap, station_id: &Uuid) -> Option<Arc<StationStreamer>> {
    let map = streamers.lock().unwrap_or_else(|e| e.into_inner());
    map.get(station_id).cloned()
}
