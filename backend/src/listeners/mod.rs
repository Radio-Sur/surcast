pub mod handlers;
pub mod models;
pub mod poller;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

/// Poll interval for the Icecast stats endpoint.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30);
/// How long raw listener samples are kept before pruning.
pub const RETENTION_DAYS: i64 = 90;

/// Latest known listener count for a single station.
#[derive(Debug, Clone, Serialize)]
pub struct LiveCount {
    pub listeners: i32,
    pub updated_at: DateTime<Utc>,
    pub online: bool,
}

/// Broadcast payload sent whenever a station's live count changes.
#[derive(Debug, Clone, Serialize)]
pub struct ListenerUpdate {
    pub station_id: Uuid,
    pub listeners: i32,
    pub updated_at: DateTime<Utc>,
    pub online: bool,
}

/// In-memory snapshot of live listener counts, fed by [`poller`] and
/// consumed by the global WebSocket and the REST handlers.
pub struct ListenersState {
    cache: RwLock<HashMap<Uuid, LiveCount>>,
    tx: broadcast::Sender<ListenerUpdate>,
}

impl ListenersState {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(128);
        Arc::new(Self {
            cache: RwLock::new(HashMap::new()),
            tx,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ListenerUpdate> {
        self.tx.subscribe()
    }

    pub fn publish(&self, update: ListenerUpdate) {
        self.cache
            .try_write()
            .map(|mut cache| {
                cache.insert(
                    update.station_id,
                    LiveCount {
                        listeners: update.listeners,
                        updated_at: update.updated_at,
                        online: update.online,
                    },
                )
            })
            .ok();
        let _ = self.tx.send(update);
    }

    pub async fn live(&self, station_id: Uuid) -> Option<LiveCount> {
        self.cache.read().await.get(&station_id).cloned()
    }

    pub async fn live_all(&self) -> HashMap<Uuid, LiveCount> {
        self.cache.read().await.clone()
    }
}

/// Spawns the background poller task.
pub fn spawn_poller(db: PgPool, state: Arc<ListenersState>) {
    tokio::spawn(poller::run(db, state));
}
