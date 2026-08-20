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

/// How often the Icecast stats endpoint is polled for live UI updates.
pub const LIVE_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// How often live counts are persisted for historical charts.
pub const HISTORY_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
/// Maximum duration of one Icecast stats request.
pub const STATS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
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

    pub async fn publish(&self, update: ListenerUpdate) {
        let should_broadcast = {
            let mut cache = self.cache.write().await;
            let changed = cache
                .get(&update.station_id)
                .is_none_or(|current| current.listeners != update.listeners || current.online != update.online);
            cache.insert(
                update.station_id,
                LiveCount {
                    listeners: update.listeners,
                    updated_at: update.updated_at,
                    online: update.online,
                },
            );
            changed
        };
        if should_broadcast {
            let _ = self.tx.send(update);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn broadcasts_listener_changes_without_repeating_unchanged_counts() {
        let state = ListenersState::new();
        let mut updates = state.subscribe();
        let station_id = Uuid::new_v4();
        let updated_at = Utc::now();

        state
            .publish(ListenerUpdate {
                station_id,
                listeners: 0,
                updated_at,
                online: true,
            })
            .await;
        assert_eq!(updates.recv().await.unwrap().listeners, 0);

        state
            .publish(ListenerUpdate {
                station_id,
                listeners: 0,
                updated_at,
                online: true,
            })
            .await;
        assert!(tokio::time::timeout(Duration::from_millis(10), updates.recv()).await.is_err());

        state
            .publish(ListenerUpdate {
                station_id,
                listeners: 1,
                updated_at,
                online: true,
            })
            .await;
        assert_eq!(updates.recv().await.unwrap().listeners, 1);
        assert_eq!(state.live(station_id).await.unwrap().listeners, 1);
    }
}
