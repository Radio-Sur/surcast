use axum::Router;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use surcast_backend::api::router;
use surcast_backend::config::Config;
use surcast_backend::icecast::IcecastManager;
use surcast_backend::listeners::ListenersState;

pub fn test_config() -> Config {
    // Keep each app's files isolated for its full lifetime.
    let music_base = tempfile::tempdir().expect("test music dir");
    let music_dir = music_base.path().join("music").to_string_lossy().to_string();
    std::mem::forget(music_base);
    Config {
        database_url: String::new(),
        jwt_secret: "test-secret-thirtytwo-chars-minimum!!".into(),
        jwt_access_expiry: 3600,
        jwt_refresh_expiry: 86400,
        server_host: "0.0.0.0".into(),
        server_port: 0,
        music_dir,
        lastfm_api_key: None,
        icecast_public_url: String::new(),
    }
}

#[allow(dead_code)]
pub fn create_test_app(pool: PgPool) -> Router {
    create_test_app_with_config(pool, test_config())
}

pub fn create_test_app_with_config(pool: PgPool, config: Config) -> Router {
    let icecast_manager = IcecastManager::new(std::path::Path::new(&config.music_dir).join(".icecast-test"));
    let listeners = ListenersState::new();
    let lifecycle = Arc::new(surcast_backend::stations::handlers::stream::StationLifecycleLocks::default());
    router::create_router(
        pool,
        config,
        Arc::new(Mutex::new(HashMap::new())),
        lifecycle,
        icecast_manager,
        listeners,
    )
}
