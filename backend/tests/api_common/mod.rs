use axum::Router;
use sqlx::PgPool;

use surcast_backend::api::router;
use surcast_backend::config::Config;
use surcast_backend::icecast::IcecastManager;
use surcast_backend::listeners::ListenersState;

pub fn test_config() -> Config {
    Config {
        database_url: String::new(),
        jwt_secret: "test-secret-thirtytwo-chars-minimum!!".into(),
        jwt_access_expiry: 3600,
        jwt_refresh_expiry: 86400,
        server_host: "0.0.0.0".into(),
        server_port: 0,
        upload_dir: std::env::temp_dir().to_string_lossy().to_string(),
        lastfm_api_key: None,
    }
}

pub fn create_test_app(pool: PgPool) -> Router {
    let config = test_config();
    let icecast_manager = IcecastManager::new(std::path::PathBuf::from("../.icecast"));
    let listeners = ListenersState::new();
    router::create_router(pool, config, icecast_manager, listeners)
}
