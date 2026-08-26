use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use surcast_backend::api;
use surcast_backend::api::router::StreamersMap;
use surcast_backend::config::Config;
use surcast_backend::db;
use surcast_backend::icecast;
use surcast_backend::icecast::models::IcecastMode;
use surcast_backend::listeners;
use surcast_backend::stations::handlers::stream as stream_handlers;
use surcast_backend::stations::handlers::stream::StationLifecycleLocks;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let _ = dotenvy::from_path("../.env");

    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let config = Config::from_env();
    let pool = db::create_pool(&config.database_url).await;
    db::run_migrations(&pool).await;

    let icecast_dir = std::env::var("ICECAST_DIR").unwrap_or_else(|_| "../.icecast".into());
    let icecast_dir = std::path::Path::new(&icecast_dir);
    let icecast_dir = std::env::current_dir()
        .ok()
        .map(|cwd| cwd.join(icecast_dir))
        .and_then(|p| p.canonicalize().ok())
        .unwrap_or_else(|| icecast_dir.to_path_buf());
    let icecast_manager = icecast::IcecastManager::new(icecast_dir);
    let listeners_state = listeners::ListenersState::new();
    listeners::spawn_poller(pool.clone(), listeners_state.clone());
    let streamers: StreamersMap = Arc::new(Mutex::new(HashMap::new()));
    let lifecycle = Arc::new(StationLifecycleLocks::default());
    let app = api::router::create_router(
        pool.clone(),
        config.clone(),
        streamers.clone(),
        lifecycle.clone(),
        icecast_manager.clone(),
        listeners_state,
    );

    let settings = icecast::models::get_settings(&pool).await;
    if let Ok(settings) = settings {
        if settings.enabled && settings.mode == IcecastMode::Managed {
            match icecast_manager
                .start(
                    settings.port,
                    &settings.source_password,
                    &settings.admin_user,
                    &settings.admin_password,
                )
                .await
            {
                Ok(msg) => tracing::info!("{msg}"),
                Err(e) => tracing::warn!("Icecast auto-start failed: {e}"),
            }
        }
    }

    // Startup restore: every station persisted as started is started again,
    // after the managed Icecast had its chance to come up. A single failing
    // station is logged and skipped — it must not block the boot.
    stream_handlers::restore_started_stations(&pool, &streamers, &lifecycle, &config.upload_dir).await;

    let addr = SocketAddr::new(config.server_host.parse().expect("Invalid server host"), config.server_port);

    tracing::info!("Surcast backend listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind TCP listener — is port already in use?");

    let im = icecast_manager.clone();
    let shutdown_streamers = streamers.clone();
    let shutdown = async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutting down...");
        let active = {
            let mut streamers = shutdown_streamers.lock().unwrap_or_else(|error| error.into_inner());
            streamers.drain().map(|(_, streamer)| streamer).collect::<Vec<_>>()
        };
        futures::future::join_all(active.into_iter().map(|streamer| async move {
            streamer.shutdown().await;
        }))
        .await;
        let _ = im.stop().await;
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .expect("Server exited with error");
}
