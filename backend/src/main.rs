use std::net::SocketAddr;
use surcast_backend::api;
use surcast_backend::config::Config;
use surcast_backend::db;
use surcast_backend::icecast;
use surcast_backend::icecast::models::IcecastMode;
use surcast_backend::listeners;
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
    let app = api::router::create_router(pool.clone(), config.clone(), icecast_manager.clone(), listeners_state);

    // Auto-start Icecast if enabled and managed
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

    let addr = SocketAddr::new(config.server_host.parse().expect("Invalid server host"), config.server_port);

    tracing::info!("Surcast backend listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind TCP listener — is port already in use?");

    let im = icecast_manager.clone();
    let shutdown = async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutting down...");
        let _ = im.stop().await;
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .expect("Server exited with error");
}
