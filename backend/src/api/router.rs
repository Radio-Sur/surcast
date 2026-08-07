use axum::extract::DefaultBodyLimit;
use axum::http::header;
use axum::http::{StatusCode, Uri};
use axum::middleware;
use axum::response::{Html, IntoResponse};
use axum::routing::{delete, get, post, put};
use axum::Router;
use sqlx::PgPool;
use std::path::Path;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use uuid::Uuid;

use crate::api_keys;
use crate::auth;
use crate::config::Config;
use crate::icecast;
use crate::icecast::IcecastManager;
use crate::listeners;
use crate::listeners::ListenersState;
use crate::playlists;
use crate::scheduling;
use crate::songs;
use crate::stations;
use crate::streamer::StationStreamer;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type StreamersMap = Arc<Mutex<HashMap<Uuid, Arc<StationStreamer>>>>;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub config: Config,
    pub streamers: StreamersMap,
    pub icecast_manager: IcecastManager,
    pub listeners: Arc<ListenersState>,
}

use axum::extract::FromRef;

impl FromRef<AppState> for PgPool {
    fn from_ref(state: &AppState) -> Self {
        state.db.clone()
    }
}

impl FromRef<AppState> for Config {
    fn from_ref(state: &AppState) -> Self {
        state.config.clone()
    }
}

impl FromRef<AppState> for StreamersMap {
    fn from_ref(state: &AppState) -> Self {
        state.streamers.clone()
    }
}

impl FromRef<AppState> for IcecastManager {
    fn from_ref(state: &AppState) -> Self {
        state.icecast_manager.clone()
    }
}

impl FromRef<AppState> for Arc<ListenersState> {
    fn from_ref(state: &AppState) -> Self {
        state.listeners.clone()
    }
}

pub fn create_router(db: PgPool, config: Config, icecast_manager: IcecastManager, listeners: Arc<ListenersState>) -> Router {
    let state = AppState {
        db,
        config,
        streamers: Arc::new(Mutex::new(HashMap::new())),
        icecast_manager,
        listeners,
    };

    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);

    let protected_routes = Router::new()
        .route("/api/auth/me", get(auth::handlers::me))
        .route("/api/users", get(auth::handlers::list_users))
        .route(
            "/api/stations",
            get(stations::handlers::list_stations).post(stations::handlers::create_station),
        )
        .route(
            "/api/stations/:id",
            get(stations::handlers::get_station)
                .put(stations::handlers::update_station)
                .delete(stations::handlers::delete_station),
        )
        .route(
            "/api/api-keys",
            get(api_keys::handlers::list_api_keys).post(api_keys::handlers::create_api_key),
        )
        .route(
            "/api/api-keys/:id",
            put(api_keys::handlers::update_api_key).delete(api_keys::handlers::delete_api_key),
        )
        .route(
            "/api/songs",
            get(songs::handlers::list_songs)
                .post(songs::handlers::upload_song)
                .layer(DefaultBodyLimit::max(500 * 1024 * 1024)),
        )
        .route(
            "/api/songs/zip",
            post(songs::handlers::upload_zip).layer(DefaultBodyLimit::max(500 * 1024 * 1024)),
        )
        .route("/api/songs/search", get(songs::handlers::search_songs))
        .route("/api/songs/artists", get(songs::handlers::list_artists))
        .route("/api/songs/count", post(songs::handlers::count_songs))
        .route("/api/songs/batch", delete(songs::handlers::delete_songs_batch))
        .route(
            "/api/songs/:id",
            get(songs::handlers::get_song)
                .put(songs::handlers::update_song)
                .delete(songs::handlers::delete_song),
        )
        .route("/api/songs/:id/stations", post(songs::handlers::add_song_stations))
        .route("/api/songs/:id/stations/:station_id", delete(songs::handlers::remove_song_station))
        .route(
            "/api/stations/:id/songs",
            get(stations::handlers::list_station_songs).post(stations::handlers::add_station_songs),
        )
        .route("/api/stations/:id/songs/:song_id", delete(stations::handlers::remove_station_song))
        .route(
            "/api/stations/:id/queue",
            get(stations::handlers::list_queue).post(stations::handlers::add_songs_to_queue),
        )
        .route("/api/stations/:id/queue/reorder", put(stations::handlers::reorder_queue))
        .route(
            "/api/stations/:id/queue/insert",
            post(stations::handlers::insert_song_at_queue_position),
        )
        .route(
            "/api/stations/:id/queue/playlist/:playlist_id",
            delete(stations::handlers::remove_playlist_songs_from_queue),
        )
        .route(
            "/api/stations/:id/queue/:song_id",
            delete(stations::handlers::remove_song_from_queue),
        )
        .route("/api/stations/:id/playlist", get(stations::handlers::get_station_playlist_m3u))
        .route("/api/stations/:id/stream/status", get(stations::handlers::stream_status))
        .route("/api/stations/:id/stream/skip", post(stations::handlers::stream_skip))
        .route("/api/stations/:id/stream/play", post(stations::handlers::stream_play))
        .route("/api/stations/:id/stream/pause", post(stations::handlers::stream_pause))
        .route("/api/stations/:id/stream/stop", post(stations::handlers::stream_stop))
        .route("/api/stations/:id/stream/restart", post(stations::handlers::stream_restart))
        .route("/api/stations/:id/listeners/live", get(listeners::handlers::station_listeners_live))
        .route(
            "/api/stations/:id/listeners/history",
            get(listeners::handlers::station_listeners_history),
        )
        .route("/api/listeners/overview", get(listeners::handlers::listeners_overview))
        .route(
            "/api/playlists",
            get(playlists::handlers::list_playlists).post(playlists::handlers::create_playlist),
        )
        .route(
            "/api/playlists/:id",
            get(playlists::handlers::get_playlist)
                .put(playlists::handlers::update_playlist)
                .delete(playlists::handlers::delete_playlist),
        )
        .route(
            "/api/playlists/:id/songs",
            get(playlists::handlers::list_playlist_songs).post(playlists::handlers::add_playlist_songs),
        )
        .route("/api/playlists/:id/songs/reorder", put(playlists::handlers::reorder_playlist_songs))
        .route(
            "/api/playlists/:id/songs/batch",
            delete(playlists::handlers::remove_playlist_songs_batch),
        )
        .route(
            "/api/playlists/:id/songs/:song_id",
            delete(playlists::handlers::remove_playlist_song),
        )
        .route(
            "/api/playlists/:id/add-to-queue/:station_id",
            post(playlists::handlers::add_playlist_to_queue),
        )
        .route(
            "/api/stations/:id/schedule-events",
            get(scheduling::handlers::list_schedule_events).post(scheduling::handlers::create_schedule_event),
        )
        .route(
            "/api/stations/:id/schedule-events/:event_id",
            put(scheduling::handlers::update_schedule_event).delete(scheduling::handlers::delete_schedule_event),
        )
        .route(
            "/api/stations/:id/schedules",
            get(scheduling::handlers::list_schedules).post(scheduling::handlers::create_schedule),
        )
        .route(
            "/api/stations/:id/schedules/:schedule_id",
            put(scheduling::handlers::update_schedule).delete(scheduling::handlers::delete_schedule),
        )
        .route(
            "/api/stations/:id/auto-fill",
            get(scheduling::handlers::get_auto_fill).put(scheduling::handlers::update_auto_fill),
        )
        .route(
            "/api/stations/:id/auto-fill/playlists",
            post(scheduling::handlers::add_auto_fill_playlist),
        )
        .route(
            "/api/stations/:id/auto-fill/playlists/:playlist_id",
            put(scheduling::handlers::update_auto_fill_playlist).delete(scheduling::handlers::delete_auto_fill_playlist),
        )
        .route("/api/stations/:id/auto-fill/trigger", post(scheduling::handlers::trigger_auto_fill))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth::middleware::auth_middleware));

    let admin_routes = Router::new()
        .route(
            "/api/users/:id",
            put(auth::handlers::update_user).delete(auth::handlers::delete_user),
        )
        .route(
            "/api/admin/icecast",
            get(icecast::handlers::get_settings).patch(icecast::handlers::patch_settings),
        )
        .route("/api/admin/icecast/start", post(icecast::handlers::start_icecast))
        .route("/api/admin/icecast/stop", post(icecast::handlers::stop_icecast))
        .route("/api/admin/icecast/test", post(icecast::handlers::test_connection))
        .route_layer(middleware::from_fn(auth::middleware::require_admin))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth::middleware::auth_middleware));

    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/config", get(config_info))
        .route("/api/setup/status", get(auth::handlers::setup_status))
        .route("/api/setup/init", axum::routing::post(auth::handlers::setup_init))
        .route("/api/auth/login", axum::routing::post(auth::handlers::login))
        .route("/api/auth/refresh", axum::routing::post(auth::handlers::refresh))
        .route("/api/songs/:id/file", get(songs::handlers::serve_song_file))
        .route("/api/songs/:id/cover", get(songs::handlers::serve_song_cover))
        .route("/api/ws", get(stations::handlers::global_ws))
        .merge(protected_routes)
        .merge(admin_routes)
        .route("/api/docs/openapi.json", get(openapi_json))
        .route("/api/docs", get(docs_page))
        .fallback(fallback_handler)
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

async fn health_check() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "status": "ok" }))
}

async fn config_info(axum::extract::State(config): axum::extract::State<Config>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "icecast_public_url": config.icecast_public_url }))
}

async fn docs_page() -> Html<&'static str> {
    Html(include_str!("../../static/docs.html"))
}

async fn openapi_json() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::from_str(include_str!("../../static/openapi.json")).expect("Invalid openapi.json"))
}

async fn fallback_handler(uri: Uri) -> impl axum::response::IntoResponse {
    let path = uri.path();
    if path.starts_with("/api/") {
        (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({ "error": format!("Route not found: {path}") })),
        )
            .into_response()
    } else if path.contains("..") {
        (StatusCode::NOT_FOUND, "Not found").into_response()
    } else {
        let root = Path::new("../frontend/dist");
        let file_path = root.join(path.trim_start_matches('/'));
        match tokio::fs::read(&file_path).await {
            Ok(bytes) => {
                let content_type = mime_guess::from_path(&file_path).first_or_octet_stream();
                (StatusCode::OK, [(header::CONTENT_TYPE, content_type.as_ref())], bytes).into_response()
            }
            Err(_) => match tokio::fs::read(root.join("index.html")).await {
                Ok(index) => (StatusCode::OK, [(header::CONTENT_TYPE, "text/html")], index).into_response(),
                Err(_) => (StatusCode::NOT_FOUND, "Not found").into_response(),
            },
        }
    }
}
