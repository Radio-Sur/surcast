//! Sandbox control plane. Data-plane handlers run unchanged against a dedicated
//! pool whose search_path contains ONLY the sandbox schema (never public).
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, Executor, PgPool};
use tokio::sync::{watch, Mutex, RwLock};
use tower::Service;
use uuid::Uuid;

use crate::api::router::{router_for_state, AppState};
use crate::auth::{middleware::verify_token, models::Role, repository};
use crate::errors::{AppError, DbResult};

type Slot = Arc<RwLock<Option<Arc<Workspace>>>>;

#[derive(Default)]
pub struct Registry {
    slots: Mutex<HashMap<Uuid, Slot>>,
}

impl Registry {
    async fn slot(&self, id: Uuid) -> Slot {
        self.slots.lock().await.entry(id).or_default().clone()
    }

    pub async fn shutdown(&self) {
        let slots: Vec<_> = self.slots.lock().await.values().cloned().collect();
        for slot in slots {
            let mut guard = slot.write().await;
            if let Some(workspace) = guard.take() {
                let _operations = workspace.gate.write().await;
                workspace.cancelled.send_replace(true);
                let active: Vec<_> = workspace
                    .streamers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .drain()
                    .map(|(_, streamer)| streamer)
                    .collect();
                for streamer in active {
                    streamer.shutdown().await;
                }
                workspace.db.close().await;
            }
        }
    }
}

pub struct Workspace {
    pub gate: Arc<RwLock<()>>,
    pub generation: Uuid,
    pub db: PgPool,
    pub music_dir: String,
    pub cancelled: watch::Sender<bool>,
    pub streamers: crate::api::StreamersMap,
    pub lifecycle: Arc<crate::stations::handlers::stream::StationLifecycleLocks>,
    pub listeners: Arc<crate::listeners::ListenersState>,
}

fn schema(generation: Uuid) -> String {
    format!("demo_{}", generation.simple())
}

fn music_dir(state: &AppState, user: Uuid) -> String {
    // Sibling, NOT a child of MUSIC_DIR: production's recursive scanner must
    // never discover sandbox copies.
    format!("{}.sandboxes/{user}", state.config.music_dir.trim_end_matches('/'))
}

pub fn unavailable() -> AppError {
    AppError::forbidden("This feature is unavailable in demo mode".into()).with_code("DEMO_RESTRICTED")
}

pub async fn ready(db: &PgPool, user: Uuid) -> Result<bool, AppError> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM demo_sandboxes WHERE user_id=$1 AND ready)")
        .bind(user)
        .fetch_one(db)
        .await
        .db_error("check demo readiness")
}

async fn open_pool(state: &AppState, generation: Uuid) -> Result<PgPool, AppError> {
    let options = state
        .auth_db
        .connect_options()
        .as_ref()
        .clone()
        .options([("search_path", format!("{},pg_catalog", schema(generation)))]);
    PgPoolOptions::new()
        .max_connections(3)
        .connect_with(options)
        .await
        .db_error("open sandbox database")
}

pub async fn workspace(state: &AppState, user: Uuid) -> Result<Arc<Workspace>, AppError> {
    let slot = state.demos.slot(user).await;
    let mut guard = slot.write().await;
    let stored: Option<(Uuid, Option<String>)> =
        sqlx::query_as("SELECT generation, seed->>'library_id' FROM demo_sandboxes WHERE user_id=$1 AND ready")
            .bind(user)
            .fetch_optional(&state.auth_db)
            .await
            .db_error("load sandbox")?;
    let (generation, library_id) =
        stored.ok_or_else(|| AppError::forbidden("Demo sandbox is not yet configured".into()).with_code("DEMO_NOT_READY"))?;
    if let Some(current) = guard.as_ref().filter(|w| w.generation == generation) {
        return Ok(current.clone());
    }
    let db = open_pool(state, generation).await?;
    let mut library_dir = std::path::PathBuf::from(music_dir(state, user));
    if let Some(library_id) = library_id {
        let library_id: Uuid = library_id
            .parse()
            .map_err(|_| AppError::internal("Invalid sandbox library identifier".into()))?;
        library_dir.push(library_id.to_string());
    }
    // A sandbox does not auto-broadcast after a server restart. Only a guest's
    // explicit Play starts its isolated output again.
    sqlx::query("UPDATE stations SET is_started=false")
        .execute(&db)
        .await
        .db_error("initialize demo playback")?;
    let (cancelled, _) = watch::channel(false);
    let result = Arc::new(Workspace {
        gate: Arc::new(RwLock::new(())),
        generation,
        db,
        music_dir: library_dir.to_string_lossy().into_owned(),
        cancelled,
        streamers: Arc::new(std::sync::Mutex::new(HashMap::new())),
        lifecycle: Arc::new(Default::default()),
        listeners: crate::listeners::ListenersState::new(),
    });
    *guard = Some(result.clone());
    Ok(result)
}

pub fn scoped(state: &AppState, user: Uuid, workspace: &Workspace) -> AppState {
    let mut state = state.clone();
    state.db = workspace.db.clone();
    state.config.music_dir = workspace.music_dir.clone();
    state.streamers = workspace.streamers.clone();
    state.lifecycle = workspace.lifecycle.clone();
    state.listeners = workspace.listeners.clone();
    state.sandbox = Some(user);
    state
}

fn allowed(method: &str, path: &str) -> bool {
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    match parts.as_slice() {
        ["api", "auth", "me"] => method == "GET",
        ["api", "songs"] | ["api", "songs", "search" | "artists"] => method == "GET",
        ["api", "songs", "count"] => method == "POST",
        ["api", "songs", _, "file" | "cover" | "details"] | ["api", "songs", _] => method == "GET",
        ["api", "files"] | ["api", "files", "folders"] => method == "GET",
        ["api", "listeners", "overview"] => method == "GET",
        ["api", "admin", "icecast"] => method == "GET",
        ["api", "stations"] => method == "GET",
        ["api", "stations", _] => matches!(method, "GET" | "PUT"),
        ["api", "stations", _, "songs" | "playlist"] => method == "GET",
        ["api", "stations", _, "stream", "status"] => method == "GET",
        ["api", "stations", _, "stream", "play" | "pause" | "stop" | "skip" | "restart"] => method == "POST",
        ["api", "stations", _, "listeners", "live" | "history"] => method == "GET",
        ["api", "stations", _, "auto-fill", ..]
        | ["api", "stations", _, "schedules", ..]
        | ["api", "stations", _, "schedule-events", ..] => true,
        ["api", "stations", _, "queue", ..] => true,
        ["api", "playlists", ..] => true,
        _ => false,
    }
}

pub async fn dispatch(State(state): State<AppState>, mut req: Request<Body>, next: Next) -> Response {
    let path = req.uri().path().to_owned();
    let media = path.starts_with("/api/songs/") && (path.ends_with("/cover") || path.ends_with("/file"));
    if media && !req.headers().contains_key("Authorization") {
        if let Ok(Query(params)) = Query::<HashMap<String, String>>::try_from_uri(req.uri()) {
            if let Some(token) = params.get("token") {
                if let Ok(header) = format!("Bearer {token}").parse() {
                    req.headers_mut().insert("Authorization", header);
                }
            }
        }
    }
    // WebSockets authenticate their first frame and select their own scope.
    if !path.starts_with("/api/")
        || matches!(
            path.as_str(),
            "/api/ws" | "/api/config" | "/api/health" | "/api/auth/login" | "/api/auth/refresh"
        )
    {
        return next.run(req).await;
    }
    let token = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let Some(verified) = token.and_then(|t| verify_token(t, &state.config.jwt_secret).ok()) else {
        return next.run(req).await;
    };
    let user = match repository::find_user_by_id(&state.auth_db, verified.id).await {
        Ok(Some(user)) => user,
        Ok(None) => return AppError::unauthorized("User not found".into()).into_response(),
        Err(error) => return error.into_response(),
    };
    if user.role != Role::Demo {
        return next.run(req).await;
    }
    if !allowed(req.method().as_str(), &path) {
        return unavailable().into_response();
    }
    let workspace = match workspace(&state, user.id).await {
        Ok(workspace) => workspace,
        Err(error) => return error.into_response(),
    };
    let slot = state.demos.slot(user.id).await;
    let _guard = slot.read().await;
    let _operation = workspace.gate.read().await;
    if *workspace.cancelled.borrow() {
        return AppError::forbidden("Sandbox is being reset; refresh the page".into())
            .with_code("DEMO_RESET")
            .into_response();
    }
    // /me is always read from the authoritative user table, not the seed.
    if path == "/api/auth/me" {
        return next.run(req).await;
    }
    let mut router = router_for_state(scoped(&state, user.id, &workspace));
    // The root router already extracted URL parameters. Do not accumulate
    // them a second time when routing the request into the sandbox.
    req.extensions_mut().clear();
    match Box::pin(router.call(req)).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configure {
    pub station_id: Uuid,
    #[serde(default)]
    pub playlist_ids: Vec<Uuid>,
}

pub async fn status(State(state): State<AppState>, Path(user): Path<Uuid>) -> Result<Json<Value>, AppError> {
    ensure_demo(&state.auth_db, user).await?;
    let row: Option<(bool, Value)> = sqlx::query_as("SELECT ready, seed FROM demo_sandboxes WHERE user_id=$1")
        .bind(user)
        .fetch_optional(&state.auth_db)
        .await
        .db_error("sandbox status")?;
    Ok(Json(match row {
        Some((ready, seed)) => {
            json!({"ready":ready, "songs":seed["songs"].as_array().map(Vec::len).unwrap_or(0), "station":seed["stations"][0]["name"], "playlists":seed["playlists"].as_array().map(Vec::len).unwrap_or(0)})
        }
        None => json!({"ready":false}),
    }))
}

async fn ensure_demo(db: &PgPool, user: Uuid) -> Result<(), AppError> {
    let user = repository::find_user_by_id(db, user)
        .await?
        .ok_or_else(|| AppError::not_found("User not found".into()))?;
    if user.role != Role::Demo {
        return Err(AppError::bad_request("Sandbox requires a demo account".into()));
    }
    Ok(())
}

async fn rows(db: &PgPool, query: &str, ids: &[Uuid]) -> Result<Vec<Value>, AppError> {
    sqlx::query_scalar(query)
        .bind(ids)
        .fetch_all(db)
        .await
        .db_error("read sandbox seed")
}

fn remap(value: &mut Value, field: &str, map: &HashMap<Uuid, Uuid>) {
    if let Some(id) = value[field].as_str().and_then(|v| v.parse::<Uuid>().ok()) {
        value[field] = map.get(&id).map(|id| json!(id)).unwrap_or(Value::Null);
    }
}

#[axum::debug_handler]
pub async fn configure(
    State(state): State<AppState>,
    Path(user): Path<Uuid>,
    Json(config): Json<Configure>,
) -> Result<Json<Value>, AppError> {
    ensure_demo(&state.auth_db, user).await?;
    if config.playlist_ids.len() > 100 {
        return Err(AppError::bad_request("Select at most 100 playlists".into()));
    }
    let db = &state.auth_db;
    let mut stations = rows(db, "SELECT to_jsonb(t) FROM stations t WHERE id=ANY($1)", &[config.station_id]).await?;
    if stations.is_empty() {
        return Err(AppError::not_found("Source station not found".into()));
    }
    let mut playlists = rows(db, "SELECT to_jsonb(t) FROM playlists t WHERE id=ANY($1)", &config.playlist_ids).await?;
    if playlists.len() != config.playlist_ids.iter().collect::<HashSet<_>>().len() {
        return Err(AppError::not_found("Source playlist not found".into()));
    }
    let mut library = rows(
        db,
        "SELECT to_jsonb(t) FROM station_songs t WHERE station_id=ANY($1) ORDER BY position",
        &[config.station_id],
    )
    .await?;
    let mut queue = rows(
        db,
        "SELECT to_jsonb(t) FROM station_queue t WHERE station_id=ANY($1) ORDER BY position",
        &[config.station_id],
    )
    .await?;
    let mut playlist_songs = rows(
        db,
        "SELECT to_jsonb(t) FROM playlist_songs t WHERE playlist_id=ANY($1) ORDER BY position",
        &config.playlist_ids,
    )
    .await?;
    let ids: HashSet<Uuid> = library
        .iter()
        .chain(queue.iter())
        .chain(playlist_songs.iter())
        .filter_map(|r| r["song_id"].as_str()?.parse().ok())
        .collect();
    if ids.is_empty() || ids.len() > 1000 {
        return Err(AppError::bad_request("Sandbox must contain 1–1000 songs".into()));
    }
    let mut songs = rows(
        db,
        "SELECT to_jsonb(t) FROM songs t WHERE id=ANY($1) AND missing_since IS NULL",
        &ids.iter().copied().collect::<Vec<_>>(),
    )
    .await?;
    if songs.len() != ids.len() {
        return Err(AppError::bad_request("Some source songs are missing".into()));
    }
    let song_map: HashMap<Uuid, Uuid> = ids.into_iter().map(|id| (id, Uuid::new_v4())).collect();
    let playlist_map: HashMap<Uuid, Uuid> = config.playlist_ids.iter().map(|id| (*id, Uuid::new_v4())).collect();
    let station_id = Uuid::new_v4();
    // Each prepared seed gets immutable audio storage. Normal relative paths
    // are preserved without overwriting the active sandbox during provisioning.
    let library_id = Uuid::new_v4();
    let directory = std::path::PathBuf::from(music_dir(&state, user)).join(library_id.to_string());
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|e| AppError::internal(format!("Create sandbox library: {e}")))?;
    let source_root = tokio::fs::canonicalize(&state.config.music_dir)
        .await
        .map_err(|e| AppError::bad_request(format!("Music directory unavailable: {e}")))?;
    for song in &mut songs {
        let original_path = song["file_path"].as_str().unwrap_or_default();
        let source = crate::songs::handlers::resolve_audio_path(&state.config.music_dir, original_path);
        let source = tokio::fs::canonicalize(source)
            .await
            .map_err(|e| AppError::bad_request(format!("Source file unavailable: {e}")))?;
        if !source.starts_with(&source_root) {
            return Err(AppError::bad_request("Source file is outside the music library".into()));
        }
        let relative = if std::path::Path::new(original_path).is_absolute() {
            source
                .strip_prefix(&source_root)
                .map_err(|_| AppError::bad_request("Source file is outside the music library".into()))?
                .to_string_lossy()
                .into_owned()
        } else {
            crate::songs::files::validate_relative_path(original_path)?
        };
        let destination = crate::songs::files::resolve_library_path(&directory.to_string_lossy(), &relative)?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| AppError::internal(format!("Create sandbox audio folder: {e}")))?;
        }
        remap(song, "id", &song_map);
        tokio::fs::copy(&source, &destination)
            .await
            .map_err(|e| AppError::internal(format!("Copy sandbox audio: {e}")))?;
        song["file_path"] = json!(relative);
        song["uploaded_by"] = json!(user);
    }
    for station in &mut stations {
        station["id"] = json!(station_id);
        station["created_by"] = json!(user);
        station["is_started"] = json!(false);
        station["current_song_index"] = json!(0);
        station["current_queue_item_id"] = Value::Null;
        station["consumed_queue_item_ids"] = json!([]);
        station["current_queue_cursor_format"] = json!(0);
    }
    for playlist in &mut playlists {
        remap(playlist, "id", &playlist_map);
        playlist["created_by"] = json!(user);
    }
    for row in library.iter_mut().chain(queue.iter_mut()) {
        row["id"] = json!(Uuid::new_v4());
        row["station_id"] = json!(station_id);
        remap(row, "song_id", &song_map);
        if row.get("origin_playlist_id").is_some() {
            remap(row, "origin_playlist_id", &playlist_map);
        }
    }
    // Every selected song is available in the station library, including songs
    // originating only from the queue or a selected playlist.
    let library_ids: HashSet<Value> = library.iter().map(|r| r["song_id"].clone()).collect();
    for song in &songs {
        if !library_ids.contains(&song["id"]) {
            library.push(json!({"id":Uuid::new_v4(),"station_id":station_id,"song_id":song["id"],"position":library.len(),"created_at":chrono::Utc::now()}));
        }
    }
    if queue.is_empty() {
        queue = library.iter().enumerate().map(|(position,r)| json!({"id":Uuid::new_v4(),"station_id":station_id,"song_id":r["song_id"],"position":position,"is_auto_dj":false,"origin_playlist_id":null,"created_at":chrono::Utc::now()})).collect();
    }
    for row in &mut playlist_songs {
        row["id"] = json!(Uuid::new_v4());
        remap(row, "song_id", &song_map);
        remap(row, "playlist_id", &playlist_map);
    }
    if playlists.is_empty() {
        let playlist = Uuid::new_v4();
        let now = chrono::Utc::now();
        playlists.push(json!({"id":playlist,"name":"Demo","description":"Demo sandbox library","slug":"demo","created_by":user,"created_at":now,"updated_at":now}));
        playlist_songs = library.iter().enumerate().map(|(position, row)| json!({"id":Uuid::new_v4(),"playlist_id":playlist,"song_id":row["song_id"],"position":position,"created_at":now})).collect();
    }
    let mut owner: Value = sqlx::query_scalar("SELECT to_jsonb(t) FROM users t WHERE id=$1")
        .bind(user)
        .fetch_one(db)
        .await
        .db_error("read demo owner")?;
    // The local user row exists only for foreign keys. Authentication always
    // uses auth_db, so neither the seed nor the sandbox needs a password hash.
    owner["password_hash"] = json!("!sandbox-no-login");
    let seed = json!({"library_id":library_id,"users":[owner],"songs":songs,"stations":stations,"station_songs":library,"station_queue":queue,"playlists":playlists,"playlist_songs":playlist_songs});
    let owned = state.clone();
    tokio::spawn(async move { install(&owned, user, seed).await })
        .await
        .map_err(|_| AppError::internal("Sandbox provisioning task failed".into()))??;
    status(State(state), Path(user)).await
}

#[axum::debug_handler]
pub async fn reset(State(state): State<AppState>, Path(user): Path<Uuid>) -> Result<Json<Value>, AppError> {
    ensure_demo(&state.auth_db, user).await?;
    let seed: Value = sqlx::query_scalar("SELECT seed FROM demo_sandboxes WHERE user_id=$1 AND ready")
        .bind(user)
        .fetch_optional(&state.auth_db)
        .await
        .db_error("load sandbox seed")?
        .ok_or_else(|| AppError::bad_request("Configure the sandbox first".into()))?;
    let owned = state.clone();
    tokio::spawn(async move { install(&owned, user, seed).await })
        .await
        .map_err(|_| AppError::internal("Sandbox reset task failed".into()))??;
    status(State(state), Path(user)).await
}

/// Revoke sockets and stop the isolated output when an account is removed or
/// promoted. No sandbox process may outlive its account's permissions.
pub async fn retire(state: &AppState, user: Uuid) -> Result<(), AppError> {
    let slot = state.demos.slot(user).await;
    let mut guard = slot.write().await;
    let generation: Option<Uuid> = sqlx::query_scalar("DELETE FROM demo_sandboxes WHERE user_id=$1 RETURNING generation")
        .bind(user)
        .fetch_optional(&state.auth_db)
        .await
        .db_error("retire sandbox")?;
    if let Some(workspace) = guard.take() {
        let _operations = workspace.gate.write().await;
        workspace.cancelled.send_replace(true);
        let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM stations")
            .fetch_all(&workspace.db)
            .await
            .db_error("list sandbox stations")?;
        for id in ids {
            crate::stations::handlers::stream::stop_station(&workspace.db, &workspace.streamers, &workspace.lifecycle, id).await?;
        }
        workspace.db.close().await;
    }
    if let Some(generation) = generation {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema(generation)))
            .execute(&state.auth_db)
            .await
            .db_error("remove sandbox schema")?;
        let directory = std::path::PathBuf::from(music_dir(state, user));
        // This path is derived solely from the configured sandbox root and a
        // typed UUID, never from a request-supplied path or a source filename.
        match tokio::fs::remove_dir_all(directory).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(%user, %error, "could not remove retired sandbox audio"),
        }
    }
    Ok(())
}

fn install(
    state: &AppState,
    user: Uuid,
    seed: Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AppError>> + Send + '_>> {
    Box::pin(async move {
        let slot = state.demos.slot(user).await;
        let mut guard = slot.write().await;
        let generation = Uuid::new_v4();
        let schema = schema(generation);
        let mut tx = state.auth_db.begin().await.db_error("begin sandbox provisioning")?;
        // The per-user DB lock also serializes provisioning across server processes.
        let role: Option<Role> = sqlx::query_scalar("SELECT role FROM users WHERE id=$1 FOR UPDATE")
            .bind(user)
            .fetch_optional(&mut *tx)
            .await
            .db_error("lock sandbox owner")?;
        if role != Some(Role::Demo) {
            return Err(AppError::bad_request("Sandbox requires a demo account".into()));
        }
        let retired: Option<Uuid> = sqlx::query_scalar("SELECT generation FROM public.demo_sandboxes WHERE user_id=$1")
            .bind(user)
            .fetch_optional(&mut *tx)
            .await
            .db_error("read previous sandbox generation")?;
        (&mut *tx)
            .execute(format!("CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema}, public").as_str())
            .await
            .db_error("create sandbox schema")?;
        for migration in [
            include_str!("../migrations/0001_initial.sql"),
            include_str!("../migrations/0002_folder_cover_cache.sql"),
            include_str!("../migrations/0003_drop_user_name.sql"),
            include_str!("../migrations/0004_only_admin_and_demo_roles.sql"),
        ] {
            (&mut *tx).execute(migration).await.db_error("initialize sandbox schema")?;
        }
        sqlx::query(&format!(
            "ALTER TABLE {schema}.station_auto_fill ALTER COLUMN avoid_artist_repeat SET DEFAULT false"
        ))
        .execute(&mut *tx)
        .await
        .db_error("set sandbox AutoDJ defaults")?;
        for table in [
            "users",
            "songs",
            "stations",
            "playlists",
            "station_songs",
            "station_queue",
            "playlist_songs",
        ] {
            let mut data = seed[table].clone();
            if table == "stations" {
                for row in data.as_array_mut().unwrap() {
                    row["slug"] = json!(format!("demo-{}", user.simple()));
                }
            }
            sqlx::query(&format!(
                "INSERT INTO {schema}.{table} SELECT * FROM jsonb_populate_recordset(NULL::{schema}.{table}, $1)"
            ))
            .bind(data)
            .execute(&mut *tx)
            .await
            .db_error("populate sandbox")?;
        }
        // Connection details are copied from the real output service. The
        // configured station URL is preserved, including for sandbox playback.
        sqlx::query(&format!("DELETE FROM {schema}.icecast_settings"))
            .execute(&mut *tx)
            .await
            .db_error("prepare sandbox output")?;
        sqlx::query(&format!(
            "INSERT INTO {schema}.icecast_settings SELECT * FROM public.icecast_settings"
        ))
        .execute(&mut *tx)
        .await
        .db_error("configure sandbox output")?;
        sqlx::query("INSERT INTO public.demo_sandboxes(user_id,generation,seed,ready) VALUES($1,$2,$3,true) ON CONFLICT(user_id) DO UPDATE SET generation=$2,seed=$3,ready=true,updated_at=NOW()")
        .bind(user).bind(generation).bind(&seed).execute(&mut *tx).await.db_error("publish sandbox")?;
        tx.commit().await.db_error("commit sandbox")?;
        if let Some(previous) = guard.take() {
            let _operations = previous.gate.write().await;
            previous.cancelled.send_replace(true);
            let stations: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM stations")
                .fetch_all(&previous.db)
                .await
                .db_error("list old demo stations")?;
            for station in stations {
                crate::stations::handlers::stream::stop_station(&previous.db, &previous.streamers, &previous.lifecycle, station).await?;
            }
            previous.db.close().await;
        }
        if let Some(retired) = retired {
            // All admitted REST/WS operations finished, old sockets are cancelled,
            // and committed lifecycle operations were joined through their locks.
            sqlx::query(&format!("DROP SCHEMA IF EXISTS demo_{} CASCADE", retired.simple()))
                .execute(&state.auth_db)
                .await
                .db_error("retire sandbox schema")?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn demo_permissions_are_an_allowlist() {
        assert!(allowed("POST", "/api/stations/id/schedule-events"));
        assert!(allowed("PUT", "/api/stations/id/schedule-events/event"));
        assert!(allowed("DELETE", "/api/stations/id/schedules/schedule"));
        assert!(allowed("PUT", "/api/stations/id/auto-fill"));
        assert!(allowed("POST", "/api/stations/id/auto-fill/playlists"));
        assert!(allowed("POST", "/api/stations/id/stream/play"));
        assert!(allowed("PUT", "/api/stations/id/queue/reorder"));
        assert!(allowed("POST", "/api/playlists"));
        assert!(!allowed("POST", "/api/stations"));
        assert!(!allowed("DELETE", "/api/songs/id"));
        assert!(!allowed("POST", "/api/uploads"));
        assert!(allowed("GET", "/api/admin/icecast"));
        assert!(allowed("PUT", "/api/stations/id"));
        assert!(!allowed("PATCH", "/api/admin/icecast"));
        assert!(!allowed("POST", "/api/admin/icecast/start"));
        assert!(!allowed("GET", "/api/users"));
    }
}
