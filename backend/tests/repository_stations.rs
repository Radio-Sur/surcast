mod common;

use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository as auth_repo;
use surcast_backend::songs::repository as songs_repo;
use surcast_backend::songs::repository::InsertSongParams;
use surcast_backend::stations::repository;
use surcast_backend::stations::repository::{CreateStationParams, UpdateStationParams};
use uuid::Uuid;

async fn make_user(db: &sqlx::PgPool) -> Uuid {
    let id = Uuid::new_v4();
    auth_repo::insert_user(db, id, &format!("user_{id}"), "hash", "Station Tester", &Role::Admin)
        .await
        .unwrap();
    id
}

async fn make_song(db: &sqlx::PgPool, user_id: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    songs_repo::insert_song_record(
        db,
        &InsertSongParams {
            id,
            title: "Test Song".into(),
            artist: "Artist".into(),
            album: "Album".into(),
            cover_path: "".into(),
            file_path: "/tmp/test.mp3".into(),
            file_size: 1024,
            mime_type: "audio/mpeg".into(),
            duration: 180,
            uploaded_by: user_id,
            ..InsertSongParams::default()
        },
    )
    .await
    .unwrap();
    id
}

#[tokio::test]
async fn test_insert_and_find_station() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id,
            name: "Test Station".into(),
            description: "A station".into(),
            slug: "test-station".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .expect("insert failed");

    let station = repository::find_station_by_id(&db, id)
        .await
        .expect("find failed")
        .expect("not found");

    assert_eq!(station.name, "Test Station");
    assert_eq!(station.slug, "test-station");
}

#[tokio::test]
async fn test_new_station_persists_enabled_auto_dj_defaults() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id: station_id,
            name: "AutoDJ Station".into(),
            description: "".into(),
            slug: "autodj-station".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "autocue".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    let config: (bool, String, String, Option<Uuid>, bool, i32, i32) = sqlx::query_as(
        "SELECT enabled, mode, source_type, source_playlist_id, avoid_artist_repeat, min_song_gap, songs_ahead
         FROM station_auto_fill WHERE station_id = $1",
    )
    .bind(station_id)
    .fetch_one(&db)
    .await
    .expect("new station must persist its enabled AutoDJ configuration");

    assert_eq!(config, (true, "random".into(), "station_library".into(), None, true, 3, 4,));
}

#[tokio::test]
async fn test_find_all_stations() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;

    repository::insert_station(
        &db,
        &CreateStationParams {
            id: Uuid::new_v4(),
            name: "Alpha".into(),
            description: "".into(),
            slug: "alpha".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id: Uuid::new_v4(),
            name: "Beta".into(),
            description: "".into(),
            slug: "beta".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    let stations = repository::find_all_stations(&db).await.expect("find_all failed");
    assert!(stations.len() >= 2);
}

#[tokio::test]
async fn test_update_station() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id,
            name: "Original".into(),
            description: "Original desc".into(),
            slug: "original".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    repository::update_station_fields(
        &db,
        &UpdateStationParams {
            id,
            name: "Updated".into(),
            description: "Updated desc".into(),
            slug: "updated".into(),
            stream_url: Some("https://example.com/stream".into()),
            prebuffer_bytes: 10,
            played_limit: 10,
            default_fade_ms: 3000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
        },
    )
    .await
    .expect("update failed");

    let station = repository::find_station_by_id(&db, id)
        .await
        .expect("find failed")
        .expect("not found");

    assert_eq!(station.name, "Updated");
    assert_eq!(station.slug, "updated");
    assert_eq!(station.stream_url, Some("https://example.com/stream".into()));
}

#[tokio::test]
async fn test_delete_station() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id,
            name: "Delete Me".into(),
            description: "".into(),
            slug: "delete-me".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    repository::delete_station(&db, id).await.expect("delete failed");

    let station = repository::find_station_by_id(&db, id).await.expect("find failed");
    assert!(station.is_none());
}

#[tokio::test]
async fn test_verify_station_exists() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id,
            name: "Exists".into(),
            description: "".into(),
            slug: "exists".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    assert!(repository::verify_station_exists(&db, id).await.is_ok());
    assert!(repository::verify_station_exists(&db, Uuid::new_v4()).await.is_err());
}

#[tokio::test]
async fn test_list_station_songs_empty() {
    let db = common::setup_db().await;
    let mut conn = db.acquire().await.unwrap();
    let user_id = make_user(&db).await;
    let station_id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id: station_id,
            name: "Empty Library".into(),
            description: "".into(),
            slug: "empty-library".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    let songs = repository::find_station_songs_joined(&mut conn, station_id)
        .await
        .expect("list failed");
    assert!(songs.is_empty());
}

#[tokio::test]
async fn test_insert_and_delete_station_song() {
    let db = common::setup_db().await;
    let mut conn = db.acquire().await.unwrap();
    let user_id = make_user(&db).await;
    let station_id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id: station_id,
            name: "Song Station".into(),
            description: "".into(),
            slug: "song-station".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    let song_id = make_song(&db, user_id).await;

    repository::insert_station_song(&mut conn, station_id, song_id)
        .await
        .expect("insert station song failed");

    let songs = repository::find_station_songs_joined(&mut conn, station_id)
        .await
        .expect("list failed");
    assert_eq!(songs.len(), 1);

    repository::delete_station_song(&db, station_id, song_id)
        .await
        .expect("delete station song failed");

    let songs = repository::find_station_songs_joined(&mut conn, station_id)
        .await
        .expect("list failed");
    assert!(songs.is_empty());
}

#[tokio::test]
async fn test_resolve_station_id_from_slug() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_station(
        &db,
        &CreateStationParams {
            id,
            name: "Slug Station".into(),
            description: "".into(),
            slug: "my-custom-slug".into(),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 5,
            default_fade_ms: 2000,
            transition_mode: "crossfade".into(),
            autocue_fade_max_ms: 5000,
            created_by: user_id,
        },
    )
    .await
    .unwrap();

    let resolved = repository::resolve_station_id_from_slug(&db, "my-custom-slug")
        .await
        .expect("resolve failed");
    assert_eq!(resolved, id);

    let err = repository::resolve_station_id_from_slug(&db, "nonexistent")
        .await
        .expect_err("should error");
    assert!(matches!(err, surcast_backend::errors::AppError::NotFound(_)));
}
