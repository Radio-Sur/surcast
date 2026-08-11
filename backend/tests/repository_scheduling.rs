mod common;

use chrono::{Datelike, NaiveDate, NaiveTime};
use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository as auth_repo;
use surcast_backend::playlists::repository as playlists_repo;
use surcast_backend::scheduling::models::{AutoDjMode, RecurrenceType, SourceType};
use surcast_backend::scheduling::repository;
use surcast_backend::scheduling::service;
use surcast_backend::stations::repository as stations_repo;
use surcast_backend::stations::repository::CreateStationParams;
use uuid::Uuid;

async fn make_user(db: &sqlx::PgPool) -> Uuid {
    let id = Uuid::new_v4();
    auth_repo::insert_user(db, id, &format!("user_{id}"), "hash", "Sched Tester", &Role::Admin)
        .await
        .unwrap();
    id
}

async fn make_station(db: &sqlx::PgPool, user_id: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    stations_repo::insert_station(
        db,
        &CreateStationParams {
            id,
            name: "Sched Station".into(),
            description: "".into(),
            slug: format!("sched-{id}"),
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
    id
}

#[tokio::test]
async fn test_queue_cursor_expand_columns_persist_identity_and_format_marker() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let current = Uuid::new_v4();
    let consumed = vec![Uuid::new_v4(), Uuid::new_v4()];

    sqlx::query(
        "UPDATE stations SET current_queue_item_id = $1, consumed_queue_item_ids = $2, current_queue_cursor_format = 1 WHERE id = $3",
    )
    .bind(current)
    .bind(&consumed)
    .bind(station_id)
    .execute(&db)
    .await
    .unwrap();

    let row: (Option<Uuid>, Vec<Uuid>, i16) =
        sqlx::query_as("SELECT current_queue_item_id, consumed_queue_item_ids, current_queue_cursor_format FROM stations WHERE id = $1")
            .bind(station_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(row.0, Some(current));
    assert_eq!(row.1, consumed);
    assert_eq!(row.2, 1);
}

#[tokio::test]
async fn test_schedule_crud() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;

    let schedule = repository::insert_schedule(
        &db,
        station_id,
        1,
        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
        &SourceType::StationLibrary,
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("insert schedule failed");

    assert_eq!(schedule.day_of_week, 1);

    let found = repository::find_schedule_by_id(&db, schedule.id)
        .await
        .expect("find failed")
        .expect("not found");
    assert_eq!(found.id, schedule.id);

    let schedules = repository::find_schedules_for_station(&db, station_id).await.expect("list failed");
    assert!(!schedules.is_empty());

    let updated = repository::update_schedule(
        &db,
        schedule.id,
        2,
        NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
        &SourceType::Playlist,
        None,
        &Some(AutoDjMode::Sequential),
        Some(true),
        Some(3),
        Some(5),
    )
    .await
    .expect("update failed");

    assert_eq!(updated.day_of_week, 2);
    assert_eq!(updated.auto_dj_mode, Some(AutoDjMode::Sequential));

    let deleted = repository::delete_schedule(&db, schedule.id).await.expect("delete failed");
    assert_eq!(deleted, 1);

    let not_found = repository::find_schedule_by_id(&db, schedule.id).await.expect("find failed");
    assert!(not_found.is_none());
}

#[tokio::test]
async fn test_event_crud_with_recurrence() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;

    let event = repository::insert_event(
        &db,
        station_id,
        &Some("Test Event".into()),
        NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
        NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        &SourceType::Playlist,
        None,
        None,
        None,
        None,
        None,
        &RecurrenceType::Weekly,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("insert event failed");

    assert_eq!(event.title, Some("Test Event".into()));
    assert_eq!(event.recurrence_type, RecurrenceType::Weekly);

    let found = repository::find_event_by_id(&db, event.id)
        .await
        .expect("find failed")
        .expect("not found");
    assert_eq!(found.id, event.id);

    let events = repository::find_events_for_station(&db, station_id).await.expect("list failed");
    assert!(!events.is_empty());

    let updated = repository::update_event(
        &db,
        event.id,
        Some("Updated Event".into()),
        NaiveDate::from_ymd_opt(2025, 2, 1).unwrap(),
        NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        &SourceType::GlobalLibrary,
        None,
        Some(AutoDjMode::Random),
        Some(false),
        Some(2),
        Some(3),
        &RecurrenceType::Daily,
        Some(1),
        None,
        None,
        None,
    )
    .await
    .expect("update failed");

    assert_eq!(updated.title, Some("Updated Event".into()));
    assert_eq!(updated.recurrence_type, RecurrenceType::Daily);

    let deleted = repository::delete_event(&db, event.id).await.expect("delete failed");
    assert_eq!(deleted, 1);

    let not_found = repository::find_event_by_id(&db, event.id).await.expect("find failed");
    assert!(not_found.is_none());
}

#[tokio::test]
async fn test_auto_fill_config_upsert_and_find() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;

    let config = repository::find_auto_fill_config(&db, station_id)
        .await
        .expect("find config failed");
    assert!(config.is_none());

    repository::upsert_auto_fill_config(
        &db,
        station_id,
        true,
        &AutoDjMode::Random,
        &SourceType::StationLibrary,
        None,
        true,
        3,
        10,
    )
    .await
    .expect("upsert failed");

    let config = repository::find_auto_fill_config(&db, station_id)
        .await
        .expect("find config failed")
        .expect("not found");
    assert!(config.enabled);
    assert_eq!(config.mode, AutoDjMode::Random);

    repository::upsert_auto_fill_config(
        &db,
        station_id,
        false,
        &AutoDjMode::Sequential,
        &SourceType::Playlist,
        None,
        false,
        5,
        8,
    )
    .await
    .expect("second upsert failed");

    let config = repository::find_auto_fill_config(&db, station_id)
        .await
        .expect("find config failed")
        .expect("not found");
    assert!(!config.enabled);
    assert_eq!(config.mode, AutoDjMode::Sequential);
}

#[tokio::test]
async fn test_auto_fill_playlists_add_update_delete() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let playlist_id = Uuid::new_v4();
    playlists_repo::insert_playlist(&db, playlist_id, "Auto-Fill Playlist", "desc", "auto-fill-playlist", user_id)
        .await
        .unwrap();

    let pl = repository::insert_auto_fill_playlist(&db, station_id, playlist_id, 10)
        .await
        .expect("insert auto-fill playlist failed");
    assert_eq!(pl.weight, 10);

    let playlists = repository::find_auto_fill_playlists(&db, station_id)
        .await
        .expect("find auto-fill playlists failed");
    assert_eq!(playlists.len(), 1);

    let updated = repository::update_auto_fill_playlist_weight(&db, pl.id, 20)
        .await
        .expect("update weight failed")
        .expect("not found");
    assert_eq!(updated.weight, 20);

    let deleted = repository::delete_auto_fill_playlist(&db, pl.id).await.expect("delete failed");
    assert_eq!(deleted, 1);

    let playlists = repository::find_auto_fill_playlists(&db, station_id)
        .await
        .expect("find auto-fill playlists failed");
    assert!(playlists.is_empty());
}

#[tokio::test]
async fn test_auto_fill_excludes_durable_current_and_upcoming_songs() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let songs = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    for (position, song_id) in songs.into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO songs (id, title, file_path, uploaded_by)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(song_id)
        .bind(format!("song-{position}"))
        .bind(format!("/tmp/song-{position}.mp3"))
        .bind(user_id)
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("INSERT INTO station_songs (station_id, song_id, position) VALUES ($1, $2, $3)")
            .bind(station_id)
            .bind(song_id)
            .bind(position as i32)
            .execute(&db)
            .await
            .unwrap();
    }

    let current_queue_item_id = Uuid::new_v4();
    sqlx::query("INSERT INTO station_queue (id, station_id, song_id, position) VALUES ($1, $2, $3, 0), ($4, $2, $5, 1)")
        .bind(current_queue_item_id)
        .bind(station_id)
        .bind(songs[0])
        .bind(Uuid::new_v4())
        .bind(songs[1])
        .execute(&db)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE stations
         SET current_queue_item_id = $1, consumed_queue_item_ids = ARRAY[]::uuid[], current_queue_cursor_format = 1
         WHERE id = $2",
    )
    .bind(current_queue_item_id)
    .bind(station_id)
    .execute(&db)
    .await
    .unwrap();

    let weekday = chrono::Local::now().weekday().num_days_from_monday() as i16;
    sqlx::query(
        "INSERT INTO station_schedules
         (station_id, day_of_week, start_time, end_time, source_type, auto_dj_mode, auto_dj_avoid_repeat, auto_dj_min_gap, auto_dj_songs_ahead)
         VALUES ($1, $2, '00:00', '23:59:59', 'station_library', 'sequential', false, 0, 2)",
    )
    .bind(station_id)
    .bind(weekday)
    .execute(&db)
    .await
    .unwrap();

    service::fill_queue_from_schedule(&db, station_id, Some(1), "/tmp").await.unwrap();

    let queued_song_ids: Vec<Uuid> = sqlx::query_scalar("SELECT song_id FROM station_queue WHERE station_id = $1 ORDER BY position")
        .bind(station_id)
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(queued_song_ids, vec![songs[0], songs[1], songs[2]]);
}
