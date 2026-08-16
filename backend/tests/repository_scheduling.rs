use chrono::{Datelike, NaiveDate, NaiveTime};
use sqlx::PgPool;
use std::time::Duration;
use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository as auth_repo;
use surcast_backend::playlists::repository as playlists_repo;
use surcast_backend::scheduling::models::{AutoDjMode, RecurrenceType, SourceType};
use surcast_backend::scheduling::repository;
use surcast_backend::scheduling::service;
use surcast_backend::stations::repository as stations_repo;
use surcast_backend::stations::repository::CreateStationParams;
use uuid::Uuid;

async fn make_user(db: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    auth_repo::insert_user(db, id, &format!("user_{id}"), "hash", "Sched Tester", &Role::Admin)
        .await
        .unwrap();
    id
}

async fn make_station(db: &PgPool, user_id: Uuid) -> Uuid {
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
async fn make_auto_dj_schedule(db: &PgPool, station_id: Uuid) {
    repository::insert_schedule(
        db,
        station_id,
        chrono::Local::now().weekday().num_days_from_monday() as i16,
        NaiveTime::MIN,
        NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        &SourceType::StationLibrary,
        None,
        Some(AutoDjMode::Sequential),
        Some(false),
        Some(0),
        Some(2),
    )
    .await
    .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queue_cursor_expand_columns_persist_identity_and_format_marker(db: PgPool) {
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

#[sqlx::test(migrations = "./migrations")]
async fn test_schedule_crud(db: PgPool) {
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

#[sqlx::test(migrations = "./migrations")]
async fn test_event_crud_with_recurrence(db: PgPool) {
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

#[sqlx::test(migrations = "./migrations")]
async fn test_auto_fill_config_upsert_and_find(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;

    let config = repository::find_auto_fill_config(&db, station_id)
        .await
        .expect("find config failed")
        .expect("new station must have default AutoDJ configuration");
    assert!(config.enabled);
    assert_eq!(config.mode, AutoDjMode::Random);
    assert_eq!(config.source_type, SourceType::StationLibrary);
    assert!(config.source_playlist_id.is_none());
    assert!(config.avoid_artist_repeat);
    assert_eq!(config.min_song_gap, 3);
    assert_eq!(config.songs_ahead, 4);

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

#[sqlx::test(migrations = "./migrations")]
async fn test_auto_fill_playlists_add_update_delete(db: PgPool) {
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

#[sqlx::test(migrations = "./migrations")]
async fn test_new_station_default_auto_dj_fills_queue_without_schedule(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO songs (id, title, file_path, uploaded_by)
         VALUES ($1, 'Default AutoDJ Song', '/tmp/default-autodj-song.mp3', $2)",
    )
    .bind(song_id)
    .bind(user_id)
    .execute(&db)
    .await
    .unwrap();
    sqlx::query("INSERT INTO station_songs (station_id, song_id, position) VALUES ($1, $2, 0)")
        .bind(station_id)
        .bind(song_id)
        .execute(&db)
        .await
        .unwrap();

    service::fill_queue_from_schedule(&db, station_id, "/tmp").await.unwrap();

    let queued: Vec<(Uuid, bool)> = sqlx::query_as("SELECT song_id, is_auto_dj FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_all(&db)
        .await
        .unwrap();
    assert!(queued
        .iter()
        .any(|(queued_song_id, is_auto_dj)| *queued_song_id == song_id && *is_auto_dj));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_auto_fill_excludes_durable_current_and_upcoming_songs(db: PgPool) {
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

    make_auto_dj_schedule(&db, station_id).await;

    service::fill_queue_from_schedule(&db, station_id, "/tmp").await.unwrap();

    let queued_song_ids: Vec<Uuid> = sqlx::query_scalar("SELECT song_id FROM station_queue WHERE station_id = $1 ORDER BY position")
        .bind(station_id)
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(queued_song_ids, vec![songs[0], songs[1], songs[2]]);
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_auto_dj_refills_do_not_overfill_queue(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    for position in 0..8 {
        let song_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO songs (id, title, artist, file_path, uploaded_by)
             VALUES ($1, $2, 'test', $3, $4)",
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
            .bind(position)
            .execute(&db)
            .await
            .unwrap();
    }

    make_auto_dj_schedule(&db, station_id).await;

    let (first, second) = tokio::join!(
        service::fill_queue_from_schedule(&db, station_id, "/tmp"),
        service::fill_queue_from_schedule(&db, station_id, "/tmp"),
    );
    first.unwrap();
    second.unwrap();

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(rows, 3, "one current row plus songs_ahead=2");
}

#[sqlx::test(migrations = "./migrations")]
async fn cancelled_auto_dj_refill_releases_the_station_lock(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    make_auto_dj_schedule(&db, station_id).await;

    let mut table_lock = db.begin().await.unwrap();
    sqlx::query("LOCK TABLE station_queue IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *table_lock)
        .await
        .unwrap();

    let fill_db = db.clone();
    let fill = tokio::spawn(async move { service::fill_queue_from_schedule(&fill_db, station_id, "/tmp").await });

    let mut probe = db.acquire().await.unwrap();
    let mut held = false;
    for _ in 0..100 {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
            .bind(station_id.to_string())
            .fetch_one(&mut *probe)
            .await
            .unwrap();
        if acquired {
            sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
                .bind(station_id.to_string())
                .execute(&mut *probe)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        } else {
            held = true;
            break;
        }
    }
    assert!(held, "the refill never acquired the station lock");

    fill.abort();
    let _ = fill.await;
    table_lock.rollback().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), service::fill_queue_from_schedule(&db, station_id, "/tmp"))
        .await
        .expect("cancelled refill left the station lock held")
        .unwrap();
}
