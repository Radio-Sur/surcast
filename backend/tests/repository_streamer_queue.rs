//! Regression tests for the transactional AutoDJ cursor commit.
//!
//! `commit_cursor_and_refill` must retry the whole atomic unit (advisory
//! lock + cursor persist + trim + refill) on a *fresh* transaction after a
//! SQL error: a failed statement aborts the PostgreSQL transaction, so
//! retrying on the same handle would run every later statement on an aborted
//! transaction. The tests simulate transient failures with PL/pgSQL triggers
//! that raise once (sequences are not rolled back, so the "fail once" state
//! survives the rollback).

use sqlx::PgPool;
use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository as auth_repo;
use surcast_backend::songs::repository as songs_repo;
use surcast_backend::songs::repository::InsertSongParams;
use surcast_backend::stations::repository as stations_repo;
use surcast_backend::stations::repository::CreateStationParams;
use surcast_backend::streamer::queue_repository::QueueRepository;
use surcast_backend::streamer::queue_state::QueueCursor;
use uuid::Uuid;

async fn make_user(db: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    auth_repo::insert_user(db, id, &format!("user_{id}"), "hash", "Streamer Queue Tester", &Role::Admin)
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
            name: "Streamer Queue Station".into(),
            description: "".into(),
            slug: format!("streamer-queue-{id}"),
            stream_url: None,
            prebuffer_bytes: 0,
            played_limit: 1,
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

async fn make_song(db: &PgPool, user_id: Uuid, position: i32) -> Uuid {
    let id = Uuid::new_v4();
    songs_repo::insert_song_record(
        db,
        &InsertSongParams {
            id,
            title: format!("Song {position}"),
            artist: "Artist".into(),
            album: "Album".into(),
            cover_path: "".into(),
            file_path: format!("/tmp/streamer-{id}.mp3"),
            file_size: 1024,
            mime_type: "audio/mpeg".into(),
            duration: 200,
            uploaded_by: user_id,
            ..InsertSongParams::default()
        },
    )
    .await
    .unwrap();
    id
}

/// Queues `song_id` at `position` (following the repository's own insert
/// helper so position shifts and defaults stay consistent with the app).
async fn queue_song(db: &PgPool, station_id: Uuid, song_id: Uuid) -> Uuid {
    let queue_item_id = Uuid::new_v4();
    let position: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(position) + 1, 0) FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO station_queue (id, station_id, song_id, position, is_auto_dj)
         VALUES ($1, $2, $3, $4, false)",
    )
    .bind(queue_item_id)
    .bind(station_id)
    .bind(song_id)
    .bind(position)
    .execute(db)
    .await
    .unwrap();
    queue_item_id
}

/// A BEFORE INSERT trigger on station_queue that raises exactly once (the
/// "fail once" flag lives in a sequence, which rollbacks never rewind).
async fn fail_first_insert(db: &PgPool, sequence_name: &str, trigger_name: &str) {
    sqlx::query(&format!("CREATE SEQUENCE {sequence_name}")).execute(db).await.unwrap();
    sqlx::query(&format!(
        "CREATE FUNCTION {trigger_name}_fn() RETURNS trigger AS $$
         BEGIN
           IF nextval('{sequence_name}') = 1 THEN
             RAISE EXCEPTION 'simulated transient insert failure';
           END IF;
           RETURN NEW;
         END $$ LANGUAGE plpgsql"
    ))
    .execute(db)
    .await
    .unwrap();
    sqlx::query(&format!(
        "CREATE TRIGGER {trigger_name} BEFORE INSERT ON station_queue FOR EACH ROW EXECUTE FUNCTION {trigger_name}_fn()"
    ))
    .execute(db)
    .await
    .unwrap();
}

/// A BEFORE DELETE trigger on station_queue that raises exactly once.
async fn fail_first_delete(db: &PgPool, sequence_name: &str, trigger_name: &str) {
    sqlx::query(&format!("CREATE SEQUENCE {sequence_name}")).execute(db).await.unwrap();
    sqlx::query(&format!(
        "CREATE FUNCTION {trigger_name}_fn() RETURNS trigger AS $$
         BEGIN
           IF nextval('{sequence_name}') = 1 THEN
             RAISE EXCEPTION 'simulated transient delete failure';
           END IF;
           RETURN OLD;
         END $$ LANGUAGE plpgsql"
    ))
    .execute(db)
    .await
    .unwrap();
    sqlx::query(&format!(
        "CREATE TRIGGER {trigger_name} BEFORE DELETE ON station_queue FOR EACH ROW EXECUTE FUNCTION {trigger_name}_fn()"
    ))
    .execute(db)
    .await
    .unwrap();
}

/// An active `station_schedule_events` row for today covering the whole day,
/// so the schedule fill is active regardless of the weekday.
async fn station_autodj_songs_ahead(db: &PgPool, station_id: Uuid, songs_ahead: i32) {
    // The scheduler matches the event against `Local::now()` (the server's
    // wall clock), while Postgres' CURRENT_DATE uses the session timezone
    // (GMT here). Around midnight the two dates can differ, which would
    // silently disable the event; bind the same local date the scheduler
    // compares against.
    let local_today = chrono::Local::now().date_naive();
    sqlx::query(
        "INSERT INTO station_schedule_events (id, station_id, title, start_date, start_time, end_time, source_type,
                                                auto_dj_mode, auto_dj_songs_ahead, recurrence_type)
         VALUES ($1, $2, 'AutoDJ', $4, '00:00:00', '23:59:59', 'global_library', 'random', $3, 'none')",
    )
    .bind(Uuid::new_v4())
    .bind(station_id)
    .bind(songs_ahead)
    .bind(local_today)
    .execute(db)
    .await
    .unwrap();
}

/// Test A: the first refill attempt hits a transient SQL error; the retry
/// starts a brand-new transaction and succeeds. Expects: success, consistent
/// data, and no SQL ever executed on the aborted transaction.
#[sqlx::test(migrations = "./migrations")]
async fn commit_cursor_and_refill_retries_a_transient_fill_error_on_a_fresh_transaction(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    // Several distinct songs (and artists) so the AutoDJ window can fill.
    let mut songs = Vec::new();
    for position in 0..5 {
        songs.push(make_song(&db, user_id, position).await);
    }
    let current = queue_song(&db, station_id, songs[0]).await;
    station_autodj_songs_ahead(&db, station_id, 4).await;

    fail_first_insert(&db, "streamer_test_fill_seq", "streamer_test_fill_trigger").await;

    let repository = QueueRepository::new(db.clone(), station_id, "".into());
    let cursor = QueueCursor {
        current_queue_item_id: Some(current),
        consumed_queue_item_ids: vec![],
        legacy_position: 0,
    };

    repository
        .commit_cursor_and_refill(None, &cursor)
        .await
        .expect("the retry must succeed on a fresh transaction");

    // The cursor was persisted...
    let persisted: Option<Uuid> = sqlx::query_scalar("SELECT current_queue_item_id FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(persisted, Some(current));
    // ...and the refill eventually ran: the AutoDJ window (4 ahead) was
    // filled with auto rows on top of the one seeded song.
    let (total, auto): (i64, i64) =
        sqlx::query_as("SELECT COUNT(*), COUNT(*) FILTER (WHERE is_auto_dj) FROM station_queue WHERE station_id = $1")
            .bind(station_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(total, 5, "seeded song plus a four-song AutoDJ window");
    assert_eq!(auto, 4);
}

/// Test B: a trim DELETE fails. The error must not be swallowed; the
/// transaction is rolled back and the retry uses a fresh transaction, so the
/// cursor ends up persisted and the trim completes.
#[sqlx::test(migrations = "./migrations")]
async fn commit_cursor_and_refill_rolls_back_and_retries_after_a_trim_error(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    // played_limit = 1. Two previously consumed rows (older than the current
    // track) force the trim to delete one of them.
    let mut songs = Vec::new();
    for position in 0..5 {
        songs.push(make_song(&db, user_id, position).await);
    }
    let played_a = queue_song(&db, station_id, songs[0]).await;
    let played_b = queue_song(&db, station_id, songs[1]).await;
    let current = queue_song(&db, station_id, songs[2]).await;
    station_autodj_songs_ahead(&db, station_id, 4).await;

    // Mark the two older rows as consumed so the trim wants to delete one.
    sqlx::query(
        "UPDATE stations
         SET current_queue_item_id = $1,
             consumed_queue_item_ids = $2,
             current_song_index = 2,
             current_queue_cursor_format = 1
         WHERE id = $3",
    )
    .bind(current)
    .bind(vec![played_a, played_b])
    .bind(station_id)
    .execute(&db)
    .await
    .unwrap();

    fail_first_delete(&db, "streamer_test_trim_seq", "streamer_test_trim_trigger").await;

    let repository = QueueRepository::new(db.clone(), station_id, "".into());
    let cursor = QueueCursor {
        current_queue_item_id: Some(current),
        consumed_queue_item_ids: vec![played_a, played_b],
        legacy_position: 2,
    };

    repository
        .commit_cursor_and_refill(None, &cursor)
        .await
        .expect("the retry must succeed on a fresh transaction after the trim failure");

    // Cursor persisted and trimmed: `played_a` was deleted and removed from
    // the consumed set.
    let (persisted, consumed): (Option<Uuid>, Vec<Uuid>) =
        sqlx::query_as("SELECT current_queue_item_id, consumed_queue_item_ids FROM stations WHERE id = $1")
            .bind(station_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(persisted, Some(current));
    assert_eq!(consumed, vec![played_b]);
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(
        remaining, 6,
        "current + kept played_b + four AutoDJ picks; only the over-limit played row was trimmed"
    );
}

/// Test B': when the trim error never goes away the error must be propagated
/// (not swallowed) and the failed transaction must not have persisted
/// anything.
#[sqlx::test(migrations = "./migrations")]
async fn commit_cursor_and_refill_propagates_a_persistent_trim_error_without_partial_state(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let mut songs = Vec::new();
    for position in 0..5 {
        songs.push(make_song(&db, user_id, position).await);
    }
    let played_a = queue_song(&db, station_id, songs[0]).await;
    let played_b = queue_song(&db, station_id, songs[1]).await;
    let current = queue_song(&db, station_id, songs[2]).await;
    station_autodj_songs_ahead(&db, station_id, 4).await;

    sqlx::query(
        "UPDATE stations
         SET current_queue_item_id = $1,
             consumed_queue_item_ids = $2,
             current_song_index = 2,
             current_queue_cursor_format = 1
         WHERE id = $3",
    )
    .bind(current)
    .bind(vec![played_a, played_b])
    .bind(station_id)
    .execute(&db)
    .await
    .unwrap();

    // Fires on every DELETE: every retry attempt fails, so the error must
    // surface to the caller.
    sqlx::query("CREATE SEQUENCE streamer_test_trim_seq2").execute(&db).await.unwrap();
    sqlx::query(
        "CREATE FUNCTION streamer_test_trim_trigger2_fn() RETURNS trigger AS $$
         BEGIN
           PERFORM nextval('streamer_test_trim_seq2');
           RAISE EXCEPTION 'simulated persistent delete failure';
         END $$ LANGUAGE plpgsql",
    )
    .execute(&db)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER streamer_test_trim_trigger2 BEFORE DELETE ON station_queue FOR EACH ROW EXECUTE FUNCTION streamer_test_trim_trigger2_fn()",
    )
    .execute(&db)
    .await
    .unwrap();

    let repository = QueueRepository::new(db.clone(), station_id, "".into());
    let cursor = QueueCursor {
        current_queue_item_id: Some(current),
        consumed_queue_item_ids: vec![played_a, played_b],
        legacy_position: 2,
    };

    let error = repository.commit_cursor_and_refill(None, &cursor).await;
    match error {
        Err(error) => assert!(
            error.to_string().contains("simulated persistent delete failure"),
            "unexpected error: {error}"
        ),
        Ok(()) => {
            let (consumed_now, remaining_now): (Vec<Uuid>, i64) = sqlx::query_as(
                "SELECT consumed_queue_item_ids, (SELECT COUNT(*) FROM station_queue WHERE station_id = $1) FROM stations WHERE id = $1",
            )
            .bind(station_id)
            .fetch_one(&db)
            .await
            .unwrap();
            panic!("commit unexpectedly succeeded; consumed={consumed_now:?} remaining={remaining_now}");
        }
    }

    // Nothing from the failed attempts was committed: the stored cursor is
    // still the pre-call state and the consumed rows are untouched.
    let (persisted, consumed): (Option<Uuid>, Vec<Uuid>) =
        sqlx::query_as("SELECT current_queue_item_id, consumed_queue_item_ids FROM stations WHERE id = $1")
            .bind(station_id)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(persisted, Some(current));
    assert_eq!(consumed, vec![played_a, played_b]);
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(total, 3);
}
